"""Contract verdicts for native Parquet layouts and writer output."""

from __future__ import annotations

from typing import Any

from .layout.reducer import _native_recursive_layout_summary_from_footer_info
from .memory import (
    _native_parquet_batch_size_contract_issue,
    _native_parquet_max_row_group_rows,
)

_NATIVE_PARQUET_WRITER_CREATED_BY = "schema-sanitizer native parquet writer"


def _native_nested_contract_status_from_summary(
    summary: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return an explicit yes/no contract verdict for native nested layouts.

    The recursive summary is intentionally detailed. This helper reduces it to
    a production gate: were all row groups decoded, did every contract family
    remain stable, and were there no component/leaf ownership collisions? It is
    deliberately defensive so malformed diagnostics fail closed instead of
    accidentally certifying a nested plan.
    """
    issues: list[str] = []
    if not isinstance(summary, dict):
        return {
            "applicable": False,
            "satisfied": False,
            "issues": ["recursive layout summary unavailable"],
            "row_group_count": 0,
            "decoded_row_group_count": 0,
            "field_count": 0,
            "canonical_layout_fingerprint": "",
            "canonical_leaf_contract_fingerprint": "",
            "canonical_root_contract_fingerprint": "",
        }

    fields = list(summary.get("fields") or [])
    row_group_count = summary.get("row_group_count")
    decoded_row_group_count = summary.get("decoded_row_group_count")
    try:
        expected_row_groups = int(row_group_count or 0)
    except (TypeError, ValueError):
        expected_row_groups = 0
    try:
        decoded_row_groups = int(decoded_row_group_count or 0)
    except (TypeError, ValueError):
        decoded_row_groups = 0

    applicable = bool(fields) or decoded_row_groups > 0
    if not applicable:
        issues.append("recursive layout summary contains no decoded fields")
    if applicable and expected_row_groups and decoded_row_groups != expected_row_groups:
        issues.append(
            "decoded row-group count does not match footer row-group count: "
            f"decoded={decoded_row_groups} expected={expected_row_groups}"
        )
    if summary.get("stable_across_row_groups") is not True:
        issues.append("recursive layout is not stable across row groups")
    for mismatch in list(summary.get("mismatches") or []):
        issues.append(str(mismatch))

    stable_flags = {
        "row_group_layout_fingerprints_stable": "layout fingerprints drifted",
        "row_group_leaf_level_fingerprints_stable": "leaf level fingerprints drifted",
        "row_group_repetition_path_fingerprints_stable": "repetition path fingerprints drifted",
        "row_group_repeated_ancestor_fingerprints_stable": (
            "repeated ancestor fingerprints drifted"
        ),
        "row_group_leaf_contract_fingerprints_stable": "leaf contract fingerprints drifted",
        "row_group_root_contract_fingerprints_stable": "root contract fingerprints drifted",
    }
    for flag, message in stable_flags.items():
        if summary.get(flag) is not True:
            issues.append(message)

    if summary.get("leaf_path_collisions"):
        issues.append("leaf path ownership collisions detected")
    if summary.get("repeated_node_path_collisions"):
        issues.append("repeated-node path ownership collisions detected")
    if fields and not summary.get("field_fingerprints_by_name"):
        issues.append("field fingerprints are missing")
    if fields and not summary.get("leaf_contract_fingerprints_by_name"):
        issues.append("leaf contract fingerprints are missing")
    if fields and not summary.get("root_contract_fingerprints_by_name"):
        issues.append("root contract fingerprints are missing")

    deduped_issues = list(dict.fromkeys(issues))
    return {
        "applicable": applicable,
        "satisfied": applicable and not deduped_issues,
        "issues": deduped_issues,
        "row_group_count": expected_row_groups,
        "decoded_row_group_count": decoded_row_groups,
        "field_count": len(fields),
        "field_order": list(summary.get("field_order") or []),
        "canonical_layout_fingerprint": str(summary.get("canonical_layout_fingerprint") or ""),
        "canonical_leaf_level_fingerprint": str(
            summary.get("canonical_leaf_level_fingerprint") or ""
        ),
        "canonical_leaf_repetition_path_fingerprint": str(
            summary.get("canonical_leaf_repetition_path_fingerprint") or ""
        ),
        "canonical_leaf_repeated_ancestor_fingerprint": str(
            summary.get("canonical_leaf_repeated_ancestor_fingerprint") or ""
        ),
        "canonical_leaf_contract_fingerprint": str(
            summary.get("canonical_leaf_contract_fingerprint") or ""
        ),
        "canonical_root_contract_fingerprint": str(
            summary.get("canonical_root_contract_fingerprint") or ""
        ),
    }


def _native_nested_contract_diagnostics(info: dict[str, Any] | None) -> dict[str, Any]:
    """Return native nested contract fields suitable for last-read diagnostics."""
    if isinstance(info, dict) and info.get("bounded_preflight") == 1:
        try:
            row_group_count = int(info.get("row_group_count") or 0)
        except (TypeError, ValueError):
            row_group_count = 0
        applicable = (
            info.get("created_by") == _NATIVE_PARQUET_WRITER_CREATED_BY and row_group_count > 0
        )
        satisfied = applicable and info.get("native_reader_ready") == 1
        issues = (
            []
            if satisfied or not applicable
            else [str(item) for item in list(info.get("native_reader_blockers") or [])]
        )
        return {
            "native_nested_contract_applicable": applicable,
            "native_nested_contract_satisfied": satisfied,
            "native_nested_contract_issues": issues,
        }
    status = _native_nested_contract_status_from_summary(
        _native_recursive_layout_summary_from_footer_info(info)
    )
    return {
        "native_nested_contract_applicable": bool(status.get("applicable")),
        "native_nested_contract_satisfied": bool(status.get("satisfied")),
        "native_nested_contract_issues": list(status.get("issues") or []),
    }


def _native_parquet_writer_contract_status_from_footer_info(
    info: dict[str, Any] | None,
    *,
    native_stream_available: bool | None = None,
    batch_size: int | None = None,
    filters: Any | None = None,
) -> dict[str, Any]:
    """Return a compact contract verdict for schema-sanitizer-native files."""
    issues: list[str] = []
    info = info or {}
    created_by = info.get("created_by")
    native_writer_detected = created_by == _NATIVE_PARQUET_WRITER_CREATED_BY
    if not native_writer_detected:
        issues.append("Parquet file was not created by schema-sanitizer's native writer")

    native_ready = info.get("native_reader_ready") == 1
    blockers = [str(item) for item in list(info.get("native_reader_blockers") or [])]
    if not native_ready:
        issues.append("native reader did not mark the file ready")
    for blocker in blockers:
        issues.append(f"native reader blocker: {blocker}")

    if native_stream_available is False:
        issues.append("native Parquet stream function is unavailable")

    batch_size_issue = _native_parquet_batch_size_contract_issue(info, batch_size)
    if batch_size_issue is not None:
        issues.append(f"native reader batch-size contract: {batch_size_issue}")

    filters_present = filters is not None
    if filters_present:
        issues.append(
            "native reader filter contract: predicate filters require the "
            "PyArrow dataset fallback route"
        )

    nested_status = _native_nested_contract_status_from_summary(
        _native_recursive_layout_summary_from_footer_info(info)
    )
    if nested_status.get("applicable") and nested_status.get("satisfied") is not True:
        for issue in list(nested_status.get("issues") or []):
            issues.append(f"nested contract: {issue}")

    deduped_issues = list(dict.fromkeys(issues))
    return {
        "applicable": native_writer_detected,
        "satisfied": native_writer_detected and native_ready and not deduped_issues,
        "issues": deduped_issues,
        "created_by": created_by,
        "native_writer_detected": native_writer_detected,
        "native_reader_ready": native_ready,
        "native_reader_blockers": blockers,
        "native_stream_available": native_stream_available is not False,
        "batch_size": batch_size,
        "max_row_group_rows": _native_parquet_max_row_group_rows(info),
        "batch_size_contract_satisfied": batch_size_issue is None,
        "filters_present": filters_present,
        "filter_contract_satisfied": not filters_present,
        "row_group_count": info.get("row_group_count"),
        "num_rows": info.get("num_rows"),
        "nested_contract_applicable": bool(nested_status.get("applicable")),
        "nested_contract_satisfied": bool(nested_status.get("satisfied")),
        "nested_contract_issues": list(nested_status.get("issues") or []),
        "canonical_layout_fingerprint": nested_status.get("canonical_layout_fingerprint", ""),
        "canonical_leaf_contract_fingerprint": nested_status.get(
            "canonical_leaf_contract_fingerprint", ""
        ),
        "canonical_root_contract_fingerprint": nested_status.get(
            "canonical_root_contract_fingerprint", ""
        ),
    }
