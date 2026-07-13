"""Subset contract audits for native Parquet recursive projections."""

from __future__ import annotations

from typing import Any

from .summary import (
    canonical_fingerprint,
    duplicate_names,
    note_mismatch,
    ordered_fingerprint,
    summary_dict,
    summary_list,
)


def build_subset_audit(
    full_summary: dict[str, Any] | None,
    projected_summary: dict[str, Any] | None,
    columns: list[str] | tuple[str, ...] | None,
) -> tuple[dict[str, Any], list[str], list[str]]:
    """Create the JSON-friendly audit state and validate its prerequisites."""
    projected_order = summary_list(projected_summary, "field_order")
    expected_columns = (
        [str(column) for column in columns] if columns is not None else list(projected_order)
    )
    audit: dict[str, Any] = {
        "stable": True,
        "mismatches": [],
        "full_summary_ready": isinstance(full_summary, dict),
        "projected_summary_ready": isinstance(projected_summary, dict),
        "full_stable_across_row_groups": bool(
            full_summary.get("stable_across_row_groups")
            if isinstance(full_summary, dict)
            else False
        ),
        "projected_stable_across_row_groups": bool(
            projected_summary.get("stable_across_row_groups")
            if isinstance(projected_summary, dict)
            else False
        ),
        "expected_columns": expected_columns,
        "projected_field_order": projected_order,
        "projection_order_matches": projected_order == expected_columns,
        "duplicate_expected_columns": duplicate_names(expected_columns),
        "missing_source_columns": [],
        "missing_projected_columns": [],
        "unexpected_projected_columns": [],
        "field_fingerprint_matches_by_name": {},
        "leaf_contract_matches_by_name": {},
        "root_contract_matches_by_name": {},
        "expected_field_fingerprints_by_name": {},
        "projected_field_fingerprints_by_name": {},
        "expected_leaf_contract_fingerprints_by_name": {},
        "projected_leaf_contract_fingerprints_by_name": {},
        "expected_root_contract_fingerprints_by_name": {},
        "projected_root_contract_fingerprints_by_name": {},
        "expected_projection_field_fingerprint": "",
        "actual_projection_field_fingerprint": "",
        "expected_projection_leaf_contract_fingerprint": "",
        "actual_projection_leaf_contract_fingerprint": "",
        "expected_projection_root_contract_fingerprint": "",
        "actual_projection_root_contract_fingerprint": "",
        "canonical_expected_root_contract_fingerprint": "",
        "canonical_actual_root_contract_fingerprint": "",
    }
    if full_summary is None:
        note_mismatch(audit, "full recursive layout summary is unavailable")
    if projected_summary is None:
        note_mismatch(audit, "projected recursive layout summary is unavailable")
    if full_summary is None or projected_summary is None:
        return audit, expected_columns, projected_order
    if not audit["full_stable_across_row_groups"]:
        note_mismatch(audit, "full recursive layout summary is not stable across row groups")
    if not audit["projected_stable_across_row_groups"]:
        note_mismatch(audit, "projected recursive layout summary is not stable across row groups")
    if not audit["projection_order_matches"]:
        note_mismatch(
            audit,
            "projected recursive field order does not match requested projection order",
        )
    if audit["duplicate_expected_columns"]:
        note_mismatch(
            audit,
            "projected recursive contract audit received duplicate columns: "
            f"{audit['duplicate_expected_columns']!r}",
        )
    return audit, expected_columns, projected_order


def compare_subset_contracts(
    audit: dict[str, Any],
    full_summary: dict[str, Any],
    projected_summary: dict[str, Any],
    expected_columns: list[str],
    projected_order: list[str],
) -> tuple[
    dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]
]:
    """Compare projected columns and per-root contracts, mutating ``audit``."""
    full_fields = summary_dict(full_summary, "field_fingerprints_by_name")
    projected_fields = summary_dict(projected_summary, "field_fingerprints_by_name")
    full_leaf_contracts = summary_dict(full_summary, "leaf_contract_fingerprints_by_name")
    projected_leaf_contracts = summary_dict(projected_summary, "leaf_contract_fingerprints_by_name")
    full_roots = summary_dict(full_summary, "root_contract_fingerprints_by_name")
    projected_roots = summary_dict(projected_summary, "root_contract_fingerprints_by_name")

    expected_set = set(expected_columns)
    projected_set = set(projected_order)
    audit["missing_source_columns"] = sorted(expected_set - set(full_roots))
    audit["missing_projected_columns"] = sorted(expected_set - projected_set)
    audit["unexpected_projected_columns"] = sorted(projected_set - expected_set)
    if audit["missing_source_columns"]:
        note_mismatch(
            audit,
            "projected recursive contract references columns absent from full layout: "
            f"{audit['missing_source_columns']!r}",
        )
    if audit["missing_projected_columns"]:
        note_mismatch(
            audit,
            "projected recursive layout omitted requested columns: "
            f"{audit['missing_projected_columns']!r}",
        )
    if audit["unexpected_projected_columns"]:
        note_mismatch(
            audit,
            "projected recursive layout returned unexpected columns: "
            f"{audit['unexpected_projected_columns']!r}",
        )

    comparable_columns = [
        name for name in expected_columns if name in full_roots and name in projected_roots
    ]
    for name in comparable_columns:
        audit["field_fingerprint_matches_by_name"][name] = full_fields.get(
            name
        ) == projected_fields.get(name)
        audit["leaf_contract_matches_by_name"][name] = full_leaf_contracts.get(
            name
        ) == projected_leaf_contracts.get(name)
        audit["root_contract_matches_by_name"][name] = full_roots.get(name) == projected_roots.get(
            name
        )
        if not audit["field_fingerprint_matches_by_name"][name]:
            note_mismatch(audit, f"projected recursive field fingerprint drifted for {name!r}")
        if not audit["leaf_contract_matches_by_name"][name]:
            note_mismatch(audit, f"projected recursive leaf contract drifted for {name!r}")
        if not audit["root_contract_matches_by_name"][name]:
            note_mismatch(audit, f"projected recursive root contract drifted for {name!r}")

    audit["expected_field_fingerprints_by_name"] = {
        name: full_fields[name] for name in sorted(expected_set & set(full_fields))
    }
    audit["projected_field_fingerprints_by_name"] = {
        name: projected_fields[name] for name in sorted(projected_set & set(projected_fields))
    }
    audit["expected_leaf_contract_fingerprints_by_name"] = {
        name: full_leaf_contracts[name] for name in sorted(expected_set & set(full_leaf_contracts))
    }
    audit["projected_leaf_contract_fingerprints_by_name"] = {
        name: projected_leaf_contracts[name]
        for name in sorted(projected_set & set(projected_leaf_contracts))
    }
    audit["expected_root_contract_fingerprints_by_name"] = {
        name: full_roots[name] for name in sorted(expected_set & set(full_roots))
    }
    audit["projected_root_contract_fingerprints_by_name"] = {
        name: projected_roots[name] for name in sorted(projected_set & set(projected_roots))
    }
    return (
        full_fields,
        projected_fields,
        full_leaf_contracts,
        projected_leaf_contracts,
        full_roots,
        projected_roots,
    )


def record_subset_fingerprints(
    audit: dict[str, Any],
    expected_columns: list[str],
    projected_order: list[str],
    full_fields: dict[str, Any],
    projected_fields: dict[str, Any],
    full_leaf_contracts: dict[str, Any],
    projected_leaf_contracts: dict[str, Any],
    full_roots: dict[str, Any],
    projected_roots: dict[str, Any],
) -> None:
    """Record ordered and canonical fingerprints, noting canonical drift."""
    audit["expected_projection_field_fingerprint"] = ordered_fingerprint(
        full_fields, expected_columns
    )
    audit["actual_projection_field_fingerprint"] = ordered_fingerprint(
        projected_fields, projected_order
    )
    audit["expected_projection_leaf_contract_fingerprint"] = ordered_fingerprint(
        full_leaf_contracts, expected_columns
    )
    audit["actual_projection_leaf_contract_fingerprint"] = ordered_fingerprint(
        projected_leaf_contracts, projected_order
    )
    audit["expected_projection_root_contract_fingerprint"] = ordered_fingerprint(
        full_roots, expected_columns
    )
    audit["actual_projection_root_contract_fingerprint"] = ordered_fingerprint(
        projected_roots, projected_order
    )
    audit["canonical_expected_root_contract_fingerprint"] = canonical_fingerprint(
        full_roots, expected_columns
    )
    audit["canonical_actual_root_contract_fingerprint"] = canonical_fingerprint(
        projected_roots, projected_order
    )
    if (
        audit["canonical_expected_root_contract_fingerprint"]
        != audit["canonical_actual_root_contract_fingerprint"]
    ):
        note_mismatch(
            audit,
            "projected recursive canonical root-contract fingerprint does not match full layout subset",
        )


def _native_recursive_projection_contract_audit_from_summaries(
    full_summary: dict[str, Any] | None,
    projected_summary: dict[str, Any] | None,
    *,
    columns: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Compare full-file and projected recursive root contracts defensively."""
    audit, expected_columns, projected_order = build_subset_audit(
        full_summary, projected_summary, columns
    )
    if full_summary is None or projected_summary is None:
        return audit
    mappings = compare_subset_contracts(
        audit,
        full_summary,
        projected_summary,
        expected_columns,
        projected_order,
    )
    record_subset_fingerprints(
        audit,
        expected_columns,
        projected_order,
        *mappings,
    )
    return audit
