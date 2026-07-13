"""Parquet runtime status, footer diagnostics, and projection audits."""

from __future__ import annotations

import json
from typing import Any

from ...core_impl.dependencies import pyarrow_importable
from ...core_impl.native_symbols import (
    ARROW_SCHEMA_CONTRACT_PAYLOAD,
    PARQUET_FOOTER_INFO_JSON,
)
from ..pyarrow.schema_decision_cache import SchemaDecisionCache
from .contract_gates.native import (
    _native_nested_contract_status_from_summary,
    _native_parquet_writer_contract_status_from_footer_info,
)
from .layout.reducer import _native_recursive_layout_summary_from_footer_info
from .projection.audits.composition import (
    _native_recursive_projection_chain_contract_audit_from_summaries,
)
from .projection.audits.coverage import (
    _native_recursive_projection_coverage_contract_audit_from_summaries,
)
from .projection.audits.partitions import (
    _native_recursive_projection_partition_contract_audit_from_summaries,
)
from .projection.audits.subset import (
    _native_recursive_projection_contract_audit_from_summaries,
)

_DIRECT_SCHEMA_SUPPORT_CACHE = SchemaDecisionCache()


def _status_snapshot(
    status: dict[str, Any] | None,
    *,
    list_fields: tuple[str, ...],
) -> dict[str, Any]:
    """Copy one contract status and the mutable list fields consumed here."""
    snapshot = dict(status or {})
    for name in list_fields:
        if name in snapshot:
            snapshot[name] = list(snapshot.get(name) or [])
    return snapshot


def _parquet_contract_runtime_readiness_status_from_capabilities(
    *,
    pyarrow_available: bool,
    native_footer_available: bool,
    native_stream_available: bool,
    require_pyarrow: bool = True,
    require_native: bool = True,
) -> dict[str, Any]:
    """Return a fail-closed environment gate for the Parquet contract suite."""
    issues: list[str] = []
    if require_pyarrow and not pyarrow_available:
        issues.append("PyArrow is required for the safe fallback contract but is not importable")
    if require_native and not native_footer_available:
        issues.append("native Parquet footer diagnostics are required but unavailable")
    if require_native and not native_stream_available:
        issues.append("native Parquet stream reader is required but unavailable")

    native_reader_available = native_footer_available and native_stream_available
    return {
        "satisfied": not issues,
        "issues": list(dict.fromkeys(issues)),
        "pyarrow_available": bool(pyarrow_available),
        "native_footer_available": bool(native_footer_available),
        "native_stream_available": bool(native_stream_available),
        "safe_fallback_runtime_available": bool(pyarrow_available),
        "native_reader_runtime_available": bool(native_reader_available),
        "schema_sanitizer_native_contracts_gateable": bool(native_reader_available),
        "nested_native_contracts_gateable": bool(
            native_footer_available and native_stream_available
        ),
        "require_pyarrow": bool(require_pyarrow),
        "require_native": bool(require_native),
    }


def _parquet_preflight_contract_status_from_writer_status(
    writer_status: dict[str, Any] | None,
    *,
    pyarrow_available: bool | None = None,
) -> dict[str, Any]:
    """Return a pre-read contract status for safe native-or-PyArrow coverage."""
    writer = _status_snapshot(
        writer_status,
        list_fields=("issues", "nested_contract_issues", "native_reader_blockers"),
    )
    if pyarrow_available is None:
        pyarrow_available = pyarrow_importable()
    native_satisfied = writer.get("satisfied") is True
    native_applicable = writer.get("applicable") is True
    native_issues = [str(issue) for issue in writer.get("issues", [])]
    issues: list[str] = []
    route: str | None
    if native_satisfied:
        route = "native_parquet_stream"
    elif pyarrow_available:
        route = "pyarrow_fallback_available"
    else:
        route = None
        issues.append(
            "PyArrow is not installed and the schema-sanitizer native writer "
            "contract is not satisfied"
        )
        issues.extend(native_issues)

    return {
        "satisfied": not issues,
        "route": route,
        "issues": list(dict.fromkeys(issues)),
        "pyarrow_available": bool(pyarrow_available),
        "native_writer_contract_satisfied": native_satisfied,
        "native_writer_contract_applicable": native_applicable,
        "native_writer_issues": native_issues,
        "safe_fallback_contract_satisfied": (not native_satisfied and bool(pyarrow_available)),
        "filters_present": bool(writer.get("filters_present")),
        "filter_contract_satisfied": writer.get("filter_contract_satisfied") is not False,
        "nested_contract_applicable": bool(writer.get("nested_contract_applicable")),
        "nested_contract_satisfied": bool(writer.get("nested_contract_satisfied")),
        "native_reader_ready": writer.get("native_reader_ready") is True,
        "created_by": writer.get("created_by"),
    }


def _parquet_contract_certification_status_from_parts(
    *,
    preflight_status: dict[str, Any] | None,
    writer_status: dict[str, Any] | None,
    projection_audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return one fail-closed certification report for Parquet contracts."""
    preflight = _status_snapshot(preflight_status, list_fields=("issues",))
    writer = _status_snapshot(
        writer_status,
        list_fields=("issues", "nested_contract_issues", "native_reader_blockers"),
    )
    projection = (
        _status_snapshot(projection_audit, list_fields=("mismatches",))
        if projection_audit is not None
        else None
    )
    issues: list[str] = []

    preflight_satisfied = preflight.get("satisfied") is True
    if not preflight_satisfied:
        issues.extend(f"preflight: {issue}" for issue in preflight.get("issues", []))
        if not preflight.get("issues"):
            issues.append("preflight contract is not satisfied")

    native_applicable = writer.get("applicable") is True
    native_satisfied = writer.get("satisfied") is True
    if native_applicable and not native_satisfied:
        issues.extend(f"native-writer: {issue}" for issue in writer.get("issues", []))
        if not writer.get("issues"):
            issues.append("native-writer contract is not satisfied")

    nested_applicable = writer.get("nested_contract_applicable") is True
    nested_satisfied = not nested_applicable or writer.get("nested_contract_satisfied") is True
    if native_applicable and not nested_satisfied:
        issues.extend(f"nested: {issue}" for issue in writer.get("nested_contract_issues", []))
        if not writer.get("nested_contract_issues"):
            issues.append("nested contract is applicable but not satisfied")

    projection_applicable = projection is not None
    projection_satisfied = not projection_applicable or projection.get("stable") is True
    if projection_applicable and not projection_satisfied:
        issues.extend(f"projection: {issue}" for issue in projection.get("mismatches", []))
        if not projection.get("mismatches"):
            issues.append("projection contract audit is not stable")

    safe_fallback = preflight.get("safe_fallback_contract_satisfied") is True
    native_route = preflight.get("route") == "native_parquet_stream"
    fallback_route = preflight.get("route") == "pyarrow_fallback_available"
    if preflight_satisfied and not (native_route or fallback_route):
        issues.append(f"preflight returned unknown route: {preflight.get('route')!r}")
    if fallback_route and not safe_fallback:
        issues.append("fallback route was selected but safe fallback was not certified")
    if native_route and not native_satisfied:
        issues.append("native route was selected but native writer contract was not certified")

    return {
        "satisfied": not issues,
        "route": preflight.get("route"),
        "issues": list(dict.fromkeys(str(issue) for issue in issues)),
        "pipeline_safe_with_fallback": preflight_satisfied,
        "native_writer_contract_applicable": native_applicable,
        "native_writer_contract_satisfied": native_satisfied,
        "nested_contract_applicable": nested_applicable,
        "nested_contract_satisfied": nested_satisfied,
        "projection_contract_applicable": projection_applicable,
        "projection_contract_satisfied": projection_satisfied,
        "pyarrow_available": preflight.get("pyarrow_available") is True,
        "safe_fallback_contract_satisfied": safe_fallback,
        "created_by": writer.get("created_by") or preflight.get("created_by"),
        "batch_size": writer.get("batch_size"),
        "max_row_group_rows": writer.get("max_row_group_rows"),
        "batch_size_contract_satisfied": (writer.get("batch_size_contract_satisfied") is not False),
        "filters_present": bool(writer.get("filters_present") or preflight.get("filters_present")),
        "filter_contract_satisfied": writer.get("filter_contract_satisfied") is not False,
        "preflight_status": preflight,
        "native_writer_status": writer,
        "projection_audit": projection,
    }


def parquet_schema_is_direct_native_eligible(
    schema: Any,
    *,
    pa: Any,
    timestamp_precision: str,
) -> bool:
    """Return whether a Parquet schema can use the direct native Arrow path."""
    del pa
    del timestamp_precision
    cache = _DIRECT_SCHEMA_SUPPORT_CACHE
    cached = cache.get_by_object(schema)
    if cached is not None:
        return cached
    cached = cache.get_by_text(schema)
    if cached is not None:
        return cache.set(schema, cached, include_text=False)

    try:
        fingerprint = bytes(ARROW_SCHEMA_CONTRACT_PAYLOAD(schema))
    except TypeError:
        fingerprint = b""
    if not fingerprint:
        return cache.set(schema, False, include_text=True)
    cached = cache.get_by_fingerprint(fingerprint)
    if cached is not None:
        return cache.set_fingerprint(schema, fingerprint, cached)
    cache.set_fingerprint(schema, fingerprint, True)
    return cache.set(schema, True, include_text=True)


def native_parquet_footer_info(
    path: Any, *, columns: list[str] | tuple[str, ...] | None = None
) -> dict[str, Any] | None:
    """Return native Parquet footer metadata for a local path when available."""
    if columns is None:
        return json.loads(PARQUET_FOOTER_INFO_JSON(path))
    return json.loads(PARQUET_FOOTER_INFO_JSON(path, list(columns)))


def native_parquet_recursive_layout_summary(
    path: Any, *, columns: list[str] | tuple[str, ...] | None = None
) -> dict[str, Any] | None:
    """Return a row-group-stability summary for native recursive Parquet layout."""
    return _native_recursive_layout_summary_from_footer_info(
        native_parquet_footer_info(path, columns=columns)
    )


def native_parquet_nested_contract_status(
    path: Any, *, columns: list[str] | tuple[str, ...] | None = None
) -> dict[str, Any]:
    """Return a production yes/no verdict for native nested layout stability."""
    return _native_nested_contract_status_from_summary(
        native_parquet_recursive_layout_summary(path, columns=columns)
    )


def native_parquet_writer_contract_status(
    path: Any,
    *,
    columns: list[str] | tuple[str, ...] | None = None,
    batch_size: int | None = None,
    filters: Any | None = None,
) -> dict[str, Any]:
    """Return a yes/no gate for schema-sanitizer-native Parquet files."""
    try:
        info = native_parquet_footer_info(path, columns=columns)
    except Exception as exc:
        return {
            "applicable": False,
            "satisfied": False,
            "issues": [f"native footer diagnostics failed: {type(exc).__name__}: {exc}"],
            "created_by": None,
            "native_writer_detected": False,
            "native_reader_ready": False,
            "native_reader_blockers": [],
            "native_stream_available": True,
            "batch_size": batch_size,
            "max_row_group_rows": 0,
            "batch_size_contract_satisfied": False,
            "filters_present": filters is not None,
            "filter_contract_satisfied": filters is None,
            "row_group_count": None,
            "num_rows": None,
            "nested_contract_applicable": False,
            "nested_contract_satisfied": False,
            "nested_contract_issues": [],
            "canonical_layout_fingerprint": "",
            "canonical_leaf_contract_fingerprint": "",
            "canonical_root_contract_fingerprint": "",
        }
    return _native_parquet_writer_contract_status_from_footer_info(
        info,
        native_stream_available=True,
        batch_size=batch_size,
        filters=filters,
    )


def native_parquet_recursive_projection_contract_audit(
    path: Any, *, columns: list[str] | tuple[str, ...]
) -> dict[str, Any]:
    """Audit that a recursive projection preserves full-file root contracts."""
    return _native_recursive_projection_contract_audit_from_summaries(
        native_parquet_recursive_layout_summary(path),
        native_parquet_recursive_layout_summary(path, columns=columns),
        columns=columns,
    )


def native_parquet_recursive_projection_chain_contract_audit(
    path: Any,
    *,
    source_columns: list[str] | tuple[str, ...],
    columns: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    """Audit that recursive projections compose transitively."""
    return _native_recursive_projection_chain_contract_audit_from_summaries(
        native_parquet_recursive_layout_summary(path),
        native_parquet_recursive_layout_summary(path, columns=source_columns),
        native_parquet_recursive_layout_summary(path, columns=columns),
        source_columns=source_columns,
        columns=columns,
    )


def native_parquet_recursive_projection_partition_contract_audit(
    path: Any,
    *,
    partitions: list[list[str] | tuple[str, ...]] | tuple[list[str] | tuple[str, ...], ...],
) -> dict[str, Any]:
    """Audit that recursive projections exactly partition the full layout."""
    normalized: list[list[str] | tuple[str, ...]] = [
        [str(column) for column in partition] for partition in partitions
    ]
    return _native_recursive_projection_partition_contract_audit_from_summaries(
        native_parquet_recursive_layout_summary(path),
        [native_parquet_recursive_layout_summary(path, columns=part) for part in normalized],
        partitions=normalized,
    )


def native_parquet_recursive_projection_coverage_contract_audit(
    path: Any,
    *,
    projections: list[list[str] | tuple[str, ...]] | tuple[list[str] | tuple[str, ...], ...],
    require_full_coverage: bool = False,
    allow_overlaps: bool = True,
) -> dict[str, Any]:
    """Audit partial or overlapping projected reads against the full layout."""
    normalized: list[list[str] | tuple[str, ...]] = [
        [str(column) for column in projection] for projection in projections
    ]
    return _native_recursive_projection_coverage_contract_audit_from_summaries(
        native_parquet_recursive_layout_summary(path),
        [native_parquet_recursive_layout_summary(path, columns=part) for part in normalized],
        projections=normalized,
        require_full_coverage=require_full_coverage,
        allow_overlaps=allow_overlaps,
    )


def parquet_contract_runtime_readiness_status(
    *, require_pyarrow: bool = True, require_native: bool = True
) -> dict[str, Any]:
    """Return whether the installed runtime can enforce Parquet contracts."""
    return _parquet_contract_runtime_readiness_status_from_capabilities(
        pyarrow_available=pyarrow_importable(),
        native_footer_available=True,
        native_stream_available=True,
        require_pyarrow=require_pyarrow,
        require_native=require_native,
    )


def parquet_preflight_contract_status(
    path: Any,
    *,
    columns: list[str] | tuple[str, ...] | None = None,
    batch_size: int | None = None,
    filters: Any | None = None,
) -> dict[str, Any]:
    """Return a fail-closed preflight gate for the Parquet read pipeline."""
    return _parquet_preflight_contract_status_from_writer_status(
        native_parquet_writer_contract_status(
            path, columns=columns, batch_size=batch_size, filters=filters
        ),
        pyarrow_available=pyarrow_importable(),
    )


def parquet_contract_certification_status(
    path: Any,
    *,
    columns: list[str] | tuple[str, ...] | None = None,
    batch_size: int | None = None,
    filters: Any | None = None,
    projections: list[list[str] | tuple[str, ...]]
    | tuple[list[str] | tuple[str, ...], ...]
    | None = None,
    require_full_projection_coverage: bool = False,
    allow_projection_overlaps: bool = True,
) -> dict[str, Any]:
    """Return one fail-closed certification report for the Parquet pipeline."""
    writer_status = native_parquet_writer_contract_status(
        path, columns=columns, batch_size=batch_size, filters=filters
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status, pyarrow_available=pyarrow_importable()
    )
    projection_audit = None
    if projections is not None and writer_status.get("applicable") is True:
        projection_audit = native_parquet_recursive_projection_coverage_contract_audit(
            path,
            projections=projections,
            require_full_coverage=require_full_projection_coverage,
            allow_overlaps=allow_projection_overlaps,
        )
    return _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status,
        writer_status=writer_status,
        projection_audit=projection_audit,
    )
