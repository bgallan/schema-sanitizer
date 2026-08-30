"""Diagnostics patching for table and file materialization.

It patches table and file results with final stream, sink, native-route, row, byte, and
schema diagnostics after materialization.
"""

from __future__ import annotations

import csv
import os
from importlib import import_module
from types import SimpleNamespace
from typing import Any

from ..core_impl.process_resources import open_governed_file
from .results import Result
from .streams import patch_diagnostics_values


def _diagnostics_target(result: Result) -> Any:
    """Return a mutable diagnostics target, creating one when absent."""
    raw = getattr(result, "_raw", None)
    diagnostics = getattr(raw, "diagnostics", None)
    if diagnostics is not None:
        return diagnostics
    diagnostics = SimpleNamespace()
    if raw is not None:
        setattr(raw, "diagnostics", diagnostics)
    return diagnostics


def _file_size_or_zero(path: Any) -> int:
    """Return a local file size, or zero when unavailable."""
    try:
        return int(os.path.getsize(path))
    except Exception:
        return 0


def _materialized_table_batch_count(table: Any) -> int:
    """Return the table batch count without scanning rows."""
    to_batches = getattr(table, "to_batches", None)
    if not callable(to_batches):
        return 0
    try:
        return len(to_batches())
    except Exception:
        return 0


def patch_table_diagnostics(
    stream_raw: Any,
    result: Result,
    table: Any,
    *,
    source_rows: int | None = None,
    fill_inferred_rows_when_missing: bool = False,
) -> None:
    """Finalize diagnostics after table materialization."""
    diag_raw = getattr(stream_raw, "diagnostics", None)
    if diag_raw is None:
        return

    materialized_rows = getattr(table, "num_rows", None)
    if materialized_rows is None:
        return
    batches = diag_raw.batches or 0
    if batches <= 0 and materialized_rows > 0:
        batches = _materialized_table_batch_count(table) or 1

    native_skipped_rows = diag_raw.skipped_rows or 0
    if native_skipped_rows > 0:
        skipped_rows = native_skipped_rows
    else:
        inferred_rows = diag_raw.inferred_rows or 0
        row_count = source_rows if source_rows is not None and inferred_rows == 0 else inferred_rows
        skipped_rows = max(0, row_count - materialized_rows)

    values = {
        "materialized_rows": materialized_rows,
        "batches": batches,
        "skipped_rows": skipped_rows,
    }
    if fill_inferred_rows_when_missing and (diag_raw.inferred_rows or 0) <= 0:
        values["inferred_rows"] = materialized_rows + skipped_rows

    patch_diagnostics_values(diag_raw, values)


def _patch_record_count(result: Result, path: Any, row_count: int) -> None:
    """Patch file diagnostics from a known record count."""
    diagnostics = _diagnostics_target(result)
    skipped_rows = int(getattr(diagnostics, "skipped_rows", 0) or 0)
    patch_diagnostics_values(
        diagnostics,
        {
            "materialized_rows": row_count,
            "batches": int(getattr(diagnostics, "batches", 0) or 0) or (1 if row_count > 0 else 0),
            "inferred_rows": int(getattr(diagnostics, "inferred_rows", 0) or 0)
            or (row_count + skipped_rows),
            "inferred_bytes": int(getattr(diagnostics, "inferred_bytes", 0) or 0)
            or _file_size_or_zero(path),
        },
    )


def _native_stats_int(stats: Any, key: str) -> int | None:
    """Return a non-negative native writer stat, or None when unavailable."""
    if not isinstance(stats, dict):
        return None
    try:
        return max(0, int(stats[key]))
    except Exception:
        return None


def _patch_native_record_file_diagnostics(
    result: Result,
    path: Any,
    stats: Any,
) -> bool:
    """Patch record diagnostics from native writer counters."""
    materialized_rows = _native_stats_int(stats, "materialized_rows")
    batches = _native_stats_int(stats, "batches")
    if materialized_rows is None or batches is None:
        return False
    diagnostics = _diagnostics_target(result)
    skipped_rows = int(getattr(diagnostics, "skipped_rows", 0) or 0)
    logical_batches = int(getattr(diagnostics, "batches", 0) or 0)
    patch_diagnostics_values(
        diagnostics,
        {
            "materialized_rows": materialized_rows,
            "batches": logical_batches or batches or (1 if materialized_rows > 0 else 0),
            "inferred_rows": int(getattr(diagnostics, "inferred_rows", 0) or 0)
            or (materialized_rows + skipped_rows),
            "inferred_bytes": int(getattr(diagnostics, "inferred_bytes", 0) or 0)
            or _file_size_or_zero(path),
        },
    )
    return True


def _patch_jsonl_file_diagnostics(result: Result, path: Any) -> None:
    """Patch JSONL diagnostics by streaming output records."""
    try:
        with open_governed_file(path, "rb") as handle:
            row_count = sum(1 for line in handle if line.strip())
    except Exception:
        return
    _patch_record_count(result, path, row_count)


def _patch_csv_file_diagnostics(result: Result, path: Any) -> None:
    """Patch CSV diagnostics by streaming logical records."""
    try:
        with open_governed_file(path, "r", newline="", encoding="utf-8") as handle:
            record_count = sum(1 for _row in csv.reader(handle))
    except Exception:
        return
    _patch_record_count(result, path, max(0, record_count - 1))


def _arrow_type_depth(data_type: Any) -> int:
    """Return an approximate nested Arrow depth."""
    try:
        pa = import_module("pyarrow")
    except Exception:
        return 0
    if pa.types.is_struct(data_type):
        return 1 + max(
            (_arrow_type_depth(child.type) for child in data_type),
            default=0,
        )
    if (
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or (hasattr(pa.types, "is_fixed_size_list") and pa.types.is_fixed_size_list(data_type))
    ):
        return 1 + _arrow_type_depth(data_type.value_type)
    if pa.types.is_map(data_type):
        return 1 + max(
            _arrow_type_depth(data_type.key_type),
            _arrow_type_depth(data_type.item_type),
        )
    return 0


def _metadata_uncompressed_bytes(metadata: Any) -> int:
    """Return best-effort uncompressed row-group bytes."""
    total = 0
    for index in range(getattr(metadata, "num_row_groups", 0) or 0):
        try:
            total += int(metadata.row_group(index).total_byte_size or 0)
        # Malformed advisory metadata is intentionally skipped.
        except Exception as ignored_error:
            del ignored_error
            continue
    return total


def _patch_parquet_file_diagnostics(result: Result, path: Any) -> None:
    """Patch diagnostics from a Parquet footer without materializing rows."""
    try:
        pq = import_module("pyarrow.parquet")

        with open_governed_file(path, "rb") as handle:
            parquet_file = pq.ParquetFile(handle)
            metadata = parquet_file.metadata
            arrow_schema = parquet_file.schema_arrow
    except Exception:
        return

    materialized_rows = int(getattr(metadata, "num_rows", 0) or 0)
    batches = int(getattr(metadata, "num_row_groups", 0) or 0)
    if batches <= 0 and materialized_rows > 0:
        batches = 1
    diagnostics = _diagnostics_target(result)
    arrow_depth = max(
        (_arrow_type_depth(field.type) for field in arrow_schema),
        default=0,
    )
    values = {
        "materialized_rows": materialized_rows,
        "batches": batches,
        "inferred_rows": int(getattr(diagnostics, "inferred_rows", 0) or 0) or materialized_rows,
        "inferred_bytes": int(getattr(diagnostics, "inferred_bytes", 0) or 0)
        or _metadata_uncompressed_bytes(metadata),
        "arrow_schema_depth": int(getattr(diagnostics, "arrow_schema_depth", 0) or 0)
        or arrow_depth,
        "parquet_schema_depth": int(getattr(diagnostics, "parquet_schema_depth", 0) or 0)
        or max(0, arrow_depth - 1),
    }
    patch_diagnostics_values(diagnostics, values)


def patch_file_output_diagnostics(
    result: Result,
    path: Any,
    feature: str,
    *,
    native_stats: Any = None,
    file_output_route: str | None = None,
    file_metadata_route: str | None = None,
) -> None:
    """Patch output diagnostics using the cheapest format-specific source."""
    routes = {
        key: value
        for key, value in (
            ("file_output_route", file_output_route),
            ("file_metadata_route", file_metadata_route),
        )
        if value is not None
    }
    if routes:
        patch_diagnostics_values(_diagnostics_target(result), routes)
    if feature == "to_parquet":
        _patch_parquet_file_diagnostics(result, path)
    elif feature == "to_jsonl":
        if not _patch_native_record_file_diagnostics(result, path, native_stats):
            _patch_jsonl_file_diagnostics(result, path)
    elif feature == "to_csv" and not _patch_native_record_file_diagnostics(
        result,
        path,
        native_stats,
    ):
        _patch_csv_file_diagnostics(result, path)
