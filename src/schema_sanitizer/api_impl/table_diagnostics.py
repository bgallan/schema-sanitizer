"""Diagnostics patching helpers for table and file materialization."""

from __future__ import annotations

import csv
import os
from types import SimpleNamespace
from typing import Any

from .ingest_diagnostics import _patch_diagnostics_values
from .ingest_runtime_types import Result


def _materialized_table_batch_count(table: Any) -> int:
    """Return the materialized table's batch/chunk count without scanning rows."""
    to_batches = getattr(table, "to_batches", None)
    if callable(to_batches):
        try:
            return len(to_batches())
        except Exception:
            return 0
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

    _patch_diagnostics_values(diag_raw, values)


def _arrow_type_depth(data_type: Any) -> int:
    """Return an approximate nested Arrow depth for file-footer diagnostics."""
    try:
        import pyarrow as pa
    except Exception:
        return 0

    if pa.types.is_struct(data_type):
        return 1 + max((_arrow_type_depth(child.type) for child in data_type), default=0)
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


def _arrow_schema_depth(schema: Any) -> int:
    """Return max nested Arrow depth for a PyArrow schema."""
    return max((_arrow_type_depth(field.type) for field in schema), default=0)


def _parquet_metadata_uncompressed_bytes(metadata: Any) -> int:
    """Return best-effort uncompressed Parquet row-group bytes from footer metadata."""
    total = 0
    for index in range(getattr(metadata, "num_row_groups", 0) or 0):
        try:
            total += int(metadata.row_group(index).total_byte_size or 0)
        except Exception:
            continue
    return total


def _result_diagnostics_target(result: Result) -> Any:
    """Return a mutable diagnostics target, creating one when native diagnostics are absent."""
    raw = getattr(result, "_raw", None)
    diagnostics = getattr(raw, "diagnostics", None)
    if diagnostics is not None:
        return diagnostics
    diagnostics = SimpleNamespace()
    if raw is not None:
        setattr(raw, "diagnostics", diagnostics)
    return diagnostics


def patch_parquet_file_diagnostics(result: Result, path: Any) -> None:
    """Patch file-output diagnostics from a Parquet footer without materializing rows."""
    try:
        import pyarrow.parquet as pq

        parquet_file = pq.ParquetFile(path)
        metadata = parquet_file.metadata
        arrow_schema = parquet_file.schema_arrow
    except Exception:
        return

    materialized_rows = int(getattr(metadata, "num_rows", 0) or 0)
    batches = int(getattr(metadata, "num_row_groups", 0) or 0)
    if batches <= 0 and materialized_rows > 0:
        batches = 1

    diagnostics = _result_diagnostics_target(result)
    existing_inferred_rows = int(getattr(diagnostics, "inferred_rows", 0) or 0)
    existing_inferred_bytes = int(getattr(diagnostics, "inferred_bytes", 0) or 0)
    existing_arrow_depth = int(getattr(diagnostics, "arrow_schema_depth", 0) or 0)
    existing_parquet_depth = int(getattr(diagnostics, "parquet_schema_depth", 0) or 0)
    arrow_depth = _arrow_schema_depth(arrow_schema)
    parquet_depth = max(0, arrow_depth - 1)

    values = {
        "materialized_rows": materialized_rows,
        "batches": batches,
        "inferred_rows": existing_inferred_rows or materialized_rows,
        "inferred_bytes": existing_inferred_bytes or _parquet_metadata_uncompressed_bytes(metadata),
        "arrow_schema_depth": existing_arrow_depth or arrow_depth,
        "parquet_schema_depth": existing_parquet_depth or parquet_depth,
    }
    _patch_diagnostics_values(diagnostics, values)


def _file_size_or_zero(path: Any) -> int:
    """Return a local file size, or zero when unavailable."""
    try:
        return int(os.path.getsize(path))
    except Exception:
        return 0


def _patch_record_file_diagnostics(result: Result, path: Any, row_count: int) -> None:
    """Patch file-output diagnostics from a known record count."""
    diagnostics = _result_diagnostics_target(result)
    existing_inferred_rows = int(getattr(diagnostics, "inferred_rows", 0) or 0)
    existing_inferred_bytes = int(getattr(diagnostics, "inferred_bytes", 0) or 0)
    existing_batches = int(getattr(diagnostics, "batches", 0) or 0)
    skipped_rows = int(getattr(diagnostics, "skipped_rows", 0) or 0)
    values = {
        "materialized_rows": row_count,
        "batches": existing_batches or (1 if row_count > 0 else 0),
        "inferred_rows": existing_inferred_rows or (row_count + skipped_rows),
        "inferred_bytes": existing_inferred_bytes or _file_size_or_zero(path),
    }
    _patch_diagnostics_values(diagnostics, values)


def _native_stats_int(stats: Any, key: str) -> int | None:
    """Return a non-negative native writer stat, or None when unavailable."""
    if not isinstance(stats, dict):
        return None
    try:
        value = int(stats[key])
    except Exception:
        return None
    return max(0, value)


def patch_native_record_file_diagnostics(result: Result, path: Any, stats: Any) -> bool:
    """Patch record-file diagnostics from native writer counters."""
    materialized_rows = _native_stats_int(stats, "materialized_rows")
    batches = _native_stats_int(stats, "batches")
    if materialized_rows is None or batches is None:
        return False

    diagnostics = _result_diagnostics_target(result)
    existing_inferred_rows = int(getattr(diagnostics, "inferred_rows", 0) or 0)
    existing_inferred_bytes = int(getattr(diagnostics, "inferred_bytes", 0) or 0)
    skipped_rows = int(getattr(diagnostics, "skipped_rows", 0) or 0)
    values = {
        "materialized_rows": materialized_rows,
        "batches": batches or (1 if materialized_rows > 0 else 0),
        "inferred_rows": existing_inferred_rows or (materialized_rows + skipped_rows),
        "inferred_bytes": existing_inferred_bytes or _file_size_or_zero(path),
    }
    _patch_diagnostics_values(diagnostics, values)
    return True


def patch_jsonl_file_diagnostics(result: Result, path: Any) -> None:
    """Patch JSONL file-output diagnostics by streaming output records."""
    try:
        with open(path, "rb") as handle:
            row_count = sum(1 for line in handle if line.strip())
    except Exception:
        return
    _patch_record_file_diagnostics(result, path, row_count)


def patch_csv_file_diagnostics(result: Result, path: Any) -> None:
    """Patch CSV file-output diagnostics by streaming logical CSV records."""
    try:
        with open(path, newline="", encoding="utf-8") as handle:
            record_count = sum(1 for _row in csv.reader(handle))
    except Exception:
        return
    row_count = max(0, record_count - 1)
    _patch_record_file_diagnostics(result, path, row_count)


def patch_file_output_diagnostics(
    result: Result,
    path: Any,
    feature: str,
    *,
    native_stats: Any = None,
) -> None:
    """Patch file-output diagnostics using the cheapest format-specific footer/count."""
    if feature == "to_parquet":
        patch_parquet_file_diagnostics(result, path)
    elif feature == "to_jsonl":
        if patch_native_record_file_diagnostics(result, path, native_stats):
            return
        patch_jsonl_file_diagnostics(result, path)
    elif feature == "to_csv":
        if patch_native_record_file_diagnostics(result, path, native_stats):
            return
        patch_csv_file_diagnostics(result, path)
