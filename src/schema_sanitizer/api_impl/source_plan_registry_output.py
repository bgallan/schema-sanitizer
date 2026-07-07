"""Shared consumers for opened source-plan registry streams."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from ..adapters import pyarrow_streams as _pyarrow_streams
from .ingest_runtime_types import Result
from .stream_writer_core import write_raw_stream_to_file
from .table_diagnostics import patch_file_output_diagnostics, patch_table_diagnostics
from .table_output import convert_arrow_table_output


def result_from_opened_registry_stream(opened: Any) -> Result:
    """Return a Result carrying registry metadata from an opened registry stream."""
    return Result(
        SimpleNamespace(diagnostics=opened.diagnostics),
        schema_registry_json=opened.schema_registry_json,
        schema_drifts_json=opened.schema_drifts_json,
        native_registry_state=getattr(opened, "native_registry_state", None),
    )


def materialize_opened_registry_stream(
    opened: Any,
    *,
    target: str,
) -> Result:
    """Materialize an opened registry stream into the requested analytical target."""
    try:
        table = _pyarrow_streams.table_from_stream_like(
            opened.materialization_stream(),
            feature=f"to_{target}",
        )
        clean_data = convert_arrow_table_output(
            table,
            target,
            feature=f"to_{target}",
        )
        result = Result(
            SimpleNamespace(diagnostics=opened.diagnostics),
            clean_data=clean_data,
            schema_registry_json=opened.schema_registry_json,
            schema_drifts_json=opened.schema_drifts_json,
            native_registry_state=getattr(opened, "native_registry_state", None),
        )
        patch_table_diagnostics(
            SimpleNamespace(diagnostics=opened.diagnostics),
            result,
            table,
            fill_inferred_rows_when_missing=True,
        )
        return result
    finally:
        opened.close()


def write_opened_registry_stream_to_file(
    opened: Any,
    out_path: Any,
    *,
    writer: Any,
    feature: str,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> Result:
    """Write an opened registry stream whose generated metadata is already present."""
    try:
        raw_stream = opened.take_raw_output_stream()
        if raw_stream is not None:
            result = write_raw_stream_to_file(
                raw_stream,
                out_path,
                writer=writer,
                feature=feature,
                first_row_columns=None,
                all_row_columns=None,
                row_span_columns=None,
                timestamp_columns=(),
                parquet_compression=parquet_compression,
                parquet_gzip_level=parquet_gzip_level,
            )
            result.schema_registry_json = opened.schema_registry_json
            result.schema_drifts_json = opened.schema_drifts_json
            result.native_registry_state = getattr(opened, "native_registry_state", None)
            return result

        parquet_kwargs = (
            {
                "parquet_compression": parquet_compression,
                "parquet_gzip_level": parquet_gzip_level,
            }
            if parquet_compression is not None or parquet_gzip_level is not None
            else {}
        )
        native_stats = writer(
            opened.output_stream(),
            out_path,
            feature=feature,
            first_row_columns=None,
            all_row_columns=None,
            row_span_columns=None,
            timestamp_columns=(),
            **parquet_kwargs,
        )
        result = result_from_opened_registry_stream(opened)
        patch_file_output_diagnostics(result, out_path, feature, native_stats=native_stats)
        return result
    finally:
        opened.close()
