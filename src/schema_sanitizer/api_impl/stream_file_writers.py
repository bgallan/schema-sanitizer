"""Plain registry-free streaming file writer orchestration."""

from __future__ import annotations

from typing import Any

from .ingest_runtime_selectors import _Format, _Source
from .ingest_runtime_types import Result
from .native_file_output import (
    write_csv_native_first_stream,
    write_jsonl_native_first_stream,
    write_parquet_native_first_stream,
)
from .shared import Options
from .stream_writer_core import write_with_file_output


def _to_parquet_stream(
    data: Any,
    out_path: Any,
    options: Options | dict[str, Any] | None = None,
    *,
    format: _Format = "auto",
    source: _Source = "auto",
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> Result:
    """Ingest and write a Parquet file."""
    return write_with_file_output(
        data,
        out_path,
        options=options,
        format=format,
        source=source,
        writer=write_parquet_native_first_stream,
        feature="to_parquet",
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
    )


def _to_csv_stream(
    data: Any,
    out_path: Any,
    options: Options | dict[str, Any] | None = None,
    *,
    format: _Format = "auto",
    source: _Source = "auto",
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
) -> Result:
    """Ingest and write a CSV file."""
    return write_with_file_output(
        data,
        out_path,
        options=options,
        format=format,
        source=source,
        writer=write_csv_native_first_stream,
        feature="to_csv",
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
    )


def _to_jsonl_stream(
    data: Any,
    out_path: Any,
    options: Options | dict[str, Any] | None = None,
    *,
    format: _Format = "auto",
    source: _Source = "auto",
    first_row_columns: dict[str, Any] | None = None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
) -> Result:
    """Ingest and write a JSONL file."""
    return write_with_file_output(
        data,
        out_path,
        options=options,
        format=format,
        source=source,
        writer=write_jsonl_native_first_stream,
        feature="to_jsonl",
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
    )
