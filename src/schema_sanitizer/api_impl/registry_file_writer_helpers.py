"""Shared helpers for registry-backed streaming file writers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .file_conversion_metadata import (
    SCHEMA_DRIFTS_COLUMN,
    SCHEMA_REGISTRY_COLUMN,
)
from .ingest_runtime_selectors import _Source
from .ingest_runtime_types import Result
from .parquet_direct import (
    last_parquet_direct_route,
    parquet_direct_registry_sink_raw_or_none,
    parquet_direct_stream_factory_or_none,
)
from .pool import default_pool
from .shared import Options
from .stream_writer_core import write_raw_stream_to_file


def _write_registry_raw_stream_to_file(
    raw: Any,
    out_path: Any,
    *,
    writer: Callable[..., None],
    feature: str,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
    metadata_already_in_stream: bool = False,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
    schema_registry_json: str | None = None,
    schema_drifts_json: str | None = None,
) -> Result:
    """Write a registry-backed sink from an already-open native raw stream."""
    if schema_registry_json is None:
        schema_registry_json = raw.schema_registry_json
    if schema_drifts_json is None:
        schema_drifts_json = raw.schema_drifts_json
    if metadata_already_in_stream:
        merged_first_row_columns = None
        all_row_columns = None
        row_span_columns = None
        timestamp_columns = ()
    else:
        merged_first_row_columns = dict(first_row_columns or {})
        merged_first_row_columns.update(
            {
                SCHEMA_REGISTRY_COLUMN: schema_registry_json,
                SCHEMA_DRIFTS_COLUMN: schema_drifts_json,
            }
        )
    result = write_raw_stream_to_file(
        raw,
        out_path,
        writer=writer,
        feature=feature,
        first_row_columns=merged_first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
    )
    result.schema_registry_json = schema_registry_json
    result.schema_drifts_json = schema_drifts_json
    result.native_registry_state = getattr(raw, "native_registry_state", None)
    return result


def _try_write_direct_parquet_registry_to_file(
    data: Any,
    out_path: Any,
    *,
    source: _Source,
    writer: Callable[..., None],
    feature: str,
    call_options: Options | None,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None = None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
    timestamp_columns: tuple[str, ...] = (),
    schema_registry_json: str,
    schema_mode: str,
    field_name_policy: str,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> Result | None:
    """Write direct Parquet registry output when the native Arrow path applies."""
    direct_raw = parquet_direct_registry_sink_raw_or_none(
        default_pool().get()._raw,
        data,
        source=source,
        feature=feature,
        call_options=call_options,
        schema_registry_json=schema_registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
    )
    if direct_raw is None:
        if last_parquet_direct_route() == "pyarrow_registry_unavailable":
            fallback_raw = parquet_direct_stream_factory_or_none(
                data,
                source=source,
                feature=feature,
                call_options=call_options,
            )
            if fallback_raw is not None:
                return _write_registry_raw_stream_to_file(
                    fallback_raw,
                    out_path,
                    writer=writer,
                    feature=feature,
                    first_row_columns=first_row_columns,
                    all_row_columns=all_row_columns,
                    row_span_columns=row_span_columns,
                    timestamp_columns=timestamp_columns,
                    parquet_compression=parquet_compression,
                    parquet_gzip_level=parquet_gzip_level,
                    schema_registry_json=schema_registry_json,
                    schema_drifts_json="[]",
                )
        return None
    return _write_registry_raw_stream_to_file(
        direct_raw,
        out_path,
        writer=writer,
        feature=feature,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
    )
