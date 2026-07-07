"""Direct native file-output attempts for Arrow streams."""

from __future__ import annotations

import os
from contextlib import suppress
from typing import Any

from ..adapters.pyarrow_common import ensure_pyarrow
from ..adapters.pyarrow_csv_sink import _native_csv_schema_supported, mark_csv_stream_route
from ..adapters.pyarrow_jsonl_sink import (
    _schema_supports_native_jsonl,
    mark_jsonl_stream_route,
)
from ..core_impl.native_functions import (
    CSV_STREAM_WRITE,
    CSV_STREAM_WRITE_WITH_METADATA,
    JSONL_STREAM_WRITE,
    JSONL_STREAM_WRITE_WITH_METADATA,
    PARQUET_STREAM_WRITE,
    PARQUET_STREAM_WRITE_WITH_METADATA,
)
from ..core_impl.path_uris import local_path_or_reject_remote
from .file_output_metadata import (
    has_metadata_columns,
    mark_metadata_route,
    native_metadata_args_or_none,
)
from .parquet_compression_options import native_parquet_compression_environment


def _local_output_path(out_path: Any, *, format_name: str) -> str:
    """Return a local output path or reject unstaged remote targets."""
    return local_path_or_reject_remote(
        os.fspath(out_path),
        remote_error=f"Remote outputs must be staged before {format_name} sink writing",
    )


def _cleanup_failed_output(path: str | None) -> None:
    """Remove a direct native output file after a failed write."""
    if path is None:
        return
    with suppress(OSError):
        os.unlink(path)


def _call_native_writer(write: Any, *args: Any, output_path: str | None = None) -> Any:
    """Call a native file writer and preserve public metadata-collision errors."""
    try:
        return write(*args)
    except RuntimeError as exc:
        _cleanup_failed_output(output_path)
        message = str(exc)
        if "generated metadata column" in message:
            raise ValueError(message) from exc
        raise
    except Exception:
        _cleanup_failed_output(output_path)
        raise


def _is_native_parquet_unsupported(exc: RuntimeError) -> bool:
    """Return whether native Parquet declined a schema that should fall back."""
    message = str(exc)
    return any(
        marker in message
        for marker in (
            "native Parquet writer: unsupported Arrow",
            "native Parquet writer: unsupported dictionary",
            "native Parquet writer: unsupported list",
            "native Parquet writer: unsupported map",
            "native Parquet writer: unsupported root schema",
            "native Parquet writer: unsupported column value kind",
            "native Parquet writer: gzip compression requested but zlib is not available",
            "native Parquet writer: gzip compression is the default but zlib is not available",
        )
    )


def try_write_jsonl_direct_native(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: tuple[str, ...],
) -> Any:
    """Write JSONL by composing native metadata injection and native output."""
    write = JSONL_STREAM_WRITE_WITH_METADATA.get()
    if write is None:
        return False
    metadata_args = native_metadata_args_or_none(
        stream,
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
    )
    if metadata_args is None:
        return False
    pa = ensure_pyarrow(feature=feature)
    if not _schema_supports_native_jsonl(stream.schema, pa=pa):
        return False
    output_path = _local_output_path(out_path, format_name="JSONL")
    stats = _call_native_writer(write, stream, output_path, *metadata_args, output_path=output_path)
    mark_metadata_route("native")
    mark_jsonl_stream_route("native")
    return stats or True


def try_write_csv_direct_native(
    stream: Any,
    out_path: Any,
    *,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: tuple[str, ...],
) -> Any:
    """Write CSV by composing native metadata injection and native output."""
    write = CSV_STREAM_WRITE_WITH_METADATA.get()
    if write is None:
        return False
    metadata_args = native_metadata_args_or_none(
        stream,
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
    )
    if metadata_args is None:
        return False
    if not _native_csv_schema_supported(stream.schema):
        return False
    output_path = _local_output_path(out_path, format_name="CSV")
    stats = _call_native_writer(write, stream, output_path, *metadata_args, output_path=output_path)
    mark_metadata_route("native")
    mark_csv_stream_route("native")
    return stats or True


def try_write_parquet_direct_native(
    stream: Any,
    out_path: Any,
    *,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: tuple[str, ...],
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> bool:
    """Write Parquet directly through native output when the native writer exists."""
    metadata_stream = stream if hasattr(stream, "schema") else None
    metadata_args = native_metadata_args_or_none(
        metadata_stream,
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
    )
    if metadata_args is not None:
        write_with_metadata = PARQUET_STREAM_WRITE_WITH_METADATA.get()
        if write_with_metadata is None:
            return False
        output_path = _local_output_path(out_path, format_name="Parquet")
        try:
            with native_parquet_compression_environment(
                parquet_compression=parquet_compression,
                parquet_gzip_level=parquet_gzip_level,
            ):
                _call_native_writer(
                    write_with_metadata,
                    stream,
                    output_path,
                    *metadata_args,
                    output_path=output_path,
                )
        except RuntimeError as exc:
            if _is_native_parquet_unsupported(exc):
                return False
            raise
        mark_metadata_route("native")
        return True
    if has_metadata_columns(
        first_row_columns, all_row_columns, row_span_columns, timestamp_columns
    ):
        return False
    write = PARQUET_STREAM_WRITE.get()
    if write is None:
        return False
    output_path = _local_output_path(out_path, format_name="Parquet")
    try:
        with native_parquet_compression_environment(
            parquet_compression=parquet_compression,
            parquet_gzip_level=parquet_gzip_level,
        ):
            _call_native_writer(write, stream, output_path, output_path=output_path)
    except RuntimeError as exc:
        if _is_native_parquet_unsupported(exc):
            return False
        raise
    mark_metadata_route("none")
    return True


def try_write_jsonl_raw_direct_native(
    raw: Any,
    out_path: Any,
    *,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: tuple[str, ...],
) -> Any:
    """Write a raw native stream directly to JSONL when the native path applies."""
    metadata_args = native_metadata_args_or_none(
        None,
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
    )
    if metadata_args is not None:
        write_with_metadata = JSONL_STREAM_WRITE_WITH_METADATA.get()
        if write_with_metadata is None:
            return False
        output_path = _local_output_path(out_path, format_name="JSONL")
        stats = _call_native_writer(
            write_with_metadata,
            raw,
            output_path,
            *metadata_args,
            output_path=output_path,
        )
        mark_metadata_route("native")
        mark_jsonl_stream_route("native")
        return stats or True
    if has_metadata_columns(
        first_row_columns, all_row_columns, row_span_columns, timestamp_columns
    ):
        return False
    write = JSONL_STREAM_WRITE.get()
    if write is None:
        return False
    output_path = _local_output_path(out_path, format_name="JSONL")
    stats = _call_native_writer(write, raw, output_path, output_path=output_path)
    mark_metadata_route("none")
    mark_jsonl_stream_route("native")
    return stats or True


def try_write_csv_raw_direct_native(
    raw: Any,
    out_path: Any,
    *,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: tuple[str, ...],
) -> Any:
    """Write a raw native stream directly to CSV when the native path applies."""
    metadata_args = native_metadata_args_or_none(
        None,
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
    )
    if metadata_args is not None:
        write_with_metadata = CSV_STREAM_WRITE_WITH_METADATA.get()
        if write_with_metadata is None:
            return False
        output_path = _local_output_path(out_path, format_name="CSV")
        stats = _call_native_writer(
            write_with_metadata,
            raw,
            output_path,
            *metadata_args,
            output_path=output_path,
        )
        mark_metadata_route("native")
        mark_csv_stream_route("native")
        return stats or True
    if has_metadata_columns(
        first_row_columns, all_row_columns, row_span_columns, timestamp_columns
    ):
        return False
    write = CSV_STREAM_WRITE.get()
    if write is None:
        return False
    output_path = _local_output_path(out_path, format_name="CSV")
    stats = _call_native_writer(write, raw, output_path, output_path=output_path)
    mark_metadata_route("none")
    mark_csv_stream_route("native")
    return stats or True


def try_write_parquet_raw_direct_native(
    raw: Any,
    out_path: Any,
    *,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: tuple[str, ...],
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> bool:
    """Write a raw native stream directly to Parquet when native output exists."""
    return try_write_parquet_direct_native(
        raw,
        out_path,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
    )
