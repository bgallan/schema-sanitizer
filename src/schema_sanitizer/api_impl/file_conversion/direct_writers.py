"""Direct native file-output implementations and error translation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ...adapters.parquet.compression import native_parquet_writer_options
from ...adapters.pyarrow.csv_sink import _native_csv_schema_supported
from ...adapters.pyarrow.file_metadata import (
    has_metadata_columns,
    native_metadata_args_or_none,
)
from ...adapters.pyarrow.jsonl_sink import _schema_supports_native_jsonl
from ...core_impl.atomic_output import atomic_local_output
from ...core_impl.dependencies import ensure_pyarrow
from ...core_impl.generated_metadata import TimestampColumns
from ...core_impl.native_options import ThreadingMode, coerce_enum_member
from ...core_impl.native_symbols import (
    CSV_STREAM_WRITE,
    CSV_STREAM_WRITE_WITH_METADATA,
    JSONL_STREAM_WRITE,
    JSONL_STREAM_WRITE_WITH_METADATA,
    PARQUET_STREAM_WRITE,
    PARQUET_STREAM_WRITE_WITH_METADATA,
)
from ...core_impl.uris import local_output_path_or_reject_remote

_NATIVE_PARQUET_UNSUPPORTED_MARKERS = (
    "native Parquet writer: unsupported Arrow",
    "native Parquet writer: unsupported dictionary",
    "native Parquet writer: unsupported list",
    "native Parquet writer: unsupported map",
    "native Parquet writer: unsupported root schema",
    "native Parquet writer: gzip compression requested but zlib is not available",
    "native Parquet writer: gzip compression is the default but zlib is not available",
)


@dataclass(frozen=True, slots=True)
class FileWriteOutcome:
    """Stats and route selected for one completed file write."""

    stats: Any
    route: str
    metadata_route: str


def _call_native_writer(
    write: Any,
    *args: Any,
    output_path: str,
    output_arg_index: int = 1,
) -> Any:
    """Call a native writer through an atomic sibling staging file."""
    staged_args = list(args)
    if not 0 <= output_arg_index < len(staged_args):
        raise IndexError("native writer output argument index is out of range")
    try:
        with atomic_local_output(output_path) as staged_path:
            staged_args[output_arg_index] = staged_path
            return write(*staged_args)
    except RuntimeError as exc:
        message = str(exc)
        if "generated metadata column" in message:
            raise ValueError(message) from exc
        raise


def _native_threading_mode_value(threading_mode: Any) -> int:
    """Return the validated native integer for one output threading mode."""
    member = coerce_enum_member(
        ThreadingMode,
        threading_mode,
        label="option 'threading_mode'",
    )
    return int(member.value)


def _is_native_parquet_unsupported(exc: RuntimeError) -> bool:
    """Return whether native Parquet declined a schema that should fall back."""
    message = str(exc)
    return any(marker in message for marker in _NATIVE_PARQUET_UNSUPPORTED_MARKERS)


def _try_write_record_direct_native(
    source: Any,
    out_path: Any,
    *,
    raw: bool,
    sink_name: str,
    write: Any,
    write_with_metadata: Any,
    schema_supported: Callable[[Any], bool] | None,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: TimestampColumns,
    memory_limit_bytes: int | None,
    threading_mode: str,
) -> FileWriteOutcome | None:
    """Run the shared native-first CSV/JSONL file-output flow."""
    metadata_args = native_metadata_args_or_none(
        None if raw else source,
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
    )
    if metadata_args is None:
        if not raw or has_metadata_columns(
            first_row_columns, all_row_columns, row_span_columns, timestamp_columns
        ):
            return None
    elif not raw and (schema_supported is None or not schema_supported(source.schema)):
        return None
    output_path = local_output_path_or_reject_remote(out_path, sink_name=sink_name)
    writer = write_with_metadata if metadata_args is not None else write
    metadata = () if metadata_args is None else metadata_args
    stats = _call_native_writer(
        writer,
        source,
        output_path,
        *metadata,
        -1 if memory_limit_bytes is None else memory_limit_bytes,
        _native_threading_mode_value(threading_mode),
        output_path=output_path,
    )
    return FileWriteOutcome(
        stats or True,
        "native_direct",
        "native" if metadata_args is not None else "none",
    )


def try_write_csv_direct_native(
    stream: Any,
    out_path: Any,
    *,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: TimestampColumns,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> FileWriteOutcome | None:
    """Write CSV by composing native metadata injection and native output."""
    return _try_write_record_direct_native(
        stream,
        out_path,
        raw=False,
        sink_name="CSV",
        write=CSV_STREAM_WRITE,
        write_with_metadata=CSV_STREAM_WRITE_WITH_METADATA,
        schema_supported=_native_csv_schema_supported,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )


def try_write_csv_raw_direct_native(
    raw: Any,
    out_path: Any,
    *,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: TimestampColumns,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> FileWriteOutcome | None:
    """Write a raw native stream directly to CSV when supported."""
    return _try_write_record_direct_native(
        raw,
        out_path,
        raw=True,
        sink_name="CSV",
        write=CSV_STREAM_WRITE,
        write_with_metadata=CSV_STREAM_WRITE_WITH_METADATA,
        schema_supported=None,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )


def try_write_jsonl_direct_native(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: TimestampColumns,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> FileWriteOutcome | None:
    """Write JSONL by composing native metadata injection and native output."""
    return _try_write_record_direct_native(
        stream,
        out_path,
        raw=False,
        sink_name="JSONL",
        write=JSONL_STREAM_WRITE,
        write_with_metadata=JSONL_STREAM_WRITE_WITH_METADATA,
        schema_supported=lambda schema: _schema_supports_native_jsonl(
            schema, pa=ensure_pyarrow(feature=feature)
        ),
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )


def try_write_jsonl_raw_direct_native(
    raw: Any,
    out_path: Any,
    *,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: TimestampColumns,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> FileWriteOutcome | None:
    """Write a raw native stream directly to JSONL when supported."""
    return _try_write_record_direct_native(
        raw,
        out_path,
        raw=True,
        sink_name="JSONL",
        write=JSONL_STREAM_WRITE,
        write_with_metadata=JSONL_STREAM_WRITE_WITH_METADATA,
        schema_supported=None,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )


def try_write_parquet_direct_native(
    stream: Any,
    out_path: Any,
    *,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: TimestampColumns,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> FileWriteOutcome | None:
    """Write Parquet directly through native output when supported."""
    native_threading_mode = _native_threading_mode_value(threading_mode)
    metadata_stream = stream if hasattr(stream, "schema") else None
    metadata_args = native_metadata_args_or_none(
        metadata_stream,
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
    )
    if metadata_args is None and has_metadata_columns(
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
    ):
        return None

    output_path = local_output_path_or_reject_remote(out_path, sink_name="Parquet")
    write = (
        PARQUET_STREAM_WRITE_WITH_METADATA if metadata_args is not None else PARQUET_STREAM_WRITE
    )
    args = (
        (stream, output_path, *metadata_args)
        if metadata_args is not None
        else (stream, output_path)
    )
    compression, gzip_level = native_parquet_writer_options(
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
    )
    native_memory_limit = -1 if memory_limit_bytes is None else memory_limit_bytes
    try:
        _call_native_writer(
            write,
            *args,
            compression,
            gzip_level,
            native_memory_limit,
            native_threading_mode,
            output_path=output_path,
        )
    except RuntimeError as exc:
        if _is_native_parquet_unsupported(exc):
            return None
        raise
    return FileWriteOutcome(
        True,
        "native_direct",
        "native" if metadata_args is not None else "none",
    )


def try_write_parquet_raw_direct_native(
    raw: Any,
    out_path: Any,
    *,
    first_row_columns: dict[str, Any] | None,
    all_row_columns: dict[str, Any] | None,
    row_span_columns: dict[str, list[tuple[int, str | None]]] | None,
    timestamp_columns: TimestampColumns,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> FileWriteOutcome | None:
    """Write a raw native stream directly to Parquet."""
    _native_threading_mode_value(threading_mode)
    return try_write_parquet_direct_native(
        raw,
        out_path,
        first_row_columns=first_row_columns,
        all_row_columns=all_row_columns,
        row_span_columns=row_span_columns,
        timestamp_columns=timestamp_columns,
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )
