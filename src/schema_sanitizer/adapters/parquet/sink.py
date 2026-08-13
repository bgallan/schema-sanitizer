"""PyArrow Parquet sink for record-batch streams."""

from __future__ import annotations

import os
from contextlib import AbstractContextManager, nullcontext
from typing import Any

from ...core_impl.atomic_output import atomic_local_output
from ...core_impl.dependencies import ensure_optional_dependency, ensure_pyarrow
from ...core_impl.execution_policy import normalize_threading_mode
from ...core_impl.native_symbols import COALESCING_STREAM_WRAP
from ...core_impl.process_resources import open_governed_file
from ...core_impl.uris import local_output_path_or_reject_remote
from ..pyarrow.file_metadata import prepare_file_output_metadata_stream
from ..pyarrow.metadata_native import CapsuleArrowStream
from ..pyarrow.metadata_specs import (
    AllRowColumns,
    FirstRowColumns,
    RowSpanColumns,
    TimestampColumns,
)
from .compression import pyarrow_parquet_writer_options

_LAST_PARQUET_COALESCE_ROUTE = "none"
_COALESCING_UNAVAILABLE_MESSAGES = (
    "requires an Arrow C stream",
    "requires a schema supported by the native C++ coalescing stream wrapper",
)


def last_parquet_coalesce_route() -> str:
    """Return how the most recent Parquet stream write coalesced batches."""
    return _LAST_PARQUET_COALESCE_ROUTE


def _native_coalesced_reader(
    batches: Any,
    *,
    pa: Any,
    memory_limit_bytes: int | None,
) -> Any:
    """Return a native coalescing reader or fail before Python buffering."""
    if not hasattr(batches, "__arrow_c_stream__"):
        raise RuntimeError("Parquet output coalescing requires an Arrow C stream.")
    capsule = COALESCING_STREAM_WRAP(
        batches, -1 if memory_limit_bytes is None else memory_limit_bytes
    )
    if capsule is None:
        raise RuntimeError(
            "Parquet output requires a schema supported by the native C++ coalescing "
            "stream wrapper."
        )
    return pa.RecordBatchReader.from_stream(CapsuleArrowStream(capsule))


def _write_coalesced_batches(
    writer: Any,
    batches: Any,
    *,
    schema: Any,
    pa: Any,
    memory_limit_bytes: int | None = None,
) -> None:
    """Write small incoming batches as bounded native-coalesced Parquet row groups."""
    global _LAST_PARQUET_COALESCE_ROUTE
    del schema
    _LAST_PARQUET_COALESCE_ROUTE = "none"
    try:
        native_reader = _native_coalesced_reader(
            batches,
            pa=pa,
            memory_limit_bytes=memory_limit_bytes,
        )
    except RuntimeError as exc:
        if not any(message in str(exc) for message in _COALESCING_UNAVAILABLE_MESSAGES):
            raise
        _LAST_PARQUET_COALESCE_ROUTE = "pyarrow"
        for batch in batches:
            writer.write_batch(batch)
        return
    _LAST_PARQUET_COALESCE_ROUTE = "native"
    for batch in native_reader:
        writer.write_batch(batch)


def write_parquet_stream(
    stream: Any,
    out_path: Any,
    *,
    feature: str,
    first_row_columns: FirstRowColumns = None,
    all_row_columns: AllRowColumns = None,
    row_span_columns: RowSpanColumns = None,
    timestamp_columns: TimestampColumns = None,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> None:
    """Write an Arrow batch stream to Parquet."""
    normalize_threading_mode(threading_mode)
    pa = ensure_pyarrow(feature=feature)
    pq = ensure_optional_dependency(
        "pyarrow.parquet", extra="pyarrow", feature=feature, dependency_name="pyarrow"
    )
    metadata = prepare_file_output_metadata_stream(
        stream,
        first_row_columns,
        all_row_columns,
        row_span_columns,
        timestamp_columns,
        pa=pa,
    )

    output_context: AbstractContextManager[Any]
    if isinstance(out_path, (str, os.PathLike)):
        output_path = local_output_path_or_reject_remote(out_path, sink_name="Parquet")
        output_context = atomic_local_output(output_path)
    else:
        output_context = nullcontext(out_path)

    try:
        with output_context as target:
            governed_target = None
            sink_target = target
            if isinstance(target, (str, os.PathLike)):
                governed_target = open_governed_file(target, "wb")
                sink_target = governed_target
            writer: Any | None = None
            try:
                writer = pq.ParquetWriter(
                    sink_target,
                    metadata.schema,
                    **pyarrow_parquet_writer_options(
                        parquet_compression=parquet_compression,
                        parquet_gzip_level=parquet_gzip_level,
                    ),
                )
                _write_coalesced_batches(
                    writer,
                    metadata.batches,
                    schema=metadata.schema,
                    pa=pa,
                    memory_limit_bytes=memory_limit_bytes,
                )
            finally:
                try:
                    if writer is not None:
                        writer.close()
                finally:
                    if governed_target is not None:
                        governed_target.close()
    finally:
        metadata.close()
