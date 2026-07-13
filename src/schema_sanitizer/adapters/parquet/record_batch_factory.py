"""Reusable Parquet Arrow C Stream factory and PyArrow fallback."""

from __future__ import annotations

import logging
import os
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from ...core_impl.dependencies import ensure_optional_dependency, ensure_pyarrow
from ...core_impl.native_symbols import PARQUET_STREAM_READ
from ...core_impl.uris import local_path_or_reject_remote
from ..pyarrow.streams import record_batch_reader_from_iterable
from .memory import DEFAULT_PARQUET_BATCH_ROWS, _native_parquet_batch_size_contract_issue
from .native_reader import (
    native_nested_contract_blockers,
    native_writer_detected,
    native_writer_diagnostics,
    try_native_parquet_stream,
)
from .status import native_parquet_footer_info
from .telemetry import (
    record_parquet_fallback_attempt,
    record_parquet_fallback_failure,
    record_parquet_fallback_success,
)

_LOGGER = logging.getLogger(__name__)


def local_parquet_path_or_none(data: Any, *, source: str, feature: str) -> str | None:
    """Return a local filesystem path when a Parquet source names one."""
    if source == "path":
        return os.fspath(data)
    if source == "uri":
        return local_path_or_reject_remote(
            data,
            remote_error=f"{feature} URI inputs must be staged before Parquet decoding",
        )
    return None


def _parquet_buffer(data: bytes | bytearray | memoryview) -> bytes | bytearray | memoryview:
    """Return a contiguous byte-oriented buffer, copying only when required."""
    if not isinstance(data, memoryview):
        return data
    if not data.contiguous:
        return data.tobytes()
    if data.itemsize != 1 or data.format != "B":
        return data.cast("B")
    return data


def open_parquet_source(data: Any, *, source: str, feature: str, pa: Any) -> tuple[Any, Any | None]:
    """Open a Parquet source and return ``(source, owned_file)``."""
    local_path = local_parquet_path_or_none(data, source=source, feature=feature)
    if local_path is not None:
        return local_path, None
    if source == "uri":
        raise ValueError(f"{feature} URI inputs must be staged before Parquet decoding")
    if source == "text":
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"{feature} expects bytes for source='text', got {type(data)!r}")
        opened_file = pa.BufferReader(_parquet_buffer(data))
        return opened_file, opened_file
    if source == "stream":
        seek = getattr(data, "seek", None)
        if not callable(seek):
            raise TypeError("Parquet stream inputs require seek(0)")
        seek(0)
        return data, None
    raise TypeError(f"Unsupported Parquet source: {source!r}")


def local_stream_path(data: Any) -> str | None:
    """Return a local path for a file-like object backed by a named file."""
    name = getattr(data, "name", None)
    if not isinstance(name, (str, os.PathLike)):
        return None
    try:
        path = os.fspath(name)
    except TypeError:
        return None
    if not path or path.startswith("<"):
        return None
    try:
        return path if os.path.isfile(path) else None
    except OSError:
        return None


def stage_parquet_buffer(data: bytes | bytearray | memoryview) -> str:
    """Stage buffer-backed Parquet bytes without materializing another copy."""
    handle = tempfile.NamedTemporaryFile(
        prefix="schema-sanitizer-parquet-",
        suffix=".parquet",
        delete=False,
    )
    path = handle.name
    try:
        with handle:
            handle.write(_parquet_buffer(data))
    except Exception:
        with suppress(Exception):
            handle.close()
        with suppress(OSError):
            Path(path).unlink()
        raise
    return path


def remove_staged_parquet(path: str | None) -> bool:
    """Remove a staged Parquet file and report whether it is gone."""
    if not path:
        return True
    try:
        Path(path).unlink()
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return True


@dataclass(frozen=True, slots=True)
class PreparedParquetFactorySource:
    """Resolved native source details owned by a record-batch factory."""

    local_path: Any | None
    staged_path: str | None
    native_source_kind: str


def prepare_parquet_factory_source(
    data: Any,
    *,
    source: str,
    feature: str,
    logger: Any,
) -> PreparedParquetFactorySource:
    """Resolve a local path or stage an in-memory Parquet buffer."""
    native_source_kind = source
    local_path = local_parquet_path_or_none(data, source=source, feature=feature)
    staged_path: str | None = None

    if local_path is None and source == "stream":
        local_path = local_stream_path(data)
        if local_path is not None:
            native_source_kind = "stream_path"

    if local_path is None and source == "text" and isinstance(data, (bytes, bytearray, memoryview)):
        try:
            staged_path = stage_parquet_buffer(data)
        except OSError as exc:
            logger.debug(
                "Native Parquet buffer staging failed; retrying via PyArrow buffer reader: %s",
                exc,
            )
        else:
            local_path = staged_path
            native_source_kind = "staged_text"

    if local_path is not None and source == "path":
        native_source_kind = "path"
    elif local_path is not None and source == "uri":
        native_source_kind = "uri_path"

    return PreparedParquetFactorySource(
        local_path=local_path,
        staged_path=staged_path,
        native_source_kind=native_source_kind,
    )


def close_factory(factory: Any) -> None:
    """Close pending Parquet resources and remove staged native files."""
    pending = (factory._pending_parquet_file, factory._pending_opened_file)
    factory._pending_parquet_file = None
    factory._pending_opened_file = None
    for obj in pending:
        if obj is not None:
            with suppress(Exception):
                obj.close()
    if remove_staged_parquet(factory._staged_path):
        factory._staged_path = None


def open_parquet_file(factory: Any) -> tuple[Any, Any | None]:
    """Open a ParquetFile and return it with any owned file handle."""
    source, opened_file = open_parquet_source(
        factory._data,
        source=factory._source,
        feature=factory._feature,
        pa=factory._pa,
    )
    return factory._pq.ParquetFile(source), opened_file


def project_schema(factory: Any, schema: Any) -> Any:
    """Return a top-level projected schema when columns were requested."""
    if factory._columns is None:
        return schema
    fields = []
    for column in factory._columns:
        index = schema.get_field_index(column)
        if index < 0:
            raise KeyError(f"Parquet projection column not found: {column!r}")
        fields.append(schema.field(index))
    return factory._pa.schema(fields, metadata=schema.metadata)


def initialize_factory_schema(factory: Any, *, logger: Any) -> None:
    """Read the source schema using dataset or ParquetFile fallback setup."""
    if factory._local_path is not None:
        try:
            factory._ds = ensure_optional_dependency(
                "pyarrow.dataset",
                extra="pyarrow",
                feature=factory._feature,
                dependency_name="pyarrow",
            )
            factory._dataset = factory._ds.dataset(factory._local_path, format="parquet")
        except Exception as exc:
            factory._dataset = None
            factory._dataset_error = exc
            logger.debug(
                "PyArrow dataset construction failed for Parquet source; "
                "native reader or ParquetFile fallback may still recover: %s",
                exc,
            )
            factory._pending_parquet_file, factory._pending_opened_file = open_parquet_file(factory)
            schema = factory._pending_parquet_file.schema_arrow
        else:
            schema = factory._dataset.schema
    else:
        factory._dataset = None
        factory._pending_parquet_file, factory._pending_opened_file = open_parquet_file(factory)
        schema = factory._pending_parquet_file.schema_arrow
    factory.schema = project_schema(factory, schema)


def pyarrow_fallback_arrow_stream(
    factory: Any,
    *,
    record_batch_reader_from_iterable: Callable[..., Any],
    logger: Any,
) -> Any:
    """Return an Arrow C Stream capsule using PyArrow fallback routes."""
    if factory._filters is not None and factory._dataset is None:
        fallback_route = "pyarrow_dataset_scanner"
        record_parquet_fallback_attempt(fallback_route)
        exc = factory._dataset_error or RuntimeError(
            "Parquet filters require the PyArrow dataset fallback route"
        )
        record_parquet_fallback_failure(fallback_route, exc)
        raise exc

    if factory._dataset is not None:
        fallback_route = "pyarrow_dataset_scanner"
        record_parquet_fallback_attempt(fallback_route)
        try:
            scanner = factory._dataset.scanner(
                columns=None if factory._columns is None else list(factory._columns),
                filter=factory._filters,
                batch_size=factory._batch_size,
                use_threads=factory._use_threads,
            )
            reader = scanner.to_reader()
            stream = reader.__arrow_c_stream__()
        except Exception as exc:
            record_parquet_fallback_failure(fallback_route, exc)
            if factory._filters is not None:
                raise
            logger.debug(
                "PyArrow dataset fallback failed; trying ParquetFile.iter_batches: %s",
                exc,
            )
        else:
            factory._keepalive = (scanner, reader)
            record_parquet_fallback_success(fallback_route)
            return stream

    dataset_error = getattr(factory, "_dataset_error", None)
    if dataset_error is not None:
        dataset_route = "pyarrow_dataset_scanner"
        record_parquet_fallback_attempt(dataset_route)
        record_parquet_fallback_failure(dataset_route, dataset_error)

    fallback_route = "pyarrow_parquetfile_iter_batches"
    record_parquet_fallback_attempt(fallback_route)
    try:
        if factory._pending_parquet_file is not None:
            parquet_file = factory._pending_parquet_file
            opened_file = factory._pending_opened_file
            factory._pending_parquet_file = None
            factory._pending_opened_file = None
        else:
            parquet_file, opened_file = open_parquet_file(factory)
        batches = parquet_file.iter_batches(
            batch_size=factory._batch_size,
            columns=None if factory._columns is None else list(factory._columns),
            use_threads=factory._use_threads,
        )
        reader = record_batch_reader_from_iterable(factory._pa, factory.schema, batches)
        stream = reader.__arrow_c_stream__()
    except Exception as exc:
        record_parquet_fallback_failure(fallback_route, exc)
        raise
    record_parquet_fallback_success(fallback_route)
    factory._keepalive = tuple(x for x in (opened_file, parquet_file, reader) if x is not None)
    return stream


class ParquetRecordBatchStreamFactory:
    """Reusable PyArrow RecordBatchReader factory for direct native ingestion."""

    def __init__(
        self,
        data: Any,
        *,
        source: str,
        feature: str,
        batch_size: int = DEFAULT_PARQUET_BATCH_ROWS,
        use_threads: bool = False,
        columns: list[str] | tuple[str, ...] | None = None,
        filters: Any | None = None,
    ) -> None:
        """Store the Parquet source and read its schema once."""
        self._data = data
        self._source = source
        self._feature = feature
        self._batch_size = batch_size
        self._use_threads = use_threads
        self._columns = None if columns is None else tuple(columns)
        self._filters = filters

        prepared = prepare_parquet_factory_source(
            data, source=source, feature=feature, logger=_LOGGER
        )
        self._local_path = prepared.local_path
        self._staged_path = prepared.staged_path
        self._native_source_kind = prepared.native_source_kind
        if self._filters is not None and self._local_path is None:
            raise ValueError("Parquet filters require a path-backed source")

        self.sink = "stream"
        self.diagnostics = None
        self.native_registry_state = None
        self._pa = ensure_pyarrow(feature=feature)
        self._pq = ensure_optional_dependency(
            "pyarrow.parquet",
            extra="pyarrow",
            feature=feature,
            dependency_name="pyarrow",
        )
        self._ds = None
        self._dataset_error: BaseException | None = None
        self._keepalive: tuple[Any, ...] = ()
        self._pending_parquet_file: Any | None = None
        self._pending_opened_file: Any | None = None
        initialize_factory_schema(self, logger=_LOGGER)

    def _native_batch_size_blocker(self, info: dict[str, Any]) -> str | None:
        """Return a native-reader blocker for the configured batch size."""
        return _native_parquet_batch_size_contract_issue(info, self._batch_size)

    @staticmethod
    def _native_writer_detected(info: dict[str, Any]) -> bool:
        """Return whether footer metadata identifies the native writer."""
        return native_writer_detected(info)

    def _native_writer_diagnostics(self, info: dict[str, Any]) -> dict[str, Any]:
        """Return native-writer diagnostics derived from footer metadata."""
        return native_writer_diagnostics(info)

    @staticmethod
    def _native_nested_contract_blockers(info: dict[str, Any]) -> list[str]:
        """Return blockers for unsafe nested native-reader layouts."""
        return native_nested_contract_blockers(info)

    def _try_native_stream(self) -> Any | None:
        """Attempt to open the source through the native Parquet reader."""
        return try_native_parquet_stream(
            self,
            native_stream_read_hook=PARQUET_STREAM_READ,
            footer_info=native_parquet_footer_info,
            logger=_LOGGER,
        )

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Return a fresh Arrow C Stream capsule for native ingestion."""
        del requested_schema
        native_capsule = self._try_native_stream()
        if native_capsule is not None:
            return native_capsule
        return pyarrow_fallback_arrow_stream(
            self,
            record_batch_reader_from_iterable=record_batch_reader_from_iterable,
            logger=_LOGGER,
        )

    def close(self) -> None:
        """Close pending Parquet resources and remove staged native files."""
        close_factory(self)

    def __del__(self) -> None:
        """Best-effort cleanup for pending files and staged buffers."""
        with suppress(Exception):
            self.close()


def open_parquet_record_batch_stream_factory(
    data: Any,
    *,
    source: str,
    feature: str,
    batch_size: int = DEFAULT_PARQUET_BATCH_ROWS,
    use_threads: bool = False,
    columns: list[str] | tuple[str, ...] | None = None,
    filters: Any | None = None,
) -> ParquetRecordBatchStreamFactory:
    """Open Parquet input as a reusable Arrow C Stream factory."""
    return ParquetRecordBatchStreamFactory(
        data,
        source=source,
        feature=feature,
        batch_size=batch_size,
        use_threads=use_threads,
        columns=columns,
        filters=filters,
    )
