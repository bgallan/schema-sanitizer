"""Direct PyArrow Parquet-to-Arrow stream adapter."""

from __future__ import annotations

import json
import logging
from contextlib import suppress
from typing import Any

from ..core_impl.native_functions import (
    ARROW_DIRECT_SCHEMA_SUPPORTED,
    ARROW_SCHEMA_CONTRACT_PAYLOAD,
    PARQUET_FOOTER_INFO_JSON,
    PARQUET_STREAM_READ,
)
from ._optional import ensure_optional_dependency
from .pyarrow_parquet_common import (
    DEFAULT_PARQUET_BATCH_ROWS,
    ensure_pyarrow,
    open_parquet_source,
)
from .pyarrow_schema_support import SchemaSupportCache

_DIRECT_SCHEMA_SUPPORT_CACHE = SchemaSupportCache()
_LAST_PARQUET_STREAM_FACTORY_ROUTE = "none"
_LOGGER = logging.getLogger(__name__)


def last_parquet_stream_factory_route() -> str:
    """Return the route used by the most recent Parquet stream factory."""
    return _LAST_PARQUET_STREAM_FACTORY_ROUTE


def _set_parquet_stream_factory_route(route: str) -> None:
    """Record the route used by the most recent Parquet stream factory."""
    global _LAST_PARQUET_STREAM_FACTORY_ROUTE
    _LAST_PARQUET_STREAM_FACTORY_ROUTE = route


def record_batch_reader_from_iterable(pa: Any, schema: Any, batches: Any) -> Any:
    """Build a RecordBatchReader from a Python iterable fallback."""
    _set_parquet_stream_factory_route("pyarrow_parquetfile_iter_batches")
    return pa.RecordBatchReader.from_batches(schema, batches)


def native_parquet_footer_info(path: Any) -> dict[str, Any] | None:
    """Return native Parquet footer metadata for a local path when available."""
    footer_info_json = PARQUET_FOOTER_INFO_JSON.get()
    if footer_info_json is None:
        return None
    return json.loads(footer_info_json(path))


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
    ) -> None:
        """Store the Parquet source and read its schema once."""
        self._data = data
        self._source = source
        self._feature = feature
        self._batch_size = batch_size
        self._use_threads = use_threads
        self.sink = "stream"
        self.diagnostics = None
        self._pa = ensure_pyarrow(feature=feature)
        self._pq = ensure_optional_dependency(
            "pyarrow.parquet", extra="pyarrow", feature=feature, dependency_name="pyarrow"
        )
        self._ds = None
        self._keepalive: tuple[Any, ...] = ()
        self._pending_parquet_file: Any | None = None
        self._pending_opened_file: Any | None = None
        if source == "path":
            self._ds = ensure_optional_dependency(
                "pyarrow.dataset",
                extra="pyarrow",
                feature=feature,
                dependency_name="pyarrow",
            )
            self._dataset = self._ds.dataset(data, format="parquet")
            self.schema = self._dataset.schema
        else:
            self._dataset = None
            self._pending_parquet_file, self._pending_opened_file = self._open_parquet_file()
            self.schema = self._pending_parquet_file.schema_arrow

    def _open_parquet_file(self) -> tuple[Any, Any | None]:
        """Open a ParquetFile and return it with any owned file handle."""
        src, opened_file = open_parquet_source(
            self._data,
            source=self._source,
            feature=self._feature,
            pa=self._pa,
        )
        return self._pq.ParquetFile(src), opened_file

    def _try_native_stream(self) -> Any | None:
        """Return a native Parquet Arrow C stream capsule when supported."""
        if self._source != "path":
            return None
        native_read = PARQUET_STREAM_READ.get()
        if native_read is None:
            return None
        info = native_parquet_footer_info(self._data)
        if not info or info.get("native_reader_ready") != 1:
            return None
        try:
            capsule = native_read(self._data)
        except RuntimeError as exc:
            _LOGGER.error(
                "Native Parquet reader failed; retrying input with PyArrow",
                exc_info=exc,
            )
            return None
        _set_parquet_stream_factory_route("native_parquet_stream")
        self._keepalive = (capsule,)
        return capsule

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Return a fresh Arrow C Stream capsule for native ingestion."""
        del requested_schema
        if self._dataset is not None:
            native_capsule = self._try_native_stream()
            if native_capsule is not None:
                return native_capsule
            scanner = self._dataset.scanner(
                batch_size=self._batch_size,
                use_threads=self._use_threads,
            )
            reader = scanner.to_reader()
            self._keepalive = (scanner, reader)
            _set_parquet_stream_factory_route("pyarrow_dataset_scanner")
            return reader.__arrow_c_stream__()
        if self._pending_parquet_file is not None:
            parquet_file = self._pending_parquet_file
            opened_file = self._pending_opened_file
            self._pending_parquet_file = None
            self._pending_opened_file = None
        else:
            parquet_file, opened_file = self._open_parquet_file()
        batches = parquet_file.iter_batches(
            batch_size=self._batch_size,
            use_threads=self._use_threads,
        )
        reader = record_batch_reader_from_iterable(self._pa, self.schema, batches)
        self._keepalive = tuple(x for x in (opened_file, parquet_file, reader) if x is not None)
        return reader.__arrow_c_stream__()

    def close(self) -> None:
        """Close any pending Parquet resources not handed to an Arrow stream."""
        pending = (self._pending_parquet_file, self._pending_opened_file)
        self._pending_parquet_file = None
        self._pending_opened_file = None
        for obj in pending:
            if obj is not None:
                with suppress(Exception):
                    obj.close()

    def __del__(self) -> None:
        """Best-effort close any pending Parquet resources."""
        with suppress(Exception):
            self.close()


def parquet_schema_supports_direct_native_ingest(
    schema: Any,
    *,
    pa: Any,
    timestamp_precision: str,
) -> bool:
    """Return whether a Parquet schema can use the direct native Arrow path."""
    del pa
    del timestamp_precision
    cached = _DIRECT_SCHEMA_SUPPORT_CACHE.get_by_object(schema)
    if cached is not None:
        return cached
    cached = _DIRECT_SCHEMA_SUPPORT_CACHE.get_by_text(schema)
    if cached is not None:
        return _DIRECT_SCHEMA_SUPPORT_CACHE.set(schema, cached, include_text=False)

    contract_payload = ARROW_SCHEMA_CONTRACT_PAYLOAD.get()
    if contract_payload is not None:
        try:
            fingerprint = bytes(contract_payload(schema))
        except TypeError:
            fingerprint = b""
        if fingerprint:
            cached = _DIRECT_SCHEMA_SUPPORT_CACHE.get_by_fingerprint(fingerprint)
            if cached is not None:
                return _DIRECT_SCHEMA_SUPPORT_CACHE.set_fingerprint(
                    schema,
                    fingerprint,
                    cached,
                )
            _DIRECT_SCHEMA_SUPPORT_CACHE.set_fingerprint(schema, fingerprint, True)
            return _DIRECT_SCHEMA_SUPPORT_CACHE.set(schema, True, include_text=True)

    native_supported = ARROW_DIRECT_SCHEMA_SUPPORTED.get()
    if native_supported is not None:
        try:
            supported = bool(native_supported(schema))
            return _DIRECT_SCHEMA_SUPPORT_CACHE.set(schema, supported, include_text=True)
        except TypeError:
            return _DIRECT_SCHEMA_SUPPORT_CACHE.set(schema, False, include_text=True)

    return _DIRECT_SCHEMA_SUPPORT_CACHE.set(schema, False, include_text=True)


def open_parquet_record_batch_stream_factory(
    data: Any,
    *,
    source: str,
    feature: str,
    batch_size: int = DEFAULT_PARQUET_BATCH_ROWS,
    use_threads: bool = False,
) -> ParquetRecordBatchStreamFactory:
    """Open Parquet input as a reusable Arrow C Stream factory."""
    return ParquetRecordBatchStreamFactory(
        data,
        source=source,
        feature=feature,
        batch_size=batch_size,
        use_threads=use_threads,
    )


__all__ = [
    "ParquetRecordBatchStreamFactory",
    "last_parquet_stream_factory_route",
    "native_parquet_footer_info",
    "open_parquet_record_batch_stream_factory",
    "parquet_schema_supports_direct_native_ingest",
    "record_batch_reader_from_iterable",
]
