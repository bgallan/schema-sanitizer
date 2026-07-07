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
    local_parquet_path_or_none,
    open_parquet_source,
)
from .pyarrow_schema_support import SchemaSupportCache

_DIRECT_SCHEMA_SUPPORT_CACHE = SchemaSupportCache()
_LAST_PARQUET_STREAM_FACTORY_ROUTE = "none"
_LAST_PARQUET_NATIVE_READER_DIAGNOSTICS: dict[str, Any] = {
    "attempted": False,
    "ready": False,
    "reason": "none",
    "blockers": [],
}
_LOGGER = logging.getLogger(__name__)


def last_parquet_stream_factory_route() -> str:
    """Return the route used by the most recent Parquet stream factory."""
    return _LAST_PARQUET_STREAM_FACTORY_ROUTE


def last_parquet_native_reader_diagnostics() -> dict[str, Any]:
    """Return diagnostics for the most recent native Parquet reader attempt."""
    diagnostics = dict(_LAST_PARQUET_NATIVE_READER_DIAGNOSTICS)
    diagnostics["blockers"] = list(diagnostics.get("blockers") or [])
    return diagnostics


def _set_parquet_stream_factory_route(route: str) -> None:
    """Record the route used by the most recent Parquet stream factory."""
    global _LAST_PARQUET_STREAM_FACTORY_ROUTE
    _LAST_PARQUET_STREAM_FACTORY_ROUTE = route


def _set_parquet_native_reader_diagnostics(**diagnostics: Any) -> None:
    """Record diagnostics for the most recent native reader attempt."""
    global _LAST_PARQUET_NATIVE_READER_DIAGNOSTICS
    normalized = {
        "attempted": False,
        "ready": False,
        "reason": "none",
        "blockers": [],
    }
    normalized.update(diagnostics)
    normalized["blockers"] = list(normalized.get("blockers") or [])
    _LAST_PARQUET_NATIVE_READER_DIAGNOSTICS = normalized


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
        self._local_path = local_parquet_path_or_none(data, source=source, feature=feature)
        if self._filters is not None and self._local_path is None:
            raise ValueError("Parquet filters require a path-backed source")
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
        if self._local_path is not None:
            self._ds = ensure_optional_dependency(
                "pyarrow.dataset",
                extra="pyarrow",
                feature=feature,
                dependency_name="pyarrow",
            )
            self._dataset = self._ds.dataset(self._local_path, format="parquet")
            self.schema = self._project_schema(self._dataset.schema)
        else:
            self._dataset = None
            self._pending_parquet_file, self._pending_opened_file = self._open_parquet_file()
            self.schema = self._project_schema(self._pending_parquet_file.schema_arrow)

    def _open_parquet_file(self) -> tuple[Any, Any | None]:
        """Open a ParquetFile and return it with any owned file handle."""
        src, opened_file = open_parquet_source(
            self._data,
            source=self._source,
            feature=self._feature,
            pa=self._pa,
        )
        return self._pq.ParquetFile(src), opened_file

    def _project_schema(self, schema: Any) -> Any:
        """Return a top-level projected schema when columns were requested."""
        if self._columns is None:
            return schema
        fields = []
        for column in self._columns:
            index = schema.get_field_index(column)
            if index < 0:
                raise KeyError(f"Parquet projection column not found: {column!r}")
            fields.append(schema.field(index))
        return self._pa.schema(fields, metadata=schema.metadata)

    def _native_batch_size_blocker(self, info: dict[str, Any]) -> str | None:
        """Return a blocker when native row-group batches exceed requested size."""
        if self._batch_size <= 0:
            return None
        max_row_group_rows = 0
        for row_group in info.get("row_groups") or []:
            try:
                row_group_rows = int(row_group.get("num_rows") or 0)
            except (TypeError, ValueError):
                continue
            max_row_group_rows = max(max_row_group_rows, row_group_rows)
        if max_row_group_rows <= self._batch_size:
            return None
        return (
            f"native reader row group has {max_row_group_rows} rows but requested "
            f"batch_size is {self._batch_size}"
        )

    def _try_native_stream(self) -> Any | None:
        """Return a native Parquet Arrow C stream capsule when supported."""
        if self._filters is not None:
            _set_parquet_native_reader_diagnostics(
                attempted=False,
                ready=False,
                reason="filter_requires_dataset_scanner",
            )
            return None
        if self._local_path is None:
            _set_parquet_native_reader_diagnostics(
                attempted=False,
                ready=False,
                reason="source_not_path",
            )
            return None
        native_read = PARQUET_STREAM_READ.get()
        if native_read is None:
            _set_parquet_native_reader_diagnostics(
                attempted=False,
                ready=False,
                reason="native_function_unavailable",
            )
            return None
        try:
            info = native_parquet_footer_info(self._local_path)
        except (RuntimeError, TypeError, ValueError) as exc:
            _set_parquet_native_reader_diagnostics(
                attempted=True,
                ready=False,
                reason="footer_info_error",
                blockers=[str(exc)],
            )
            _LOGGER.debug(
                "Native Parquet reader skipped; footer info failed; retrying "
                "input with PyArrow: %s",
                exc,
            )
            return None
        if not info:
            _set_parquet_native_reader_diagnostics(
                attempted=True,
                ready=False,
                reason="footer_info_unavailable",
            )
            _LOGGER.debug("Native Parquet reader skipped: footer info unavailable")
            return None
        blockers = list(info.get("native_reader_blockers") or [])
        if info.get("native_reader_ready") == 1:
            batch_size_blocker = self._native_batch_size_blocker(info)
            if batch_size_blocker is not None:
                _set_parquet_native_reader_diagnostics(
                    attempted=True,
                    ready=False,
                    reason="not_ready",
                    blockers=[batch_size_blocker],
                    row_group_count=info.get("row_group_count"),
                    num_rows=info.get("num_rows"),
                )
                _LOGGER.debug(
                    "Native Parquet reader skipped; retrying input with PyArrow: %s",
                    batch_size_blocker,
                )
                return None
        if info.get("native_reader_ready") != 1:
            _set_parquet_native_reader_diagnostics(
                attempted=True,
                ready=False,
                reason="not_ready",
                blockers=blockers,
                row_group_count=info.get("row_group_count"),
                num_rows=info.get("num_rows"),
            )
            first_blocker = blockers[0] if blockers else "unknown blocker"
            _LOGGER.debug(
                "Native Parquet reader skipped; retrying input with PyArrow: %s",
                first_blocker,
            )
            return None
        try:
            if self._columns is None:
                capsule = native_read(self._local_path)
            else:
                capsule = native_read(self._local_path, list(self._columns))
        except RuntimeError as exc:
            _set_parquet_native_reader_diagnostics(
                attempted=True,
                ready=True,
                reason="native_error",
                blockers=[],
                error=str(exc),
                row_group_count=info.get("row_group_count"),
                num_rows=info.get("num_rows"),
            )
            _LOGGER.error(
                "Native Parquet reader failed; retrying input with PyArrow",
                exc_info=exc,
            )
            return None
        _set_parquet_native_reader_diagnostics(
            attempted=True,
            ready=True,
            reason="native_stream",
            blockers=[],
            row_group_count=info.get("row_group_count"),
            num_rows=info.get("num_rows"),
        )
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
                columns=None if self._columns is None else list(self._columns),
                filter=self._filters,
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
        if self._local_path is None:
            _set_parquet_native_reader_diagnostics(
                attempted=False,
                ready=False,
                reason="source_not_path",
            )
        batches = parquet_file.iter_batches(
            batch_size=self._batch_size,
            columns=None if self._columns is None else list(self._columns),
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


__all__ = [
    "ParquetRecordBatchStreamFactory",
    "last_parquet_native_reader_diagnostics",
    "last_parquet_stream_factory_route",
    "native_parquet_footer_info",
    "open_parquet_record_batch_stream_factory",
    "parquet_schema_supports_direct_native_ingest",
    "record_batch_reader_from_iterable",
]
