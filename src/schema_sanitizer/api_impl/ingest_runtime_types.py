"""Implements `schema_sanitizer.api_impl.ingest_runtime_types`."""

from __future__ import annotations

import json
from contextlib import suppress
from typing import Any

from ..adapters import pyarrow_streams as _pyarrow_streams
from .ingest_lifecycle import (
    _close_and_clear_attrs,
    _close_keepalive_attr,
    _close_resource_owner_attr,
    _close_suppressing_errors,
)
from .ingest_runtime_streams import (
    ArrowCStream,
    Stream,
    _ClosableContextManagerMixin,
    _DiagnosticsAccessMixin,
)


class Result(_DiagnosticsAccessMixin):
    """Result returned by format-specific reader and writer APIs."""

    _UNSET = object()

    def __init__(
        self,
        raw: Any,
        *,
        clean_data: Any = _UNSET,
        schema_registry: dict[str, Any] | None = None,
        schema_registry_json: str | None = None,
        schema_drifts: list[dict[str, Any]] | None = None,
        schema_drifts_json: str | None = None,
        native_registry_state: Any = None,
    ):
        """Wrap raw reader output and optional materialized clean data."""
        self._raw = raw
        self._clean_data_cache = clean_data
        self._table_cache: Any = self._UNSET
        self._schema_registry_cache: Any = (
            schema_registry if schema_registry is not None else self._UNSET
        )
        self.schema_registry_json = schema_registry_json
        self._schema_drifts_cache: Any = schema_drifts if schema_drifts is not None else self._UNSET
        self.schema_drifts_json = schema_drifts_json
        self.native_registry_state = native_registry_state

    @property
    def schema_registry(self) -> dict[str, Any] | None:
        """Return the parsed schema registry, parsing JSON lazily when needed."""
        if self._schema_registry_cache is self._UNSET:
            if self.schema_registry_json is None:
                return None
            self._schema_registry_cache = json.loads(self.schema_registry_json or "{}")
        return self._schema_registry_cache

    @schema_registry.setter
    def schema_registry(self, value: dict[str, Any] | None) -> None:
        """Set the parsed schema registry cache."""
        self._schema_registry_cache = value if value is not None else self._UNSET

    @property
    def schema_drifts(self) -> list[dict[str, Any]] | None:
        """Return parsed schema drifts, parsing JSON lazily when needed."""
        if self._schema_drifts_cache is self._UNSET:
            if self.schema_drifts_json is None:
                return None
            self._schema_drifts_cache = json.loads(self.schema_drifts_json or "[]")
        return self._schema_drifts_cache

    @schema_drifts.setter
    def schema_drifts(self, value: list[dict[str, Any]] | None) -> None:
        """Set the parsed schema drift cache."""
        self._schema_drifts_cache = value if value is not None else self._UNSET

    @property
    def clean_data(self):
        """Return clean data in the reader's requested output format."""
        if self._clean_data_cache is not self._UNSET:
            return self._clean_data_cache
        return self._clean_table()

    def _clean_table(self):
        """Return a :class:`pyarrow.Table` (PyArrow is required)."""
        if self._table_cache is not self._UNSET:
            return self._table_cache
        t = getattr(self._raw, "table", None)
        if t is None:
            self._table_cache = None
            return None
        if hasattr(t, "__arrow_c_stream__"):
            t = _pyarrow_streams.table_from_stream_like(t, feature="Result.clean_data")
        self._table_cache = t
        return self._table_cache

    def __del__(self):
        """Best-effort release resources retained by the result."""
        with suppress(Exception):
            _close_resource_owner_attr(self)
            _close_keepalive_attr(self)

    def __repr__(self) -> str:
        """Return a compact row and column count representation."""
        t = self._clean_table()
        return f"Result(rows={t.num_rows if t is not None else 0}, columns={t.num_columns if t is not None else 0})"


class SinkResult(_DiagnosticsAccessMixin, _ClosableContextManagerMixin):
    """Generic sink output wrapper."""

    def __init__(self, raw: Any):
        """Wrap a raw native sink output."""
        self._raw = raw
        self._table: Any | None = None
        self._stream: Stream | None = None

    @property
    def raw(self) -> Any:
        """Return the wrapped sink output."""
        return self._raw

    def close(self) -> None:
        """Close all sink resources."""
        _close_and_clear_attrs(self, "_stream", "_raw")
        _close_keepalive_attr(self)

    def __del__(self):
        """Best-effort close an unconsumed sink result."""
        with suppress(Exception):
            if getattr(self, "_stream", None) is None:
                self.close()

    @property
    def table(self):
        """Materialize and return the sink table when available."""
        if self._table is not None:
            return self._table

        t = getattr(self._raw, "table", None)
        if t is None:
            return None

        if hasattr(t, "__arrow_c_stream__"):
            try:
                t = _pyarrow_streams.table_from_stream_like(t, feature="sink table output")
            finally:
                # Materializing a table consumes only the main stream. Keep
                # diagnostics alive until the SinkResult itself is closed or
                # dropped.
                _close_suppressing_errors(self._raw, main_stream_only=True)
                _close_keepalive_attr(self)
        self._table = t
        return self._table

    @property
    def stream(self) -> Stream | None:
        """Return a stream wrapper when the sink exposes one."""
        if self._stream is not None:
            return self._stream
        sink = getattr(self._raw, "sink", None)
        if sink is not None and sink != "stream":
            return None
        if not hasattr(self._raw, "__arrow_c_stream__"):
            return None
        self._stream = Stream(self._raw)
        ka = getattr(self, "_keepalive", None)
        if ka is not None:
            with suppress(Exception):
                object.__setattr__(self._stream, "_keepalive", ka)
            with suppress(Exception):
                delattr(self, "_keepalive")
        return self._stream


__all__ = ["ArrowCStream", "Result", "SinkResult", "Stream"]
