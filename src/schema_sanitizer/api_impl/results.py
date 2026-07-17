"""Analytical output conversion and result wrappers."""

from __future__ import annotations

import json
from contextlib import suppress
from typing import Any

from ..adapters.pyarrow import streams as _pyarrow_streams
from ..core_impl.dependencies import ensure_optional_dependency
from ..core_impl.resource_lifecycle import (
    _close_and_clear_attrs,
    _close_keepalive_attr,
    _close_resource_owner_attr,
    _close_suppressing_errors,
)
from .streams import ClosableContextManagerMixin, DiagnosticsAccessMixin, Stream

TABLE_OUTPUT_FORMATS = frozenset({"pyarrow", "pandas", "polars", "duckdb"})
TABLE_ADAPTER_FORMATS = TABLE_OUTPUT_FORMATS - {"pyarrow"}
TABLE_OUTPUT_FORMAT_ERROR = "output_format must be 'pyarrow', 'pandas', 'polars', or 'duckdb'."


def normalize_table_output_format(output_format: str) -> str:
    """Normalize and validate a table output format."""
    if not isinstance(output_format, str):
        raise TypeError("output_format must be a string")
    target = output_format.strip().lower()
    if target not in TABLE_OUTPUT_FORMATS:
        raise ValueError(TABLE_OUTPUT_FORMAT_ERROR)
    return target


def _to_pandas(table: Any, *, feature: str) -> Any:
    """Convert one Arrow table to pandas with a stable error boundary."""
    ensure_optional_dependency("pandas", extra="pandas", feature=feature)
    try:
        return table.to_pandas()
    except Exception as exc:
        raise RuntimeError(
            f"{feature} could not convert the Arrow table to pandas DataFrame."
        ) from exc


def convert_arrow_table_output(table: Any, target: str, *, feature: str) -> Any:
    """Convert a PyArrow table to a validated analytical output target."""
    if target == "pyarrow":
        return table
    if target == "pandas":
        return _to_pandas(table, feature=feature)
    if target == "polars":
        polars = ensure_optional_dependency("polars", extra="polars", feature=feature)
        return polars.from_arrow(table)
    if target == "duckdb":
        duckdb = ensure_optional_dependency("duckdb", extra="duckdb", feature=feature)
        return duckdb.from_arrow(table)
    raise AssertionError(f"validated table output target was not handled: {target!r}")


class Result(DiagnosticsAccessMixin):
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
        conversion_cpu_seconds: float | None = None,
        file_io_seconds: float | None = None,
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
        self.conversion_cpu_seconds = conversion_cpu_seconds
        self.file_io_seconds = file_io_seconds

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
        table = getattr(self._raw, "table", None)
        if table is None:
            self._table_cache = None
            return None
        if hasattr(table, "__arrow_c_stream__"):
            table = _pyarrow_streams.table_from_stream_like(table, feature="Result.clean_data")
        self._table_cache = table
        return table

    def __del__(self):
        """Best-effort release resources retained by the result."""
        try:
            _close_resource_owner_attr(self)
            _close_keepalive_attr(self)
        except Exception:
            pass

    def __repr__(self) -> str:
        """Return a compact row and column count representation."""
        table = self._clean_table()
        rows = table.num_rows if table is not None else 0
        columns = table.num_columns if table is not None else 0
        return f"Result(rows={rows}, columns={columns})"


class SinkResult(DiagnosticsAccessMixin, ClosableContextManagerMixin):
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
        try:
            if getattr(self, "_stream", None) is None:
                self.close()
        except Exception:
            pass

    @property
    def table(self):
        """Materialize and return the sink table when available."""
        if self._table is not None:
            return self._table
        table = getattr(self._raw, "table", None)
        if table is None:
            return None
        if hasattr(table, "__arrow_c_stream__"):
            try:
                table = _pyarrow_streams.table_from_stream_like(table, feature="sink table output")
            finally:
                _close_suppressing_errors(self._raw, main_stream_only=True)
                _close_keepalive_attr(self)
        self._table = table
        return table

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
        keepalive = getattr(self, "_keepalive", None)
        if keepalive is not None:
            with suppress(Exception):
                object.__setattr__(self._stream, "_keepalive", keepalive)
            with suppress(Exception):
                delattr(self, "_keepalive")
        return self._stream
