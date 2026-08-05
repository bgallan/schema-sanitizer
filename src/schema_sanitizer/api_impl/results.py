"""Analytical output conversion and result wrappers."""

from __future__ import annotations

import json
import os
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from ..adapters.pyarrow import streams as _pyarrow_streams
from ..core_impl.dependencies import ensure_optional_dependency
from ..core_impl.finalization import runtime_is_finalizing
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


@dataclass(frozen=True, slots=True)
class AnalyticalOutputConversion:
    """Converted analytical value plus bounded diagnostics and route metadata."""

    clean_data: Any
    diagnostics_shape: Any
    route: str


@dataclass(frozen=True, slots=True)
class _AnalyticalShape:
    """Minimal table-like shape used to finalize stream-consumer diagnostics."""

    num_rows: int
    batch_count: int = 0

    def to_batches(self) -> range:
        """Expose a cheap batch-count-compatible sequence."""
        return range(max(0, self.batch_count))


def normalize_table_output_format(output_format: str) -> str:
    """Normalize and validate a table output format."""
    if not isinstance(output_format, str):
        raise TypeError("output_format must be a string")
    target = output_format.strip().lower()
    if target not in TABLE_OUTPUT_FORMATS:
        raise ValueError(TABLE_OUTPUT_FORMAT_ERROR)
    return target


def _to_pandas(table: Any, *, feature: str, threading_mode: str = "single") -> Any:
    """Convert one Arrow table to pandas with a stable error boundary."""
    ensure_optional_dependency("pandas", extra="pandas", feature=feature)
    try:
        return table.to_pandas(use_threads=threading_mode == "multi")
    except Exception as exc:
        raise RuntimeError(
            f"{feature} could not convert the Arrow table to pandas DataFrame."
        ) from exc


def _reader_row_count_from_pandas(frame: Any) -> int:
    """Return a pandas row count without converting or copying the frame."""
    index = getattr(frame, "index", None)
    if index is not None:
        try:
            return int(len(index))
        except Exception:
            pass
    try:
        return int(len(frame))
    except Exception:
        return 0


def _polars_from_arrow_preserving_chunks(
    polars: Any, value: Any, *, feature: str
) -> tuple[Any, str]:
    """Convert Arrow input without a full-frame rechunk when supported."""
    from_arrow = getattr(polars, "from_arrow", None)
    if not callable(from_arrow):
        raise RuntimeError(f"{feature} could not access polars.from_arrow().")
    try:
        return from_arrow(value, rechunk=False), "record_batch_reader_to_polars"
    except TypeError as exc:
        message = str(exc).lower()
        unsupported_rechunk = "rechunk" in message and any(
            marker in message
            for marker in (
                "unexpected keyword",
                "keyword argument",
                "invalid keyword",
                "unsupported keyword",
            )
        )
        if not unsupported_rechunk:
            raise
        try:
            return from_arrow(value), "record_batch_reader_to_polars"
        except Exception as fallback_exc:
            raise RuntimeError(
                f"{feature} could not convert the Arrow stream to Polars DataFrame."
            ) from fallback_exc


def _reader_row_count_from_polars(frame: Any) -> int:
    """Return a Polars row count without materializing Python rows."""
    try:
        return max(0, int(frame.height))
    except Exception:
        shape = getattr(frame, "shape", None)
        if shape is None:
            return 0
        try:
            return max(0, int(shape[0]))
        except Exception:
            return 0


def _reader_batch_count(reader: Any) -> int:
    """Return a reader batch count when an adapter exposes one cheaply."""
    for name in ("num_record_batches", "num_batches"):
        value = getattr(reader, name, None)
        try:
            if value is not None:
                return max(0, int(value))
        except Exception:
            continue
    return 0


def _reader_batches(reader: Any, *, feature: str) -> tuple[list[Any], int]:
    """Consume a reader into ordered batches without building an Arrow table."""
    try:
        batches = list(reader)
    except Exception as exc:
        raise RuntimeError(f"{feature} could not consume the Arrow batches.") from exc
    row_count = 0
    for batch in batches:
        try:
            row_count += max(0, int(batch.num_rows))
        except Exception:
            continue
    return batches, row_count


def _read_all_from_reader(reader: Any, *, feature: str) -> Any:
    """Consume a record-batch reader into one table with a stable boundary."""
    read_all = getattr(reader, "read_all", None)
    if callable(read_all):
        try:
            return read_all()
        except Exception as exc:
            raise RuntimeError(f"{feature} could not materialize the Arrow stream.") from exc
    return _pyarrow_streams.table_from_stream_like(reader, feature=feature)


def convert_arrow_stream_output(
    stream: Any,
    target: str,
    *,
    feature: str,
    threading_mode: str = "single",
) -> AnalyticalOutputConversion:
    """Convert an Arrow C Stream directly into one analytical output target."""
    reader = _pyarrow_streams.reader_from_stream_like(stream, feature=feature)
    try:
        if target == "pyarrow":
            table = _read_all_from_reader(reader, feature=feature)
            return AnalyticalOutputConversion(
                table,
                table,
                "record_batch_reader_to_pyarrow_table",
            )

        if target == "pandas":
            ensure_optional_dependency("pandas", extra="pandas", feature=feature)
            read_pandas = getattr(reader, "read_pandas", None)
            if not callable(read_pandas):
                table = _read_all_from_reader(reader, feature=feature)
                frame = _to_pandas(
                    table,
                    feature=feature,
                    threading_mode=threading_mode,
                )
                return AnalyticalOutputConversion(
                    frame,
                    table,
                    "pyarrow_table_fallback_to_pandas",
                )
            try:
                frame = read_pandas(use_threads=threading_mode == "multi")
            except Exception as exc:
                raise RuntimeError(
                    f"{feature} could not convert the Arrow stream to pandas DataFrame."
                ) from exc
            return AnalyticalOutputConversion(
                frame,
                _AnalyticalShape(
                    _reader_row_count_from_pandas(frame),
                    _reader_batch_count(reader),
                ),
                "record_batch_reader_to_pandas",
            )

        if target == "polars":
            polars = ensure_optional_dependency("polars", extra="polars", feature=feature)
            try:
                frame, route = _polars_from_arrow_preserving_chunks(polars, reader, feature=feature)
            except RuntimeError:
                raise
            except Exception as exc:
                raise RuntimeError(
                    f"{feature} could not convert the Arrow stream to Polars DataFrame."
                ) from exc
            return AnalyticalOutputConversion(
                frame,
                _AnalyticalShape(
                    _reader_row_count_from_polars(frame),
                    _reader_batch_count(reader),
                ),
                route,
            )

        if target == "duckdb":
            dataset_module = ensure_optional_dependency(
                "pyarrow.dataset",
                extra="pyarrow",
                feature=feature,
                dependency_name="pyarrow",
            )
            duckdb = ensure_optional_dependency("duckdb", extra="duckdb", feature=feature)
            try:
                schema = reader.schema
                batches, row_count = _reader_batches(reader, feature=feature)
                dataset = dataset_module.dataset(batches, schema=schema)
                relation = duckdb.from_arrow(dataset)
            except Exception as exc:
                raise RuntimeError(
                    f"{feature} could not bind the Arrow stream as a DuckDB relation."
                ) from exc
            return AnalyticalOutputConversion(
                relation,
                _AnalyticalShape(row_count, len(batches)),
                "record_batch_reader_to_arrow_dataset_to_duckdb",
            )

        raise AssertionError(f"validated table output target was not handled: {target!r}")
    finally:
        _close_suppressing_errors(reader)


def convert_arrow_table_output(
    table: Any,
    target: str,
    *,
    feature: str,
    threading_mode: str = "single",
) -> Any:
    """Convert a PyArrow table to a validated analytical output target."""
    if target == "pyarrow":
        return table
    if target == "pandas":
        return _to_pandas(table, feature=feature, threading_mode=threading_mode)
    if target == "polars":
        polars = ensure_optional_dependency("polars", extra="polars", feature=feature)
        frame, _route = _polars_from_arrow_preserving_chunks(polars, table, feature=feature)
        return frame
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
        execution_policy: dict[str, Any] | None = None,
        conversion_route: str | None = None,
    ):
        """Wrap raw reader output and optional materialized clean data."""
        self._pid = os.getpid()
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
        self.execution_policy = execution_policy
        self.conversion_route = conversion_route

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
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
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
        self._pid = os.getpid()
        self._raw = raw
        self._table: Any | None = None
        self._stream: Stream | None = None

    @property
    def raw(self) -> Any:
        """Return the wrapped sink output."""
        return self._raw

    def close(self) -> None:
        """Close all sink resources without orphaning failed ownership."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return
        _close_and_clear_attrs(self, "_stream", "_raw")
        if self._stream is None and self._raw is None:
            _close_keepalive_attr(self)

    def __del__(self):
        """Best-effort close an unconsumed sink result."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
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
                if _close_suppressing_errors(self._raw, main_stream_only=True):
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
