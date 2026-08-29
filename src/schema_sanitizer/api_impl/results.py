"""Analytical output conversion and result wrappers.

It normalizes analytical targets and converts owned Arrow streams into public result
wrappers while preserving diagnostics and resource lifetime.
"""

from __future__ import annotations

import json
import os
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from ..adapters.pyarrow import streams as _pyarrow_streams
from ..core_impl.concurrency_stage_evidence import observe_successful_output_runtime_stage
from ..core_impl.dependencies import ensure_optional_dependency
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    acknowledge_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_finalizer_cleanup,
    reserve_resource_finalizer_cleanup,
)
from ..core_impl.process_resources import (
    ExternalRuntimeConcurrencyLease,
    acquire_external_runtime_threads,
    constrain_external_runtime_worker_pool,
)
from ..core_impl.resource_lifecycle import (
    _cleanup_with_note,
    _close_and_clear_attrs,
    _close_keepalive_attr,
    _close_resource_owner_attr,
    _close_suppressing_errors,
)
from ..errors import SchemaSanitizerResourceError
from .duckdb_relation import _duckdb_from_arrow_serial, _OwnedDuckDBRelation
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
    resource_owner: Any = None

    def transfer_resource_owner_to(self, result: Any) -> bool:
        """Publish the optional lazy owner on a Result before it escapes."""
        if self.resource_owner is None:
            return False
        result._resource_owner = self.resource_owner
        result._sync_finalizer_capsule()
        return True

    def rollback_resource_owner(self, primary: BaseException) -> None:
        """Close an owner whose Result publication did not commit."""
        _cleanup_with_note(
            primary,
            self.resource_owner,
            label="analytical resource-owner rollback also failed",
        )


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


def _external_worker_target() -> int:
    """Return a conservative upper bound for CPU-backed external pools."""
    return max(2, os.cpu_count() or 2)


def _configurable_external_threads(
    threading_mode: str, runtime: Any | None = None
) -> ExternalRuntimeConcurrencyLease:
    """Reserve a shared full-pool envelope or degrade a runtime to serial."""
    return acquire_external_runtime_threads(
        _external_worker_target(),
        allow_parallel=threading_mode == "multi",
        runtime=runtime,
    )


def _unconfigurable_external_threads(runtime: Any | None = None) -> ExternalRuntimeConcurrencyLease:
    """Reserve the runtime's observed physical pool or reject unsafe execution."""
    if runtime is None:
        raise TypeError("an external runtime is required")
    desired = max(1, int(runtime.thread_pool_size()))
    lease = acquire_external_runtime_threads(desired, allow_parallel=desired > 1, runtime=runtime)
    if lease.workers != desired or (desired > 1 and not lease.parallel):
        lease.close()
        raise SchemaSanitizerResourceError(
            "external runtime worker pool cannot be admitted within the process thread envelope",
            detail={
                "stage": "external_runtime_threads",
                "limit_name": "external_runtime_worker_threads",
                "actual_items": desired,
            },
        )
    return lease


def _to_pandas(table: Any, *, feature: str, threading_mode: str = "single") -> Any:
    """Convert one Arrow table to pandas under an external worker envelope."""
    ensure_optional_dependency("pandas", extra="pandas", feature=feature)
    pa = ensure_optional_dependency("pyarrow", extra="pyarrow", feature=feature)
    runtime = _configurable_external_threads(threading_mode, pa)
    try:
        if runtime.parallel:
            configured = constrain_external_runtime_worker_pool(pa, runtime.workers)
            runtime.shrink_to(configured)
        frame = table.to_pandas(use_threads=runtime.parallel)
        if runtime.parallel:
            observe_successful_output_runtime_stage("pandas")
        return frame
    except Exception as exc:
        raise RuntimeError(
            f"{feature} could not convert the Arrow table to pandas DataFrame."
        ) from exc
    finally:
        runtime.close()


def _reader_row_count_from_pandas(frame: Any) -> int:
    """Return a pandas row count without converting or copying the frame."""
    return max(0, int(len(frame.index)))


def _polars_from_arrow_preserving_chunks(
    polars: Any, value: Any, *, feature: str
) -> tuple[Any, str]:
    """Convert Arrow input without a full-frame rechunk."""
    try:
        return polars.from_arrow(value, rechunk=False), "record_batch_reader_to_polars"
    except Exception as exc:
        raise RuntimeError(
            f"{feature} could not convert the Arrow stream to Polars DataFrame."
        ) from exc


def _reader_row_count_from_polars(frame: Any) -> int:
    """Return a Polars row count without materializing Python rows."""
    return max(0, int(frame.height))


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


def _read_all_from_reader(reader: Any, *, feature: str) -> Any:
    """Consume a record-batch reader into one table with a stable boundary."""
    try:
        return reader.read_all()
    except Exception as exc:
        raise RuntimeError(f"{feature} could not materialize the Arrow stream.") from exc


def convert_arrow_stream_output(
    stream: Any,
    target: str,
    *,
    feature: str,
    threading_mode: str = "single",
) -> AnalyticalOutputConversion:
    """Convert an Arrow C Stream directly into one analytical output target."""
    reader = _pyarrow_streams.reader_from_stream_like(stream, feature=feature)
    reader_transferred = False
    try:
        if target == "pyarrow":
            table = _read_all_from_reader(reader, feature=feature)
            observe_successful_output_runtime_stage("pyarrow")
            return AnalyticalOutputConversion(
                table,
                table,
                "record_batch_reader_to_pyarrow_table",
            )

        if target == "pandas":
            ensure_optional_dependency("pandas", extra="pandas", feature=feature)
            pa = ensure_optional_dependency("pyarrow", extra="pyarrow", feature=feature)
            runtime = _configurable_external_threads(threading_mode, pa)
            try:
                if runtime.parallel:
                    configured = constrain_external_runtime_worker_pool(pa, runtime.workers)
                    runtime.shrink_to(configured)
                frame = reader.read_pandas(use_threads=runtime.parallel)
                if runtime.parallel:
                    observe_successful_output_runtime_stage("pandas")
            except Exception as exc:
                raise RuntimeError(
                    f"{feature} could not convert the Arrow stream to pandas DataFrame."
                ) from exc
            finally:
                runtime.close()
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
            runtime = _unconfigurable_external_threads(polars)
            try:
                frame, route = _polars_from_arrow_preserving_chunks(polars, reader, feature=feature)
                observe_successful_output_runtime_stage("polars")
            except RuntimeError:
                raise
            except Exception as exc:
                raise RuntimeError(
                    f"{feature} could not convert the Arrow stream to Polars DataFrame."
                ) from exc
            finally:
                runtime.close()
            return AnalyticalOutputConversion(
                frame,
                _AnalyticalShape(
                    _reader_row_count_from_polars(frame),
                    _reader_batch_count(reader),
                ),
                route,
            )

        if target == "duckdb":
            duckdb = ensure_optional_dependency("duckdb", extra="duckdb", feature=feature)
            resource_owner = None
            try:
                # Bind the reader through a dedicated one-thread connection so
                # the lazy external runtime stays inside the process envelope.
                relation, resource_owner = _duckdb_from_arrow_serial(duckdb, reader)
                observe_successful_output_runtime_stage("duckdb")
            except Exception as exc:
                raise RuntimeError(
                    f"{feature} could not bind the Arrow stream as a DuckDB relation."
                ) from exc
            # The relation is lazy and retains its Arrow source. Publish the
            # complete conversion owner before suppressing reader cleanup.
            try:
                conversion = AnalyticalOutputConversion(
                    relation,
                    _AnalyticalShape(0, _reader_batch_count(reader)),
                    "record_batch_reader_to_duckdb",
                    resource_owner,
                )
            except BaseException:
                relation = None
                if resource_owner is not None:
                    resource_owner.close()
                raise
            reader_transferred = True
            return conversion

        raise AssertionError(f"validated table output target was not handled: {target!r}")
    finally:
        if not reader_transferred:
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
        runtime = _unconfigurable_external_threads(polars)
        try:
            frame, _route = _polars_from_arrow_preserving_chunks(polars, table, feature=feature)
            observe_successful_output_runtime_stage("polars")
            return frame
        finally:
            runtime.close()
    if target == "duckdb":
        duckdb = ensure_optional_dependency("duckdb", extra="duckdb", feature=feature)
        relation, owner = _duckdb_from_arrow_serial(duckdb, table)
        return relation if owner is None else _OwnedDuckDBRelation(relation, owner)
    raise AssertionError(f"validated table output target was not handled: {target!r}")


@dataclass(slots=True)
class _ResultFinalizerState:
    """Named detached graph retained until Result cleanup reaches a safe point."""

    raw: Any = None
    native_registry_state: Any = None
    clean_data_cache: Any = None
    table_cache: Any = None
    schema_registry_cache: Any = None
    schema_drifts_cache: Any = None
    resource_owner: Any = None
    keepalive: Any = None


def _close_result_finalizer_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Close result owners at a governed safe point, not on the GC thread."""
    state = capsule.arg0
    if not isinstance(state, _ResultFinalizerState):
        return
    # Lazy analytical values may themselves retain the upstream reader.  Drop
    # those dependent cache graphs at this governed safe point before asking
    # the operation keepalive to release its thread/FD/memory authorities.
    # This mirrors ``Result.close()`` without running rich destructors on the
    # GC thread that merely published this capsule.
    # The Result owns only one reference to an analytical value.  Do not call
    # ``close`` here: callers may have retained that same value independently.
    # Dropping the capsule reference lets the value's own safe-point finalizer
    # decide whether it is the last owner.
    state.clean_data_cache = None
    state.table_cache = None
    state.schema_registry_cache = None
    state.schema_drifts_cache = None
    raw = state.raw
    native_registry_state = state.native_registry_state
    resource_owner = state.resource_owner
    keepalive = state.keepalive
    failures: list[str] = []
    if raw is not None and not _close_suppressing_errors(raw):
        failures.append("raw")
    else:
        state.raw = None
    if (
        native_registry_state is not None
        and native_registry_state is not raw
        and not _close_suppressing_errors(native_registry_state)
    ):
        failures.append("registry")
    else:
        state.native_registry_state = None
    if (
        resource_owner is not None
        and resource_owner is not raw
        and resource_owner is not native_registry_state
        and not _close_suppressing_errors(resource_owner)
    ):
        failures.append("resource_owner")
    else:
        state.resource_owner = None
    if (
        keepalive is not None
        and keepalive is not raw
        and keepalive is not native_registry_state
        and keepalive is not resource_owner
        and not _close_suppressing_errors(keepalive)
    ):
        failures.append("keepalive")
    else:
        state.keepalive = None
    if failures:
        raise RuntimeError("deferred Result cleanup failed: " + ",".join(failures))
    # The escrow clears the remaining capsule/state after every closeable owner
    # has committed.


class Result(DiagnosticsAccessMixin):
    """Result returned by format-specific reader and writer APIs."""

    _UNSET = object()
    _keepalive: Any
    _resource_owner: Any

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
        capsule = reserve_finalizer_cleanup(_close_result_finalizer_capsule)
        ticket = capsule.ticket
        self._finalizer_ticket = ticket
        self._finalizer_capsule: PreparedFinalizerCleanup | None = capsule
        self._finalizer_state: _ResultFinalizerState | None = _ResultFinalizerState()
        capsule.arg0 = self._finalizer_state
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
        self._sync_finalizer_capsule()

    def _sync_finalizer_capsule(self) -> None:
        """Synchronize a result wrapper with its finalizer cleanup capsule."""
        capsule = self._finalizer_capsule
        state = self._finalizer_state
        if capsule is None or state is None:
            return
        state.raw = self._raw
        state.native_registry_state = self.native_registry_state
        state.clean_data_cache = self._clean_data_cache
        state.table_cache = self._table_cache
        state.schema_registry_cache = self._schema_registry_cache
        state.schema_drifts_cache = self._schema_drifts_cache
        state.resource_owner = getattr(self, "_resource_owner", None)
        state.keepalive = getattr(self, "_keepalive", None)

    def _retire_finalizer_slot(self) -> None:
        """Retire the finalizer escrow slot owned by this result."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None
            self._finalizer_state = None

    @property
    def schema_registry(self) -> dict[str, Any] | None:
        """Return the parsed schema registry, parsing JSON lazily when needed."""
        if self._schema_registry_cache is self._UNSET:
            if self.schema_registry_json is None:
                return None
            self._schema_registry_cache = json.loads(self.schema_registry_json or "{}")
            self._sync_finalizer_capsule()
        return self._schema_registry_cache

    @schema_registry.setter
    def schema_registry(self, value: dict[str, Any] | None) -> None:
        """Set the parsed schema registry cache."""
        self._schema_registry_cache = value if value is not None else self._UNSET
        self._sync_finalizer_capsule()

    @property
    def schema_drifts(self) -> list[dict[str, Any]] | None:
        """Return parsed schema drifts, parsing JSON lazily when needed."""
        if self._schema_drifts_cache is self._UNSET:
            if self.schema_drifts_json is None:
                return None
            self._schema_drifts_cache = json.loads(self.schema_drifts_json or "[]")
            self._sync_finalizer_capsule()
        return self._schema_drifts_cache

    @schema_drifts.setter
    def schema_drifts(self, value: list[dict[str, Any]] | None) -> None:
        """Set the parsed schema drift cache."""
        self._schema_drifts_cache = value if value is not None else self._UNSET
        self._sync_finalizer_capsule()

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
            self._sync_finalizer_capsule()
            return None
        if hasattr(table, "__arrow_c_stream__"):
            table = _pyarrow_streams.table_from_stream_like(table, feature="Result.clean_data")
        self._table_cache = table
        self._sync_finalizer_capsule()
        return table

    def close(self) -> None:
        """Release native owners and large cached values at a governed safe point."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return
        _close_and_clear_attrs(self, "_raw", "native_registry_state")
        # Once the wrapper itself is closing there is no external owner for
        # these caches. Dropping them at the safe point prevents the generic
        # finalizer escrow from retaining table/dataframe graphs indefinitely.
        clean_data = self._clean_data_cache
        if isinstance(clean_data, _OwnedDuckDBRelation):
            if _close_suppressing_errors(clean_data):
                self._clean_data_cache = self._UNSET
        else:
            self._clean_data_cache = self._UNSET
        self._table_cache = self._UNSET
        self._schema_registry_cache = self._UNSET
        self._schema_drifts_cache = self._UNSET
        _close_resource_owner_attr(self)
        _close_keepalive_attr(self)
        self._sync_finalizer_capsule()
        if (
            self._raw is None
            and self.native_registry_state is None
            and getattr(self, "_resource_owner", None) is None
            and getattr(self, "_keepalive", None) is None
        ):
            self._retire_finalizer_slot()

    def __del__(self):
        """Detach large graphs into a pre-reserved safe-point capsule."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if not ticket or capsule is None:
                return
            self._sync_finalizer_capsule()
            if defer_prepared_finalizer_cleanup(capsule):
                # Capsule now roots all potentially rich owners. Clearing the
                # wrapper itself cannot run their destructors on the GC thread.
                self._raw = None
                self.native_registry_state = None
                self._clean_data_cache = self._UNSET
                self._table_cache = self._UNSET
                self._schema_registry_cache = self._UNSET
                self._schema_drifts_cache = self._UNSET
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
                self._finalizer_state = None
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
        """Wrap a raw native sink output with pre-reserved raw cleanup."""
        capsule = reserve_resource_finalizer_cleanup(raw)
        ticket = capsule.ticket
        self._finalizer_ticket = ticket
        self._finalizer_capsule: PreparedFinalizerCleanup | None = capsule
        self._pid = os.getpid()
        self._raw = raw
        self._table: Any | None = None
        self._stream: Stream | None = None
        self._input_source_route: str | None = None
        self._parquet_input_route: str | None = None

    def _retire_finalizer_slot(self) -> None:
        """Retire the finalizer escrow slot owned by this sink result."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

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
            self._retire_finalizer_slot()

    def __del__(self):
        """Publish only the pre-reserved raw sink owner when unconsumed."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            if getattr(self, "_stream", None) is not None:
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            raw = getattr(self, "_raw", None)
            if not ticket or capsule is None or raw is None:
                return
            capsule.arg0 = raw
            if defer_prepared_finalizer_cleanup(capsule):
                self._raw = None
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
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
