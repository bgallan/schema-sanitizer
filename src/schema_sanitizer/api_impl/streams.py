"""Runtime Arrow stream wrappers and diagnostics helpers."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import suppress
from typing import Any, Literal

from ..adapters.pyarrow import streams as _pyarrow_streams
from ..core_impl.error_translation import translate_core_error
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_detached_resources_finalizer_cleanup,
    reserve_finalizer_cleanup,
)
from ..core_impl.json_payloads import json_object_loads
from ..core_impl.resource_lifecycle import (
    _close_and_clear_attrs,
    _close_keepalive_attr,
    _close_suppressing_errors,
)

_DIAGNOSTIC_INT_KEYS = (
    "inferred_rows",
    "inferred_bytes",
    "arrow_schema_depth",
    "parquet_schema_depth",
    "materialized_rows",
    "batches",
    "flattened_fields",
    "scalar_wrappings",
    "direct_arrow_input",
    "skipped_rows",
    "cancellations",
    "current_charged_memory_bytes",
    "peak_charged_memory_bytes",
    "operation_memory_limit_bytes",
    "parser_max_depth",
    "decoded_bytes",
    "reader_records",
    "reader_nodes",
    "compressed_bytes",
    "decompressed_bytes",
    "warnings",
    "errors",
    "soft_errors",
)
_DIAGNOSTIC_STRING_KEYS = ("file_output_route", "file_metadata_route")


def diagnostics_raw_json(raw: Any) -> str:
    """Return a JSON representation of raw diagnostics."""
    fn = getattr(raw, "to_json", None)
    if callable(fn):
        return str(fn())

    payload: dict[str, Any] = {}
    missing = object()
    for key in _DIAGNOSTIC_INT_KEYS:
        value = getattr(raw, key, missing)
        if value is not missing:
            payload[key] = value
    for key in _DIAGNOSTIC_STRING_KEYS:
        value = getattr(raw, key, missing)
        if value is not missing:
            payload[key] = value
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def diagnostics_target(raw: Any) -> Any:
    """Return the diagnostics object associated with a wrapper."""
    if raw is None:
        return None
    try:
        return object.__getattribute__(raw, "diagnostics")
    except AttributeError:
        return raw


def diagnostics_payload(raw: Any) -> dict[str, Any]:
    """Parse raw diagnostics into a dictionary."""
    diag = diagnostics_target(raw)
    if diag is not None:
        try:
            cached = object.__getattribute__(diag, "_obj")
        except AttributeError:
            cached = None
        if isinstance(cached, dict):
            return cached
    try:
        return json_object_loads(diagnostics_raw_json(raw))
    except Exception:
        return {}


def patch_diagnostics_values(raw: Any, values: dict[str, Any]) -> None:
    """Patch live and serialized diagnostics values."""
    diag = diagnostics_target(raw)
    if diag is None:
        return

    ensure_obj = getattr(diag, "_ensure_obj", None)
    if callable(ensure_obj):
        obj = ensure_obj()
    else:
        try:
            obj = object.__getattribute__(diag, "_obj")
        except AttributeError:
            for key, value in values.items():
                setattr(diag, key, value)
            return
    if not isinstance(obj, dict):
        for key, value in values.items():
            setattr(diag, key, value)
        return

    for key, value in values.items():
        setattr(diag, key, value)

    obj.update(values)
    diag._diag_json = json.dumps(obj, separators=(",", ":"), sort_keys=True, default=str)


def increment_diagnostics_counter(raw: Any, key: str, delta: int) -> None:
    """Increment a diagnostics counter when available."""
    diag = diagnostics_target(raw)
    if diag is None:
        return
    try:
        current = int(getattr(diag, key, 0) or 0)
    except Exception:
        current = 0
    patch_diagnostics_values(diag, {key: current + delta})


def diagnostics_stats(raw: Any) -> dict[str, Any]:
    """Return normalized integer diagnostics statistics."""
    payload = diagnostics_payload(raw)
    out: dict[str, Any] = {}
    for key in _DIAGNOSTIC_INT_KEYS:
        value = getattr(raw, key, payload.get(key, 0))
        try:
            out[key] = int(value)
        except Exception:
            out[key] = 0
    out["cancellation_reason"] = str(
        getattr(raw, "cancellation_reason", payload.get("cancellation_reason", "")) or ""
    )
    for key in _DIAGNOSTIC_STRING_KEYS:
        out[key] = str(getattr(raw, key, payload.get(key, "")) or "")
    compressed = out.get("compressed_bytes", 0)
    decompressed = out.get("decompressed_bytes", 0)
    out["decompression_ratio"] = float(decompressed) / float(compressed) if compressed > 0 else 0.0
    manifest_keys = ("source_manifest_uri", "source_object_count", "source_objects")
    try:
        explicit_attributes = vars(raw)
    except TypeError:
        explicit_attributes = {}
    if any(key in payload or key in explicit_attributes for key in manifest_keys):
        out["source_manifest_uri"] = str(
            getattr(raw, "source_manifest_uri", payload.get("source_manifest_uri", "")) or ""
        )
        try:
            out["source_object_count"] = int(
                getattr(raw, "source_object_count", payload.get("source_object_count", 0)) or 0
            )
        except Exception:
            out["source_object_count"] = 0
        source_objects = getattr(raw, "source_objects", payload.get("source_objects", []))
        out["source_objects"] = (
            list(source_objects) if isinstance(source_objects, (list, tuple)) else []
        )
    return out


class DiagnosticsAccessMixin:
    """Provide normalized diagnostics statistics."""

    _raw: Any

    @property
    def stats(self) -> dict[str, Any]:
        """Return normalized ingestion statistics."""
        return diagnostics_stats(getattr(self._raw, "diagnostics", None))


class ClosableContextManagerMixin:
    """Provide context manager behavior for closable wrappers."""

    def close(self) -> None:
        """Release resources owned by the wrapper."""
        raise NotImplementedError

    def __enter__(self):
        """Return the wrapper for context manager use."""
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> Literal[False]:
        """Close the wrapper when leaving a context."""
        self.close()
        return False


def _close_arrow_c_stream_finalizer_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Close only the primary Arrow stream plus its detached keepalive."""
    raw = capsule.arg0
    keepalive = capsule.arg1
    if raw is not None and not _close_suppressing_errors(raw, main_stream_only=True):
        raise RuntimeError("deferred ArrowCStream main-stream cleanup failed")
    if keepalive is not None and keepalive is not raw and not _close_suppressing_errors(keepalive):
        raise RuntimeError("deferred ArrowCStream keepalive cleanup failed")


class ArrowCStream(DiagnosticsAccessMixin, ClosableContextManagerMixin):
    """Lightweight wrapper exposing the Arrow C Stream protocol."""

    def __init__(self, raw: Any):
        """Wrap an Arrow C Stream-capable backend."""
        capsule = reserve_finalizer_cleanup(_close_arrow_c_stream_finalizer_capsule)
        ticket = capsule.ticket
        self._finalizer_ticket = ticket
        self._finalizer_capsule: PreparedFinalizerCleanup | None = capsule
        self._pid = os.getpid()
        self._raw = raw

    @property
    def raw(self) -> Any:
        """Return the wrapped backend object."""
        return self._raw

    def __arrow_c_stream__(self, requested_schema=None):
        """Export the wrapped stream through the Arrow C Stream protocol."""
        fn = getattr(self._raw, "__arrow_c_stream__", None)
        if fn is None:
            raise TypeError("backend does not expose __arrow_c_stream__")
        if requested_schema is not None:
            with suppress(TypeError):
                return fn(requested_schema)
        return fn()

    def __arrow_c_schema__(self):
        """Export the wrapped schema through the Arrow C Data protocol."""
        fn = getattr(self._raw, "__arrow_c_schema__", None)
        if fn is None:
            raise AttributeError("__arrow_c_schema__")
        return fn()

    def close(self) -> None:
        """Close the primary wrapped stream without losing failed ownership."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return
        raw = self._raw
        if raw is not None and not _close_suppressing_errors(raw, main_stream_only=True):
            return
        if self._raw is raw:
            self._raw = None
        _close_keepalive_attr(self)
        if self._raw is None and getattr(self, "_keepalive", None) is None:
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                cancel_prepared_finalizer_cleanup(capsule)
                self._finalizer_ticket = 0
                self._finalizer_capsule = None

    def __del__(self):
        """Best-effort close the wrapped stream."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                capsule.arg0 = getattr(self, "_raw", None)
                capsule.arg1 = getattr(self, "_keepalive", None)
                if defer_prepared_finalizer_cleanup(capsule):
                    self._finalizer_ticket = 0
                    self._finalizer_capsule = None
        except BaseException:
            pass

    @property
    def schema(self):
        """Return the wrapped schema when available."""
        return getattr(self._raw, "schema", None)

    def __repr__(self) -> str:
        """Return a debug representation."""
        return f"ArrowCStream({self._raw!r})"


class Stream(DiagnosticsAccessMixin, ClosableContextManagerMixin, Iterator):
    """Iterator yielding :class:`pyarrow.RecordBatch`."""

    def __init__(self, raw: Any):
        """Create an iterator from an Arrow stream-capable backend."""
        capsule = reserve_detached_resources_finalizer_cleanup()
        ticket = capsule.ticket
        self._finalizer_ticket = ticket
        self._finalizer_capsule: PreparedFinalizerCleanup | None = capsule
        self._pid = os.getpid()
        self._raw = raw
        self._keepalive: Any = None
        self._close_on_exhaustion = False
        self._exhausted = False
        self.schema_registry_json: str | None = None
        self.schema_drifts_json: str | None = None
        self.native_registry_state: Any = None
        self.execution_policy: dict[str, Any] | None = None
        if _pyarrow_streams.is_record_batch_reader(raw, feature="Stream construction"):
            self._reader = raw
            return
        if hasattr(raw, "__arrow_c_stream__"):
            self._reader = _pyarrow_streams.reader_from_stream_like(
                raw, feature="Stream construction"
            )
            return
        raise TypeError(
            "Stream backend does not expose the Arrow C Stream protocol (__arrow_c_stream__)."
        )

    @property
    def schema(self):
        """Return the stream schema when available."""
        try:
            return self._reader.schema
        except Exception:
            return getattr(self._raw, "schema", None)

    def __iter__(self) -> Stream:
        """Return this stream iterator."""
        return self

    def __next__(self):
        """Return the next record batch and update diagnostics."""
        if self._exhausted:
            raise StopIteration
        try:
            batch = next(self._reader)
        except StopIteration:
            if self._close_on_exhaustion:
                self._exhausted = True
                self.close()
            raise
        except Exception as exc:
            raise translate_core_error(exc) from exc
        if batch.num_rows > 0:
            increment_diagnostics_counter(self._raw, "batches", 1)
            increment_diagnostics_counter(self._raw, "materialized_rows", batch.num_rows)
        return batch

    def __arrow_c_stream__(self, requested_schema=None):
        """Export the wrapped stream through the Arrow C Stream protocol."""
        for obj in (getattr(self, "_reader", None), getattr(self, "_raw", None)):
            if obj is None:
                continue
            fn = getattr(obj, "__arrow_c_stream__", None)
            if fn is None:
                continue
            if requested_schema is not None:
                with suppress(TypeError):
                    return fn(requested_schema)
            return fn()
        raise AttributeError("__arrow_c_stream__")

    def __arrow_c_schema__(self):
        """Export the wrapped schema through the Arrow C Data protocol."""
        for obj in (getattr(self, "_reader", None), getattr(self, "_raw", None)):
            if obj is None:
                continue
            fn = getattr(obj, "__arrow_c_schema__", None)
            if fn is not None:
                return fn()
        raise AttributeError("__arrow_c_schema__")

    def close_main_stream(self) -> None:
        """Close the primary stream while preserving failed ownership."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return
        reader = getattr(self, "_reader", None)
        raw = getattr(self, "_raw", None)
        if reader is raw and reader is not None:
            if _close_suppressing_errors(reader):
                object.__setattr__(self, "_reader", None)
                object.__setattr__(self, "_raw", None)
        else:
            _close_and_clear_attrs(self, "_reader")
            raw = getattr(self, "_raw", None)
            if raw is not None and _close_suppressing_errors(raw, main_stream_only=True):
                object.__setattr__(self, "_raw", None)
        if getattr(self, "_reader", None) is None and getattr(self, "_raw", None) is None:
            _close_keepalive_attr(self)

    def close(self) -> None:
        """Close the stream and all owned resources transactionally."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return
        _close_and_clear_attrs(self, "_reader", "_raw")
        if getattr(self, "_reader", None) is None and getattr(self, "_raw", None) is None:
            _close_keepalive_attr(self)
        if (
            getattr(self, "_reader", None) is None
            and getattr(self, "_raw", None) is None
            and getattr(self, "_keepalive", None) is None
        ):
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                cancel_prepared_finalizer_cleanup(capsule)
                self._finalizer_ticket = 0
                self._finalizer_capsule = None

    def __del__(self):
        """Best-effort close the stream."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                capsule.arg0 = getattr(self, "_reader", None)
                capsule.arg1 = getattr(self, "_raw", None)
                capsule.arg2 = getattr(self, "_keepalive", None)
                if defer_prepared_finalizer_cleanup(capsule):
                    self._finalizer_ticket = 0
                    self._finalizer_capsule = None
        except BaseException:
            pass

    def to_table(self):
        """Materialize the stream as a PyArrow table."""
        return _pyarrow_streams.table_from_stream_like(self, feature="Stream.to_table")
