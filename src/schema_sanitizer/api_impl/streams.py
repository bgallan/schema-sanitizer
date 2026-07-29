"""Runtime Arrow stream wrappers and diagnostics helpers."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import suppress
from typing import Any, Literal

from ..adapters.pyarrow import streams as _pyarrow_streams
from ..core_impl.error_translation import translate_core_error
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
    "warnings",
    "errors",
    "soft_errors",
)


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


class ArrowCStream(DiagnosticsAccessMixin, ClosableContextManagerMixin):
    """Lightweight wrapper exposing the Arrow C Stream protocol."""

    def __init__(self, raw: Any):
        """Wrap an Arrow C Stream-capable backend."""
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
        """Close the primary wrapped stream."""
        _close_suppressing_errors(self._raw, main_stream_only=True)
        self._raw = None
        _close_keepalive_attr(self)

    def __del__(self):
        """Best-effort close the wrapped stream."""
        with suppress(Exception):
            self.close()

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
        """Close the primary stream while preserving diagnostic resources."""
        _close_and_clear_attrs(self, "_reader")
        raw = getattr(self, "_raw", None)
        if raw is not None:
            _close_suppressing_errors(raw, main_stream_only=True)
            with suppress(Exception):
                object.__setattr__(self, "_raw", None)
        _close_keepalive_attr(self)

    def close(self) -> None:
        """Close the stream and all owned resources."""
        _close_and_clear_attrs(self, "_reader", "_raw")
        _close_keepalive_attr(self)

    def __del__(self):
        """Best-effort close the stream."""
        with suppress(Exception):
            self.close()

    def to_table(self):
        """Materialize the stream as a PyArrow table."""
        return _pyarrow_streams.table_from_stream_like(self, feature="Stream.to_table")
