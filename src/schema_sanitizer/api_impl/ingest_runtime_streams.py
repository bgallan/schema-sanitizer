"""Runtime stream wrappers for ingest results."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import suppress
from typing import Any, Literal

from ..adapters import pyarrow_streams as _pyarrow_streams
from .ingest_diagnostics import (
    _diagnostics_stats,
    _increment_diagnostics_counter,
)
from .ingest_lifecycle import (
    _close_and_clear_attrs,
    _close_keepalive_attr,
    _close_suppressing_errors,
)
from .shared import _translate_core_error


class _DiagnosticsAccessMixin:
    """Provide normalized diagnostics statistics."""

    _raw: Any

    @property
    def stats(self) -> dict[str, Any]:
        """Return normalized ingestion statistics."""
        return _diagnostics_stats(getattr(self._raw, "diagnostics", None))


class _ClosableContextManagerMixin:
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


class Stream(_DiagnosticsAccessMixin, _ClosableContextManagerMixin, Iterator):
    """Iterator yielding :class:`pyarrow.RecordBatch`."""

    def __init__(self, raw: Any):
        """Create an iterator from an Arrow stream-capable backend."""
        self._raw = raw

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
        try:
            b = next(self._reader)
        except StopIteration:
            raise
        except Exception as e:
            raise _translate_core_error(e) from e

        n = b.num_rows
        if n > 0:
            _increment_diagnostics_counter(self._raw, "batches", 1)
            _increment_diagnostics_counter(self._raw, "materialized_rows", n)

        return b

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
            if fn is None:
                continue
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


class ArrowCStream(_DiagnosticsAccessMixin, _ClosableContextManagerMixin):
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
        # When wrapping a SinkOutput for table materialization, only the main
        # stream should be released here so diagnostics stay readable.
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


__all__ = ["ArrowCStream", "Stream"]
