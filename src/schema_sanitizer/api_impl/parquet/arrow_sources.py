"""Direct Parquet Arrow-source planning and lifecycle."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from itertools import islice
from typing import Any

from ...adapters.parquet.memory import (
    parquet_batch_size_from_memory_limit,
    parquet_memory_limit_allows_direct_ingest,
    parquet_use_threads,
)
from ...adapters.parquet.record_batch_factory import (
    open_parquet_record_batch_stream_factory,
)
from ...adapters.parquet.status import parquet_schema_is_direct_native_eligible
from ...core_impl.dependencies import ensure_pyarrow
from ...core_impl.finalization import runtime_is_finalizing
from ...core_impl.resource_lifecycle import _close_suppressing_errors
from ...input_impl.selection import _Source
from ...options_impl.options import Options, memory_limit_bytes_or_none
from .errors import direct_parquet_memory_limit_error

RouteCallback = Callable[[str], None]


@dataclass(frozen=True, slots=True)
class ParquetArrowSource:
    """One Parquet input exposed as a native Arrow stream source."""

    data: Any
    source: _Source
    source_file: str


def _parquet_option_settings(
    call_options: Options | None,
) -> tuple[int | None, str, str]:
    """Return direct Parquet memory, timestamp, and threading settings."""
    if isinstance(call_options, Options):
        return (
            memory_limit_bytes_or_none(call_options),
            call_options.timestamp_precision,
            call_options.performance.threading_mode.name.lower(),
        )
    return None, "TIMESTAMP_MICROS", "single"


def _set_route(set_route: RouteCallback | None, route: str) -> None:
    """Record a route decision when diagnostics are enabled."""
    if set_route is not None:
        set_route(route)


def close_parquet_arrow_factory(factory: Any) -> None:
    """Close one Parquet Arrow factory-like object when supported."""
    close = getattr(factory, "close", None)
    if callable(close):
        close()


def close_parquet_arrow_sources(
    sources: Iterable[tuple[Any, str]],
) -> list[tuple[Any, str]]:
    """Close factories in LIFO order and retain failures in mutable inputs."""
    owned = sources if isinstance(sources, list) else list(sources)
    failed: list[tuple[Any, str]] = []
    outcomes: dict[int, bool] = {}
    failed_ids: set[int] = set()
    while owned:
        entry = owned.pop()
        ident = id(entry[0])
        succeeded = outcomes.get(ident)
        if succeeded is None:
            succeeded = _close_suppressing_errors(entry[0])
            outcomes[ident] = succeeded
        if not succeeded and ident not in failed_ids:
            failed.append(entry)
            failed_ids.add(ident)
    owned.extend(reversed(failed))
    return failed


def parquet_arrow_stream_factory_or_none(
    data: Any,
    *,
    source: _Source,
    feature: str,
    call_options: Options | None,
    set_route: RouteCallback | None = None,
) -> Any | None:
    """Return a direct Parquet Arrow stream factory when supported."""
    memory_limit_bytes, timestamp_precision, threading_mode = _parquet_option_settings(call_options)
    if hasattr(data, "__arrow_c_stream__") and hasattr(data, "schema"):
        factory = data
    else:
        if source not in {"path", "uri", "text", "stream"}:
            _set_route(set_route, "unsupported_source")
            return None
        if not parquet_memory_limit_allows_direct_ingest(memory_limit_bytes):
            _set_route(set_route, "memory_limit")
            assert memory_limit_bytes is not None
            raise direct_parquet_memory_limit_error(memory_limit_bytes)
        factory = open_parquet_record_batch_stream_factory(
            data,
            source=source,
            feature=feature,
            batch_size=parquet_batch_size_from_memory_limit(memory_limit_bytes),
            use_threads=parquet_use_threads(threading_mode, memory_limit_bytes),
            memory_limit_bytes=memory_limit_bytes,
        )
    if not parquet_memory_limit_allows_direct_ingest(memory_limit_bytes):
        _set_route(set_route, "memory_limit")
        close_parquet_arrow_factory(factory)
        assert memory_limit_bytes is not None
        raise direct_parquet_memory_limit_error(memory_limit_bytes)
    pa = ensure_pyarrow(feature=feature)
    if not parquet_schema_is_direct_native_eligible(
        factory.schema,
        pa=pa,
        timestamp_precision=timestamp_precision,
    ):
        _set_route(set_route, "schema_unsupported")
        close_parquet_arrow_factory(factory)
        return None
    _set_route(set_route, "factory")
    return factory


def parquet_arrow_source_chunk_size(call_options: Options | None) -> int:
    """Derive the lazy Parquet file window from the memory budget."""
    memory_limit_bytes, _timestamp_precision, threading_mode = _parquet_option_settings(
        call_options
    )
    from ...core_impl.execution_policy import execution_policy

    return execution_policy(threading_mode, memory_limit_bytes).async_prefetch_files * 4


def parquet_arrow_sources_or_none(
    sources_in: Iterable[ParquetArrowSource],
    *,
    call_options: Options | None,
    feature: str,
    set_route: RouteCallback | None = None,
) -> list[tuple[Any, str]] | None:
    """Return reusable Arrow stream factories for every Parquet source."""
    sources: list[tuple[Any, str]] = []
    try:
        for source_in in sources_in:
            factory = parquet_arrow_stream_factory_or_none(
                source_in.data,
                source=source_in.source,
                feature=feature,
                call_options=call_options,
                set_route=set_route,
            )
            if factory is None:
                close_parquet_arrow_sources(sources)
                return None
            sources.append((factory, source_in.source_file))
        return sources
    except Exception:
        close_parquet_arrow_sources(sources)
        raise


class ParquetArrowSourceChunkProvider:
    """Provide bounded Parquet Arrow-source chunks to native streams."""

    def __init__(
        self,
        sources: Iterable[ParquetArrowSource],
        *,
        call_options: Options | None,
        feature: str,
        set_route: RouteCallback | None = None,
    ) -> None:
        """Store source descriptors without opening Parquet files yet."""
        self._pid = os.getpid()
        self._sources = iter(sources)
        self._call_options = call_options
        self._feature = feature
        self._chunk_size = parquet_arrow_source_chunk_size(call_options)
        self._set_route = set_route
        self._current: list[tuple[Any, str]] = []
        self._closed = False

    def next_sources(self) -> list[tuple[Any, str]] | None:
        """Return the next bounded Arrow-source chunk, or ``None`` when exhausted."""
        if self._closed:
            return None
        self._close_current()
        if self._current:
            raise RuntimeError("previous Parquet source chunk cleanup failed and must be retried")
        chunk = list(islice(self._sources, self._chunk_size))
        if not chunk:
            self.close()
            return None
        current = parquet_arrow_sources_or_none(
            chunk,
            call_options=self._call_options,
            feature=self._feature,
            set_route=self._set_route,
        )
        if current is None:
            return None
        self._current = current
        return current

    def close(self) -> None:
        """Close the active chunk and mark the provider exhausted."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return
        if self._closed and not self._current:
            return
        self._closed = True
        self._close_current()

    def __del__(self) -> None:
        """Best-effort cleanup for abandoned providers."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            self.close()
        except Exception:
            pass

    def _close_current(self) -> None:
        """Close factories from the currently yielded chunk."""
        if self._current:
            close_parquet_arrow_sources(self._current)
