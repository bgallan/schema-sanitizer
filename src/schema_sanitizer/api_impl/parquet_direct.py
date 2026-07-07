"""Shared direct Parquet-to-native Arrow routing helpers."""

from __future__ import annotations

import logging
from typing import Any

from ..errors import (
    ErrorCode,
    SchemaSanitizerCancelledError,
    SchemaSanitizerError,
    SchemaSanitizerInvalidArgumentError,
    SchemaSanitizerOutOfMemoryError,
    SchemaSanitizerResourceError,
)
from .ingest_runtime_selectors import _Source
from .parquet_arrow_sources import (
    close_parquet_arrow_sources,
    parquet_arrow_stream_factory_or_none,
)
from .shared import Options, _call_core, _unwrap_options

_LAST_PARQUET_DIRECT_ROUTE = "none"
_logger = logging.getLogger(__name__)


def last_parquet_direct_route() -> str:
    """Return the most recent direct Parquet route decision."""
    return _LAST_PARQUET_DIRECT_ROUTE


def _set_parquet_direct_route(route: str) -> None:
    """Record the most recent direct Parquet route decision."""
    global _LAST_PARQUET_DIRECT_ROUTE
    _LAST_PARQUET_DIRECT_ROUTE = route


def _should_retry_native_parquet_reader_failure(exc: Exception) -> bool:
    """Return whether a native Parquet direct-read failure should use PyArrow."""
    if isinstance(
        exc,
        (
            SchemaSanitizerCancelledError,
            SchemaSanitizerInvalidArgumentError,
            SchemaSanitizerOutOfMemoryError,
            SchemaSanitizerResourceError,
        ),
    ):
        return False
    if isinstance(exc, SchemaSanitizerError):
        return exc.code == ErrorCode.RUNTIME
    return isinstance(exc, RuntimeError)


def _log_native_parquet_reader_fallback(exc: Exception) -> None:
    """Log that the direct native Parquet reader path is falling back."""
    del exc
    _logger.exception("Native Parquet reader failed; retrying input with PyArrow.")


def _close_stream_factory(factory: Any) -> None:
    """Close a stream factory without surfacing cleanup errors."""
    close_parquet_arrow_sources([(factory, "")])


def parquet_direct_stream_factory_or_none(
    data: Any,
    *,
    source: _Source,
    feature: str,
    call_options: Options | None,
) -> Any | None:
    """Return a direct Parquet Arrow stream factory when supported."""
    return parquet_arrow_stream_factory_or_none(
        data,
        source=source,
        feature=feature,
        call_options=call_options,
        set_route=_set_parquet_direct_route,
    )


def parquet_direct_sink_raw_or_none(
    raw_ctx: Any,
    data: Any,
    *,
    sink: str,
    source: _Source,
    feature: str,
    call_options: Options | None,
    prepared: Any | None = None,
) -> Any | None:
    """Return native sink output for direct Parquet ingestion when possible."""
    stream_factory = parquet_direct_stream_factory_or_none(
        data,
        source=source,
        feature=feature,
        call_options=call_options,
    )
    if stream_factory is None:
        return None
    try:
        raw = _call_core(
            raw_ctx.to_sink_arrow_stream,
            sink,
            "arrow",
            stream_factory,
            _unwrap_options(call_options) if prepared is None else prepared,
        )
        _set_parquet_direct_route("native")
        return raw
    except Exception as exc:
        if not _should_retry_native_parquet_reader_failure(exc):
            raise
        _log_native_parquet_reader_fallback(exc)
        _close_stream_factory(stream_factory)
        fallback_factory = parquet_direct_stream_factory_or_none(
            data,
            source=source,
            feature=feature,
            call_options=call_options,
        )
        if fallback_factory is None:
            return None
        _set_parquet_direct_route("pyarrow")
        return fallback_factory


def parquet_direct_registry_sink_raw_or_none(
    raw_ctx: Any,
    data: Any,
    *,
    source: _Source,
    feature: str,
    call_options: Options | None,
    schema_registry_json: str,
    field_name_policy: str,
    schema_mode: str,
) -> Any | None:
    """Return registry-backed native sink output for direct Parquet ingestion."""
    stream_factory = parquet_direct_stream_factory_or_none(
        data,
        source=source,
        feature=feature,
        call_options=call_options,
    )
    if stream_factory is None:
        return None
    try:
        raw = _call_core(
            raw_ctx.to_registry_sink_arrow_stream,
            "stream",
            "arrow",
            stream_factory,
            _unwrap_options(call_options),
            registry_json=schema_registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )
        _set_parquet_direct_route("native_registry")
        return raw
    except Exception as exc:
        if not _should_retry_native_parquet_reader_failure(exc):
            raise
        _log_native_parquet_reader_fallback(exc)
        _close_stream_factory(stream_factory)
        _set_parquet_direct_route("pyarrow_registry_unavailable")
        return None
