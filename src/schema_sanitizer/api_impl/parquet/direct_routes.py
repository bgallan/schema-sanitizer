"""Direct native Parquet routing and retry policy."""

from __future__ import annotations

import logging
from typing import Any

from ...core_impl.error_translation import call_core
from ...core_impl.resource_lifecycle import _close_suppressing_errors
from ...errors import (
    ErrorCode,
    SchemaSanitizerCancelledError,
    SchemaSanitizerError,
    SchemaSanitizerInvalidArgumentError,
    SchemaSanitizerOutOfMemoryError,
    SchemaSanitizerResourceError,
)
from ...input_impl.selection import _Source
from ...options_impl.call_options import unwrap_options
from ...options_impl.options import Options
from .arrow_sources import parquet_arrow_stream_factory_or_none

_logger = logging.getLogger(__name__)
_LAST_PARQUET_DIRECT_ROUTE = "none"


def last_parquet_direct_route() -> str:
    """Return the most recent direct Parquet route decision."""
    return _LAST_PARQUET_DIRECT_ROUTE


def set_parquet_direct_route(route: str) -> None:
    """Record the most recent direct Parquet route decision."""
    global _LAST_PARQUET_DIRECT_ROUTE
    _LAST_PARQUET_DIRECT_ROUTE = route


def should_retry_native_parquet_reader_failure(exc: Exception) -> bool:
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


def log_native_parquet_reader_fallback() -> None:
    """Log that the direct native Parquet reader path is falling back."""
    _logger.exception("Native Parquet reader failed; retrying input with PyArrow.")


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
        set_route=set_parquet_direct_route,
    )


def close_parquet_direct_stream_factory(factory: Any) -> None:
    """Close a direct Parquet stream factory without surfacing cleanup errors."""
    _close_suppressing_errors(factory)


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
        raw = call_core(
            raw_ctx.to_sink_arrow_stream,
            sink,
            "arrow",
            stream_factory,
            unwrap_options(call_options) if prepared is None else prepared,
        )
        set_parquet_direct_route("native")
        return raw
    except Exception as exc:
        if not should_retry_native_parquet_reader_failure(exc):
            raise
        log_native_parquet_reader_fallback()
        close_parquet_direct_stream_factory(stream_factory)
        fallback_factory = parquet_direct_stream_factory_or_none(
            data,
            source=source,
            feature=feature,
            call_options=call_options,
        )
        if fallback_factory is None:
            return None
        set_parquet_direct_route("pyarrow")
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
        raw = call_core(
            raw_ctx.to_registry_sink_arrow_stream,
            "stream",
            "arrow",
            stream_factory,
            unwrap_options(call_options),
            registry_json=schema_registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )
        set_parquet_direct_route("native_registry")
        return raw
    except Exception as exc:
        if not should_retry_native_parquet_reader_failure(exc):
            raise
        log_native_parquet_reader_fallback()
        close_parquet_direct_stream_factory(stream_factory)
        set_parquet_direct_route("pyarrow_registry_unavailable")
        return None
