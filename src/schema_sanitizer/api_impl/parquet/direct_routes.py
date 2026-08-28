"""Direct native Parquet routing and retry policy."""

from __future__ import annotations

import logging
from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class ParquetDirectOutcome:
    """One direct Parquet route decision and its optional result."""

    raw: Any | None
    route: str


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


def parquet_direct_stream_factory(
    data: Any,
    *,
    source: _Source,
    feature: str,
    call_options: Options | None,
) -> ParquetDirectOutcome:
    """Return a direct Parquet Arrow stream factory outcome."""
    route = "none"

    def record_route(value: str) -> None:
        nonlocal route
        route = value

    raw = parquet_arrow_stream_factory_or_none(
        data,
        source=source,
        feature=feature,
        call_options=call_options,
        set_route=record_route,
    )
    return ParquetDirectOutcome(raw, route)


def close_parquet_direct_stream_factory(factory: Any) -> None:
    """Close a direct Parquet stream factory without surfacing cleanup errors."""
    _close_suppressing_errors(factory)


def parquet_direct_sink(
    raw_ctx: Any,
    data: Any,
    *,
    sink: str,
    source: _Source,
    feature: str,
    call_options: Options | None,
    prepared: Any | None = None,
) -> ParquetDirectOutcome:
    """Return the direct Parquet native-sink outcome."""
    factory_outcome = parquet_direct_stream_factory(
        data,
        source=source,
        feature=feature,
        call_options=call_options,
    )
    stream_factory = factory_outcome.raw
    if stream_factory is None:
        return factory_outcome
    try:
        raw = call_core(
            raw_ctx.to_sink_arrow_stream,
            sink,
            "arrow",
            stream_factory,
            unwrap_options(call_options) if prepared is None else prepared,
        )
        return ParquetDirectOutcome(raw, "native")
    except Exception as exc:
        if not should_retry_native_parquet_reader_failure(exc):
            raise
        log_native_parquet_reader_fallback()
        close_parquet_direct_stream_factory(stream_factory)
        fallback_outcome = parquet_direct_stream_factory(
            data,
            source=source,
            feature=feature,
            call_options=call_options,
        )
        if fallback_outcome.raw is None:
            return fallback_outcome
        return ParquetDirectOutcome(fallback_outcome.raw, "pyarrow")


def parquet_direct_registry_sink(
    raw_ctx: Any,
    data: Any,
    *,
    source: _Source,
    feature: str,
    call_options: Options | None,
    schema_registry_json: str,
    field_name_policy: str,
    schema_mode: str,
) -> ParquetDirectOutcome:
    """Return the direct Parquet registry-sink outcome."""
    factory_outcome = parquet_direct_stream_factory(
        data,
        source=source,
        feature=feature,
        call_options=call_options,
    )
    stream_factory = factory_outcome.raw
    if stream_factory is None:
        return factory_outcome
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
        return ParquetDirectOutcome(raw, "native_registry")
    except Exception as exc:
        if not should_retry_native_parquet_reader_failure(exc):
            raise
        log_native_parquet_reader_fallback()
        close_parquet_direct_stream_factory(stream_factory)
        return ParquetDirectOutcome(None, "pyarrow_registry_unavailable")
