"""Translate native execution failures to public schema-sanitizer errors."""

from __future__ import annotations

import re
from typing import Any, Callable, TypeVar

from ..errors import (
    SchemaSanitizerCancelledError,
    SchemaSanitizerError,
    SchemaSanitizerInvalidArgumentError,
    SchemaSanitizerOutOfMemoryError,
    SchemaSanitizerResourceError,
)

_ResultT = TypeVar("_ResultT")

_RUNTIME_ERROR_TEXT_MATCHES: tuple[tuple[tuple[str, ...], type[SchemaSanitizerError]], ...] = (
    (("invalid:", "invalid argument"), SchemaSanitizerInvalidArgumentError),
    (("out of memory",), SchemaSanitizerOutOfMemoryError),
    (("cancelled", "canceled"), SchemaSanitizerCancelledError),
)


def translate_core_error(exc: Exception) -> SchemaSanitizerError:
    """Translate a native or Python exception to a public error type."""
    if isinstance(exc, SchemaSanitizerError):
        return exc

    message = str(exc)
    if isinstance(exc, MemoryError):
        return SchemaSanitizerOutOfMemoryError(message)

    lowered = message.lower()
    if "memory_limit_bytes limit exceeded" in lowered:
        return SchemaSanitizerResourceError(
            message,
            detail=_memory_limit_detail(message, lowered),
        )
    for needles, error_type in _RUNTIME_ERROR_TEXT_MATCHES:
        if any(needle in lowered for needle in needles):
            return error_type(message)
    if "schema_mode='strict'" in lowered and "canonical_schema" in lowered:
        return SchemaSanitizerInvalidArgumentError(message)
    return SchemaSanitizerError(message)


def call_core(fn: Callable[..., _ResultT], *args: Any, **kwargs: Any) -> _ResultT:
    """Call one core function and translate any raised exception."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        raise translate_core_error(exc) from exc


def _memory_limit_detail(message: str, lowered: str) -> dict[str, Any]:
    """Extract structured resource-limit details from one native error message."""
    stage = "xml_parse"
    for marker, candidate in (
        ("remote_download", "remote_download"),
        ("json_parse", "json_parse"),
        ("csv_parse", "csv_parse"),
        ("parquet conversion", "parquet_conversion"),
    ):
        if marker in lowered:
            stage = candidate
            break

    detail: dict[str, Any] = {"stage": stage, "limit_name": "memory_limit_bytes"}
    match = re.search(r"(\d+)\s+bytes\s+>\s+(\d+)\s+bytes", message)
    if match:
        detail["actual_bytes"] = int(match.group(1))
        detail["limit_bytes"] = int(match.group(2))
    if "; file: " in message:
        detail["file"] = message.rsplit("; file: ", 1)[1]
    return detail
