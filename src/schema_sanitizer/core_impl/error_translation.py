"""Translate native execution failures into public schema-sanitizer errors.

Reader context and native detail payloads are decoded and mapped to stable exception classes
without leaking ABI-specific failure shapes through the public API.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
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
    (("out of memory", "outofmemory:"), SchemaSanitizerOutOfMemoryError),
    (("cancelled", "canceled"), SchemaSanitizerCancelledError),
)
_READER_FORMAT_MARKERS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("xml parse", "xml nesting", "xml entity"), "xml"),
    (("csv parse", "csv record", "csv field"), "csv"),
    (("json parse", "json object", "json nesting"), "json"),
    (("parquet ",), "parquet"),
)


def reader_error_context(
    format_name: str | None,
    source_kind: str | None = None,
    source_value: Any = None,
) -> dict[str, Any]:
    """Build privacy-safe reader context without copying in-memory payloads."""
    detail: dict[str, Any] = {}
    normalized_format = str(format_name or "").strip().lower()
    if normalized_format == "jsonl":
        normalized_format = "json"
    if normalized_format:
        detail["format"] = normalized_format

    normalized_kind = str(source_kind or "").strip().lower()
    if normalized_kind in {"path", "uri"} and isinstance(source_value, (str, os.PathLike)):
        detail["source"] = os.fspath(source_value)
    elif normalized_kind:
        detail["source"] = f"<{normalized_kind}>"
    return detail


def translate_core_error(
    exc: Exception,
    *,
    error_context: Mapping[str, Any] | None = None,
) -> SchemaSanitizerError:
    """Translate a native or Python exception to a public error type."""
    if isinstance(exc, SchemaSanitizerError):
        return exc

    message = str(exc)
    detail = _reader_error_detail(message, error_context)
    if isinstance(exc, MemoryError):
        return SchemaSanitizerOutOfMemoryError(message, detail=detail or None)

    lowered = message.lower()
    if "memory_limit_bytes limit exceeded" in lowered:
        resource_detail = detail
        resource_detail.update(_memory_limit_detail(message, lowered))
        return SchemaSanitizerResourceError(message, detail=resource_detail)
    for needles, error_type in _RUNTIME_ERROR_TEXT_MATCHES:
        if any(needle in lowered for needle in needles):
            return error_type(message, detail=detail or None)
    if "schema_mode='strict'" in lowered and "canonical_schema" in lowered:
        return SchemaSanitizerInvalidArgumentError(message, detail=detail or None)
    return SchemaSanitizerError(message, detail=detail or None)


def call_core(
    fn: Callable[..., _ResultT],
    *args: Any,
    error_context: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> _ResultT:
    """Call one core function and translate any raised exception."""
    try:
        from .concurrency_contracts import observe_runtime_concurrency_contract_noexcept

        observe_runtime_concurrency_contract_noexcept("native_payload_core_call")
        return fn(*args, **kwargs)
    except Exception as exc:
        raise translate_core_error(exc, error_context=error_context) from exc


def _reader_error_detail(
    message: str,
    context: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Extract safe structural reader diagnostics from one native message."""
    detail = dict(context or {})
    lowered = message.lower()

    if "format" not in detail:
        for markers, detected_format in _READER_FORMAT_MARKERS:
            if any(marker in lowered for marker in markers):
                detail["format"] = detected_format
                break
    format_name = detail.get("format")
    if isinstance(format_name, str) and "stage" not in detail:
        detail["stage"] = "parquet_read" if format_name == "parquet" else f"{format_name}_parse"

    if match := re.search(r"\bat byte\s+(\d+)\b", lowered):
        detail["byte_offset"] = int(match.group(1))
    elif match := re.search(r"\bbyte offset\s+(\d+)\b", lowered):
        detail["byte_offset"] = int(match.group(1))

    if match := re.search(r"\brow group\s+(\d+)\b", lowered):
        detail["row_group_index"] = int(match.group(1))
    elif match := re.search(r"\brow\s+(\d+)\b", lowered):
        detail["row_index"] = int(match.group(1))
    if match := re.search(r"\belement\s+(\d+)\b", lowered):
        detail["element_index"] = int(match.group(1))

    comparison = re.search(r"\b(\d+)\s*>\s*(\d+)\b", lowered)
    if comparison:
        detail["observed_value"] = int(comparison.group(1))
        detail["limit_value"] = int(comparison.group(2))
    else:
        limit = re.search(r"(?:internal\s+)?safety limit(?:\s+of|\s*:)?\s*(\d+)\b", lowered)
        if limit:
            detail["limit_value"] = int(limit.group(1))
        else:
            limit = re.search(r"effective operation limit\s+(\d+)\b", lowered)
            if limit:
                detail["limit_value"] = int(limit.group(1))

    if "limit_value" in detail and "limit_name" not in detail:
        if "nesting" in lowered or "depth" in lowered:
            detail["limit_name"] = "parser_depth"
        elif "field count" in lowered:
            detail["limit_name"] = "object_fields"
        elif "node" in lowered:
            detail["limit_name"] = "xml_nodes"
        elif "attribute" in lowered:
            detail["limit_name"] = "xml_attributes"
        elif "page" in lowered:
            detail["limit_name"] = "parquet_page_bytes"
        else:
            detail["limit_name"] = "reader_safety_limit"

    return detail


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


from .concurrency_contracts import (  # noqa: E402
    register_runtime_concurrency_contract as _register_runtime_concurrency_contract,
)

_register_runtime_concurrency_contract("native_payload_core_call", call_core)
