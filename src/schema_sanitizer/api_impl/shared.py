"""Implements `schema_sanitizer.api_impl.shared`."""

from __future__ import annotations

import json
import re
from typing import Any

from ..core_impl.options_bytes import Options as _NativeOptions
from ..errors import (
    SchemaSanitizerCancelledError,
    SchemaSanitizerError,
    SchemaSanitizerInvalidArgumentError,
    SchemaSanitizerOutOfMemoryError,
    SchemaSanitizerResourceError,
)
from ..options_impl.options import Options

_RUNTIME_ERROR_TEXT_MATCHES: tuple[tuple[tuple[str, ...], type[SchemaSanitizerError]], ...] = (
    (("invalid:", "invalid argument"), SchemaSanitizerInvalidArgumentError),
    (("out of memory",), SchemaSanitizerOutOfMemoryError),
    (("cancelled", "canceled"), SchemaSanitizerCancelledError),
)


def _translate_core_error(exc: Exception) -> SchemaSanitizerError:
    """Translate a native or Python exception to a public error type."""
    # Avoid double-wrapping.
    if isinstance(exc, SchemaSanitizerError):
        return exc

    msg = str(exc)

    # If Python raises MemoryError directly (rare), map it to the public type.
    if isinstance(exc, MemoryError):
        return SchemaSanitizerOutOfMemoryError(msg)

    # Fallback for native lanes that raise plain RuntimeError strings.
    low = msg.lower()
    if "memory_limit_bytes limit exceeded" in low:
        if "remote_download" in low:
            stage = "remote_download"
        elif "json_parse" in low:
            stage = "json_parse"
        elif "csv_parse" in low:
            stage = "csv_parse"
        elif "parquet conversion" in low:
            stage = "parquet_conversion"
        else:
            stage = "xml_parse"
        detail: dict[str, Any] = {
            "stage": stage,
            "limit_name": "memory_limit_bytes",
        }
        match = re.search(r"(\d+)\s+bytes\s+>\s+(\d+)\s+bytes", msg)
        if match:
            detail["actual_bytes"] = int(match.group(1))
            detail["limit_bytes"] = int(match.group(2))
        file_marker = "; file: "
        if file_marker in msg:
            detail["file"] = msg.rsplit(file_marker, 1)[1]
        return SchemaSanitizerResourceError(
            msg,
            detail=detail,
        )
    for needles, error_type in _RUNTIME_ERROR_TEXT_MATCHES:
        if any(needle in low for needle in needles):
            return error_type(msg)
    if "schema_mode='strict'" in low and "canonical_schema" in low:
        return SchemaSanitizerInvalidArgumentError(msg)
    return SchemaSanitizerError(msg)


def _call_core(fn, *args, **kwargs):
    """Call a core function and translate any raised exception."""
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        raise _translate_core_error(e) from e


def _unwrap_options(options: Any) -> Any:
    """Return the options object expected by the C++ extension.

    Accepted types:
    - None
    - internal Options objects produced from per-call option keywords
    """

    if options is None:
        return None

    if isinstance(options, Options):
        return options.raw

    if isinstance(options, _NativeOptions):
        raise TypeError(
            "Passing raw native option objects to the high-level API is not supported. "
            "Use per-call option keywords."
        )

    raise TypeError("options must be None or internal call options")


def _estimate_python_payload_bytes(data: Any) -> int | None:
    """Best-effort byte estimate for Python-object ingestion payloads."""

    class _Utf8ByteCounter:
        """Count UTF-8 bytes written by the JSON encoder."""

        size = 0

        def write(self, s: str) -> int:
            """Count a text fragment and report its character length."""
            self.size += len(s.encode("utf-8", "replace"))
            return len(s)

    counter = _Utf8ByteCounter()
    try:
        json.dump(data, counter, ensure_ascii=False)
    except Exception:
        return None
    return counter.size


def _maybe_enforce_memory_limit(
    data: Any,
    fmt: str,
    *,
    memory_limit_bytes: int | None,
    source: str | None = None,
) -> None:
    """Best-effort guard for the internal memory_limit_bytes option.

    File inputs may be larger than this limit because they are streamed in
    batches. Python list inputs are already resident in memory, so we can reject
    obviously oversized payloads before native dispatch.
    """
    if memory_limit_bytes is None:
        return
    lim = memory_limit_bytes
    if lim <= 0:
        return

    actual: int | None = None
    if fmt == "python":
        actual = _estimate_python_payload_bytes(data)
    elif fmt == "xml" and source == "text":
        if isinstance(data, str):
            actual = len(data.encode("utf-8", "replace"))
        elif isinstance(data, (bytes, bytearray, memoryview)):
            actual = len(data)

    if actual is not None and actual > lim:
        raise SchemaSanitizerResourceError(
            f"memory_limit_bytes limit exceeded: {actual} bytes > {lim} bytes",
            detail={
                "stage": "xml_parse" if fmt == "xml" else "memory",
                "limit_name": "memory_limit_bytes",
                "limit_bytes": lim,
                "actual_bytes": actual,
            },
        )
