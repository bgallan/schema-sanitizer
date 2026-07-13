"""Apply eager memory guards to already-materialized input payloads."""

from __future__ import annotations

import json
from typing import Any

from ...errors import SchemaSanitizerResourceError


class _Utf8ByteCounter:
    """Count UTF-8 bytes emitted by the JSON encoder."""

    def __init__(self) -> None:
        """Initialize an empty byte count."""
        self.size = 0

    def write(self, text: str) -> int:
        """Count one text fragment and report its character length."""
        self.size += len(text.encode("utf-8", "replace"))
        return len(text)


def estimate_python_payload_bytes(data: Any) -> int | None:
    """Return a best-effort byte estimate for Python-object ingestion."""
    counter = _Utf8ByteCounter()
    try:
        json.dump(data, counter, ensure_ascii=False)
    except Exception:
        return None
    return counter.size


def enforce_materialized_input_limit(
    data: Any,
    fmt: str,
    *,
    memory_limit_bytes: int | None,
    source: str | None = None,
) -> None:
    """Reject an already-materialized input when it exceeds its memory limit."""
    if memory_limit_bytes is None or memory_limit_bytes <= 0:
        return

    actual: int | None = None
    if fmt == "python":
        actual = estimate_python_payload_bytes(data)
    elif fmt == "xml" and source == "text":
        if isinstance(data, str):
            actual = len(data.encode("utf-8", "replace"))
        elif isinstance(data, (bytes, bytearray, memoryview)):
            actual = len(data)

    if actual is not None and actual > memory_limit_bytes:
        raise SchemaSanitizerResourceError(
            f"memory_limit_bytes limit exceeded: {actual} bytes > {memory_limit_bytes} bytes",
            detail={
                "stage": "xml_parse" if fmt == "xml" else "memory",
                "limit_name": "memory_limit_bytes",
                "limit_bytes": memory_limit_bytes,
                "actual_bytes": actual,
            },
        )
