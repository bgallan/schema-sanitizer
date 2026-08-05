"""Apply eager memory guards to already-materialized input payloads."""

from __future__ import annotations

from typing import Any

from ...errors import SchemaSanitizerResourceError


def materialized_input_size_bytes(
    data: Any,
    fmt: str,
    *,
    source: str | None = None,
) -> int:
    """Return bytes retained by an already-materialized native text input."""
    if source != "text" or fmt == "python":
        return 0
    if isinstance(data, str):
        return len(data.encode("utf-8", "replace"))
    if isinstance(data, memoryview):
        return int(data.nbytes)
    if isinstance(data, (bytes, bytearray)):
        return len(data)
    return 0


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

    if fmt == "python":
        # Python row streams enforce the same limit during their first native
        # JSONL pass, avoiding a complete duplicate json.dump traversal here.
        return
    actual = materialized_input_size_bytes(data, fmt, source=source)

    if actual > memory_limit_bytes:
        raise SchemaSanitizerResourceError(
            f"memory_limit_bytes limit exceeded: {actual} bytes > {memory_limit_bytes} bytes",
            detail={
                "stage": "xml_parse" if fmt == "xml" else "memory",
                "limit_name": "memory_limit_bytes",
                "limit_bytes": memory_limit_bytes,
                "actual_bytes": actual,
            },
        )
