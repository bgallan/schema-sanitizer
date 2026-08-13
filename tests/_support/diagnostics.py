"""Assertions for diagnostics whose resource peaks are execution-mode dependent."""

from __future__ import annotations

import json
from typing import Any

_VOLATILE_RESOURCE_KEYS = {
    "current_charged_memory_bytes",
    "peak_charged_memory_bytes",
}


def comparable_diagnostics(raw: Any) -> dict[str, Any]:
    """Return deterministic diagnostics while retaining semantic resource counters."""
    payload = json.loads(raw.to_json())
    for key in _VOLATILE_RESOURCE_KEYS:
        payload.pop(key, None)
    return payload


def assert_diagnostics_semantically_equal(left: Any, right: Any) -> None:
    """Require equal counters except allocator samples that legitimately vary."""
    assert comparable_diagnostics(left) == comparable_diagnostics(right)
