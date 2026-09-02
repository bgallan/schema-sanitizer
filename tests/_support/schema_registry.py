"""Normalize schema-registry diagnostics for deterministic assertions.

The helper validates native detection timestamps before removing them from otherwise exact drift
comparisons.
"""

from __future__ import annotations

import re

UTC_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")


def without_detected_at(drifts: list[dict[str, object]]) -> list[dict[str, object]]:
    """Validate and remove native conversion timestamps for stable comparisons."""
    normalized = []
    for drift in drifts:
        detected_at = drift.get("detected_at")
        assert isinstance(detected_at, str)
        assert UTC_TIMESTAMP_RE.fullmatch(detected_at)
        item = dict(drift)
        item.pop("detected_at")
        normalized.append(item)
    return normalized
