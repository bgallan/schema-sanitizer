"""Shared JSON payload parsing helpers."""

from __future__ import annotations

import json
from typing import Any


def json_object_loads(text: str) -> dict[str, Any]:
    """Parse a JSON object string, returning an empty mapping on invalid input."""
    if not text:
        return {}
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return obj if isinstance(obj, dict) else {}
