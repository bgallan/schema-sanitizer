"""Shared JSON payload parsing helpers.

It accepts decoded JSON only when it is an object; blank, malformed, and other JSON values become
an empty mapping so diagnostic parsing never expands the failure surface.
"""

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
