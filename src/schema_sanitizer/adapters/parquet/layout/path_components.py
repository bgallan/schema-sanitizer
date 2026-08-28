"""Component-safe path handling for recursive Parquet layouts.

It validates component vectors, creates unambiguous diagnostic keys, and appends nested
path segments without string-splitting errors.
"""

from __future__ import annotations

import json
from typing import Any


def normalize_path_components(raw: Any) -> list[list[str]] | None:
    """Return normalized path-component vectors when diagnostics provide them."""
    if raw is None or not isinstance(raw, list):
        return None
    out: list[list[str]] = []
    for item in raw:
        if isinstance(item, (list, tuple)):
            out.append([str(part) for part in item])
        else:
            out.append([str(item)])
    return out


def path_components_key(components: list[str]) -> str:
    """Return a stable unambiguous key for a recursive path."""
    return json.dumps(components, ensure_ascii=False, separators=(",", ":"))


def component_fingerprint(components: list[list[str]] | None) -> str:
    """Return a stable fingerprint for an ordered component-path collection."""
    if components is None:
        return ""
    return "|".join(path_components_key(path) for path in components)


def is_component_prefix(prefix: list[str], path: list[str]) -> bool:
    """Return whether one component path is a prefix of another."""
    return len(prefix) <= len(path) and path[: len(prefix)] == prefix
