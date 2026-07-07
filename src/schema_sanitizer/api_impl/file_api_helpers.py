"""Private helpers shared by public file API wrappers."""

from __future__ import annotations

from typing import Any


def _call_options_from_locals(values: dict[str, Any], excluded: frozenset[str]) -> dict[str, Any]:
    """Remove helper-only arguments from a public call's local values."""
    return {k: v for k, v in values.items() if k not in excluded}
