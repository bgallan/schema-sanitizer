"""Shared optional dependency helpers for internal adapters."""

from __future__ import annotations

import importlib
from functools import lru_cache
from typing import Any


@lru_cache(maxsize=32)
def _import_optional_dependency(import_name: str) -> Any:
    """Import and cache one optional dependency module."""
    return importlib.import_module(import_name)


def ensure_optional_dependency(
    import_name: str, *, extra: str, feature: str, dependency_name: str | None = None
) -> Any:
    """Import an optional dependency or raise an installation hint."""
    try:
        return _import_optional_dependency(import_name)
    except Exception as e:  # pragma: no cover
        dependency = dependency_name or import_name
        hint = f"Install with: python -m pip install 'schema-sanitizer[{extra}]'"
        raise RuntimeError(f"{feature} requires optional dependency '{dependency}'. {hint}") from e
