"""Shared helpers for PyArrow-backed adapters."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from ._optional import ensure_optional_dependency


@lru_cache(maxsize=1)
def _cached_pyarrow() -> Any:
    """Import pyarrow once for adapter fast paths."""
    return ensure_optional_dependency("pyarrow", extra="pyarrow", feature="pyarrow adapter")


def ensure_pyarrow(*, feature: str) -> Any:
    """Import pyarrow lazily and raise a clear error when unavailable."""
    try:
        return _cached_pyarrow()
    except RuntimeError as exc:
        hint = "Install with: python -m pip install 'schema-sanitizer[pyarrow]'"
        raise RuntimeError(f"{feature} requires optional dependency 'pyarrow'. {hint}") from exc
