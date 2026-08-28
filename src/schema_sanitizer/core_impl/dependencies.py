"""Load optional dependencies lazily and cache their availability.

Imports are translated into extra-specific installation errors, while PyArrow capability checks
avoid forcing optional packages on callers that do not use them.
"""

from __future__ import annotations

import importlib
import importlib.util
from functools import lru_cache
from typing import Any


@lru_cache(maxsize=32)
def _import_optional_dependency(import_name: str) -> Any:
    """Import and cache one optional dependency module."""
    return importlib.import_module(import_name)


def ensure_optional_dependency(
    import_name: str,
    *,
    extra: str,
    feature: str,
    dependency_name: str | None = None,
) -> Any:
    """Import an optional dependency or raise an installation hint."""
    try:
        return _import_optional_dependency(import_name)
    except Exception as exc:  # pragma: no cover - depends on local environment
        dependency = dependency_name or import_name
        hint = f"Install with: python -m pip install 'schema-sanitizer[{extra}]'"
        raise RuntimeError(
            f"{feature} requires optional dependency '{dependency}'. {hint}"
        ) from exc


def ensure_pyarrow(*, feature: str) -> Any:
    """Import PyArrow lazily and raise a feature-specific installation hint."""
    return ensure_optional_dependency(
        "pyarrow",
        extra="pyarrow",
        feature=feature,
        dependency_name="pyarrow",
    )


@lru_cache(maxsize=1)
def pyarrow_importable() -> bool:
    """Return whether PyArrow is importable in this process."""
    try:
        return importlib.util.find_spec("pyarrow") is not None
    except (ImportError, AttributeError, ValueError):
        return False
