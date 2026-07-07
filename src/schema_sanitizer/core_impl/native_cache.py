"""Small cache helper for optional native extension functions."""

from __future__ import annotations

from typing import Any

_UNSET = object()


class NativeFunctionCache:
    """Lazily resolve and cache one native extension function."""

    def __init__(self, name: str):
        """Store the native function name to resolve on first use."""
        self._name = name
        self._value: Any = _UNSET

    def get(self) -> Any | None:
        """Return the native function, or None when the extension lacks it."""
        if self._value is _UNSET:
            try:
                from .native import _native
            except ImportError:
                self._value = None
            else:
                self._value = getattr(_native, self._name, None)
        return self._value
