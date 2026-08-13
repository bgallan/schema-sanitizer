"""Best-effort allocator RSS reclamation after large pressured operations."""

from __future__ import annotations

import ctypes
import os
import sys
from threading import Lock
from time import monotonic

_LOCK = Lock()
_LAST_TRIM = 0.0
_COOLDOWN_SECONDS = 30.0
_MIN_PEAK_BYTES = 256 << 20
_MIN_UNTRACKED_BYTES = 64 << 20


def _mode() -> str:
    """Implement the internal _mode helper."""
    return os.getenv("SCHEMA_SANITIZER_MALLOC_TRIM", "auto").strip().lower()


def maybe_trim_allocator(*, peak_bytes: int, untracked_rss_bytes: int | None) -> bool:
    """Invoke glibc ``malloc_trim`` only when bounded pressure signals justify it."""
    global _LAST_TRIM
    mode = _mode()
    if mode in {"0", "false", "no", "off"} or not sys.platform.startswith("linux"):
        return False
    from .system_pressure import system_pressure_snapshot

    pressured = system_pressure_snapshot().scale <= 0.5
    large_release = peak_bytes >= _MIN_PEAK_BYTES
    opaque_high = (untracked_rss_bytes or 0) >= _MIN_UNTRACKED_BYTES
    if mode not in {"1", "true", "yes", "on"} and not (
        pressured and (large_release or opaque_high)
    ):
        return False
    now = monotonic()
    with _LOCK:
        if now - _LAST_TRIM < _COOLDOWN_SECONDS:
            return False
        try:
            trim = ctypes.CDLL(None).malloc_trim
            trim.argtypes = [ctypes.c_size_t]
            trim.restype = ctypes.c_int
            trimmed = bool(trim(0))
        except (AttributeError, OSError):
            return False
        _LAST_TRIM = now
        return trimmed


def reset_after_fork() -> None:
    """Implement the internal reset_after_fork helper."""
    global _LOCK, _LAST_TRIM
    _LOCK = Lock()
    _LAST_TRIM = 0.0


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("allocator-control", mode="quarantine_only")


__all__ = ["maybe_trim_allocator"]
