"""Tiny process-wide seqlock epoch for cross-service diagnostics.

Completed transitions leave an even epoch.  Writers that need to expose a
multi-step mutation can use :func:`diagnostic_write` so readers reject the odd
in-flight epoch.  Legacy one-shot transition sites advance by two and therefore
remain completed writes.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager

_LOCK = threading.Lock()
_PID = os.getpid()
_EPOCH = 0
_MAX_EPOCH = (1 << 63) - 2
_ACTIVE_WRITERS = 0


def _ensure_process() -> None:
    global _PID, _EPOCH, _ACTIVE_WRITERS, _LOCK
    pid = os.getpid()
    if pid != _PID:
        # Never acquire an inherited lock in a post-fork child.
        _LOCK = threading.Lock()
        _PID = pid
        _EPOCH = 0
        _ACTIVE_WRITERS = 0


def diagnostic_transition() -> int:
    """Publish bounded telemetry without ever breaking an authoritative commit."""
    global _EPOCH
    try:
        _ensure_process()
        with _LOCK:
            current = _EPOCH
            if current <= _MAX_EPOCH - 2:
                try:
                    _EPOCH = current + 2
                except BaseException:
                    _EPOCH = _MAX_EPOCH
            else:
                _EPOCH = _MAX_EPOCH
            return _EPOCH
    except BaseException:
        return _EPOCH if isinstance(_EPOCH, int) else 0


def diagnostic_write_begin() -> int:
    """Best-effort enter of the diagnostic seqlock; never throws outward."""
    global _EPOCH, _ACTIVE_WRITERS
    try:
        _ensure_process()
        with _LOCK:
            current = _EPOCH
            writers = _ACTIVE_WRITERS
            try:
                _EPOCH = min(_MAX_EPOCH, current + (1 if writers == 0 else 2))
                _ACTIVE_WRITERS = min(_MAX_EPOCH, writers + 1)
            except BaseException:
                _EPOCH = _MAX_EPOCH
            return _EPOCH
    except BaseException:
        return _EPOCH if isinstance(_EPOCH, int) else 0


def diagnostic_write_end() -> int:
    """Best-effort leave of the diagnostic seqlock; never throws outward."""
    global _EPOCH, _ACTIVE_WRITERS
    try:
        _ensure_process()
        with _LOCK:
            writers = _ACTIVE_WRITERS
            if writers <= 0:
                return _EPOCH
            writers -= 1
            _ACTIVE_WRITERS = writers
            try:
                _EPOCH = min(_MAX_EPOCH, _EPOCH + (1 if writers == 0 else 2))
            except BaseException:
                _EPOCH = _MAX_EPOCH
            return _EPOCH
    except BaseException:
        return _EPOCH if isinstance(_EPOCH, int) else 0


@contextmanager
def diagnostic_write() -> Iterator[None]:
    """Context manager for a bounded multi-step diagnostic mutation."""
    diagnostic_write_begin()
    try:
        yield
    finally:
        diagnostic_write_end()


def diagnostic_epoch() -> int:
    """Return the current diagnostic epoch without perturbing authoritative state."""
    try:
        _ensure_process()
        with _LOCK:
            return _EPOCH
    except BaseException:
        # Diagnostics are never allowed to turn a successful ownership commit
        # into an apparent failure. Zero means "observation unavailable".
        return 0


__all__ = [
    "diagnostic_epoch",
    "diagnostic_transition",
    "diagnostic_write",
    "diagnostic_write_begin",
    "diagnostic_write_end",
]
