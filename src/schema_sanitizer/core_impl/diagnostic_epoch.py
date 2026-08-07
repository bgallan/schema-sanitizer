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
    """Publish one transition while preserving odd parity for active writers."""
    global _EPOCH
    _ensure_process()
    with _LOCK:
        _EPOCH += 2
        return _EPOCH


def diagnostic_write_begin() -> int:
    """Enter a multi-step mutation; concurrent writers preserve odd parity."""
    global _EPOCH, _ACTIVE_WRITERS
    _ensure_process()
    with _LOCK:
        if _ACTIVE_WRITERS == 0:
            _EPOCH += 1
        else:
            _EPOCH += 2
        _ACTIVE_WRITERS += 1
        return _EPOCH


def diagnostic_write_end() -> int:
    """Leave one writer; only the last writer publishes an even epoch."""
    global _EPOCH, _ACTIVE_WRITERS
    _ensure_process()
    with _LOCK:
        if _ACTIVE_WRITERS <= 0:
            raise RuntimeError("diagnostic seqlock writer is not active")
        _ACTIVE_WRITERS -= 1
        _EPOCH += 1 if _ACTIVE_WRITERS == 0 else 2
        return _EPOCH


@contextmanager
def diagnostic_write() -> Iterator[None]:
    """Context manager for a bounded multi-step diagnostic mutation."""
    diagnostic_write_begin()
    try:
        yield
    finally:
        diagnostic_write_end()


def diagnostic_epoch() -> int:
    """Return the current epoch without invoking callbacks or user code."""
    _ensure_process()
    with _LOCK:
        return _EPOCH


__all__ = [
    "diagnostic_epoch",
    "diagnostic_transition",
    "diagnostic_write",
    "diagnostic_write_begin",
    "diagnostic_write_end",
]
