"""Fail-fast protection against reusing the initialized runtime after fork."""

from __future__ import annotations

import os
from threading import Lock

_INITIAL_PID = os.getpid()
_FORKED_CHILD = False
_FORK_GENERATION = 0
_LOCK = Lock()
_FORK_INHERITED_CAPSULE: dict[str, tuple[object, ...]] = {}
_MAX_FORK_CAPSULE_ENTRIES = 64
_REJECTED_FORK_CAPSULE_ENTRIES = 0


def _mark_forked_child() -> None:
    """Implement the internal _mark_forked_child helper."""
    global _FORKED_CHILD, _FORK_GENERATION, _LOCK
    _FORKED_CHILD = True
    _FORK_GENERATION += 1
    _LOCK = Lock()


def fork_quarantine_generation() -> int:
    """Return the number of unsupported fork generations in this child."""
    return int(_FORK_GENERATION)


def runtime_fork_poisoned() -> bool:
    """Return whether this interpreter is an unsupported post-fork child."""
    return bool(_FORKED_CHILD or os.getpid() != _INITIAL_PID)


def quarantine_inherited_state(label: str, *owners: object) -> bool:
    """Retain one opaque inherited graph in the single bounded child capsule."""
    global _REJECTED_FORK_CAPSULE_ENTRIES
    if _FORK_GENERATION > 1:
        return False
    if type(label) is not str or not label:
        return False
    if label in _FORK_INHERITED_CAPSULE:
        return True
    if len(_FORK_INHERITED_CAPSULE) >= _MAX_FORK_CAPSULE_ENTRIES:
        _REJECTED_FORK_CAPSULE_ENTRIES += 1
        return False
    _FORK_INHERITED_CAPSULE[label] = tuple(owners)
    return True


def fork_inherited_capsule_snapshot() -> dict[str, int]:
    """Return bounded diagnostics for state quarantined after a fork."""
    return {
        "entries": len(_FORK_INHERITED_CAPSULE),
        "capacity": _MAX_FORK_CAPSULE_ENTRIES,
        "rejected": _REJECTED_FORK_CAPSULE_ENTRIES,
        "generation": _FORK_GENERATION,
    }


def ensure_runtime_fork_safe() -> None:
    """Reject inherited native/runtime state instead of risking a child deadlock."""
    if not runtime_fork_poisoned():
        return
    raise RuntimeError(
        "schema-sanitizer runtime cannot be reused after fork; use the "
        "'spawn' or 'forkserver' multiprocessing start method, or exec a fresh process"
    )


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_mark_forked_child)


__all__ = [
    "ensure_runtime_fork_safe",
    "fork_inherited_capsule_snapshot",
    "fork_quarantine_generation",
    "quarantine_inherited_state",
    "runtime_fork_poisoned",
]
