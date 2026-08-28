"""Fail-fast protection against reusing the initialized runtime after fork.

The child-side quarantine is deliberately preallocated.  ``after_in_child``
callbacks can therefore root inherited graphs without growing dicts/lists or
creating replacement locks while the child contains locks formerly owned by
threads that no longer exist.
"""

from __future__ import annotations

import os
from threading import Lock

_INITIAL_PID = os.getpid()
_FORKED_CHILD = False
_FORK_GENERATION = 0
_LOCK = Lock()
_CHILD_LOCK_BANK = (Lock(), Lock())
_CHILD_LOCK_BANK_INDEX = 0
_PREPARED_CHILD_LOCK: Lock | None = None
_MAX_FORK_CAPSULE_ENTRIES = 64
_MAX_FORK_QUARANTINE_GENERATIONS = 2
_MAX_INLINE_OWNERS = 8
_FORK_LABELS: list[str | None] = [None] * (
    _MAX_FORK_CAPSULE_ENTRIES * _MAX_FORK_QUARANTINE_GENERATIONS
)
_FORK_OWNERS: list[object | None] = [None] * (
    _MAX_FORK_CAPSULE_ENTRIES * _MAX_FORK_QUARANTINE_GENERATIONS * _MAX_INLINE_OWNERS
)
# Each nested child gets a one-shot slab. Generation-2 quarantine can therefore
# root generation-1 banks before swapping to B without deduplicating by label.
_FORK_CAPSULE_COUNTS = [0, 0]
_FORK_CAPSULE_COUNT = 0
_REJECTED_FORK_CAPSULE_ENTRIES = 0
_REJECTED_FORK_CAPSULE_OVERFLOWED = False


def _prepare_fork_child_state() -> None:
    """Select one unused replacement lock preallocated during normal runtime."""
    global _PREPARED_CHILD_LOCK
    if _FORK_GENERATION > 1:
        # A/B are the only preallocated generations. A third nested fork would
        # recycle an ancestor-active bank; keep the bootstrap inert instead.
        _PREPARED_CHILD_LOCK = None
        return
    _PREPARED_CHILD_LOCK = _CHILD_LOCK_BANK[_CHILD_LOCK_BANK_INDEX]


def _clear_parent_fork_preparation() -> None:
    global _PREPARED_CHILD_LOCK
    _PREPARED_CHILD_LOCK = None


def _mark_forked_child() -> None:
    """Poison inherited runtime state using only preallocated child objects."""
    global _FORKED_CHILD, _FORK_GENERATION, _LOCK, _PREPARED_CHILD_LOCK
    global _CHILD_LOCK_BANK_INDEX
    _FORKED_CHILD = True
    # More than one unsupported fork generation is intentionally collapsed.
    _FORK_GENERATION = 1 if _FORK_GENERATION == 0 else 2
    prepared = _PREPARED_CHILD_LOCK
    if prepared is not None:
        _LOCK = prepared
        _CHILD_LOCK_BANK_INDEX = 1 - _CHILD_LOCK_BANK_INDEX
    _PREPARED_CHILD_LOCK = None


def fork_quarantine_generation() -> int:
    """Return the number of unsupported fork generations in this child."""
    return int(_FORK_GENERATION)


def runtime_fork_poisoned() -> bool:
    """Return whether this interpreter is an unsupported post-fork child."""
    return bool(_FORKED_CHILD or os.getpid() != _INITIAL_PID)


def quarantine_inherited_state(
    label: str,
    owner1: object | None = None,
    owner2: object | None = None,
    owner3: object | None = None,
    owner4: object | None = None,
    owner5: object | None = None,
    owner6: object | None = None,
    owner7: object | None = None,
    owner8: object | None = None,
) -> bool:
    """Root inherited graphs in a bounded preallocated child quarantine.

    Up to eight references are stored directly into arrays allocated at import
    time, so the child callback never has to grow a container.
    """
    global _FORK_CAPSULE_COUNT, _REJECTED_FORK_CAPSULE_ENTRIES
    global _REJECTED_FORK_CAPSULE_OVERFLOWED
    if _FORK_GENERATION < 1 or _FORK_GENERATION > _MAX_FORK_QUARANTINE_GENERATIONS:
        return False
    if type(label) is not str or not label:
        return False
    generation_index = _FORK_GENERATION - 1
    generation_count = _FORK_CAPSULE_COUNTS[generation_index]
    generation_base = generation_index * _MAX_FORK_CAPSULE_ENTRIES
    for offset in range(generation_count):
        if _FORK_LABELS[generation_base + offset] == label:
            return True
    if generation_count >= _MAX_FORK_CAPSULE_ENTRIES:
        _REJECTED_FORK_CAPSULE_OVERFLOWED = True
        try:
            _REJECTED_FORK_CAPSULE_ENTRIES += 1
        except MemoryError:
            pass
        return False

    index = generation_base + generation_count
    base = index * _MAX_INLINE_OWNERS
    # Assign into the generation's preallocated slab; generation-2 uses a
    # physically distinct range even when handler labels repeat.
    _FORK_LABELS[index] = label
    _FORK_OWNERS[base] = owner1
    _FORK_OWNERS[base + 1] = owner2
    _FORK_OWNERS[base + 2] = owner3
    _FORK_OWNERS[base + 3] = owner4
    _FORK_OWNERS[base + 4] = owner5
    _FORK_OWNERS[base + 5] = owner6
    _FORK_OWNERS[base + 6] = owner7
    _FORK_OWNERS[base + 7] = owner8
    _FORK_CAPSULE_COUNTS[generation_index] = generation_count + 1
    _FORK_CAPSULE_COUNT += 1
    return True


def fork_inherited_capsule_snapshot() -> dict[str, int]:
    """Return bounded diagnostics for state quarantined after a fork."""
    rejected = (
        max(1, _REJECTED_FORK_CAPSULE_ENTRIES)
        if _REJECTED_FORK_CAPSULE_OVERFLOWED
        else _REJECTED_FORK_CAPSULE_ENTRIES
    )
    return {
        "entries": _FORK_CAPSULE_COUNT,
        "capacity": _MAX_FORK_CAPSULE_ENTRIES * _MAX_FORK_QUARANTINE_GENERATIONS,
        "rejected": rejected,
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
    os.register_at_fork(
        before=_prepare_fork_child_state,
        after_in_parent=_clear_parent_fork_preparation,
        after_in_child=_mark_forked_child,
    )


__all__ = [
    "ensure_runtime_fork_safe",
    "fork_inherited_capsule_snapshot",
    "fork_quarantine_generation",
    "quarantine_inherited_state",
    "runtime_fork_poisoned",
]
