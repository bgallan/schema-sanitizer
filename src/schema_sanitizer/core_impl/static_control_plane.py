"""Exact registration ledger for process-static concurrency control memory.

Components register conservative retained-byte charges while normal imports are
allowed.  The aggregate is read by the dynamic control-plane budget and frozen
at terminal shutdown, replacing the former anonymous fixed baseline constant.
"""

from __future__ import annotations

import sys
from threading import Lock

from .fork_manager import register_fork_handler
from .fork_safety import quarantine_inherited_state

_MAX_STATIC_CONTROL_KINDS = 256
_LOCK = Lock()
_FORK_LOCK_BANK = (Lock(), Lock())
_FORK_LOCK_BANK_INDEX = 0
_FORK_FRESH_LOCK: Lock | None = None
_ENTRIES: dict[str, int] = {}
_TOTAL = 0
_FROZEN = False
_CORRUPTED = False


def _authoritative_total_locked() -> int:
    """Recompute the bounded exact total from authoritative registrations."""
    total = 0
    for amount in _ENTRIES.values():
        total += amount
    return total


def _validate_total_locked() -> int:
    """Latch corruption if the aggregate cache disagrees with registrations."""
    global _CORRUPTED
    authoritative = _authoritative_total_locked()
    if type(_TOTAL) is not int or _TOTAL != authoritative:
        _CORRUPTED = True
    return authoritative


def _sync_dynamic_shadow_noexcept() -> None:
    module = sys.modules.get("schema_sanitizer.core_impl.control_plane_budget")
    sync = getattr(module, "try_synchronize_control_plane_native_shadow", None) if module else None
    if callable(sync):
        try:
            sync()
        except BaseException:
            pass


def reserve_static_control_plane(kind: str, amount: int) -> bool:
    """Pre-admit one exact static charge before component allocation.

    If the governed budget is loaded, publication and native-shadow charging
    share the global admission lock. The caller therefore cannot materialize
    the static component unless its conservative footprint is already excluded
    from payload headroom.
    """
    global _TOTAL
    if type(kind) is not str or not kind or type(amount) is not int or amount < 0:
        raise ValueError("invalid static control-plane registration")

    module = sys.modules.get("schema_sanitizer.core_impl.control_plane_budget")
    admission = getattr(module, "_GOVERNED_MEMORY_ADMISSION_LOCK", None) if module else None
    sync_locked = (
        getattr(module, "_synchronize_control_plane_native_shadow_under_admission_lock", None)
        if module
        else None
    )

    def publish() -> bool:
        global _TOTAL
        with _LOCK:
            authoritative = _validate_total_locked()
            if _CORRUPTED:
                raise RuntimeError(
                    "static control-plane registry is corrupted; admission is closed"
                )
            existing = _ENTRIES.get(kind)
            if existing is not None:
                if existing != amount:
                    raise RuntimeError(f"static control-plane registration changed for {kind!r}")
                return False
            if _FROZEN:
                raise RuntimeError("static control-plane registry is frozen")
            if len(_ENTRIES) >= _MAX_STATIC_CONTROL_KINDS:
                raise RuntimeError("static control-plane registry capacity exhausted")
            # pass50 ordering breadcrumb: next_total = _TOTAL + amount
            # Pass77 derives the transition from the authoritative bounded sum.
            next_total = authoritative + amount
            _ENTRIES[kind] = amount
            _TOTAL = next_total
            return True

    if admission is None or not callable(sync_locked):
        created = publish()
        _sync_dynamic_shadow_noexcept()
        return created

    with admission:
        created = publish()
        if not created:
            return False
        try:
            sync_locked()
        except BaseException:
            with _LOCK:
                authoritative = _validate_total_locked()
                if _ENTRIES.get(kind) == amount:
                    next_total = authoritative - amount
                    _ENTRIES.pop(kind, None)
                    _TOTAL = next_total
            try:
                sync_locked()
            except BaseException:
                pass
            raise
        return True


def rollback_static_control_plane(kind: str, amount: int) -> bool:
    """Rollback only an exact uncommitted static construction reservation."""
    global _TOTAL
    module = sys.modules.get("schema_sanitizer.core_impl.control_plane_budget")
    admission = getattr(module, "_GOVERNED_MEMORY_ADMISSION_LOCK", None) if module else None
    sync_locked = (
        getattr(module, "_synchronize_control_plane_native_shadow_under_admission_lock", None)
        if module
        else None
    )

    def retire() -> bool:
        global _TOTAL
        with _LOCK:
            authoritative = _validate_total_locked()
            if _FROZEN or _ENTRIES.get(kind) != amount:
                return False
            # The exact registration is the cleanup authority.  A corrupt
            # aggregate closes future reserve(), but must not strand an exact
            # failed-construction rollback or manufacture capacity from _TOTAL.
            # pass76 breadcrumb: if _TOTAL < amount was the former narrow guard.
            if authoritative < amount:
                return False
            next_total = authoritative - amount
            _ENTRIES.pop(kind, None)
            _TOTAL = next_total
            return True

    if admission is None or not callable(sync_locked):
        retired = retire()
        _sync_dynamic_shadow_noexcept()
        return retired
    with admission:
        retired = retire()
        if retired:
            try:
                sync_locked()
            except BaseException:
                # Rollback occurs only because construction itself failed. A
                # stale-high shadow is fail-closed and reconciles on admission.
                pass
        return retired


def register_static_control_plane(kind: str, amount: int) -> None:
    """Register a permanent control-plane allocation by stable category."""
    reserve_static_control_plane(kind, amount)


def static_control_plane_bytes() -> int:
    """Return validated permanent control-plane bytes."""
    with _LOCK:
        return _validate_total_locked()


def static_control_plane_entries() -> tuple[tuple[str, int], ...]:
    """Return a detached view of permanent control-plane categories."""
    with _LOCK:
        return tuple(_ENTRIES.items())


def freeze_static_control_plane() -> int:
    """Close static registration and return its authoritative byte total."""
    global _FROZEN
    with _LOCK:
        authoritative = _validate_total_locked()
        _FROZEN = True
        return authoritative


def _reset_static_control_plane_for_tests() -> None:
    """Reopen registration after an isolated in-process shutdown test."""
    global _FROZEN, _CORRUPTED, _TOTAL
    with _LOCK:
        _TOTAL = _authoritative_total_locked()
        _FROZEN = False
        _CORRUPTED = False


def _prepare_for_fork() -> None:
    global _FORK_FRESH_LOCK
    _FORK_FRESH_LOCK = _FORK_LOCK_BANK[_FORK_LOCK_BANK_INDEX]


def _clear_fork() -> None:
    global _FORK_FRESH_LOCK
    _FORK_FRESH_LOCK = None


def _reset_after_fork() -> None:
    global _LOCK, _FORK_FRESH_LOCK, _FORK_LOCK_BANK_INDEX
    prepared = _FORK_FRESH_LOCK
    if prepared is None:
        return
    quarantine_inherited_state("static-control-plane", _LOCK)
    _LOCK = prepared
    _FORK_FRESH_LOCK = None
    _FORK_LOCK_BANK_INDEX = 1 - _FORK_LOCK_BANK_INDEX


register_fork_handler(
    "static-control-plane",
    before=_prepare_for_fork,
    after_in_parent=_clear_fork,
    after_in_child=_reset_after_fork,
)

__all__ = [
    "freeze_static_control_plane",
    "register_static_control_plane",
    "reserve_static_control_plane",
    "rollback_static_control_plane",
    "static_control_plane_bytes",
    "static_control_plane_entries",
]
