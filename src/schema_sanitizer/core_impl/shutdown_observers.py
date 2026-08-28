"""Registry of already-loaded terminal ownership observers.

Shutdown never imports an optional subsystem merely to ask whether it owns
resources.  Any subsystem capable of owning terminal state registers its exact
snapshot callback during normal import; the registry is frozen before teardown.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Callable

from .callable_contract import callable_contract
from .fork_safety import quarantine_inherited_state

_EMPTY_CLOSURE = object()


@dataclass(frozen=True, slots=True)
class ShutdownObserver:
    """Named snapshot callback for one terminal ownership subsystem."""

    name: str
    snapshot: Callable[[], object]


_MAX_SHUTDOWN_OBSERVERS = 128
_LOCK = Lock()
_FORK_LOCK_BANK = (Lock(), Lock())
_FORK_LOCK_BANK_INDEX = 0
_FORK_FRESH_LOCK: Lock | None = None
_OBSERVERS: dict[str, ShutdownObserver] = {}
_FROZEN: tuple[ShutdownObserver, ...] | None = None
_FROZEN_FLAG = False


def _same_callable_contract(left: Callable[..., object], right: Callable[..., object]) -> bool:
    return callable_contract(left) == callable_contract(right)


def register_shutdown_observer(name: str, snapshot: Callable[[], object]) -> None:
    """Register one reload-stable shutdown ownership observer."""
    global _FROZEN
    if type(name) is not str or not name or not callable(snapshot):
        raise ValueError("invalid shutdown observer")
    observer = ShutdownObserver(name, snapshot)
    with _LOCK:
        existing = _OBSERVERS.get(name)
        if existing is not None:
            if existing.snapshot is snapshot:
                return
            if not _same_callable_contract(existing.snapshot, snapshot):
                raise RuntimeError(f"shutdown observer {name!r} changed callback")
            if _FROZEN_FLAG:
                raise RuntimeError("shutdown observer registry is frozen")
            _OBSERVERS[name] = observer
            _FROZEN = None
            return
        if _FROZEN_FLAG:
            raise RuntimeError("shutdown observer registry is frozen")
        if len(_OBSERVERS) >= _MAX_SHUTDOWN_OBSERVERS:
            raise RuntimeError("shutdown observer registry capacity exhausted")
        _OBSERVERS[name] = observer
        _FROZEN = None


def freeze_shutdown_observers() -> tuple[ShutdownObserver, ...]:
    """Freeze registration and return the immutable observer sequence."""
    global _FROZEN, _FROZEN_FLAG
    with _LOCK:
        if _FROZEN is None:
            _FROZEN = tuple(_OBSERVERS.values())
        _FROZEN_FLAG = True
        return _FROZEN


def shutdown_observers() -> tuple[ShutdownObserver, ...]:
    """Return the frozen observer sequence or a detached live view."""
    frozen = _FROZEN
    if frozen is not None:
        return frozen
    with _LOCK:
        return tuple(_OBSERVERS.values())


def _reset_shutdown_observers_for_tests() -> None:
    global _FROZEN, _FROZEN_FLAG
    with _LOCK:
        _FROZEN = None
        _FROZEN_FLAG = False


def _prepare_fork() -> None:
    global _FORK_FRESH_LOCK
    _FORK_FRESH_LOCK = _FORK_LOCK_BANK[_FORK_LOCK_BANK_INDEX]


def _clear_fork() -> None:
    global _FORK_FRESH_LOCK
    _FORK_FRESH_LOCK = None


def _reset_fork() -> None:
    global _LOCK, _FORK_FRESH_LOCK, _FORK_LOCK_BANK_INDEX
    prepared = _FORK_FRESH_LOCK
    if prepared is None:
        return
    quarantine_inherited_state("shutdown-observers", _LOCK)
    _LOCK = prepared
    _FORK_FRESH_LOCK = None
    _FORK_LOCK_BANK_INDEX = 1 - _FORK_LOCK_BANK_INDEX


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "shutdown-observers",
    before=_prepare_fork,
    after_in_parent=_clear_fork,
    after_in_child=_reset_fork,
)

__all__ = [
    "ShutdownObserver",
    "freeze_shutdown_observers",
    "register_shutdown_observer",
    "shutdown_observers",
]
