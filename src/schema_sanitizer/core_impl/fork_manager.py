"""Single bounded at-fork dispatcher for runtime-owned state.

Modules register handlers during normal import. Only this module registers with
``os.register_at_fork``. The fixed registry prevents per-object callback leaks
and avoids container growth inside fork callbacks.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from threading import Lock
from typing import Callable

# Register the poison/quarantine bootstrap before the single runtime dispatcher.
from . import fork_safety as _fork_safety_bootstrap  # noqa: F401
from .callable_contract import callable_contract

_MAX_FORK_HANDLERS = 256


@dataclass(slots=True)
class _ForkHandler:
    name: str
    before: Callable[[], None] | None
    after_in_parent: Callable[[], None] | None
    after_in_child: Callable[[], None] | None
    contract_generation: int
    child_safe_without_prepare: bool
    mode: str


_LOCK = Lock()
_HANDLERS: list[_ForkHandler | None] = [None] * _MAX_FORK_HANDLERS
_COUNT = 0
# Exact fork generation captured by ``before``. These arrays are allocated at
# module import and only overwritten in-place inside at-fork callbacks. A handler
# registered while another handler's ``before`` runs therefore belongs to the
# *next* fork, never the in-flight child generation.
_FORK_GENERATION: list[_ForkHandler | None] = [None] * _MAX_FORK_HANDLERS
_FORK_PREPARED = bytearray(_MAX_FORK_HANDLERS)
_FORK_GENERATION_COUNT = 0
_FORK_GENERATION_ACTIVE = False


def _fork_callback_contract(callback: Callable[[], None] | None) -> tuple[object, ...] | None:
    """Fingerprint code/defaults/closures while treating a named bound owner as a role.

    The handler name + generation identifies the singleton role. Module reloads
    legitimately replace that role's instance, so object identity must not make
    an otherwise identical bound method look like a different contract. Opaque
    closure/default owners remain identity-sensitive through callable_contract.
    """
    contract = callable_contract(callback)
    if contract is None or callback is None or getattr(callback, "__self__", None) is None:
        return contract
    owner = getattr(callback, "__self__")
    mutable = list(contract)
    mutable[2] = ("fork-owner-role", type(owner).__module__, type(owner).__qualname__)
    return tuple(mutable)


def register_fork_handler(
    name: str,
    *,
    before: Callable[[], None] | None = None,
    after_in_parent: Callable[[], None] | None = None,
    after_in_child: Callable[[], None] | None = None,
    contract_generation: int = 1,
    child_safe_without_prepare: bool = False,
    mode: str | None = None,
) -> None:
    """Register or reload-replace one named runtime fork contract."""
    global _COUNT
    if type(name) is not str or not name:
        raise ValueError("fork handler name must be a non-empty string")
    for cb in (before, after_in_parent, after_in_child):
        if cb is not None and not callable(cb):
            raise TypeError("fork handler callbacks must be callable")
    if type(contract_generation) is not int or contract_generation <= 0:
        raise ValueError("fork handler contract_generation must be a positive exact integer")
    if type(child_safe_without_prepare) is not bool:
        raise TypeError("child_safe_without_prepare must be an exact bool")
    if mode is None:
        mode = (
            "prepared_swap"
            if before is not None
            else "child_safe"
            if child_safe_without_prepare
            else "quarantine_only"
        )
    if mode not in {"prepared_swap", "child_safe", "quarantine_only"}:
        raise ValueError("invalid fork handler mode")
    if (
        not any(cb is not None for cb in (before, after_in_parent, after_in_child))
        and mode != "quarantine_only"
    ):
        raise ValueError("fork handler requires at least one callback")
    if mode == "prepared_swap" and before is None:
        raise ValueError("prepared_swap fork handlers require a before callback")
    if mode == "child_safe" and not child_safe_without_prepare:
        raise ValueError("child_safe fork handlers must opt in explicitly")
    if mode == "quarantine_only" and after_in_child is not None:
        raise ValueError(
            "quarantine_only fork handlers cannot register an unreachable child callback"
        )
    if mode == "quarantine_only" and any(cb is not None for cb in (before, after_in_parent)):
        raise ValueError(
            "quarantine_only fork handlers are marker-only and cannot register callbacks"
        )
    new = _ForkHandler(
        name,
        before,
        after_in_parent,
        after_in_child,
        contract_generation,
        child_safe_without_prepare,
        mode,
    )
    with _LOCK:
        for index in range(_COUNT):
            old = _HANDLERS[index]
            if old is None or old.name != name:
                continue
            old_contract = (
                old.contract_generation,
                old.child_safe_without_prepare,
                old.mode,
                *(
                    _fork_callback_contract(x)
                    for x in (old.before, old.after_in_parent, old.after_in_child)
                ),
            )
            new_contract = (
                contract_generation,
                child_safe_without_prepare,
                mode,
                *(_fork_callback_contract(x) for x in (before, after_in_parent, after_in_child)),
            )
            if old_contract != new_contract:
                raise RuntimeError(f"fork handler {name!r} changed contract")
            _HANDLERS[index] = new
            return
        if _COUNT >= _MAX_FORK_HANDLERS:
            raise RuntimeError("fork handler registry capacity exhausted")
        _HANDLERS[_COUNT] = new
        _COUNT += 1


def _before() -> None:
    """Freeze one exact handler generation and prepare it in registration order.

    The runtime owns only two preallocated child-state generations.  Once a
    nested child is already beyond that one-shot bank depth, executing any
    prepared-swap callback could select state that was active in an ancestor.
    Skip the whole managed generation instead; fork_safety has already poisoned
    the runtime and the child must remain fail-closed.
    """
    global _FORK_GENERATION_COUNT, _FORK_GENERATION_ACTIVE
    if _fork_safety_bootstrap.fork_quarantine_generation() > 1:
        _FORK_GENERATION_COUNT = 0
        _FORK_GENERATION_ACTIVE = True
        return
    try:
        with _LOCK:
            count = _COUNT
            for index in range(count):
                _FORK_GENERATION[index] = _HANDLERS[index]
                _FORK_PREPARED[index] = 0
            _FORK_GENERATION_COUNT = count
            _FORK_GENERATION_ACTIVE = True
    except BaseException:
        # CPython cannot abort fork from a before callback. An empty generation
        # makes every runtime handler inert in the child, which is fail-closed.
        _FORK_GENERATION_COUNT = 0
        _FORK_GENERATION_ACTIVE = True
        return
    for index in range(count):
        handler = _FORK_GENERATION[index]
        if handler is None:
            continue
        callback = handler.before
        if callback is None:
            if handler.mode == "child_safe" and handler.child_safe_without_prepare:
                _FORK_PREPARED[index] = 1
            continue
        try:
            callback()
        except BaseException:
            # A failed prepare explicitly does *not* authorize child mutation.
            _FORK_PREPARED[index] = 0
            continue
        _FORK_PREPARED[index] = 1


def _parent() -> None:
    """Run cleanup only for the exact generation captured by ``before``."""
    global _FORK_GENERATION_COUNT, _FORK_GENERATION_ACTIVE
    count = _FORK_GENERATION_COUNT if _FORK_GENERATION_ACTIVE else 0
    for index in range(count - 1, -1, -1):
        handler = _FORK_GENERATION[index]
        if handler is None:
            continue
        callback = handler.after_in_parent
        if callback is not None:
            try:
                callback()
            except BaseException:
                pass
        _FORK_PREPARED[index] = 0
        _FORK_GENERATION[index] = None
    _FORK_GENERATION_COUNT = 0
    _FORK_GENERATION_ACTIVE = False


def _child() -> None:
    """Apply only successfully prepared handlers from the frozen generation."""
    global _FORK_GENERATION_COUNT, _FORK_GENERATION_ACTIVE
    count = _FORK_GENERATION_COUNT if _FORK_GENERATION_ACTIVE else 0
    for index in range(count):
        handler = _FORK_GENERATION[index]
        prepared = bool(_FORK_PREPARED[index])
        _FORK_PREPARED[index] = 0
        _FORK_GENERATION[index] = None
        if handler is None or not prepared:
            continue
        callback = handler.after_in_child
        if callback is None:
            continue
        try:
            callback()
        except BaseException:
            continue
    _FORK_GENERATION_COUNT = 0
    _FORK_GENERATION_ACTIVE = False


def fork_handler_contracts() -> tuple[tuple[str, str, int], ...]:
    """Return the bounded explicit classification of registered fork handlers."""
    with _LOCK:
        return tuple(
            (handler.name, handler.mode, handler.contract_generation)
            for handler in _HANDLERS[:_COUNT]
            if handler is not None
        )


if hasattr(os, "register_at_fork"):
    os.register_at_fork(before=_before, after_in_parent=_parent, after_in_child=_child)


__all__ = ["fork_handler_contracts", "register_fork_handler"]
