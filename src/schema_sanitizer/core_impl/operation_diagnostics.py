"""Bounded per-operation diagnostics for concurrent runtime observability."""

from __future__ import annotations

import hashlib
import os
import weakref
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from threading import Lock
from time import time
from typing import Any, Callable

from .fork_safety import quarantine_inherited_state
from .safe_errors import safe_exception_summary

_MAX_LIVE = 4096
_MAX_COMPLETED = 128
_MAX_DIAGNOSTIC_ERROR_CHARS = 512
_MAX_DIAGNOSTIC_DEPTH = 10
_MAX_DIAGNOSTIC_CONTAINER_ITEMS = 128
_MAX_DIAGNOSTIC_TOTAL_NODES = 512
_MAX_DIAGNOSTIC_TEXT_CHARS = 256
_MAX_DIAGNOSTIC_INTEGER_BITS = 4096
_MAX_RETAINED_OPERATION_ID_CHARS = 256
_OPERATION_ID_HASH_CHUNK_CHARS = 4096
_FORKED_DIAGNOSTICS_KEEPALIVE: list[object] = []
_LOCK = Lock()
_LIVE: dict[str, weakref.WeakMethod[Any]] = {}
_COMPLETED: deque[dict[str, Any]] = deque(maxlen=_MAX_COMPLETED)
_LIVE_REGISTRATION_REJECTIONS = 0


@dataclass(frozen=True, slots=True)
class OperationDiagnosticRegistrySnapshot:
    """Bounded process-wide operation-registry pressure diagnostics."""

    live_entries: int
    live_capacity: int
    completed_entries: int
    completed_capacity: int
    registration_rejections: int


@dataclass(slots=True)
class _DiagnosticCloneBudget:
    """Mutable bounds applied while copying one diagnostic snapshot."""

    remaining_nodes: int = _MAX_DIAGNOSTIC_TOTAL_NODES
    truncated: bool = False


def _bounded_text(value: str, *, limit: int = _MAX_DIAGNOSTIC_TEXT_CHARS) -> str:
    """Return text without retaining an arbitrarily large backing value."""
    if len(value) <= limit:
        return value
    return f"{value[:limit]}...<truncated:{len(value) - limit}>"


def _bounded_diagnostic_value(
    value: Any,
    *,
    depth: int,
    budget: _DiagnosticCloneBudget,
    active_containers: set[int],
) -> Any:
    """Clone JSON-like diagnostics under depth, item, text, and node limits."""
    if budget.remaining_nodes <= 0:
        budget.truncated = True
        return "<diagnostic-node-limit>"
    budget.remaining_nodes -= 1

    if value is None:
        return None
    if type(value) is bool:
        return value
    if type(value) is float:
        return value
    if type(value) is int:
        if value.bit_length() <= _MAX_DIAGNOSTIC_INTEGER_BITS:
            return value
        budget.truncated = True
        return f"<integer:{value.bit_length()}-bits>"
    if type(value) is str:
        if len(value) > _MAX_DIAGNOSTIC_TEXT_CHARS:
            budget.truncated = True
        return _bounded_text(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        size = len(value)
        preview = bytes(value[: min(size, 32)]).hex()
        budget.truncated |= size > 32
        return f"<bytes:{size}:{preview}{'...' if size > 32 else ''}>"
    if depth >= _MAX_DIAGNOSTIC_DEPTH:
        budget.truncated = True
        return "<diagnostic-depth-limit>"

    if isinstance(value, dict):
        identity = id(value)
        if identity in active_containers:
            budget.truncated = True
            return "<diagnostic-cycle>"
        active_containers.add(identity)
        try:
            out: dict[str, Any] = {}
            for index, (key, item) in enumerate(value.items()):
                if index >= _MAX_DIAGNOSTIC_CONTAINER_ITEMS:
                    budget.truncated = True
                    out["<truncated-items>"] = max(0, len(value) - index)
                    break
                if type(key) is str:
                    key_text = key
                elif type(key) in (bool, int, float):
                    key_text = str(key)
                else:
                    key_type = type(key)
                    key_text = f"<{key_type.__module__}.{key_type.__qualname__}>"
                    budget.truncated = True
                bounded_key = _bounded_text(key_text, limit=128)
                if len(key_text) > 128:
                    budget.truncated = True
                out[bounded_key] = _bounded_diagnostic_value(
                    item,
                    depth=depth + 1,
                    budget=budget,
                    active_containers=active_containers,
                )
                if budget.remaining_nodes <= 0:
                    break
            return out
        finally:
            active_containers.remove(identity)

    if isinstance(value, (list, tuple, set, frozenset)):
        identity = id(value)
        if identity in active_containers:
            budget.truncated = True
            return "<diagnostic-cycle>"
        active_containers.add(identity)
        try:
            sequence_out: list[Any] = []
            for index, item in enumerate(value):
                if index >= _MAX_DIAGNOSTIC_CONTAINER_ITEMS:
                    budget.truncated = True
                    sequence_out.append(f"<truncated-items:{max(0, len(value) - index)}>")
                    break
                sequence_out.append(
                    _bounded_diagnostic_value(
                        item,
                        depth=depth + 1,
                        budget=budget,
                        active_containers=active_containers,
                    )
                )
                if budget.remaining_nodes <= 0:
                    break
            return sequence_out
        finally:
            active_containers.remove(identity)

    budget.truncated = True
    value_type = type(value)
    return _bounded_text(f"<{value_type.__module__}.{value_type.__qualname__}>")


def _bounded_diagnostic_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Return an ownership-independent, bounded diagnostic mapping."""
    budget = _DiagnosticCloneBudget()
    cloned = _bounded_diagnostic_value(snapshot, depth=0, budget=budget, active_containers=set())
    payload = cloned if isinstance(cloned, dict) else {"value": cloned}
    if budget.truncated:
        payload["diagnostic_payload_truncated"] = True
    return payload


def _bounded_operation_id(operation_id: object) -> str:
    """Return a stable bounded identity for diagnostic registry retention."""
    text = str(operation_id)
    if len(text) <= _MAX_RETAINED_OPERATION_ID_CHARS:
        return text
    digest = hashlib.blake2b(digest_size=16)
    for offset in range(0, len(text), _OPERATION_ID_HASH_CHUNK_CHARS):
        digest.update(
            text[offset : offset + _OPERATION_ID_HASH_CHUNK_CHARS].encode(
                "utf-8", errors="surrogatepass"
            )
        )
    return f"long-operation:{len(text)}:{digest.hexdigest()}"


def _remove_dead_registration(
    operation_id: str,
    dead_method: weakref.WeakMethod[Any],
) -> None:
    """Remove one dead weak method without deleting a newer same-key registration."""
    with _LOCK:
        if _LIVE.get(operation_id) is dead_method:
            _LIVE.pop(operation_id, None)


def register_operation(operation_id: str, snapshotter: Callable[[], dict[str, Any]]) -> None:
    """Register one live operation without retaining its resource domain."""
    global _LIVE_REGISTRATION_REJECTIONS
    key = _bounded_operation_id(operation_id)

    def remove(dead_method: weakref.WeakMethod[Any]) -> None:
        """Drop the registry key as soon as its resource domain is collected."""
        _remove_dead_registration(key, dead_method)

    weak_method = weakref.WeakMethod(snapshotter, remove)
    with _LOCK:
        if key not in _LIVE and len(_LIVE) >= _MAX_LIVE:
            stale = [registered for registered, reference in _LIVE.items() if reference() is None]
            for registered in stale:
                _LIVE.pop(registered, None)
        if key not in _LIVE and len(_LIVE) >= _MAX_LIVE:
            _LIVE_REGISTRATION_REJECTIONS += 1
            return
        _LIVE[key] = weak_method


def complete_operation(operation_id: str, final_snapshot: dict[str, Any]) -> None:
    """Move one live operation into the bounded completed ring."""
    key = _bounded_operation_id(operation_id)
    payload = _bounded_diagnostic_snapshot(final_snapshot)
    payload["operation_id"] = key
    payload.setdefault("completed_at", time())
    with _LOCK:
        _LIVE.pop(key, None)
        _COMPLETED.append(payload)


def operation_diagnostic_registry_snapshot() -> OperationDiagnosticRegistrySnapshot:
    """Return bounded registry usage without invoking operation snapshotters."""
    with _LOCK:
        return OperationDiagnosticRegistrySnapshot(
            live_entries=len(_LIVE),
            live_capacity=_MAX_LIVE,
            completed_entries=len(_COMPLETED),
            completed_capacity=_MAX_COMPLETED,
            registration_rejections=_LIVE_REGISTRATION_REJECTIONS,
        )


def process_operation_diagnostics(
    operation_id: str | None = None,
) -> tuple[dict[str, Any], ...]:
    """Return immutable copies for live and recently completed operations."""
    key = None if operation_id is None else _bounded_operation_id(operation_id)
    with _LOCK:
        if key is None:
            live_items = list(_LIVE.items())
            completed_items = tuple(_COMPLETED)
        else:
            reference = _LIVE.get(key)
            live_items = [] if reference is None else [(key, reference)]
            completed_items = tuple(item for item in _COMPLETED if item.get("operation_id") == key)
    # Completed payloads contain only bounded primitive containers. Clone them
    # after releasing the registry lock so readers cannot block completion of
    # unrelated operations while copying the retained history.
    completed = [deepcopy(item) for item in completed_items]
    live: list[dict[str, Any]] = []
    stale: list[tuple[str, weakref.WeakMethod[Any]]] = []
    for registered, weak_method in live_items:
        method = weak_method()
        if method is None:
            stale.append((registered, weak_method))
            continue
        try:
            payload = _bounded_diagnostic_snapshot(dict(method()))
            payload["operation_id"] = _bounded_operation_id(payload.get("operation_id", registered))
            live.append(payload)
        except Exception as exc:
            error = safe_exception_summary(exc, max_chars=_MAX_DIAGNOSTIC_ERROR_CHARS)
            live.append(
                {
                    "operation_id": registered,
                    "state": "diagnostic_error",
                    "error": error,
                }
            )
    for registered, weak_method in stale:
        _remove_dead_registration(registered, weak_method)
    return tuple(live + completed)


def _reset_after_fork() -> None:
    """Discard inherited diagnostic locks and retained operation state."""
    global _LOCK, _LIVE, _COMPLETED, _LIVE_REGISTRATION_REJECTIONS
    quarantine_inherited_state("operation-diagnostics", _LIVE, _COMPLETED)
    _LOCK = Lock()
    _LIVE = {}
    _COMPLETED = deque(maxlen=_MAX_COMPLETED)
    _LIVE_REGISTRATION_REJECTIONS = 0


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


__all__ = [
    "OperationDiagnosticRegistrySnapshot",
    "complete_operation",
    "operation_diagnostic_registry_snapshot",
    "process_operation_diagnostics",
    "register_operation",
]
