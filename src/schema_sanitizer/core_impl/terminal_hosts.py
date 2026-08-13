"""Bounded diagnostics for terminal runtime hosts.

Ownership is held by the strong runtime-service control block.  This registry
stores only integer identities, so diagnostics cannot become a second unbounded
owner graph.
"""

from __future__ import annotations

import weakref
from dataclasses import dataclass
from threading import Lock

from .diagnostic_epoch import diagnostic_transition
from .terminal_ownership import publish_terminal_owner, retire_terminal_owner


@dataclass(frozen=True, slots=True)
class TerminalHostSnapshot:
    hosts: int
    capacity: int
    rejected: int
    circuit_open: bool
    dead_pruned: int = 0


class TerminalHostMarkers:
    def __init__(self, capacity: int = 256, *, category: str = "terminal_host") -> None:
        if type(capacity) is not int:
            raise TypeError("terminal host capacity must be an exact integer")
        if capacity <= 0:
            raise ValueError("terminal host capacity must be > 0")
        if type(category) is not str:
            raise TypeError("terminal host category must be an exact string")
        self._category = category[:64]
        self._lock = Lock()
        self._entries: dict[int, tuple[int, weakref.ReferenceType[object] | None]] = {}
        self._sequence = 0
        self._capacity = capacity
        self._rejected = 0
        self._circuit_open = False
        self._dead_pruned = 0

    def _prune_dead_locked(self) -> int:
        """Retire dead weak owners before they consume bounded capacity."""
        dead = tuple(
            owner_id
            for owner_id, (_token, owner_ref) in self._entries.items()
            if owner_ref is not None and owner_ref() is None
        )
        if not dead:
            return 0
        for owner_id in dead:
            self._entries.pop(owner_id, None)
            retire_terminal_owner(self._category, owner_id)
        self._dead_pruned += len(dead)
        if len(self._entries) < self._capacity:
            self._circuit_open = False
        diagnostic_transition()
        return len(dead)

    def add(self, owner: object) -> bool:
        owner_id = id(owner)
        with self._lock:
            self._prune_dead_locked()
            existing = self._entries.get(owner_id)
            if existing is not None:
                _token, existing_ref = existing
                if existing_ref is None or existing_ref() is owner:
                    return True
                # ABA: a dead weak owner's address was reused. Retire only the
                # stale marker, never the new host.
                self._entries.pop(owner_id, None)
                retire_terminal_owner(self._category, owner_id)
            if len(self._entries) >= self._capacity:
                self._rejected += 1
                self._circuit_open = True
                diagnostic_transition()
                return False
            try:
                owner_ref: weakref.ReferenceType[object] | None = weakref.ref(owner)
            except TypeError:
                owner_ref = None
            # This token is diagnostic only; owner_id + weakref are authoritative.
            # Keep it fixed-width and reusable rather than lifetime-monotonic.
            self._sequence = 1 if self._sequence >= (1 << 63) - 1 else self._sequence + 1
            self._entries[owner_id] = (self._sequence, owner_ref)
            if not publish_terminal_owner(self._category, owner_id, retained_bytes=0):
                self._entries.pop(owner_id, None)
                self._rejected += 1
                self._circuit_open = True
                diagnostic_transition()
                return False
            diagnostic_transition()
            return True

    def discard(self, owner: object) -> None:
        with self._lock:
            owner_id = id(owner)
            existing = self._entries.get(owner_id)
            if existing is not None:
                _token, owner_ref = existing
                if owner_ref is not None and owner_ref() is not owner:
                    return
                self._entries.pop(owner_id, None)
                retire_terminal_owner(self._category, owner_id)
                if not self._entries:
                    self._circuit_open = False
                diagnostic_transition()

    def snapshot(self) -> TerminalHostSnapshot:
        with self._lock:
            self._prune_dead_locked()
            return TerminalHostSnapshot(
                len(self._entries),
                self._capacity,
                self._rejected,
                self._circuit_open,
                self._dead_pruned,
            )
