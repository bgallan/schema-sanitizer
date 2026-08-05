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


@dataclass(frozen=True, slots=True)
class TerminalHostSnapshot:
    hosts: int
    capacity: int
    rejected: int
    circuit_open: bool


class TerminalHostMarkers:
    def __init__(self, capacity: int = 256) -> None:
        self._lock = Lock()
        self._entries: dict[int, tuple[int, weakref.ReferenceType[object] | None]] = {}
        self._sequence = 0
        self._capacity = max(1, int(capacity))
        self._rejected = 0
        self._circuit_open = False

    def add(self, owner: object) -> bool:
        owner_id = id(owner)
        with self._lock:
            existing = self._entries.get(owner_id)
            if existing is not None:
                _token, existing_ref = existing
                if existing_ref is None or existing_ref() is owner:
                    return True
                # ABA: a dead weak owner's address was reused. Retire only the
                # stale marker, never the new host.
                self._entries.pop(owner_id, None)
            if len(self._entries) >= self._capacity:
                self._rejected += 1
                self._circuit_open = True
                diagnostic_transition()
                return False
            try:
                owner_ref: weakref.ReferenceType[object] | None = weakref.ref(owner)
            except TypeError:
                owner_ref = None
            self._sequence += 1
            self._entries[owner_id] = (self._sequence, owner_ref)
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
                if not self._entries:
                    self._circuit_open = False
                diagnostic_transition()

    def snapshot(self) -> TerminalHostSnapshot:
        with self._lock:
            return TerminalHostSnapshot(
                len(self._entries), self._capacity, self._rejected, self._circuit_open
            )
