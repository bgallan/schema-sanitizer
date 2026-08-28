"""Fixed-capacity reusable slot+generation namespaces.

Admissions are rooted by owner identity before their bounded ABA-resistant
token is returned. Derived counters can therefore be reconstructed after an
interrupted Python bytecode sequence without relying on a naked integer token.
"""

from __future__ import annotations

from dataclasses import dataclass

_MAX_TOKEN = (1 << 63) - 1
_FREE = 0
_ACTIVE = 1
_RETIRED = 2
_RETIRING = 3


@dataclass(frozen=True, slots=True)
class BoundedGenerationSnapshot:
    """Report exact slot usage for one bounded generation namespace."""

    capacity: int
    active: int
    available: int
    retired: int
    corrupted: bool = False


class BoundedGenerationPool:
    """Allocate ABA-resistant tokens backed by exact rooted owner identities."""

    __slots__ = (
        "_capacity",
        "_slot_bits",
        "_slot_mask",
        "_max_generation",
        "_count",
        "_generations",
        "_states",
        "_owners",
        "_active",
        "_retired",
        "_corrupted",
    )

    def __init__(self, capacity: int) -> None:
        if type(capacity) is not int or capacity <= 0:
            raise ValueError("bounded generation capacity must be a positive exact integer")
        self._capacity = capacity
        self._slot_bits = max(1, (capacity - 1).bit_length())
        self._slot_mask = (1 << self._slot_bits) - 1
        self._max_generation = _MAX_TOKEN >> self._slot_bits
        self._count = capacity
        self._generations = [0] * capacity
        self._states = bytearray(capacity)
        self._owners: list[object | None] = [None] * capacity
        self._active = 0
        self._retired = 0
        self._corrupted = False

    def _encode(self, slot: int, generation: int) -> int:
        return (generation << self._slot_bits) | slot

    def _slot_for_owner(self, owner: object) -> int:
        for slot in range(self._capacity):
            if self._owners[slot] is owner:
                return slot
        return -1

    def _rebuild_derived_from_owners(self) -> None:
        """Reconstruct state and counters from exact slot ownership.

        This operation is intentionally idempotent.  If an asynchronous
        exception interrupts the rebuild, the next owner-aware operation simply
        runs it again; exact ``_owners`` entries remain the authority.
        """
        available = 0
        active = 0
        retired = 0
        for slot in range(self._capacity):
            owner = self._owners[slot]
            generation = self._generations[slot]
            if owner is not None:
                self._states[slot] = _ACTIVE
                active += 1
                continue
            if generation >= self._max_generation or self._states[slot] == _RETIRED:
                self._states[slot] = _RETIRED
                retired += 1
                continue
            self._states[slot] = _FREE
            available += 1
        self._count = available
        self._active = active
        self._retired = retired

    def acquire_for(self, owner: object) -> int | None:
        """Reserve one exact generation for an already-created *owner*.

        A caller can always roll back with ``release_for(owner)`` even if an
        exception lands before the returned token is stored.  Internal partial
        publication is likewise recoverable because owner identity is written
        before any derived state or counter.
        """
        if owner is None:
            raise ValueError("bounded generation owner must not be None")
        if self._corrupted:
            return None
        # Repair any interrupted derived bookkeeping before new admission.
        self._rebuild_derived_from_owners()
        if self._slot_for_owner(owner) >= 0:
            raise RuntimeError("bounded generation owner already has a live slot")

        slot = -1
        for candidate in range(self._capacity):
            if self._owners[candidate] is not None:
                continue
            if self._states[candidate] == _RETIRED:
                continue
            generation = self._generations[candidate]
            if generation >= self._max_generation:
                self._states[candidate] = _RETIRED
                continue
            slot = candidate
            break
        if slot < 0:
            self._rebuild_derived_from_owners()
            return None

        generation = self._generations[slot] + 1
        token = self._encode(slot, generation)
        committed_owner = False
        try:
            # Exact authority first.  Any exception from this point is
            # recoverable by identity, without the integer handoff.
            self._owners[slot] = owner
            committed_owner = True
            self._generations[slot] = generation
            self._states[slot] = _ACTIVE
            self._rebuild_derived_from_owners()
            return token
        except BaseException:
            if committed_owner:
                try:
                    if self._owners[slot] is owner:
                        self._owners[slot] = None
                    if self._generations[slot] < self._max_generation:
                        self._states[slot] = _FREE
                    self._rebuild_derived_from_owners()
                except BaseException:
                    # Exact owner identity remains inspectable/recoverable even
                    # if secondary rollback bookkeeping is itself interrupted.
                    pass
            raise

    def release_for(self, owner: object) -> bool:
        """Idempotently retire the generation owned by *owner*.

        The owner mapping is the retirement commit. State/counter publication
        afterwards is derived and may be retried/reconciled without replaying
        resource cleanup.
        """
        self._rebuild_derived_from_owners()
        slot = self._slot_for_owner(owner)
        if slot < 0:
            return True
        committed = False
        try:
            self._states[slot] = _RETIRING
            # Authoritative retirement point.
            self._owners[slot] = None
            committed = True
            self._states[slot] = (
                _RETIRED if self._generations[slot] >= self._max_generation else _FREE
            )
            self._rebuild_derived_from_owners()
            return True
        except BaseException:
            if not committed:
                try:
                    if self._owners[slot] is owner:
                        self._states[slot] = _ACTIVE
                except BaseException:
                    pass
            else:
                try:
                    self._rebuild_derived_from_owners()
                except BaseException:
                    pass
            raise

    def token_for(self, owner: object) -> int | None:
        slot = self._slot_for_owner(owner)
        if slot < 0:
            return None
        return self._encode(slot, self._generations[slot])

    def owner_for(self, token: int) -> object | None:
        """Return the exact owner for *token*, or ``None`` if stale/unknown."""
        if type(token) is not int or token <= 0:
            return None
        slot = token & self._slot_mask
        generation = token >> self._slot_bits
        if slot >= self._capacity or self._generations[slot] != generation:
            return None
        return self._owners[slot]

    def owns_owner(self, owner: object, token: int | None = None) -> bool:
        slot = self._slot_for_owner(owner)
        if slot < 0:
            return False
        if token is None:
            return True
        return self._encode(slot, self._generations[slot]) == token

    def exact_active_count(self) -> int:
        active = 0
        for owner in self._owners:
            if owner is not None:
                active += 1
        return active

    def snapshot(self) -> BoundedGenerationSnapshot:
        return BoundedGenerationSnapshot(
            self._capacity, self._active, self._count, self._retired, self._corrupted
        )


def next_reusable_token(current: int, occupied) -> int | None:
    """Return a fixed-width free token without lifetime-monotonic growth."""
    if type(current) is not int or current < 0 or current > _MAX_TOKEN:
        current = 0
    candidate = current + 1
    if candidate > _MAX_TOKEN:
        candidate = 1
    start = candidate
    while candidate in occupied:
        candidate += 1
        if candidate > _MAX_TOKEN:
            candidate = 1
        if candidate == start:
            return None
    return candidate


__all__ = ["BoundedGenerationPool", "BoundedGenerationSnapshot", "next_reusable_token"]
