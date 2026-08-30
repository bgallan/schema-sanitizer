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
    """Allocate ABA-resistant tokens backed by exact rooted owner identities.

    Callers must externally serialize mutations; reads may share that same
    owner-domain synchronization.
    """

    __slots__ = (
        "_capacity",
        "_slot_bits",
        "_slot_mask",
        "_max_generation",
        "_count",
        "_generations",
        "_states",
        "_owners",
        "_owner_slots",
        "_free_ring",
        "_free_head",
        "_free_tail",
        "_active",
        "_retired",
        "_corrupted",
        "_derived_dirty",
    )

    def __init__(self, capacity: int) -> None:
        """Initialize the bounded generation pool and its owned runtime state."""
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
        self._owner_slots: dict[int, int] = {}
        self._free_ring = list(range(capacity))
        self._free_head = 0
        self._free_tail = 0
        self._active = 0
        self._retired = 0
        self._corrupted = False
        self._derived_dirty = False

    def _encode(self, slot: int, generation: int) -> int:
        """Encode an owner slot and generation into one bounded token."""
        return (generation << self._slot_bits) | slot

    def _slot_for_owner(self, owner: object) -> int:
        """Return an identity-verified owner slot from the derived index."""
        self._ensure_derived_from_owners()
        slot = self._owner_slots.get(id(owner), -1)
        if 0 <= slot < self._capacity and self._owners[slot] is owner:
            return slot
        if slot >= 0:
            # A stale identity hint can only be trusted after reconstruction;
            # exact owner slots, never object ids, remain authoritative.
            self._derived_dirty = True
            self._rebuild_derived_from_owners()
            slot = self._owner_slots.get(id(owner), -1)
            if 0 <= slot < self._capacity and self._owners[slot] is owner:
                return slot
        return -1

    def _ensure_derived_from_owners(self) -> None:
        """Rebuild derived indexes only after an interrupted mutation."""
        if self._derived_dirty:
            self._rebuild_derived_from_owners()

    def _prepare_free_pop(self) -> tuple[int, int, int] | None:
        """Prepare an O(1) free-ring pop without publishing it."""
        if self._count <= 0:
            return None
        slot = self._free_ring[self._free_head]
        next_head = self._free_head + 1
        if next_head == self._capacity:
            next_head = 0
        return slot, next_head, self._count - 1

    def _commit_free_pop(self, next_head: int, next_count: int) -> None:
        """Publish one previously prepared free-ring pop."""
        self._free_head = next_head
        self._count = next_count

    def _prepare_free_push(self) -> tuple[int, int, int] | None:
        """Prepare an O(1) free-ring push without publishing it."""
        if self._count >= self._capacity:
            return None
        tail = self._free_tail
        next_tail = tail + 1
        if next_tail == self._capacity:
            next_tail = 0
        return tail, next_tail, self._count + 1

    def _commit_free_push(self, slot: int, tail: int, next_tail: int, next_count: int) -> None:
        """Publish one previously prepared free-ring push."""
        self._free_ring[tail] = slot
        self._free_tail = next_tail
        self._count = next_count

    def _rebuild_derived_from_owners(self) -> None:
        """Reconstruct state and counters from exact slot ownership.

        This operation is intentionally idempotent.  If an asynchronous
        exception interrupts the rebuild, the next owner-aware operation simply
        runs it again; exact ``_owners`` entries remain the authority.
        """
        # Publish the dirty marker before touching any mirror. If reconstruction
        # is interrupted, the next owner-aware operation retries the full pass.
        self._derived_dirty = True
        available = 0
        active = 0
        retired = 0
        states = bytearray(self._capacity)
        free_ring = [0] * self._capacity
        owner_slots: dict[int, int] = {}
        for slot in range(self._capacity):
            owner = self._owners[slot]
            generation = self._generations[slot]
            if owner is not None:
                states[slot] = _ACTIVE
                owner_slots[id(owner)] = slot
                active += 1
                continue
            if generation >= self._max_generation or self._states[slot] == _RETIRED:
                states[slot] = _RETIRED
                retired += 1
                continue
            states[slot] = _FREE
            free_ring[available] = slot
            available += 1
        self._states = states
        self._free_ring = free_ring
        self._free_head = 0
        self._free_tail = 0 if available == self._capacity else available
        self._owner_slots = owner_slots
        self._count = available
        self._active = active
        self._retired = retired
        self._derived_dirty = False

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
        # Ordinary admissions use only the identity index and free ring. A full
        # authority scan is reserved for interrupted/corrupted mirror recovery.
        self._ensure_derived_from_owners()
        if self._slot_for_owner(owner) >= 0:
            raise RuntimeError("bounded generation owner already has a live slot")

        prepared = self._prepare_free_pop()
        if prepared is None:
            return None
        slot, next_head, next_count = prepared
        if (
            slot < 0
            or slot >= self._capacity
            or self._owners[slot] is not None
            or self._states[slot] != _FREE
            or self._generations[slot] >= self._max_generation
        ):
            self._derived_dirty = True
            self._rebuild_derived_from_owners()
            prepared = self._prepare_free_pop()
            if prepared is None:
                return None
            slot, next_head, next_count = prepared

        generation = self._generations[slot] + 1
        token = self._encode(slot, generation)
        committed_owner = False
        try:
            self._derived_dirty = True
            # Exact authority first.  Any exception from this point is
            # recoverable by identity, without the integer handoff.
            self._owners[slot] = owner
            committed_owner = True
            self._owner_slots[id(owner)] = slot
            self._generations[slot] = generation
            self._states[slot] = _ACTIVE
            self._commit_free_pop(next_head, next_count)
            self._active = self._active + 1
            self._derived_dirty = False
            return token
        except BaseException:
            # ``_owners[slot] = owner`` can commit before an asynchronous
            # exception prevents the following flag write. Exact slot
            # authority, not that advisory local flag, decides rollback.
            if committed_owner or self._owners[slot] is owner:
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
        self._ensure_derived_from_owners()
        slot = self._slot_for_owner(owner)
        if slot < 0:
            return True
        retiring = self._generations[slot] >= self._max_generation
        prepared_push = None if retiring else self._prepare_free_push()
        if not retiring and prepared_push is None:
            self._derived_dirty = True
            self._rebuild_derived_from_owners()
            slot = self._slot_for_owner(owner)
            if slot < 0:
                return True
            retiring = self._generations[slot] >= self._max_generation
            prepared_push = None if retiring else self._prepare_free_push()
            if not retiring and prepared_push is None:
                self._corrupted = True
                return False
        committed = False
        try:
            self._derived_dirty = True
            self._states[slot] = _RETIRING
            # Authoritative retirement point.
            self._owners[slot] = None
            committed = True
            self._owner_slots.pop(id(owner), None)
            self._active = self._active - 1
            if retiring:
                self._states[slot] = _RETIRED
                self._retired = self._retired + 1
            else:
                assert prepared_push is not None
                tail, next_tail, next_count = prepared_push
                self._commit_free_push(slot, tail, next_tail, next_count)
                self._states[slot] = _FREE
            self._derived_dirty = False
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
        """Return the active generation token for an owner."""
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
        """Return whether this generation pool owns the supplied owner identity."""
        slot = self._slot_for_owner(owner)
        if slot < 0:
            return False
        if token is None:
            return True
        return self._encode(slot, self._generations[slot]) == token

    def exact_active_count(self) -> int:
        """Return the exact active count."""
        active = 0
        for owner in self._owners:
            if owner is not None:
                active += 1
        return active

    def snapshot(self) -> BoundedGenerationSnapshot:
        """Return a bounded snapshot of the current bounded generation pool."""
        self._ensure_derived_from_owners()
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
