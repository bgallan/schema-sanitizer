"""Provide preallocated, non-blocking handoff slots for Python finalizers.

Generation-stamped tickets and explicit slot states prevent stale ABA recycling without
container growth or lock waits on the producer path. Governed consumers retain each
owner while processing one slot at a time until cleanup acknowledges success.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from threading import Lock
from typing import Callable, Generic, TypeAlias, TypeVar

from .atomic_epoch import AtomicEpoch

T = TypeVar("T")
_EMPTY = object()

_ReservedForkBank: TypeAlias = tuple[
    list[object],
    bytearray,
    list[int],
    list[Lock],
    list[int | None],
    dict[int, int],
    list[int],
    Lock,
    AtomicEpoch,
    AtomicEpoch,
    AtomicEpoch,
    AtomicEpoch,
    AtomicEpoch,
    AtomicEpoch,
    AtomicEpoch,
]

_FREE = 0
_RESERVED = 1
_PUBLISHED = 2
_CLAIMED = 3
_RETIRED = 4
_RECYCLE_PENDING = 5
_PROCESSED = 6
_MAX_TICKET = (1 << 63) - 1
_MAX_FORK_QUARANTINE_GENERATIONS = 4

_PTR_BYTES = max(8, sys.getsizeof([None]) - sys.getsizeof([]))
_LIST_BASE_BYTES = sys.getsizeof([])
_BYTEARRAY_BASE_BYTES = sys.getsizeof(bytearray())
_LOCK_BYTES = sys.getsizeof(Lock())
_FIXED_INT_BYTES = sys.getsizeof((1 << 63) - 1)
# AtomicEpoch owns a Python wrapper and, with ABI3, a tiny native atomic/capsule.
# Reserve a deliberately high fixed charge so the static baseline remains
# conservative across supported Python allocators.
_ATOMIC_EPOCH_BYTES = 256


def _list_storage_bytes(count: int, *, element_bytes: int = 0) -> int:
    """Return the list storage bytes."""
    return _LIST_BASE_BYTES + count * (_PTR_BYTES + element_bytes)


def _reserved_escrow_static_bytes(capacity: int) -> int:
    """Return the reserved escrow static bytes."""
    per_bank = (
        _list_storage_bytes(capacity)  # owner slots
        + _BYTEARRAY_BASE_BYTES
        + capacity
        + _list_storage_bytes(capacity, element_bytes=_FIXED_INT_BYTES)
        + _list_storage_bytes(capacity, element_bytes=_LOCK_BYTES)
        + _list_storage_bytes(capacity, element_bytes=_FIXED_INT_BYTES)
        + _list_storage_bytes(capacity)  # exact ticket values by slot
        + capacity * 192  # bounded ticket->slot dict entries + allocator reserve
        + _LOCK_BYTES
        + 7 * _ATOMIC_EPOCH_BYTES
    )
    roots = _list_storage_bytes(14 * _MAX_FORK_QUARANTINE_GENERATIONS)
    raw = 3 * per_bank + roots + 4096
    # 25% allocator/alignment reserve keeps this a conservative footprint, not
    # the former unexplained bytes-per-slot heuristic.
    return max(4096, raw + raw // 4)


def _write_u64(value: int, target: bytearray, offset: int) -> int:
    """Append one unsigned 64-bit integer to a fixed buffer."""
    if value < 0 or value > 0xFFFFFFFFFFFFFFFF:
        raise OverflowError("activity counter exceeds u64")
    for shift in range(0, 64, 8):
        target[offset] = (value >> shift) & 0xFF
        offset += 1
    return offset


def _write_u32(value: int, target: bytearray, offset: int) -> int:
    """Write a bounded non-negative integer without constructing bytes."""
    if value < 0 or value > 0xFFFFFFFF:
        raise OverflowError("activity counter exceeds u32")
    for shift in (0, 8, 16, 24):
        target[offset] = (value >> shift) & 0xFF
        offset += 1
    return offset


class _StaticFootprintGuard:
    """Rollback one pre-admitted static footprint if construction aborts."""

    __slots__ = ("kind", "amount", "created", "rollback")

    def __init__(self, kind: str, amount: int) -> None:
        """Initialize the static footprint guard and its owned runtime state."""
        from .static_control_plane import (
            reserve_static_control_plane,
            rollback_static_control_plane,
        )

        self.kind = kind
        self.amount = amount
        self.rollback = rollback_static_control_plane
        self.created = reserve_static_control_plane(kind, amount)

    def commit(self) -> None:
        """Commit the admission retained by this static footprint guard."""
        self.created = False

    def rollback_now(self) -> None:
        """Explicitly undo construction admission; never defer locks to GC."""
        if not self.created:
            return
        self.created = False
        self.rollback(self.kind, self.amount)


def _static_footprint_guard(
    kind: str | None, *, prefix: str, amount: int
) -> _StaticFootprintGuard | None:
    """Register a static escrow footprint when control-plane accounting is available."""
    if kind is None:
        return None
    if type(kind) is not str or not kind:
        raise ValueError("static_kind must be a non-empty string")
    try:
        return _StaticFootprintGuard(f"{prefix}:{kind}", amount)
    except (ImportError, AttributeError):
        return None


@dataclass(frozen=True, slots=True)
class FinalizerEscrowCapacitySnapshot:
    """Fixed-capacity teardown admission for one reserved escrow.

    ``overflowed`` is reserved for an irreversible publication/corruption
    failure. Temporary admission saturation is reported separately and does
    not poison terminal shutdown once capacity becomes available again.
    """

    capacity: int
    active: int
    available: int
    retired: int
    overflowed: bool
    admission_rejections: int = 0
    publication_failures: int = 0
    publication_epoch: int = 0
    progress_epoch: int = 0
    recycle_pending: int = 0

    @property
    def invariant_ok(self) -> bool:
        """Return whether the snapshot satisfies its capacity invariant."""
        return (
            0 <= self.active <= self.capacity
            and 0 <= self.retired <= self.capacity
            and 0 <= self.recycle_pending <= self.capacity
            and self.active + self.retired + self.available + self.recycle_pending == self.capacity
        )


class ReservedFinalizerEscrow(Generic[T]):
    """Generation-stamped private finalizer slots with O(1) admission.

    All counters changed by finalizer publication are fixed-width byte arrays, so
    progress cannot silently disappear because Python failed to grow an integer.
    A preallocated free ring makes reserve/recycle O(1), while exact generation
    tickets preserve ABA protection.
    """

    __slots__ = (
        "_capacity",
        "_pid",
        "_slot_bits",
        "_slot_mask",
        "_max_generation",
        "_slots",
        "_states",
        "_generations",
        "_slot_locks",
        "_tickets",
        "_ticket_slots",
        "_free_ring",
        "_free_head",
        "_free_tail",
        "_free_count",
        "_consume_cursor",
        "_reserve_lock",
        "_overflowed",
        "_active_counter",
        "_published_counter",
        "_retired_counter",
        "_admission_rejections_counter",
        "_publication_failures_counter",
        "_publication_epoch_counter",
        "_progress_epoch_counter",
        "_fork_roots",
        "_fork_root_count",
        "_fork_prepare_index",
        "_fork_fresh",
        "_fork_spare2",
        "_fork_prepare_exhausted",
        "_fork_unusable_after_fork",
        "_post_fork_quarantine_pending",
        "__weakref__",
    )

    def __init__(self, capacity: int, *, static_kind: str | None = None) -> None:
        """Initialize the reserved finalizer escrow and its owned runtime state."""
        if type(capacity) is not int:
            raise TypeError("reserved finalizer escrow capacity must be an exact integer")
        if capacity <= 0:
            raise ValueError("reserved finalizer escrow capacity must be > 0")
        self._capacity = capacity
        static_guard = _static_footprint_guard(
            static_kind,
            prefix="reserved_finalizer_escrow",
            amount=_reserved_escrow_static_bytes(self._capacity),
        )
        try:
            self._pid = os.getpid()
            self._slot_bits = max(1, (capacity - 1).bit_length())
            self._slot_mask = (1 << self._slot_bits) - 1
            self._max_generation = _MAX_TICKET >> self._slot_bits
            self._slots: list[object] = [_EMPTY] * capacity
            self._states = bytearray(capacity)
            self._generations: list[int] = [0] * capacity
            self._slot_locks: list[Lock] = [Lock() for _ in range(capacity)]
            # Exact ticket metadata is prepared before reservation commit. Finalizer
            # publication therefore performs only bounded dict lookup + fixed-slot
            # assignments; it never decodes a PyLong with allocating arithmetic.
            self._tickets: list[int | None] = [None] * capacity
            self._ticket_slots: dict[int, int] = {}
            self._free_ring = list(range(capacity))
            self._free_head = 0
            self._free_tail = 0
            self._free_count = capacity
            self._consume_cursor = 0
            self._reserve_lock = Lock()
            self._overflowed = False
            self._active_counter = AtomicEpoch()
            self._published_counter = AtomicEpoch()
            self._retired_counter = AtomicEpoch()
            self._admission_rejections_counter = AtomicEpoch()
            self._publication_failures_counter = AtomicEpoch()
            self._publication_epoch_counter = AtomicEpoch()
            self._progress_epoch_counter = AtomicEpoch()
            self._fork_roots: list[object | None] = [None] * (16 * _MAX_FORK_QUARANTINE_GENERATIONS)
            self._fork_root_count = 0
            self._fork_prepare_index = -1
            self._fork_fresh: _ReservedForkBank | None
            self._fork_spare2: _ReservedForkBank | None
            self._fork_fresh = self._make_fresh_bank()
            self._fork_spare2 = self._make_fresh_bank()
            self._fork_prepare_exhausted = False
            self._fork_unusable_after_fork = False
            self._post_fork_quarantine_pending = False
            if static_kind is not None:
                from .fork_manager import register_fork_handler

                register_fork_handler(
                    f"reserved-finalizer:{static_kind}",
                    before=self.prepare_for_fork,
                    after_in_parent=self.clear_fork_preparation,
                    after_in_child=self.reset_after_fork,
                )
            if static_guard is not None:
                static_guard.commit()
        except BaseException:
            if static_guard is not None:
                try:
                    static_guard.rollback_now()
                except BaseException:
                    pass
            raise

    def _make_fresh_bank(self) -> _ReservedForkBank:
        """Allocate a fresh fixed-capacity escrow bank."""
        return (
            [_EMPTY] * self._capacity,
            bytearray(self._capacity),
            [0] * self._capacity,
            [Lock() for _ in range(self._capacity)],
            [None] * self._capacity,
            {},
            list(range(self._capacity)),
            Lock(),
            AtomicEpoch(),
            AtomicEpoch(),
            AtomicEpoch(),
            AtomicEpoch(),
            AtomicEpoch(),
            AtomicEpoch(),
            AtomicEpoch(),
        )

    def _safe_point_after_fork(self) -> None:
        """Release synthetic-test roots; never decref inherited graphs in a real child."""
        if not self._post_fork_quarantine_pending:
            return
        try:
            from .fork_safety import runtime_fork_poisoned

            if runtime_fork_poisoned():
                return
        except BaseException:
            return
        self._post_fork_quarantine_pending = False
        limit = self._fork_root_count * 16
        for index in range(limit):
            self._fork_roots[index] = None
        self._fork_root_count = 0
        if self._fork_fresh is None:
            self._fork_fresh = self._make_fresh_bank()
        if self._fork_spare2 is None:
            self._fork_spare2 = self._make_fresh_bank()

    def _bump_progress(self) -> None:
        """Advance and return the escrow progress epoch."""
        if not self._progress_epoch_counter.increment():
            self._overflowed = True
            self._publication_failures_counter.increment()

    def _bump_publication(self) -> None:
        """Advance and return the escrow publication epoch."""
        publication_ok = self._publication_epoch_counter.increment()
        progress_ok = self._progress_epoch_counter.increment()
        if not publication_ok or not progress_ok:
            self._overflowed = True
            self._publication_failures_counter.increment()

    @property
    def capacity(self) -> int:
        """Return the escrow's fixed slot capacity."""
        return self._capacity

    @property
    def overflowed(self) -> bool:
        """Return whether the escrow observed an irreversible publication failure."""
        return self._overflowed

    def _encode_ticket(self, slot: int, generation: int) -> int:
        """Encode an escrow slot and generation as one ticket."""
        return (generation << self._slot_bits) | slot

    def _decode_ticket(self, ticket: int) -> tuple[int, int] | None:
        """Decode an escrow ticket into its slot and generation."""
        if type(ticket) is not int or ticket < 0:
            return None
        slot = ticket & self._slot_mask
        generation = ticket >> self._slot_bits
        if slot >= self._capacity or generation <= 0:
            return None
        return slot, generation

    @staticmethod
    def _armed_ticket(value: object) -> int:
        """Return the ownership ticket currently armed on a value."""
        try:
            return max(0, int(value._escrow_armed_ticket))  # type: ignore[attr-defined]
        except BaseException:
            return 0

    @classmethod
    def _is_armed_for(cls, value: object, ticket: int | None) -> bool:
        """Return whether the value is armed for the supplied escrow ticket."""
        return ticket is not None and ticket > 0 and cls._armed_ticket(value) == ticket

    @staticmethod
    def _arm_value_for(value: object, ticket: int) -> bool:
        """Arm a retained value with its authoritative escrow ticket."""
        try:
            value.arm_for_ticket(ticket)  # type: ignore[attr-defined]
            return True
        except BaseException:
            return False

    @staticmethod
    def _disarm_value_for(value: object, ticket: int | None = None) -> None:
        """Disarm finalization for a value carrying the matching escrow ticket."""
        try:
            value.disarm_ticket(ticket)  # type: ignore[attr-defined]
        except BaseException:
            pass

    @staticmethod
    def _clear_value_ticket(value: object, ticket: int | None) -> None:
        """Clear a matching escrow ticket from a retained value."""
        if ticket is None:
            return
        try:
            if int(getattr(value, "ticket", 0) or 0) == int(ticket):
                setattr(value, "ticket", 0)
        except BaseException:
            pass

    def _ring_prepare_pop_locked(self) -> tuple[int, int, int] | None:
        """Prepare one free-ring pop without mutating authoritative metadata."""
        if self._free_count <= 0:
            return None
        slot = self._free_ring[self._free_head]
        next_head = self._free_head + 1
        if next_head == self._capacity:
            next_head = 0
        next_count = self._free_count - 1
        return slot, next_head, next_count

    def _ring_commit_pop_locked(self, next_head: int, next_count: int) -> None:
        """Commit removal from the escrow ring while holding its lock."""
        self._free_head = next_head
        self._free_count = next_count

    def _ring_prepare_push_locked(self, slot: int) -> tuple[int, int, int] | None:
        """Prepare a recycle before the slot is made FREE."""
        if self._free_count >= self._capacity:
            return None
        tail = self._free_tail
        next_tail = tail + 1
        if next_tail == self._capacity:
            next_tail = 0
        next_count = self._free_count + 1
        return tail, next_tail, next_count

    def _ring_commit_push_locked(
        self, slot: int, tail: int, next_tail: int, next_count: int
    ) -> None:
        """Commit insertion into the escrow ring while holding its lock."""
        self._free_ring[tail] = slot
        self._free_tail = next_tail
        self._free_count = next_count

    def _recycle_one_pending_locked(self) -> bool:
        """Return one owner-free pending slot to the free ring.

        This helper is only called on the *admission* side, before a new owner
        commits.  It may therefore perform ordinary Python arithmetic without
        endangering an already-processed resource.
        """
        if self._free_count >= self._capacity:
            return False
        for slot in range(self._capacity):
            if self._states[slot] != _RECYCLE_PENDING:
                continue
            tail = self._free_tail
            next_tail = tail + 1
            if next_tail == self._capacity:
                next_tail = 0
            next_count = self._free_count + 1
            with self._slot_locks[slot]:
                if self._states[slot] != _RECYCLE_PENDING or self._slots[slot] is not _EMPTY:
                    continue
                self._ring_commit_push_locked(slot, tail, next_tail, next_count)
                self._states[slot] = _FREE
                return True
        return False

    def reserve_ticket(self) -> int | None:
        """Reserve one generation with every fallible value prepared pre-commit."""
        if self._fork_unusable_after_fork:
            return None
        self._safe_point_after_fork()
        with self._reserve_lock:
            while True:
                prepared = self._ring_prepare_pop_locked()
                if prepared is None:
                    # A successful cleanup may have retired ownership before its
                    # slot could be recycled. Reclaim one such owner-free slot
                    # here, before any new reservation commits.
                    if self._recycle_one_pending_locked():
                        prepared = self._ring_prepare_pop_locked()
                if prepared is None:
                    self._admission_rejections_counter.increment()
                    return None
                slot, next_head, next_count = prepared
                with self._slot_locks[slot]:
                    if self._states[slot] != _FREE:
                        # Ring corruption is terminal; consume this bad entry so
                        # repeated reservations cannot spin on it forever.
                        self._ring_commit_pop_locked(next_head, next_count)
                        self._overflowed = True
                        self._publication_failures_counter.increment()
                        continue
                    generation = self._generations[slot] + 1
                    if generation > self._max_generation:
                        self._ring_commit_pop_locked(next_head, next_count)
                        self._states[slot] = _RETIRED
                        self._retired_counter.increment()
                        self._bump_progress()
                        continue
                    # Ticket construction is deliberately before active/state/ring
                    # publication. A PyLong OOM therefore leaves no hidden owner.
                    ticket = self._encode_ticket(slot, generation)
                    # Prepublish the exact ticket lookup before any authoritative
                    # reservation state changes. Dict growth/OOM is therefore a
                    # pre-commit failure, while finalizer publication is lookup-only.
                    self._ticket_slots[ticket] = slot
                    if not self._active_counter.increment():
                        self._ticket_slots.pop(ticket, None)
                        self._ring_commit_pop_locked(next_head, next_count)
                        self._overflowed = True
                        self._publication_failures_counter.increment()
                        self._states[slot] = _RETIRED
                        self._retired_counter.increment()
                        continue
                    # Commit tail: only assignments to already allocated storage.
                    self._ring_commit_pop_locked(next_head, next_count)
                    self._generations[slot] = generation
                    self._tickets[slot] = ticket
                    self._states[slot] = _RESERVED
                    self._bump_progress()
                    return ticket

    def _rebuild_capacity_mirrors_locked(self) -> None:
        """Rebuild free-ring/cardinality mirrors from fixed slot authority.

        Admission-side recovery may allocate fresh AtomicEpoch wrappers. Exact
        slot owner/state/ticket arrays remain the authority and cleanup never
        waits on this repair.
        """
        free_count = 0
        active = 0
        published = 0
        retired = 0
        rebuilt_tickets: dict[int, int] = {}
        for slot in range(self._capacity):
            state = self._states[slot]
            value = self._slots[slot]
            ticket = self._tickets[slot]
            if value is not _EMPTY and state not in (_FREE, _RETIRED, _RECYCLE_PENDING):
                active += 1
                if state in (_PUBLISHED, _CLAIMED, _PROCESSED):
                    published += 1
                if ticket is not None:
                    rebuilt_tickets[ticket] = slot
                continue
            if state == _RETIRED:
                retired += 1
                continue
            # Owner-free pending/free state is admission capacity.
            self._slots[slot] = _EMPTY
            self._states[slot] = _FREE
            self._tickets[slot] = None
            self._free_ring[free_count] = slot
            free_count += 1
        for index in range(free_count, self._capacity):
            self._free_ring[index] = 0
        self._free_head = 0
        self._free_count = free_count
        self._free_tail = 0 if free_count == self._capacity else free_count
        self._ticket_slots = rebuilt_tickets
        active_counter = AtomicEpoch()
        published_counter = AtomicEpoch()
        retired_counter = AtomicEpoch()
        for _ in range(active):
            active_counter.increment()
        for _ in range(published):
            published_counter.increment()
        for _ in range(retired):
            retired_counter.increment()
        self._active_counter = active_counter
        self._published_counter = published_counter
        self._retired_counter = retired_counter

    def reserve_rooted(self, value: T) -> int | None:
        """Reserve a generation with *value* rooted before token handoff.

        Unlike ``reserve_ticket(); root_reserved(...)``, this owner-first form
        has no naked-ticket window. If its return is interrupted, the caller can
        still identify the reservation by ``value``; if publication inside this
        method is interrupted, rollback rebuilds every derived capacity mirror.
        """
        if self._fork_unusable_after_fork:
            return None
        self._safe_point_after_fork()
        slot = -1
        ticket: int | None = None
        rollback_owner = False
        try:
            with self._reserve_lock:
                # One exact owner may occupy at most one generation.  This
                # scan is bounded by escrow capacity and runs only on admission;
                # it avoids a second dynamically-grown authority index.
                for existing_slot in range(self._capacity):
                    if self._slots[existing_slot] is not value:
                        continue
                    with self._slot_locks[existing_slot]:
                        existing_ticket = self._tickets[existing_slot]
                        existing_state = self._states[existing_slot]
                        if (
                            existing_ticket is not None
                            and existing_state == _RESERVED
                            and not self._is_armed_for(value, existing_ticket)
                        ):
                            # Lost handoff/retry of the same pre-armed owner.
                            try:
                                setattr(value, "ticket", existing_ticket)
                            except BaseException:
                                pass
                            return existing_ticket
                        raise RuntimeError(
                            "reserved finalizer owner already has an active generation"
                        )
                while True:
                    prepared = self._ring_prepare_pop_locked()
                    if prepared is None and self._recycle_one_pending_locked():
                        prepared = self._ring_prepare_pop_locked()
                    if prepared is None:
                        self._admission_rejections_counter.increment()
                        return None
                    slot, next_head, next_count = prepared
                    with self._slot_locks[slot]:
                        if self._states[slot] != _FREE or self._slots[slot] is not _EMPTY:
                            self._ring_commit_pop_locked(next_head, next_count)
                            self._overflowed = True
                            self._publication_failures_counter.increment()
                            continue
                        generation = self._generations[slot] + 1
                        if generation > self._max_generation:
                            self._ring_commit_pop_locked(next_head, next_count)
                            self._states[slot] = _RETIRED
                            self._retired_counter.increment()
                            self._bump_progress()
                            continue
                        ticket = self._encode_ticket(slot, generation)
                        # Exact owner first. From this assignment onward a
                        # rollback can locate authority without the returned int.
                        rollback_owner = True
                        self._slots[slot] = value
                        try:
                            setattr(value, "ticket", ticket)
                        except BaseException:
                            pass
                        self._ticket_slots[ticket] = slot
                        if not self._active_counter.increment():
                            raise RuntimeError("reserved finalizer active counter exhausted")
                        self._ring_commit_pop_locked(next_head, next_count)
                        self._generations[slot] = generation
                        self._tickets[slot] = ticket
                        self._states[slot] = _RESERVED
                        self._bump_progress()
                        return ticket
        except BaseException:
            if not rollback_owner:
                raise
            # The owner identity is available even when ``ticket = CALL`` never
            # reached its STORE in the caller. Recover the slot from fixed arrays.
            try:
                with self._reserve_lock:
                    found = -1
                    for candidate in range(self._capacity):
                        if self._slots[candidate] is value:
                            found = candidate
                            break
                    if found >= 0:
                        with self._slot_locks[found]:
                            old_ticket = self._tickets[found]
                            published_ticket = old_ticket if old_ticket is not None else ticket
                            self._slots[found] = _EMPTY
                            self._tickets[found] = None
                            self._states[found] = _RECYCLE_PENDING
                            if old_ticket is not None:
                                self._ticket_slots.pop(old_ticket, None)
                            if ticket is not None:
                                self._ticket_slots.pop(ticket, None)
                            self._disarm_value_for(value, published_ticket)
                            self._clear_value_ticket(value, published_ticket)
                        self._rebuild_capacity_mirrors_locked()
            except BaseException:
                self._overflowed = True
                self._publication_failures_counter.increment()
            raise

    def release_rooted_owner(self, value: T) -> bool:
        """Idempotently retire an unarmed rooted reservation by identity."""
        if self._fork_unusable_after_fork:
            return False
        with self._reserve_lock:
            for slot in range(self._capacity):
                if self._slots[slot] is not value:
                    continue
                with self._slot_locks[slot]:
                    ticket = self._tickets[slot]
                    if self._is_armed_for(value, ticket):
                        return False
                    self._slots[slot] = _EMPTY
                    self._tickets[slot] = None
                    self._states[slot] = _RECYCLE_PENDING
                    if ticket is not None:
                        self._ticket_slots.pop(ticket, None)
                self._rebuild_capacity_mirrors_locked()
                self._disarm_value_for(value, ticket)
                self._clear_value_ticket(value, ticket)
                self._bump_progress()
                return True
        # Already retired is success for exact-owner rollback/retry.
        stale_ticket = None
        try:
            stale_ticket = int(getattr(value, "ticket", 0) or 0)
        except BaseException:
            pass
        self._disarm_value_for(value, stale_ticket)
        self._clear_value_ticket(value, stale_ticket)
        return True

    def root_reserved(self, ticket: int, value: T) -> bool:
        """Root *value* in an exact RESERVED generation before finalizer exposure.

        This admission-side operation may wait for the private slot lock.  Once it
        succeeds, the escrow itself owns a strong reference to ``value`` even if
        a later non-blocking publication attempt cannot acquire that lock.
        """
        if self._fork_unusable_after_fork:
            return False
        slot = self._ticket_slots.get(ticket, -1)
        if slot < 0 or slot >= self._capacity:
            return False
        with self._slot_locks[slot]:
            if self._tickets[slot] != ticket:
                return False
            if self._states[slot] != _RESERVED or self._slots[slot] is not _EMPTY:
                return False
            self._slots[slot] = value
            self._bump_progress()
            return True

    def release_rooted_ticket(self, ticket: int, value: T) -> bool:
        """Idempotently retire one unarmed rooted owner generation.

        Owner removal is the semantic commit; free-ring/counter work is derived
        and may complete later from RECYCLE_PENDING without resurrecting cleanup.
        """
        if self._fork_unusable_after_fork:
            return False
        slot = self._ticket_slots.get(ticket, -1)
        if slot < 0 or slot >= self._capacity:
            # If the exact rooted owner no longer exists, a previous retirement
            # may already have committed before its caller observed success.
            for candidate in range(self._capacity):
                if self._slots[candidate] is value:
                    return False
            self._disarm_value_for(value, ticket)
            self._clear_value_ticket(value, ticket)
            return True
        committed = False
        try:
            with self._slot_locks[slot]:
                if self._tickets[slot] != ticket:
                    return False
                if self._states[slot] != _RESERVED or self._slots[slot] is not value:
                    return False
                if self._is_armed_for(value, ticket):
                    return False
                self._slots[slot] = _EMPTY
                self._states[slot] = _RECYCLE_PENDING
                self._tickets[slot] = None
                self._ticket_slots.pop(ticket, None)
                committed = True
                if not self._active_counter.decrement():
                    self._overflowed = True
                    self._publication_failures_counter.increment()
                self._bump_progress()
            try:
                with self._reserve_lock:
                    self._recycle_one_pending_locked()
            except BaseException:
                # Owner retirement already committed. RECYCLE_PENDING is an
                # explicit recoverable capacity state, not corruption.
                pass
            self._disarm_value_for(value, ticket)
            self._clear_value_ticket(value, ticket)
            return True
        except BaseException:
            if committed:
                # Do not report a replayable primary failure after exact owner
                # retirement. Capacity bookkeeping can be scavenged later.
                self._disarm_value_for(value, ticket)
                self._clear_value_ticket(value, ticket)
                return True
            raise

    def publish_rooted(self, ticket: int, value: T) -> bool:
        """Arm one pre-rooted owner for eventual safe-point processing.

        The arm bit lives on the already-rooted owner.  Therefore slot-lock
        contention cannot lose authority: a safe point can later promote the
        RESERVED generation to PUBLISHED even after the publishing wrapper has
        disappeared.
        """
        if self._fork_unusable_after_fork:
            return False
        slot = self._ticket_slots.get(ticket, -1)
        if slot < 0 or slot >= self._capacity:
            # A stale wrapper may retain its mirror after exact owner rollback.
            # If the owner no longer authenticates this generation, retirement
            # already won and publication is an idempotent ACK, not corruption.
            for candidate in range(self._capacity):
                if self._slots[candidate] is value:
                    self._overflowed = True
                    self._publication_failures_counter.increment()
                    return False
            try:
                if int(getattr(value, "ticket", 0) or 0) != int(ticket):
                    self._disarm_value_for(value, ticket)
                    return True
            except BaseException:
                pass
            self._overflowed = True
            self._publication_failures_counter.increment()
            return False
        if not self._arm_value_for(value, ticket):
            self._overflowed = True
            self._publication_failures_counter.increment()
            return False
        lock = self._slot_locks[slot]
        if not lock.acquire(blocking=False):
            # Durable handoff already committed via the rooted, armed owner.
            self._bump_progress()
            return True
        try:
            if self._tickets[slot] != ticket:
                self._disarm_value_for(value, ticket)
                return False
            if self._states[slot] == _PUBLISHED and self._slots[slot] is value:
                return True
            if self._states[slot] != _RESERVED or self._slots[slot] is not value:
                self._disarm_value_for(value, ticket)
                return False
            self._states[slot] = _PUBLISHED
            self._published_counter.increment()
            self._bump_publication()
            return True
        finally:
            lock.release()

    def release_ticket(self, ticket: int) -> bool:
        """Idempotently release an unarmed RESERVED generation."""
        if self._fork_unusable_after_fork:
            return False
        slot = self._ticket_slots.get(ticket, -1)
        if slot < 0 or slot >= self._capacity:
            return True
        committed = False
        try:
            with self._slot_locks[slot]:
                if self._tickets[slot] != ticket:
                    return True
                rooted = self._slots[slot]
                if self._states[slot] != _RESERVED:
                    return False
                if rooted is not _EMPTY and self._is_armed_for(rooted, ticket):
                    return False
                self._slots[slot] = _EMPTY
                self._states[slot] = _RECYCLE_PENDING
                self._tickets[slot] = None
                self._ticket_slots.pop(ticket, None)
                committed = True
                if not self._active_counter.decrement():
                    self._overflowed = True
                    self._publication_failures_counter.increment()
                self._bump_progress()
            try:
                with self._reserve_lock:
                    self._recycle_one_pending_locked()
            except BaseException:
                pass
            return True
        except BaseException:
            if committed:
                return True
            raise

    def publish_reserved(self, ticket: int, value: T) -> bool:
        """Publish into an exclusive generation without waiting or allocation.

        The exact slot is looked up from metadata inserted before reserve commit;
        no ticket arithmetic or multi-value decode occurs in the finalizer tail.
        """
        if self._fork_unusable_after_fork:
            return False
        slot = self._ticket_slots.get(ticket, -1)
        if slot < 0 or slot >= self._capacity:
            self._overflowed = True
            self._publication_failures_counter.increment()
            return False
        lock = self._slot_locks[slot]
        if not lock.acquire(blocking=False):
            self._overflowed = True
            self._publication_failures_counter.increment()
            return False
        try:
            if self._tickets[slot] != ticket:
                return False
            if self._states[slot] != _RESERVED or self._slots[slot] is not _EMPTY:
                return False
            self._slots[slot] = value
            self._states[slot] = _PUBLISHED
            self._published_counter.increment()
            self._bump_publication()
            return True
        finally:
            lock.release()

    def process_one(self, processor: Callable[[int, T], object]) -> bool:
        """Process one exact owner with recoverable claim bookkeeping.

        ``CLAIMED`` is never left unreachable: the whole claim+invoke region is
        protected by one outer exception handler.  ``PROCESSED`` means the
        processor returned successfully and only owner/ring bookkeeping remains;
        a later safe point retires it without invoking the processor again.

        Arbitrary Python effects cannot be proven exactly-once across the single
        bytecode boundary between callback return and publishing PROCESSED. All
        production cleanup callbacks are therefore target-based/idempotent.
        """
        if self._fork_unusable_after_fork:
            return False
        self._safe_point_after_fork()
        slot = -1
        generation = 0
        ticket = 0
        value: object = _EMPTY
        try:
            start = self._consume_cursor
            for offset in range(self._capacity):
                candidate = start + offset
                if candidate >= self._capacity:
                    candidate -= self._capacity
                state = self._states[candidate]
                if state == _PROCESSED:
                    lock = self._slot_locks[candidate]
                    if not lock.acquire(blocking=False):
                        continue
                    try:
                        if self._states[candidate] != _PROCESSED:
                            continue
                        processed_ticket = self._tickets[candidate]
                        processed_value = self._slots[candidate]
                        self._slots[candidate] = _EMPTY
                        self._states[candidate] = _RECYCLE_PENDING
                        self._tickets[candidate] = None
                        if processed_ticket is not None:
                            self._ticket_slots.pop(processed_ticket, None)
                        if not self._active_counter.decrement():
                            self._overflowed = True
                            self._publication_failures_counter.increment()
                        if not self._published_counter.decrement():
                            self._overflowed = True
                            self._publication_failures_counter.increment()
                        self._bump_progress()
                    finally:
                        lock.release()
                    if processed_value is not _EMPTY:
                        self._disarm_value_for(processed_value, processed_ticket)
                        self._clear_value_ticket(processed_value, processed_ticket)
                    try:
                        with self._reserve_lock:
                            self._recycle_one_pending_locked()
                    except BaseException:
                        pass
                    return True
                if state not in (_PUBLISHED, _RESERVED):
                    continue
                if state == _RESERVED:
                    rooted = self._slots[candidate]
                    if rooted is _EMPTY or not self._is_armed_for(rooted, self._tickets[candidate]):
                        continue
                lock = self._slot_locks[candidate]
                if not lock.acquire(blocking=False):
                    continue
                try:
                    if self._states[candidate] == _RESERVED:
                        rooted = self._slots[candidate]
                        if rooted is _EMPTY or not self._is_armed_for(
                            rooted, self._tickets[candidate]
                        ):
                            continue
                        self._states[candidate] = _PUBLISHED
                        self._published_counter.increment()
                        self._bump_publication()
                    if self._states[candidate] != _PUBLISHED:
                        continue
                    generation = self._generations[candidate]
                    found_ticket = self._tickets[candidate]
                    if found_ticket is None:
                        self._overflowed = True
                        self._publication_failures_counter.increment()
                        continue
                    next_cursor = candidate + 1
                    if next_cursor == self._capacity:
                        next_cursor = 0
                    self._states[candidate] = _CLAIMED
                    slot = candidate
                    ticket = found_ticket
                    value = self._slots[candidate]
                    self._consume_cursor = next_cursor
                    break
                finally:
                    lock.release()
            if slot < 0 or value is _EMPTY:
                return False

            processor(ticket, value)  # type: ignore[arg-type]

            # Publish the no-replay marker before any owner/ring bookkeeping.
            with self._slot_locks[slot]:
                if (
                    self._generations[slot] == generation
                    and self._states[slot] == _CLAIMED
                    and self._slots[slot] is value
                ):
                    self._states[slot] = _PROCESSED
                    self._bump_progress()
                elif self._states[slot] != _PROCESSED:
                    self._overflowed = True
                    self._publication_failures_counter.increment()
                    return True

            # Complete owner retirement. If interrupted, PROCESSED is consumed
            # by the next safe point without callback replay.
            with self._slot_locks[slot]:
                if (
                    self._generations[slot] == generation
                    and self._states[slot] == _PROCESSED
                    and self._slots[slot] is value
                ):
                    self._slots[slot] = _EMPTY
                    self._states[slot] = _RECYCLE_PENDING
                    self._tickets[slot] = None
                    self._ticket_slots.pop(ticket, None)
                    if not self._active_counter.decrement():
                        self._overflowed = True
                        self._publication_failures_counter.increment()
                    if not self._published_counter.decrement():
                        self._overflowed = True
                        self._publication_failures_counter.increment()
                    self._bump_progress()
            self._disarm_value_for(value, ticket)
            self._clear_value_ticket(value, ticket)
            try:
                with self._reserve_lock:
                    self._recycle_one_pending_locked()
            except BaseException:
                pass
            return True
        except BaseException:
            # Covers the formerly unprotected PUBLISHED->CLAIMED handoff too.
            if slot >= 0 and value is not _EMPTY:
                try:
                    with self._slot_locks[slot]:
                        if (
                            self._generations[slot] == generation
                            and self._states[slot] == _CLAIMED
                            and self._slots[slot] is value
                        ):
                            self._states[slot] = _PUBLISHED
                            self._bump_progress()
                        # PROCESSED is intentionally not rolled back: the next
                        # safe point performs bookkeeping only.
                except BaseException:
                    self._overflowed = True
            raise

    def drain(self) -> tuple[T, ...]:
        """Drain all published values from this reserved finalizer escrow."""
        items: list[T] = []
        while True:
            box: list[T] = []

            def collect(_ticket: int, value: T) -> None:
                """Collect one value drained from this reserved finalizer escrow."""
                box.append(value)

            if not self.process_one(collect):
                break
            items.extend(box)
        return tuple(items)

    def drain_with_tickets(self) -> tuple[tuple[int, T], ...]:
        """Drain published values together with their ownership tickets."""
        items: list[tuple[int, T]] = []
        while True:
            box: list[tuple[int, T]] = []

            def collect(ticket: int, value: T) -> None:
                """Collect one value drained from this reserved finalizer escrow."""
                box.append((ticket, value))

            if not self.process_one(collect):
                break
            items.extend(box)
        return tuple(items)

    def published_count(self) -> int:
        """Return the published count."""
        if self._fork_unusable_after_fork:
            return 0
        published = 0
        for state in self._states:
            if state in (_PUBLISHED, _CLAIMED, _PROCESSED):
                published += 1
        return published

    def reserved_count(self) -> int:
        """Return the reserved count."""
        return self.active_count()

    def active_count(self) -> int:
        """Return exact slot authority; AtomicEpoch counters are telemetry mirrors."""
        if self._fork_unusable_after_fork:
            return 0
        active = 0
        for slot in range(self._capacity):
            state = self._states[slot]
            if self._slots[slot] is not _EMPTY or state in (
                _RESERVED,
                _PUBLISHED,
                _CLAIMED,
                _PROCESSED,
            ):
                active += 1
        return active

    def retired_count(self) -> int:
        """Return the retired count."""
        if self._fork_unusable_after_fork:
            return 0
        retired = 0
        for state in self._states:
            if state == _RETIRED:
                retired += 1
        return retired

    def capacity_snapshot(self) -> FinalizerEscrowCapacitySnapshot:
        """Return counters without acquiring a publisher slot lock."""
        if self._fork_unusable_after_fork:
            return FinalizerEscrowCapacitySnapshot(self._capacity, 0, 0, 0, True, 0, 1, 0, 0, 0)
        self._safe_point_after_fork()
        active = self.active_count()
        retired = self.retired_count()
        recycle_pending = 0
        for slot in range(self._capacity):
            if self._states[slot] == _RECYCLE_PENDING and self._slots[slot] is _EMPTY:
                recycle_pending += 1
        # RECYCLE_PENDING slots contain no owner but are not yet admissible. The
        # free-ring count therefore gives the exact admission-side availability.
        available = self._free_count
        return FinalizerEscrowCapacitySnapshot(
            self._capacity,
            active,
            available,
            retired,
            self._overflowed,
            self._admission_rejections_counter.value(),
            self._publication_failures_counter.value(),
            self._publication_epoch_counter.value(),
            self._progress_epoch_counter.value(),
            recycle_pending,
        )

    def write_activity_into(self, target: bytearray, offset: int) -> int:
        """Write exact quiescence counters without PyLong materialization."""
        self._active_counter.write_into(target, offset)
        self._publication_failures_counter.write_into(target, offset + 8)
        self._publication_epoch_counter.write_into(target, offset + 16)
        self._progress_epoch_counter.write_into(target, offset + 24)
        return offset + 32

    def activity_counters(self) -> tuple[AtomicEpoch, AtomicEpoch, AtomicEpoch, AtomicEpoch]:
        """Return activity counters retained by this reserved finalizer escrow."""
        return (
            self._active_counter,
            self._publication_failures_counter,
            self._publication_epoch_counter,
            self._progress_epoch_counter,
        )

    def activity_is_quiescent(self) -> bool:
        """Return whether the supplied activity counters prove quiescence."""
        if self._fork_unusable_after_fork:
            return False
        if self.active_count() != 0:
            return False
        if self._publication_failures_counter.value() != 0:
            return False
        return True

    def inherited_roots(self) -> tuple[object, ...]:
        """Return inherited roots retained by this reserved finalizer escrow."""
        return (
            self._slots,
            self._states,
            self._generations,
            self._slot_locks,
            self._tickets,
            self._ticket_slots,
        )

    def prepare_for_fork(self) -> None:
        """Root inherited owners without ever raising from an at-fork callback."""
        self._fork_prepare_exhausted = False
        generation = self._fork_root_count
        if self._fork_fresh is None or generation >= _MAX_FORK_QUARANTINE_GENERATIONS:
            # CPython does not abort fork when a callback raises. Record an inert
            # child sentinel instead; reset_after_fork never touches inherited
            # locks and every publication/admission path then fails closed.
            self._fork_prepare_index = -2
            self._fork_prepare_exhausted = True
            return
        roots = self._fork_roots
        current = (
            self._slots,
            self._states,
            self._generations,
            self._slot_locks,
            self._tickets,
            self._ticket_slots,
            self._free_ring,
            self._reserve_lock,
            self._active_counter,
            self._published_counter,
            self._retired_counter,
            self._admission_rejections_counter,
            self._publication_failures_counter,
            self._publication_epoch_counter,
            self._progress_epoch_counter,
            self._fork_spare2,
        )
        base = generation * 16
        for offset, value in enumerate(current):
            roots[base + offset] = value
        self._fork_prepare_index = generation

    def clear_fork_preparation(self) -> None:
        """Parent drops only the newest temporary root generation."""
        generation = self._fork_prepare_index
        if generation >= 0:
            base = generation * 16
            for offset in range(16):
                self._fork_roots[base + offset] = None
        self._fork_prepare_index = -1
        self._fork_prepare_exhausted = False
        # _fork_fresh remains untouched and reusable in the parent.

    def reset_after_fork(self) -> None:
        """Swap once per child PID; duplicate module callbacks are harmless."""
        child_pid = os.getpid()
        if self._fork_prepare_index == -2 or self._fork_prepare_exhausted:
            self._pid = child_pid
            self._fork_prepare_index = -1
            self._fork_prepare_exhausted = False
            self._fork_unusable_after_fork = True
            self._post_fork_quarantine_pending = False
            return
        if self._fork_prepare_index < 0:
            return
        fresh = self._fork_fresh
        if fresh is None:
            fresh = self._make_fresh_bank()
        (
            self._slots,
            self._states,
            self._generations,
            self._slot_locks,
            self._tickets,
            self._ticket_slots,
            self._free_ring,
            self._reserve_lock,
            self._active_counter,
            self._published_counter,
            self._retired_counter,
            self._admission_rejections_counter,
            self._publication_failures_counter,
            self._publication_epoch_counter,
            self._progress_epoch_counter,
        ) = fresh
        self._free_head = 0
        self._free_tail = 0
        self._free_count = self._capacity
        self._consume_cursor = 0
        self._overflowed = False
        self._pid = child_pid
        prepared = self._fork_prepare_index
        if prepared >= 0 and prepared + 1 > self._fork_root_count:
            self._fork_root_count = prepared + 1
        self._fork_prepare_index = -1
        self._post_fork_quarantine_pending = True
        self._fork_unusable_after_fork = False
        # The second preallocated bank becomes the child's immediate next-fork bank.
        self._fork_fresh = self._fork_spare2
        self._fork_spare2 = None


__all__ = ["FinalizerEscrowCapacitySnapshot", "ReservedFinalizerEscrow"]
