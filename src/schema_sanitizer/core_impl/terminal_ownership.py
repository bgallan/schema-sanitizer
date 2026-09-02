"""Track terminal runtime ownership in a bounded metadata-only ledger.

Authoritative records live in a preallocated fixed-capacity bank, so publishing or
retiring ownership cannot grow containers under memory pressure. The ledger avoids a
second payload-owner graph and reports its metadata overhead explicitly in bytes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from threading import Lock
from time import monotonic_ns

from .diagnostic_epoch import diagnostic_transition

_MAX_TERMINAL_OWNERS = 8192
_MAX_LABEL_CHARS = 64
# Conservative resident attribution for one physically preallocated record.
# This is diagnostic/accounting metadata, not a second payload ownership charge.
_TERMINAL_OWNER_RECORD_BYTES = 128


def _bounded_label(value: object) -> str:
    """Validate and truncate a terminal-ownership label to its fixed bound."""
    if type(value) is not str:
        raise TypeError("terminal ownership labels must be exact strings")
    return value if len(value) <= _MAX_LABEL_CHARS else value[:_MAX_LABEL_CHARS]


@dataclass(frozen=True, slots=True)
class TerminalOwnershipSnapshot:
    """Describe retryable terminal owners and their retained byte attribution."""

    owners: int
    retained_bytes: int
    metadata_bytes: int
    capacity: int
    rejected: int
    oldest_since_ns: int
    generation: int
    generation_exhausted: bool = False
    categories: tuple[tuple[str, int], ...] = ()
    corrupted: bool = False

    @property
    def total_attributed_bytes(self) -> int:
        """Return payload-retention plus terminal-ledger metadata attribution."""
        return self.retained_bytes + self.metadata_bytes


@dataclass(slots=True)
class _TerminalOwnerSlot:
    """One preallocated authoritative terminal metadata record."""

    active: bool = False
    category: str = ""
    token: int = 0
    retained_bytes: int = 0
    since_ns: int = 0
    generation: int = 0

    def clear(self) -> None:
        """Clear values and ownership retained by this terminal owner slot."""
        self.active = False
        self.category = ""
        self.token = 0
        self.retained_bytes = 0
        self.since_ns = 0
        self.generation = 0


class TerminalOwnershipLedger:
    """Process-local bounded metadata authority for terminal owners."""

    def __init__(self, capacity: int = _MAX_TERMINAL_OWNERS) -> None:
        """Initialize the terminal ownership ledger and its owned runtime state."""
        if type(capacity) is not int:
            raise TypeError("terminal ownership capacity must be an exact integer")
        if capacity <= 0 or capacity > _MAX_TERMINAL_OWNERS:
            raise ValueError(f"terminal ownership capacity must be in [1, {_MAX_TERMINAL_OWNERS}]")
        self._capacity = capacity
        # Allocate the complete physical metadata bank before any terminal owner
        # can exist.  The bank is reused across normal operation and fork reset.
        self._slots = [_TerminalOwnerSlot() for _ in range(self._capacity)]
        self._reset(os.getpid())

    def _reset(self, pid: int) -> None:
        """Reset process-local state owned by this terminal ownership ledger."""
        self._pid = pid
        # A child must not touch the parent's inherited lock.  Replacing this one
        # small synchronization primitive is the only post-fork allocation; the
        # terminal record bank itself is already physically present.
        self._lock = Lock()
        for slot in self._slots:
            slot.clear()
        self._owners = 0
        self._generation = 0
        self._generation_exhausted = False
        self._rejected = 0
        self._rejected_latched = False
        self._corrupted = False

    def _ensure_process(self) -> None:
        """Ensure the owner still belongs to the active process."""
        if self._pid != os.getpid():
            self._reset(os.getpid())

    def _prepare_generation_advance_locked(self) -> int | None:
        """Prepare generation advance while holding the governing lock."""
        if self._generation_exhausted:
            return None
        if self._generation >= (1 << 63) - 1:
            self._generation_exhausted = True
            self._rejected_latched = True
            return None
        return self._generation + 1

    def _find_slot_locked(self, category: str, token: int) -> _TerminalOwnerSlot | None:
        # Full bounded scan is deletion-safe and cannot allocate.  Terminal
        # publication is not a hot data-plane path; correctness under pressure is
        # preferred to an auxiliary dynamic hash index that would itself need
        # ownership and failure handling.
        """Find slot while holding the governing lock."""
        for slot in self._slots:
            if slot.active and slot.token == token and slot.category == category:
                return slot
        return None

    def _find_free_slot_locked(self) -> _TerminalOwnerSlot | None:
        """Find free slot while holding the governing lock."""
        for slot in self._slots:
            if not slot.active:
                return slot
        return None

    def _record_rejection_locked(self) -> None:
        """Latch lost-proof state without trusting diagnostic arithmetic."""
        self._rejected_latched = True
        # Diagnostic counters must never turn a capacity rejection into an OOM.
        # ``type(...) is int`` also protects against hostile/coercive subclasses
        # whose arithmetic methods allocate or throw.
        if type(self._rejected) is not int:
            self._rejected = 1
            return
        if self._rejected < (1 << 63) - 1:
            self._rejected += 1

    def _active_slot_count_locked(self) -> int:
        """Return authoritative owner count from the fixed slot bank."""
        active = 0
        for slot in self._slots:
            if slot.active:
                active += 1
        return active

    def _validate_owner_cache_locked(self) -> int:
        """Quarantine publication if the advisory owner cache diverges."""
        authoritative = self._active_slot_count_locked()
        if type(self._owners) is not int or self._owners != authoritative:
            self._corrupted = True
            self._record_rejection_locked()
        return authoritative

    def publish(
        self,
        category: str,
        token: int,
        *,
        retained_bytes: int = 0,
    ) -> bool:
        """Publish the prepared value."""
        self._ensure_process()
        category = _bounded_label(category)
        if type(token) is not int:
            raise TypeError("terminal ownership token must be an exact integer")
        if type(retained_bytes) is not int:
            raise TypeError("terminal retained_bytes must be an exact integer")
        if retained_bytes < 0:
            raise ValueError("terminal retained_bytes must be >= 0")
        now = monotonic_ns()
        with self._lock:
            authoritative = self._validate_owner_cache_locked()
            if self._corrupted:
                # Existing exact slots remain cleanup-authoritative, but no new
                # terminal ownership may be admitted after disagreement.
                diagnostic_transition()
                return False
            existing = self._find_slot_locked(category, token)
            if existing is not None:
                if retained_bytes != existing.retained_bytes:
                    next_generation = self._prepare_generation_advance_locked()
                    if next_generation is None:
                        return False
                    existing.retained_bytes = retained_bytes
                    self._generation = next_generation
                    existing.generation = next_generation
                    diagnostic_transition()
                return True
            if authoritative >= self._capacity:
                self._record_rejection_locked()
                diagnostic_transition()
                return False
            next_generation = self._prepare_generation_advance_locked()
            if next_generation is None:
                return False
            slot = self._find_free_slot_locked()
            if slot is None:
                # Slots are the authority: no free slot means no capacity even
                # if a stale auxiliary count would have claimed otherwise.
                self._corrupted = True
                self._record_rejection_locked()
                diagnostic_transition()
                return False
            next_owners = authoritative + 1
            slot.category = category
            slot.token = token
            slot.retained_bytes = retained_bytes
            slot.since_ns = now
            slot.generation = next_generation
            slot.active = True  # publication commit point
            self._owners = next_owners
            self._generation = next_generation
            diagnostic_transition()
            return True

    def retire(self, category: str, token: int) -> None:
        """Retire the retained runtime entry."""
        self._ensure_process()
        category = _bounded_label(category)
        if type(token) is not int:
            return
        with self._lock:
            authoritative = self._validate_owner_cache_locked()
            slot = self._find_slot_locked(category, token)
            if slot is None:
                return
            # The slot itself is the exact cleanup capability.  Even after a
            # cache mismatch closes publication, cleanup must be able to retire
            # this proof rather than strand it forever.
            slot.active = False
            slot.clear()
            self._owners = authoritative - 1
            next_generation = self._prepare_generation_advance_locked()
            if next_generation is not None:
                self._generation = next_generation
            diagnostic_transition()

    def retire_category(self, category: str) -> None:
        """Retire every terminal owner in the requested category."""
        self._ensure_process()
        category = _bounded_label(category)
        with self._lock:
            # No tuple/list of keys is constructed under terminal pressure.
            authoritative = self._validate_owner_cache_locked()
            retired = 0
            for slot in self._slots:
                if slot.active and slot.category == category:
                    retired += 1
            if not retired:
                return
            for slot in self._slots:
                if slot.active and slot.category == category:
                    slot.active = False
                    slot.clear()
            self._owners = authoritative - retired
            next_generation = self._prepare_generation_advance_locked()
            if next_generation is not None:
                self._generation = next_generation
            diagnostic_transition()

    def snapshot(self) -> TerminalOwnershipSnapshot:
        """Return a bounded snapshot of the current terminal ownership ledger."""
        self._ensure_process()
        with self._lock:
            # Snapshot construction is advisory/read-only and may allocate; the
            # authoritative terminal publish/retire paths above never do.
            counts: dict[str, int] = {}
            retained = 0
            oldest = 0
            owners = 0
            for slot in self._slots:
                if not slot.active:
                    continue
                owners += 1
                counts[slot.category] = counts.get(slot.category, 0) + 1
                retained += slot.retained_bytes
                if oldest == 0 or slot.since_ns < oldest:
                    oldest = slot.since_ns
            metadata_bytes = owners * _TERMINAL_OWNER_RECORD_BYTES
            return TerminalOwnershipSnapshot(
                owners=owners,
                retained_bytes=retained,
                metadata_bytes=metadata_bytes,
                capacity=self._capacity,
                rejected=max(1, self._rejected) if self._rejected_latched else self._rejected,
                oldest_since_ns=oldest,
                generation=self._generation,
                generation_exhausted=self._generation_exhausted,
                categories=tuple(sorted(counts.items())),
                corrupted=self._corrupted,
            )

    def reset_after_fork(self) -> None:
        """Reset process-local state inherited across a fork."""
        self._reset(os.getpid())


_TERMINAL_OWNERSHIP = TerminalOwnershipLedger()


def publish_terminal_owner(
    category: str,
    token: int,
    *,
    retained_bytes: int = 0,
) -> bool:
    """Publish one retryable terminal owner under an exact category/token key."""
    return _TERMINAL_OWNERSHIP.publish(category, token, retained_bytes=retained_bytes)


def retire_terminal_owner(category: str, token: int) -> None:
    """Retire one exact terminal owner."""
    _TERMINAL_OWNERSHIP.retire(category, token)


def retire_terminal_category(category: str) -> None:
    """Retire every terminal owner in ``category`` after subsystem cleanup."""
    _TERMINAL_OWNERSHIP.retire_category(category)


def terminal_ownership_snapshot() -> TerminalOwnershipSnapshot:
    """Return process terminal-ownership diagnostics."""
    return _TERMINAL_OWNERSHIP.snapshot()


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("terminal-ownership", mode="quarantine_only")


from .shutdown_observers import (  # noqa: E402
    register_shutdown_observer as _register_shutdown_observer,
)

_register_shutdown_observer("terminal_ownership", terminal_ownership_snapshot)


__all__ = [
    "TerminalOwnershipLedger",
    "TerminalOwnershipSnapshot",
    "publish_terminal_owner",
    "retire_terminal_category",
    "retire_terminal_owner",
    "terminal_ownership_snapshot",
]
