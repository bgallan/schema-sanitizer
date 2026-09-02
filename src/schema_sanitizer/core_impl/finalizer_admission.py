"""Check cross-subsystem teardown capacity for finalizable resource owners.

Escrow capacities are aggregated from every participating subsystem and reported against active,
available, and overflow invariants before new finalizable work is admitted.
"""

from __future__ import annotations

from dataclasses import dataclass

from .finalizer_escrow import FinalizerEscrowCapacitySnapshot, ReservedFinalizerEscrow
from .finalizer_registry import registered_finalizer_escrows


@dataclass(frozen=True, slots=True)
class FinalizerAdmissionDomain:
    """Capacity and publication state for one registered finalizer domain."""

    name: str
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
            self.capacity >= 0
            and 0 <= self.active <= self.capacity
            and 0 <= self.retired <= self.capacity
            and 0 <= self.recycle_pending <= self.capacity
            and self.active + self.available + self.retired + self.recycle_pending == self.capacity
        )


@dataclass(frozen=True, slots=True)
class FinalizerAdmissionSnapshot:
    """Aggregate finalizer capacity and invariant status across domains."""

    domains: tuple[FinalizerAdmissionDomain, ...]
    total_capacity: int
    active: int
    available: int
    retired: int
    invariant_ok: bool
    admission_rejections: int = 0
    publication_failures: int = 0
    publication_epoch: int = 0
    progress_epoch: int = 0
    recycle_pending: int = 0


def _domain(name: str, escrow: ReservedFinalizerEscrow[object]) -> FinalizerAdmissionDomain:
    """Build one finalizer-admission domain from its escrow capacity snapshot."""
    snapshot: FinalizerEscrowCapacitySnapshot = escrow.capacity_snapshot()
    return FinalizerAdmissionDomain(
        name,
        snapshot.capacity,
        snapshot.active,
        snapshot.available,
        snapshot.retired,
        snapshot.overflowed,
        snapshot.admission_rejections,
        snapshot.publication_failures,
        snapshot.publication_epoch,
        snapshot.progress_epoch,
        snapshot.recycle_pending,
    )


def finalizer_admission_snapshot() -> FinalizerAdmissionSnapshot:
    """Return the exact envelope for every *loaded* finalizer owner domain.

    Finalizable modules register their escrows during normal import.  A domain
    that was never loaded cannot have admitted an owner, so shutdown does not
    need to import it merely to prove quiescence.
    """
    domains = tuple(_domain(name, escrow) for name, escrow in registered_finalizer_escrows())
    total = sum(item.capacity for item in domains)
    active = sum(item.active for item in domains)
    available = sum(item.available for item in domains)
    retired = sum(item.retired for item in domains)
    recycle_pending = sum(item.recycle_pending for item in domains)
    ok = (
        all(item.invariant_ok for item in domains)
        and active + available + retired + recycle_pending == total
    )
    admission_rejections = sum(item.admission_rejections for item in domains)
    publication_failures = sum(item.publication_failures for item in domains)
    publication_epoch = sum(item.publication_epoch for item in domains)
    progress_epoch = sum(item.progress_epoch for item in domains)
    return FinalizerAdmissionSnapshot(
        domains,
        total,
        active,
        available,
        retired,
        ok,
        admission_rejections,
        publication_failures,
        publication_epoch,
        progress_epoch,
        recycle_pending,
    )


__all__ = [
    "FinalizerAdmissionDomain",
    "FinalizerAdmissionSnapshot",
    "finalizer_admission_snapshot",
]
