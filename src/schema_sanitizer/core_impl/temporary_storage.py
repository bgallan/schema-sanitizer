"""Provide operation-wide permits for bounded temporary filesystem usage.

Exact byte leases, resizing, stream reservations, finalizer recovery, diagnostics, and the current
operation context share one accounting boundary.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from threading import Condition, Lock
from time import monotonic
from typing import cast

from ..errors import SchemaSanitizerResourceError
from .bounded_generation import next_reusable_token
from .cancellation import check_operation_cancelled
from .control_plane_budget import ControlPlaneTicket, release_control_plane, reserve_control_plane
from .finalization import runtime_is_finalizing
from .finalizer_escrow import ReservedFinalizerEscrow
from .memory_budget import memory_budget
from .rooted_finalizer import FinalizerReplayCapability, RootedFinalizerAuthority
from .safe_errors import add_bounded_note
from .temporary_storage_governor import (
    _PROCESS_TEMPORARY_STORAGE,
    ProcessTemporaryStorageCapability,
    ProcessTemporaryStorageDiagnostics,
    ProcessTemporaryStorageSnapshot,
    process_temporary_storage_diagnostics,
    process_temporary_storage_snapshot,
)


@dataclass(frozen=True, slots=True)
class TemporaryStorageDiagnostics:
    """Cleanup anomalies and live bytes observed when an operation closes."""

    close_outstanding_bytes: int
    close_active_leases: int
    over_release_count: int
    over_release_bytes: int


@dataclass(frozen=True, slots=True)
class TemporaryStorageSnapshot:
    """Immutable diagnostics for one temporary-storage permit pool."""

    limit_bytes: int
    reserved_bytes: int
    peak_reserved_bytes: int
    active_leases: int
    deferred_finalizer_leases: int = 0


@dataclass(slots=True)
class _StorageLeaseEntry:
    owner_id: int
    capability: object
    reserved_bytes: int
    filesystem_key: int
    filesystem_path: Path
    inode_count: int
    process_capability: ProcessTemporaryStorageCapability
    control_ticket: ControlPlaneTicket | None = None
    resize_inflight: bool = False
    resize_reconcile: bool = False
    resize_target_bytes: int = 0
    resize_target_key: int = -1
    resize_target_path: Path | None = None
    resize_pending_delta: int = 0
    resize_pending_growth: int = 0
    # Exact process capability returned by an external resize before the pool
    # has completed its local reconcile.  This pre-existing owner slot prevents
    # a post-commit exception from leaving the replacement stack-only.
    resize_replacement: ProcessTemporaryStorageCapability | None = None
    release_inflight: bool = False
    process_released: bool = False
    local_released: bool = False


class _StorageLeasePublication:
    __slots__ = ("lease_id", "capability")

    def __init__(self, capability: object) -> None:
        """Initialize the storage lease publication and its owned runtime state."""
        self.lease_id = 0
        self.capability = capability

    def __iter__(self):
        """Iterate over the retained values."""
        yield self.lease_id
        yield self.capability


class _StorageResizeResult:
    __slots__ = ("requested", "filesystem_key", "filesystem_path")

    def __init__(
        self,
        requested: int,
        filesystem_key: int,
        filesystem_path: Path,
    ) -> None:
        """Initialize the storage resize result and its owned runtime state."""
        self.requested = requested
        self.filesystem_key = filesystem_key
        self.filesystem_path = filesystem_path

    def __iter__(self):
        """Iterate over the retained values."""
        yield self.requested
        yield self.filesystem_key
        yield self.filesystem_path


_MAX_TEMP_STORAGE_FINALIZER_OWNERS = 16384
_TEMP_STORAGE_FINALIZER_ESCROW: ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
    ReservedFinalizerEscrow(_MAX_TEMP_STORAGE_FINALIZER_OWNERS, static_kind="temporary_storage")
)
_TEMP_STORAGE_FINALIZER_OVERFLOWS = 0
_TEMP_STORAGE_FINALIZER_OVERFLOWED = False


def _mark_temporary_storage_finalizer_overflow() -> None:
    """Latch overflow without letting interpreter teardown escape ``__del__``."""
    global _TEMP_STORAGE_FINALIZER_OVERFLOWS, _TEMP_STORAGE_FINALIZER_OVERFLOWED
    try:
        _TEMP_STORAGE_FINALIZER_OVERFLOWED = True
        current = _TEMP_STORAGE_FINALIZER_OVERFLOWS
        if type(current) is int:
            _TEMP_STORAGE_FINALIZER_OVERFLOWS = current + 1
    except BaseException:
        pass


def _run_temporary_storage_finalizer(authority: RootedFinalizerAuthority) -> None:
    """Release exact temporary-storage authority without the wrapper object."""
    pool = authority.arg0
    if pool is None:
        return
    lease_id = int(cast(int, authority.arg1) or 0)
    capability = authority.arg2
    owner_id = int(cast(int, authority.arg3) or 0)
    if lease_id > 0 and capability is not None:
        if not isinstance(pool, TemporaryStoragePermitPool):
            raise RuntimeError("temporary-storage finalizer lost its permit pool")
        pool._release_lease_authority(lease_id, owner_id, capability)
        authority.arg1 = 0
        authority.arg2 = None
        return
    process_capability = authority.arg4
    if process_capability is not None:
        exact_process_capability = cast(ProcessTemporaryStorageCapability, process_capability)
        if not _PROCESS_TEMPORARY_STORAGE.release_capability(exact_process_capability):
            if exact_process_capability.active:
                raise RuntimeError(
                    "orphan temporary-storage process capability is not authoritative"
                )
        authority.arg4 = None
    control_ticket = authority.arg5
    if control_ticket is not None:
        if not release_control_plane(cast(ControlPlaneTicket, control_ticket)):
            raise RuntimeError("temporary-storage orphan control retirement did not commit")
        authority.arg5 = None


class TemporaryStorageLease:
    """Own one byte reservation until its staged artifact is released."""

    def __init__(
        self,
        pool: TemporaryStoragePermitPool,
        reserved_bytes: int,
        *,
        label: str,
        filesystem_key: int,
        filesystem_path: Path,
        inode_count: int,
    ) -> None:
        """Store the pool, reservation size, filesystem, and label."""
        self._pool = pool
        self._pid = os.getpid()
        self._reserved_bytes = reserved_bytes
        self._filesystem_key = filesystem_key
        self._filesystem_path = filesystem_path
        self._inode_count = max(0, int(inode_count))
        self.label = label
        self._lock = Lock()
        self._lease_id = 0
        self._capability: object | None = None
        self._finalizer_ticket = -1
        self._finalizer_owner = RootedFinalizerAuthority(_run_temporary_storage_finalizer)
        self._finalizer_owner.arg0 = pool
        self._finalizer_owner.arg3 = id(self)
        self._orphan_process_capability: ProcessTemporaryStorageCapability | None = None
        self._orphan_control_ticket: ControlPlaneTicket | None = None
        self._released = True

    def _activate(
        self,
        filesystem_key: int,
        *,
        lease_id: int,
        capability: object,
        finalizer_ticket: int,
    ) -> None:
        """Publish an already-admitted lease without any rollback side effect."""
        self._filesystem_key = filesystem_key
        self._lease_id = lease_id
        self._capability = capability
        self._finalizer_ticket = finalizer_ticket
        owner = self._finalizer_owner
        owner.ticket = finalizer_ticket
        owner.arg1 = lease_id
        owner.arg2 = capability
        owner.arg3 = id(self)
        self._released = False

    def _activate_cleanup_owner(self, *, finalizer_ticket: int) -> None:
        """Arm cleanup for exact authorities retained after publication failure."""
        self._finalizer_ticket = finalizer_ticket
        self._finalizer_owner.ticket = finalizer_ticket
        self._released = False

    @property
    def reserved_bytes(self) -> int:
        """Return the currently reserved byte count."""
        if os.getpid() != self._pid:
            return 0
        with self._lock:
            if self._released:
                return 0
            if not self._lease_id:
                return self._reserved_bytes
            return self._pool._lease_reserved_bytes(self)

    def resize(self, size_bytes: int, *, path: str | Path | None = None) -> None:
        """Resize this reservation atomically against concurrent release."""
        if os.getpid() != self._pid:
            raise RuntimeError("temporary-storage lease cannot be reused after fork")
        with self._lock:
            if self._released:
                raise RuntimeError("temporary-storage lease is already released")
            if not self._lease_id:
                raise RuntimeError("temporary-storage lease has no exact authority")
            effective_path = self._filesystem_path if path is None else Path(path)
            resize_result = self._pool._resize_lease(self, size_bytes, path=effective_path)
            self._reserved_bytes = resize_result.requested
            self._filesystem_key = resize_result.filesystem_key
            self._filesystem_path = resize_result.filesystem_path

    def adjust(self, delta_bytes: int, *, path: str | Path | None = None) -> int:
        """Atomically add or subtract bytes and return the new reservation."""
        if os.getpid() != self._pid:
            raise RuntimeError("temporary-storage lease cannot be reused after fork")
        if isinstance(delta_bytes, bool) or not isinstance(delta_bytes, int):
            raise TypeError("temporary-storage adjustment must be an integer")
        with self._lock:
            if self._released:
                raise RuntimeError("temporary-storage lease is already released")
            if not self._lease_id:
                raise RuntimeError("temporary-storage lease has no exact authority")
            requested_size = self._reserved_bytes + delta_bytes
            if requested_size < 0:
                raise ValueError("temporary-storage adjustment exceeds the active lease")
            effective_path = self._filesystem_path if path is None else Path(path)
            resize_result = self._pool._resize_lease(self, requested_size, path=effective_path)
            self._reserved_bytes = resize_result.requested
            self._filesystem_key = resize_result.filesystem_key
            self._filesystem_path = resize_result.filesystem_path
            return resize_result.requested

    def release(self) -> None:
        """Return this reservation exactly once across competing threads."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            owner = self._finalizer_owner
            if self._released:
                ticket = self._finalizer_ticket
                owner.make_ack_only()
                if owner.is_armed_for(ticket):
                    self._finalizer_ticket = -1
                    return
                if ticket >= 0:
                    if _TEMP_STORAGE_FINALIZER_ESCROW.release_ticket(ticket):
                        self._finalizer_ticket = -1
                        owner.clear()
                    elif not _TEMP_STORAGE_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                        raise RuntimeError(
                            "temporary-storage finalizer ACK publication did not commit"
                        )
                return

            if self._lease_id:
                self._pool._release_lease(self)
                owner.arg1 = 0
                owner.arg2 = None
            else:
                orphan_process = self._orphan_process_capability
                if orphan_process is not None:
                    if not _PROCESS_TEMPORARY_STORAGE.release_capability(orphan_process):
                        if orphan_process.active:
                            raise RuntimeError(
                                "orphan temporary-storage process capability is not authoritative"
                            )
                    self._orphan_process_capability = None
                    owner.arg4 = None
                orphan_control = self._orphan_control_ticket
                if orphan_control is not None:
                    if not release_control_plane(orphan_control):
                        raise RuntimeError(
                            "temporary-storage orphan control retirement did not commit"
                        )
                    self._orphan_control_ticket = None
                    owner.arg5 = None

            self._released = True
            self._reserved_bytes = 0
            self._inode_count = 0
            owner.make_ack_only()
            ticket = self._finalizer_ticket
            if ticket >= 0:
                if _TEMP_STORAGE_FINALIZER_ESCROW.release_ticket(ticket):
                    self._finalizer_ticket = -1
                    owner.clear()
                else:
                    if not _TEMP_STORAGE_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                        raise RuntimeError(
                            "temporary-storage finalizer ACK publication did not commit"
                        )
                    raise RuntimeError("temporary-storage finalizer slot retirement did not commit")

    def __enter__(self) -> TemporaryStorageLease:
        """Return this active lease."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Release this lease."""
        self.release()

    def __del__(self) -> None:
        """Arm the pre-rooted authority without blocking the GC thread."""
        try:
            if runtime_is_finalizing():
                return
            if os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", -1)
            owner = getattr(self, "_finalizer_owner", None)
            if ticket < 0 and isinstance(owner, RootedFinalizerAuthority):
                ticket = owner.ticket
            if ticket < 0 or not isinstance(owner, RootedFinalizerAuthority):
                return
            if getattr(self, "_released", True):
                owner.make_ack_only()
            if _TEMP_STORAGE_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                self._finalizer_ticket = -1
                return
            _mark_temporary_storage_finalizer_overflow()
        except BaseException:
            try:
                _mark_temporary_storage_finalizer_overflow()
            except BaseException:
                pass


def drain_temporary_storage_finalizers() -> int:
    """Run deferred lease releases without unrooting before commit."""
    drained = 0

    def process(ticket: int, value: RootedFinalizerAuthority) -> None:
        """Process one retained work item."""
        nonlocal drained
        if isinstance(value, RootedFinalizerAuthority):
            value.ticket = ticket
            value.run()
            value.clear()
            drained += 1
            return
        raise RuntimeError("unknown temporary-storage finalizer owner")

    while True:
        try:
            if not _TEMP_STORAGE_FINALIZER_ESCROW.process_one(process):
                break
        except BaseException:
            break
    return drained


def temporary_storage_finalizer_snapshot() -> tuple[int, int, int]:
    """Return reserved tickets, published owners and irreversible overflows."""
    return (
        _TEMP_STORAGE_FINALIZER_ESCROW.reserved_count(),
        _TEMP_STORAGE_FINALIZER_ESCROW.published_count(),
        max(1, _TEMP_STORAGE_FINALIZER_OVERFLOWS)
        if (_TEMP_STORAGE_FINALIZER_OVERFLOWED or _TEMP_STORAGE_FINALIZER_ESCROW.overflowed)
        else _TEMP_STORAGE_FINALIZER_OVERFLOWS,
    )


class StreamingStorageReservation:
    """Grow one shared storage lease before each streamed write.

    ``initial_credit_bytes`` is the caller's existing estimate for this file.
    Growth beyond that credit is reserved in amortized blocks before bytes reach
    disk. Finalization reconciles the shared lease to the exact file size.
    """

    def __init__(
        self,
        lease: TemporaryStorageLease | None,
        *,
        initial_credit_bytes: int,
        path: str | Path,
        quantum_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        """Initialize the streaming storage reservation and its owned runtime state."""
        self._lease = lease
        self._credit = max(0, int(initial_credit_bytes))
        self._extra = 0
        self._written = 0
        self._path = Path(path)
        configured_quantum = max(64 * 1024, int(quantum_bytes))
        pool_limit = lease._pool.limit_bytes if lease is not None else None
        if pool_limit is not None and pool_limit > 0:
            # Amortization must never request a block larger than the complete
            # artifact window.  Tiny operation budgets otherwise reject the
            # first short write merely because the default quantum is 4 MiB.
            configured_quantum = max(1, min(configured_quantum, pool_limit))
        self._quantum = configured_quantum
        self._lock = Lock()

    def before_write(self, chunk_bytes: int) -> None:
        """Reserve any required growth before writing one chunk."""
        amount = max(0, int(chunk_bytes))
        if amount == 0:
            return
        check_operation_cancelled(stage="temporary_stream_write")
        with self._lock:
            desired = self._written + amount
            covered = self._credit + self._extra
            if self._lease is not None and desired > covered:
                shortage = desired - covered
                growth = ((shortage + self._quantum - 1) // self._quantum) * self._quantum
                self._lease.adjust(growth, path=self._path)
                self._extra += growth
            self._written = desired

    def reset_after_truncate(self) -> None:
        """Return retry-only growth after a failed attempt truncates the file."""
        with self._lock:
            if self._lease is not None and self._extra:
                self._lease.adjust(-self._extra, path=self._path)
            self._extra = 0
            self._written = 0

    def finalize(self, actual_size_bytes: int | None = None) -> None:
        """Reconcile this file's credit to its exact retained size."""
        actual = (
            self._path.stat().st_size
            if actual_size_bytes is None
            else max(0, int(actual_size_bytes))
        )
        with self._lock:
            reserved = self._credit + self._extra
            if self._lease is not None and actual != reserved:
                self._lease.adjust(actual - reserved, path=self._path)
            self._credit = actual
            self._extra = 0
            self._written = actual


class TemporaryStoragePermitPool:
    """Bound operation-owned staging bytes without adding a public option."""

    def __init__(self, memory_limit_bytes: int | None) -> None:
        """Derive the spool ceiling from the canonical memory budget."""
        self.limit_bytes = memory_budget(memory_limit_bytes).replay_spool_bytes
        self._lock = Lock()
        self._condition = Condition(self._lock)
        self._reserved_bytes = 0
        self._pending_reserved_bytes = 0
        self._pending_active_leases = 0
        self._resize_inflight = 0
        self._pending_resize_growth = 0
        self._peak_reserved_bytes = 0
        self._active_leases = 0
        self._closed = False
        self._close_complete = False
        self._close_outstanding_bytes = 0
        self._close_active_leases = 0
        self._over_release_count = 0
        self._over_release_bytes = 0
        self._lease_sequence = 0
        self._leases: dict[int, _StorageLeaseEntry] = {}
        self._unknown_lease_releases = 0
        self._protocol_violations = 0

    def _finish_resize_inflight_locked(self) -> None:
        """Decrement a quiescence latch without hiding protocol underflow."""
        if self._resize_inflight <= 0:
            self._protocol_violations += 1
            return
        self._resize_inflight -= 1

    def _consume_pending_resize_growth_locked(self, amount: int) -> bool:
        """Remove an admission byte charge without ever under-reporting it."""
        if amount < 0 or self._pending_resize_growth < amount:
            self._protocol_violations += 1
            return False
        self._pending_resize_growth -= amount
        return True

    def _finish_pending_admission_locked(self, requested: int) -> None:
        """Retire one pending lease without manufacturing quiescence on underflow."""
        violated = False
        if self._pending_active_leases <= 0:
            violated = True
        else:
            self._pending_active_leases -= 1
        if requested < 0 or self._pending_reserved_bytes < requested:
            # Keep the conservative charge. Dropping it to zero could admit new
            # storage despite an unresolved ownership/accounting violation.
            violated = True
        else:
            self._pending_reserved_bytes -= requested
        if violated:
            self._protocol_violations += 1

    def _finish_active_lease_locked(self, amount: int) -> None:
        """Retire local lease counters with a sticky protocol-failure latch."""
        violated = False
        if self._active_leases <= 0:
            violated = True
        else:
            self._active_leases -= 1
        if amount < 0 or self._reserved_bytes < amount:
            # Preserve the high-water charge until an authoritative reconcile.
            violated = True
        else:
            self._reserved_bytes -= amount
        if violated:
            self._protocol_violations += 1

    def _publish_lease_locked(
        self,
        lease: TemporaryStorageLease,
        *,
        reserved_bytes: int,
        filesystem_key: int,
        filesystem_path: Path,
        inode_count: int,
        process_capability: ProcessTemporaryStorageCapability,
        control_ticket: ControlPlaneTicket,
    ) -> _StorageLeasePublication:
        """Publish lease while holding the governing lock."""
        lease_id = next_reusable_token(self._lease_sequence, self._leases)
        if lease_id is None:
            raise RuntimeError("temporary-storage lease namespace exhausted")
        capability = FinalizerReplayCapability()
        publication = _StorageLeasePublication(capability)
        self._leases[lease_id] = _StorageLeaseEntry(
            id(lease),
            capability,
            reserved_bytes,
            filesystem_key,
            filesystem_path,
            inode_count,
            process_capability,
            control_ticket,
        )
        self._lease_sequence = lease_id
        publication.lease_id = lease_id
        return publication

    def _lease_entry_authority_locked(
        self, lease_id: int, owner_id: int, capability: object
    ) -> _StorageLeaseEntry:
        """Return the lease-entry authority while holding the pool lock."""
        entry = self._leases.get(lease_id)
        if entry is None or entry.owner_id != owner_id or capability is not entry.capability:
            self._unknown_lease_releases += 1
            raise RuntimeError("temporary-storage lease is not authoritative")
        return entry

    def _lease_entry_locked(self, lease: TemporaryStorageLease) -> _StorageLeaseEntry:
        """Return the live lease entry while holding the pool lock."""
        return self._lease_entry_authority_locked(lease._lease_id, id(lease), lease._capability)

    def _lease_reserved_bytes(self, lease: TemporaryStorageLease) -> int:
        """Return the lease reserved bytes."""
        with self._condition:
            return self._lease_entry_locked(lease).reserved_bytes

    def try_acquire(
        self,
        size_bytes: int,
        *,
        label: str,
        path: str | Path | None = None,
        artifact_count: int = 1,
    ) -> TemporaryStorageLease | None:
        """Reserve bytes without holding the operation lock across filesystem I/O."""
        drain_temporary_storage_finalizers()
        check_operation_cancelled(stage="temporary_storage_admission")
        requested = self._normalize_size(size_bytes)
        inode_count = self._normalize_artifact_count(artifact_count)
        self._validate_one_artifact(requested, label=label)
        filesystem_key, filesystem_path, _free_bytes = _PROCESS_TEMPORARY_STORAGE.filesystem(path)
        # Construct the rollback owner before reserving its exact generation.
        lease = TemporaryStorageLease(
            self,
            requested,
            label=label,
            filesystem_key=filesystem_key,
            filesystem_path=filesystem_path,
            inode_count=inode_count,
        )
        try:
            finalizer_ticket = _TEMP_STORAGE_FINALIZER_ESCROW.reserve_rooted(lease._finalizer_owner)
            if finalizer_ticket is None:
                raise SchemaSanitizerResourceError(
                    "temporary-storage finalizer escrow capacity exhausted",
                    detail={
                        "stage": "temporary_storage",
                        "limit_name": "temporary_storage_finalizer_owners",
                        "limit_items": _MAX_TEMP_STORAGE_FINALIZER_OWNERS,
                        "actual_items": _MAX_TEMP_STORAGE_FINALIZER_OWNERS + 1,
                    },
                )
            lease._finalizer_ticket = finalizer_ticket
        except BaseException:
            try:
                _TEMP_STORAGE_FINALIZER_ESCROW.release_rooted_owner(lease._finalizer_owner)
            except BaseException:
                pass
            raise
        with self._condition:
            if self._closed:
                lease._finalizer_owner.make_ack_only()
                if _TEMP_STORAGE_FINALIZER_ESCROW.release_ticket(finalizer_ticket):
                    lease._finalizer_ticket = -1
                raise RuntimeError("temporary-storage permit pool is closed")
            next_reserved = self._reserved_bytes + self._pending_reserved_bytes + requested
            if next_reserved > self.limit_bytes:
                lease._finalizer_owner.make_ack_only()
                if _TEMP_STORAGE_FINALIZER_ESCROW.release_ticket(finalizer_ticket):
                    lease._finalizer_ticket = -1
                return None
            next_pending_reserved = self._pending_reserved_bytes + requested
            next_pending_leases = self._pending_active_leases + 1
            self._pending_reserved_bytes = next_pending_reserved
            self._pending_active_leases = next_pending_leases

        process_capability: ProcessTemporaryStorageCapability | None = None
        try:
            process_capability = _PROCESS_TEMPORARY_STORAGE.reserve_capability(
                requested,
                path=filesystem_path,
                label=label,
                inode_count=inode_count,
            )
            actual_filesystem_key = process_capability.device
        except BaseException:
            lease._finalizer_owner.make_ack_only()
            if _TEMP_STORAGE_FINALIZER_ESCROW.release_ticket(finalizer_ticket):
                lease._finalizer_ticket = -1
            with self._condition:
                self._finish_pending_admission_locked(requested)
                self._condition.notify_all()
            raise

        control_ticket: ControlPlaneTicket | None = None
        try:
            control_ticket = reserve_control_plane("temporary_storage_lease", 384)
            with self._condition:
                if self._pending_reserved_bytes < requested or self._pending_active_leases <= 0:
                    self._protocol_violations += 1
                    raise RuntimeError("temporary-storage pending admission underflow")
                next_pending_reserved = self._pending_reserved_bytes - requested
                next_pending_leases = self._pending_active_leases - 1
                next_reserved = self._reserved_bytes + requested
                next_active = self._active_leases + 1
                next_peak = max(self._peak_reserved_bytes, next_reserved)
                lease_id = 0
                try:
                    publication = self._publish_lease_locked(
                        lease,
                        reserved_bytes=requested,
                        filesystem_key=actual_filesystem_key,
                        filesystem_path=filesystem_path,
                        inode_count=inode_count,
                        process_capability=process_capability,
                        control_ticket=control_ticket,
                    )
                    lease_id = publication.lease_id
                    lease._activate(
                        actual_filesystem_key,
                        lease_id=lease_id,
                        capability=publication.capability,
                        finalizer_ticket=finalizer_ticket,
                    )
                except BaseException:
                    if lease_id:
                        self._leases.pop(lease_id, None)
                    raise
                self._pending_reserved_bytes = next_pending_reserved
                self._pending_active_leases = next_pending_leases
                self._reserved_bytes = next_reserved
                self._peak_reserved_bytes = next_peak
                self._active_leases = next_active
                self._condition.notify_all()
                control_ticket = None
                return lease
        except BaseException as primary:
            with self._condition:
                self._finish_pending_admission_locked(requested)
                self._condition.notify_all()
            orphaned = False
            try:
                if not _PROCESS_TEMPORARY_STORAGE.release_capability(process_capability):
                    raise RuntimeError(
                        "temporary-storage rollback capability was not authoritative"
                    )
            except BaseException as cleanup_error:
                orphaned = True
                lease._orphan_process_capability = process_capability
                lease._finalizer_owner.arg4 = process_capability
                add_bounded_note(
                    primary, "temporary-storage lease publication rollback failed", cleanup_error
                )
            if control_ticket is not None:
                try:
                    if not release_control_plane(control_ticket):
                        raise RuntimeError(
                            "temporary-storage control-ticket rollback did not commit"
                        )
                except BaseException as cleanup_error:
                    orphaned = True
                    lease._orphan_control_ticket = control_ticket
                    lease._finalizer_owner.arg5 = control_ticket
                    add_bounded_note(
                        primary, "temporary-storage control-ticket rollback failed", cleanup_error
                    )
            if orphaned:
                lease._activate_cleanup_owner(finalizer_ticket=finalizer_ticket)
            else:
                lease._finalizer_owner.make_ack_only()
                if _TEMP_STORAGE_FINALIZER_ESCROW.release_ticket(finalizer_ticket):
                    lease._finalizer_ticket = -1
                    lease._finalizer_owner.ticket = 0
                    lease._finalizer_owner.clear()
            raise

    def acquire(
        self,
        size_bytes: int,
        *,
        label: str,
        path: str | Path | None = None,
        artifact_count: int = 1,
    ) -> TemporaryStorageLease:
        """Reserve bytes or raise when the operation window is exhausted."""
        lease = self.try_acquire(
            size_bytes,
            label=label,
            path=path,
            artifact_count=artifact_count,
        )
        if lease is None:
            snapshot = self.snapshot()
            raise SchemaSanitizerResourceError(
                "temporary storage window exhausted: "
                f"{size_bytes} requested with {snapshot.reserved_bytes} bytes already "
                f"reserved and a {snapshot.limit_bytes}-byte operation limit",
                detail={
                    "stage": "temporary_storage",
                    "limit_name": "temporary_storage_bytes",
                    "limit_bytes": snapshot.limit_bytes,
                    "actual_bytes": snapshot.reserved_bytes + max(0, int(size_bytes)),
                    "artifact": label,
                },
            )
        return lease

    def snapshot(self) -> TemporaryStorageSnapshot:
        """Return current and peak reservations for diagnostics and tests."""
        drain_temporary_storage_finalizers()
        with self._lock:
            return TemporaryStorageSnapshot(
                limit_bytes=self.limit_bytes,
                reserved_bytes=self._reserved_bytes,
                peak_reserved_bytes=self._peak_reserved_bytes,
                active_leases=self._active_leases,
                deferred_finalizer_leases=_TEMP_STORAGE_FINALIZER_ESCROW.published_count(),
            )

    def diagnostics(self) -> TemporaryStorageDiagnostics:
        """Return operation-local close and over-release anomalies."""
        with self._lock:
            return TemporaryStorageDiagnostics(
                self._close_outstanding_bytes,
                self._close_active_leases,
                self._over_release_count,
                self._over_release_bytes,
            )

    def close(self) -> None:
        """Stop admission and wait for already-started reservations to commit."""
        drain_temporary_storage_finalizers()
        with self._condition:
            self._closed = True
            deadline = monotonic() + 30.0
            while self._pending_active_leases or self._resize_inflight:
                remaining = deadline - monotonic()
                if remaining <= 0 or not self._condition.wait(timeout=remaining):
                    raise RuntimeError("temporary-storage admissions exceeded their close deadline")
            if self._protocol_violations:
                raise RuntimeError("temporary-storage protocol violation prevents clean close")
            if self._close_complete:
                return
            self._close_outstanding_bytes = self._reserved_bytes
            self._close_active_leases = self._active_leases
            self._close_complete = True
            self._condition.notify_all()

    def _finish_pending_resize_locked(self, entry: _StorageLeaseEntry) -> None:
        """Reconcile one already-committed process resize into local accounting.

        The exact process capability remains rooted in *entry* while this tail is
        pending, so any Python allocation failure here is retryable and cannot
        orphan the physical reservation.
        """
        if not entry.resize_reconcile:
            return
        replacement = entry.resize_replacement
        if replacement is not None:
            entry.process_capability = replacement
            entry.resize_replacement = None
        delta = entry.resize_pending_delta
        target = entry.resize_target_bytes
        target_path = entry.resize_target_path
        if target_path is None:
            raise RuntimeError("temporary-storage pending resize lost target path")
        next_reserved = self._reserved_bytes + delta
        if next_reserved < 0 or next_reserved > self.limit_bytes:
            raise RuntimeError("temporary-storage pending resize violates pool accounting")
        next_peak = max(self._peak_reserved_bytes, next_reserved)
        if (
            entry.resize_pending_growth < 0
            or self._pending_resize_growth < entry.resize_pending_growth
        ):
            self._protocol_violations += 1
            raise RuntimeError("temporary-storage pending resize growth underflow")
        next_pending_growth = self._pending_resize_growth - entry.resize_pending_growth
        # No external side effects below: if any preparation above fails, the
        # process capability and retry metadata remain authoritative.
        self._reserved_bytes = next_reserved
        self._peak_reserved_bytes = next_peak
        self._pending_resize_growth = next_pending_growth
        entry.reserved_bytes = target
        entry.filesystem_key = entry.resize_target_key
        entry.filesystem_path = target_path
        entry.resize_reconcile = False
        entry.resize_target_bytes = 0
        entry.resize_target_key = -1
        entry.resize_target_path = None
        entry.resize_pending_delta = 0
        entry.resize_pending_growth = 0

    def _resize_lease(
        self,
        lease: TemporaryStorageLease,
        size_bytes: int,
        *,
        path: str | Path,
    ) -> _StorageResizeResult:
        """Resize one exact capability without holding the pool lock across I/O."""
        check_operation_cancelled(stage="temporary_storage_resize")
        requested = self._normalize_size(size_bytes)
        self._validate_one_artifact(requested, label=lease.label)
        target_key, target_path, _free = _PROCESS_TEMPORARY_STORAGE.filesystem(path)
        result = _StorageResizeResult(requested, target_key, target_path)

        with self._condition:
            if self._closed:
                raise RuntimeError("temporary-storage permit pool is closed")
            entry = self._lease_entry_locked(lease)
            self._finish_pending_resize_locked(entry)
            if entry.resize_inflight:
                raise RuntimeError("temporary-storage lease resize is already in flight")
            current = entry.reserved_bytes
            growth = requested - current
            growth_charge = max(0, growth)
            admission_total = (
                self._reserved_bytes
                + self._pending_reserved_bytes
                + self._pending_resize_growth
                + growth_charge
            )
            if admission_total > self.limit_bytes:
                raise SchemaSanitizerResourceError(
                    "temporary storage limit exceeded after staging: "
                    f"{admission_total} bytes > {self.limit_bytes} bytes",
                    detail={
                        "stage": "temporary_storage",
                        "limit_name": "temporary_storage_bytes",
                        "limit_bytes": self.limit_bytes,
                        "actual_bytes": admission_total,
                        "artifact": lease.label,
                    },
                )
            process_capability = entry.process_capability
            next_resize_inflight = self._resize_inflight + 1
            next_pending_growth = self._pending_resize_growth + growth_charge
            # Publish all retry metadata before external filesystem/journal work.
            entry.resize_target_bytes = requested
            entry.resize_target_key = target_key
            entry.resize_target_path = target_path
            entry.resize_pending_delta = growth
            entry.resize_pending_growth = growth_charge
            entry.resize_inflight = True
            self._resize_inflight = next_resize_inflight
            self._pending_resize_growth = next_pending_growth

        try:
            replacement = _PROCESS_TEMPORARY_STORAGE.resize_capability(
                process_capability,
                requested,
                path=path,
                label=lease.label,
                inode_count=entry.inode_count,
            )
        except BaseException:
            with self._condition:
                current_entry = self._lease_entry_locked(lease)
                if current_entry is entry and entry.resize_inflight:
                    entry.resize_inflight = False
                    self._finish_resize_inflight_locked()
                    self._consume_pending_resize_growth_locked(entry.resize_pending_growth)
                    entry.resize_target_bytes = 0
                    entry.resize_target_key = -1
                    entry.resize_target_path = None
                    entry.resize_pending_delta = 0
                    entry.resize_pending_growth = 0
                    self._condition.notify_all()
            raise

        # Root the external commit in an already-existing owner slot before
        # reacquiring the pool condition or performing any validating lookup.
        # If the following local reconcile raises, the exact replacement remains
        # reachable rather than living only in this stack frame.
        entry.resize_replacement = replacement
        entry.resize_reconcile = True
        cleanup_replacement: ProcessTemporaryStorageCapability | None = None
        ownership_error: BaseException | None = None
        with self._condition:
            try:
                reconciled_entry = self._lease_entry_locked(lease)
            except BaseException as exc:
                reconciled_entry = None
                ownership_error = exc
            if ownership_error is None and (
                reconciled_entry is not entry or not entry.resize_inflight
            ):
                ownership_error = RuntimeError(
                    "temporary-storage resize ownership changed during commit"
                )
            if ownership_error is not None:
                # No external release is allowed while holding the pool
                # condition.  Transfer cleanup back to a local owner and finish
                # the quiescence latch first.
                cleanup_replacement = entry.resize_replacement
                entry.resize_replacement = None
                entry.resize_reconcile = False
                if entry.resize_inflight:
                    entry.resize_inflight = False
                    self._finish_resize_inflight_locked()
                self._consume_pending_resize_growth_locked(entry.resize_pending_growth)
                entry.resize_target_bytes = 0
                entry.resize_target_key = -1
                entry.resize_target_path = None
                entry.resize_pending_delta = 0
                entry.resize_pending_growth = 0
                self._condition.notify_all()
            else:
                entry.resize_inflight = False
                self._finish_resize_inflight_locked()
                try:
                    self._finish_pending_resize_locked(entry)
                finally:
                    self._condition.notify_all()
                result.filesystem_key = entry.filesystem_key
                result.filesystem_path = entry.filesystem_path
        if ownership_error is not None:
            if cleanup_replacement is not None:
                try:
                    _PROCESS_TEMPORARY_STORAGE.release_capability(cleanup_replacement)
                except BaseException as cleanup_error:
                    add_bounded_note(
                        ownership_error,
                        "temporary-storage post-commit replacement cleanup also failed",
                        cleanup_error,
                    )
            raise ownership_error
        return result

    def _release_lease_authority(self, lease_id: int, owner_id: int, capability: object) -> None:
        """Release exact process/local/control authorities without a wrapper."""
        replay_committed = False
        with self._condition:
            entry = self._leases.get(lease_id)
            if entry is None:
                if isinstance(capability, FinalizerReplayCapability) and capability.released:
                    return
                self._unknown_lease_releases += 1
                raise RuntimeError("temporary-storage lease is not authoritative")
            if entry.owner_id != owner_id or entry.capability is not capability:
                self._unknown_lease_releases += 1
                raise RuntimeError("temporary-storage lease is not authoritative")
            if isinstance(capability, FinalizerReplayCapability) and capability.released:
                self._leases.pop(lease_id, None)
                self._condition.notify_all()
                replay_committed = True
            if replay_committed:
                return
            while entry.resize_inflight or entry.release_inflight:
                self._condition.wait(timeout=0.05)
                entry = self._lease_entry_authority_locked(lease_id, owner_id, capability)
            self._finish_pending_resize_locked(entry)
            if not entry.process_released:
                entry.release_inflight = True
                process_capability = entry.process_capability
            else:
                process_capability = None

        if not entry.process_released:
            try:
                assert process_capability is not None
                released = _PROCESS_TEMPORARY_STORAGE.release_capability(process_capability)
                if not released and process_capability.active:
                    raise RuntimeError(
                        "temporary-storage process capability release did not commit"
                    )
            except BaseException:
                with self._condition:
                    current = self._lease_entry_authority_locked(lease_id, owner_id, capability)
                    if current is entry:
                        entry.release_inflight = False
                        self._condition.notify_all()
                raise
            with self._condition:
                current = self._lease_entry_authority_locked(lease_id, owner_id, capability)
                if current is not entry:
                    raise RuntimeError("temporary-storage lease changed during process release")
                entry.process_released = True
                entry.release_inflight = False
                self._condition.notify_all()

        control_ticket: ControlPlaneTicket | None = None
        with self._condition:
            current = self._lease_entry_authority_locked(lease_id, owner_id, capability)
            if current is not entry:
                raise RuntimeError("temporary-storage lease changed during local release")
            if not entry.local_released:
                amount = entry.reserved_bytes
                excess = max(0, amount - self._reserved_bytes)
                missing_lease = self._active_leases <= 0
                next_over_count = self._over_release_count + (1 if excess or missing_lease else 0)
                next_over_bytes = self._over_release_bytes + excess
                self._over_release_count = next_over_count
                self._over_release_bytes = next_over_bytes
                self._finish_active_lease_locked(amount)
                entry.local_released = True
                self._condition.notify_all()
            control_ticket = entry.control_ticket

        if control_ticket is not None:
            if not release_control_plane(control_ticket):
                raise RuntimeError("temporary-storage control-plane retirement did not commit")
            with self._condition:
                current = self._lease_entry_authority_locked(lease_id, owner_id, capability)
                if current is entry and entry.control_ticket is control_ticket:
                    entry.control_ticket = None
                    self._condition.notify_all()

        with self._condition:
            current = self._lease_entry_authority_locked(lease_id, owner_id, capability)
            if current is entry and entry.local_released and entry.control_ticket is None:
                if isinstance(capability, FinalizerReplayCapability):
                    capability.released = True
                self._leases.pop(lease_id, None)
                self._condition.notify_all()

    def _release_lease(self, lease: TemporaryStorageLease) -> None:
        """Release one wrapper-owned exact capability."""
        self._release_lease_authority(lease._lease_id, id(lease), lease._capability)

    def _validate_one_artifact(self, size_bytes: int, *, label: str) -> None:
        """Reject an artifact that cannot fit even in an otherwise empty pool."""
        if size_bytes <= self.limit_bytes:
            return
        raise SchemaSanitizerResourceError(
            "temporary storage limit exceeded: "
            f"{size_bytes} bytes > {self.limit_bytes} bytes; artifact: {label}",
            detail={
                "stage": "temporary_storage",
                "limit_name": "temporary_storage_bytes",
                "limit_bytes": self.limit_bytes,
                "actual_bytes": size_bytes,
                "artifact": label,
            },
        )

    @staticmethod
    def _normalize_size(size_bytes: int) -> int:
        """Return a non-negative integer reservation size."""
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int):
            raise TypeError("temporary-storage reservation must be an integer")
        if size_bytes < 0:
            raise ValueError("temporary-storage reservation must be >= 0")
        return size_bytes

    @staticmethod
    def _normalize_artifact_count(artifact_count: int) -> int:
        """Return a non-negative inode reservation count."""
        if isinstance(artifact_count, bool) or not isinstance(artifact_count, int):
            raise TypeError("temporary-storage artifact_count must be an integer")
        if artifact_count < 0:
            raise ValueError("temporary-storage artifact_count must be >= 0")
        return artifact_count


def _reset_temporary_storage_finalizers_after_fork() -> None:
    """Drop parent-owned finalizer tickets/references in a forked child."""
    global _TEMP_STORAGE_FINALIZER_OVERFLOWS, _TEMP_STORAGE_FINALIZER_OVERFLOWED
    _TEMP_STORAGE_FINALIZER_ESCROW.reset_after_fork()
    _TEMP_STORAGE_FINALIZER_OVERFLOWS = 0
    _TEMP_STORAGE_FINALIZER_OVERFLOWED = False


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("temporary-storage-finalizers", mode="quarantine_only")


from .finalizer_registry import (  # noqa: E402
    register_finalizer_domain as _register_finalizer_domain,
)

_register_finalizer_domain(
    "temporary_storage",
    drain=drain_temporary_storage_finalizers,
    snapshot=temporary_storage_finalizer_snapshot,
    escrows=(("temporary_storage", _TEMP_STORAGE_FINALIZER_ESCROW),),
)


__all__ = [
    "ProcessTemporaryStorageDiagnostics",
    "ProcessTemporaryStorageSnapshot",
    "TemporaryStorageDiagnostics",
    "TemporaryStorageLease",
    "TemporaryStoragePermitPool",
    "TemporaryStorageSnapshot",
    "StreamingStorageReservation",
    "process_temporary_storage_diagnostics",
    "process_temporary_storage_snapshot",
    "drain_temporary_storage_finalizers",
    "temporary_storage_finalizer_snapshot",
]
