"""Fair process-wide weighted admission for remote I/O coroutines.

It implements a fair weighted governor with FIFO admission, dynamic capacity,
cancellation-safe leases, snapshots, and fork reset.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections import OrderedDict
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from itertools import islice
from threading import Event, Lock, local
from typing import Final

from schema_sanitizer.core_impl.control_plane_budget import (
    ControlPlaneTicket,
    release_control_plane,
    reserve_control_plane,
)
from schema_sanitizer.core_impl.safe_errors import add_bounded_note

from ..core_impl.cancellation import (
    await_cancellable_future,
    check_operation_cancelled,
)
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    acknowledge_prepared_finalizer_cleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_finalizer_cleanup,
)
from ..core_impl.fork_safety import quarantine_inherited_state
from ..core_impl.system_pressure import system_pressure_snapshot
from ..errors import SchemaSanitizerResourceError

_MAX_HEAD_BYPASSES: Final = 4
_MAX_LOCAL_BYPASS_SCAN: Final = 32
_DEFAULT_CAPACITY: Final = min(256, max(4, (os.cpu_count() or 1) * 4))
_DEFAULT_MAX_WAITERS: Final = 4096
_DEFAULT_MAX_PENDING_SUBMISSIONS: Final = 4096
_DEFAULT_MAX_CAPACITY_REGISTRATIONS: Final = 1024
_MAX_RETAINED_METADATA_CHARS: Final = 256
_METADATA_HASH_CHUNK_CHARS: Final = 4096


def _bounded_metadata(value: object, *, kind: str) -> str:
    """Return a compact stable identity for attacker-sized queue metadata."""
    text = str(value)
    if len(text) <= _MAX_RETAINED_METADATA_CHARS:
        return text
    digest = hashlib.blake2b(digest_size=16)
    for offset in range(0, len(text), _METADATA_HASH_CHUNK_CHARS):
        digest.update(
            text[offset : offset + _METADATA_HASH_CHUNK_CHARS].encode(
                "utf-8", errors="surrogatepass"
            )
        )
    return f"long-{kind}:{len(text)}:{digest.hexdigest()}"


@dataclass(frozen=True, slots=True)
class RemoteIoPermitSnapshot:
    """Atomic process-wide remote-I/O admission statistics."""

    capacity: int
    in_use: int
    peak_in_use: int
    waiting: int
    peak_waiting: int
    grants: int
    cancellations: int
    bounded_bypasses: int
    active_capacity_registrations: int
    over_release_count: int
    over_release_weight: int
    queue_capacity: int = 0
    rejected_waiters: int = 0
    pending_submissions: int = 0
    peak_pending_submissions: int = 0
    submission_capacity: int = 0
    rejected_submissions: int = 0
    delivery_failures: int = 0
    active_permits: int = 0
    unknown_permit_releases: int = 0
    unknown_submission_releases: int = 0
    unknown_capacity_releases: int = 0
    capacity_registration_capacity: int = 0
    rejected_capacity_registrations: int = 0
    active_submission_reservations: int = 0
    active_capacity_capabilities: int = 0
    scheduling_dirty: bool = False
    post_commit_failures: int = 0
    bucket_rebuilds: int = 0
    sync_waiters: int = 0
    peak_sync_waiters: int = 0
    protocol_violations: int = 0
    admission_closed: bool = False


@dataclass(slots=True)
class _CapabilityEntry:
    """Authoritative identity for one externally held logical capability."""

    owner_id: int
    capability: object
    amount: int
    metadata: object | None = None
    control_ticket: ControlPlaneTicket | None = None
    resource_released: bool = False


class _CapabilityPublication:
    """Preallocated single-owner receipt for an authoritative capability."""

    __slots__ = ("lease_id", "capability")

    def __init__(self, capability: object) -> None:
        """Preallocate a receipt that publishes the admitted capability with its lease ID."""
        self.lease_id = 0
        self.capability = capability


@dataclass(slots=True)
class _Waiter:
    """One weighted request shared by async Futures and sync Events."""

    loop: asyncio.AbstractEventLoop | None
    future: asyncio.Future["RemoteIoPermit"] | None
    requested_weight: int
    label: str
    operation_id: str
    bypasses: int = 0
    granted_weight: int = 0
    state: str = "queued"
    indexed: bool = False
    control_ticket: ControlPlaneTicket | None = None
    prepared_next_in_use: int = 0
    prepared_next_peak: int = 0
    prepared_next_grants: int = 0
    delivery_callback: Callable[[], None] | None = None
    sync_event: Event | None = None
    delivered_permit: "RemoteIoPermit | None" = None
    delivery_error: BaseException | None = None


class _GrantBatch:
    """Preallocated grant handoff built before scheduler ownership mutates."""

    __slots__ = ("_items", "count", "next_batch", "chain_tail")

    def __init__(self, capacity: int) -> None:
        """Preallocate fixed slots for committed waiter handoffs."""
        self._items: list[_Waiter | None] = [None] * max(0, capacity)
        self.count = 0
        self.next_batch: _GrantBatch | None = None
        self.chain_tail: _GrantBatch = self

    def append(self, waiter: _Waiter) -> None:
        # Capacity was allocated before any grant transition. Assignment cannot
        # grow the list, so a committed waiter can always be retained here.
        """Append one value to the bounded collection."""
        if self.count >= len(self._items):
            raise RuntimeError("remote I/O grant batch capacity invariant exceeded")
        self._items[self.count] = waiter
        self.count += 1

    def extend_chain(self, other: "_GrantBatch | None") -> None:
        """Append another retained permit grant to this batch."""
        if other is None or other.count == 0:
            return
        self.chain_tail.next_batch = other
        self.chain_tail = other.chain_tail

    def item(self, index: int) -> _Waiter:
        """Return the indexed permit grant from this batch."""
        waiter = self._items[index]
        if waiter is None:
            raise RuntimeError("remote I/O grant batch contains an empty committed slot")
        return waiter

    def __bool__(self) -> bool:
        """Return whether the instance currently carries a value."""
        return self.count > 0

    def __len__(self) -> int:
        """Return the number of retained values."""
        return self.count

    def __iter__(self) -> Iterator[_Waiter]:
        """Iterate over the retained values."""
        for index in range(self.count):
            yield self.item(index)


_RemoteIoForkBank = tuple[
    Lock,
    local,
    dict[int, int],
    dict[int, _CapabilityEntry],
    dict[int, _CapabilityEntry],
    dict[int, _CapabilityEntry],
    dict[str, OrderedDict[int, _Waiter]],
    OrderedDict[str, None],
    dict[int, OrderedDict[str, None]],
    OrderedDict[int, None],
    dict[str, int],
]


def _release_remote_submission_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Release remote submission capsule."""
    governor = capsule.arg0
    lease_id = capsule.arg1
    capability = capsule.arg2
    release = getattr(governor, "_release_submission_capability", None)
    if callable(release) and type(lease_id) is int and lease_id > 0:
        release(lease_id, capability)


def _release_remote_capacity_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Release remote capacity capsule."""
    governor = capsule.arg0
    lease_id = capsule.arg1
    capability = capsule.arg2
    release = getattr(governor, "_release_capacity_capability", None)
    if callable(release) and type(lease_id) is int and lease_id > 0:
        release(lease_id, capability)


def _release_remote_permit_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Release remote permit capsule."""
    governor = capsule.arg0
    lease_id = capsule.arg1
    capability = capsule.arg2
    release = getattr(governor, "_release_permit_capability", None)
    if callable(release) and type(lease_id) is int and lease_id > 0:
        release(lease_id, capability)


class RemoteIoSubmissionReservation:
    """Exactly-once process-wide slot for one submitted remote coroutine."""

    def __init__(self, governor: "RemoteIoPermitGovernor", *, _active: bool = True) -> None:
        """Initialize an active reservation or inert pre-publication owner."""
        self._finalizer_ticket = 0
        self._finalizer_capsule: PreparedFinalizerCleanup | None = None
        capsule = reserve_finalizer_cleanup(_release_remote_submission_capsule)
        ticket = capsule.ticket
        capsule.arg0 = governor
        self._finalizer_ticket = ticket
        self._finalizer_capsule = capsule
        self._governor = governor
        self._pid = os.getpid()
        self._lock = Lock()
        self._lease_id = 0
        self._capability: object | None = None
        self._released = not _active

    def _activate(self, *, lease_id: int = 0, capability: object | None = None) -> None:
        """Publish this owner after submission accounting commits."""
        self._lease_id = lease_id
        self._capability = capability
        capsule = self._finalizer_capsule
        if capsule is not None:
            capsule.arg1 = lease_id
            capsule.arg2 = capability
        self._released = False

    def _retire_finalizer_slot(self) -> None:
        """Retire the finalizer escrow slot owned by this remote io submission reservation."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def _acknowledge_finalizer_slot_locked(self) -> None:
        """Disarm primary replay before retiring a post-release cleanup slot."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def release(self) -> None:
        """Return this submission slot exactly once.

        Once the governor release commits, only finalizer-slot acknowledgement
        remains retryable; a failed acknowledgement must never reactivate the
        primary submission capability.
        """
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._released:
                self._acknowledge_finalizer_slot_locked()
                return
            self._released = True
        try:
            self._governor._release_submission_reservation(self)
        except BaseException:
            with self._lock:
                self._released = False
            raise
        with self._lock:
            self._acknowledge_finalizer_slot_locked()

    close = release

    def __del__(self) -> None:
        """Publish only the preallocated submission capability capsule."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None and defer_prepared_finalizer_cleanup(capsule):
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
        except BaseException:
            pass


class RemoteIoCapacityRegistration:
    """Exactly-once lifetime registration for one coordinator's I/O ceiling."""

    def __init__(
        self,
        governor: "RemoteIoPermitGovernor",
        token: int,
        *,
        _active: bool = True,
    ) -> None:
        """Initialize an active registration or inert pre-publication owner."""
        self._finalizer_ticket = 0
        self._finalizer_capsule: PreparedFinalizerCleanup | None = None
        capsule = reserve_finalizer_cleanup(_release_remote_capacity_capsule)
        ticket = capsule.ticket
        capsule.arg0 = governor
        self._finalizer_ticket = ticket
        self._finalizer_capsule = capsule
        self._governor = governor
        self._token = token
        self._pid = os.getpid()
        self._lock = Lock()
        self._lease_id = 0
        self._capability: object | None = None
        self._released = not _active

    def _activate(self, *, lease_id: int = 0, capability: object | None = None) -> None:
        """Publish this owner after capacity registration commits."""
        self._lease_id = lease_id
        self._capability = capability
        capsule = self._finalizer_capsule
        if capsule is not None:
            capsule.arg1 = lease_id
            capsule.arg2 = capability
        self._released = False

    def _retire_finalizer_slot(self) -> None:
        """Retire the finalizer escrow slot owned by this remote io capacity registration."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def _acknowledge_finalizer_slot_locked(self) -> None:
        """Disarm primary replay before retiring a post-release cleanup slot."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def release(self) -> None:
        """Remove this coordinator's requested capacity exactly once."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._released:
                self._acknowledge_finalizer_slot_locked()
                return
            self._released = True
        try:
            self._governor._release_capacity_registration(self)
        except BaseException:
            with self._lock:
                self._released = False
            raise
        with self._lock:
            self._acknowledge_finalizer_slot_locked()

    close = release

    def __del__(self) -> None:
        """Publish only the preallocated capacity capability capsule."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None and defer_prepared_finalizer_cleanup(capsule):
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
        except BaseException:
            pass


class RemoteIoPermit:
    """Exactly-once weighted permit returned by the shared governor."""

    def __init__(self, governor: "RemoteIoPermitGovernor", weight: int, label: str) -> None:
        """Prearm finalization and bind the governor, requested weight, and diagnostic label."""
        self._finalizer_ticket = 0
        self._finalizer_capsule: PreparedFinalizerCleanup | None = None
        capsule = reserve_finalizer_cleanup(_release_remote_permit_capsule)
        ticket = capsule.ticket
        capsule.arg0 = governor
        self._finalizer_ticket = ticket
        self._finalizer_capsule = capsule
        self._governor = governor
        self._weight = weight
        self.label = label
        self._pid = os.getpid()
        self._lock = Lock()
        self._lease_id = 0
        self._capability: object | None = None
        self._released = False

    @property
    def weight(self) -> int:
        """Return the immutable diagnostic weight assigned at admission."""
        return self._weight

    def _activate(self, *, lease_id: int, capability: object, weight: int) -> None:
        """Activate this remote io permit with authoritative capacity and capability."""
        self._lease_id = lease_id
        self._capability = capability
        self._weight = weight
        capsule = self._finalizer_capsule
        if capsule is not None:
            capsule.arg1 = lease_id
            capsule.arg2 = capability

    def _retire_finalizer_slot(self) -> None:
        """Retire the finalizer escrow slot owned by this remote io permit."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def _acknowledge_finalizer_slot_locked(self) -> None:
        """Disarm primary replay before retiring a post-release cleanup slot."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def release(self) -> None:
        """Return this permit exactly once from any thread or event loop."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._released:
                self._acknowledge_finalizer_slot_locked()
                return
            self._released = True
        try:
            self._governor._release_permit(self)
        except BaseException:
            with self._lock:
                self._released = False
            raise
        with self._lock:
            self._acknowledge_finalizer_slot_locked()

    close = release

    async def __aenter__(self) -> "RemoteIoPermit":
        """Enter this managed resource."""
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Exit this managed resource."""
        self.release()

    def __del__(self) -> None:
        """Publish only the preallocated permit capability capsule."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None and defer_prepared_finalizer_cleanup(capsule):
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
        except BaseException:
            pass


class RemoteIoPermitGovernor:
    """Coordinate weighted remote work fairly across operations and event loops.

    Each operation exposes only its oldest queued request to the round-robin
    selector. Within a single operation, the historical bounded-bypass rule is
    retained so tiny control calls can pass one temporarily blocked large call.
    Requested weight is preserved until admission and clamped against the live
    capacity at grant time, avoiding stale undercharging after capacity changes.
    """

    def __init__(
        self,
        capacity: int = _DEFAULT_CAPACITY,
        *,
        max_waiters: int = _DEFAULT_MAX_WAITERS,
        max_pending_submissions: int = _DEFAULT_MAX_PENDING_SUBMISSIONS,
        max_capacity_registrations: int = _DEFAULT_MAX_CAPACITY_REGISTRATIONS,
    ) -> None:
        """Validate admission bounds and initialize fair queues, counters, and fork banks."""
        if isinstance(capacity, bool) or not isinstance(capacity, int):
            raise TypeError("remote I/O permit capacity must be an integer")
        if capacity <= 0:
            raise ValueError("remote I/O permit capacity must be > 0")
        if isinstance(max_waiters, bool) or not isinstance(max_waiters, int):
            raise TypeError("remote I/O maximum waiters must be an integer")
        if max_waiters <= 0:
            raise ValueError("remote I/O maximum waiters must be > 0")
        if isinstance(max_pending_submissions, bool) or not isinstance(
            max_pending_submissions, int
        ):
            raise TypeError("remote I/O submission capacity must be an integer")
        if max_pending_submissions <= 0:
            raise ValueError("remote I/O submission capacity must be > 0")
        if type(max_capacity_registrations) is not int:
            raise TypeError("remote I/O capacity-registration limit must be an exact integer")
        if max_capacity_registrations <= 0:
            raise ValueError("remote I/O capacity-registration limit must be > 0")
        self._lock = Lock()
        self._delivery_local = local()
        # Cache the bound callback while allocation is unquestionably safe.
        # Delivery after a grant uses this exact object instead of allocating a
        # new closure/bound-method wrapper at the commit boundary.
        self._delivery_callback = self._deliver_waiter_callback
        self._base_capacity = capacity
        self._pressure_scale = 1.0
        self._requested_capacity = capacity
        self._pressure_capacity = capacity
        self._capacity = capacity
        self._registrations: dict[int, int] = {}
        self._max_capacity_registrations = max_capacity_registrations
        self._rejected_capacity_registrations = 0
        self._capacity_owners: dict[int, _CapabilityEntry] = {}
        self._next_registration_token = 0
        self._capability_sequence = 0
        self._submission_owners: dict[int, _CapabilityEntry] = {}
        self._permit_owners: dict[int, _CapabilityEntry] = {}
        self._in_use = 0
        self._peak_in_use = 0
        self._operation_waiters: dict[str, OrderedDict[int, _Waiter]] = {}
        self._operation_order: OrderedDict[str, None] = OrderedDict()
        self._weight_buckets: dict[int, OrderedDict[str, None]] = {}
        self._weight_order: OrderedDict[int, None] = OrderedDict()
        self._operation_weights: dict[str, int] = {}
        self._bucket_capacity = capacity
        self._waiting_count = 0
        self._sync_waiters = 0
        self._peak_sync_waiters = 0
        self._max_waiters = max_waiters
        self._rejected_waiters = 0
        self._pending_submissions = 0
        self._peak_pending_submissions = 0
        self._max_pending_submissions = max_pending_submissions
        self._rejected_submissions = 0
        self._last_granted_operation: str | None = None
        self._grants = 0
        self._cancellations = 0
        self._bounded_bypasses = 0
        self._peak_waiting = 0
        self._over_release_count = 0
        self._over_release_weight = 0
        self._delivery_failures = 0
        self._unknown_permit_releases = 0
        self._unknown_submission_releases = 0
        self._unknown_capacity_releases = 0
        self._scheduling_dirty = False
        self._post_commit_failures = 0
        self._protocol_violations = 0
        self._admission_closed = False
        self._fork_prepared: _RemoteIoForkBank | None = None
        self._fork_banks: tuple[_RemoteIoForkBank, ...] = (
            self._make_fork_bank(),
            self._make_fork_bank(),
        )
        self._fork_bank_index = 0
        self._bucket_rebuilds = 0

    def _record_protocol_violation_locked(self) -> None:
        """Record protocol violation while holding the governing lock."""
        self._protocol_violations += 1

    def _return_in_use_locked(self, amount: int) -> bool:
        """Return in use while holding the governing lock."""
        if amount < 0 or self._in_use < amount:
            self._record_protocol_violation_locked()
            excess = max(0, amount - self._in_use)
            self._over_release_count += 1
            self._over_release_weight += excess
            return False
        self._in_use -= amount
        return True

    def _finish_sync_waiter_locked(self) -> None:
        """Finish sync waiter while holding the governing lock."""
        if self._sync_waiters <= 0:
            self._record_protocol_violation_locked()
            return
        self._sync_waiters -= 1

    def close_admission(self) -> None:
        """Freeze new remote-I/O owners/waiters for runtime shutdown."""
        with self._lock:
            self._admission_closed = True

    def reopen_admission_for_tests(self) -> None:
        """Reopen a quiescent governor after an isolated shutdown test.

        Production shutdown remains terminal.  This narrow test hook refuses
        to erase the admission barrier while any exact remote-I/O authority or
        waiter is still live.
        """
        with self._lock:
            if (
                self._in_use
                or self._waiting_count
                or self._sync_waiters
                or self._pending_submissions
                or self._registrations
                or self._capacity_owners
                or self._submission_owners
                or self._permit_owners
                or self._operation_waiters
            ):
                raise RuntimeError("cannot reopen remote I/O admission while owners remain live")
            self._admission_closed = False

    def _publish_capability_locked(
        self,
        owner: object,
        ledger: dict[int, _CapabilityEntry],
        *,
        amount: int,
        metadata: object | None = None,
        control_ticket: ControlPlaneTicket | None = None,
    ) -> _CapabilityPublication:
        """Prepare one authoritative entry with a preallocated return receipt."""
        if self._capability_sequence >= (1 << 63) - 1:
            raise RuntimeError("remote I/O capability generation exhausted")
        lease_id = self._capability_sequence + 1
        capability = object()
        publication = _CapabilityPublication(capability)
        entry = _CapabilityEntry(id(owner), capability, amount, metadata, control_ticket)
        ledger[lease_id] = entry
        self._capability_sequence = lease_id
        publication.lease_id = lease_id
        return publication

    @staticmethod
    def _entry_matches(owner: object, entry: _CapabilityEntry) -> bool:
        """Return whether entry matches."""
        return (
            entry.owner_id == id(owner) and getattr(owner, "_capability", None) is entry.capability
        )

    def configure_capacity(self, requested: int) -> None:
        """Raise the permanent base ceiling for explicitly configured governors."""
        self._validate_capacity(requested)
        pressure_scale = system_pressure_snapshot().scale
        with self._lock:
            if self._admission_closed:
                raise RuntimeError("remote I/O admission is closed")
            self._pressure_scale = pressure_scale
            self._base_capacity = max(self._base_capacity, requested)
            self._requested_capacity = max(self._requested_capacity, self._base_capacity)
            self._recompute_capacity_locked()
            deliveries = self._grant_ready_locked()
        self._deliver(deliveries)

    def register_capacity(self, requested: int) -> RemoteIoCapacityRegistration:
        """Register one live coordinator transactionally."""
        self._validate_capacity(requested)
        pressure_scale = system_pressure_snapshot().scale
        registration: RemoteIoCapacityRegistration | None = None
        deliveries: _GrantBatch | None = None
        control_ticket: ControlPlaneTicket | None = None
        with self._lock:
            if self._admission_closed:
                raise RuntimeError("remote I/O admission is closed")
            self._pressure_scale = pressure_scale
            if len(self._registrations) >= self._max_capacity_registrations:
                self._rejected_capacity_registrations += 1
                raise SchemaSanitizerResourceError(
                    "remote I/O capacity-registration limit exhausted",
                    detail={
                        "stage": "remote_io_capacity_registration",
                        "limit_name": "remote_io_capacity_registrations",
                        "limit_items": self._max_capacity_registrations,
                        "actual_items": len(self._registrations) + 1,
                    },
                )
            if self._next_registration_token >= (1 << 63) - 1:
                raise RuntimeError("remote I/O registration generation exhausted")
            token = self._next_registration_token + 1
            registration = RemoteIoCapacityRegistration(self, token, _active=False)
            lease_id = 0
            try:
                control_ticket = reserve_control_plane("remote_io_capacity_registration", 384)
                publication = self._publish_capability_locked(
                    registration,
                    self._capacity_owners,
                    amount=requested,
                    metadata=token,
                    control_ticket=control_ticket,
                )
                lease_id = publication.lease_id
                self._registrations[token] = requested
                registration._activate(lease_id=lease_id, capability=publication.capability)
                self._next_registration_token = token
                self._requested_capacity = max(self._requested_capacity, requested)
                self._recompute_capacity_locked()
                deliveries = self._grant_ready_locked()
            except BaseException:
                self._registrations.pop(token, None)
                if lease_id:
                    self._capacity_owners.pop(lease_id, None)
                if control_ticket is not None:
                    release_control_plane(control_ticket)
                    control_ticket = None
                registration._retire_finalizer_slot()
                self._requested_capacity = self._requested_capacity_from_registrations_locked()
                self._recompute_capacity_locked()
                raise
        try:
            self._deliver(deliveries)
        except BaseException as exc:
            try:
                registration.release()
            except BaseException as cleanup_error:
                add_bounded_note(
                    exc,
                    "remote I/O capacity registration rollback also failed",
                    cleanup_error,
                )
            raise
        return registration

    def reserve_submission(self) -> RemoteIoSubmissionReservation:
        """Reserve one bounded submitted-coroutine slot transactionally."""
        reservation = RemoteIoSubmissionReservation(self, _active=False)
        with self._lock:
            if self._admission_closed:
                reservation._retire_finalizer_slot()
                raise RuntimeError("remote I/O admission is closed")
            if self._pending_submissions >= self._max_pending_submissions:
                self._rejected_submissions += 1
                reservation._retire_finalizer_slot()
                raise SchemaSanitizerResourceError(
                    "remote I/O submission capacity exhausted",
                    detail={
                        "stage": "remote_io_submission",
                        "limit_name": "remote_io_pending_submissions",
                        "limit_items": self._max_pending_submissions,
                        "actual_items": self._pending_submissions + 1,
                    },
                )
            next_pending = self._pending_submissions + 1
            next_peak = max(self._peak_pending_submissions, next_pending)
            control_ticket = reserve_control_plane("remote_io_submission_reservation", 384)
            lease_id = 0
            try:
                publication = self._publish_capability_locked(
                    reservation,
                    self._submission_owners,
                    amount=1,
                    control_ticket=control_ticket,
                )
                lease_id = publication.lease_id
                reservation._activate(lease_id=lease_id, capability=publication.capability)
            except BaseException:
                if lease_id:
                    self._submission_owners.pop(lease_id, None)
                release_control_plane(control_ticket)
                reservation._retire_finalizer_slot()
                raise
            self._pending_submissions = next_pending
            self._peak_pending_submissions = next_peak
        return reservation

    def _release_submission_capability(self, lease_id: int, capability: object) -> None:
        """Release a submission slot while retaining secondary ticket authority."""
        with self._lock:
            entry = self._submission_owners.get(lease_id)
            if entry is None or entry.capability is not capability:
                self._unknown_submission_releases += 1
                raise RuntimeError("remote I/O submission reservation is not authoritative")
            if not entry.resource_released:
                if entry.amount < 0 or self._pending_submissions < entry.amount:
                    self._record_protocol_violation_locked()
                    raise RuntimeError("remote I/O submission counter underflow")
                self._pending_submissions -= entry.amount
                entry.resource_released = True
            control_ticket = entry.control_ticket
        if control_ticket is not None:
            if not release_control_plane(control_ticket):
                raise RuntimeError("remote I/O submission control-plane retirement did not commit")
            entry.control_ticket = None
        with self._lock:
            if self._submission_owners.get(lease_id) is entry and entry.control_ticket is None:
                self._submission_owners.pop(lease_id, None)

    def _release_submission_reservation(self, reservation: RemoteIoSubmissionReservation) -> None:
        """Release one exact submitted-coroutine capability transactionally."""
        lease_id = reservation._lease_id
        with self._lock:
            entry = self._submission_owners.get(lease_id)
            if entry is None or not self._entry_matches(reservation, entry):
                self._unknown_submission_releases += 1
                raise RuntimeError("remote I/O submission reservation is not authoritative")
            if not entry.resource_released:
                if entry.amount < 0 or self._pending_submissions < entry.amount:
                    self._record_protocol_violation_locked()
                    raise RuntimeError("remote I/O submission counter underflow")
                self._pending_submissions -= entry.amount
                entry.resource_released = True
            control_ticket = entry.control_ticket
        if control_ticket is not None:
            if not release_control_plane(control_ticket):
                raise RuntimeError("remote I/O submission control-plane retirement did not commit")
            entry.control_ticket = None
        with self._lock:
            if self._submission_owners.get(lease_id) is entry and entry.control_ticket is None:
                self._submission_owners.pop(lease_id, None)

    @staticmethod
    def _validate_capacity(requested: int) -> None:
        """Validate a strictly positive integer permit capacity."""
        if isinstance(requested, bool) or not isinstance(requested, int):
            raise TypeError("remote I/O permit capacity must be an integer")
        if requested <= 0:
            raise ValueError("remote I/O permit capacity must be > 0")

    def _requested_capacity_from_registrations_locked(
        self, *, excluding_token: int | None = None
    ) -> int:
        """Compute a new capacity maximum before any registration commit."""
        requested = self._base_capacity
        for token, value in self._registrations.items():
            if token == excluding_token:
                continue
            if value > requested:
                requested = value
        return requested

    def _apply_cached_capacity_locked(self) -> None:
        """Apply already-prepared pressure capacity without container allocation."""
        previous = self._capacity
        self._capacity = (
            self._in_use if self._in_use > self._pressure_capacity else self._pressure_capacity
        )
        if self._capacity != previous:
            self._scheduling_dirty = True

    def _recompute_capacity_locked(self) -> None:
        """Recompute pressure capacity on an allocation-capable pre-commit path."""
        self._pressure_capacity = max(1, int(self._requested_capacity * self._pressure_scale))
        self._apply_cached_capacity_locked()

    def _prepare_registration_removal_locked(self, token: int) -> tuple[int, int]:
        """Prepare all arithmetic/scans before retiring a capacity capability."""
        next_requested = self._requested_capacity_from_registrations_locked(excluding_token=token)
        next_pressure = max(1, int(next_requested * self._pressure_scale))
        return next_requested, next_pressure

    def _commit_registration_capacity_locked(self, next_requested: int, next_pressure: int) -> None:
        """Install precomputed capacity state after a registration commit."""
        self._requested_capacity = next_requested
        self._pressure_capacity = next_pressure
        self._apply_cached_capacity_locked()

    def _release_capacity_capability(self, lease_id: int, capability: object) -> None:
        """Release capacity registration with a retryable control-plane tail."""
        deliveries: _GrantBatch | None = None
        with self._lock:
            entry = self._capacity_owners.get(lease_id)
            if entry is None or entry.capability is not capability:
                self._unknown_capacity_releases += 1
                raise RuntimeError("remote I/O capacity registration is not authoritative")
            if not entry.resource_released:
                token = entry.metadata
                if type(token) is not int:
                    raise RuntimeError("remote I/O capacity ledger is corrupt")
                next_requested, next_pressure = self._prepare_registration_removal_locked(token)
                self._registrations.pop(token, None)
                self._commit_registration_capacity_locked(next_requested, next_pressure)
                entry.resource_released = True
                deliveries = self._post_commit_reschedule_locked()
            control_ticket = entry.control_ticket
        if control_ticket is not None:
            if not release_control_plane(control_ticket):
                raise RuntimeError("remote I/O capacity control-plane retirement did not commit")
            entry.control_ticket = None
        with self._lock:
            if self._capacity_owners.get(lease_id) is entry and entry.control_ticket is None:
                self._capacity_owners.pop(lease_id, None)
        self._deliver_noexcept(deliveries)

    def _release_capacity_registration(self, registration: RemoteIoCapacityRegistration) -> None:
        """Release one exact registration with retryable secondary authority."""
        lease_id = registration._lease_id
        deliveries: _GrantBatch | None = None
        with self._lock:
            entry = self._capacity_owners.get(lease_id)
            if entry is None or not self._entry_matches(registration, entry):
                self._unknown_capacity_releases += 1
                raise RuntimeError("remote I/O capacity registration is not authoritative")
            if not entry.resource_released:
                token = entry.metadata
                if type(token) is not int:
                    raise RuntimeError("remote I/O capacity ledger is corrupt")
                next_requested, next_pressure = self._prepare_registration_removal_locked(token)
                self._registrations.pop(token, None)
                self._commit_registration_capacity_locked(next_requested, next_pressure)
                entry.resource_released = True
                deliveries = self._post_commit_reschedule_locked()
            control_ticket = entry.control_ticket
        if control_ticket is not None:
            if not release_control_plane(control_ticket):
                raise RuntimeError("remote I/O capacity control-plane retirement did not commit")
            entry.control_ticket = None
        with self._lock:
            if self._capacity_owners.get(lease_id) is entry and entry.control_ticket is None:
                self._capacity_owners.pop(lease_id, None)
        self._deliver_noexcept(deliveries)

    def _repair_scheduler_locked(self) -> None:
        """Advance the capacity epoch without rebuilding all operation buckets.

        Bucket entries are derived hints. After a capacity change each operation
        is repaired lazily when visited by the selector, avoiding an O(waiters)
        allocation burst under the governor lock.
        """
        if self._scheduling_dirty or self._bucket_capacity != self._capacity:
            self._bucket_capacity = self._capacity
            self._scheduling_dirty = False
            self._bucket_rebuilds += 1

    def _post_commit_reschedule_locked(self) -> _GrantBatch | None:
        """Best-effort scheduler repair after an authoritative release commit."""
        try:
            self._apply_cached_capacity_locked()
            self._repair_scheduler_locked()
            return self._grant_ready_locked()
        except BaseException:
            self._record_post_commit_failure_locked()
            return None

    def _deliver_noexcept(self, deliveries: _GrantBatch | None) -> None:
        """Best-effort delivery for work produced after a release commit."""
        if not deliveries:
            return
        try:
            self._deliver(deliveries)
        except BaseException:
            with self._lock:
                self._record_post_commit_failure_locked()

    def _repair_waiter_progress_noexcept(self, waiter: _Waiter) -> None:
        """Level-trigger scheduler repair while an authoritative waiter sleeps.

        A release/capacity commit is allowed to survive an injected allocation
        failure in derived scheduling state.  The waiter itself therefore acts
        as a bounded repair trigger on each cancellation poll, so one failed
        post-commit scheduling attempt cannot strand capacity until unrelated
        activity happens.
        """
        deliveries: _GrantBatch | None = None
        try:
            with self._lock:
                if waiter.state != "queued":
                    return
                if not self._scheduling_dirty and self._in_use >= self._capacity:
                    return
                deliveries = self._post_commit_reschedule_locked()
        except BaseException:
            return
        self._deliver_noexcept(deliveries)

    async def acquire(
        self,
        weight: int = 1,
        *,
        label: str = "remote_io",
        operation_id: str | None = None,
    ) -> RemoteIoPermit:
        """Wait asynchronously for one weighted, operation-fair process permit."""
        check_operation_cancelled(stage="remote_io_admission")
        if isinstance(weight, bool) or not isinstance(weight, int):
            raise TypeError("remote I/O permit weight must be an integer")
        if weight <= 0:
            raise ValueError("remote I/O permit weight must be > 0")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[RemoteIoPermit] = loop.create_future()
        waiter = _Waiter(
            loop,
            future,
            weight,
            _bounded_metadata(label, kind="label"),
            _bounded_metadata(operation_id or "standalone", kind="operation"),
        )
        waiter.control_ticket = reserve_control_plane("remote_io_waiter", 512)
        rejected = False
        pressure_scale = system_pressure_snapshot().scale
        deliveries_before: _GrantBatch | None = None
        deliveries_after: _GrantBatch | None = None
        enqueued = False
        try:
            with self._lock:
                if self._admission_closed:
                    raise RuntimeError("remote I/O admission is closed")
                self._pressure_scale = pressure_scale
                self._recompute_capacity_locked()
                deliveries_before = self._grant_ready_locked()
                if self._waiting_count >= self._max_waiters:
                    self._rejected_waiters += 1
                    rejected = True
                else:
                    self._enqueue_waiter_locked(waiter)
                    enqueued = waiter.indexed
                    self._peak_waiting = max(self._peak_waiting, self._waiting_count)
                    try:
                        deliveries_after = self._grant_ready_locked()
                    except BaseException:
                        # The waiter is already authoritative. Scheduler work is
                        # repairable and must not turn successful enqueue into an
                        # apparent failed acquisition.
                        self._record_post_commit_failure_locked()
        except BaseException:
            if not enqueued:
                self._retire_waiter_control(waiter)
            raise
        if rejected:
            # No ownership for this waiter was published, so delivery of older
            # grants cannot strand the rejected request.
            self._deliver(deliveries_before)
            self._deliver(deliveries_after)
            if release_control_plane(waiter.control_ticket):
                waiter.control_ticket = None
            raise SchemaSanitizerResourceError(
                "remote I/O wait queue exhausted",
                detail={
                    "stage": "remote_io_admission",
                    "limit_name": "remote_io_waiters",
                    "limit_items": self._max_waiters,
                    "actual_items": self._max_waiters + 1,
                },
            )
        try:
            # Publication, delivery, and awaiting share one ownership region.
            # Any BaseException after enqueue is reclaimed by the block below.
            self._deliver(deliveries_before)
            self._deliver(deliveries_after)
            permit = await await_cancellable_future(
                future,
                stage="remote_io_admission",
                on_poll=lambda: self._repair_waiter_progress_noexcept(waiter),
            )
            check_operation_cancelled(stage="remote_io_admission")
            return permit
        except BaseException as exc:
            # Cancellation is not the only control-flow exception that can
            # abandon an await. KeyboardInterrupt, SystemExit, or an injected
            # BaseException must remove queued waiters and reclaim committed
            # grants by the same exactly-once path.
            cancellation_deliveries: _GrantBatch | None = None
            delivered_permit: RemoteIoPermit | None = None
            with self._lock:
                if waiter.state == "queued":
                    waiter.state = "cancelled"
                    self._cancellations += 1
                    self._remove_waiter_locked(waiter)
                    cancellation_deliveries = self._post_commit_reschedule_locked()
                elif waiter.state == "granted":
                    waiter.state = "cancelled"
                    self._cancellations += 1
                    self._return_in_use_locked(waiter.granted_weight)
                    self._apply_cached_capacity_locked()
                    self._retire_waiter_control(waiter)
                    cancellation_deliveries = self._post_commit_reschedule_locked()
                elif waiter.state == "delivered" and future.done() and not future.cancelled():
                    try:
                        delivered_permit = future.result()
                    except BaseException:
                        delivered_permit = None
            if delivered_permit is not None:
                try:
                    delivered_permit.release()
                except BaseException as cleanup_error:
                    try:
                        add_bounded_note(
                            exc,
                            "remote I/O permit rollback also failed",
                            cleanup_error,
                        )
                    except BaseException:
                        pass
            self._deliver(cancellation_deliveries)
            raise

    def acquire_sync(
        self,
        weight: int = 1,
        *,
        label: str = "remote_io_sync",
        operation_id: str | None = None,
    ) -> RemoteIoPermit:
        """Acquire through the exact same fair ticket queue as async callers."""
        check_operation_cancelled(stage="remote_io_admission")
        if isinstance(weight, bool) or not isinstance(weight, int):
            raise TypeError("remote I/O permit weight must be an integer")
        if weight <= 0:
            raise ValueError("remote I/O permit weight must be > 0")
        event = Event()
        waiter = _Waiter(
            None,
            None,
            weight,
            _bounded_metadata(label, kind="label"),
            _bounded_metadata(operation_id or "standalone-sync", kind="operation"),
            sync_event=event,
        )
        waiter.control_ticket = reserve_control_plane("remote_io_waiter", 512)
        pressure_scale = system_pressure_snapshot().scale
        deliveries_before: _GrantBatch | None = None
        deliveries_after: _GrantBatch | None = None
        enqueued = False
        registered = False
        try:
            with self._lock:
                if self._admission_closed:
                    raise RuntimeError("remote I/O admission is closed")
                self._pressure_scale = pressure_scale
                self._recompute_capacity_locked()
                deliveries_before = self._grant_ready_locked()
                if self._waiting_count >= self._max_waiters:
                    self._rejected_waiters += 1
                    raise SchemaSanitizerResourceError(
                        "remote I/O wait queue exhausted",
                        detail={
                            "stage": "remote_io_admission",
                            "limit_name": "remote_io_waiters",
                            "limit_items": self._max_waiters,
                            "actual_items": self._max_waiters + 1,
                        },
                    )
                self._sync_waiters += 1
                self._peak_sync_waiters = max(self._peak_sync_waiters, self._sync_waiters)
                registered = True
                self._enqueue_waiter_locked(waiter)
                enqueued = waiter.indexed
                self._peak_waiting = max(self._peak_waiting, self._waiting_count)
                try:
                    deliveries_after = self._grant_ready_locked()
                except BaseException:
                    self._record_post_commit_failure_locked()
            self._deliver(deliveries_before)
            self._deliver(deliveries_after)
            while True:
                check_operation_cancelled(stage="remote_io_admission")
                self._repair_waiter_progress_noexcept(waiter)
                if event.wait(timeout=0.05):
                    break
            check_operation_cancelled(stage="remote_io_admission")
            if waiter.delivery_error is not None:
                raise waiter.delivery_error
            permit = waiter.delivered_permit
            if permit is None:
                raise RuntimeError("synchronous remote I/O waiter woke without a permit")
            return permit
        except BaseException as exc:
            cancellation_deliveries: _GrantBatch | None = None
            delivered_permit: RemoteIoPermit | None = None
            with self._lock:
                if waiter.state == "queued":
                    waiter.state = "cancelled"
                    self._cancellations += 1
                    self._remove_waiter_locked(waiter)
                    cancellation_deliveries = self._post_commit_reschedule_locked()
                elif waiter.state == "granted":
                    waiter.state = "cancelled"
                    self._cancellations += 1
                    self._return_in_use_locked(waiter.granted_weight)
                    self._apply_cached_capacity_locked()
                    self._retire_waiter_control(waiter)
                    cancellation_deliveries = self._post_commit_reschedule_locked()
                elif waiter.state == "delivered":
                    delivered_permit = waiter.delivered_permit
            if delivered_permit is not None:
                try:
                    delivered_permit.release()
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc, "remote I/O sync permit rollback also failed", cleanup_error
                    )
            self._deliver(cancellation_deliveries)
            if not enqueued:
                self._retire_waiter_control(waiter)
            raise
        finally:
            if registered:
                with self._lock:
                    self._finish_sync_waiter_locked()

    def snapshot(self) -> RemoteIoPermitSnapshot:
        """Return a bounded snapshot of remote I/O permit activity."""
        with self._lock:
            return RemoteIoPermitSnapshot(
                capacity=self._capacity,
                in_use=self._in_use,
                peak_in_use=self._peak_in_use,
                waiting=self._waiting_count,
                peak_waiting=self._peak_waiting,
                grants=self._grants,
                cancellations=self._cancellations,
                bounded_bypasses=self._bounded_bypasses,
                active_capacity_registrations=len(self._registrations),
                over_release_count=self._over_release_count,
                over_release_weight=self._over_release_weight,
                queue_capacity=self._max_waiters,
                rejected_waiters=self._rejected_waiters,
                pending_submissions=self._pending_submissions,
                peak_pending_submissions=self._peak_pending_submissions,
                submission_capacity=self._max_pending_submissions,
                rejected_submissions=self._rejected_submissions,
                delivery_failures=self._delivery_failures,
                active_permits=len(self._permit_owners),
                unknown_permit_releases=self._unknown_permit_releases,
                unknown_submission_releases=self._unknown_submission_releases,
                unknown_capacity_releases=self._unknown_capacity_releases,
                capacity_registration_capacity=self._max_capacity_registrations,
                rejected_capacity_registrations=self._rejected_capacity_registrations,
                active_submission_reservations=len(self._submission_owners),
                active_capacity_capabilities=len(self._capacity_owners),
                scheduling_dirty=self._scheduling_dirty,
                post_commit_failures=self._post_commit_failures,
                bucket_rebuilds=self._bucket_rebuilds,
                sync_waiters=self._sync_waiters,
                peak_sync_waiters=self._peak_sync_waiters,
                protocol_violations=self._protocol_violations,
                admission_closed=self._admission_closed,
            )

    def _publish_permit_locked(
        self,
        permit: RemoteIoPermit,
        weight: int,
        *,
        control_ticket: ControlPlaneTicket | None = None,
    ) -> None:
        """Publish permit while holding the governing lock."""
        publication = self._publish_capability_locked(
            permit, self._permit_owners, amount=weight, control_ticket=control_ticket
        )
        lease_id = publication.lease_id
        try:
            permit._activate(lease_id=lease_id, capability=publication.capability, weight=weight)
        except BaseException:
            self._permit_owners.pop(lease_id, None)
            permit._retire_finalizer_slot()
            raise

    def _release_permit_capability(self, lease_id: int, capability: object) -> None:
        """Return one permit while retaining control-ticket authority until commit."""
        deliveries: _GrantBatch | None = None
        with self._lock:
            entry = self._permit_owners.get(lease_id)
            if entry is None or entry.capability is not capability:
                self._unknown_permit_releases += 1
                raise RuntimeError("remote I/O permit is not authoritative")
            if not entry.resource_released:
                amount = entry.amount
                excess = max(0, amount - self._in_use)
                next_over_count = self._over_release_count + (1 if excess else 0)
                next_over_weight = self._over_release_weight + excess
                next_in_use = self._in_use - amount if not excess else self._in_use
                if excess:
                    self._protocol_violations += 1
                    self._over_release_count = next_over_count
                    self._over_release_weight = next_over_weight
                    raise RuntimeError("remote I/O permit counter underflow")
                # Capacity release is the primary commit. Keep ``entry`` rooted
                # until its control-plane ticket also retires.
                self._over_release_count = next_over_count
                self._over_release_weight = next_over_weight
                self._in_use = next_in_use
                entry.resource_released = True
                deliveries = self._post_commit_reschedule_locked()
            control_ticket = entry.control_ticket
        if control_ticket is not None:
            if not release_control_plane(control_ticket):
                raise RuntimeError("remote I/O permit control-plane retirement did not commit")
            entry.control_ticket = None
        with self._lock:
            current = self._permit_owners.get(lease_id)
            if current is entry and entry.resource_released and entry.control_ticket is None:
                self._permit_owners.pop(lease_id, None)
        self._deliver_noexcept(deliveries)

    def _release_permit(self, permit: RemoteIoPermit) -> None:
        """Return only the authoritative weight with a retryable control tail."""
        deliveries: _GrantBatch | None = None
        with self._lock:
            entry = self._permit_owners.get(permit._lease_id)
            if entry is None or not self._entry_matches(permit, entry):
                self._unknown_permit_releases += 1
                raise RuntimeError("remote I/O permit is not authoritative")
            if not entry.resource_released:
                amount = entry.amount
                excess = max(0, amount - self._in_use)
                next_over_count = self._over_release_count + (1 if excess else 0)
                next_over_weight = self._over_release_weight + excess
                next_in_use = self._in_use - amount if not excess else self._in_use
                if excess:
                    self._protocol_violations += 1
                    self._over_release_count = next_over_count
                    self._over_release_weight = next_over_weight
                    raise RuntimeError("remote I/O permit counter underflow")
                self._over_release_count = next_over_count
                self._over_release_weight = next_over_weight
                self._in_use = next_in_use
                entry.resource_released = True
                deliveries = self._post_commit_reschedule_locked()
            control_ticket = entry.control_ticket
        if control_ticket is not None:
            if not release_control_plane(control_ticket):
                raise RuntimeError("remote I/O permit control-plane retirement did not commit")
            entry.control_ticket = None
        with self._lock:
            current = self._permit_owners.get(permit._lease_id)
            if current is entry and entry.resource_released and entry.control_ticket is None:
                self._permit_owners.pop(permit._lease_id, None)
        self._deliver_noexcept(deliveries)

    def _effective_weight(self, waiter: _Waiter) -> int:
        """Clamp requested weight against the capacity at admission time."""
        return max(1, min(waiter.requested_weight, self._capacity))

    def _enqueue_waiter_locked(self, waiter: _Waiter) -> None:
        """Transactionally publish one waiter across all authoritative indexes."""
        if waiter.delivery_callback is None:
            # Allocate the loop callback before the waiter becomes authoritative.
            # Production delivery therefore creates no closure after grant commit.
            def deliver_waiter(waiter: _Waiter = waiter) -> None:
                """Deliver the acquired permit to its waiting operation."""
                self._delivery_callback(waiter)

            waiter.delivery_callback = deliver_waiter
        if waiter.indexed or waiter.state != "queued":
            return
        operation_id = waiter.operation_id
        queue = self._operation_waiters.get(operation_id)
        created_queue = queue is None
        if queue is None:
            # Construct before publishing so allocation failure has no side effect.
            queue = OrderedDict()
        next_waiting = self._waiting_count + 1
        published_queue = False
        published_order = False
        published_waiter = False
        try:
            queue[id(waiter)] = waiter
            published_waiter = True
            if created_queue:
                self._operation_waiters[operation_id] = queue
                published_queue = True
                self._operation_order[operation_id] = None
                published_order = True
            waiter.indexed = True
            self._waiting_count = next_waiting
            self._refresh_operation_weight_locked(operation_id)
        except BaseException:
            # Roll back only this waiter. Derived weight state is rebuildable.
            try:
                self._remove_operation_weight_locked(operation_id)
            except BaseException:
                self._scheduling_dirty = True
            self._waiting_count = next_waiting - 1
            waiter.indexed = False
            if published_waiter:
                queue.pop(id(waiter), None)
            if created_queue:
                if published_order:
                    self._operation_order.pop(operation_id, None)
                if published_queue:
                    self._operation_waiters.pop(operation_id, None)
            elif queue:
                try:
                    self._refresh_operation_weight_locked(operation_id)
                except BaseException:
                    self._scheduling_dirty = True
            raise

    def _operation_bucket_weight_locked(self, queue: OrderedDict[int, _Waiter]) -> int | None:
        """Return the lightest bounded local request without a temporary list."""
        head: _Waiter | None = None
        weight: int | None = None
        examined = 0
        for waiter in queue.values():
            if waiter.state != "queued":
                continue
            if head is None:
                head = waiter
                weight = self._effective_weight(waiter)
                if head.bypasses >= _MAX_HEAD_BYPASSES:
                    return weight
            else:
                assert weight is not None
                weight = min(weight, self._effective_weight(waiter))
            examined += 1
            if examined >= _MAX_LOCAL_BYPASS_SCAN + 1:
                break
        return weight

    def _remove_operation_weight_locked(self, operation_id: str) -> None:
        """Remove operation weight while holding the governing lock."""
        weight = self._operation_weights.pop(operation_id, None)
        if weight is None:
            return
        bucket = self._weight_buckets.get(weight)
        if bucket is not None:
            bucket.pop(operation_id, None)
            if not bucket:
                self._weight_buckets.pop(weight, None)
                self._weight_order.pop(weight, None)

    def _refresh_operation_weight_locked(self, operation_id: str) -> None:
        """Publish one operation in the eligible-weight ring."""
        self._remove_operation_weight_locked(operation_id)
        queue = self._operation_waiters.get(operation_id)
        if not queue:
            return
        weight = self._operation_bucket_weight_locked(queue)
        if weight is None:
            return
        bucket = self._weight_buckets.get(weight)
        if bucket is None:
            bucket = OrderedDict()
            self._weight_buckets[weight] = bucket
            self._weight_order[weight] = None
        bucket[operation_id] = None
        self._operation_weights[operation_id] = weight

    def _rebuild_weight_buckets_locked(self) -> None:
        """Rebuild derived weights only when live permit capacity changes."""
        self._weight_buckets = {}
        self._weight_order = OrderedDict()
        self._operation_weights = {}
        self._bucket_capacity = self._capacity
        for operation_id in self._operation_order:
            self._refresh_operation_weight_locked(operation_id)

    def _remove_operation_if_empty_locked(self, operation_id: str) -> None:
        """Drop one empty operation from both the queue map and active ring."""
        queue = self._operation_waiters.get(operation_id)
        if queue:
            return
        self._operation_waiters.pop(operation_id, None)
        self._operation_order.pop(operation_id, None)
        self._remove_operation_weight_locked(operation_id)

    @staticmethod
    def _retire_waiter_control(waiter: _Waiter) -> None:
        """Retire control-plane ownership for a completed permit waiter."""
        ticket = waiter.control_ticket
        if ticket is None:
            return
        try:
            if not release_control_plane(ticket):
                return
        except BaseException:
            # Keep the exact ticket attached so a later safe point can retry.
            return
        waiter.control_ticket = None

    def _remove_waiter_locked(self, waiter: _Waiter) -> None:
        """Remove one queued waiter; derived repair cannot undo the commit."""
        if not waiter.indexed:
            return
        queue = self._operation_waiters.get(waiter.operation_id)
        removed = queue is not None and queue.pop(id(waiter), None) is not None
        if removed:
            if self._waiting_count <= 0:
                self._record_protocol_violation_locked()
            else:
                self._waiting_count -= 1
        waiter.indexed = False
        # Everything below is derived cleanup. A failure may leave repair debt
        # but must never make a cancelled authoritative waiter reappear.
        try:
            self._retire_waiter_control(waiter)
            self._refresh_operation_weight_locked(waiter.operation_id)
            self._remove_operation_if_empty_locked(waiter.operation_id)
        except BaseException:
            self._record_post_commit_failure_locked()

    def _operation_candidate_locked(
        self,
        queue: OrderedDict[int, _Waiter],
        available: int,
    ) -> tuple[_Waiter, bool] | None:
        """Return one fitting local candidate with bounded head bypass."""
        while queue:
            head = next(iter(queue.values()))
            if head.state == "queued":
                break
            _key, stale = queue.popitem(last=False)
            stale.indexed = False
        if not queue:
            return None
        head = next(iter(queue.values()))
        if self._effective_weight(head) <= available:
            return head, False
        if head.bypasses >= _MAX_HEAD_BYPASSES:
            return None
        for waiter in islice(queue.values(), 1, _MAX_LOCAL_BYPASS_SCAN + 1):
            if waiter.state == "queued" and self._effective_weight(waiter) <= available:
                return waiter, True
        return None

    def _record_post_commit_failure_locked(self) -> None:
        """Latch repair debt without allowing diagnostic growth to escape."""
        self._scheduling_dirty = True
        try:
            if self._post_commit_failures < (1 << 31) - 1:
                self._post_commit_failures += 1
        except BaseException:
            pass

    def _recover_unindexed_operation_locked(self) -> bool:
        """Repair one missing derived index from authoritative operation queues."""
        for operation_id in self._operation_order:
            queue = self._operation_waiters.get(operation_id)
            if not queue:
                continue
            weight = self._operation_weights.get(operation_id)
            bucket = self._weight_buckets.get(weight) if weight is not None else None
            if (
                weight is None
                or bucket is None
                or operation_id not in bucket
                or weight not in self._weight_order
            ):
                self._refresh_operation_weight_locked(operation_id)
                return True
        return False

    def _take_candidate_locked(self, available: int) -> _Waiter | None:
        """Select one waiter; no fallible derived work follows queue retirement."""
        weight_count = len(self._weight_order)
        for skipped in range(weight_count):
            weight, _unused = self._weight_order.popitem(last=False)
            bucket = self._weight_buckets.get(weight)
            if not bucket:
                self._weight_buckets.pop(weight, None)
                continue
            operation_id, _unused = bucket.popitem(last=False)
            queue = self._operation_waiters.get(operation_id)
            if not queue:
                self._operation_weights.pop(operation_id, None)
                if bucket:
                    self._weight_order[weight] = None
                else:
                    self._weight_buckets.pop(weight, None)
                continue

            current_weight = self._operation_bucket_weight_locked(queue)
            if current_weight is None:
                self._operation_weights.pop(operation_id, None)
                if bucket:
                    self._weight_order[weight] = None
                else:
                    self._weight_buckets.pop(weight, None)
                continue
            if current_weight != weight:
                # All allocations happen while the authoritative waiter remains
                # in its operation queue. A failure can only damage derived hints.
                target = self._weight_buckets.get(current_weight)
                if target is None:
                    target = OrderedDict()
                    target[operation_id] = None
                    self._weight_buckets[current_weight] = target
                    self._weight_order[current_weight] = None
                else:
                    target[operation_id] = None
                self._operation_weights[operation_id] = current_weight
                if bucket:
                    self._weight_order[weight] = None
                else:
                    self._weight_buckets.pop(weight, None)
                continue

            if weight > available:
                bucket[operation_id] = None
                self._weight_order[weight] = None
                continue

            candidate = self._operation_candidate_locked(queue, available)
            if candidate is None:
                bucket[operation_id] = None
                self._weight_order[weight] = None
                continue
            waiter, bypassed_head = candidate
            head = next(iter(queue.values()))

            # Prepare every integer used by the grant before removing the waiter.
            granted_weight = self._effective_weight(waiter)
            next_in_use = self._in_use + granted_weight
            next_peak = max(self._peak_in_use, next_in_use)
            next_grants = self._grants + 1
            if self._waiting_count <= 0:
                self._record_protocol_violation_locked()
                return None
            next_waiting = self._waiting_count - 1
            next_bypasses = self._bounded_bypasses + skipped + (1 if bypassed_head else 0)
            next_head_bypasses = head.bypasses + 1 if bypassed_head else head.bypasses
            waiter.granted_weight = granted_weight
            waiter.prepared_next_in_use = next_in_use
            waiter.prepared_next_peak = next_peak
            waiter.prepared_next_grants = next_grants

            # Authoritative queue-retirement commit. Everything after this point
            # is assignment/pop/move or a best-effort derived-index repair.
            if waiter is head:
                queue.popitem(last=False)
            else:
                queue.pop(id(waiter), None)
                head.bypasses = next_head_bypasses
            waiter.indexed = False
            self._waiting_count = next_waiting
            self._bounded_bypasses = next_bypasses
            if queue:
                try:
                    self._operation_order.move_to_end(operation_id)
                except BaseException:
                    self._record_post_commit_failure_locked()
            else:
                self._operation_waiters.pop(operation_id, None)
                self._operation_order.pop(operation_id, None)
            self._operation_weights.pop(operation_id, None)

            try:
                self._retire_waiter_control(waiter)
                if queue:
                    self._refresh_operation_weight_locked(operation_id)
                if bucket:
                    self._weight_order[weight] = None
                elif not self._weight_buckets.get(weight):
                    self._weight_buckets.pop(weight, None)
            except BaseException:
                self._record_post_commit_failure_locked()
            return waiter
        return None

    def _grant_ready_locked(self) -> _GrantBatch:
        """Grant into a batch whose storage is allocated before any grant commit."""
        self._repair_scheduler_locked()
        available_units = max(0, self._capacity - self._in_use)
        max_grants = min(self._waiting_count, available_units)
        deliveries = _GrantBatch(max_grants)
        while self._waiting_count and deliveries.count < max_grants:
            self._apply_cached_capacity_locked()
            available = self._capacity - self._in_use
            if available <= 0:
                break
            try:
                waiter = self._take_candidate_locked(available)
            except BaseException:
                self._record_post_commit_failure_locked()
                break
            if waiter is None:
                try:
                    if self._recover_unindexed_operation_locked():
                        continue
                except BaseException:
                    self._record_post_commit_failure_locked()
                break

            # Storage was reserved before _take_candidate_locked could mutate an
            # authoritative queue, so retaining the grant cannot allocate here.
            deliveries.append(waiter)
            waiter.state = "granted"
            self._in_use = waiter.prepared_next_in_use
            self._peak_in_use = waiter.prepared_next_peak
            self._grants = waiter.prepared_next_grants
            self._last_granted_operation = waiter.operation_id
        return deliveries

    def _reclaim_granted_waiter_locked(
        self, current: _Waiter, *, delivery_failure: bool
    ) -> _GrantBatch | None:
        """Return one undelivered grant with all arithmetic prepared first."""
        if current.state != "granted":
            return None
        next_cancellations = self._cancellations + 1
        next_delivery_failures = self._delivery_failures + (1 if delivery_failure else 0)
        if not self._return_in_use_locked(current.granted_weight):
            current.state = "cancelled"
            return None
        current.state = "cancelled"
        self._cancellations = next_cancellations
        self._delivery_failures = next_delivery_failures
        self._apply_cached_capacity_locked()
        self._retire_waiter_control(current)
        return self._post_commit_reschedule_locked()

    def _deliver_waiter_callback(self, current: _Waiter) -> None:
        """Publish one pre-granted waiter; callback object is cached at init."""
        deliveries: _GrantBatch | None = None
        permit: RemoteIoPermit | None = None
        construction_error: BaseException | None = None
        with self._lock:
            if current.state != "granted":
                return
            future = current.future
            if future is not None and (future.cancelled() or future.done()):
                deliveries = self._reclaim_granted_waiter_locked(current, delivery_failure=False)
            else:
                try:
                    permit = RemoteIoPermit(self, current.granted_weight, current.label)
                except BaseException as exc:
                    construction_error = exc
                    deliveries = self._reclaim_granted_waiter_locked(current, delivery_failure=True)
                else:
                    try:
                        self._publish_permit_locked(
                            permit,
                            current.granted_weight,
                            control_ticket=current.control_ticket,
                        )
                    except BaseException as exc:
                        construction_error = exc
                        permit = None
                        deliveries = self._reclaim_granted_waiter_locked(
                            current, delivery_failure=True
                        )
                    else:
                        # Transfer the exact waiter charge to the permit ledger.
                        # The active permit therefore remains represented in the
                        # global control-plane envelope without a reserve/release gap.
                        current.control_ticket = None
                        current.state = "delivered"

        # Any replacement grants are already committed; enqueue their callbacks
        # before diagnostic/future publication can fail.
        self._deliver(deliveries)
        if current.sync_event is not None:
            current.delivery_error = construction_error
            current.delivered_permit = permit
            current.sync_event.set()
            return
        future = current.future
        if future is None:
            raise RuntimeError("remote I/O waiter has no delivery endpoint")
        if construction_error is not None:
            try:
                future.set_exception(construction_error)
            except BaseException as publication_error:
                if not isinstance(publication_error, (asyncio.InvalidStateError, RuntimeError)):
                    raise
        if permit is not None:
            try:
                future.set_result(permit)
            except BaseException as publication_error:
                with self._lock:
                    current.state = "cancelled"
                    try:
                        self._cancellations += 1
                    except BaseException:
                        pass
                try:
                    permit.release()
                except BaseException as cleanup_error:
                    add_bounded_note(
                        publication_error,
                        "remote-I/O permit rollback also failed after delivery publication",
                        cleanup_error,
                    )
                if not isinstance(publication_error, (asyncio.InvalidStateError, RuntimeError)):
                    raise

    def _deliver(self, waiters: _GrantBatch | None) -> None:
        """Deliver grant batches without allocating a post-commit queue/closure."""
        if waiters is None:
            return
        if not waiters:
            return
        active = getattr(self._delivery_local, "pending", None)
        if active is not None:
            active.extend_chain(waiters)
            return
        waiters.chain_tail = waiters
        self._delivery_local.pending = waiters
        current_batch: _GrantBatch | None = waiters
        try:
            while current_batch is not None:
                index = 0
                while index < current_batch.count:
                    waiter = current_batch.item(index)
                    index += 1
                    try:
                        callback = waiter.delivery_callback
                        if callback is None:
                            raise RuntimeError("remote I/O waiter has no delivery callback")
                        if waiter.loop is None:
                            callback()
                        else:
                            waiter.loop.call_soon_threadsafe(callback)
                    except BaseException as delivery_error:
                        replacements: _GrantBatch | None = None
                        with self._lock:
                            replacements = self._reclaim_granted_waiter_locked(
                                waiter, delivery_failure=True
                            )
                        # Reentrant call only links a preallocated batch onto the
                        # active chain; it cannot recurse or allocate a deque.
                        self._deliver(replacements)
                        try:
                            if waiter.sync_event is not None:
                                waiter.delivery_error = delivery_error
                                waiter.sync_event.set()
                            elif waiter.future is not None and not waiter.future.done():
                                waiter.future.set_exception(delivery_error)
                        except BaseException as publication_error:
                            if isinstance(delivery_error, (KeyboardInterrupt, SystemExit)):
                                add_bounded_note(
                                    delivery_error,
                                    "remote-I/O waiter failure publication also failed",
                                    publication_error,
                                )
                        if isinstance(delivery_error, (KeyboardInterrupt, SystemExit)):
                            # A batch contains grants whose capacity is already
                            # committed. Before propagating an asynchronous
                            # BaseException, reclaim every unvisited grant in
                            # this batch and the dynamically-linked replacement
                            # chain so no waiter can retain capacity forever.
                            tail_batch: _GrantBatch | None = current_batch
                            tail_index = index
                            while tail_batch is not None:
                                while tail_index < tail_batch.count:
                                    pending = tail_batch.item(tail_index)
                                    tail_index += 1
                                    try:
                                        with self._lock:
                                            more = self._reclaim_granted_waiter_locked(
                                                pending, delivery_failure=True
                                            )
                                        self._deliver(more)
                                    except BaseException as cleanup_error:
                                        add_bounded_note(
                                            delivery_error,
                                            "remote-I/O batch-tail grant reclamation also failed",
                                            cleanup_error,
                                        )
                                    try:
                                        if pending.sync_event is not None:
                                            pending.delivery_error = delivery_error
                                            pending.sync_event.set()
                                        elif pending.future is not None:
                                            pending.future.cancel()
                                    except BaseException:
                                        pass
                                tail_batch = tail_batch.next_batch
                                tail_index = 0
                            raise
                current_batch = current_batch.next_batch
        finally:
            try:
                del self._delivery_local.pending
            except AttributeError:
                pass

    def _make_fork_bank(self) -> _RemoteIoForkBank:
        """Allocate one child-only mutable bank during ordinary runtime."""
        return (
            Lock(),
            local(),
            {},
            {},
            {},
            {},
            {},
            OrderedDict(),
            {},
            OrderedDict(),
            {},
        )

    def prepare_for_fork(self) -> None:
        """Select a preallocated child bank; perform no at-fork allocation."""
        self._fork_prepared = self._fork_banks[self._fork_bank_index]

    def clear_fork_preparation(self) -> None:
        """Clear state established while preparing for a fork."""
        self._fork_prepared = None

    def reset_after_fork(self) -> None:
        """Swap only preallocated child state; never grow containers after fork."""
        prepared = self._fork_prepared
        if prepared is None:
            from ..core_impl.fork_safety import runtime_fork_poisoned

            if runtime_fork_poisoned():
                return
            self.prepare_for_fork()
            prepared = self._fork_prepared
            if prepared is None:
                return
        quarantine_inherited_state(
            "remote-io-a",
            self._lock,
            self._delivery_local,
            self._registrations,
            self._capacity_owners,
            self._submission_owners,
            self._permit_owners,
            self._operation_waiters,
        )
        quarantine_inherited_state(
            "remote-io-b",
            self._operation_order,
            self._weight_buckets,
            self._weight_order,
            self._operation_weights,
        )
        (
            self._lock,
            self._delivery_local,
            self._registrations,
            self._capacity_owners,
            self._submission_owners,
            self._permit_owners,
            self._operation_waiters,
            self._operation_order,
            self._weight_buckets,
            self._weight_order,
            self._operation_weights,
        ) = prepared
        self._fork_prepared = None
        self._fork_bank_index = 1 - self._fork_bank_index
        self._delivery_callback = self._deliver_waiter_callback
        self._rejected_capacity_registrations = 0
        self._capability_sequence = 0
        self._next_registration_token = 0
        self._pressure_scale = 1.0
        self._requested_capacity = self._base_capacity
        self._pressure_capacity = self._base_capacity
        self._capacity = self._base_capacity
        self._in_use = 0
        self._peak_in_use = 0
        self._bucket_capacity = self._capacity
        self._waiting_count = 0
        self._sync_waiters = 0
        self._peak_sync_waiters = 0
        self._rejected_waiters = 0
        self._pending_submissions = 0
        self._peak_pending_submissions = 0
        self._rejected_submissions = 0
        self._last_granted_operation = None
        self._grants = 0
        self._cancellations = 0
        self._bounded_bypasses = 0
        self._peak_waiting = 0
        self._over_release_count = 0
        self._over_release_weight = 0
        self._delivery_failures = 0
        self._unknown_permit_releases = 0
        self._unknown_submission_releases = 0
        self._unknown_capacity_releases = 0
        self._protocol_violations = 0
        self._admission_closed = False


_SHARED_REMOTE_IO_GOVERNOR = RemoteIoPermitGovernor(1)


def default_remote_io_permit_capacity() -> int:
    """Return the default weighted capacity for process-wide remote I/O admission."""
    return _DEFAULT_CAPACITY


def shared_remote_io_permit_governor() -> RemoteIoPermitGovernor:
    """Return the process-wide remote I/O permit governor."""
    return _SHARED_REMOTE_IO_GOVERNOR


def close_remote_io_permit_admission() -> None:
    """Freeze process-wide remote-I/O waiter/submission admission."""
    _SHARED_REMOTE_IO_GOVERNOR.close_admission()


def _reopen_remote_io_permit_admission_for_tests() -> None:
    """Reopen only the quiescent shared governor for isolated shutdown tests."""
    _SHARED_REMOTE_IO_GOVERNOR.reopen_admission_for_tests()


def process_remote_io_permit_snapshot() -> RemoteIoPermitSnapshot:
    """Capture the process-wide remote I/O admission snapshot."""
    return _SHARED_REMOTE_IO_GOVERNOR.snapshot()


def _reset_shared_after_fork() -> None:
    """Reset the shared remote I/O governor in a forked child."""
    _SHARED_REMOTE_IO_GOVERNOR.reset_after_fork()


from ..core_impl.fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "remote-io-permits",
    before=_SHARED_REMOTE_IO_GOVERNOR.prepare_for_fork,
    after_in_parent=_SHARED_REMOTE_IO_GOVERNOR.clear_fork_preparation,
    after_in_child=_reset_shared_after_fork,
)


from ..core_impl.shutdown_observers import (  # noqa: E402
    register_shutdown_observer as _register_shutdown_observer,
)

_register_shutdown_observer("remote_io_permits", process_remote_io_permit_snapshot)


__all__ = [
    "RemoteIoCapacityRegistration",
    "RemoteIoPermit",
    "RemoteIoPermitGovernor",
    "RemoteIoPermitSnapshot",
    "RemoteIoSubmissionReservation",
    "close_remote_io_permit_admission",
    "default_remote_io_permit_capacity",
    "process_remote_io_permit_snapshot",
    "shared_remote_io_permit_governor",
]
