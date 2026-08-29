"""Govern process-wide capacity for project-owned threads and file handles.

Exact leases, callbacks, runtime pools, native permits, physical descriptor transitions, and
governed file wrappers all share the admission and diagnostic accounting maintained here.
"""

from __future__ import annotations

import io
import os
import sys
import threading
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import Enum, auto
from functools import partial
from threading import Condition, Lock
from time import monotonic, monotonic_ns
from typing import Any, Callable, Iterator, TypeGuard

from ..errors import SchemaSanitizerResourceError
from .bounded_generation import BoundedGenerationPool, next_reusable_token
from .cancellation import bounded_wait_timeout, check_operation_cancelled
from .cgroup_view import (
    CgroupValueState,
    current_cgroup_view,
    read_effective_cgroup_headroom,
    read_effective_cgroup_integer,
)
from .control_plane_budget import ControlPlaneTicket, release_control_plane, reserve_control_plane
from .diagnostic_epoch import diagnostic_transition
from .durations import deadline_from_timeout, deadline_ns_from_timeout, remaining_seconds
from .finalization import runtime_is_finalizing
from .finalizer_cleanup import (
    PreparedFinalizerCleanup,
    acknowledge_prepared_finalizer_cleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    drain_finalizer_cleanup,
    reserve_finalizer_cleanup,
)
from .fork_safety import ensure_runtime_fork_safe, quarantine_inherited_state
from .governed_thread import defer_governed_thread_retirement, start_governed_thread
from .safe_errors import add_bounded_note, clear_exception_traceback
from .terminal_ownership import (
    publish_terminal_owner,
    retire_terminal_category,
)

try:
    import resource
except ImportError:  # pragma: no cover - Windows
    resource = None  # type: ignore[assignment]

_ORIGINAL_THREAD = threading.Thread

_RESOURCE_CLOSE_WAIT_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class ProcessResourceSnapshot:
    """Current and peak logical usage for one process resource."""

    capacity: int
    in_use: int
    peak_in_use: int
    waiting: int
    queue_capacity: int
    rejected_waiters: int
    over_release_count: int = 0
    over_release_amount: int = 0
    opportunistic_rejections: int = 0
    active_leases: int = 0
    availability_callbacks: int = 0
    rejected_callbacks: int = 0
    unknown_lease_releases: int = 0
    admission_closed: bool = False
    teardown_admission_closed: bool = False
    teardown_reserve: int = 0
    teardown_in_use: int = 0
    external_capacity: int = 0
    external_in_use: int = 0
    availability_dirty: bool = False
    availability_publication_failures: int = 0


@dataclass(frozen=True, slots=True)
class UncertainFdCloseSnapshot:
    """Describe descriptor capacity retained after uncertain closes."""

    debts: int
    capacity: int
    oldest_debt_ns: int
    rejected: int = 0


class AvailabilityEvent(Enum):
    """Closed set of privileged process-thread availability wakeups."""

    RETRY_SCHEDULER = auto()
    CLEANUP_DISPATCHER = auto()
    TEMPORARY_JANITOR = auto()


@dataclass(slots=True, eq=False)
class _AvailabilityDelivery:
    governor: "_Governor"
    event: AvailabilityEvent
    generation: int
    attempts: int = 0
    next_attempt_ns: int = 0
    dispatcher: Callable[[AvailabilityEvent], None] | None = dataclass_field(
        default=None, repr=False, compare=False
    )

    @property
    def key(self) -> tuple[int, AvailabilityEvent, int]:
        """Return the immutable queue key retained by this entry."""
        return (id(self.governor), self.event, self.generation)


def _dispatch_availability_event(event: AvailabilityEvent) -> None:
    """Dispatch one sealed event without executing caller-supplied code."""
    if event is AvailabilityEvent.RETRY_SCHEDULER:
        from .retry_scheduler import _SCHEDULER

        _SCHEDULER._ensure_workers()
        return
    if event is AvailabilityEvent.CLEANUP_DISPATCHER:
        from .cleanup_dispatcher import _DISPATCHER

        _DISPATCHER._availability_wakeup()
        return
    if event is AvailabilityEvent.TEMPORARY_JANITOR:
        from .temporary_janitor import _JANITOR

        _JANITOR._availability_wakeup()
        return
    raise RuntimeError("unknown internal availability event")


# Immutable production dispatcher identity.  Tests that need an alternate
# dispatcher inject it into a private governor explicitly; no runtime governor
# ever consults the mutable module-global after construction/registration.
_RUNTIME_AVAILABILITY_DISPATCHER = _dispatch_availability_event


@dataclass(eq=False, slots=True)
class _Waiter:
    amount: int
    teardown: bool = False
    control_ticket: ControlPlaneTicket | None = None


def _release_process_lease_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Release process lease capsule."""
    governor = capsule.arg0
    lease_id = capsule.arg1
    capability = capsule.arg2
    borrow_budget = capsule.arg3
    if borrow_budget is not None and int(getattr(borrow_budget, "borrowed", 0)) > 0:
        # A detached external runtime still subdivides this exact parent lease.
        # Keep the finalizer capsule retryable rather than bypassing the normal
        # release() guard and re-admitting credits beneath live workers.
        raise RuntimeError(
            "cannot finalize process resource lease while external runtime workers are borrowed"
        )
    if not isinstance(governor, _Governor) or type(lease_id) is not int or lease_id <= 0:
        return
    governor._release_lease_capability(lease_id, capability)


class _ExternalRuntimeBorrowResult:
    """Single-owner child-borrow handoff allocated before admission commits."""

    __slots__ = ("lease",)

    def __init__(self, lease: "_OperationThreadBorrowLease | None" = None) -> None:
        """Initialize the external runtime borrow result and its owned runtime state."""
        self.lease = lease


class _Lease:
    """Exactly-once reservation authenticated by the issuing governor ledger."""

    __slots__ = (
        "_governor",
        "_amount",
        "_lease_id",
        "_capability",
        "_pid",
        "_lock",
        "_released",
        "_sealed",
        "_finalizer_ticket",
        "_finalizer_capsule",
        "__dict__",
        "__weakref__",
    )

    _IMMUTABLE_FIELDS = frozenset(
        {"_governor", "_amount", "_lease_id", "_capability", "_pid", "_lock"}
    )
    _governor: _Governor
    _amount: int
    _lease_id: int
    _capability: object | None
    _pid: int
    _lock: Any
    _released: bool
    _sealed: bool
    _finalizer_ticket: int
    _finalizer_capsule: PreparedFinalizerCleanup | None

    def __init__(self, governor: "_Governor", amount: int, *, _active: bool = True) -> None:
        """Initialize the lease and its owned runtime state."""
        object.__setattr__(self, "_sealed", False)
        object.__setattr__(self, "_finalizer_ticket", 0)
        object.__setattr__(self, "_finalizer_capsule", None)
        capsule = reserve_finalizer_cleanup(_release_process_lease_capsule)
        ticket = capsule.ticket
        capsule.arg0 = governor
        object.__setattr__(self, "_finalizer_ticket", ticket)
        object.__setattr__(self, "_finalizer_capsule", capsule)
        object.__setattr__(self, "_governor", governor)
        object.__setattr__(self, "_amount", int(amount))
        object.__setattr__(self, "_lease_id", 0)
        object.__setattr__(self, "_capability", None)
        object.__setattr__(self, "_pid", os.getpid())
        object.__setattr__(self, "_lock", Lock())
        object.__setattr__(self, "_released", not _active)
        if _active:
            object.__setattr__(self, "_sealed", True)

    def __setattr__(self, name: str, value: object) -> None:
        """Reject mutation of lease identity after authoritative publication."""
        if getattr(self, "_sealed", False) and name in self._IMMUTABLE_FIELDS:
            raise AttributeError(f"{name} is immutable after lease publication")
        object.__setattr__(self, name, value)

    @property
    def amount(self) -> int:
        """Return the exact capacity retained by this lease."""
        return self._amount

    @property
    def lease_id(self) -> int:
        """Return the lease id."""
        return self._lease_id

    def _activate(
        self,
        *,
        amount: int | None = None,
        lease_id: int,
        capability: object,
    ) -> None:
        """Activate this lease with authoritative capacity and capability."""
        if self._sealed:
            raise RuntimeError("lease already published")
        if amount is not None:
            object.__setattr__(self, "_amount", int(amount))
        object.__setattr__(self, "_lease_id", int(lease_id))
        object.__setattr__(self, "_capability", capability)
        object.__setattr__(self, "_released", False)
        capsule = self._finalizer_capsule
        if capsule is not None:
            capsule.arg1 = lease_id
            capsule.arg2 = capability
        object.__setattr__(self, "_sealed", True)

    def _retire_finalizer_slot(self) -> None:
        """Retire the finalizer escrow slot owned by this lease."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
            object.__setattr__(self, "_finalizer_ticket", 0)
            object.__setattr__(self, "_finalizer_capsule", None)

    def _acknowledge_finalizer_slot(self) -> None:
        """Disarm primary replay after the governor release has committed."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            object.__setattr__(self, "_finalizer_ticket", 0)
            object.__setattr__(self, "_finalizer_capsule", None)

    def release(self) -> None:
        """Release resources owned by this lease."""
        if os.getpid() != self._pid:
            return
        # Keep the per-lease lock until the governor acknowledges removal from
        # its ledger.  This linearizes concurrent release() calls without ever
        # claiming success before the authoritative ledger has committed.
        with self._lock:
            if self._released:
                # A prior resource release may have committed while finalizer-slot
                # retirement failed. Keep retrying that exact authority.
                if self._finalizer_ticket and self._finalizer_capsule is not None:
                    self._acknowledge_finalizer_slot()
                return
            borrow_budget = self.__dict__.get("_external_runtime_borrow_budget")
            if borrow_budget is not None and int(getattr(borrow_budget, "borrowed", 0)) > 0:
                raise RuntimeError(
                    "cannot release operation thread lease while external runtime workers are borrowed"
                )
            self._governor._release_lease(self)
            self._released = True
            self._acknowledge_finalizer_slot()

    def reserve_exact_external_runtime_threads(self, amount: int) -> None:
        """Protect a suffix of this lease for one fixed-width runtime stage.

        Lazy sources may still be executing when an analytical adapter starts.
        A configurable source pool must not consume credits that were acquired
        specifically for a later, unshrinkable pool (for example Polars).  The
        reservation is admission metadata only: exact child claims remain the
        resource authority and the parent still cannot close while they exist.
        """
        if os.getpid() != self._pid:
            ensure_runtime_fork_safe()
            raise RuntimeError("process resource lease belongs to a different process")
        protected = max(0, int(amount))
        with self._lock:
            if self._released:
                raise RuntimeError("cannot reserve a released process resource lease")
            self.__dict__["_external_runtime_exact_reservation"] = protected
            budget = self.__dict__.get("_external_runtime_borrow_budget")
            if isinstance(budget, _OperationThreadBorrowBudget):
                budget.set_exact_reservation(protected)

    def borrow_external_runtime_threads(
        self, desired: int, *, minimum: int = 1, exact: bool = False
    ) -> _ExternalRuntimeBorrowResult:
        """Atomically subdivide this operation-owned thread lease.

        Publication of the child borrow is linearized by ``self._lock`` with
        release() and shrink(); a child can never appear after the parent has
        returned its process-wide credits.  The owner-PID check intentionally
        precedes every inherited mutex acquisition so a post-fork child fails
        fast instead of waiting forever on a lock whose owning thread vanished.
        """
        result = _ExternalRuntimeBorrowResult()
        if os.getpid() != self._pid:
            ensure_runtime_fork_safe()
            raise RuntimeError("process resource lease belongs to a different process")
        if self._governor is not _THREAD_GOVERNOR:
            return result
        wanted = max(0, int(desired))
        floor = max(0, int(minimum))
        with self._lock:
            if self._released:
                return result
            existing = self.__dict__.get("_external_runtime_borrow_budget")
            exact_reservation = max(
                0,
                int(self.__dict__.get("_external_runtime_exact_reservation", 0)),
            )
            if isinstance(existing, _OperationThreadBorrowBudget):
                budget = existing
                budget.set_capacity(max(0, self.amount - 1))
                budget.set_exact_reservation(exact_reservation)
            else:
                budget = _OperationThreadBorrowBudget(
                    max(0, self.amount - 1),
                    exact_reservation=exact_reservation,
                )
                self.__dict__["_external_runtime_borrow_budget"] = budget
                capsule = self._finalizer_capsule
                if capsule is not None:
                    capsule.arg3 = budget
            borrow_lease = budget.try_borrow_up_to_exact(
                wanted,
                minimum=floor,
                exact=bool(exact),
            )
            result.lease = borrow_lease
            return result

    def shrink(self, amount: int) -> None:
        """Return a suffix of an exact multi-resource lease without reacquiring."""
        if type(amount) is not int or amount <= 0:
            raise ValueError("process resource lease shrink amount must be > 0")
        if os.getpid() != self._pid:
            ensure_runtime_fork_safe()
            raise RuntimeError("process resource lease belongs to a different process")
        with self._lock:
            if self._released:
                raise RuntimeError("cannot shrink a released process resource lease")
            budget = self.__dict__.get("_external_runtime_borrow_budget")
            if isinstance(budget, _OperationThreadBorrowBudget):
                target_capacity = max(0, amount - 1)
                if budget.borrowed > target_capacity:
                    raise RuntimeError(
                        "cannot shrink operation thread lease below live external borrows"
                    )
            self._governor._shrink_lease(self, amount)
            if isinstance(budget, _OperationThreadBorrowBudget):
                budget.set_capacity(max(0, self.amount - 1))

    close = release

    def __enter__(self) -> "_Lease":
        """Enter the managed resource context."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Release managed resources when leaving the context."""
        self.release()

    def __del__(self) -> None:
        """Publish only a preallocated compact capability capsule from GC."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                if defer_prepared_finalizer_cleanup(capsule):
                    object.__setattr__(self, "_finalizer_ticket", 0)
                    object.__setattr__(self, "_finalizer_capsule", None)
        except BaseException:
            pass


@dataclass(slots=True)
class _LedgerEntry:
    owner_id: int
    amount: int
    capability: object
    teardown: bool = False
    control_ticket: ControlPlaneTicket | None = None
    native_fd_amount: int = 0
    # Exact native authority. ``native_fd_amount`` is mirrored diagnostics only
    # when this receipt is present.
    native_fd_lease: object | None = None
    resource_released: bool = False


_FORKED_GOVERNOR_KEEPALIVE: list[tuple[object, ...]] = []
_UNCERTAIN_FD_CLOSE_LOCK = Lock()


@dataclass(slots=True)
class _UncertainFdCloseDebtSlot:
    key: int = 0
    lease: object | None = None
    created_ns: int = 0
    label: str | None = None


# Physically preallocated terminal ownership: descriptor-close uncertainty must
# not allocate a dict node or tuple while the process may already be under OOM.
_UNCERTAIN_FD_CLOSE_DEBTS: list[_UncertainFdCloseDebtSlot] = [
    _UncertainFdCloseDebtSlot() for _ in range(16_384)
]
_UNCERTAIN_FD_CLOSE_COUNT = 0
_UNCERTAIN_FD_CLOSE_REJECTED = 0
_UNCERTAIN_FD_TERMINAL_RETAINED_BYTES = 256


class _Governor:
    """FIFO cancellable admission backed by exact per-lease capabilities."""

    def __init__(
        self,
        capacity: int,
        label: str,
        *,
        max_waiters: int | None = None,
        level_triggered_availability: bool = False,
        teardown_reserve: int = 0,
        availability_dispatcher: Callable[[AvailabilityEvent], None] | None = None,
    ) -> None:
        """Initialize the governor and its owned runtime state."""
        if type(capacity) is not int:
            raise TypeError("process resource capacity must be an exact integer")
        if type(teardown_reserve) is not int:
            raise TypeError("process resource teardown reserve must be an exact integer")
        self.capacity = max(0, capacity)
        self.label = label
        self._configured_teardown_reserve = max(0, teardown_reserve)
        self.teardown_reserve = max(0, min(teardown_reserve, max(0, self.capacity - 1)))
        self.external_capacity = self.capacity - self.teardown_reserve
        default_waiters = max(64, min(4096, self.capacity * 2))
        if max_waiters is not None and type(max_waiters) is not int:
            raise TypeError("process resource max_waiters must be an exact integer or None")
        configured_waiters = default_waiters if max_waiters is None else max_waiters
        self.max_waiters = max(1, configured_waiters)
        self._condition = Condition()
        self._in_use = 0
        self._external_in_use = 0
        self._teardown_in_use = 0
        self._peak = 0
        self._waiters: deque[_Waiter] = deque()
        self._rejected_waiters = 0
        self._over_release_count = 0
        self._over_release_amount = 0
        self._opportunistic_rejections = 0
        self._availability_events: dict[AvailabilityEvent, _AvailabilityDelivery] = {}
        self._availability_dirty = False
        self._availability_publication_failures = 0
        self._availability_sequence = 0
        self._max_availability_callbacks = 1024
        self._rejected_callbacks = 0
        self._lease_sequence = 0
        self._active_leases: dict[int, _LedgerEntry] = {}
        self._unknown_lease_releases = 0
        self._corrupted = False
        self._external_admission_closed = False
        self._teardown_admission_closed = False
        self._level_triggered_availability = bool(level_triggered_availability)
        self._availability_dispatcher = (
            _RUNTIME_AVAILABILITY_DISPATCHER
            if availability_dispatcher is None
            else availability_dispatcher
        )
        # Prebuild autonomous level-trigger retry metadata before any release
        # commit can need it.  A failed notifier publication therefore does not
        # depend on a future resource transition for liveness.
        self._availability_retry_key = ("process-resource-availability", id(self))
        self._availability_retry_callback = self._retry_dirty_availability

    def refresh_capacity(self, capacity: int) -> None:
        """Lower/raise future admission to current OS headroom without evicting owners."""
        if type(capacity) is not int:
            raise TypeError("process resource refreshed capacity must be an exact integer")
        with self._condition:
            self._reconcile_authority_locked()
            # Existing leases are physical facts. If the environment shrinks
            # below them, freeze new admission at the in-use watermark.
            effective = max(self._in_use, max(0, capacity))
            old_capacity = self.capacity
            self.capacity = effective
            self.teardown_reserve = max(
                0, min(self._configured_teardown_reserve, max(0, effective - 1))
            )
            self.external_capacity = max(0, effective - self.teardown_reserve)
            if effective != old_capacity:
                self._availability_dirty = bool(
                    self._availability_events and self._in_use < effective
                )
                self._condition.notify_all()

    def _raise_closed(self, *, teardown: bool = False) -> None:
        """Raise the standard closed-governor error."""
        domain = "teardown" if teardown else "external"
        raise SchemaSanitizerResourceError(
            f"process {self.label} {domain} admission is closed",
            detail={
                "stage": self.label,
                "limit_name": f"{self.label}_admission",
                "limit_items": 0,
                "actual_items": 1,
            },
        )

    def _authoritative_totals_locked(self) -> tuple[int, int, int]:
        """Return exact live ownership totals from authenticated lease entries."""
        total = 0
        external = 0
        teardown = 0
        for entry in self._active_leases.values():
            if entry.resource_released:
                continue
            amount = entry.amount
            if type(amount) is not int or amount < 0:
                self._corrupted = True
                continue
            total += amount
            if entry.teardown:
                teardown += amount
            else:
                external += amount
        return total, external, teardown

    def _reconcile_authority_locked(self) -> bool:
        """Repair derived counters from exact leases and quarantine on any drift.

        Admission counters are caches only.  A mismatch is sticky corruption: new
        work is closed, while exact lease shrink/release remains available to drain
        the authoritative owners.
        """
        total, external, teardown = self._authoritative_totals_locked()
        mismatch = (
            self._in_use != total
            or self._external_in_use != external
            or self._teardown_in_use != teardown
        )
        if mismatch:
            self._corrupted = True
            self._external_admission_closed = True
            self._teardown_admission_closed = True
            self._over_release_count += 1
            self._in_use = total
            self._external_in_use = external
            self._teardown_in_use = teardown
            try:
                diagnostic_transition()
            except BaseException:
                pass
            try:
                self._condition.notify_all()
            except BaseException:
                pass
        return not self._corrupted

    def _publish_lease_locked(
        self,
        lease: _Lease,
        amount: int,
        *,
        teardown: bool = False,
        control_ticket: ControlPlaneTicket | None = None,
    ) -> None:
        """Publish the authoritative capability before physical accounting commits."""
        lease_id = next_reusable_token(self._lease_sequence, self._active_leases)
        if lease_id is None:
            raise RuntimeError(f"process {self.label} lease namespace exhausted")
        capability = object()
        entry = _LedgerEntry(
            id(lease), amount, capability, teardown=teardown, control_ticket=control_ticket
        )
        self._active_leases[lease_id] = entry
        try:
            lease._activate(amount=amount, lease_id=lease_id, capability=capability)
        except BaseException:
            self._active_leases.pop(lease_id, None)
            raise
        self._lease_sequence = lease_id

    def _waiter_is_domain_head_locked(self, waiter: _Waiter) -> bool:
        """Preserve FIFO inside each admission domain without starving teardown."""
        for candidate in self._waiters:
            if candidate is waiter:
                return True
            if candidate.teardown is waiter.teardown:
                return False
        return False

    def _can_grant_locked(self, amount: int, *, teardown: bool) -> bool:
        """Return whether a request can be granted while holding the governor lock."""
        if not self._reconcile_authority_locked():
            return False
        if self._in_use + amount > self.capacity:
            return False
        if teardown:
            return True
        return self._external_in_use + amount <= self.external_capacity

    def _available_locked(self, *, teardown: bool) -> int:
        """Return available capacity while holding the governor lock."""
        total_available = max(0, self.capacity - self._in_use)
        if teardown:
            return total_available
        return min(
            total_available,
            max(0, self.external_capacity - self._external_in_use),
        )

    def _request_capacity_locked(self, *, teardown: bool) -> int:
        """Derive request capacity while holding the governor lock."""
        return self.capacity if teardown else self.external_capacity

    def _raise_if_waiter_became_impossible_locked(self, waiter: _Waiter) -> None:
        """Reject a queued request that no longer fits after a live capacity shrink.

        FIFO is preserved among requests that remain feasible.  An impossible
        head waiter must not become a permanent head-of-line barrier for smaller
        requests after cgroup/RLIMIT/process headroom changes.
        """
        capacity = self._request_capacity_locked(teardown=waiter.teardown)
        if waiter.amount <= capacity:
            return
        raise SchemaSanitizerResourceError(
            f"process {self.label} request no longer fits refreshed capacity",
            detail={
                "stage": self.label,
                "limit_name": f"{self.label}_refreshed_capacity",
                "limit_items": capacity,
                "actual_items": waiter.amount,
                "reason": "capacity_shrunk",
            },
        )

    def acquire(
        self,
        amount: int = 1,
        *,
        timeout_seconds: float | None = None,
        _teardown: bool = False,
    ) -> _Lease:
        """Acquire governed capacity through this governor."""
        if type(amount) is not int:
            raise TypeError(f"process {self.label} request must be an exact integer")
        if amount <= 0:
            raise ValueError(f"process {self.label} request must be > 0")
        requested = amount
        request_capacity = self.capacity if _teardown else self.external_capacity
        if requested > request_capacity:
            raise SchemaSanitizerResourceError(
                f"process {self.label} request exceeds process capacity",
                detail={
                    "stage": self.label,
                    "limit_name": self.label,
                    "limit_items": request_capacity,
                    "actual_items": requested,
                },
            )
        ensure_runtime_fork_safe()
        deadline = (
            None
            if timeout_seconds is None
            else deadline_from_timeout(
                timeout_seconds,
                name=f"process {self.label} timeout",
                allow_zero=True,
            )
        )
        check_operation_cancelled(stage=self.label)
        lease = _Lease(self, requested, _active=False)
        with self._condition:
            self._reconcile_authority_locked()
            if self._teardown_admission_closed or (
                self._external_admission_closed and not _teardown
            ):
                lease._retire_finalizer_slot()
                self._raise_closed(teardown=_teardown)
            if len(self._waiters) >= self.max_waiters:
                self._rejected_waiters += 1
                lease._retire_finalizer_slot()
                raise SchemaSanitizerResourceError(
                    f"process {self.label} wait queue exhausted",
                    detail={
                        "stage": self.label,
                        "limit_name": f"{self.label}_waiters",
                        "limit_items": self.max_waiters,
                        "actual_items": len(self._waiters) + 1,
                    },
                )
            waiter = _Waiter(requested, teardown=_teardown)
            try:
                waiter.control_ticket = reserve_control_plane(
                    f"process_resource_waiter:{self.label}", 256
                )
            except BaseException:
                lease._retire_finalizer_slot()
                raise
            try:
                self._waiters.append(waiter)
            except BaseException:
                release_control_plane(waiter.control_ticket)
                waiter.control_ticket = None
                lease._retire_finalizer_slot()
                raise
            granted = False
            try:
                while not self._waiter_is_domain_head_locked(waiter) or not self._can_grant_locked(
                    waiter.amount, teardown=waiter.teardown
                ):
                    self._raise_if_waiter_became_impossible_locked(waiter)
                    if self._teardown_admission_closed or (
                        self._external_admission_closed and not _teardown
                    ):
                        self._raise_closed(teardown=_teardown)
                    check_operation_cancelled(stage=self.label)
                    remaining = None if deadline is None else max(0.0, deadline - monotonic())
                    remaining = bounded_wait_timeout(remaining)
                    if remaining is not None and remaining <= 0:
                        raise SchemaSanitizerResourceError(
                            f"timed out waiting for process {self.label} capacity",
                            detail={
                                "stage": self.label,
                                "limit_name": self.label,
                                "limit_items": self.capacity,
                                "actual_items": self._in_use + requested,
                            },
                        )
                    self._condition.wait(timeout=0.1 if remaining is None else min(0.1, remaining))
                if self._teardown_admission_closed or (
                    self._external_admission_closed and not _teardown
                ):
                    self._raise_closed(teardown=_teardown)
                self._raise_if_waiter_became_impossible_locked(waiter)
                check_operation_cancelled(stage=self.label)
                self._waiters.remove(waiter)
                # Compute every potentially allocating integer result before
                # publishing either the capability or physical counters.
                next_in_use = self._in_use + waiter.amount
                next_teardown = self._teardown_in_use
                next_external = self._external_in_use
                if waiter.teardown:
                    next_teardown = self._teardown_in_use + waiter.amount
                else:
                    next_external = self._external_in_use + waiter.amount
                next_peak = max(self._peak, next_in_use)
                self._publish_lease_locked(
                    lease,
                    waiter.amount,
                    teardown=waiter.teardown,
                    control_ticket=waiter.control_ticket,
                )
                # The waiter charge becomes the active-lease charge. No gap and
                # no second allocation is needed at the admission commit point.
                waiter.control_ticket = None
                self._in_use = next_in_use
                self._teardown_in_use = next_teardown
                self._external_in_use = next_external
                self._peak = next_peak
                granted = True
                diagnostic_transition()
                self._condition.notify_all()
                return lease
            finally:
                if not granted:
                    try:
                        self._waiters.remove(waiter)
                    except ValueError:
                        pass
                    lease._retire_finalizer_slot()
                    self._condition.notify_all()
                ticket = waiter.control_ticket
                if ticket is not None and release_control_plane(ticket):
                    waiter.control_ticket = None

    def try_acquire_up_to(
        self, desired: int, *, minimum: int = 1, _teardown: bool = False
    ) -> _Lease:
        """Acquire up to the requested capacity without blocking."""
        if type(desired) is not int:
            raise TypeError(f"process {self.label} desired amount must be an exact integer")
        if type(minimum) is not int:
            raise TypeError(f"process {self.label} minimum amount must be an exact integer")
        if desired <= 0 or minimum <= 0:
            raise ValueError(f"process {self.label} desired and minimum amounts must be > 0")
        request_capacity = self.capacity if _teardown else self.external_capacity
        if minimum > request_capacity:
            raise SchemaSanitizerResourceError(
                f"process {self.label} minimum request exceeds process capacity",
                detail={
                    "stage": self.label,
                    "limit_name": self.label,
                    "limit_items": request_capacity,
                    "actual_items": minimum,
                },
            )
        ensure_runtime_fork_safe()
        required = minimum
        wanted = max(required, min(request_capacity, desired))
        lease = _Lease(self, wanted, _active=False)
        with self._condition:
            self._reconcile_authority_locked()
            if self._teardown_admission_closed or (
                self._external_admission_closed and not _teardown
            ):
                lease._retire_finalizer_slot()
                self._raise_closed(teardown=_teardown)
            if any(waiter.teardown is _teardown for waiter in self._waiters):
                self._opportunistic_rejections += 1
                lease._retire_finalizer_slot()
                raise SchemaSanitizerResourceError(
                    f"process {self.label} capacity reserved for queued waiters",
                    detail={
                        "stage": self.label,
                        "limit_name": f"{self.label}_fifo",
                        "limit_items": request_capacity,
                        "actual_items": self._in_use + required,
                    },
                )
            available = self._available_locked(teardown=_teardown)
            granted = min(wanted, available)
            if granted < required:
                granted = 0
            if granted <= 0:
                lease._retire_finalizer_slot()
                raise SchemaSanitizerResourceError(
                    f"process {self.label} capacity exhausted",
                    detail={
                        "stage": self.label,
                        "limit_name": self.label,
                        "limit_items": self.capacity,
                        "actual_items": self._in_use + required,
                    },
                )
            next_in_use = self._in_use + granted
            next_teardown = self._teardown_in_use + granted if _teardown else self._teardown_in_use
            next_external = self._external_in_use if _teardown else self._external_in_use + granted
            next_peak = max(self._peak, next_in_use)
            control_ticket = reserve_control_plane(f"process_resource_lease:{self.label}", 256)
            try:
                self._publish_lease_locked(
                    lease, granted, teardown=_teardown, control_ticket=control_ticket
                )
            except BaseException:
                release_control_plane(control_ticket)
                lease._retire_finalizer_slot()
                raise
            self._in_use = next_in_use
            self._teardown_in_use = next_teardown
            self._external_in_use = next_external
            self._peak = next_peak
            diagnostic_transition()
            return lease

    def _return_capacity_locked(self, returned: int) -> tuple[_AvailabilityDelivery, ...]:
        """Return capacity while holding the governing lock."""
        if returned < 0 or returned > self._in_use:
            self._over_release_count += 1
            self._over_release_amount += max(0, returned - self._in_use)
            returned = min(max(0, returned), self._in_use)
        self._in_use -= returned
        callbacks: tuple[_AvailabilityDelivery, ...] = ()
        if self._in_use < self.capacity and self._availability_events:
            # Deliveries are preconstructed at registration time; release never
            # needs to allocate a new callback control block.
            callbacks = tuple(self._availability_events.values())
        self._condition.notify_all()
        return callbacks

    def _release_lease_capability(self, lease_id: int, capability: object) -> None:
        """Release from a compact finalizer capsule using ledger authority only."""
        self._release_lease_entry(lease_id, capability, owner_id=None)

    def _release_lease_entry(
        self, lease_id: int, capability: object, *, owner_id: int | None
    ) -> None:
        """Release exact process authority with retryable native/control tails.

        Exact FD receipts are shrunk to zero *before* Python forgets them.  If
        an asynchronous exception lands after the native commit, retrying the
        same receipt is idempotent and the ledger still knows the owner.
        """
        native_fd_api: Any | None = None
        native_fd_lease: object | None = None
        should_publish = False

        with self._condition:
            entry = self._active_leases.get(lease_id)
            if (
                entry is None
                or (owner_id is not None and entry.owner_id != owner_id)
                or entry.capability is not capability
            ):
                self._unknown_lease_releases += 1
                diagnostic_transition()
                raise RuntimeError(f"unknown or corrupted process {self.label} lease release")
            self._reconcile_authority_locked()
            if not entry.resource_released and entry.native_fd_lease is not None:
                native_fd_api = _native_file_descriptor_api()
                native_fd_lease = entry.native_fd_lease

        if native_fd_lease is not None:
            committed = _native_fd_exact_resize(native_fd_api, native_fd_lease, 0)
            authoritative_after = committed[1]
            if authoritative_after != 0:
                raise RuntimeError("exact native FD release did not retire authority")

        with self._condition:
            entry = self._active_leases.get(lease_id)
            if (
                entry is None
                or (owner_id is not None and entry.owner_id != owner_id)
                or entry.capability is not capability
            ):
                self._unknown_lease_releases += 1
                diagnostic_transition()
                raise RuntimeError(f"unknown or corrupted process {self.label} lease release")
            self._reconcile_authority_locked()
            if not entry.resource_released:
                raw_returned = entry.amount
                bounded_returned = min(max(0, raw_returned), self._in_use)
                next_in_use = self._in_use - bounded_returned
                next_teardown = self._teardown_in_use
                next_external = self._external_in_use
                next_over_count = self._over_release_count
                next_over_amount = self._over_release_amount
                if raw_returned < 0 or raw_returned > self._in_use:
                    next_over_count = self._over_release_count + 1
                    next_over_amount = self._over_release_amount + max(
                        0, raw_returned - self._in_use
                    )
                if entry.teardown:
                    next_teardown = max(0, self._teardown_in_use - bounded_returned)
                else:
                    next_external = max(0, self._external_in_use - bounded_returned)
                should_publish = bool(next_in_use < self.capacity and self._availability_events)
                self._in_use = next_in_use
                self._teardown_in_use = next_teardown
                self._external_in_use = next_external
                self._over_release_count = next_over_count
                self._over_release_amount = next_over_amount
                entry.native_fd_lease = None
                entry.native_fd_amount = 0
                entry.amount = 0
                entry.resource_released = True
                if should_publish:
                    self._availability_dirty = True
                try:
                    self._condition.notify_all()
                except BaseException as exc:
                    clear_exception_traceback(exc)
                try:
                    diagnostic_transition()
                except BaseException as exc:
                    clear_exception_traceback(exc)
            control_ticket = entry.control_ticket

        if control_ticket is not None:
            if not release_control_plane(control_ticket):
                raise RuntimeError(f"process {self.label} control-plane retirement did not commit")
            with self._condition:
                if self._active_leases.get(lease_id) is entry:
                    entry.control_ticket = None
        with self._condition:
            if (
                self._active_leases.get(lease_id) is entry
                and entry.resource_released
                and entry.control_ticket is None
            ):
                self._active_leases.pop(lease_id, None)
        if should_publish:
            self._publish_available_events_noexcept()

    def _shrink_lease(self, lease: _Lease, amount: int) -> None:
        """Return a suffix while keeping exact native FD ownership retryable."""
        native_fd_api: Any | None = None
        native_fd_lease: object | None = None
        native_fd_target = 0
        with self._condition:
            entry = self._active_leases.get(lease.lease_id)
            if (
                entry is None
                or entry.owner_id != id(lease)
                or entry.capability is not lease._capability
            ):
                raise RuntimeError(f"unknown or corrupted process {self.label} lease shrink")
            if entry.resource_released:
                raise RuntimeError(f"cannot shrink process {self.label} lease after release commit")
            self._reconcile_authority_locked()
            if amount > entry.amount:
                raise ValueError("cannot grow a process resource lease via shrink")
            if amount == entry.amount:
                return
            returned = entry.amount - amount
            if entry.native_fd_lease is not None:
                native_fd_api = _native_file_descriptor_api()
                native_fd_lease = entry.native_fd_lease
                authoritative = _native_fd_exact_amount(native_fd_api, native_fd_lease)
                # Target is derived from the requested final lease width, not
                # from mirrored pre-shrink state. If native shrink committed and
                # Python was interrupted, retry observes authoritative==amount
                # and therefore becomes a no-op instead of shrinking twice.
                native_fd_target = min(max(0, int(amount)), authoritative)
        if native_fd_lease is not None:
            committed = _native_fd_exact_resize(native_fd_api, native_fd_lease, native_fd_target)
            authoritative_after = committed[1]
            if authoritative_after != native_fd_target:
                raise RuntimeError("exact native FD shrink did not publish target authority")

        with self._condition:
            entry = self._active_leases.get(lease.lease_id)
            if (
                entry is None
                or entry.owner_id != id(lease)
                or entry.capability is not lease._capability
                or entry.resource_released
            ):
                raise RuntimeError(f"process {self.label} lease changed during shrink")
            self._reconcile_authority_locked()
            if amount > entry.amount:
                raise ValueError("cannot grow a process resource lease via shrink")
            if amount == entry.amount:
                return
            returned = entry.amount - amount
            next_in_use = self._in_use - returned
            next_teardown = (
                self._teardown_in_use - returned if entry.teardown else self._teardown_in_use
            )
            next_external = (
                self._external_in_use if entry.teardown else self._external_in_use - returned
            )
            entry.amount = amount
            if native_fd_lease is not None:
                entry.native_fd_amount = native_fd_target
            self._in_use = next_in_use
            self._teardown_in_use = next_teardown
            self._external_in_use = next_external
            object.__setattr__(lease, "_amount", amount)
            if self._availability_events:
                self._availability_dirty = True
            try:
                self._condition.notify_all()
            except BaseException as exc:
                clear_exception_traceback(exc)
            try:
                diagnostic_transition()
            except BaseException as exc:
                clear_exception_traceback(exc)
        if self._availability_events:
            self._publish_available_events_noexcept()

    def _release_lease(self, lease: _Lease) -> None:
        """Commit one exact ledger release; never fail after the commit point."""
        return self._release_lease_entry(lease.lease_id, lease._capability, owner_id=id(lease))

    def _retry_dirty_availability(self) -> None:
        """Autonomously retry a level-triggered publication debt."""
        with self._condition:
            dirty = self._availability_dirty
        if dirty:
            self._publish_available_events_noexcept()

    def _schedule_availability_retry_noexcept(self) -> None:
        """Arm the notifier-owned emergency slot without using retry_scheduler."""
        try:
            _AVAILABILITY_NOTIFIER.arm_emergency_republish(self)
        except BaseException as exc:
            clear_exception_traceback(exc)

    def _publish_available_events_noexcept(self) -> None:
        """Level-triggered re-publication using only preconstructed deliveries."""
        # AvailabilityEvent is a sealed three-value enum, so capture references
        # into fixed locals instead of allocating a temporary collection.
        with self._condition:
            if self._in_use >= self.capacity or not self._availability_events:
                self._availability_dirty = False
                return
            retry_delivery = self._availability_events.get(AvailabilityEvent.RETRY_SCHEDULER)
            cleanup_delivery = self._availability_events.get(AvailabilityEvent.CLEANUP_DISPATCHER)
            janitor_delivery = self._availability_events.get(AvailabilityEvent.TEMPORARY_JANITOR)
        failed = False
        if retry_delivery is not None:
            try:
                failed = not _AVAILABILITY_NOTIFIER.publish_one(retry_delivery) or failed
            except BaseException as exc:
                clear_exception_traceback(exc)
                failed = True
        if cleanup_delivery is not None:
            try:
                failed = not _AVAILABILITY_NOTIFIER.publish_one(cleanup_delivery) or failed
            except BaseException as exc:
                clear_exception_traceback(exc)
                failed = True
        if janitor_delivery is not None:
            try:
                failed = not _AVAILABILITY_NOTIFIER.publish_one(janitor_delivery) or failed
            except BaseException as exc:
                clear_exception_traceback(exc)
                failed = True
        with self._condition:
            self._availability_dirty = failed and bool(self._availability_events)
            if failed:
                try:
                    if self._availability_publication_failures < (1 << 31) - 1:
                        self._availability_publication_failures += 1
                except BaseException as exc:
                    clear_exception_traceback(exc)
        if failed and self._level_triggered_availability:
            self._schedule_availability_retry_noexcept()

    def _delivery_is_current(self, delivery: _AvailabilityDelivery) -> bool:
        """Return whether a queued delivery still belongs to this governor generation."""
        with self._condition:
            return self._delivery_is_current_locked(delivery)

    def _ack_delivery(self, delivery: _AvailabilityDelivery) -> bool:
        """Acknowledge a completed asynchronous permit delivery."""
        with self._condition:
            if not self._delivery_is_current_locked(delivery):
                return False
            self._availability_events.pop(delivery.event, None)
            self._availability_dirty = False
            diagnostic_transition()
            self._condition.notify_all()
            return True

    def _delivery_is_current_locked(self, delivery: _AvailabilityDelivery) -> bool:
        """Return whether a delivery is current while holding the governor lock."""
        current = self._availability_events.get(delivery.event)
        return delivery.governor is self and current is delivery

    def register_availability_event(self, event: object) -> bool:
        """Register a callback for the next capacity transition."""
        ensure_runtime_fork_safe()
        if not isinstance(event, AvailabilityEvent):
            raise TypeError("availability event must be AvailabilityEvent")
        delivery: _AvailabilityDelivery | None = None
        with self._condition:
            if self._teardown_admission_closed:
                self._rejected_callbacks += 1
                return False
            existing = self._availability_events.get(event)
            if existing is not None:
                delivery = existing
            else:
                if len(self._availability_events) >= self._max_availability_callbacks:
                    self._rejected_callbacks += 1
                    return False
                if self._availability_sequence >= (1 << 63) - 1:
                    self._rejected_callbacks += 1
                    return False
                next_generation = self._availability_sequence + 1
                # Construct and seal the exact internal dispatcher before
                # publishing the registration.  Asynchronous work registered
                # before an unrelated module-global replacement cannot later
                # observe that replacement.
                delivery = _AvailabilityDelivery(
                    self,
                    event,
                    next_generation,
                    dispatcher=self._availability_dispatcher,
                )
                self._availability_events[event] = delivery
                self._availability_sequence = next_generation
                diagnostic_transition()
            immediate = bool(self._level_triggered_availability and self._in_use < self.capacity)
            if immediate:
                self._availability_dirty = True
        if immediate and delivery is not None:
            try:
                accepted = _AVAILABILITY_NOTIFIER.publish_one(delivery)
            except BaseException as exc:
                clear_exception_traceback(exc)
                accepted = False
            with self._condition:
                self._availability_dirty = not accepted
                if not accepted:
                    try:
                        if self._availability_publication_failures < (1 << 31) - 1:
                            self._availability_publication_failures += 1
                    except BaseException as exc:
                        clear_exception_traceback(exc)
            if not accepted and self._level_triggered_availability:
                self._schedule_availability_retry_noexcept()
        return True

    def unregister_availability_event(self, event: object) -> None:
        """Remove a capacity-transition callback registration."""
        if not isinstance(event, AvailabilityEvent):
            return
        with self._condition:
            if self._availability_events.pop(event, None) is not None:
                self._availability_dirty = (
                    bool(self._availability_events) and self._availability_dirty
                )
                diagnostic_transition()

    def close_external_admission(self) -> None:
        """Reject new user work while preserving the teardown reserve."""
        with self._condition:
            if not self._external_admission_closed:
                self._external_admission_closed = True
                diagnostic_transition()
            self._condition.notify_all()

    def close_admission(self) -> None:
        """Close both external and internal teardown admission."""
        with self._condition:
            if (
                not self._external_admission_closed
                or not self._teardown_admission_closed
                or self._availability_events
            ):
                self._external_admission_closed = True
                self._teardown_admission_closed = True
                self._availability_events.clear()
                diagnostic_transition()
            self._condition.notify_all()

    def reopen_admission_for_tests(self) -> None:
        """Reopen admission for an isolated test run."""
        with self._condition:
            self._corrupted = False
            self._reconcile_authority_locked()
            if self._external_admission_closed or self._teardown_admission_closed:
                self._external_admission_closed = False
                self._teardown_admission_closed = False
                diagnostic_transition()
            self._condition.notify_all()

    def snapshot(self) -> ProcessResourceSnapshot:
        # Snapshot is a normal governed safe point: make abandoned lease
        # handoffs observable and retry any level-triggered availability that
        # an earlier OOM could not publish.
        """Return a bounded snapshot of the current governor."""
        drain_finalizer_cleanup()
        if self._availability_dirty:
            self._publish_available_events_noexcept()
        with self._condition:
            return ProcessResourceSnapshot(
                self.capacity,
                self._in_use,
                self._peak,
                len(self._waiters),
                self.max_waiters,
                self._rejected_waiters,
                self._over_release_count,
                self._over_release_amount,
                self._opportunistic_rejections,
                len(self._active_leases),
                len(self._availability_events),
                self._rejected_callbacks,
                self._unknown_lease_releases,
                self._external_admission_closed,
                self._teardown_admission_closed,
                self.teardown_reserve,
                self._teardown_in_use,
                self.external_capacity,
                self._external_in_use,
                self._availability_dirty,
                self._availability_publication_failures,
            )

    def reset_after_fork(self) -> None:
        """Reset process-local state inherited across a fork."""
        quarantine_inherited_state(f"governor:{self.label}", self.__dict__)
        self._condition = Condition()
        self._in_use = 0
        self._external_in_use = 0
        self._teardown_in_use = 0
        self._peak = 0
        self._waiters = deque()
        self._rejected_waiters = 0
        self._over_release_count = 0
        self._over_release_amount = 0
        self._opportunistic_rejections = 0
        self._availability_events = {}
        self._availability_sequence = 0
        self._availability_dirty = False
        self._availability_publication_failures = 0
        self._availability_retry_key = ("process-resource-availability", id(self))
        self._availability_retry_callback = self._retry_dirty_availability
        self._rejected_callbacks = 0
        self._lease_sequence = 0
        self._active_leases = {}
        self._unknown_lease_releases = 0
        self._corrupted = False
        self._external_admission_closed = True
        self._teardown_admission_closed = True


@dataclass(frozen=True, slots=True)
class AvailabilityNotifierSnapshot:
    """Describe bounded resource-availability notification work."""

    pending_callbacks: int
    worker_alive: bool
    worker_starting: bool
    rejected_callbacks: int
    failed_worker_leases: int = 0
    worker_start_failures: int = 0
    lifecycle_state: str = "RUNNING"
    delayed_callbacks: int = 0
    parked_callbacks: int = 0
    callback_failures: int = 0
    retiring_worker: bool = False
    rearmed_events: int = 0
    emergency_republish_pending: bool = False


class _NotifierLifecycle(Enum):
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()
    FAILED = auto()


_MAX_AVAILABILITY_ATTEMPTS = 16
_MAX_NOTIFIER_RETRY_OWNERS = 1024
_NOTIFIER_RETRY_OWNERS_LOCK = Lock()
_NOTIFIER_RETRY_OWNERS: dict[int, object] = {}


def _retry_availability_notifier_token(token: int) -> None:
    """Resume the notifier owner retained under a retry token."""
    with _NOTIFIER_RETRY_OWNERS_LOCK:
        owner = _NOTIFIER_RETRY_OWNERS.pop(token, None)
    if owner is None:
        return
    owner._restart_from_retry()  # type: ignore[attr-defined]


class _AvailabilityNotifier:
    """Bounded host for sealed, acknowledged internal availability events."""

    def __init__(self, *, reserve_thread_slot: bool = False) -> None:
        """Initialize the availability notifier and its owned runtime state."""
        self._condition = Condition()
        self._queued: dict[tuple[int, AvailabilityEvent, int], _AvailabilityDelivery] = {}
        self._queued_keys: set[tuple[int, AvailabilityEvent, int]] = set()
        self._parked: dict[tuple[int, AvailabilityEvent, int], _AvailabilityDelivery] = {}
        self._rearmed: set[tuple[int, AvailabilityEvent]] = set()
        self._capacity = 1024
        self._worker: threading.Thread | None = None
        # Bind the emergency thread-governor on first worker admission.  This
        # prevents a later module-global replacement (tests/fork repair) from
        # redirecting an already-active notifier instance onto another owner's
        # permit pool.
        self._thread_governor: object | None = None
        self._starting = False
        self._rejected = 0
        self._start_failures = 0
        self._callback_failures = 0
        self._failed_leases: deque[_Lease] = deque()
        self._restart_scheduled = False
        self._state = _NotifierLifecycle.RUNNING
        self._shutdown_deadline_ns = 0
        self._retiring = False
        self._emergency_governor: _Governor | None = None
        self._reserved_thread_lease: _Lease | None = None
        if reserve_thread_slot:
            # One permanent logical permit belongs exclusively to this notifier.
            # Worker restarts therefore never depend on the scheduler/resource
            # availability event they are responsible for delivering.
            self._reserved_thread_lease = _NOTIFIER_THREAD_GOVERNOR.try_acquire_up_to(
                1, minimum=1, _teardown=True
            )
            self._thread_governor = _NOTIFIER_THREAD_GOVERNOR

    def arm_emergency_republish(self, governor: _Governor) -> None:
        """Remember one level-triggered debt without growing a container."""
        with self._condition:
            if self._state is not _NotifierLifecycle.RUNNING:
                return
            self._emergency_governor = governor
            self._condition.notify_all()
        try:
            self._ensure_worker(allow_stopping=False)
        except BaseException as exc:
            clear_exception_traceback(exc)

    def _park_delivery_locked(self, delivery: _AvailabilityDelivery) -> None:
        """Publish terminal destination before retiring queued authority."""
        self._parked[delivery.key] = delivery
        if globals().get("_AVAILABILITY_NOTIFIER") is self:
            publish_terminal_owner("availability_notifier", id(delivery), retained_bytes=256)

    def _schedule_restart(self) -> None:
        """Schedule governed restart of the availability notifier."""
        with self._condition:
            if (
                self._state is not _NotifierLifecycle.RUNNING
                or self._restart_scheduled
                or (not self._queued and self._emergency_governor is None)
            ):
                return
            self._restart_scheduled = True
        scheduled = False
        try:
            from .retry_scheduler import schedule_retry

            retry_token = id(self)
            with _NOTIFIER_RETRY_OWNERS_LOCK:
                if (
                    retry_token not in _NOTIFIER_RETRY_OWNERS
                    and len(_NOTIFIER_RETRY_OWNERS) >= _MAX_NOTIFIER_RETRY_OWNERS
                ):
                    scheduled = False
                else:
                    _NOTIFIER_RETRY_OWNERS[retry_token] = self
                    scheduled = schedule_retry(
                        ("availability-notifier", retry_token),
                        partial(_retry_availability_notifier_token, retry_token),
                        delay_seconds=0.05,
                        retained_bytes=256,
                        jitter_fraction=0.1,
                    )
                    if not scheduled:
                        _NOTIFIER_RETRY_OWNERS.pop(retry_token, None)
        except BaseException as exc:
            clear_exception_traceback(exc)
        if not scheduled:
            with self._condition:
                self._restart_scheduled = False
                self._condition.notify_all()

    def _restart_from_retry(self) -> None:
        """Restart the availability notifier after a governed retry."""
        with self._condition:
            self._restart_scheduled = False
            if self._state is not _NotifierLifecycle.RUNNING:
                return
        self._ensure_worker(allow_stopping=False)

    def publish_one(self, delivery: _AvailabilityDelivery) -> bool:
        """Transactionally admit one preconstructed delivery."""
        if type(delivery) is not _AvailabilityDelivery or delivery.dispatcher is None:
            with self._condition:
                self._rejected += 1
            return False
        self._retry_failed_leases()
        with self._condition:
            if self._state is not _NotifierLifecycle.RUNNING:
                self._rejected += 1
                return False
            key = delivery.key
            if key in self._queued or key in self._parked:
                try:
                    self._rearmed.add((id(delivery.governor), delivery.event))
                except BaseException:
                    self._rejected += 1
                    return False
                return True
            if len(self._queued) + len(self._parked) >= self._capacity:
                self._rejected += 1
                return False
            # Two growable indexes are prepared before commit. No deque node is
            # needed: the queued owner map itself is the bounded runnable index.
            try:
                self._queued_keys.add(key)
                try:
                    self._queued[key] = delivery
                except BaseException:
                    self._queued_keys.discard(key)
                    raise
            except BaseException:
                self._rejected += 1
                return False
            try:
                diagnostic_transition()
                self._condition.notify_all()
            except BaseException as exc:
                clear_exception_traceback(exc)
        try:
            self._ensure_worker(allow_stopping=False)
        except BaseException as exc:
            clear_exception_traceback(exc)
        return True

    def _ensure_worker(self, *, allow_stopping: bool) -> None:
        """Ensure the availability notifier owns a live governed worker."""
        self._retry_failed_leases()
        with self._condition:
            allowed = self._state is _NotifierLifecycle.RUNNING or (
                allow_stopping and self._state is _NotifierLifecycle.STOPPING
            )
            worker = self._worker
            if (
                not allowed
                or (not self._queued and self._emergency_governor is None)
                or self._failed_leases
                or self._starting
                or (worker is not None and worker.is_alive())
            ):
                return
            thread_governor = self._thread_governor
            if thread_governor is None:
                thread_governor = _NOTIFIER_THREAD_GOVERNOR
                self._thread_governor = thread_governor
            self._starting = True
        try:
            lease = self._reserved_thread_lease
            if lease is None:
                lease = thread_governor.try_acquire_up_to(1, minimum=1, _teardown=True)  # type: ignore[attr-defined]
        except BaseException as exc:
            clear_exception_traceback(exc)
            with self._condition:
                self._starting = False
                self._start_failures += 1
                diagnostic_transition()
                self._condition.notify_all()
            if not allow_stopping:
                self._schedule_restart()
            return
        thread = _ORIGINAL_THREAD(
            target=self._run,
            args=(lease,),
            name="schema-sanitizer-availability-notifier",
            daemon=True,
        )
        with self._condition:
            self._worker = thread
        try:
            start_governed_thread(thread)
        except BaseException as exc:
            clear_exception_traceback(exc)
            with self._condition:
                self._worker = None
                self._starting = False
                self._start_failures += 1
                diagnostic_transition()
                self._condition.notify_all()
            self._release_or_retain_lease(lease)
            if not allow_stopping:
                self._schedule_restart()
        else:
            try:
                from .retry_scheduler import cancel_retry

                cancel_retry(("availability-notifier", os.getpid()))
            except BaseException as exc:
                clear_exception_traceback(exc)
            with self._condition:
                self._restart_scheduled = False
                self._starting = False
                self._condition.notify_all()

    def _release_or_retain_lease(self, lease: _Lease) -> None:
        """Release or retain lease."""
        try:
            lease.release()
            return
        except BaseException as exc:
            clear_exception_traceback(exc)
        try:
            from .retry_scheduler import adopt_failed_release

            if adopt_failed_release(lease, retained_bytes=256):
                return
        except BaseException as exc:
            clear_exception_traceback(exc)
        with self._condition:
            if any(existing is lease for existing in self._failed_leases):
                return
            if len(self._failed_leases) >= 1:
                self._state = _NotifierLifecycle.FAILED
                self._condition.notify_all()
                raise RuntimeError("availability notifier lease ownership invariant exceeded")
            self._failed_leases.append(lease)
            diagnostic_transition()
            self._condition.notify_all()

    def _retry_failed_leases(self) -> None:
        """Retry release of thread leases retained after worker failure."""
        with self._condition:
            pending = tuple(self._failed_leases)
        for lease in pending:
            try:
                lease.release()
            except BaseException as exc:
                clear_exception_traceback(exc)
                continue
            with self._condition:
                try:
                    self._failed_leases.remove(lease)
                except ValueError:
                    pass
                else:
                    diagnostic_transition()
                self._condition.notify_all()

    def _take_due_locked(self) -> _AvailabilityDelivery | None:
        """Remove due availability callbacks while holding the notifier lock."""
        if not self._queued:
            return None
        if self._state is not _NotifierLifecycle.RUNNING:
            for delivery in self._queued.values():
                return delivery
            return None
        now_ns = monotonic_ns()
        due: _AvailabilityDelivery | None = None
        earliest = 0
        for delivery in self._queued.values():
            if delivery.next_attempt_ns <= now_ns:
                if due is None or delivery.next_attempt_ns < due.next_attempt_ns:
                    due = delivery
            elif earliest == 0 or delivery.next_attempt_ns < earliest:
                earliest = delivery.next_attempt_ns
        if due is not None:
            return due
        if earliest:
            self._condition.wait(timeout=min(0.25, max(0.001, (earliest - now_ns) / 1_000_000_000)))
        return None

    def _run(self, lease: _Lease) -> None:
        """Run the background worker loop for this availability notifier."""
        current = threading.current_thread()
        try:
            while True:
                emergency: _Governor | None = None
                with self._condition:
                    if self._emergency_governor is not None:
                        emergency = self._emergency_governor
                        self._emergency_governor = None
                        delivery = None
                    else:
                        delivery = self._take_due_locked()
                    if delivery is None and emergency is None:
                        if not self._queued:
                            return
                        continue
                if emergency is not None:
                    emergency._retry_dirty_availability()
                    continue
                if delivery is None:
                    continue
                if not delivery.governor._delivery_is_current(delivery):
                    with self._condition:
                        self._queued.pop(delivery.key, None)
                        self._queued_keys.discard(delivery.key)
                        diagnostic_transition()
                        self._condition.notify_all()
                    continue
                with self._condition:
                    # STOPPING itself is the hard dispatch barrier. The
                    # deadline bounds how long close() waits for a callback
                    # that had already linearized before STOPPING; it must not
                    # authorize new callbacks during the shutdown window.
                    terminal = self._state is not _NotifierLifecycle.RUNNING
                    if terminal:
                        self._park_delivery_locked(delivery)
                        self._queued.pop(delivery.key, None)
                        self._queued_keys.discard(delivery.key)
                        diagnostic_transition()
                        self._condition.notify_all()
                        continue
                succeeded = False
                try:
                    dispatcher = delivery.dispatcher
                    if dispatcher is None:
                        # Publication seals the dispatcher before queue
                        # visibility. Treat a malformed node as failed so bounded
                        # retry/parking preserves ownership.
                        succeeded = False
                    else:
                        dispatcher(delivery.event)
                        succeeded = True
                except BaseException as exc:
                    clear_exception_traceback(exc)
                with self._condition:
                    if succeeded:
                        base_key = (id(delivery.governor), delivery.event)
                        if base_key in self._rearmed:
                            self._rearmed.discard(base_key)
                            delivery.attempts = 0
                            delivery.next_attempt_ns = 0
                            # Owner remains in the authoritative queued map.
                        else:
                            delivery.governor._ack_delivery(delivery)
                            self._queued.pop(delivery.key, None)
                            self._queued_keys.discard(delivery.key)
                    else:
                        self._callback_failures += 1
                        delivery.attempts += 1
                        deadline_expired = bool(
                            self._state is _NotifierLifecycle.STOPPING
                            and self._shutdown_deadline_ns
                            and monotonic_ns() >= self._shutdown_deadline_ns
                        )
                        if delivery.attempts >= _MAX_AVAILABILITY_ATTEMPTS or deadline_expired:
                            self._park_delivery_locked(delivery)
                            self._queued.pop(delivery.key, None)
                            self._queued_keys.discard(delivery.key)
                        else:
                            delay_ns = min(
                                1_000_000_000,
                                10_000_000 * (2 ** min(delivery.attempts, 7)),
                            )
                            delivery.next_attempt_ns = monotonic_ns() + delay_ns
                            # Retry ownership remains in-place; no popleft/append.
                    diagnostic_transition()
                    self._condition.notify_all()
        finally:
            with self._condition:
                self._retiring = True
                self._condition.notify_all()
            if lease is not self._reserved_thread_lease:
                try:
                    defer_governed_thread_retirement(current, lease.release)
                except BaseException as exc:
                    clear_exception_traceback(exc)
            with self._condition:
                self._retiring = False
                if self._worker is current:
                    self._worker = None
                self._condition.notify_all()
            # A future publish/close is responsible for starting a successor.
            # Starting one from the retiring current thread would race its
            # physical exit even though the permit remains correctly charged.

    def snapshot(self) -> AvailabilityNotifierSnapshot:
        """Return a bounded snapshot of the current availability notifier."""
        with self._condition:
            worker = self._worker
            now_ns = monotonic_ns()
            return AvailabilityNotifierSnapshot(
                len(self._queued),
                bool(worker is not None and worker.is_alive()),
                self._starting,
                self._rejected,
                len(self._failed_leases),
                self._start_failures,
                self._state.name,
                sum(delivery.next_attempt_ns > now_ns for delivery in self._queued.values()),
                len(self._parked),
                self._callback_failures,
                self._retiring,
                len(self._rearmed),
                self._emergency_governor is not None,
            )

    def close(self, *, deadline_seconds: float = 5.0) -> bool:
        """Release resources owned by this availability notifier."""
        deadline_ns = deadline_ns_from_timeout(
            deadline_seconds, name="availability notifier shutdown deadline"
        )
        try:
            from .retry_scheduler import cancel_retry

            cancel_retry(("availability-notifier", os.getpid()))
        except BaseException as exc:
            clear_exception_traceback(exc)
        with self._condition:
            if self._state is _NotifierLifecycle.STOPPED:
                return not self._queued and not self._parked and not self._failed_leases
            if self._state is _NotifierLifecycle.FAILED:
                return False
            self._state = _NotifierLifecycle.STOPPING
            self._shutdown_deadline_ns = deadline_ns
            self._restart_scheduled = False
            diagnostic_transition()
            self._condition.notify_all()
        while True:
            self._retry_failed_leases()
            self._ensure_worker(allow_stopping=True)
            with self._condition:
                worker = self._worker
                worker_alive = bool(worker is not None and worker.is_alive())
                quiescent = not (
                    self._queued
                    or self._starting
                    or worker_alive
                    or self._retiring
                    or self._failed_leases
                )
                if quiescent:
                    self._state = _NotifierLifecycle.STOPPED
                    diagnostic_transition()
                    self._condition.notify_all()
                    return not self._parked
                remaining = remaining_seconds(deadline_ns)
                if remaining <= 0:
                    self._state = _NotifierLifecycle.FAILED
                    diagnostic_transition()
                    self._condition.notify_all()
                    return False
            if worker is not None and worker is not threading.current_thread():
                worker.join(timeout=min(0.01, remaining_seconds(deadline_ns)))
            with self._condition:
                self._condition.wait(timeout=min(0.01, remaining_seconds(deadline_ns)))

    def reopen_for_tests(self) -> None:
        """Reopen for an isolated test run."""
        with self._condition:
            if self._state is _NotifierLifecycle.RUNNING:
                return
            worker = self._worker
            if (
                self._queued
                or self._parked
                or self._failed_leases
                or (worker is not None and worker.is_alive())
            ):
                raise RuntimeError("cannot reopen a non-quiescent notifier")
            self._state = _NotifierLifecycle.RUNNING
            self._shutdown_deadline_ns = 0
            self._restart_scheduled = False
            diagnostic_transition()
            self._condition.notify_all()

    def reset_after_fork(self) -> None:
        """Reset process-local state inherited across a fork."""
        if globals().get("_AVAILABILITY_NOTIFIER") is self:
            retire_terminal_category("availability_notifier")
        quarantine_inherited_state(
            "availability-notifier",
            self._condition,
            self._queued,
            self._parked,
            self._worker,
            self._failed_leases,
        )
        self._condition = Condition()
        self._queued = {}
        self._queued_keys = set()
        self._parked = {}
        self._rearmed = set()
        self._worker = None
        self._thread_governor = None
        self._starting = False
        self._rejected = 0
        self._start_failures = 0
        self._callback_failures = 0
        self._failed_leases = deque()
        self._restart_scheduled = False
        self._state = _NotifierLifecycle.STOPPED
        self._shutdown_deadline_ns = 0
        self._retiring = False
        self._emergency_governor = None
        self._reserved_thread_lease = None


_FORKED_NOTIFIER_KEEPALIVE: list[tuple[object, ...]] = []
_NOTIFIER_THREAD_GOVERNOR = _Governor(1, "availability_notifier_threads")
_AVAILABILITY_NOTIFIER = _AvailabilityNotifier(reserve_thread_slot=True)


_DEFAULT_MAX_PROJECT_THREADS = 256
_ABSOLUTE_MAX_PROJECT_THREADS = 512
_ABSOLUTE_MAX_OPEN_FILES = 16_384
_CONSERVATIVE_THREAD_STACK_BYTES = 8 * 1024 * 1024
_MIN_THREAD_MEMORY_RESERVE_BYTES = 256 * 1024 * 1024


def _thread_stack_reservation_bytes() -> int:
    """Use the native stack model as the single source of truth when available."""
    minimum = _CONSERVATIVE_THREAD_STACK_BYTES
    try:
        from .native_runtime import native_core

        method = getattr(native_core, "process_thread_stack_reservation_bytes", None)
        if callable(method):
            return max(minimum, int(method()))
    except BaseException:
        pass
    configured = os.getenv("SCHEMA_SANITIZER_THREAD_STACK_RESERVATION_BYTES")
    if configured:
        try:
            minimum = max(minimum, int(configured))
        except (TypeError, ValueError):
            pass
    if resource is not None and hasattr(resource, "RLIMIT_STACK"):
        try:
            soft, _ = resource.getrlimit(resource.RLIMIT_STACK)
            if 0 < soft < (1 << 62):
                minimum = max(minimum, int(soft))
        except (OSError, ValueError):
            pass
    return minimum


def _read_bounded_system_integer(paths: tuple[str, ...]) -> int | None:
    """Read a positive cgroup-style integer without trusting hostile magnitudes."""
    for path in paths:
        try:
            value = open(path, "rt", encoding="ascii").read(64).strip()
        except (OSError, ValueError):
            continue
        if not value or value == "max":
            continue
        try:
            parsed = int(value, 10)
        except ValueError:
            continue
        if 0 < parsed < (1 << 62):
            return parsed
    return None


def _effective_memory_limit_sample():
    """Return the effective memory limit for the active cgroup generation."""
    view = current_cgroup_view()
    version = view.controller_version("memory")
    if version == 2:
        return read_effective_cgroup_integer("memory.max", controller="memory")
    if version == 1:
        return read_effective_cgroup_integer("memory.limit_in_bytes", controller="memory")
    # A known host without a cgroup controller is genuinely unbounded.  An
    # unresolved Linux hierarchy remains UNKNOWN and is handled fail-closed.
    state = CgroupValueState.UNBOUNDED if view.resolution_known else CgroupValueState.UNKNOWN
    from .cgroup_view import CgroupIntegerSample

    return CgroupIntegerSample(state)


def _effective_memory_ceiling_bytes() -> int | None:
    """Return the smallest trustworthy host/current-cgroup memory ceiling."""
    candidates: list[int] = []
    cgroup = _effective_memory_limit_sample()
    if cgroup.state is CgroupValueState.UNKNOWN:
        # Unknown is materially different from unlimited.  Returning zero makes
        # thread-stack admission stop rather than silently falling back to host
        # RAM and overcommitting a nested container.
        return 0
    if cgroup.state is CgroupValueState.VALUE and cgroup.value is not None:
        candidates.append(max(0, cgroup.value))
    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        pages = int(os.sysconf("SC_PHYS_PAGES"))
        if page_size > 0 and pages > 0 and pages <= (1 << 50) // page_size:
            candidates.append(page_size * pages)
    except (AttributeError, OSError, ValueError):
        pass
    return min(candidates) if candidates else None


def _effective_memory_headroom_bytes() -> int | None:
    """Return live memory headroom for new thread-stack admission."""
    view = current_cgroup_view()
    version = view.controller_version("memory")
    if version == 2:
        cgroup_headroom = read_effective_cgroup_headroom(
            "memory.max", "memory.current", controller="memory"
        )
    elif version == 1:
        cgroup_headroom = read_effective_cgroup_headroom(
            "memory.limit_in_bytes",
            "memory.usage_in_bytes",
            controller="memory",
        )
    else:
        from .cgroup_view import CgroupIntegerSample

        cgroup_headroom = CgroupIntegerSample(
            CgroupValueState.UNBOUNDED if view.resolution_known else CgroupValueState.UNKNOWN
        )
    if cgroup_headroom.state is CgroupValueState.UNKNOWN:
        return 0

    host_available: int | None = None
    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        pages = int(os.sysconf("SC_AVPHYS_PAGES"))
        if page_size > 0 and pages >= 0 and pages <= (1 << 50) // page_size:
            host_available = page_size * pages
    except (AttributeError, OSError, ValueError):
        pass
    cgroup_value = (
        cgroup_headroom.value if cgroup_headroom.state is CgroupValueState.VALUE else None
    )
    candidates = [value for value in (cgroup_value, host_available) if value is not None]
    return min(candidates) if candidates else None


def _process_physical_thread_count() -> int | None:
    """Return an OS/native count so Python admission sees C++/provider threads."""
    try:
        from .native_runtime import native_core

        value = native_core.process_physical_thread_count()
        if value is not None:
            count = int(value)
            if count >= 0:
                return count
    except BaseException:
        pass
    try:
        with os.scandir("/proc/self/task") as entries:
            return sum(1 for entry in entries if entry.name[:1].isdigit())
    except (OSError, AttributeError):
        return None


def _thread_requested_capacity() -> int:
    # This is the process-wide envelope, not a worker-count recommendation.
    # CPU-sized executors select their own widths, while the live hard-cap
    # calculation below accounts for native/provider threads already present
    # in the process.  A CPU-scaled envelope would therefore count the same
    # constraint twice and can collapse to zero after importing a runtime with
    # a persistent pool (Arrow, Polars, DuckDB, ...).
    """Return the configured process-wide project-thread envelope."""
    default = _DEFAULT_MAX_PROJECT_THREADS
    configured = os.getenv("SCHEMA_SANITIZER_MAX_PROJECT_THREADS")
    if configured:
        try:
            requested = int(configured)
        except ValueError:
            requested = default
    else:
        requested = default
    return max(0, min(_ABSOLUTE_MAX_PROJECT_THREADS, requested))


def _cgroup_pid_headroom() -> int | None:
    """Return the cgroup process-count headroom, when known."""
    view = current_cgroup_view()
    if view.controller_version("pids") not in (1, 2):
        return None if view.resolution_known else 0
    maximum = read_effective_cgroup_integer("pids.max", controller="pids")
    if maximum.state is CgroupValueState.UNKNOWN:
        return 0
    if maximum.state is CgroupValueState.UNBOUNDED:
        return None
    headroom = read_effective_cgroup_headroom("pids.max", "pids.current", controller="pids")
    if headroom.state is CgroupValueState.UNKNOWN:
        return 0
    if headroom.state is CgroupValueState.UNBOUNDED:
        return None
    assert maximum.value is not None and headroom.value is not None
    reserve = max(8, min(32, maximum.value // 16))
    return max(0, headroom.value - reserve)


def _thread_hard_capacity(*, governed_in_use: int = 0) -> int:
    """Return the live thread ceiling after process and cgroup headroom."""
    requested = _thread_requested_capacity()
    hard = requested
    observed_threads = _process_physical_thread_count()
    if observed_threads is not None:
        # Worst-case existing governed reservations are not yet represented in
        # the OS count.  Double-counting already-running governed threads is
        # conservative; under-counting a not-yet-started reservation is not.
        process_headroom = max(0, requested - observed_threads)
        hard = min(hard, max(governed_in_use, process_headroom))
    pid_headroom = _cgroup_pid_headroom()
    if pid_headroom is not None:
        hard = min(hard, governed_in_use + pid_headroom)
    if resource is not None and hasattr(resource, "RLIMIT_NPROC"):
        try:
            soft, _ = resource.getrlimit(resource.RLIMIT_NPROC)
        except (OSError, ValueError):
            soft = -1
        if 0 < soft < (1 << 50):
            hard = min(hard, max(0, int(soft) - 16))
    memory_headroom = _effective_memory_headroom_bytes()
    if memory_headroom is not None:
        usable = max(0, memory_headroom - _MIN_THREAD_MEMORY_RESERVE_BYTES)
        # Every live logical thread permit carries a conservative virtual stack
        # reservation even if the OS has not committed those pages yet. Without
        # subtracting existing permits here, repeated admissions could each see
        # the same live headroom and overbook future stack growth.
        stack_bytes = _thread_stack_reservation_bytes()
        virtual_stack_reserved = min(usable, max(0, governed_in_use) * stack_bytes)
        additional = max(0, usable - virtual_stack_reserved) // stack_bytes
        hard = min(hard, governed_in_use + additional)
    return max(0, hard)


def _thread_capacity() -> int:
    """Return the thread capacity."""
    return _thread_hard_capacity(governed_in_use=0)


def _open_fd_count() -> int | None:
    """Return the open fd count."""
    try:
        with os.scandir("/proc/self/fd") as entries:
            return sum(1 for _ in entries)
    except (OSError, AttributeError):
        pass
    # macOS has no /proc/self/fd. Reuse the native process authority so Python
    # and C++ subtract the same externally-open descriptor population.
    try:
        from .native_runtime import native_core

        method = getattr(native_core, "process_file_descriptor_count", None)
        if type(native_core).__name__ != "_MissingNative" and callable(method):
            observed = method()
            if observed is not None:
                return max(0, int(observed))
    except BaseException:
        pass
    return None


def _fd_requested_capacity() -> int:
    """Return the fd requested capacity."""
    configured = os.getenv("SCHEMA_SANITIZER_MAX_OPEN_FILES")
    if configured:
        try:
            requested = int(configured)
        except ValueError:
            requested = 4096
    else:
        requested = 4096
    return max(0, min(_ABSOLUTE_MAX_OPEN_FILES, requested))


_PYTHON_GOVERNED_FDS_OPENED = 0
_PYTHON_GOVERNED_FDS_OPENED_LOCK = Lock()


def _python_governed_fds_opened() -> int:
    """Return descriptors currently opened through Python-governed leases."""
    with _PYTHON_GOVERNED_FDS_OPENED_LOCK:
        return _PYTHON_GOVERNED_FDS_OPENED


def _mark_python_file_descriptors_opened_noexcept(amount: int) -> None:
    """Mark python file descriptors opened without propagating exceptions."""
    global _PYTHON_GOVERNED_FDS_OPENED
    if amount <= 0:
        return
    try:
        with _PYTHON_GOVERNED_FDS_OPENED_LOCK:
            _PYTHON_GOVERNED_FDS_OPENED += amount
    except BaseException:
        return


def _mark_python_file_descriptors_closed_noexcept(amount: int) -> None:
    """Mark python file descriptors closed without propagating exceptions."""
    global _PYTHON_GOVERNED_FDS_OPENED
    if amount <= 0:
        return
    try:
        with _PYTHON_GOVERNED_FDS_OPENED_LOCK:
            _PYTHON_GOVERNED_FDS_OPENED = max(0, _PYTHON_GOVERNED_FDS_OPENED - amount)
    except BaseException:
        return


def record_physical_file_descriptors_opened(amount: int = 1) -> None:
    """Record physically-open governed descriptors after ``open`` commits.

    This is intentionally separate from logical reservation.  Callers that
    consume a pre-acquired composite capability (for example remote I/O) use
    this hook immediately *after* the OS has created the descriptor.
    """
    if type(amount) is not int or amount <= 0:
        raise ValueError("physical descriptor amount must be a positive integer")
    _mark_python_file_descriptors_opened_noexcept(amount)
    _mark_native_file_descriptors_opened_noexcept(amount)


def record_physical_file_descriptors_closed(amount: int = 1) -> None:
    """Commit successful physical close before any logical credit release."""
    if type(amount) is not int or amount <= 0:
        raise ValueError("physical descriptor amount must be a positive integer")
    _mark_native_file_descriptors_closed_noexcept(amount)
    _mark_python_file_descriptors_closed_noexcept(amount)


def _fd_hard_capacity() -> int:
    # Capacity follows physically-open governed FDs rather than reservations
    # that may not have reached ``open()`` yet.
    """Return the fd hard capacity."""
    hard = _fd_requested_capacity()
    if resource is not None:
        try:
            soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        except (OSError, ValueError):
            soft = -1
        if 0 < soft < (1 << 50):
            reserve = max(16, min(256, int(soft) // 8))
            open_now = _open_fd_count()
            if open_now is None:
                hard = min(hard, max(0, int(soft) - reserve))
            else:
                # Python opens are mirrored into the native counter and C++ opens
                # are recorded there directly, so it is the canonical total.
                opened_governed = _python_governed_fds_opened()
                # This helper executes during module import before the public
                # snapshot function is defined, so probe the native ABI lazily
                # instead of calling a later global. Python-governed opens are
                # mirrored into this native counter; C++ opens contribute only
                # here, making it the canonical total when available.
                from .native_runtime import native_core as _fd_native_core

                _values = tuple(_fd_native_core.process_file_descriptor_permits_snapshot())
                if len(_values) != 6:
                    raise RuntimeError("native FD snapshot returned invalid statistics")
                opened_governed = max(opened_governed, int(_values[1]))
                external_open = max(0, open_now - opened_governed)
                hard = min(hard, max(0, int(soft) - reserve - external_open))
    return max(0, hard)


def _fd_capacity() -> int:
    """Return the fd capacity."""
    return _fd_hard_capacity()


_THREAD_CAPACITY = _thread_capacity()
_FD_CAPACITY = _fd_capacity()
_THREAD_GOVERNOR = _Governor(
    _THREAD_CAPACITY,
    "project_threads",
    level_triggered_availability=True,
    teardown_reserve=max(1, min(4, max(1, _THREAD_CAPACITY) // 16)) if _THREAD_CAPACITY else 0,
)
_FD_GOVERNOR = _Governor(
    _FD_CAPACITY,
    "open_file_descriptors",
    teardown_reserve=max(2, min(32, max(2, _FD_CAPACITY) // 32)) if _FD_CAPACITY else 0,
)


def _native_file_descriptor_api() -> Any:
    """Return the current shared native FD authority."""
    from .native_runtime import native_core

    return native_core


class _NativeFdPermitAcquisition:
    """Preallocated holder whose receipt owns native FD capacity exactly."""

    __slots__ = ("native", "lease", "amount")

    def __init__(self) -> None:
        """Initialize the native fd permit acquisition and its owned runtime state."""
        self.native: Any | None = None
        self.lease: object | None = None
        self.amount = 0


def _native_fd_exact_metadata(native: Any, receipt: object) -> tuple[int, int, int, int]:
    """Return the native fd exact metadata."""
    values = native.process_file_descriptor_permit_lease_metadata(receipt)
    if not isinstance(values, tuple) or len(values) != 4:
        raise RuntimeError("native FD receipt returned invalid metadata")
    return tuple(int(value) for value in values)  # type: ignore[return-value]


def _native_fd_exact_amount(native: Any, receipt: object) -> int:
    """Return the native fd exact amount."""
    return max(0, _native_fd_exact_metadata(native, receipt)[2])


def _native_fd_exact_opened(native: Any, receipt: object) -> int:
    """Return the exact opened-descriptor count from a native lease receipt."""
    return max(0, _native_fd_exact_metadata(native, receipt)[3])


def _native_fd_exact_resize(native: Any, receipt: object, target: int) -> tuple[int, int, int]:
    """Resize a native descriptor receipt and validate its returned metadata."""
    metadata = _native_fd_exact_metadata(native, receipt)
    result = native.process_file_descriptor_permit_lease_resize(
        receipt, max(0, int(target)), metadata[1]
    )
    if not isinstance(result, tuple) or len(result) != 3:
        raise RuntimeError("native FD receipt resize returned invalid metadata")
    return int(result[0]), max(0, int(result[1])), max(0, int(result[2]))


def _native_fd_exact_mark_opened(native: Any, receipt: object, amount: int) -> tuple[int, int, int]:
    """Mark descriptors opened on a native receipt and validate the transition."""
    metadata = _native_fd_exact_metadata(native, receipt)
    result = native.process_file_descriptor_permit_lease_mark_opened(
        receipt, max(0, int(amount)), metadata[1]
    )
    if not isinstance(result, tuple) or len(result) != 3:
        raise RuntimeError("native FD receipt open transition returned invalid metadata")
    return int(result[0]), max(0, int(result[1])), max(0, int(result[2]))


def _native_fd_exact_mark_closed(native: Any, receipt: object, amount: int) -> tuple[int, int, int]:
    """Mark exact descriptors closed and report whether the native transition committed."""
    metadata = _native_fd_exact_metadata(native, receipt)
    result = native.process_file_descriptor_permit_lease_mark_closed(
        receipt, max(0, int(amount)), metadata[1]
    )
    if not isinstance(result, tuple) or len(result) != 3:
        raise RuntimeError("native FD receipt close transition returned invalid metadata")
    return int(result[0]), max(0, int(result[1])), max(0, int(result[2]))


def _acquire_native_file_descriptor_permits(
    amount: int, *, timeout_seconds: float
) -> _NativeFdPermitAcquisition:
    """Acquire canonical native FD authority through an exact RAII receipt.

    The result holder exists before the native commit.  With the exact ABI, a
    temporary capsule owns the committed permits before Python publishes any
    ledger metadata, so an asynchronous unwind cannot orphan the reservation.
    """
    result = _NativeFdPermitAcquisition()
    native = _native_file_descriptor_api()
    result.native = native
    deadline = monotonic() + max(0.0, float(timeout_seconds))
    first_attempt = True
    while first_attempt or monotonic() < deadline:
        check_operation_cancelled(stage="open_file_descriptors")
        remaining = max(0.0, deadline - monotonic())
        slice_ms = 0 if remaining <= 0 else max(1, min(50, int(remaining * 1000)))
        first_attempt = False
        exact = native.process_file_descriptor_permit_lease_acquire_wait(amount, amount, slice_ms)
        if exact is not None:
            if not isinstance(exact, tuple) or len(exact) != 2:
                raise RuntimeError("native FD exact lease returned invalid receipt")
            receipt, granted = exact
            granted = int(granted)
            if granted == amount:
                result.lease = receipt
                result.amount = granted
                return result
            _native_fd_exact_resize(native, receipt, 0)
        if remaining <= 0:
            break
    raise SchemaSanitizerResourceError(
        "process file descriptor capacity exhausted",
        detail={
            "stage": "open_file_descriptors",
            "limit_name": "process_file_descriptors",
            "actual_items": amount,
        },
    )


def _mark_native_file_descriptors_opened_noexcept(amount: int) -> None:
    """Mark native file descriptors opened without propagating exceptions."""
    native = _native_file_descriptor_api()
    if amount > 0:
        try:
            native.process_file_descriptor_mark_opened(amount)
        except BaseException as exc:
            clear_exception_traceback(exc)


def _mark_native_file_descriptors_closed_noexcept(amount: int) -> None:
    """Mark native file descriptors closed without propagating exceptions."""
    native = _native_file_descriptor_api()
    if amount > 0:
        try:
            native.process_file_descriptor_mark_closed(amount)
        except BaseException as exc:
            clear_exception_traceback(exc)


def _attach_native_file_descriptor_permits(
    lease: _Lease, acquisition: _NativeFdPermitAcquisition
) -> None:
    """Bind exact native FD ownership to the authenticated Python lease."""
    amount = max(0, int(acquisition.amount))
    if amount <= 0:
        return
    governor = lease._governor
    with governor._condition:
        entry = governor._active_leases.get(lease.lease_id)
        if (
            entry is None
            or entry.owner_id != id(lease)
            or entry.capability is not lease._capability
            or entry.native_fd_amount != 0
            or entry.native_fd_lease is not None
        ):
            raise RuntimeError("cannot attach native FD permits to unknown lease")
        # Publish the exact owner before the mirrored scalar. If an interruption
        # follows, release/shrink can derive authoritative capacity from receipt.
        entry.native_fd_lease = acquisition.lease
        entry.native_fd_amount = amount


def _native_fd_receipt_for_python_lease(lease: _Lease) -> tuple[Any, object] | None:
    """Resolve the native receipt backing a live Python descriptor lease."""
    governor = lease._governor
    if governor is not _FD_GOVERNOR:
        return None
    with governor._condition:
        entry = governor._active_leases.get(lease.lease_id)
        if (
            entry is None
            or entry.owner_id != id(lease)
            or entry.capability is not lease._capability
            or entry.resource_released
            or entry.native_fd_lease is None
        ):
            return None
        receipt = entry.native_fd_lease
    native = _native_file_descriptor_api()
    return native, receipt


def _mark_file_descriptor_lease_opened(lease: _Lease, amount: int) -> int | None:
    """Mark file descriptor lease opened."""
    exact = _native_fd_receipt_for_python_lease(lease)
    if exact is None:
        raise RuntimeError("file descriptor lease lacks exact native authority")
    state = _native_fd_exact_mark_opened(exact[0], exact[1], amount)
    _mark_python_file_descriptors_opened_noexcept(amount)
    return state[2]


def _mark_file_descriptor_lease_closed(lease: _Lease, amount: int) -> int | None:
    """Mark descriptors closed and report whether exact accounting committed."""
    exact = _native_fd_receipt_for_python_lease(lease)
    if exact is None:
        raise RuntimeError("file descriptor lease lacks exact native authority")
    state = _native_fd_exact_mark_closed(exact[0], exact[1], amount)
    _mark_python_file_descriptors_closed_noexcept(amount)
    return state[2]


def _opened_for_file_descriptor_lease(lease: _Lease) -> int | None:
    """Return the exact opened count when a descriptor lease has a native receipt."""
    exact = _native_fd_receipt_for_python_lease(lease)
    if exact is None:
        return None
    return _native_fd_exact_opened(exact[0], exact[1])


def _refresh_thread_governor_capacity() -> None:
    """Refresh and return process thread-governor capacity."""
    with _THREAD_GOVERNOR._condition:
        in_use = _THREAD_GOVERNOR._in_use
    _THREAD_GOVERNOR.refresh_capacity(_thread_hard_capacity(governed_in_use=in_use))


def _refresh_fd_governor_capacity() -> None:
    """Refresh and return process descriptor-governor capacity."""
    _FD_GOVERNOR.refresh_capacity(_fd_hard_capacity())


_GUARDIAN_THREAD_GOVERNOR = _Governor(2, "release_guardian_emergency_threads")


def acquire_project_threads(desired: int, *, minimum: int = 1) -> _Lease:
    """Acquire up to the desired number of governed project threads."""
    from .governed_thread import reap_governed_thread_retirements

    reap_governed_thread_retirements()
    _refresh_thread_governor_capacity()
    return _THREAD_GOVERNOR.try_acquire_up_to(desired, minimum=minimum)


def _cleanup_operation_thread_borrow_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Cleanup operation thread borrow capsule."""
    budget = capsule.arg0
    claim_id = capsule.arg1
    capability = capsule.arg2
    budget_type = globals().get("_OperationThreadBorrowBudget")
    if budget_type is None or not isinstance(budget, budget_type):
        return
    if type(claim_id) is not int or claim_id <= 0 or capability is None:
        return
    budget._resize_claim(claim_id, capability, 0)


class _OperationThreadBorrowLease:
    """Exact child capability for operation-local external worker borrowing.

    The budget ledger authenticates this claim by id+capability.  The object is
    fully prepared before claim publication, so a Python unwind after commit
    simply drops this owner and returns the exact claim from ``__del__``.
    """

    __slots__ = (
        "_budget",
        "_claim_id",
        "_capability",
        "_pid",
        "_lock",
        "_released",
        "_amount",
        "_finalizer_ticket",
        "_finalizer_capsule",
    )

    def __init__(
        self, budget: "_OperationThreadBorrowBudget", claim_id: int, capability: object, amount: int
    ) -> None:
        """Initialize the operation thread borrow lease and its owned runtime state."""
        self._budget = budget
        self._claim_id = int(claim_id)
        self._capability = capability
        self._pid = os.getpid()
        self._lock = Lock()
        self._released = False
        self._amount = max(0, int(amount))
        cleanup = reserve_finalizer_cleanup(_cleanup_operation_thread_borrow_capsule)
        cleanup.arg0 = budget
        cleanup.arg1 = int(claim_id)
        cleanup.arg2 = capability
        self._finalizer_ticket = cleanup.ticket
        self._finalizer_capsule: PreparedFinalizerCleanup | None = cleanup

    @property
    def amount(self) -> int:
        """Return the exact capacity retained by this operation thread borrow lease."""
        if self._released:
            return 0
        try:
            return self._budget._claim_amount(self._claim_id, self._capability)
        except RuntimeError:
            return 0

    def shrink_to(self, target: int) -> int:
        """Shrink this borrow lease to an exact retained amount."""
        wanted = max(0, int(target))
        if os.getpid() != self._pid:
            ensure_runtime_fork_safe()
            raise RuntimeError("external runtime borrow belongs to a different process")
        with self._lock:
            if self._released:
                if wanted == 0:
                    return 0
                raise RuntimeError("cannot resize a released external runtime borrow")
            current = self._budget._resize_claim(self._claim_id, self._capability, wanted)
            self._amount = current
            if current == 0:
                self._released = True
            return current

    def _ack_finalizer(self) -> None:
        """Acknowledge and retire this lease's finalizer authority."""
        capsule = self._finalizer_capsule
        if self._finalizer_ticket and capsule is not None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def release(self) -> None:
        """Release resources owned by this operation thread borrow lease."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if not self._released:
                self._budget._resize_claim(self._claim_id, self._capability, 0)
                self._amount = 0
                self._released = True
            # Acknowledgement is retried even after the exact resource commit.
            # A ticket-cleanup fault can never make release replay the borrow.
            self._ack_finalizer()

    close = release

    def __del__(self) -> None:
        """Schedule best-effort cleanup during garbage collection."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            capsule = getattr(self, "_finalizer_capsule", None)
            ticket = getattr(self, "_finalizer_ticket", 0)
            if ticket and capsule is not None and defer_prepared_finalizer_cleanup(capsule):
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
        except BaseException:
            pass


class _OperationThreadBorrowBudget:
    """Exact child ledger for subdivisions of one operation thread lease."""

    __slots__ = (
        "capacity",
        "_exact_reservation",
        "_claims",
        "_next_claim",
        "_lock",
    )

    def __init__(self, capacity: int, *, exact_reservation: int = 0) -> None:
        """Initialize the operation thread borrow budget and its owned runtime state."""
        self.capacity = max(0, int(capacity))
        self._exact_reservation = max(0, int(exact_reservation))
        self._claims: dict[int, tuple[object, int]] = {}
        self._next_claim = 1
        self._lock = Lock()

    def _exact_borrowed_locked(self) -> int:
        """Return exact borrowed capacity while holding the borrow-budget lock."""
        return sum(max(0, int(amount)) for _capability, amount in self._claims.values())

    @property
    def borrowed(self) -> int:
        """Return the exact thread capacity currently borrowed."""
        with self._lock:
            return self._exact_borrowed_locked()

    def set_capacity(self, capacity: int) -> None:
        """Set and return the exact operation-thread borrow capacity."""
        value = max(0, int(capacity))
        with self._lock:
            if value < self._exact_borrowed_locked():
                raise RuntimeError(
                    "cannot shrink operation thread lease below live external borrows"
                )
            self.capacity = value

    def set_exact_reservation(self, amount: int) -> None:
        """Reserve capacity from shrinkable pools without creating ownership."""
        with self._lock:
            self._exact_reservation = max(0, int(amount))

    def _next_claim_id_locked(self) -> int:
        """Allocate the next claim identifier while holding the borrow-budget lock."""
        candidate = max(1, int(self._next_claim))
        start = candidate
        while candidate in self._claims:
            candidate += 1
            if candidate <= 0 or candidate > (1 << 63) - 1:
                candidate = 1
            if candidate == start:
                raise RuntimeError("external runtime borrow claim id space exhausted")
        self._next_claim = candidate + 1 if candidate < (1 << 63) - 1 else 1
        return candidate

    def try_borrow_up_to_exact(
        self, desired: int, *, minimum: int = 1, exact: bool = False
    ) -> _OperationThreadBorrowLease | None:
        """Borrow up to the requested exact thread capacity."""
        requested = max(0, int(desired))
        floor = max(0, int(minimum))
        if requested == 0:
            return None

        capability = object()
        with self._lock:
            protected = 0 if exact else min(self.capacity, self._exact_reservation)
            available = max(
                0,
                self.capacity - self._exact_borrowed_locked() - protected,
            )
            granted = min(requested, available)
            if granted < floor:
                return None
            claim_id = self._next_claim_id_locked()
            owner = _OperationThreadBorrowLease(self, claim_id, capability, granted)
            self._claims[claim_id] = (capability, granted)
            return owner

    def _claim_amount(self, claim_id: int, capability: object) -> int:
        """Return the amount retained by an operation-thread claim."""
        with self._lock:
            entry = self._claims.get(int(claim_id))
            if entry is None or entry[0] is not capability:
                raise RuntimeError("unknown external runtime borrow claim")
            return max(0, int(entry[1]))

    def _resize_claim(self, claim_id: int, capability: object, target: int) -> int:
        """Resize one authoritative operation-thread borrow claim."""
        wanted = max(0, int(target))
        with self._lock:
            entry = self._claims.get(int(claim_id))
            if entry is None or entry[0] is not capability:
                if wanted == 0:
                    return 0
                raise RuntimeError("unknown external runtime borrow claim")
            current = max(0, int(entry[1]))
            if wanted > current:
                raise RuntimeError("external runtime borrow claim cannot grow")
            if wanted == 0:
                del self._claims[int(claim_id)]
                return 0
            self._claims[int(claim_id)] = (capability, wanted)
            return wanted


class _ExternalNativeThreadAuthority:
    """Expose the current exact native thread authority through one small adapter."""

    __slots__ = ("_core",)

    def __init__(self, core: Any) -> None:
        """Initialize the external native thread authority and its owned runtime state."""
        self._core = core

    def acquire_exact_permit_lease(self, desired: int, minimum: int) -> tuple[object, int] | None:
        """Acquire exact permit lease."""
        result = self._core.process_external_runtime_thread_permit_lease_acquire(
            int(desired), int(minimum)
        )
        if result is None:
            return None
        if not isinstance(result, tuple) or len(result) != 2:
            raise RuntimeError("native external-runtime permit lease returned invalid receipt")
        return result[0], int(result[1])

    def resize_exact_permit_lease(self, lease: object, target: int) -> int:
        """Resize the runtime's exact physical-thread permit lease."""
        values = self._metadata(lease)
        result = self._core.process_external_runtime_thread_permit_lease_resize(
            lease, int(target), values[1]
        )
        if not isinstance(result, tuple) or len(result) != 2:
            raise RuntimeError("native external-runtime permit resize returned invalid metadata")
        return max(0, int(result[1]))

    def exact_permit_lease_amount(self, lease: object) -> int:
        """Return the exact capacity retained by a native permit receipt."""
        return max(0, self._metadata(lease)[2])

    def _metadata(self, lease: object) -> tuple[int, int, int]:
        """Return exact native permit metadata for the supplied receipt."""
        values = self._core.process_external_runtime_thread_permit_lease_metadata(lease)
        if not isinstance(values, tuple) or len(values) != 3:
            raise RuntimeError("native external-runtime permit receipt returned invalid metadata")
        return int(values[0]), int(values[1]), int(values[2])

    @property
    def supports_resident_attribution(self) -> bool:
        """Return whether the runtime reports resident-thread attribution."""
        return True

    def external_runtime_resident_threads_add(self, amount: int) -> None:
        """Add resident threads to external-runtime accounting."""
        self._core.process_external_runtime_resident_threads_add(int(amount))

    def external_runtime_resident_threads_release(self, amount: int) -> None:
        """Release resident threads from external-runtime accounting."""
        self._core.process_external_runtime_resident_threads_release(int(amount))

    @property
    def supports_stack_debt(self) -> bool:
        """Return whether the runtime reports stack-debt threads."""
        return True

    def external_runtime_stack_debt_threads_add(self, amount: int) -> None:
        """Add stack-debt threads to external-runtime accounting."""
        self._core.process_external_runtime_stack_debt_threads_add(int(amount))

    def external_runtime_stack_debt_threads_release(self, amount: int) -> None:
        """Release stack-debt threads from external-runtime accounting."""
        self._core.process_external_runtime_stack_debt_threads_release(int(amount))

    @property
    def supports_atomic_residency_update(self) -> bool:
        """Return whether the runtime supports atomic residency updates."""
        return True

    def external_runtime_residency_update(self, identity_delta: int, stack_debt_delta: int) -> None:
        """Apply one atomic external-runtime residency update."""
        self._core.process_external_runtime_residency_update(
            int(identity_delta), int(stack_debt_delta)
        )


class _ExactExternalRuntimeNativePermit:
    """Exact non-shared external-runtime permit owner backed by a native capsule."""

    __slots__ = ("_native", "_lease", "_pid")

    def __init__(self, native: _ExternalNativeThreadAuthority, lease: object) -> None:
        """Initialize the exact external runtime native permit and its owned runtime state."""
        self._native = native
        self._lease = lease
        self._pid = os.getpid()

    @property
    def amount(self) -> int:
        """Return the exact capacity retained by this exact external runtime native permit."""
        if os.getpid() != self._pid:
            ensure_runtime_fork_safe()
            raise RuntimeError("external runtime exact permit owner belongs to a different process")
        return self._native.exact_permit_lease_amount(self._lease)

    def resize_physical_thread_permits(self, target: int) -> None:
        """Resize the physical-thread permits owned by this runtime lease."""
        wanted = max(0, int(target))
        if os.getpid() != self._pid:
            ensure_runtime_fork_safe()
            raise RuntimeError("external runtime exact permit owner belongs to a different process")
        current = self._native.exact_permit_lease_amount(self._lease)
        if wanted > current:
            raise RuntimeError("external runtime exact permit owner cannot grow")
        self._native.resize_exact_permit_lease(self._lease, wanted)


class _ExternalNativePermitAcquisition:
    """Single-owner permit receipt allocated before native capacity commits."""

    __slots__ = ("owner", "amount")

    def __init__(self) -> None:
        """Initialize the external native permit acquisition and its owned runtime state."""
        self.owner: Any | None = None
        self.amount = 0


def _native_external_thread_api() -> _ExternalNativeThreadAuthority | None:
    """Return the current exact external-runtime physical-thread authority."""
    try:
        from .native_runtime import native_core
    except BaseException:
        return None
    if type(native_core).__name__ == "_MissingNative":
        return None
    return _ExternalNativeThreadAuthority(native_core)


def _acquire_external_native_thread_permits(amount: int) -> _ExternalNativePermitAcquisition:
    """Acquire non-shared external-runtime capacity with exact ownership."""
    result = _ExternalNativePermitAcquisition()
    requested = max(0, int(amount))
    if requested == 0:
        return result
    native = _native_external_thread_api()
    if native is None:
        return result
    exact = native.acquire_exact_permit_lease(requested, requested)
    if exact is None:
        return result
    lease, granted = exact
    if granted != requested:
        native.resize_exact_permit_lease(lease, 0)
        return result
    # The wrapper is the only Python-side release authority. If interruption
    # occurs before publication into ``result``, its native capsule dies and
    # returns the permits automatically.
    result.owner = _ExactExternalRuntimeNativePermit(native, lease)
    result.amount = granted
    return result


@dataclass(slots=True)
class _ExternalRuntimePoolCoordinatorEntry:
    """Single process-global authority for one external runtime *pool*.

    A runtime wrapper may expose ``schema_sanitizer_thread_pool_identity()`` to
    identify a pool shared by multiple wrapper objects.  Declared pool identities
    deliberately do not retain a strong reference to an arbitrary wrapper.
    """

    runtime: Any | None
    native: Any | None = None
    native_lease: object | None = None
    physical_amount: int = 0
    physical_claims: dict[int, int] = dataclass_field(default_factory=dict)
    logical_lease: _Lease | None = None
    logical_width: int = 0
    logical_claims: dict[int, int] = dataclass_field(default_factory=dict)
    next_physical_claim: int = 1
    next_logical_claim: int = 1
    configured_width: int | None = None
    resident_width: int = 0
    resident_stack_debt: int = 0
    resident_native: Any | None = None
    logical_acquire_inflight: bool = False
    config_inflight: bool = False
    config_generation: int = 0
    config_owner_thread_id: int | None = None
    config_state: str = "stable"
    config_attempted_width: int | None = None
    runtime_key: tuple[str, object] | None = None


_EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK = Lock()
_EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION = Condition(_EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK)

_ExternalRuntimeKey = tuple[str, object]


def _is_external_runtime_key(value: object) -> TypeGuard[_ExternalRuntimeKey]:
    """Recognize the sealed, namespaced coordinator key stored in finalizers."""
    return isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], str)


class _ExternalRuntimeCoordinator(dict[tuple[str, object], _ExternalRuntimePoolCoordinatorEntry]):
    """Coordinator map whose explicit reset retires exact claim slots first."""

    def clear(self) -> None:
        """Clear values and ownership retained by this external runtime coordinator."""
        global _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS, _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS
        slot_pool = globals().get("_EXTERNAL_RUNTIME_CLAIM_SLOTS")
        if isinstance(slot_pool, BoundedGenerationPool):
            for entry in self.values():
                for claim_id in entry.physical_claims:
                    owner = slot_pool.owner_for(claim_id)
                    if owner is not None:
                        slot_pool.release_for(owner)
                for claim_id in entry.logical_claims:
                    owner = slot_pool.owner_for(claim_id)
                    if owner is not None:
                        slot_pool.release_for(owner)
        super().clear()
        _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS = 0
        _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS = 0


_EXTERNAL_RUNTIME_POOL_COORDINATOR: _ExternalRuntimeCoordinator = _ExternalRuntimeCoordinator()
_EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS = 0
_EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS = 0
_MAX_EXTERNAL_RUNTIME_POOL_CLAIMS = 4096
_MAX_EXTERNAL_RUNTIME_POOL_ENTRIES = 1024
_MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS = 16_384
# Exact preallocated admission slots are the authority for coordinator-wide
# claim cardinality. Dict membership routes owners; aggregate integers below are
# diagnostics only and are rebuilt on observation.
_EXTERNAL_RUNTIME_CLAIM_SLOTS = BoundedGenerationPool(_MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS)


class _ExternalRuntimeCleanupDeferred(RuntimeError):
    """Internal signal: exact cleanup is tombstoned and must remain retryable."""


_MAX_EXTERNAL_RUNTIME_REPORTED_RESIDENT_THREADS = 65_536
_MAX_EXTERNAL_RUNTIME_CONFIG_GENERATION = (1 << 63) - 1
_MAX_EXTERNAL_RUNTIME_STABLE_PROBE_RETRIES = 8
_MAX_EXTERNAL_RUNTIME_POOL_IDENTITY_UNITS = 256
_EXTERNAL_RUNTIME_POOL_ENTRY_CONTROL_BYTES = 1024
_EXTERNAL_RUNTIME_POOL_CLAIM_CONTROL_BYTES = 256
# The registry is dynamically populated but globally bounded. Charge its full
# worst-case control metadata into the same static process control-plane budget
# used by other prebounded safety structures, so it cannot become invisible
# memory merely because entries/claims are created lazily.
from .static_control_plane import (  # noqa: E402
    register_static_control_plane as _register_external_pool_static,
)

_register_external_pool_static(
    "external_runtime_pool_coordinator",
    (_MAX_EXTERNAL_RUNTIME_POOL_ENTRIES * _EXTERNAL_RUNTIME_POOL_ENTRY_CONTROL_BYTES)
    + (_MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS * _EXTERNAL_RUNTIME_POOL_CLAIM_CONTROL_BYTES),
)
del _register_external_pool_static


@dataclass(frozen=True, slots=True)
class _ExternalRuntimeIntegration:
    """Sealed adapter for one process-global third-party worker pool.

    Identity and configuration strategy belong to schema-sanitizer rather than
    to optional methods injected into third-party modules. Resident evidence is
    deliberately opt-in because configured width is not proof of live threads.
    """

    namespace: str
    width_getter: str | None = None
    width_setter: str | None = None
    resident_probe: str | None = None


_EXTERNAL_RUNTIME_INTEGRATIONS: dict[str, _ExternalRuntimeIntegration] = {
    "pyarrow": _ExternalRuntimeIntegration(
        "schema_sanitizer:pyarrow:global_cpu_pool",
        width_getter="cpu_count",
        width_setter="set_cpu_count",
    ),
    "polars": _ExternalRuntimeIntegration(
        "schema_sanitizer:polars:global_cpu_pool",
        width_getter="thread_pool_size",
    ),
}


def _external_runtime_integration(runtime: Any) -> _ExternalRuntimeIntegration | None:
    """Return a sealed integration only for the canonical imported module object.

    A wrapper that merely spoofs ``__name__ = "pyarrow"``/``"polars"`` must
    never inherit the process-global identity of those runtimes.
    """
    name = getattr(runtime, "__name__", None)
    if type(name) is not str:
        return None
    integration = _EXTERNAL_RUNTIME_INTEGRATIONS.get(name)
    if integration is None:
        return None
    return integration if sys.modules.get(name) is runtime else None


def _external_runtime_integration_namespace(runtime: Any) -> object | None:
    """Return the sealed namespace for a known process-global runtime pool."""
    integration = _external_runtime_integration(runtime)
    return integration.namespace if integration is not None else None


def _external_runtime_pool_identity_key(runtime: Any) -> tuple[str, object]:
    """Return a namespaced pool identity that cannot collide across providers."""
    integration = _external_runtime_integration_namespace(runtime)
    if integration is not None:
        return ("integration", integration)
    getter = getattr(runtime, "schema_sanitizer_thread_pool_identity", None)
    if callable(getter):
        try:
            token = getter()
        except BaseException:
            token = None
        namespace_getter = getattr(runtime, "schema_sanitizer_thread_pool_namespace", None)
        if callable(namespace_getter):
            try:
                namespace = namespace_getter()
            except BaseException:
                namespace = None
        else:
            runtime_name = getattr(runtime, "__name__", None)
            if type(runtime) is type:
                namespace = runtime
            elif (
                type(runtime_name) is str
                and 0 < len(runtime_name) <= _MAX_EXTERNAL_RUNTIME_POOL_IDENTITY_UNITS
            ):
                namespace = "runtime:" + runtime_name
            else:
                namespace = type(runtime)
        valid_token = (
            (type(token) is str and 0 < len(token) <= _MAX_EXTERNAL_RUNTIME_POOL_IDENTITY_UNITS)
            or (
                type(token) is bytes and 0 < len(token) <= _MAX_EXTERNAL_RUNTIME_POOL_IDENTITY_UNITS
            )
            or (type(token) is int and token.bit_length() <= 128)
        )
        valid_namespace = (
            type(namespace) is type
            or (
                type(namespace) is str
                and 0 < len(namespace) <= _MAX_EXTERNAL_RUNTIME_POOL_IDENTITY_UNITS
            )
            or (
                type(namespace) is bytes
                and 0 < len(namespace) <= _MAX_EXTERNAL_RUNTIME_POOL_IDENTITY_UNITS
            )
            or (type(namespace) is int and namespace.bit_length() <= 128)
        )
        if valid_token and valid_namespace:
            return ("declared", (namespace, token))
    return ("runtime", id(runtime))


def _external_runtime_entry_locked(
    runtime: Any, *, create: bool, runtime_key: tuple[str, object] | None = None
) -> _ExternalRuntimePoolCoordinatorEntry | None:
    """Return the external-runtime entry while holding the coordinator lock."""
    if runtime_key is None:
        runtime_key = _external_runtime_pool_identity_key(runtime)
    entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_key)
    if entry is not None and runtime_key[0] == "runtime" and entry.runtime is not runtime:
        raise RuntimeError("external runtime pool identity collision")
    if entry is None and create:
        if len(_EXTERNAL_RUNTIME_POOL_COORDINATOR) >= _MAX_EXTERNAL_RUNTIME_POOL_ENTRIES:
            raise SchemaSanitizerResourceError(
                "external runtime pool coordinator capacity exhausted",
                detail={
                    "stage": "external_runtime_threads",
                    "limit_name": "external_runtime_pools",
                    "limit_items": _MAX_EXTERNAL_RUNTIME_POOL_ENTRIES,
                    "actual_items": len(_EXTERNAL_RUNTIME_POOL_COORDINATOR) + 1,
                },
            )
        entry = _ExternalRuntimePoolCoordinatorEntry(
            runtime=runtime if runtime_key[0] == "runtime" else None,
            runtime_key=runtime_key,
        )
        _EXTERNAL_RUNTIME_POOL_COORDINATOR[runtime_key] = entry
    return entry


def _retire_external_runtime_entry_locked(
    runtime_key: tuple[str, object], entry: _ExternalRuntimePoolCoordinatorEntry
) -> None:
    """Retire external runtime entry while holding the governing lock."""
    if (
        not entry.physical_claims
        and entry.physical_amount == 0
        and not entry.logical_claims
        and entry.logical_width == 0
        and entry.logical_lease is None
        and entry.resident_width == 0
        and entry.resident_stack_debt == 0
        and not entry.logical_acquire_inflight
        and not entry.config_inflight
        and entry.config_state == "stable"
    ):
        _EXTERNAL_RUNTIME_POOL_COORDINATOR.pop(runtime_key, None)


def _reconcile_external_runtime_claim_totals_locked() -> tuple[int, int]:
    """Rebuild diagnostic claim totals from exact claim membership.

    Claim dictionaries are the authority.  The aggregate counters are mirrors
    only: an asynchronous exception between dict publication and a scalar update
    must never prevent cleanup or manufacture admission capacity.
    """
    global _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS, _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS
    physical = sum(
        len(entry.physical_claims) for entry in _EXTERNAL_RUNTIME_POOL_COORDINATOR.values()
    )
    logical = sum(
        len(entry.logical_claims) for entry in _EXTERNAL_RUNTIME_POOL_COORDINATOR.values()
    )
    _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS = physical
    _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS = logical
    return physical, logical


def _external_runtime_total_claims_locked() -> int:
    """Return exact bounded claim-slot cardinality without scanning mappings."""
    return _EXTERNAL_RUNTIME_CLAIM_SLOTS.exact_active_count()


def _release_external_runtime_claim_slot_locked(claim_id: int) -> None:
    """Retire one exact claim slot by owner identity."""
    owner = _EXTERNAL_RUNTIME_CLAIM_SLOTS.owner_for(claim_id)
    if owner is not None:
        if not _EXTERNAL_RUNTIME_CLAIM_SLOTS.release_for(owner):
            raise RuntimeError("external runtime exact claim slot retirement failed")


def _note_external_runtime_claim_inserted_locked(*, logical: bool) -> None:
    """Best-effort diagnostic mirror publication; never an admission authority."""
    global _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS, _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS
    try:
        if logical:
            _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS = min(
                _MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS,
                _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS + 1,
            )
        else:
            _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS = min(
                _MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS,
                _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS + 1,
            )
    except BaseException:
        pass


def _note_external_runtime_claim_removed_locked(*, logical: bool) -> None:
    """Best-effort mirror decrement; stale-low values saturate at zero."""
    global _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS, _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS
    try:
        if logical:
            _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS = max(
                0, _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS - 1
            )
        else:
            _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS = max(
                0, _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS - 1
            )
    except BaseException:
        pass


def _next_external_runtime_claim_id(claims: dict[int, int], candidate: int) -> int:
    """Choose the next unused bounded claim identifier for an external runtime."""
    claim_id = max(1, int(candidate))
    while claim_id in claims:
        claim_id += 1
        if claim_id >= (1 << 63):
            claim_id = 1
    return claim_id


def _cleanup_shared_external_physical_claim_capsule(
    capsule: PreparedFinalizerCleanup,
) -> None:
    """Cleanup shared external physical claim capsule."""
    runtime_id = capsule.arg0
    claim_id = capsule.arg1
    if not _is_external_runtime_key(runtime_id) or type(claim_id) is not int or claim_id <= 0:
        return
    # Finalizer cleanup must never wait behind arbitrary third-party runtime
    # configuration.  Publish target-zero as a tombstone and let the config
    # owner drain it when it drops the inflight latch.
    with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
        entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_id)
        if entry is not None and entry.config_inflight and claim_id in entry.physical_claims:
            entry.physical_claims[claim_id] = 0
            _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.notify_all()
            # Safe-point processors retire a prepared generation only when its
            # callback returns. Raise after publishing the target-zero tombstone
            # so the exact finalizer authority remains PUBLISHED until the claim
            # has really disappeared. Safe-point execution receives the
            # separately rooted authority, not the wrapper object; only an armed
            # escrow authority must signal retry.
            if capsule._authority.is_armed_for(capsule.ticket):
                raise _ExternalRuntimeCleanupDeferred(
                    "external runtime physical claim cleanup awaits configuration"
                )
            return
    _resize_shared_external_native_thread_claim(runtime_id, claim_id, 0, missing_ok=True)


class _SharedExternalRuntimeNativePermit:
    """Exact per-operation physical claim with prearmed detached cleanup."""

    __slots__ = (
        "_runtime_id",
        "_claim_id",
        "_pid",
        "_released",
        "_finalizer_ticket",
        "_finalizer_capsule",
    )

    def __init__(self, runtime_id: _ExternalRuntimeKey, claim_id: int = 0) -> None:
        """Initialize the shared external runtime native permit and its owned runtime state."""
        self._runtime_id = runtime_id
        self._claim_id = int(claim_id)
        self._pid = os.getpid()
        self._released = False
        capsule = reserve_finalizer_cleanup(_cleanup_shared_external_physical_claim_capsule)
        capsule.arg0 = runtime_id
        capsule.arg1 = int(claim_id)
        self._finalizer_ticket = capsule.ticket
        self._finalizer_capsule: PreparedFinalizerCleanup | None = capsule

    def _bind_claim_id(self, claim_id: int) -> None:
        """Bind the authoritative external-runtime claim identifier."""
        claim_id = int(claim_id)
        if claim_id <= 0 or self._claim_id not in (0, claim_id):
            raise RuntimeError("external runtime physical claim binding mismatch")
        self._claim_id = claim_id
        capsule = self._finalizer_capsule
        if capsule is not None:
            capsule.arg1 = claim_id

    @property
    def amount(self) -> int:
        """Return the exact capacity retained by this shared external runtime native permit."""
        if self._released:
            return 0
        if os.getpid() != self._pid:
            ensure_runtime_fork_safe()
            raise RuntimeError(
                "external runtime physical-pool claim belongs to a different process"
            )
        with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
            entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(self._runtime_id)
            if entry is None:
                return 0
            return max(0, int(entry.physical_claims.get(self._claim_id, 0)))

    def _ack_finalizer(self) -> None:
        """Acknowledge and retire this lease's finalizer authority."""
        capsule = self._finalizer_capsule
        if self._finalizer_ticket and capsule is not None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def _abort_unpublished(self) -> None:
        """Retire a construction-only owner without reentering coordinator locks."""
        self._released = True
        self._ack_finalizer()

    def resize_physical_thread_permits(self, target: int) -> None:
        """Resize the physical-thread permits owned by this runtime lease."""
        wanted = max(0, int(target))
        current = self.amount
        if wanted > current:
            raise RuntimeError("external runtime physical-pool claim cannot grow")
        # Target-zero must always touch the exact claim namespace, even when a
        # previous interrupted cleanup already removed the dict mirror and
        # ``amount`` therefore observes zero. Otherwise the owner-aware
        # generation slot could remain rooted forever.
        if wanted == 0:
            try:
                _resize_shared_external_native_thread_claim(
                    self._runtime_id, self._claim_id, 0, missing_ok=True
                )
            except _ExternalRuntimeCleanupDeferred:
                capsule = self._finalizer_capsule
                if (
                    not self._finalizer_ticket
                    or capsule is None
                    or not defer_prepared_finalizer_cleanup(capsule)
                ):
                    raise
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
                self._released = True
                return
            self._released = True
            self._ack_finalizer()
            return
        if wanted < current:
            _resize_shared_external_native_thread_claim(
                self._runtime_id, self._claim_id, wanted, missing_ok=False
            )

    def release(self) -> None:
        """Release resources owned by this shared external runtime native permit."""
        self.resize_physical_thread_permits(0)

    close = release

    def __del__(self) -> None:
        """Schedule best-effort cleanup during garbage collection."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            capsule = getattr(self, "_finalizer_capsule", None)
            ticket = getattr(self, "_finalizer_ticket", 0)
            if ticket and capsule is not None and defer_prepared_finalizer_cleanup(capsule):
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
        except BaseException:
            pass


def _sync_external_native_lease_amount_locked(entry: _ExternalRuntimePoolCoordinatorEntry) -> None:
    """Refresh mirrored capacity from the exact native owner after interruptions."""
    native = entry.native
    lease = entry.native_lease
    if native is not None and lease is not None:
        entry.physical_amount = native.exact_permit_lease_amount(lease)


def _resize_shared_external_native_thread_claim(
    runtime_id: _ExternalRuntimeKey,
    claim_id: int,
    target: int,
    *,
    missing_ok: bool = False,
) -> None:
    """Set one physical claim to an absolute target and reconcile its envelope.

    Absolute targets make finalizer/retry cleanup idempotent: a detached claim
    can always request target zero without trusting a mirrored amount.
    """
    global _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS
    ensure_runtime_fork_safe()
    wanted = max(0, int(target))
    with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
        entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_id)
        if entry is None:
            if missing_ok and wanted == 0:
                _release_external_runtime_claim_slot_locked(claim_id)
                return
            raise RuntimeError("unknown external runtime physical-pool claim")
        wait_deadline = monotonic() + _RESOURCE_CLOSE_WAIT_TIMEOUT_SECONDS
        while entry.config_inflight:
            if wanted == 0 and claim_id in entry.physical_claims:
                # Target-zero is an idempotent tombstone. Never wait behind
                # arbitrary third-party configuration or deadlock on reentrancy.
                entry.physical_claims[claim_id] = 0
                _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.notify_all()
                raise _ExternalRuntimeCleanupDeferred(
                    "external runtime physical claim cleanup awaits configuration"
                )
            if entry.config_owner_thread_id == threading.get_ident():
                raise SchemaSanitizerResourceError(
                    "external runtime physical claim resize reentered configuration",
                    detail={
                        "stage": "external_runtime_threads",
                        "reason": "reentrant_configuration",
                    },
                )
            remaining = wait_deadline - monotonic()
            if remaining <= 0:
                raise SchemaSanitizerResourceError(
                    "external runtime physical claim resize exceeded configuration deadline",
                    detail={"stage": "external_runtime_threads", "reason": "configuration_timeout"},
                )
            _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.wait(timeout=min(0.05, remaining))
            check_operation_cancelled(stage="external_runtime_threads")
            entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_id)
            if entry is None:
                if missing_ok and wanted == 0:
                    _release_external_runtime_claim_slot_locked(claim_id)
                    return
                raise RuntimeError("external runtime pool disappeared during configuration")
        _sync_external_native_lease_amount_locked(entry)
        current = entry.physical_claims.get(claim_id)
        if current is None:
            if missing_ok and wanted == 0:
                _release_external_runtime_claim_slot_locked(claim_id)
                _retire_external_runtime_entry_locked(runtime_id, entry)
                return
            raise RuntimeError("unknown external runtime physical-pool claim")
        current = max(0, int(current))
        if wanted > current:
            raise RuntimeError("external runtime physical-pool claim cannot grow")
        new_max = (
            max(
                (wanted if other_id == claim_id else max(0, int(width)))
                for other_id, width in entry.physical_claims.items()
            )
            if entry.physical_claims
            else 0
        )
        if new_max < entry.physical_amount:
            native = entry.native
            if native is None:
                raise RuntimeError("external runtime physical pool lost native authority")
            if entry.native_lease is None:
                raise RuntimeError("external runtime physical pool lost exact lease authority")
            entry.physical_amount = native.resize_exact_permit_lease(entry.native_lease, new_max)
            if entry.physical_amount == 0:
                entry.native_lease = None
                entry.native = None
                entry.configured_width = None
        if wanted > 0:
            entry.physical_claims[claim_id] = wanted
        else:
            del entry.physical_claims[claim_id]
            _note_external_runtime_claim_removed_locked(logical=False)
            _release_external_runtime_claim_slot_locked(claim_id)
        if not entry.physical_claims and entry.physical_amount != 0:
            raise RuntimeError("external runtime pool retained unclaimed physical permits")
        _retire_external_runtime_entry_locked(runtime_id, entry)


def _reported_external_runtime_resident_width(runtime: Any) -> int | None:
    """Return resident workers only from an explicit identity-bearing probe.

    ``cpu_count()`` and ``thread_pool_size()`` describe configured capacity for
    common runtimes; neither proves that those OS threads currently exist.
    Treating either as resident identity would misattribute capacity. A runtime
    integration may opt in by
    exposing ``schema_sanitizer_resident_thread_count()`` when it can prove an
    actual resident-worker count.
    """
    integration = _external_runtime_integration(runtime)
    # Polars names every Rayon CPU worker ``polars-N`` on Linux.  Counting
    # those kernel task names is identity-bearing evidence (unlike
    # ``thread_pool_size()``, which is only configured capacity) and prevents
    # the same already-running workers from being charged again as future
    # physical threads.  Failure to read any task remains UNKNOWN/fail-closed.
    if integration is _EXTERNAL_RUNTIME_INTEGRATIONS.get("polars"):
        try:
            resident = 0
            observed = False
            with os.scandir("/proc/self/task") as tasks:
                for task in tasks:
                    if not task.name[:1].isdigit():
                        continue
                    try:
                        with open(
                            os.path.join(task.path, "comm"),
                            "rt",
                            encoding="ascii",
                        ) as stream:
                            name = stream.read(32).strip()
                    except (OSError, ValueError):
                        continue
                    observed = True
                    prefix, separator, suffix = name.partition("-")
                    if prefix == "polars" and separator and suffix.isdigit():
                        resident += 1
            if observed and resident <= _MAX_EXTERNAL_RUNTIME_REPORTED_RESIDENT_THREADS:
                return resident
        except (OSError, ValueError):
            pass

    probe_name = integration.resident_probe if integration is not None else None
    getter = getattr(runtime, probe_name, None) if probe_name else None
    if not callable(getter):
        # Custom integrations remain supported, but production identities for
        # PyArrow/Polars come from the sealed registry above.
        getter = getattr(runtime, "schema_sanitizer_resident_thread_count", None)
    if not callable(getter):
        return None
    try:
        width = int(getter())
    except BaseException:
        return None
    if width < 0 or width > _MAX_EXTERNAL_RUNTIME_REPORTED_RESIDENT_THREADS:
        return None
    # Zero is an authoritative observation: it must retire prior resident
    # attribution instead of being conflated with an unavailable probe.
    return width


def _reported_external_runtime_stack_debt_width(
    runtime: Any, resident_width: int | None
) -> int | None:
    """Return a conservative memory-debt width independent of CPU identity.

    For sealed process-global integrations, configured pool width is not proof
    of live OS identity, but it *is* a conservative upper bound for stack memory
    the runtime may retain between operations. Custom runtimes still require an
    explicit resident probe.
    """
    if resident_width is not None:
        return resident_width
    integration = _external_runtime_integration(runtime)
    getter_name = integration.width_getter if integration is not None else None
    getter = getattr(runtime, getter_name, None) if getter_name else None
    if not callable(getter):
        return None
    try:
        width = int(getter())
    except BaseException:
        return None
    if width <= 0 or width > _MAX_EXTERNAL_RUNTIME_REPORTED_RESIDENT_THREADS:
        return None
    return width


_RESIDENT_STACK_DEBT_UNSET = object()


def _refresh_external_runtime_residency_stable(
    runtime: Any, native: Any, runtime_key: tuple[str, object]
) -> None:
    """Apply only a generation-stable residency observation.

    ``None`` means the identity probe is unavailable or failed and is *not* an
    authoritative zero. In that case prior resident identity is retained and
    stack debt may only stay flat or grow. Continuous configuration churn is
    bounded; admission fails closed rather than spinning forever.
    """
    owner_thread = threading.get_ident()
    attempts = 0
    while attempts < _MAX_EXTERNAL_RUNTIME_STABLE_PROBE_RETRIES:
        attempts += 1
        with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
            entry = _external_runtime_entry_locked(runtime, create=True, runtime_key=runtime_key)
            assert entry is not None
            while entry.config_inflight:
                if entry.config_owner_thread_id == owner_thread:
                    raise SchemaSanitizerResourceError(
                        "external runtime residency probe reentered worker-pool configuration",
                        detail={
                            "stage": "external_runtime_threads",
                            "reason": "reentrant_configuration",
                        },
                    )
                _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.wait(timeout=0.05)
                check_operation_cancelled(stage="external_runtime_threads")
                entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_key)
                if entry is None:
                    entry = _external_runtime_entry_locked(
                        runtime, create=True, runtime_key=runtime_key
                    )
                    assert entry is not None
            generation = entry.config_generation

        reported_resident = _reported_external_runtime_resident_width(runtime)
        reported_stack_debt = _reported_external_runtime_stack_debt_width(
            runtime, reported_resident
        )

        with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
            entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_key)
            if entry is None:
                entry = _external_runtime_entry_locked(
                    runtime, create=True, runtime_key=runtime_key
                )
                assert entry is not None
            if entry.config_inflight or entry.config_generation != generation:
                check_operation_cancelled(stage="external_runtime_threads")
                continue

            if reported_resident is None:
                # Probe failure/unavailability is conservative uncertainty, not
                # evidence that resident workers vanished. Preserve CPU identity
                # and never shrink stack debt from an unavailable sample.
                resident_target = entry.resident_width
                if reported_stack_debt is None:
                    debt_target: int | None = None
                else:
                    debt_target = max(
                        entry.resident_stack_debt,
                        resident_target,
                        int(reported_stack_debt),
                    )
            else:
                resident_target = reported_resident
                debt_target = reported_stack_debt

            _set_external_runtime_resident_width_locked(
                entry, native, resident_target, stack_debt_target=debt_target
            )
            _sync_external_native_lease_amount_locked(entry)
            if reported_resident == 0:
                _retire_external_runtime_entry_locked(runtime_key, entry)
            return

    raise SchemaSanitizerResourceError(
        "external runtime residency could not obtain a stable configuration generation",
        detail={
            "stage": "external_runtime_threads",
            "reason": "unstable_configuration_generation",
            "attempts": _MAX_EXTERNAL_RUNTIME_STABLE_PROBE_RETRIES,
        },
    )


def _set_external_runtime_resident_width_locked(
    entry: _ExternalRuntimePoolCoordinatorEntry,
    native: Any,
    identity_width: int,
    *,
    stack_debt_target: int | None | object = _RESIDENT_STACK_DEBT_UNSET,
) -> None:
    """Update CPU identity and stack debt while preserving debt >= identity.

    Growth publishes memory debt first, then CPU identity. Shrink retracts CPU
    identity first, then memory debt. Any intermediate state is conservative.
    Unknown debt retains the last known value rather than failing open.
    """
    identity_target = max(0, int(identity_width))
    current_identity = max(0, int(entry.resident_width))
    current_debt = max(0, int(entry.resident_stack_debt))
    if stack_debt_target is _RESIDENT_STACK_DEBT_UNSET:
        desired_debt = identity_target
    elif stack_debt_target is None:
        desired_debt = current_debt
    elif isinstance(stack_debt_target, int):
        desired_debt = max(0, int(stack_debt_target))
    else:
        raise TypeError("external runtime stack-debt target must be an integer or None")
    if desired_debt < identity_target:
        desired_debt = identity_target

    supports_debt = getattr(native, "supports_stack_debt", False)
    supports_identity = getattr(native, "supports_resident_attribution", False)
    identity_delta = identity_target - current_identity
    debt_delta = desired_debt - current_debt
    if getattr(native, "supports_atomic_residency_update", False):
        native.external_runtime_residency_update(identity_delta, debt_delta)
        entry.resident_width = identity_target
        entry.resident_stack_debt = desired_debt
        entry.resident_native = native if (identity_target or desired_debt) else None
        return

    # Growth applies debt before identity; shrink reverses that order.
    if supports_debt and desired_debt > current_debt:
        native.external_runtime_stack_debt_threads_add(desired_debt - current_debt)
        entry.resident_stack_debt = desired_debt
        current_debt = desired_debt
    if supports_identity and identity_target > current_identity:
        native.external_runtime_resident_threads_add(identity_target - current_identity)
        entry.resident_width = identity_target
        current_identity = identity_target

    # Shrink: identity first, debt second.
    if supports_identity and identity_target < current_identity:
        resident_native = entry.resident_native or native
        resident_native.external_runtime_resident_threads_release(
            current_identity - identity_target
        )
        entry.resident_width = identity_target
        current_identity = identity_target
    if supports_debt and desired_debt < current_debt:
        debt_native = entry.resident_native or native
        debt_native.external_runtime_stack_debt_threads_release(current_debt - desired_debt)
        entry.resident_stack_debt = desired_debt
        current_debt = desired_debt

    entry.resident_native = native if (current_identity or current_debt) else None


def _acquire_shared_external_native_thread_permits(
    runtime: Any,
    amount: int,
    *,
    minimum: int | None = None,
    overlap_minimum: int | None = None,
) -> _ExternalNativePermitAcquisition:
    """Acquire one physical claim with a preallocated single-owner receipt."""
    global _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS
    result = _ExternalNativePermitAcquisition()
    requested = max(0, int(amount))
    if requested == 0:
        return result
    minimum_width = requested if minimum is None else max(1, min(requested, int(minimum)))
    overlap_minimum_width = (
        minimum_width if overlap_minimum is None else max(1, min(requested, int(overlap_minimum)))
    )
    native = _native_external_thread_api()
    result.owner = native
    if native is None:
        return result
    ensure_runtime_fork_safe()
    # Runtime integration callbacks execute outside project locks, but their
    # results are committed only if the configuration generation is unchanged.
    runtime_key = _external_runtime_pool_identity_key(runtime)
    _refresh_external_runtime_residency_stable(runtime, native, runtime_key)
    with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
        runtime_id = runtime_key
        entry = _external_runtime_entry_locked(runtime, create=True, runtime_key=runtime_key)
        assert entry is not None
        while entry.config_inflight:
            if entry.config_owner_thread_id == threading.get_ident():
                raise SchemaSanitizerResourceError(
                    "external runtime claim reentered worker-pool configuration",
                    detail={
                        "stage": "external_runtime_threads",
                        "reason": "reentrant_configuration",
                    },
                )
            _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.wait(timeout=0.05)
            check_operation_cancelled(stage="external_runtime_threads")
            entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_id)
            if entry is None:
                entry = _external_runtime_entry_locked(
                    runtime, create=True, runtime_key=runtime_key
                )
                assert entry is not None
        # Residency was already committed against a stable config generation.
        # The entry may have been retired if the observation was zero and idle;
        # the claim path above recreates it as needed.
        _sync_external_native_lease_amount_locked(entry)
        try:
            total_claims = _external_runtime_total_claims_locked()
        except BaseException:
            _retire_external_runtime_entry_locked(runtime_id, entry)
            raise
        if (
            len(entry.physical_claims) >= _MAX_EXTERNAL_RUNTIME_POOL_CLAIMS
            or total_claims >= _MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS
        ):
            _retire_external_runtime_entry_locked(runtime_id, entry)
            return result

        # Construct the cleanup owner before generation admission. If the token
        # return is interrupted before publication, release_for(owner)
        # can still locate and retire the exact slot without the integer token.
        permit = _SharedExternalRuntimeNativePermit(runtime_id, 0)
        claim_id: int | None = None
        try:
            claim_id = _EXTERNAL_RUNTIME_CLAIM_SLOTS.acquire_for(permit)
            if claim_id is None:
                permit.release()
                _retire_external_runtime_entry_locked(runtime_id, entry)
                return result
            permit._bind_claim_id(claim_id)
            next_claim_id = claim_id + 1
            entry.physical_claims[claim_id] = 0
            _note_external_runtime_claim_inserted_locked(logical=False)
        except BaseException:
            # No native acquisition has happened yet. Roll back every exact
            # publication while still under the coordinator lock, but never call
            # permit.release(): that path reacquires this same non-reentrant lock.
            if claim_id is not None and entry.physical_claims.pop(claim_id, None) is not None:
                try:
                    _note_external_runtime_claim_removed_locked(logical=False)
                except BaseException:
                    pass
            try:
                _EXTERNAL_RUNTIME_CLAIM_SLOTS.release_for(permit)
            finally:
                try:
                    permit._abort_unpublished()
                except BaseException:
                    pass
            _retire_external_runtime_entry_locked(runtime_id, entry)
            raise
        committed = False
        try:
            if len(entry.physical_claims) > 1:
                granted_width = min(requested, entry.physical_amount)
                if granted_width < overlap_minimum_width:
                    return result
            else:
                exact = native.acquire_exact_permit_lease(requested, minimum_width)
                if exact is None:
                    return result
                native_lease, granted_width = exact
                if granted_width < minimum_width or granted_width > requested:
                    native.resize_exact_permit_lease(native_lease, 0)
                    return result
                # Publish the capsule before mirrored scalar state. On an
                # asynchronous unwind the local/capsule destructor returns
                # permits even if the claim never commits.
                entry.native_lease = native_lease
                entry.native = native
                entry.physical_amount = granted_width
                entry.configured_width = None
            entry.physical_claims[claim_id] = granted_width
            entry.next_physical_claim = next_claim_id
            committed = True
            result.owner = permit
            result.amount = granted_width
            try:
                from .concurrency_contracts import observe_runtime_concurrency_contract_noexcept

                observe_runtime_concurrency_contract_noexcept("external_runtime_pool_claim")
            except BaseException:
                pass
            return result
        finally:
            if not committed:
                if entry.physical_claims.pop(claim_id, None) is not None:
                    _note_external_runtime_claim_removed_locked(logical=False)
                    _release_external_runtime_claim_slot_locked(claim_id)
                # If this was the first/only claim and native acquisition had
                # already committed, retire the exact envelope before forgetting
                # its mirrored metadata. This closes the partial-publish
                # state (native_lease + physical_amount with no logical claim).
                has_live_claim = any(
                    max(0, int(value)) > 0 for value in entry.physical_claims.values()
                )
                if not has_live_claim and entry.native_lease is not None:
                    rollback_native = entry.native or native
                    entry.physical_amount = rollback_native.resize_exact_permit_lease(
                        entry.native_lease, 0
                    )
                    if entry.physical_amount == 0:
                        entry.native_lease = None
                        entry.native = None
                        entry.configured_width = None
                _retire_external_runtime_entry_locked(runtime_id, entry)


def _sync_external_logical_lease_width_locked(
    entry: _ExternalRuntimePoolCoordinatorEntry,
    *,
    releasing_claim_id: int | None = None,
) -> None:
    """Rebuild the logical-width mirror from its exact governor owner.

    A target-zero operation may be interrupted after the underlying governor
    lease commits ``release()`` but before the coordinator clears its claim.
    When the only positive mirror is precisely the claim being retried, treat
    the released owner as proof that target zero already committed instead of
    misclassifying the recoverable split publication as corruption.
    """
    lease = entry.logical_lease
    if lease is None:
        if not entry.logical_claims:
            entry.logical_width = 0
        return
    if bool(getattr(lease, "_released", False)):
        positive_claims = {
            int(claim_id)
            for claim_id, width in entry.logical_claims.items()
            if max(0, int(width)) > 0
        }
        recoverable_release = releasing_claim_id is not None and positive_claims.issubset(
            {int(releasing_claim_id)}
        )
        if positive_claims and not recoverable_release:
            raise RuntimeError("external runtime logical pool references a released governor lease")
        entry.logical_lease = None
        entry.logical_width = 0
        return
    entry.logical_width = max(0, int(getattr(lease, "amount", entry.logical_width)))


def _cleanup_shared_external_logical_claim_capsule(
    capsule: PreparedFinalizerCleanup,
) -> None:
    """Cleanup shared external logical claim capsule."""
    runtime_id = capsule.arg0
    claim_id = capsule.arg1
    if not _is_external_runtime_key(runtime_id) or type(claim_id) is not int or claim_id <= 0:
        return
    with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
        entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_id)
        if entry is not None and entry.config_inflight and claim_id in entry.logical_claims:
            entry.logical_claims[claim_id] = 0
            _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.notify_all()
            if capsule._authority.is_armed_for(capsule.ticket):
                raise _ExternalRuntimeCleanupDeferred(
                    "external runtime logical claim cleanup awaits configuration"
                )
            return
    _resize_shared_external_logical_thread_claim(runtime_id, claim_id, 0, missing_ok=True)


class _SharedExternalRuntimeLogicalLease:
    """Exact logical claim with a prearmed detached cleanup receipt."""

    __slots__ = (
        "_runtime_id",
        "_claim_id",
        "_amount",
        "_pid",
        "_released",
        "_finalizer_ticket",
        "_finalizer_capsule",
    )

    def __init__(self, runtime_id: _ExternalRuntimeKey, claim_id: int = 0, amount: int = 0) -> None:
        """Initialize the shared external runtime logical lease and its owned runtime state."""
        self._runtime_id = runtime_id
        self._claim_id = int(claim_id)
        self._amount = max(0, int(amount))
        self._pid = os.getpid()
        self._released = self._amount == 0
        capsule = reserve_finalizer_cleanup(_cleanup_shared_external_logical_claim_capsule)
        capsule.arg0 = runtime_id
        capsule.arg1 = int(claim_id)
        self._finalizer_ticket = capsule.ticket
        self._finalizer_capsule: PreparedFinalizerCleanup | None = capsule

    def _bind_claim_id(self, claim_id: int) -> None:
        """Bind the authoritative external-runtime claim identifier."""
        claim_id = int(claim_id)
        if claim_id <= 0 or self._claim_id not in (0, claim_id):
            raise RuntimeError("external runtime logical claim binding mismatch")
        self._claim_id = claim_id
        capsule = self._finalizer_capsule
        if capsule is not None:
            capsule.arg1 = claim_id

    @property
    def amount(self) -> int:
        """Return the exact capacity retained by this shared external runtime logical lease."""
        if self._released:
            return 0
        if os.getpid() != self._pid:
            ensure_runtime_fork_safe()
            raise RuntimeError(
                "shared external runtime logical lease belongs to a different process"
            )
        with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
            entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(self._runtime_id)
            if entry is None:
                return 0
            return max(0, int(entry.logical_claims.get(self._claim_id, 0)))

    def _ack_finalizer(self) -> None:
        """Acknowledge and retire this lease's finalizer authority."""
        capsule = self._finalizer_capsule
        if self._finalizer_ticket and capsule is not None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def _abort_unpublished(self) -> None:
        """Retire a construction-only owner without reentering coordinator locks."""
        self._amount = 0
        self._released = True
        self._ack_finalizer()

    def shrink(self, amount: int) -> None:
        """Shrink this logical runtime lease to the requested capacity."""
        target = int(amount)
        if target <= 0:
            raise ValueError("shared external runtime logical lease shrink target must be > 0")
        current = self.amount
        if target > current:
            raise ValueError("cannot grow shared external runtime logical lease via shrink")
        if self._released:
            raise RuntimeError("cannot shrink a released shared external runtime logical lease")
        _resize_shared_external_logical_thread_claim(self._runtime_id, self._claim_id, target)
        self._amount = target

    def release(self) -> None:
        """Release resources owned by this shared external runtime logical lease."""
        if os.getpid() != self._pid:
            return
        if not self._released:
            try:
                _resize_shared_external_logical_thread_claim(
                    self._runtime_id, self._claim_id, 0, missing_ok=True
                )
            except _ExternalRuntimeCleanupDeferred:
                capsule = self._finalizer_capsule
                if (
                    not self._finalizer_ticket
                    or capsule is None
                    or not defer_prepared_finalizer_cleanup(capsule)
                ):
                    raise
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
                self._amount = 0
                self._released = True
                return
            self._amount = 0
            self._released = True
        # Retry acknowledgement independently from the exact target-zero commit.
        self._ack_finalizer()

    close = release

    def __del__(self) -> None:
        """Schedule best-effort cleanup during garbage collection."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            capsule = getattr(self, "_finalizer_capsule", None)
            ticket = getattr(self, "_finalizer_ticket", 0)
            if ticket and capsule is not None and defer_prepared_finalizer_cleanup(capsule):
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
        except BaseException:
            pass


def _resize_shared_external_logical_thread_claim(
    runtime_id: _ExternalRuntimeKey, claim_id: int, target: int, *, missing_ok: bool = False
) -> None:
    """Resize a standalone claim and its single shared governor envelope."""
    global _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS
    ensure_runtime_fork_safe()
    with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
        entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_id)
        if entry is None:
            if missing_ok and target == 0:
                _release_external_runtime_claim_slot_locked(claim_id)
                return
            raise RuntimeError("unknown external runtime logical-pool claim")
        wait_deadline = monotonic() + _RESOURCE_CLOSE_WAIT_TIMEOUT_SECONDS
        while entry.config_inflight:
            if target == 0 and claim_id in entry.logical_claims:
                entry.logical_claims[claim_id] = 0
                _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.notify_all()
                raise _ExternalRuntimeCleanupDeferred(
                    "external runtime logical claim cleanup awaits configuration"
                )
            if entry.config_owner_thread_id == threading.get_ident():
                raise SchemaSanitizerResourceError(
                    "external runtime logical claim resize reentered configuration",
                    detail={
                        "stage": "external_runtime_threads",
                        "reason": "reentrant_configuration",
                    },
                )
            remaining = wait_deadline - monotonic()
            if remaining <= 0:
                raise SchemaSanitizerResourceError(
                    "external runtime logical claim resize exceeded configuration deadline",
                    detail={"stage": "external_runtime_threads", "reason": "configuration_timeout"},
                )
            _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.wait(timeout=min(0.05, remaining))
            check_operation_cancelled(stage="external_runtime_threads")
            entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_id)
            if entry is None:
                if missing_ok and target == 0:
                    _release_external_runtime_claim_slot_locked(claim_id)
                    return
                raise RuntimeError("external runtime pool disappeared during configuration")
        _sync_external_logical_lease_width_locked(
            entry, releasing_claim_id=claim_id if target == 0 else None
        )
        current = entry.logical_claims.get(claim_id)
        if current is None:
            if missing_ok and target == 0:
                _release_external_runtime_claim_slot_locked(claim_id)
                _retire_external_runtime_entry_locked(runtime_id, entry)
                return
            raise RuntimeError("unknown external runtime logical-pool claim")
        if target < 0 or target > current:
            raise RuntimeError("invalid external runtime logical-pool claim resize")
        new_max = (
            max(
                (target if other_id == claim_id else width)
                for other_id, width in entry.logical_claims.items()
            )
            if entry.logical_claims
            else 0
        )
        if new_max < entry.logical_width:
            lease = entry.logical_lease
            if lease is None:
                raise RuntimeError("external runtime logical pool lost governor lease")
            if new_max == 0:
                lease.release()
                entry.logical_lease = None
            else:
                lease.shrink(new_max)
            entry.logical_width = new_max
        if target > 0:
            entry.logical_claims[claim_id] = target
        else:
            del entry.logical_claims[claim_id]
            _note_external_runtime_claim_removed_locked(logical=True)
            _release_external_runtime_claim_slot_locked(claim_id)
        if not entry.logical_claims and (
            entry.logical_width != 0 or entry.logical_lease is not None
        ):
            raise RuntimeError("external runtime pool retained unclaimed logical capacity")
        _retire_external_runtime_entry_locked(runtime_id, entry)


def _acquire_shared_external_logical_thread_lease(
    runtime: Any, amount: int, *, minimum: int | None = None
) -> _SharedExternalRuntimeLogicalLease:
    """Acquire a shared logical claim without blocking under the pool lock.

    The first generation publishes an in-flight slot under the coordinator lock,
    performs the potentially blocking project-thread admission outside that lock,
    then reconciles the physical envelope before committing. Followers wait on a
    Condition, which releases the coordinator lock so releases/shutdown can make
    progress.
    """
    global _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS
    requested = max(1, int(amount))
    minimum_width = requested if minimum is None else max(1, min(requested, int(minimum)))
    ensure_runtime_fork_safe()
    runtime_key = _external_runtime_pool_identity_key(runtime)
    runtime_id = runtime_key
    claim: _SharedExternalRuntimeLogicalLease | None = None
    claim_id: int | None = None

    with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
        entry = _external_runtime_entry_locked(runtime, create=True, runtime_key=runtime_key)
        assert entry is not None
        while entry.logical_acquire_inflight or entry.config_inflight:
            if entry.config_inflight and entry.config_owner_thread_id == threading.get_ident():
                raise SchemaSanitizerResourceError(
                    "external runtime logical claim reentered worker-pool configuration",
                    detail={
                        "stage": "external_runtime_threads",
                        "reason": "reentrant_configuration",
                    },
                )
            _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.wait(timeout=0.05)
            check_operation_cancelled(stage="external_runtime_threads")
            entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_id)
            if entry is None:
                entry = _external_runtime_entry_locked(
                    runtime, create=True, runtime_key=runtime_key
                )
                assert entry is not None

        try:
            total_claims = _external_runtime_total_claims_locked()
        except BaseException:
            _retire_external_runtime_entry_locked(runtime_id, entry)
            raise
        if (
            len(entry.logical_claims) >= _MAX_EXTERNAL_RUNTIME_POOL_CLAIMS
            or total_claims >= _MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS
        ):
            _retire_external_runtime_entry_locked(runtime_id, entry)
            raise SchemaSanitizerResourceError(
                "external runtime logical claim capacity exhausted",
                detail={
                    "stage": "external_runtime_threads",
                    "actual_items": requested,
                    "limit_items": _MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS,
                },
            )

        claim = _SharedExternalRuntimeLogicalLease(runtime_id, 0, 0)
        try:
            claim_id = _EXTERNAL_RUNTIME_CLAIM_SLOTS.acquire_for(claim)
            if claim_id is None:
                claim.release()
                _retire_external_runtime_entry_locked(runtime_id, entry)
                raise SchemaSanitizerResourceError(
                    "external runtime exact claim-slot capacity exhausted",
                    detail={
                        "stage": "external_runtime_threads",
                        "actual_items": requested,
                        "limit_items": _MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS,
                    },
                )
            claim._bind_claim_id(claim_id)
            claim._amount = requested
            claim._released = False
            next_claim_id = claim_id + 1
            entry.logical_claims[claim_id] = 0
            _note_external_runtime_claim_inserted_locked(logical=True)
        except BaseException:
            # The logical governor has not been touched yet. Avoid reentrant
            # claim.release() while holding the coordinator condition lock.
            if claim_id is not None and entry.logical_claims.pop(claim_id, None) is not None:
                try:
                    _note_external_runtime_claim_removed_locked(logical=True)
                except BaseException:
                    pass
            try:
                _EXTERNAL_RUNTIME_CLAIM_SLOTS.release_for(claim)
            finally:
                try:
                    claim._abort_unpublished()
                except BaseException:
                    pass
            _retire_external_runtime_entry_locked(runtime_id, entry)
            raise

        _sync_external_logical_lease_width_locked(entry)
        if entry.logical_lease is not None and entry.logical_width > 0:
            granted_width = min(requested, entry.logical_width)
            if granted_width < minimum_width:
                if entry.logical_claims.pop(claim_id, None) is not None:
                    _note_external_runtime_claim_removed_locked(logical=True)
                    _release_external_runtime_claim_slot_locked(claim_id)
                _retire_external_runtime_entry_locked(runtime_id, entry)
                raise SchemaSanitizerResourceError(
                    "external runtime logical pool cannot satisfy requested minimum",
                    detail={
                        "stage": "external_runtime_threads",
                        "actual_items": requested,
                        "minimum_items": minimum_width,
                    },
                )
            entry.logical_claims[claim_id] = granted_width
            entry.next_logical_claim = next_claim_id
            claim._amount = granted_width
            claim._released = False
            return claim

        admission_desired = requested
        if entry.physical_claims and entry.physical_amount > 0:
            admission_desired = min(admission_desired, entry.physical_amount)
        if admission_desired < minimum_width:
            if entry.logical_claims.pop(claim_id, None) is not None:
                _note_external_runtime_claim_removed_locked(logical=True)
                _release_external_runtime_claim_slot_locked(claim_id)
            _retire_external_runtime_entry_locked(runtime_id, entry)
            raise SchemaSanitizerResourceError(
                "external runtime physical generation is narrower than requested minimum",
                detail={
                    "stage": "external_runtime_threads",
                    "actual_items": admission_desired,
                    "minimum_items": minimum_width,
                },
            )
        entry.logical_acquire_inflight = True

    lease: _Lease | None = None
    try:
        lease = acquire_project_threads(admission_desired, minimum=minimum_width)
        while True:
            with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
                entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_id)
                if entry is None or claim_id not in entry.logical_claims:
                    raise RuntimeError("external runtime logical claim lost during admission")
                target = lease.amount
                if entry.physical_claims and entry.physical_amount > 0:
                    target = min(target, entry.physical_amount)
                if target < minimum_width:
                    break
                if target == lease.amount:
                    entry.logical_lease = lease
                    entry.logical_width = target
                    entry.logical_claims[claim_id] = target
                    entry.next_logical_claim = next_claim_id
                    entry.logical_acquire_inflight = False
                    claim._amount = target
                    claim._released = False
                    _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.notify_all()
                    return claim
            # Shrinking can touch the process governor; never do it under the
            # coordinator lock. Re-check the physical envelope after it commits.
            lease.shrink(target)

        # Physical capacity shrank below the contract while the logical acquire
        # was outside the coordinator lock. Release before publishing failure.
        lease.release()
        lease = None
        raise SchemaSanitizerResourceError(
            "external runtime physical generation shrank below requested minimum",
            detail={
                "stage": "external_runtime_threads",
                "actual_items": target,
                "minimum_items": minimum_width,
            },
        )
    except BaseException as primary:
        if lease is not None:
            try:
                lease.release()
            except BaseException as cleanup_error:
                # Deterministically transfer the governor capability instead of
                # relying on refcount timing after this stack frame disappears.
                capsule = getattr(lease, "_finalizer_capsule", None)
                if capsule is not None and defer_prepared_finalizer_cleanup(capsule):
                    object.__setattr__(lease, "_finalizer_ticket", 0)
                    object.__setattr__(lease, "_finalizer_capsule", None)
                add_bounded_note(
                    primary,
                    "external runtime logical-admission rollback also failed",
                    cleanup_error,
                )
        with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
            entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_id)
            if entry is not None:
                # If owner publication happened before the unwind, forget the
                # released governor lease together with its mirror. Followers
                # must never reuse a stale logical_width after rollback.
                if entry.logical_lease is lease:
                    entry.logical_lease = None
                    entry.logical_width = 0
                if entry.logical_claims.pop(claim_id, None) is not None:
                    _note_external_runtime_claim_removed_locked(logical=True)
                    _release_external_runtime_claim_slot_locked(claim_id)
                entry.logical_acquire_inflight = False
                _retire_external_runtime_entry_locked(runtime_id, entry)
            _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.notify_all()
        raise


def _drain_external_runtime_tombstones(runtime_id: _ExternalRuntimeKey, *, limit: int = 64) -> int:
    """Drain target-zero claims without allocating a pending-id container.

    Tombstone membership remains the exact authority until target-zero really
    commits. A failed drain leaves both the dict claim and any prepared finalizer
    generation intact for a later safe point.
    """
    progressed = 0
    for _ in range(max(0, min(int(limit), _MAX_EXTERNAL_RUNTIME_POOL_CLAIMS))):
        claim_id = 0
        logical = False
        with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
            entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_id)
            if entry is None or entry.config_inflight:
                return progressed
            for candidate, width in entry.physical_claims.items():
                if width == 0:
                    claim_id = candidate
                    break
            if claim_id == 0:
                for candidate, width in entry.logical_claims.items():
                    if width == 0:
                        claim_id = candidate
                        logical = True
                        break
        if claim_id == 0:
            return progressed
        try:
            if logical:
                _resize_shared_external_logical_thread_claim(
                    runtime_id, claim_id, 0, missing_ok=True
                )
            else:
                _resize_shared_external_native_thread_claim(
                    runtime_id, claim_id, 0, missing_ok=True
                )
        except BaseException as exc:
            clear_exception_traceback(exc)
            return progressed
        progressed += 1
    return progressed


def _external_runtime_pool_can_reexpand(runtime: Any, target: int) -> bool:
    """Return whether *runtime* is in a fresh, non-overlapping generation."""
    ensure_runtime_fork_safe()
    runtime_key = _external_runtime_pool_identity_key(runtime)
    with _EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        entry = _external_runtime_entry_locked(runtime, create=False, runtime_key=runtime_key)
        if entry is None or entry.configured_width is not None:
            return False
        # Physical claims are authoritative when the native ABI is present. If
        # unavailable, a single logical standalone claim is the best proof that
        # no older operation overlaps this fresh generation.
        if entry.physical_claims:
            return len(entry.physical_claims) == 1 and entry.physical_amount >= max(1, int(target))
        return len(entry.logical_claims) == 1 and entry.logical_width >= max(1, int(target))


def _external_runtime_pool_mark_configured(runtime: Any, width: int) -> None:
    """Record the worker width configured for an external runtime pool."""
    ensure_runtime_fork_safe()
    runtime_key = _external_runtime_pool_identity_key(runtime)
    with _EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        entry = _external_runtime_entry_locked(runtime, create=False, runtime_key=runtime_key)
        if entry is not None:
            entry.configured_width = max(1, int(width))
            # Configuration width is deliberately not resident-thread identity.
            # Resident attribution changes only through the explicit probe used
            # by _reported_external_runtime_resident_width().


def retire_external_runtime_pool(runtime: Any) -> bool:
    """Retire idle residency/debt after an integration proves pool destruction.

    This explicit lifecycle hook never infers retirement from age. Active claims
    or in-flight configuration keep the pool authoritative and make retirement
    fail closed.
    """
    ensure_runtime_fork_safe()
    runtime_key = _external_runtime_pool_identity_key(runtime)
    with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
        entry = _external_runtime_entry_locked(runtime, create=False, runtime_key=runtime_key)
        if entry is None:
            return True
        if (
            entry.physical_claims
            or entry.logical_claims
            or entry.physical_amount
            or entry.logical_width
            or entry.logical_lease is not None
            or entry.logical_acquire_inflight
            or entry.config_inflight
        ):
            return False
        native = entry.resident_native or _native_external_thread_api()
        if native is not None:
            _set_external_runtime_resident_width_locked(entry, native, 0, stack_debt_target=0)
        # Explicit destruction proof resolves any prior configuration uncertainty.
        entry.config_state = "stable"
        entry.config_attempted_width = None
        entry.configured_width = None
        _retire_external_runtime_entry_locked(runtime_key, entry)
        _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.notify_all()
        return runtime_key not in _EXTERNAL_RUNTIME_POOL_COORDINATOR


def external_runtime_physical_pool_snapshot() -> dict[str, int]:
    """Return bounded diagnostics for shared process-global runtime pools."""
    ensure_runtime_fork_safe()
    with _EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        _reconcile_external_runtime_claim_totals_locked()
        return {
            "pools": sum(
                1
                for entry in _EXTERNAL_RUNTIME_POOL_COORDINATOR.values()
                if entry.physical_claims or entry.physical_amount
            ),
            "claims": _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS,
            "physical_permits": sum(
                entry.physical_amount for entry in _EXTERNAL_RUNTIME_POOL_COORDINATOR.values()
            ),
            "resident_width": sum(
                entry.resident_width for entry in _EXTERNAL_RUNTIME_POOL_COORDINATOR.values()
            ),
            "resident_stack_debt": sum(
                entry.resident_stack_debt for entry in _EXTERNAL_RUNTIME_POOL_COORDINATOR.values()
            ),
            "claim_capacity": _MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS,
        }


def external_runtime_pool_snapshot() -> dict[str, int]:
    """Return coordinator-wide logical/native conservation diagnostics."""
    ensure_runtime_fork_safe()
    with _EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        _reconcile_external_runtime_claim_totals_locked()
        entries = tuple(_EXTERNAL_RUNTIME_POOL_COORDINATOR.values())
        return {
            "pools": sum(1 for entry in entries if entry.physical_claims or entry.physical_amount),
            "claims": _EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS,
            "physical_permits": sum(entry.physical_amount for entry in entries),
            "logical_pools": sum(
                1 for entry in entries if entry.logical_claims or entry.logical_width
            ),
            "logical_claims": _EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS,
            "logical_width": sum(entry.logical_width for entry in entries),
            "coordinator_entries": len(entries),
            "configured_pools": sum(1 for entry in entries if entry.configured_width is not None),
            "configuration_inflight": sum(1 for entry in entries if entry.config_inflight),
            "configuration_uncertain": sum(
                1 for entry in entries if entry.config_state == "uncertain"
            ),
            "resident_width": sum(entry.resident_width for entry in entries),
            "resident_stack_debt": sum(entry.resident_stack_debt for entry in entries),
            "resident_pools": sum(1 for entry in entries if entry.resident_width > 0),
            "coordinator_capacity": _MAX_EXTERNAL_RUNTIME_POOL_ENTRIES,
            "claim_capacity": _MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS,
        }


@dataclass(slots=True)
class _ExternalRuntimeCleanupState:
    """Named retry state for one external-runtime cleanup transaction.

    Keeping the resource graph in a typed state object avoids positional
    ``arg0..arg7`` coupling between mutation code and the finalizer callback.
    Each field is cleared only after its authoritative release commits.
    """

    native: Any | None = None
    borrow_lease: _OperationThreadBorrowLease | None = None
    parent_lease: _Lease | None = None
    lease: Any | None = None


def _external_runtime_cleanup_state(
    capsule: PreparedFinalizerCleanup,
) -> _ExternalRuntimeCleanupState:
    """Return the typed cleanup state retained by a finalizer capsule."""
    state = capsule.arg0
    if isinstance(state, _ExternalRuntimeCleanupState):
        return state
    state = _ExternalRuntimeCleanupState()
    capsule.arg0 = state
    return state


def _cleanup_external_runtime_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Release detached external-runtime authorities in retryable order."""
    state = capsule.arg0
    if not isinstance(state, _ExternalRuntimeCleanupState):
        return

    native = state.native
    if native is not None:
        native.resize_physical_thread_permits(0)
        if native.amount != 0:
            raise RuntimeError("external runtime native owner failed to retire")
        state.native = None

    borrow_lease = state.borrow_lease
    if borrow_lease is not None:
        borrow_lease.release()
        if borrow_lease.amount != 0:
            raise RuntimeError("external runtime exact borrow failed to retire")
        state.borrow_lease = None
        state.parent_lease = None

    lease = state.lease
    if lease is not None:
        lease.release()
        state.lease = None


class _ExternalRuntimeConstructionEscrow:
    """Prearmed construction transaction for external-runtime ownership.

    Every authoritative acquisition is mirrored into the prepared finalizer
    immediately.  If any later constructor step raises, publication of this
    escrow transfers exactly the still-owned suffix to safe-point cleanup.
    """

    __slots__ = ("prepared", "state", "active")

    def __init__(self, prepared: PreparedFinalizerCleanup) -> None:
        """Initialize the external runtime construction escrow and its owned runtime state."""
        self.prepared = prepared
        self.state = _external_runtime_cleanup_state(prepared)
        self.active = True

    def set_borrow(
        self,
        parent_lease: _Lease | None,
        borrow: _OperationThreadBorrowLease | None,
    ) -> None:
        """Retain the logical borrow lease in this construction escrow."""
        self.state.parent_lease = parent_lease
        self.state.borrow_lease = borrow

    def set_native(self, native: Any | None) -> None:
        """Retain the native permit receipt in this construction escrow."""
        self.state.native = native

    def set_lease(self, lease: Any | None) -> None:
        """Retain the process-thread lease in this construction escrow."""
        self.state.lease = lease

    def release_native_now(self) -> None:
        """Release the retained native permit immediately."""
        native = self.state.native
        if native is not None:
            native.resize_physical_thread_permits(0)
            if native.amount != 0:
                raise RuntimeError("external runtime native owner failed to retire")
            self.state.native = None

    def release_borrow_now(self) -> None:
        """Release the retained logical borrow immediately."""
        borrow_lease = self.state.borrow_lease
        if borrow_lease is not None:
            borrow_lease.release()
            if borrow_lease.amount != 0:
                raise RuntimeError("external runtime exact borrow failed to retire")
            self.state.borrow_lease = None
            self.state.parent_lease = None

    def release_lease_now(self) -> None:
        """Release the retained process-thread lease immediately."""
        lease = self.state.lease
        if lease is not None:
            lease.release()
            self.state.lease = None

    def release_all_now(self) -> None:
        # Match finalizer/close ordering. Each state field clears only after the
        # corresponding authoritative commit succeeds.
        """Release every resource retained during runtime construction."""
        self.release_native_now()
        self.release_borrow_now()
        self.release_lease_now()

    def transfer_to_wrapper(self) -> PreparedFinalizerCleanup:
        """Transfer construction resources to their published wrapper."""
        if not self.active:
            raise RuntimeError("external runtime construction escrow already retired")
        self.active = False
        return self.prepared

    def defer_after_failure(self, primary: BaseException) -> None:
        """Transfer failed construction resources to governed deferred cleanup."""
        if not self.active:
            return
        self.active = False
        state = self.state
        has_resources = bool(
            state.native is not None or state.borrow_lease is not None or state.lease is not None
        )
        capsule = self.prepared
        if not has_resources:
            cancel_prepared_finalizer_cleanup(capsule)
            return
        if not defer_prepared_finalizer_cleanup(capsule):
            add_bounded_note(
                primary,
                "external runtime construction cleanup could not be published",
                RuntimeError("prepared finalizer publication failed"),
            )


def _runtime_has_configurable_worker_pool(runtime: Any | None) -> bool:
    """Return whether a runtime exposes a configurable worker-pool interface."""
    if runtime is None:
        return False
    return callable(getattr(runtime, "cpu_count", None)) and callable(
        getattr(runtime, "set_cpu_count", None)
    )


def _runtime_requires_exact_worker_pool(runtime: Any | None) -> bool:
    """Return whether a runtime exposes a fixed observed pool that cannot shrink."""
    if runtime is None or _runtime_has_configurable_worker_pool(runtime):
        return False
    return callable(getattr(runtime, "thread_pool_size", None))


class ExternalRuntimeConcurrencyLease:
    """Worker envelope with atomic parent borrowing and prearmed GC cleanup."""

    __slots__ = (
        "_lease",
        "_parent_lease",
        "_borrow_lease",
        "_native",
        "_lock",
        "workers",
        "parallel",
        "_pid",
        "_finalizer_ticket",
        "_finalizer_capsule",
        "_cleanup_state",
        "__weakref__",
    )

    def __init__(
        self,
        lease: Any | None,
        *,
        workers: int,
        parallel: bool,
        parent_lease: _Lease | None = None,
        borrow_lease: _OperationThreadBorrowLease | None = None,
        native: Any | None = None,
        _prepared_finalizer: PreparedFinalizerCleanup | None = None,
    ) -> None:
        """Initialize the external runtime concurrency lease and its owned runtime state."""
        self._pid = os.getpid()
        prepared = _prepared_finalizer
        if prepared is None:
            prepared = reserve_finalizer_cleanup(_cleanup_external_runtime_capsule)
        self._finalizer_capsule: PreparedFinalizerCleanup | None = prepared
        self._finalizer_ticket = prepared.ticket
        self._cleanup_state: _ExternalRuntimeCleanupState | None = _external_runtime_cleanup_state(
            prepared
        )
        self._lease: Any | None = lease
        self._parent_lease: _Lease | None = parent_lease
        self._borrow_lease = borrow_lease
        self._native = native
        self._lock = Lock()
        self.workers = max(1, int(workers))
        self.parallel = bool(parallel and self.workers > 1)
        self._sync_finalizer_capsule_locked()

    def _sync_finalizer_capsule_locked(self) -> None:
        """Sync finalizer capsule while holding the governing lock."""
        capsule = self._finalizer_capsule
        state = self._cleanup_state
        if capsule is None or state is None:
            return
        state.native = self._native
        state.borrow_lease = self._borrow_lease
        state.parent_lease = self._parent_lease
        state.lease = self._lease

    def _retire_finalizer_locked(self) -> None:
        """Retire finalizer while holding the governing lock."""
        if any(
            (
                self._native is not None,
                self._borrow_lease is not None,
                self._lease is not None,
            )
        ):
            self._sync_finalizer_capsule_locked()
            return
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
        self._finalizer_ticket = 0
        self._finalizer_capsule = None
        self._cleanup_state = None
        self._parent_lease = None
        self._borrow_lease = None

    def shrink_to(self, workers: int) -> int:
        """Return excess logical and native capacity after discovering real pool width."""
        target = max(1, int(workers))
        if os.getpid() != self._pid:
            ensure_runtime_fork_safe()
            raise RuntimeError("external runtime lease belongs to a different process")
        with self._lock:
            current = self.workers
            if target >= current:
                return current
            # A serial runtime needs no external worker envelope at all.
            keep = target if target > 1 else 0

            native = self._native
            if native is not None:
                if native.amount > keep:
                    native.resize_physical_thread_permits(keep)
                if native.amount == 0:
                    self._native = None
                self._sync_finalizer_capsule_locked()

            if self._borrow_lease is not None:
                observed_borrowed = self._borrow_lease.amount
                if observed_borrowed > keep:
                    observed_borrowed = self._borrow_lease.shrink_to(keep)
                if observed_borrowed == 0:
                    self._borrow_lease = None
                    self._parent_lease = None
                self._sync_finalizer_capsule_locked()

            if self._lease is not None and self._lease.amount > keep:
                if keep == 0:
                    lease = self._lease
                    lease.release()
                    self._lease = None
                else:
                    self._lease.shrink(keep)
                self._sync_finalizer_capsule_locked()

            self.workers = target
            self.parallel = target > 1
            self._retire_finalizer_locked()
            return self.workers

    def close(self) -> None:
        """Release resources owned by this external runtime concurrency lease."""
        if os.getpid() != self._pid:
            return
        # Clear each component only after its exact release commits, keeping the
        # prepared capsule synchronized so GC can retry a partially-failed close.
        with self._lock:
            native = self._native
            if native is not None:
                native.resize_physical_thread_permits(0)
                if native.amount != 0:
                    raise RuntimeError("external runtime native owner failed to retire")
                self._native = None
                self._sync_finalizer_capsule_locked()

            if self._borrow_lease is not None:
                self._borrow_lease.release()
                if self._borrow_lease.amount != 0:
                    raise RuntimeError("external runtime exact borrow failed to retire")
                self._borrow_lease = None
                self._parent_lease = None
                self._sync_finalizer_capsule_locked()

            lease = self._lease
            if lease is not None:
                lease.release()
                self._lease = None
                self._sync_finalizer_capsule_locked()

            self._retire_finalizer_locked()

    def __enter__(self) -> "ExternalRuntimeConcurrencyLease":
        """Enter the managed resource context."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Release managed resources when leaving the context."""
        self.close()

    def __del__(self) -> None:
        """Schedule best-effort cleanup during garbage collection."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                if defer_prepared_finalizer_cleanup(capsule):
                    self._finalizer_ticket = 0
                    self._finalizer_capsule = None
                    self._cleanup_state = None
                    self._lease = None
                    self._parent_lease = None
                    self._borrow_lease = None
                    self._native = None
        except BaseException:
            pass


def acquire_external_runtime_threads(
    desired: int, *, allow_parallel: bool, runtime: Any | None = None
) -> ExternalRuntimeConcurrencyLease:
    """Acquire an external worker envelope with construction-time escrow.

    Configurable process-global runtimes may degrade to any safe width >= 2.
    Runtimes whose pool width cannot be changed remain exact-admission: their
    observed physical pool must fit completely or execution is serialized / the
    higher-level exact helper rejects it.
    """
    wanted = max(1, int(desired))
    escrow = _ExternalRuntimeConstructionEscrow(
        reserve_finalizer_cleanup(_cleanup_external_runtime_capsule)
    )
    try:
        if not allow_parallel or wanted <= 1:
            return ExternalRuntimeConcurrencyLease(
                None,
                workers=1,
                parallel=False,
                _prepared_finalizer=escrow.transfer_to_wrapper(),
            )

        execution_lease: object | None = None
        try:
            from .concurrency_contracts import current_runtime_execution_lease

            execution_lease = current_runtime_execution_lease()
        except BaseException:
            execution_lease = None

        configurable = _runtime_has_configurable_worker_pool(runtime)
        exact_pool = _runtime_requires_exact_worker_pool(runtime)
        minimum_parallel = wanted if exact_pool else 2

        if type(execution_lease) is _Lease and execution_lease._governor is _THREAD_GOVERNOR:
            borrow_result = execution_lease.borrow_external_runtime_threads(
                wanted,
                minimum=2,
                exact=exact_pool,
            )
            borrow_lease = borrow_result.lease
            if borrow_lease is None or borrow_lease.amount < 2:
                return ExternalRuntimeConcurrencyLease(
                    None,
                    workers=1,
                    parallel=False,
                    _prepared_finalizer=escrow.transfer_to_wrapper(),
                )
            borrowed = borrow_lease.amount
            escrow.set_borrow(execution_lease, borrow_lease)
            native_result = (
                _acquire_shared_external_native_thread_permits(
                    runtime,
                    borrowed,
                    minimum=(2 if configurable else borrowed),
                    overlap_minimum=(borrowed if exact_pool else 2),
                )
                if runtime is not None
                else _acquire_external_native_thread_permits(borrowed)
            )
            native = native_result.owner
            native_amount = native_result.amount
            if native is not None:
                escrow.set_native(native)
                if native_amount != borrowed:
                    if not exact_pool and 2 <= native_amount < borrowed:
                        borrowed = borrow_lease.shrink_to(native_amount)
                        escrow.set_borrow(execution_lease, borrow_lease)
                    else:
                        escrow.release_all_now()
                        return ExternalRuntimeConcurrencyLease(
                            None,
                            workers=1,
                            parallel=False,
                            _prepared_finalizer=escrow.transfer_to_wrapper(),
                        )
            return ExternalRuntimeConcurrencyLease(
                None,
                workers=borrowed,
                parallel=True,
                parent_lease=execution_lease,
                borrow_lease=borrow_lease,
                native=native,
                _prepared_finalizer=escrow.transfer_to_wrapper(),
            )

        try:
            lease = (
                _acquire_shared_external_logical_thread_lease(
                    runtime,
                    wanted,
                    minimum=minimum_parallel,
                )
                if runtime is not None
                else acquire_project_threads(wanted, minimum=wanted)
            )
        except SchemaSanitizerResourceError:
            return ExternalRuntimeConcurrencyLease(
                None,
                workers=1,
                parallel=False,
                _prepared_finalizer=escrow.transfer_to_wrapper(),
            )
        escrow.set_lease(lease)

        native_result = (
            _acquire_shared_external_native_thread_permits(
                runtime,
                lease.amount,
                minimum=(2 if configurable else lease.amount),
                overlap_minimum=(lease.amount if exact_pool else 2),
            )
            if runtime is not None
            else _acquire_external_native_thread_permits(lease.amount)
        )
        native = native_result.owner
        native_amount = native_result.amount
        if native is not None:
            escrow.set_native(native)
            if native_amount != lease.amount:
                if not exact_pool and 2 <= native_amount < lease.amount:
                    # The physical authority committed first. Shrink the logical
                    # claim second; if shrink throws, the escrow already owns both
                    # sides and finalization can return the exact residual state.
                    lease.shrink(native_amount)
                else:
                    escrow.release_all_now()
                    return ExternalRuntimeConcurrencyLease(
                        None,
                        workers=1,
                        parallel=False,
                        _prepared_finalizer=escrow.transfer_to_wrapper(),
                    )

        return ExternalRuntimeConcurrencyLease(
            lease,
            workers=lease.amount,
            parallel=True,
            native=native,
            _prepared_finalizer=escrow.transfer_to_wrapper(),
        )
    except BaseException as primary:
        escrow.defer_after_failure(primary)
        raise


_EXTERNAL_RUNTIME_CONFIG_LOCK = Lock()


def constrain_external_runtime_worker_pool(runtime: Any, workers: int) -> int:
    """Verify/configure a process-global pool without calling runtime code under locks.

    Configuration is a per-pool two-phase transaction. A bounded condition wait
    releases the coordinator lock for other threads; same-thread reentrancy fails
    closed instead of deadlocking on a non-reentrant global config mutex.
    """
    target = max(1, int(workers))
    integration = _external_runtime_integration(runtime)
    getter_name = integration.width_getter if integration is not None else "cpu_count"
    setter_name = integration.width_setter if integration is not None else "set_cpu_count"
    getter = getattr(runtime, getter_name, None) if getter_name else None
    setter = getattr(runtime, setter_name, None) if setter_name else None
    if not callable(getter) or not callable(setter):
        raise SchemaSanitizerResourceError(
            "external runtime worker pool width cannot be verified",
            detail={
                "stage": "external_runtime_threads",
                "limit_name": "external_runtime_worker_threads",
                "actual_items": target,
                "reason": "missing cpu_count/set_cpu_count API",
            },
        )

    def read_width() -> int:
        """Read the external runtime's current worker-pool width."""
        try:
            observed = int(getter())
        except BaseException as exc:
            raise SchemaSanitizerResourceError(
                "external runtime worker pool width could not be observed",
                detail={
                    "stage": "external_runtime_threads",
                    "limit_name": "external_runtime_worker_threads",
                    "actual_items": target,
                    "reason": type(exc).__name__,
                },
            ) from exc
        if observed <= 0:
            raise SchemaSanitizerResourceError(
                "external runtime reported an invalid worker pool width",
                detail={
                    "stage": "external_runtime_threads",
                    "limit_name": "external_runtime_worker_threads",
                    "actual_items": observed,
                },
            )
        return observed

    ensure_runtime_fork_safe()
    runtime_key = _external_runtime_pool_identity_key(runtime)
    owner_thread = threading.get_ident()
    generation = 0
    can_reexpand = False
    with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
        entry = _external_runtime_entry_locked(runtime, create=True, runtime_key=runtime_key)
        assert entry is not None
        while entry.config_inflight:
            if entry.config_owner_thread_id == owner_thread:
                raise SchemaSanitizerResourceError(
                    "external runtime worker-pool configuration is reentrant",
                    detail={
                        "stage": "external_runtime_threads",
                        "limit_name": "external_runtime_worker_threads",
                        "actual_items": target,
                        "reason": "reentrant_configuration",
                    },
                )
            _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.wait(timeout=0.05)
            check_operation_cancelled(stage="external_runtime_threads")
            entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_key)
            if entry is None:
                entry = _external_runtime_entry_locked(
                    runtime, create=True, runtime_key=runtime_key
                )
                assert entry is not None
        # Config generations are fixed-width authority. Never permit unbounded
        # Python-int growth or wrap/ABA semantics in the control plane.
        if entry.config_generation >= _MAX_EXTERNAL_RUNTIME_CONFIG_GENERATION:
            raise SchemaSanitizerResourceError(
                "external runtime configuration generation exhausted",
                detail={
                    "stage": "external_runtime_threads",
                    "limit_name": "external_runtime_config_generation",
                    "reason": "generation_exhausted",
                },
            )
        next_generation = entry.config_generation + 1
        entry.config_generation = next_generation
        generation = next_generation
        entry.config_owner_thread_id = owner_thread
        entry.config_state = "inflight"
        entry.config_attempted_width = target
        entry.config_inflight = True
        if entry.configured_width is None:
            if entry.physical_claims:
                can_reexpand = len(entry.physical_claims) == 1 and entry.physical_amount >= target
            else:
                can_reexpand = len(entry.logical_claims) == 1 and entry.logical_width >= target

    observed: int | None = None
    setter_committed = False
    attempted_width: int | None = None
    try:
        # Arbitrary third-party callbacks execute entirely outside project locks.
        current = read_width()
        if current < target and not can_reexpand:
            observed = current
        else:
            desired = target if current != target else current
            attempted_width = desired
            if desired != current:
                try:
                    setter(desired)
                    setter_committed = True
                except BaseException as exc:
                    raise SchemaSanitizerResourceError(
                        "external runtime worker pool could not be configured",
                        detail={
                            "stage": "external_runtime_threads",
                            "limit_name": "external_runtime_worker_threads",
                            "actual_items": current,
                            "requested_items": desired,
                            "reason": type(exc).__name__,
                        },
                    ) from exc
                observed = read_width()
            else:
                observed = current
            if observed > target:
                raise SchemaSanitizerResourceError(
                    "external runtime worker pool exceeds admitted physical width after configuration",
                    detail={
                        "stage": "external_runtime_threads",
                        "limit_name": "external_runtime_worker_threads",
                        "actual_items": observed,
                        "requested_items": target,
                    },
                )
        return observed
    finally:
        should_drain_tombstones = False
        with _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
            entry = _EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_key)
            if entry is not None and entry.config_generation == generation:
                if observed is not None:
                    entry.configured_width = observed
                    entry.config_state = "stable"
                    entry.config_attempted_width = None
                    # A sealed process-global integration's configured width is a
                    # conservative stack-debt floor even without identity proof.
                    if _external_runtime_integration(runtime) is not None:
                        native = entry.resident_native or _native_external_thread_api()
                        if native is not None:
                            _set_external_runtime_resident_width_locked(
                                entry,
                                native,
                                entry.resident_width,
                                stack_debt_target=observed,
                            )
                elif setter_committed:
                    # The runtime accepted a mutation but verification failed.
                    # Do not invent CPU identity; retain a conservative stack
                    # debt floor until a later probe settles the generation.
                    entry.configured_width = None
                    entry.config_state = "uncertain"
                    entry.config_attempted_width = attempted_width or target
                    if _external_runtime_integration(runtime) is not None:
                        native = entry.resident_native or _native_external_thread_api()
                        if native is not None:
                            debt_floor = max(
                                entry.resident_stack_debt,
                                int(entry.config_attempted_width or target),
                            )
                            _set_external_runtime_resident_width_locked(
                                entry,
                                native,
                                entry.resident_width,
                                stack_debt_target=debt_floor,
                            )
                else:
                    entry.config_state = "stable"
                    entry.config_attempted_width = None
                entry.config_inflight = False
                entry.config_owner_thread_id = None
                should_drain_tombstones = True
                # Wrapper-only runtimes must not be retained solely because a
                # configuration attempt occurred. Process-global integrations
                # remain rooted only when they carry explicit resident stack debt.
                _retire_external_runtime_entry_locked(runtime_key, entry)
                _EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.notify_all()
        # Do not allocate a tuple of pending ids after dropping the config latch.
        # Exact claim slots and tombstone membership survive any failed drain.
        if should_drain_tombstones:
            _drain_external_runtime_tombstones(runtime_key)


def acquire_teardown_project_threads(desired: int, *, minimum: int = 1) -> _Lease:
    """Acquire from the internal reserve for bounded terminal cleanup."""
    from .governed_thread import reap_governed_thread_retirements

    reap_governed_thread_retirements()
    _refresh_thread_governor_capacity()
    return _THREAD_GOVERNOR.try_acquire_up_to(desired, minimum=minimum, _teardown=True)


def acquire_release_guardian_thread() -> _Lease:
    """Reserve the dedicated emergency slot used only by the release guardian."""
    return _GUARDIAN_THREAD_GOVERNOR.try_acquire_up_to(1, minimum=1)


def is_project_thread_lease(owner: object) -> bool:
    """Return whether *owner* is an exact permit from the project thread governor."""
    return type(owner) is _Lease and owner._governor is _THREAD_GOVERNOR


def is_release_guardian_thread_lease(owner: object) -> bool:
    """Return whether *owner* is an exact permit from the guardian bootstrap pool."""
    return type(owner) is _Lease and owner._governor is _GUARDIAN_THREAD_GOVERNOR


def register_project_thread_availability(event: AvailabilityEvent) -> bool:
    """Register a privileged wakeup for newly available thread capacity."""
    return _THREAD_GOVERNOR.register_availability_event(event)


def unregister_project_thread_availability(event: AvailabilityEvent) -> None:
    """Remove a previously registered thread-capacity wakeup."""
    _THREAD_GOVERNOR.unregister_availability_event(event)


def _acquire_file_descriptor_lease(
    amount: int, *, timeout_seconds: float, teardown: bool
) -> _Lease:
    """Acquire file descriptor lease."""
    _refresh_fd_governor_capacity()
    lease = _FD_GOVERNOR.acquire(amount, timeout_seconds=timeout_seconds, _teardown=teardown)
    acquisition: _NativeFdPermitAcquisition | None = None
    try:
        acquisition = _acquire_native_file_descriptor_permits(
            amount, timeout_seconds=timeout_seconds
        )
        if acquisition.native is not None and acquisition.amount > 0:
            _attach_native_file_descriptor_permits(lease, acquisition)
        return lease
    except BaseException as primary:
        # Exact receipts remain self-owning until attached. Once attached,
        # lease.release() retires that same idempotent receipt.
        try:
            lease.release()
        except BaseException as cleanup_error:
            add_bounded_note(primary, "file descriptor bridge rollback failed", cleanup_error)
        raise


def acquire_file_descriptors(amount: int = 1, *, timeout_seconds: float = 30.0) -> _Lease:
    """Acquire Python + native process-wide file-descriptor capacity."""
    return _acquire_file_descriptor_lease(amount, timeout_seconds=timeout_seconds, teardown=False)


def acquire_teardown_file_descriptors(amount: int = 1, *, timeout_seconds: float = 30.0) -> _Lease:
    """Acquire a teardown descriptor from the same shared physical authority."""
    return _acquire_file_descriptor_lease(amount, timeout_seconds=timeout_seconds, teardown=True)


def _reconcile_uncertain_fd_close_count_locked() -> int:
    """Rebuild the diagnostic debt count from exact retained-owner slots."""
    global _UNCERTAIN_FD_CLOSE_COUNT
    _UNCERTAIN_FD_CLOSE_COUNT = sum(
        1 for slot in _UNCERTAIN_FD_CLOSE_DEBTS if slot.lease is not None
    )
    return _UNCERTAIN_FD_CLOSE_COUNT


def _republish_uncertain_fd_terminal_owner_locked(key: int) -> None:
    """Idempotently repair terminal-owner observability for an exact debt slot."""
    publish_terminal_owner(
        "uncertain_fd_close",
        key,
        retained_bytes=_UNCERTAIN_FD_TERMINAL_RETAINED_BYTES,
    )
    diagnostic_transition()


def retain_uncertain_fd_close(lease: object, *, label: str) -> bool:
    """Retain FD capacity in a physically preallocated terminal-debt slot."""
    global _UNCERTAIN_FD_CLOSE_REJECTED, _UNCERTAIN_FD_CLOSE_COUNT
    if type(lease) is not _Lease or lease._governor is not _FD_GOVERNOR:
        return False
    if type(label) is not str:
        label = "uncertain-fd-close"
    key = id(lease)
    with _UNCERTAIN_FD_CLOSE_LOCK:
        free: _UncertainFdCloseDebtSlot | None = None
        for slot in _UNCERTAIN_FD_CLOSE_DEBTS:
            if slot.lease is lease and slot.key == key:
                # Slot membership is authority. Repair metadata/mirrors that
                # may have lagged an asynchronous exception after publication.
                if slot.created_ns <= 0:
                    slot.created_ns = monotonic_ns()
                if not slot.label:
                    slot.label = label
                count = _reconcile_uncertain_fd_close_count_locked()
                _republish_uncertain_fd_terminal_owner_locked(key)
                return count > 0
            if free is None and slot.lease is None:
                free = slot
        if free is None:
            _UNCERTAIN_FD_CLOSE_REJECTED += 1
            raise RuntimeError("uncertain FD-close debt capacity exhausted")
        # Prepare observability before publishing exact slot membership. Once
        # ``lease`` is stored, every remaining scalar is repairable on retry.
        created_ns = monotonic_ns()
        free.key = key
        free.created_ns = created_ns
        free.label = label
        free.lease = lease
        count = _reconcile_uncertain_fd_close_count_locked()
        _republish_uncertain_fd_terminal_owner_locked(key)
        return count > 0


def uncertain_fd_close_snapshot() -> UncertainFdCloseSnapshot:
    """Return diagnostics without allocating an O(n) temporary collection."""
    with _UNCERTAIN_FD_CLOSE_LOCK:
        oldest = 0
        count = _reconcile_uncertain_fd_close_count_locked()
        for slot in _UNCERTAIN_FD_CLOSE_DEBTS:
            if slot.lease is not None and (oldest == 0 or slot.created_ns < oldest):
                oldest = slot.created_ns
        return UncertainFdCloseSnapshot(
            count, _FD_GOVERNOR.capacity, oldest, _UNCERTAIN_FD_CLOSE_REJECTED
        )


class _FdOpenAttempt:
    """Preallocated identity for one in-flight FD open transaction."""

    __slots__ = ("committed", "native_before")

    def __init__(self) -> None:
        """Initialize the fd open attempt and its owned runtime state."""
        self.committed = False
        self.native_before: int | None = None


class FileDescriptorCapability:
    """One divisible FD reservation with linearizable physical ownership.

    The capability has one explicit lifecycle.  ``release()`` first publishes
    RELEASING while holding the same lock used by physical-open admission; no
    descriptor can therefore appear after logical credit starts returning.
    Uncertain close is terminal and retains capacity fail-closed.
    """

    _ACTIVE = 1
    _RELEASING = 2
    _CLOSED = 3
    _TERMINAL_DEBT = 4

    __slots__ = (
        "_lease",
        "_amount",
        "_opened",
        "_opening_attempts",
        "_lock",
        "_label",
        "_state",
    )

    def __init__(self, lease: _Lease, amount: int, *, label: str) -> None:
        """Initialize the file descriptor capability and its owned runtime state."""
        self._lease: _Lease | None = lease
        self._amount = amount
        self._opened = 0
        # Exact attempt identity makes abort idempotent after asynchronous
        # exceptions between commit and local publication.
        self._opening_attempts: set[_FdOpenAttempt] = set()
        self._lock = Lock()
        self._label = label
        self._state = self._ACTIVE

    @property
    def amount(self) -> int:
        """Return the exact capacity retained by this file descriptor capability."""
        return self._amount

    @property
    def opened(self) -> int:
        """Return descriptors whose open transition has committed."""
        with self._lock:
            self._reconcile_opened_locked()
            return self._opened

    @property
    def opening(self) -> int:
        """Return descriptors reserved for an in-flight open transition."""
        with self._lock:
            return len(self._opening_attempts)

    @property
    def retained_as_debt(self) -> bool:
        """Return whether descriptor ownership remains as terminal cleanup debt."""
        with self._lock:
            return self._state == self._TERMINAL_DEBT

    def _ensure_active_locked(self) -> None:
        """Ensure active while holding the governing lock."""
        if self._state == self._TERMINAL_DEBT:
            raise RuntimeError(
                "file descriptor capability is terminally poisoned by uncertain close"
            )
        if self._state == self._RELEASING:
            raise RuntimeError("file descriptor capability is being released")
        if self._state == self._CLOSED or self._lease is None:
            raise RuntimeError("file descriptor capability is already released")

    def _reconcile_opened_locked(self) -> None:
        """Reconcile opened while holding the governing lock."""
        lease = self._lease
        if lease is None:
            return
        exact_opened = _opened_for_file_descriptor_lease(lease)
        if exact_opened is not None:
            # The receipt is authoritative. Local ``_opened`` is only a mirror
            # used for fast admission/diagnostics and may lag an interrupted C call.
            self._opened = max(0, int(exact_opened))

    def _begin_open(self, attempt: _FdOpenAttempt) -> None:
        """Reserve one exact in-flight physical creation before kernel entry."""
        with self._lock:
            self._ensure_active_locked()
            self._reconcile_opened_locked()
            if self._opened + len(self._opening_attempts) >= self._amount:
                raise RuntimeError("file descriptor capability exhausted")
            self._opening_attempts.add(attempt)

    def _abort_open(self, attempt: _FdOpenAttempt) -> None:
        """Idempotently retire one exact opening reservation."""
        with self._lock:
            self._opening_attempts.discard(attempt)

    def _commit_opened(self, attempt: _FdOpenAttempt) -> None:
        """Commit an in-flight descriptor-open transition."""
        with self._lock:
            if attempt not in self._opening_attempts:
                raise RuntimeError("file descriptor capability open committed without reservation")
            self._ensure_active_locked()
            lease = self._lease
            assert lease is not None
            before = _opened_for_file_descriptor_lease(lease)
            attempt.native_before = before
            try:
                # The exact receipt is authoritative.  If a signal lands after
                # the C commit, the except path re-reads receipt state before
                # retiring this exact opening attempt.
                committed_opened = _mark_file_descriptor_lease_opened(lease, 1)
            except BaseException:
                after = _opened_for_file_descriptor_lease(lease)
                if before is not None and after is not None and after > before:
                    attempt.committed = True
                    self._opened = max(0, int(after))
                self._opening_attempts.discard(attempt)
                raise
            attempt.committed = True
            exact = committed_opened
            if exact is None:
                exact = _opened_for_file_descriptor_lease(lease)
            if exact is not None:
                self._opened = max(0, int(exact))
            else:
                self._opened += 1
            self._opening_attempts.discard(attempt)

    def _mark_closed(self) -> None:
        """Mark the capability closed and report whether exact accounting committed."""
        with self._lock:
            if self._state == self._TERMINAL_DEBT:
                raise RuntimeError("terminal FD debt cannot be committed as a proven close")
            lease = self._lease
            if lease is None:
                raise RuntimeError("file descriptor capability is already released")
            self._reconcile_opened_locked()
            if self._opened <= 0:
                raise RuntimeError("file descriptor capability physical over-close")
            # Physical close has already succeeded. Retire it from the exact
            # receipt before updating the Python mirror; retry/release can query
            # receipt state if an asynchronous exception lands after this call.
            committed_opened = _mark_file_descriptor_lease_closed(lease, 1)
            if committed_opened is not None:
                self._opened = max(0, int(committed_opened))
            else:
                self._opened -= 1

    def _mark_closed_if_unmirrored_open(self) -> None:
        """Repair an open-accounting commit interrupted before local publication.

        This is used only after the kernel object has been proven closed.  Exact
        receipt state is compared with the local mirror; we retire one unit only
        when native authority proves an unmirrored open actually committed.
        """
        with self._lock:
            lease = self._lease
            if lease is None or self._state == self._TERMINAL_DEBT:
                return
            exact = _opened_for_file_descriptor_lease(lease)
            if exact is None or exact <= self._opened:
                return
            _mark_file_descriptor_lease_closed(lease, 1)
            # Do not decrement ``_opened``: this branch exists precisely because
            # the corresponding increment never reached the Python mirror.

    def _retain_uncertain(self, *, label: str) -> None:
        """Retain uncertain descriptor ownership as terminal cleanup debt."""
        with self._lock:
            if self._state in (self._CLOSED, self._TERMINAL_DEBT):
                return
            lease = self._lease
        if lease is None:
            return
        if retain_uncertain_fd_close(lease, label=label):
            with self._lock:
                if self._lease is lease:
                    self._lease = None
                    self._state = self._TERMINAL_DEBT

    @contextmanager
    def open_descriptor(
        self, opener: Callable[[], int], *, label: str | None = None
    ) -> Iterator[int]:
        """Create one raw descriptor from this reservation and close it exactly once."""
        descriptor = -1
        effective_label = label or self._label
        attempt = _FdOpenAttempt()
        self._begin_open(attempt)
        try:
            descriptor = int(opener())
            if descriptor < 0:
                raise OSError(f"{effective_label} opener returned an invalid descriptor")
            self._commit_opened(attempt)
            yield descriptor
        except BaseException as primary:
            self._abort_open(attempt)
            if descriptor >= 0:
                try:
                    os.close(descriptor)
                except BaseException as cleanup_error:
                    if attempt.committed:
                        try:
                            self._retain_uncertain(label=effective_label)
                        except BaseException as debt_error:
                            add_bounded_note(
                                primary,
                                f"{effective_label} FD-debt retention also failed",
                                debt_error,
                            )
                    add_bounded_note(
                        primary, f"{effective_label} physical close also failed", cleanup_error
                    )
                else:
                    if attempt.committed:
                        self._mark_closed()
            raise
        else:
            try:
                os.close(descriptor)
            except BaseException:
                if attempt.committed:
                    self._retain_uncertain(label=effective_label)
                raise
            else:
                if attempt.committed:
                    self._mark_closed()

    @contextmanager
    def scandir_path(
        self, path: str | os.PathLike[str], *, label: str | None = None
    ) -> Iterator[Any]:
        """Open and account one path-based ``os.scandir`` iterator."""
        effective_label = label or f"{self._label}:scandir-path"
        iterator: Any | None = None
        attempt = _FdOpenAttempt()
        self._begin_open(attempt)
        try:
            iterator = os.scandir(path)
            self._commit_opened(attempt)
            yield iterator
        except BaseException as primary:
            self._abort_open(attempt)
            if iterator is not None:
                try:
                    iterator.close()
                except BaseException as cleanup_error:
                    if attempt.committed:
                        try:
                            self._retain_uncertain(label=effective_label)
                        except BaseException as debt_error:
                            add_bounded_note(
                                primary,
                                f"{effective_label} FD-debt retention also failed",
                                debt_error,
                            )
                    add_bounded_note(
                        primary, f"{effective_label} physical close also failed", cleanup_error
                    )
                else:
                    if attempt.committed:
                        self._mark_closed()
            raise
        else:
            assert iterator is not None
            try:
                iterator.close()
            except BaseException:
                if attempt.committed:
                    self._retain_uncertain(label=effective_label)
                raise
            else:
                if attempt.committed:
                    self._mark_closed()

    @contextmanager
    def scandir(self, descriptor: int, *, label: str | None = None) -> Iterator[Any]:
        """Account the descriptor duplicated internally by ``os.scandir(fd)``."""
        effective_label = label or f"{self._label}:scandir"
        iterator: Any | None = None
        attempt = _FdOpenAttempt()
        self._begin_open(attempt)
        try:
            iterator = os.scandir(descriptor)
            self._commit_opened(attempt)
            yield iterator
        except BaseException as primary:
            self._abort_open(attempt)
            if iterator is not None:
                try:
                    iterator.close()
                except BaseException as cleanup_error:
                    if attempt.committed:
                        try:
                            self._retain_uncertain(label=effective_label)
                        except BaseException as debt_error:
                            add_bounded_note(
                                primary,
                                f"{effective_label} FD-debt retention also failed",
                                debt_error,
                            )
                    add_bounded_note(
                        primary, f"{effective_label} physical close also failed", cleanup_error
                    )
                else:
                    if attempt.committed:
                        self._mark_closed()
            raise
        else:
            assert iterator is not None
            try:
                iterator.close()
            except BaseException:
                if attempt.committed:
                    self._retain_uncertain(label=effective_label)
                raise
            else:
                if attempt.committed:
                    self._mark_closed()

    def release(self) -> None:
        """Return logical credit only after atomically excluding new opens."""
        with self._lock:
            if self._state == self._TERMINAL_DEBT:
                raise RuntimeError("terminal FD debt cannot release or reuse its capability")
            if self._state == self._CLOSED or self._lease is None:
                return
            if self._state == self._RELEASING:
                raise RuntimeError("concurrent file descriptor capability release")
            if self._opening_attempts:
                raise RuntimeError(
                    "cannot release file descriptor capability while descriptors are opening"
                )
            self._reconcile_opened_locked()
            if self._opened != 0:
                raise RuntimeError(
                    "cannot release file descriptor capability with open descriptors"
                )
            lease = self._lease
            self._state = self._RELEASING
        try:
            lease.release()
        except BaseException:
            with self._lock:
                if self._lease is lease and self._state == self._RELEASING:
                    self._state = self._ACTIVE
            raise
        with self._lock:
            if self._lease is lease:
                self._lease = None
            self._state = self._CLOSED

    def __enter__(self) -> "FileDescriptorCapability":
        """Enter the managed resource context."""
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        """Release managed resources when leaving the context."""
        try:
            self.release()
        except BaseException as cleanup_error:
            if exc is not None:
                add_bounded_note(
                    exc, f"{self._label} capability cleanup also failed", cleanup_error
                )
                return
            raise


def acquire_file_descriptor_capability(
    amount: int = 1,
    *,
    timeout_seconds: float = 30.0,
    teardown: bool = False,
    label: str = "file_io",
) -> FileDescriptorCapability:
    """Acquire one atomic, divisible physical descriptor capability."""
    if type(amount) is not int or amount <= 0:
        raise ValueError("file descriptor capability amount must be a positive integer")
    lease = _acquire_file_descriptor_lease(
        amount, timeout_seconds=timeout_seconds, teardown=teardown
    )
    capability = FileDescriptorCapability(lease, amount, label=label)
    try:
        from .concurrency_contracts import observe_runtime_concurrency_contract_noexcept

        observe_runtime_concurrency_contract_noexcept("process_file_descriptor_admission")
    except BaseException:
        pass
    return capability


class ExternalFileCapability:
    """Conservative FD admission for a library that accepts only filesystem paths.

    The external runtime owns the physical descriptor and therefore cannot be
    marked opened/closed at an exact Python linearization point.  We still hold
    logical FD capacity *before* handing it the path and retain that reservation
    for the lifetime of the external object.  The /proc external-FD estimator
    may temporarily count the same descriptor as external too; that deliberate
    conservatism can reduce throughput but cannot over-admit into EMFILE.
    """

    __slots__ = ("_capability",)

    def __init__(self, capability: FileDescriptorCapability) -> None:
        """Initialize the external file capability and its owned runtime state."""
        self._capability: FileDescriptorCapability | None = capability

    def close(self) -> None:
        """Release resources owned by this external file capability."""
        capability = self._capability
        if capability is None:
            return
        capability.release()
        self._capability = None

    def __enter__(self) -> "ExternalFileCapability":
        """Enter the managed resource context."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Release managed resources when leaving the context."""
        self.close()


def acquire_external_file_capability(
    amount: int = 1, *, label: str = "external_file_runtime"
) -> ExternalFileCapability:
    """Reserve descriptor capacity before a path-only external runtime call."""
    return ExternalFileCapability(acquire_file_descriptor_capability(amount, label=label))


@contextmanager
def reserve_file_descriptors(amount: int = 1, *, label: str = "file_io") -> Iterator[None]:
    """Reserve descriptor capacity for one scoped file operation."""
    lease = acquire_file_descriptors(amount)
    try:
        yield
    except BaseException as primary:
        try:
            lease.release()
        except BaseException as cleanup_error:
            add_bounded_note(primary, f"{label} descriptor cleanup also failed", cleanup_error)
        raise
    else:
        lease.release()


def process_thread_snapshot() -> ProcessResourceSnapshot:
    """Return the process thread-governor snapshot."""
    return _THREAD_GOVERNOR.snapshot()


def release_guardian_thread_snapshot() -> ProcessResourceSnapshot:
    """Return the emergency release-guardian thread snapshot."""
    return _GUARDIAN_THREAD_GOVERNOR.snapshot()


def availability_notifier_thread_snapshot() -> ProcessResourceSnapshot:
    """Return the availability-notifier thread snapshot."""
    return _NOTIFIER_THREAD_GOVERNOR.snapshot()


def availability_notifier_snapshot() -> AvailabilityNotifierSnapshot:
    """Return bounded availability-notifier work diagnostics."""
    return _AVAILABILITY_NOTIFIER.snapshot()


def shutdown_availability_notifier(*, deadline_seconds: float = 5.0) -> bool:
    """Stop the availability notifier within the supplied deadline."""
    return _AVAILABILITY_NOTIFIER.close(deadline_seconds=deadline_seconds)


def process_file_descriptor_snapshot() -> ProcessResourceSnapshot:
    """Return the process file-descriptor governor snapshot."""
    return _FD_GOVERNOR.snapshot()


def native_file_descriptor_snapshot() -> dict[str, int | bool]:
    """Return the canonical native FD reserved/opened counters."""
    native = _native_file_descriptor_api()
    try:
        values = tuple(native.process_file_descriptor_permits_snapshot())
    except BaseException as exc:
        clear_exception_traceback(exc)
        return {"available": True, "snapshot_failed": True}
    if len(values) != 6:
        return {"available": True, "snapshot_failed": True}
    reserved, opened, capacity, rejections, protocol_violations, uncertain_close_debts = map(
        int, values
    )
    return {
        "available": True,
        "reserved": reserved,
        "opened": opened,
        "capacity": capacity,
        "rejections": rejections,
        "protocol_violations": protocol_violations,
        "uncertain_close_debts": uncertain_close_debts,
    }


def close_process_resource_external_admission() -> None:
    """Stop new public acquisitions without starving cleanup."""
    _THREAD_GOVERNOR.close_external_admission()
    _FD_GOVERNOR.close_external_admission()


def close_process_resource_admission() -> None:
    """Close the teardown reserve after every cleanup host is quiescent."""
    _THREAD_GOVERNOR.close_admission()
    _FD_GOVERNOR.close_admission()


def close_release_guardian_thread_admission() -> None:
    """Close emergency guardian and notifier thread admission."""
    _GUARDIAN_THREAD_GOVERNOR.close_admission()
    _NOTIFIER_THREAD_GOVERNOR.close_admission()


def _reopen_process_resource_admission_for_tests() -> None:
    """Reopen process resource admission for an isolated test run."""
    _THREAD_GOVERNOR.reopen_admission_for_tests()
    _FD_GOVERNOR.reopen_admission_for_tests()
    _GUARDIAN_THREAD_GOVERNOR.reopen_admission_for_tests()
    _NOTIFIER_THREAD_GOVERNOR.reopen_admission_for_tests()
    _AVAILABILITY_NOTIFIER.reopen_for_tests()


def _reset_after_fork() -> None:
    """Reset process-local state inherited across a fork."""
    global _NOTIFIER_RETRY_OWNERS_LOCK, _NOTIFIER_RETRY_OWNERS, _PYTHON_GOVERNED_FDS_OPENED
    from .fork_safety import fork_quarantine_generation

    if fork_quarantine_generation() > 1:
        return
    _THREAD_GOVERNOR.reset_after_fork()
    _FD_GOVERNOR.reset_after_fork()
    _GUARDIAN_THREAD_GOVERNOR.reset_after_fork()
    _NOTIFIER_THREAD_GOVERNOR.reset_after_fork()
    _AVAILABILITY_NOTIFIER.reset_after_fork()
    _NOTIFIER_RETRY_OWNERS_LOCK = Lock()
    _NOTIFIER_RETRY_OWNERS = {}
    # Inherited physical descriptors no longer have child-side authoritative
    # leases. Treat all of them as external when recomputing headroom.
    with _PYTHON_GOVERNED_FDS_OPENED_LOCK:
        _PYTHON_GOVERNED_FDS_OPENED = 0
    # process-resources is quarantine-only after fork; inherited debt owners are
    # intentionally not mutated or released in the child.


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("process-resources", mode="quarantine_only")


from .shutdown_observers import (  # noqa: E402
    register_shutdown_observer as _register_shutdown_observer,
)

_register_shutdown_observer("native_file_descriptors", native_file_descriptor_snapshot)
_register_shutdown_observer("external_runtime_pools", external_runtime_pool_snapshot)


class _PhysicalFileOwner:
    """Own a physical stream and its exact FD credit as one linearizable unit."""

    _OPEN = 1
    _CLOSING = 2
    _CLOSED = 3
    _TERMINAL_DEBT = 4

    __slots__ = (
        "stream",
        "lease",
        "physical_closed",
        "native_opened",
        "_lock",
        "_condition",
        "_state",
    )

    def __init__(self) -> None:
        """Initialize the physical file owner and its owned runtime state."""
        self.stream: Any | None = None
        self.lease: _Lease | None = None
        self.physical_closed = False
        self.native_opened = False
        self._lock = Lock()
        self._condition = Condition(self._lock)
        self._state = self._OPEN

    def bind(self, stream: Any, lease: _Lease) -> None:
        """Bind the physical resource and exact lease to this physical file owner."""
        with self._condition:
            if self.stream is not None or self.lease is not None or self.native_opened:
                raise RuntimeError("physical file owner is already bound")
            if self._state != self._OPEN:
                raise RuntimeError("physical file owner cannot bind after close begins")
            self.stream = stream
            self.lease = lease
            record_physical_file_descriptors_opened(1)
            self.native_opened = True

    def close(self) -> None:
        """Serialize close/commit/release and fail closed on physical uncertainty."""
        deadline = monotonic() + _RESOURCE_CLOSE_WAIT_TIMEOUT_SECONDS
        with self._condition:
            while self._state == self._CLOSING:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise SchemaSanitizerResourceError(
                        "timed out waiting for governed file close transaction",
                        detail={
                            "stage": "governed_file_close",
                            "limit_name": "resource_close_wait_seconds",
                            "limit_items": _RESOURCE_CLOSE_WAIT_TIMEOUT_SECONDS,
                            "actual_items": _RESOURCE_CLOSE_WAIT_TIMEOUT_SECONDS,
                        },
                    )
                self._condition.wait(timeout=min(0.1, remaining))
            if self._state == self._CLOSED:
                return
            if self._state == self._TERMINAL_DEBT:
                return
            self._state = self._CLOSING
            stream = self.stream
            lease = self.lease
            physical_closed = self.physical_closed

        close_error: BaseException | None = None
        if not physical_closed and stream is not None:
            try:
                stream.close()
            except BaseException as exc:
                close_error = exc
                physical_closed = bool(getattr(stream, "closed", False))
            else:
                physical_closed = True

        if physical_closed:
            with self._condition:
                if self.native_opened:
                    record_physical_file_descriptors_closed(1)
                    self.native_opened = False
                self.physical_closed = True

        lease_error: BaseException | None = None
        retained_as_debt = False
        if lease is not None:
            if physical_closed:
                try:
                    lease.release()
                except BaseException as exc:
                    lease_error = exc
            else:
                retained_as_debt = retain_uncertain_fd_close(lease, label="governed-file-close")

        with self._condition:
            if physical_closed:
                self.stream = None
                if lease is not None and lease_error is None and self.lease is lease:
                    self.lease = None
                self._state = self._CLOSED if self.lease is None else self._OPEN
            elif retained_as_debt:
                if self.lease is lease:
                    self.lease = None
                self._state = self._TERMINAL_DEBT
            else:
                # Retention itself could fail under catastrophic conditions. Keep
                # the exact lease rooted in this owner and allow a later retry.
                self._state = self._OPEN
            self._condition.notify_all()

        if close_error is not None:
            raise close_error
        if lease_error is not None:
            raise lease_error


def _cleanup_governed_file_owner_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Cleanup governed file owner capsule."""
    owner = capsule.arg0
    if isinstance(owner, _PhysicalFileOwner):
        owner.close()
        capsule.arg0 = None


class GovernedFile(io.IOBase):
    """File wrapper whose physical stream and FD lease share one finalizer owner."""

    __slots__ = ("_owner", "_finalizer_ticket", "_finalizer_capsule")

    def __init__(
        self,
        owner: _PhysicalFileOwner,
        *,
        finalizer_ticket: int,
        finalizer_capsule: PreparedFinalizerCleanup,
    ) -> None:
        """Initialize the governed file and its owned runtime state."""
        super().__init__()
        self._owner: _PhysicalFileOwner | None = owner
        self._finalizer_ticket = finalizer_ticket
        self._finalizer_capsule: PreparedFinalizerCleanup | None = finalizer_capsule

    def _open_stream(self) -> Any:
        """Return the governed stream or match the standard closed-file error."""
        owner = self._owner
        if owner is None or owner.stream is None:
            raise ValueError("I/O operation on closed file")
        return owner.stream

    def __getattr__(self, name: str) -> Any:
        """Delegate unresolved attributes to the wrapped object."""
        owner = self._owner
        if owner is None or owner.stream is None:
            raise AttributeError(name)
        return getattr(owner.stream, name)

    # aiohttp selects streaming payload adapters through ``isinstance(IOBase)``
    # and then uses these standard methods directly. IOBase supplies stubs for
    # most of them, so __getattr__ alone cannot forward the protocol.
    def read(self, size: int = -1) -> Any:
        """Read bytes through this governed file."""
        return self._open_stream().read(size)

    def readline(self, size: int | None = -1) -> Any:
        """Read one line through the governed file wrapper."""
        stream = self._open_stream()
        return stream.readline() if size is None else stream.readline(size)

    def readlines(self, hint: int = -1) -> Any:
        """Read lines through the governed file wrapper."""
        return self._open_stream().readlines(hint)

    def write(self, data: Any) -> Any:
        """Write bytes through this governed file."""
        return self._open_stream().write(data)

    def writelines(self, lines: Any) -> None:
        """Write an iterable of lines through the governed file wrapper."""
        self._open_stream().writelines(lines)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Move the governed file's stream position."""
        return int(self._open_stream().seek(offset, whence))

    def tell(self) -> int:
        """Return the governed file's current stream position."""
        return int(self._open_stream().tell())

    def truncate(self, size: int | None = None) -> int:
        """Resize the governed file to the requested byte length."""
        stream = self._open_stream()
        return int(stream.truncate() if size is None else stream.truncate(size))

    def fileno(self) -> int:
        """Return the governed file's native descriptor number."""
        return int(self._open_stream().fileno())

    def flush(self) -> None:
        """Flush buffered data through the governed file wrapper."""
        self._open_stream().flush()

    def isatty(self) -> bool:
        """Return whether the governed file is attached to a terminal."""
        return bool(self._open_stream().isatty())

    def readable(self) -> bool:
        """Return whether the governed file supports reading."""
        return bool(self._open_stream().readable())

    def writable(self) -> bool:
        """Return whether the governed file supports writing."""
        return bool(self._open_stream().writable())

    def seekable(self) -> bool:
        """Return whether the governed file supports random access."""
        return bool(self._open_stream().seekable())

    def __iter__(self) -> "GovernedFile":
        """Iterate over the retained values."""
        self._open_stream()
        return self

    def __next__(self) -> Any:
        """Return the next retained value."""
        return next(self._open_stream())

    def close(self) -> None:
        """Close physical stream first, then commit descriptor credit release."""
        owner = self._owner
        if owner is None:
            return
        owner.close()
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None
        self._owner = None

    @property
    def closed(self) -> bool:
        """Return whether the governed file has been closed."""
        owner = self._owner
        if owner is None:
            return True
        stream = owner.stream
        return bool(getattr(stream, "closed", owner.physical_closed))

    def __enter__(self) -> "GovernedFile":
        """Enter the managed resource context."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Release managed resources when leaving the context."""
        self.close()

    def __del__(self) -> None:
        """Transfer stream+lease together; never return credit before physical close."""
        try:
            if runtime_is_finalizing():
                return
            owner = getattr(self, "_owner", None)
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if owner is None or not ticket or capsule is None:
                return
            capsule.arg0 = owner
            if defer_prepared_finalizer_cleanup(capsule):
                self._owner = None
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
                return
            # A reserved finalizer publication failure is terminally conservative:
            # retain the FD credit rather than allow slot destruction to release it
            # before the physical stream destructor has run.
            lease = owner.lease
            if lease is not None:
                retain_uncertain_fd_close(lease, label="governed-file-finalizer")
                owner.lease = None
        except BaseException:
            pass


@contextmanager
def governed_os_descriptor(
    opener: Callable[[], int],
    *,
    teardown: bool = False,
    timeout_seconds: float = 30.0,
    label: str = "os_descriptor",
) -> Iterator[int]:
    """Open one raw OS descriptor with physical-close-before-credit semantics."""
    lease = _acquire_file_descriptor_lease(1, timeout_seconds=timeout_seconds, teardown=teardown)
    descriptor = -1
    opened = False
    try:
        descriptor = int(opener())
        if descriptor < 0:
            raise OSError(f"{label} opener returned an invalid descriptor")
        record_physical_file_descriptors_opened(1)
        opened = True
        yield descriptor
    except BaseException as primary:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except BaseException as cleanup_error:
                try:
                    retain_uncertain_fd_close(lease, label=label)
                    lease = None  # type: ignore[assignment]
                except BaseException:
                    pass
                add_bounded_note(primary, f"{label} physical close also failed", cleanup_error)
            else:
                if opened:
                    record_physical_file_descriptors_closed(1)
                    opened = False
        if lease is not None:
            try:
                lease.release()
            except BaseException as cleanup_error:
                add_bounded_note(
                    primary, f"{label} descriptor credit cleanup also failed", cleanup_error
                )
        raise
    else:
        try:
            os.close(descriptor)
        except BaseException:
            try:
                retain_uncertain_fd_close(lease, label=label)
                lease = None  # type: ignore[assignment]
            except BaseException:
                pass
            raise
        else:
            if opened:
                record_physical_file_descriptors_closed(1)
            if lease is not None:
                lease.release()


def open_governed_stream(
    opener: Callable[[], Any], *, teardown: bool = False, timeout_seconds: float = 30.0
) -> GovernedFile:
    """Adopt a stream created by a security-sensitive opener under FD authority.

    The finalizer slot and logical/native descriptor credits exist before
    ``opener`` may create the physical descriptor.  The opener remains
    responsible for closing a raw descriptor if it raises before returning a
    stream object.
    """
    capsule = reserve_finalizer_cleanup(_cleanup_governed_file_owner_capsule)
    ticket = capsule.ticket
    owner = _PhysicalFileOwner()
    lease: _Lease | None = None
    try:
        lease = _acquire_file_descriptor_lease(
            1, timeout_seconds=timeout_seconds, teardown=teardown
        )
        stream = opener()
        owner.bind(stream, lease)
        return GovernedFile(owner, finalizer_ticket=ticket, finalizer_capsule=capsule)
    except BaseException as primary:
        try:
            if owner.stream is not None or owner.lease is not None:
                owner.close()
            elif lease is not None:
                lease.release()
        except BaseException as cleanup_error:
            add_bounded_note(primary, "file descriptor owner rollback failed", cleanup_error)
        cancel_prepared_finalizer_cleanup(capsule)
        raise


def open_governed_file(path: Any, mode: str = "rb", *args: Any, **kwargs: Any) -> GovernedFile:
    """Open one local file only after terminal cleanup capacity and FD admission."""
    capsule = reserve_finalizer_cleanup(_cleanup_governed_file_owner_capsule)
    ticket = capsule.ticket
    owner = _PhysicalFileOwner()
    lease: _Lease | None = None
    try:
        lease = acquire_file_descriptors(1)
        stream = open(path, mode, *args, **kwargs)
        owner.bind(stream, lease)
        return GovernedFile(owner, finalizer_ticket=ticket, finalizer_capsule=capsule)
    except BaseException as primary:
        try:
            if owner.stream is not None or owner.lease is not None:
                owner.close()
            elif lease is not None:
                lease.release()
        except BaseException as cleanup_error:
            add_bounded_note(primary, "file descriptor owner rollback failed", cleanup_error)
        cancel_prepared_finalizer_cleanup(capsule)
        raise


# Safety-critical runtime evidence is bound to the authoritative acquisition
# callables themselves; route-profile release certification can therefore prove
# that transport-specific paths exercised the real governors.
from .concurrency_contracts import (  # noqa: E402
    register_runtime_concurrency_contract as _register_runtime_concurrency_contract,
)

_register_runtime_concurrency_contract(
    "external_runtime_pool_claim", _acquire_shared_external_native_thread_permits
)
_register_runtime_concurrency_contract(
    "process_file_descriptor_admission", acquire_file_descriptor_capability
)


__all__ = [
    "AvailabilityEvent",
    "AvailabilityNotifierSnapshot",
    "ProcessResourceSnapshot",
    "UncertainFdCloseSnapshot",
    "GovernedFile",
    "FileDescriptorCapability",
    "ExternalFileCapability",
    "governed_os_descriptor",
    "acquire_file_descriptor_capability",
    "acquire_external_file_capability",
    "acquire_file_descriptors",
    "acquire_teardown_file_descriptors",
    "availability_notifier_snapshot",
    "availability_notifier_thread_snapshot",
    "ExternalRuntimeConcurrencyLease",
    "acquire_external_runtime_threads",
    "constrain_external_runtime_worker_pool",
    "retire_external_runtime_pool",
    "external_runtime_physical_pool_snapshot",
    "external_runtime_pool_snapshot",
    "acquire_project_threads",
    "acquire_teardown_project_threads",
    "acquire_release_guardian_thread",
    "close_process_resource_admission",
    "close_process_resource_external_admission",
    "close_release_guardian_thread_admission",
    "is_project_thread_lease",
    "is_release_guardian_thread_lease",
    "open_governed_file",
    "open_governed_stream",
    "process_file_descriptor_snapshot",
    "native_file_descriptor_snapshot",
    "process_thread_snapshot",
    "register_project_thread_availability",
    "release_guardian_thread_snapshot",
    "reserve_file_descriptors",
    "record_physical_file_descriptors_opened",
    "record_physical_file_descriptors_closed",
    "shutdown_availability_notifier",
    "retain_uncertain_fd_close",
    "uncertain_fd_close_snapshot",
    "unregister_project_thread_availability",
]
