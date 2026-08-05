"""Process-wide logical admission for project-owned threads and file handles."""

from __future__ import annotations

import os
import threading
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum, auto
from threading import Condition, Lock
from time import monotonic, monotonic_ns
from typing import Any, Callable, Iterator

from ..errors import SchemaSanitizerResourceError
from .cancellation import bounded_wait_timeout, check_operation_cancelled
from .diagnostic_epoch import diagnostic_transition
from .durations import deadline_from_timeout, deadline_ns_from_timeout, remaining_seconds
from .finalization import runtime_is_finalizing
from .fork_safety import ensure_runtime_fork_safe, quarantine_inherited_state
from .safe_errors import add_bounded_note, clear_exception_traceback

try:
    import resource
except ImportError:  # pragma: no cover - Windows
    resource = None  # type: ignore[assignment]

_ORIGINAL_THREAD = threading.Thread


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
    compatibility_release_attempts: int = 0
    teardown_admission_closed: bool = False


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


@dataclass(slots=True)
class _AvailabilityDelivery:
    governor: "_Governor"
    event: AvailabilityEvent
    generation: int
    attempts: int = 0
    next_attempt_ns: int = 0

    @property
    def key(self) -> tuple[int, AvailabilityEvent, int]:
        return (id(self.governor), self.event, self.generation)


def _event_for_exact_internal_callback(
    callback: Callable[[], None],
) -> AvailabilityEvent | None:
    """Compatibility bridge authenticated by exact singleton/method identity."""
    try:
        from . import cleanup_dispatcher, retry_scheduler, temporary_janitor
    except BaseException as exc:
        clear_exception_traceback(exc)
        return None
    exact = (
        (
            retry_scheduler._SCHEDULER,
            retry_scheduler._RetryScheduler._ensure_workers,
            AvailabilityEvent.RETRY_SCHEDULER,
        ),
        (
            cleanup_dispatcher._DISPATCHER,
            cleanup_dispatcher._CleanupDispatcher._availability_wakeup,
            AvailabilityEvent.CLEANUP_DISPATCHER,
        ),
        (
            temporary_janitor._JANITOR,
            temporary_janitor._TemporaryArtifactJanitor._availability_wakeup,
            AvailabilityEvent.TEMPORARY_JANITOR,
        ),
    )
    owner = getattr(callback, "__self__", None)
    function = getattr(callback, "__func__", None)
    for expected_owner, expected_function, event in exact:
        if owner is expected_owner and function is expected_function:
            return event
    return None


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


@dataclass(eq=False, slots=True)
class _Waiter:
    amount: int


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

    def __init__(self, governor: "_Governor", amount: int, *, _active: bool = True) -> None:
        object.__setattr__(self, "_sealed", False)
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
        if getattr(self, "_sealed", False) and name in self._IMMUTABLE_FIELDS:
            raise AttributeError(f"{name} is immutable after lease publication")
        object.__setattr__(self, name, value)

    @property
    def amount(self) -> int:
        return self._amount

    @property
    def lease_id(self) -> int:
        return self._lease_id

    def _activate(
        self,
        *,
        amount: int | None = None,
        lease_id: int,
        capability: object,
    ) -> None:
        if self._sealed:
            raise RuntimeError("lease already published")
        if amount is not None:
            object.__setattr__(self, "_amount", int(amount))
        object.__setattr__(self, "_lease_id", int(lease_id))
        object.__setattr__(self, "_capability", capability)
        object.__setattr__(self, "_released", False)
        object.__setattr__(self, "_sealed", True)

    def release(self) -> None:
        if os.getpid() != self._pid:
            return
        # Keep the per-lease lock until the governor acknowledges removal from
        # its ledger.  This linearizes concurrent release() calls without ever
        # claiming success before the authoritative ledger has committed.
        with self._lock:
            if self._released:
                return
            self._governor._release_lease(self)
            self._released = True

    close = release

    def __enter__(self) -> "_Lease":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()

    def __del__(self) -> None:
        try:
            if runtime_is_finalizing():
                return
            self.release()
        except BaseException:
            pass


@dataclass(slots=True)
class _LedgerEntry:
    owner_id: int
    amount: int
    capability: object


_FORKED_GOVERNOR_KEEPALIVE: list[tuple[object, ...]] = []
_UNCERTAIN_FD_CLOSE_LOCK = Lock()
_UNCERTAIN_FD_CLOSE_DEBTS: dict[int, tuple[object, int, str]] = {}
_UNCERTAIN_FD_CLOSE_REJECTED = 0


class _Governor:
    """FIFO cancellable admission backed by exact per-lease capabilities."""

    def __init__(
        self,
        capacity: int,
        label: str,
        *,
        max_waiters: int | None = None,
        level_triggered_availability: bool = False,
    ) -> None:
        self.capacity = max(1, int(capacity))
        self.label = label
        default_waiters = max(64, min(4096, self.capacity * 2))
        configured_waiters = default_waiters if max_waiters is None else max_waiters
        self.max_waiters = max(1, int(configured_waiters))
        self._condition = Condition()
        self._in_use = 0
        self._peak = 0
        self._waiters: deque[_Waiter] = deque()
        self._rejected_waiters = 0
        self._over_release_count = 0
        self._over_release_amount = 0
        self._opportunistic_rejections = 0
        self._availability_events: dict[AvailabilityEvent, int] = {}
        self._availability_sequence = 0
        self._max_availability_callbacks = 1024
        self._rejected_callbacks = 0
        self._lease_sequence = 0
        self._active_leases: dict[int, _LedgerEntry] = {}
        self._unknown_lease_releases = 0
        self._compatibility_release_attempts = 0
        self._external_admission_closed = False
        self._teardown_admission_closed = False
        self._level_triggered_availability = bool(level_triggered_availability)

    def _raise_closed(self, *, teardown: bool = False) -> None:
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

    def _publish_lease_locked(self, lease: _Lease, amount: int) -> None:
        self._lease_sequence += 1
        lease_id = self._lease_sequence
        capability = object()
        self._active_leases[lease_id] = _LedgerEntry(id(lease), amount, capability)
        lease._activate(amount=amount, lease_id=lease_id, capability=capability)

    def acquire(
        self,
        amount: int = 1,
        *,
        timeout_seconds: float | None = None,
        _teardown: bool = False,
    ) -> _Lease:
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise TypeError(f"process {self.label} request must be an integer")
        if amount <= 0:
            raise ValueError(f"process {self.label} request must be > 0")
        requested = amount
        if requested > self.capacity:
            raise SchemaSanitizerResourceError(
                f"process {self.label} request exceeds process capacity",
                detail={
                    "stage": self.label,
                    "limit_name": self.label,
                    "limit_items": self.capacity,
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
            if self._teardown_admission_closed or (
                self._external_admission_closed and not _teardown
            ):
                self._raise_closed(teardown=_teardown)
            if len(self._waiters) >= self.max_waiters:
                self._rejected_waiters += 1
                raise SchemaSanitizerResourceError(
                    f"process {self.label} wait queue exhausted",
                    detail={
                        "stage": self.label,
                        "limit_name": f"{self.label}_waiters",
                        "limit_items": self.max_waiters,
                        "actual_items": len(self._waiters) + 1,
                    },
                )
            waiter = _Waiter(requested)
            self._waiters.append(waiter)
            granted = False
            try:
                while (
                    not self._waiters
                    or self._waiters[0] is not waiter
                    or self._in_use + waiter.amount > self.capacity
                ):
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
                check_operation_cancelled(stage=self.label)
                granted = True
                self._waiters.popleft()
                self._in_use += waiter.amount
                self._peak = max(self._peak, self._in_use)
                self._publish_lease_locked(lease, waiter.amount)
                diagnostic_transition()
                self._condition.notify_all()
                return lease
            finally:
                if not granted:
                    try:
                        self._waiters.remove(waiter)
                    except ValueError:
                        pass
                    self._condition.notify_all()

    def try_acquire_up_to(
        self, desired: int, *, minimum: int = 1, _teardown: bool = False
    ) -> _Lease:
        if isinstance(desired, bool) or not isinstance(desired, int):
            raise TypeError(f"process {self.label} desired amount must be an integer")
        if isinstance(minimum, bool) or not isinstance(minimum, int):
            raise TypeError(f"process {self.label} minimum amount must be an integer")
        if desired <= 0 or minimum <= 0:
            raise ValueError(f"process {self.label} desired and minimum amounts must be > 0")
        if minimum > self.capacity:
            raise SchemaSanitizerResourceError(
                f"process {self.label} minimum request exceeds process capacity",
                detail={
                    "stage": self.label,
                    "limit_name": self.label,
                    "limit_items": self.capacity,
                    "actual_items": minimum,
                },
            )
        ensure_runtime_fork_safe()
        required = minimum
        wanted = max(required, min(self.capacity, desired))
        lease = _Lease(self, wanted, _active=False)
        with self._condition:
            if self._teardown_admission_closed or (
                self._external_admission_closed and not _teardown
            ):
                self._raise_closed(teardown=_teardown)
            if self._waiters:
                self._opportunistic_rejections += 1
                raise SchemaSanitizerResourceError(
                    f"process {self.label} capacity reserved for queued waiters",
                    detail={
                        "stage": self.label,
                        "limit_name": f"{self.label}_fifo",
                        "limit_items": self.capacity,
                        "actual_items": self._in_use + required,
                    },
                )
            available = max(0, self.capacity - self._in_use)
            granted = min(wanted, available)
            if granted < required:
                granted = required if self._in_use == 0 else 0
            if granted <= 0:
                raise SchemaSanitizerResourceError(
                    f"process {self.label} capacity exhausted",
                    detail={
                        "stage": self.label,
                        "limit_name": self.label,
                        "limit_items": self.capacity,
                        "actual_items": self._in_use + required,
                    },
                )
            self._in_use += granted
            self._peak = max(self._peak, self._in_use)
            self._publish_lease_locked(lease, granted)
            diagnostic_transition()
            return lease

    def _return_capacity_locked(self, returned: int) -> tuple[_AvailabilityDelivery, ...]:
        if returned < 0 or returned > self._in_use:
            self._over_release_count += 1
            self._over_release_amount += max(0, returned - self._in_use)
            returned = min(max(0, returned), self._in_use)
        self._in_use -= returned
        callbacks: tuple[_AvailabilityDelivery, ...] = ()
        if self._in_use < self.capacity and self._availability_events:
            # Keep accepted callbacks registered until the notifier
            # acknowledges publication.  Copy-then-clear loses callbacks when
            # the bounded notifier can only accept a prefix.
            callbacks = tuple(
                _AvailabilityDelivery(self, event, generation)
                for event, generation in self._availability_events.items()
            )
        self._condition.notify_all()
        return callbacks

    def _release_lease(self, lease: _Lease) -> None:
        """Commit one exact ledger release; never fail after the commit point."""
        callbacks: tuple[_AvailabilityDelivery, ...] = ()
        with self._condition:
            lease_id = lease.lease_id
            entry = self._active_leases.get(lease_id)
            if (
                entry is None
                or entry.owner_id != id(lease)
                or entry.capability is not lease._capability
            ):
                self._unknown_lease_releases += 1
                diagnostic_transition()
                raise RuntimeError(f"unknown or corrupted process {self.label} lease release")

            # Removing the authoritative entry is the linearization/commit
            # point.  Everything after this line must be non-throwing: a
            # caller retrying a committed release would otherwise observe an
            # "unknown lease" and retain a permit that has already been
            # returned.
            self._active_leases.pop(lease_id, None)
            returned = entry.amount
            if returned < 0 or returned > self._in_use:
                self._over_release_count += 1
                self._over_release_amount += max(0, returned - self._in_use)
                returned = min(max(0, returned), self._in_use)
            self._in_use -= returned
            if self._in_use < self.capacity and self._availability_events:
                try:
                    callbacks = tuple(
                        _AvailabilityDelivery(self, event, generation)
                        for event, generation in self._availability_events.items()
                    )
                except BaseException as exc:
                    # The callbacks remain registered and a later capacity
                    # transition can publish them.  Capacity release itself is
                    # already authoritative and may not be rolled back.
                    clear_exception_traceback(exc)
                    callbacks = ()
            try:
                self._condition.notify_all()
            except BaseException as exc:
                clear_exception_traceback(exc)
            try:
                diagnostic_transition()
            except BaseException as exc:
                clear_exception_traceback(exc)

        try:
            _AVAILABILITY_NOTIFIER.publish(callbacks)
        except BaseException as exc:
            # Capacity release is already committed. Deliveries remain
            # registered in the governor and can be republished later.
            clear_exception_traceback(exc)

    def _delivery_is_current(self, delivery: _AvailabilityDelivery) -> bool:
        with self._condition:
            return (
                delivery.governor is self
                and self._availability_events.get(delivery.event) == delivery.generation
            )

    def _ack_delivery(self, delivery: _AvailabilityDelivery) -> bool:
        with self._condition:
            if not self._delivery_is_current_locked(delivery):
                return False
            self._availability_events.pop(delivery.event, None)
            diagnostic_transition()
            self._condition.notify_all()
            return True

    def _delivery_is_current_locked(self, delivery: _AvailabilityDelivery) -> bool:
        return (
            delivery.governor is self
            and self._availability_events.get(delivery.event) == delivery.generation
        )

    def release(self, amount: int) -> None:
        """Record a deprecated unscoped release attempt without changing capacity."""
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise TypeError(f"process {self.label} release must be an integer")
        with self._condition:
            self._compatibility_release_attempts += 1
            self._over_release_count += 1
            self._over_release_amount += max(0, amount)
            diagnostic_transition()
            self._condition.notify_all()

    def register_availability_event(self, event: object) -> bool:
        ensure_runtime_fork_safe()
        if not isinstance(event, AvailabilityEvent):
            raise TypeError("availability event must be AvailabilityEvent")
        delivery: _AvailabilityDelivery | None = None
        with self._condition:
            if self._teardown_admission_closed:
                self._rejected_callbacks += 1
                return False
            if event in self._availability_events:
                generation = self._availability_events[event]
            else:
                if len(self._availability_events) >= self._max_availability_callbacks:
                    self._rejected_callbacks += 1
                    return False
                self._availability_sequence += 1
                generation = self._availability_sequence
                self._availability_events[event] = generation
                diagnostic_transition()
            # Level-triggered registration: if capacity is already available,
            # publish immediately. This closes the release-before-register gap.
            if self._level_triggered_availability and self._in_use < self.capacity:
                delivery = _AvailabilityDelivery(self, event, generation)
        if delivery is not None:
            try:
                _AVAILABILITY_NOTIFIER.publish((delivery,))
            except BaseException as exc:
                clear_exception_traceback(exc)
        return True

    def unregister_availability_event(self, event: object) -> None:
        if not isinstance(event, AvailabilityEvent):
            return
        with self._condition:
            if self._availability_events.pop(event, None) is not None:
                diagnostic_transition()

    def register_availability_callback(self, callback: Callable[[], None]) -> bool:
        """Compatibility shim accepting only exact built-in singleton methods."""
        event = _event_for_exact_internal_callback(callback)
        if event is None:
            with self._condition:
                self._rejected_callbacks += 1
            return False
        return self.register_availability_event(event)

    def unregister_availability_callback(self, callback: Callable[[], None]) -> None:
        event = _event_for_exact_internal_callback(callback)
        if event is not None:
            self.unregister_availability_event(event)

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
        with self._condition:
            if self._external_admission_closed or self._teardown_admission_closed:
                self._external_admission_closed = False
                self._teardown_admission_closed = False
                diagnostic_transition()
            self._condition.notify_all()

    def snapshot(self) -> ProcessResourceSnapshot:
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
                self._compatibility_release_attempts,
                self._teardown_admission_closed,
            )

    def reset_after_fork(self) -> None:
        quarantine_inherited_state(f"governor:{self.label}", *tuple(self.__dict__.values()))
        self._condition = Condition()
        self._in_use = 0
        self._peak = 0
        self._waiters = deque()
        self._rejected_waiters = 0
        self._over_release_count = 0
        self._over_release_amount = 0
        self._opportunistic_rejections = 0
        self._availability_events = {}
        self._availability_sequence = 0
        self._rejected_callbacks = 0
        self._lease_sequence = 0
        self._active_leases = {}
        self._unknown_lease_releases = 0
        self._compatibility_release_attempts = 0
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


class _NotifierLifecycle(Enum):
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()
    FAILED = auto()


_MAX_AVAILABILITY_ATTEMPTS = 16


class _AvailabilityNotifier:
    """Bounded host for sealed, acknowledged internal availability events."""

    def __init__(self) -> None:
        self._condition = Condition()
        self._queue: deque[_AvailabilityDelivery] = deque()
        self._queued_keys: set[tuple[int, AvailabilityEvent, int]] = set()
        self._parked: dict[tuple[int, AvailabilityEvent, int], _AvailabilityDelivery] = {}
        self._rearmed: set[tuple[int, AvailabilityEvent]] = set()
        self._capacity = 1024
        self._worker: threading.Thread | None = None
        self._starting = False
        self._rejected = 0
        self._start_failures = 0
        self._callback_failures = 0
        self._failed_leases: deque[_Lease] = deque()
        self._restart_scheduled = False
        self._state = _NotifierLifecycle.RUNNING
        self._shutdown_deadline_ns = 0
        self._retiring = False

    def _schedule_restart(self) -> None:
        with self._condition:
            if (
                self._state is not _NotifierLifecycle.RUNNING
                or self._restart_scheduled
                or not self._queue
            ):
                return
            self._restart_scheduled = True
        scheduled = False
        try:
            from .retry_scheduler import schedule_retry

            scheduled = schedule_retry(
                ("availability-notifier", os.getpid()),
                self._restart_from_retry,
                delay_seconds=0.05,
                retained_bytes=256,
                jitter_fraction=0.1,
            )
        except BaseException as exc:
            clear_exception_traceback(exc)
        if not scheduled:
            with self._condition:
                self._restart_scheduled = False
                self._condition.notify_all()

    def _restart_from_retry(self) -> None:
        with self._condition:
            self._restart_scheduled = False
            if self._state is not _NotifierLifecycle.RUNNING:
                return
        self._ensure_worker(allow_stopping=False)

    def publish(
        self, deliveries: tuple[_AvailabilityDelivery, ...]
    ) -> tuple[_AvailabilityDelivery, ...]:
        """Transactionally admit sealed deliveries; rejected items stay registered."""
        self._retry_failed_leases()
        rejected: list[_AvailabilityDelivery] = []
        with self._condition:
            if self._state is not _NotifierLifecycle.RUNNING:
                self._rejected += len(deliveries)
                return deliveries
            for delivery in deliveries:
                if type(delivery) is not _AvailabilityDelivery:
                    self._rejected += 1
                    rejected.append(delivery)
                    continue
                key = delivery.key
                if key in self._queued_keys or key in self._parked:
                    # A service can re-register while its previous wakeup is
                    # EXECUTING. Preserve that level-triggered request and run
                    # it once more after the current delivery succeeds.
                    self._rearmed.add((id(delivery.governor), delivery.event))
                    continue
                if len(self._queue) + len(self._parked) >= self._capacity:
                    rejected.append(delivery)
                    continue
                self._queue.append(delivery)
                self._queued_keys.add(key)
            self._rejected += len(rejected)
            if deliveries:
                diagnostic_transition()
            self._condition.notify_all()
        self._ensure_worker(allow_stopping=False)
        return tuple(rejected)

    def _ensure_worker(self, *, allow_stopping: bool) -> None:
        self._retry_failed_leases()
        with self._condition:
            allowed = self._state is _NotifierLifecycle.RUNNING or (
                allow_stopping and self._state is _NotifierLifecycle.STOPPING
            )
            worker = self._worker
            if (
                not allowed
                or not self._queue
                or self._failed_leases
                or self._starting
                or (worker is not None and worker.is_alive())
            ):
                return
            self._starting = True
        try:
            lease = _NOTIFIER_THREAD_GOVERNOR.try_acquire_up_to(1, minimum=1, _teardown=True)
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
            thread.start()
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
        if not self._queue:
            return None
        now_ns = monotonic_ns()
        earliest = 0
        for _index in range(len(self._queue)):
            delivery = self._queue.popleft()
            if delivery.next_attempt_ns <= now_ns:
                return delivery
            self._queue.append(delivery)
            if earliest == 0 or delivery.next_attempt_ns < earliest:
                earliest = delivery.next_attempt_ns
        if earliest:
            self._condition.wait(timeout=min(0.25, max(0.001, (earliest - now_ns) / 1_000_000_000)))
        return None

    def _run(self, lease: _Lease) -> None:
        current = threading.current_thread()
        try:
            while True:
                with self._condition:
                    delivery = self._take_due_locked()
                    if delivery is None:
                        if not self._queue:
                            return
                        continue
                if not delivery.governor._delivery_is_current(delivery):
                    with self._condition:
                        self._queued_keys.discard(delivery.key)
                        diagnostic_transition()
                        self._condition.notify_all()
                    continue
                with self._condition:
                    terminal = self._state in (
                        _NotifierLifecycle.STOPPED,
                        _NotifierLifecycle.FAILED,
                    ) or bool(
                        self._state is _NotifierLifecycle.STOPPING
                        and self._shutdown_deadline_ns
                        and monotonic_ns() >= self._shutdown_deadline_ns
                    )
                    if terminal:
                        self._queued_keys.discard(delivery.key)
                        self._parked[delivery.key] = delivery
                        diagnostic_transition()
                        self._condition.notify_all()
                        continue
                succeeded = False
                try:
                    _dispatch_availability_event(delivery.event)
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
                            self._queue.append(delivery)
                        else:
                            delivery.governor._ack_delivery(delivery)
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
                            self._queued_keys.discard(delivery.key)
                            self._parked[delivery.key] = delivery
                        else:
                            delay_ns = min(
                                1_000_000_000,
                                10_000_000 * (2 ** min(delivery.attempts, 7)),
                            )
                            delivery.next_attempt_ns = monotonic_ns() + delay_ns
                            self._queue.append(delivery)
                    diagnostic_transition()
                    self._condition.notify_all()
        finally:
            with self._condition:
                self._retiring = True
                self._condition.notify_all()
            self._release_or_retain_lease(lease)
            with self._condition:
                self._retiring = False
                if self._worker is current:
                    self._worker = None
                should_restart = bool(
                    self._queue
                    and not self._failed_leases
                    and self._state is _NotifierLifecycle.RUNNING
                )
            if should_restart:
                self._ensure_worker(allow_stopping=False)

    def snapshot(self) -> AvailabilityNotifierSnapshot:
        with self._condition:
            worker = self._worker
            now_ns = monotonic_ns()
            return AvailabilityNotifierSnapshot(
                len(self._queue),
                bool(worker is not None and worker.is_alive()),
                self._starting,
                self._rejected,
                len(self._failed_leases),
                self._start_failures,
                self._state.name,
                sum(delivery.next_attempt_ns > now_ns for delivery in self._queue),
                len(self._parked),
                self._callback_failures,
                self._retiring,
                len(self._rearmed),
            )

    def close(self, *, deadline_seconds: float = 5.0) -> bool:
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
                return not self._parked and not self._failed_leases
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
                    self._queue
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
        with self._condition:
            if self._state is _NotifierLifecycle.RUNNING:
                return
            worker = self._worker
            if (
                self._queue
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
        quarantine_inherited_state(
            "availability-notifier",
            self._condition,
            self._queue,
            self._parked,
            self._worker,
            self._failed_leases,
        )
        self._condition = Condition()
        self._queue = deque()
        self._queued_keys = set()
        self._parked = {}
        self._rearmed = set()
        self._worker = None
        self._starting = False
        self._rejected = 0
        self._start_failures = 0
        self._callback_failures = 0
        self._failed_leases = deque()
        self._restart_scheduled = False
        self._state = _NotifierLifecycle.STOPPED
        self._shutdown_deadline_ns = 0
        self._retiring = False


_FORKED_NOTIFIER_KEEPALIVE: list[tuple[object, ...]] = []
_NOTIFIER_THREAD_GOVERNOR = _Governor(1, "availability_notifier_threads")
_AVAILABILITY_NOTIFIER = _AvailabilityNotifier()


def _thread_capacity() -> int:
    configured = os.getenv("SCHEMA_SANITIZER_MAX_PROJECT_THREADS")
    if configured:
        try:
            return max(2, int(configured))
        except ValueError:
            pass
    return min(256, max(8, (os.cpu_count() or 1) * 4))


def _fd_capacity() -> int:
    configured = os.getenv("SCHEMA_SANITIZER_MAX_OPEN_FILES")
    if configured:
        try:
            return max(16, int(configured))
        except ValueError:
            pass
    if resource is None:
        return 256  # type: ignore[unreachable]
    soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < 0 or soft >= 1 << 50:
        soft = 4096
    return max(16, min(4096, int(soft) - max(32, int(soft) // 8)))


_THREAD_GOVERNOR = _Governor(
    _thread_capacity(), "project_threads", level_triggered_availability=True
)
_FD_GOVERNOR = _Governor(_fd_capacity(), "open_file_descriptors")
_GUARDIAN_THREAD_GOVERNOR = _Governor(2, "release_guardian_emergency_threads")


def acquire_project_threads(desired: int, *, minimum: int = 1) -> _Lease:
    """Acquire up to the desired number of governed project threads."""
    return _THREAD_GOVERNOR.try_acquire_up_to(desired, minimum=minimum)


def acquire_teardown_project_threads(desired: int, *, minimum: int = 1) -> _Lease:
    """Acquire from the internal reserve after external admission is closed."""
    return _THREAD_GOVERNOR.try_acquire_up_to(desired, minimum=minimum, _teardown=True)


def acquire_release_guardian_thread() -> _Lease:
    """Reserve the dedicated emergency slot used only by the release guardian."""
    return _GUARDIAN_THREAD_GOVERNOR.try_acquire_up_to(1, minimum=1)


def is_release_guardian_thread_lease(owner: object) -> bool:
    """Return whether *owner* is an exact permit from the guardian bootstrap pool."""
    return type(owner) is _Lease and owner._governor is _GUARDIAN_THREAD_GOVERNOR


def register_project_thread_availability(event: AvailabilityEvent) -> bool:
    """Register a privileged wakeup for newly available thread capacity."""
    return _THREAD_GOVERNOR.register_availability_event(event)


def unregister_project_thread_availability(event: AvailabilityEvent) -> None:
    """Remove a previously registered thread-capacity wakeup."""
    _THREAD_GOVERNOR.unregister_availability_event(event)


def acquire_file_descriptors(amount: int = 1, *, timeout_seconds: float = 30.0) -> _Lease:
    """Acquire governed file-descriptor capacity within a deadline."""
    return _FD_GOVERNOR.acquire(amount, timeout_seconds=timeout_seconds)


def acquire_teardown_file_descriptors(amount: int = 1, *, timeout_seconds: float = 30.0) -> _Lease:
    """Acquire a descriptor needed solely to complete teardown."""
    return _FD_GOVERNOR.acquire(amount, timeout_seconds=timeout_seconds, _teardown=True)


def retain_uncertain_fd_close(lease: object, *, label: str) -> bool:
    """Retain capacity when close() did not prove the descriptor is closed.

    The numeric descriptor is intentionally not retried because it may already
    have been recycled by the OS. The exact governor lease stays live as a
    process-lifetime accounting debt instead of fabricating free capacity.
    """
    global _UNCERTAIN_FD_CLOSE_REJECTED
    # Only exact ledger-backed FD leases may become process-lifetime debt.
    # Historical test doubles and third-party lease-like objects have no
    # authoritative capacity ledger; retain them on the original owner so a
    # later release() can finish without ever retrying the recycled FD number.
    if type(lease) is not _Lease or lease._governor is not _FD_GOVERNOR:
        return False
    if type(label) is not str:
        label = "uncertain-fd-close"
    with _UNCERTAIN_FD_CLOSE_LOCK:
        key = id(lease)
        if key in _UNCERTAIN_FD_CLOSE_DEBTS:
            return True
        # The number of debts cannot legitimately exceed the FD governor's
        # active capacity, so this is a hard invariant bound rather than an
        # eviction policy that could lose the only owner.
        if len(_UNCERTAIN_FD_CLOSE_DEBTS) >= _FD_GOVERNOR.capacity:
            _UNCERTAIN_FD_CLOSE_REJECTED += 1
            raise RuntimeError("uncertain FD-close debt capacity exhausted")
        _UNCERTAIN_FD_CLOSE_DEBTS[key] = (lease, monotonic_ns(), label[:128])
        diagnostic_transition()
        return True


def uncertain_fd_close_snapshot() -> UncertainFdCloseSnapshot:
    """Return diagnostics for descriptor capacity retained as debt."""
    with _UNCERTAIN_FD_CLOSE_LOCK:
        oldest = min(
            (created_ns for _lease, created_ns, _label in _UNCERTAIN_FD_CLOSE_DEBTS.values()),
            default=0,
        )
        return UncertainFdCloseSnapshot(
            len(_UNCERTAIN_FD_CLOSE_DEBTS),
            _FD_GOVERNOR.capacity,
            oldest,
            _UNCERTAIN_FD_CLOSE_REJECTED,
        )


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
    _THREAD_GOVERNOR.reopen_admission_for_tests()
    _FD_GOVERNOR.reopen_admission_for_tests()
    _GUARDIAN_THREAD_GOVERNOR.reopen_admission_for_tests()
    _NOTIFIER_THREAD_GOVERNOR.reopen_admission_for_tests()
    _AVAILABILITY_NOTIFIER.reopen_for_tests()


def _reset_after_fork() -> None:
    from .fork_safety import fork_quarantine_generation

    if fork_quarantine_generation() > 1:
        return
    _THREAD_GOVERNOR.reset_after_fork()
    _FD_GOVERNOR.reset_after_fork()
    _GUARDIAN_THREAD_GOVERNOR.reset_after_fork()
    _NOTIFIER_THREAD_GOVERNOR.reset_after_fork()
    _AVAILABILITY_NOTIFIER.reset_after_fork()
    global _UNCERTAIN_FD_CLOSE_DEBTS, _UNCERTAIN_FD_CLOSE_LOCK
    quarantine_inherited_state(
        "uncertain-fd-close-debts", *tuple(_UNCERTAIN_FD_CLOSE_DEBTS.values())
    )
    _UNCERTAIN_FD_CLOSE_DEBTS = {}
    _UNCERTAIN_FD_CLOSE_LOCK = Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


__all__ = [
    "AvailabilityEvent",
    "AvailabilityNotifierSnapshot",
    "ProcessResourceSnapshot",
    "UncertainFdCloseSnapshot",
    "acquire_file_descriptors",
    "acquire_teardown_file_descriptors",
    "availability_notifier_snapshot",
    "availability_notifier_thread_snapshot",
    "acquire_project_threads",
    "acquire_teardown_project_threads",
    "acquire_release_guardian_thread",
    "close_process_resource_admission",
    "close_process_resource_external_admission",
    "close_release_guardian_thread_admission",
    "is_release_guardian_thread_lease",
    "process_file_descriptor_snapshot",
    "process_thread_snapshot",
    "register_project_thread_availability",
    "release_guardian_thread_snapshot",
    "reserve_file_descriptors",
    "shutdown_availability_notifier",
    "retain_uncertain_fd_close",
    "uncertain_fd_close_snapshot",
    "unregister_project_thread_availability",
]
