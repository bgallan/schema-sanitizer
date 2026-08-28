"""Bounded process-wide execution for blocking terminal cleanup publishers."""

from __future__ import annotations

import os
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto
from functools import partial
from typing import Any

from .compact_callback import callback_retains_hidden_owner
from .control_plane_budget import ControlPlaneTicket, release_control_plane, reserve_control_plane
from .diagnostic_epoch import diagnostic_transition
from .durations import deadline_ns_from_timeout, remaining_seconds
from .fork_safety import ensure_runtime_fork_safe, quarantine_inherited_state
from .governed_thread import defer_governed_thread_retirement, start_governed_thread
from .process_resources import (
    AvailabilityEvent,
    acquire_project_threads,
    acquire_teardown_project_threads,
    register_project_thread_availability,
    unregister_project_thread_availability,
)
from .retry_scheduler import adopt_failed_release, cancel_retry, schedule_retry
from .safe_errors import clear_exception_traceback, safe_exception_summary
from .terminal_ownership import (
    publish_terminal_owner,
    retire_terminal_category,
)

_MAX_PENDING_CLEANUPS = 4096
_MAX_PENDING_BYTES = 64 * 1024 * 1024
_DEFAULT_CALL_CHARGE = 1024
_MAX_WORKERS = 2
_MAX_SUBSYSTEM_CLEANUPS = _MAX_PENDING_CLEANUPS // 2
_MAX_SUBSYSTEM_BYTES = _MAX_PENDING_BYTES
_DRR_QUANTUM_BYTES = 1024 * 1024
_IDLE_SECONDS = 5.0
_RETRY_SECONDS = 0.5
_MAX_FAILED_WORKER_LEASES = _MAX_WORKERS
_FORKED_CLEANUP_KEEPALIVE: list[tuple[object, ...]] = []
_MAX_CLEANUP_ATTEMPTS = 16
_MAX_DEAD_LETTER_CALLS = 256
_MAX_DEAD_LETTER_BYTES = 4 * 1024 * 1024
_MAX_RETRY_SECONDS = 30.0
_MAX_DEADLINE_NS = (1 << 63) - 1
_MAX_CLEANUP_RETRY_OWNERS = 1024
_CLEANUP_RETRY_OWNERS_LOCK = threading.Lock()
_CLEANUP_RETRY_OWNERS: dict[int, object] = {}


def _retry_cleanup_dispatcher_token(token: int) -> None:
    with _CLEANUP_RETRY_OWNERS_LOCK:
        owner = _CLEANUP_RETRY_OWNERS.pop(token, None)
    if owner is None:
        return
    owner._retry_start()  # type: ignore[attr-defined]


class CleanupSubsystem(Enum):
    """Classify cleanup work for bounded subsystem diagnostics."""

    GENERIC = auto()
    RETRY = auto()
    JANITOR = auto()
    REMOTE = auto()
    STORAGE = auto()
    MEMORY = auto()


class _CleanupState(Enum):
    RUNNABLE = auto()
    ACTIVE = auto()
    DELAYED = auto()
    DEAD_LETTER = auto()
    PARKED = auto()
    FINISHED = auto()


@dataclass(slots=True)
class _CleanupCall:
    callback: Callable[..., None]
    args: tuple[Any, ...]
    retained_bytes: int
    reserved_bytes: int
    subsystem: CleanupSubsystem
    attempts: int = 0
    next_attempt_ns: int = 0
    first_failure: str = ""
    last_failure: str = ""
    parked: bool = False
    sequence: int = 0
    control_ticket: ControlPlaneTicket | None = None
    state: _CleanupState = _CleanupState.RUNNABLE


@dataclass(frozen=True, slots=True)
class CleanupDispatcherSnapshot:
    """Expose a bounded snapshot of queued and active cleanup work."""

    pending_calls: int
    pending_bytes: int
    active_calls: int
    active_bytes: int
    active_workers: int
    workers_starting: int
    worker_start_failures: int
    rejected_calls: int
    rejected_bytes: int
    failed_worker_leases: int
    active_subsystems: int = 0
    progress_epoch: int = 0
    closed: bool = False
    dead_letter_calls: int = 0
    dead_letter_bytes: int = 0
    parked_calls: int = 0
    oldest_retry_ns: int = 0
    delayed_calls: int = 0
    delayed_bytes: int = 0
    owned_calls: int = 0
    owned_bytes: int = 0
    circuit_open: bool = False
    oldest_active_ns: int = 0
    retiring_workers: int = 0
    owned_reserved_bytes: int = 0
    active_reserved_bytes: int = 0
    rejected_hidden_owner_calls: int = 0
    protocol_violations: int = 0


class _CleanupDispatcher:
    """Governed cleanup with physically separate runnable and terminal states."""

    def __init__(self) -> None:
        self._reset(os.getpid())

    def _reset(self, pid: int, *, cancel_old: bool = True) -> None:
        if globals().get("_DISPATCHER") is self:
            retire_terminal_category("cleanup_dispatcher")
        old_pid = getattr(self, "_pid", None)
        if cancel_old and old_pid is not None:
            cancel_retry(("cleanup-dispatcher", old_pid))
        self._pid = pid
        self._condition = threading.Condition()
        self._queues: dict[CleanupSubsystem, deque[_CleanupCall]] = {}
        self._ready_subsystems: deque[CleanupSubsystem] = deque()
        self._deficits: dict[CleanupSubsystem, int] = {}
        self._owned_index: dict[int, _CleanupCall] = {}
        self._delayed_calls = 0
        self._delayed_bytes = 0
        self._sequence = 0
        # Pre-seed subsystem keys so admission never needs to grow these
        # authoritative accounting dictionaries after ownership is accepted.
        self._subsystem_counts: dict[CleanupSubsystem, int] = {
            subsystem: 0 for subsystem in CleanupSubsystem
        }
        self._subsystem_bytes: dict[CleanupSubsystem, int] = {
            subsystem: 0 for subsystem in CleanupSubsystem
        }
        self._queued_calls = 0
        self._queued_bytes = 0
        self._owned_calls = 0
        self._owned_bytes = 0
        self._owned_reserved_bytes = 0
        self._active_calls = 0
        self._active_bytes = 0
        self._active_reserved_bytes = 0
        self._workers: set[threading.Thread] = set()
        self._worker_leases: dict[threading.Thread, Any] = {}
        self._retiring_workers: dict[threading.Thread, Any] = {}
        self._workers_starting = 0
        self._protocol_violations = 0
        self._retry_scheduled = False
        self._availability_registered = False
        self._failed_worker_leases: deque[Any] = deque()
        self._terminal_failed_worker_lease: Any | None = None
        self._worker_start_failures = 0
        self._rejected_calls = 0
        self._rejected_bytes = 0
        self._rejected_hidden_owner_calls = 0
        self._closed = False
        self._progress_epoch = 0
        self._dead_letters: list[_CleanupCall | None] = [None] * _MAX_DEAD_LETTER_CALLS
        self._dead_letter_count = 0
        self._dead_letter_bytes = 0
        self._parked_calls = 0
        self._oldest_active_ns = 0
        self._active_started_ns: dict[int, int] = {}
        self._circuit_open = False
        self._corrupted = False

    def _ensure_process(self) -> None:
        if self._pid != os.getpid():
            self._reset(os.getpid())

    def _mark_progress_locked(self) -> None:
        try:
            self._progress_epoch = min((1 << 63) - 1, self._progress_epoch + 1)
            diagnostic_transition()
        except BaseException:
            pass

    def _decrement_counter_locked(self, name: str, amount: int = 1) -> None:
        """Decrease an authoritative/quiescence counter without masking underflow."""
        current = int(getattr(self, name))
        if amount < 0 or current < amount:
            self._protocol_violations += 1
            return
        setattr(self, name, current - amount)

    def _reconcile_owner_counters_locked(self) -> bool:
        """Rebuild admission caches from the exact owned-call index."""
        calls = 0
        retained = 0
        reserved = 0
        counts = {subsystem: 0 for subsystem in CleanupSubsystem}
        bytes_by_subsystem = {subsystem: 0 for subsystem in CleanupSubsystem}
        try:
            for call in self._owned_index.values():
                calls += 1
                retained += call.retained_bytes
                reserved += call.reserved_bytes
                counts[call.subsystem] += 1
                bytes_by_subsystem[call.subsystem] += call.retained_bytes
        except BaseException:
            self._corrupted = True
            self._circuit_open = True
            self._protocol_violations += 1
            return False
        mismatch = (
            self._owned_calls != calls
            or self._owned_bytes != retained
            or self._owned_reserved_bytes != reserved
            or any(self._subsystem_counts.get(k, 0) != v for k, v in counts.items())
            or any(self._subsystem_bytes.get(k, 0) != v for k, v in bytes_by_subsystem.items())
        )
        if mismatch:
            self._corrupted = True
            self._circuit_open = True
            self._protocol_violations += 1
            self._owned_calls = calls
            self._owned_bytes = retained
            self._owned_reserved_bytes = reserved
            self._subsystem_counts = counts
            self._subsystem_bytes = bytes_by_subsystem
            self._mark_progress_locked()
        return not self._corrupted

    def _charge_owner_locked(self, call: _CleanupCall) -> None:
        subsystem = call.subsystem
        # Compute first; if Python cannot allocate one of the bounded integer
        # results, no authoritative field has changed yet.
        owned_calls = self._owned_calls + 1
        owned_bytes = self._owned_bytes + call.retained_bytes
        owned_reserved = self._owned_reserved_bytes + call.reserved_bytes
        subsystem_count = self._subsystem_counts[subsystem] + 1
        subsystem_bytes = self._subsystem_bytes[subsystem] + call.retained_bytes
        self._owned_calls = owned_calls
        self._owned_bytes = owned_bytes
        self._owned_reserved_bytes = owned_reserved
        self._subsystem_counts[subsystem] = subsystem_count
        self._subsystem_bytes[subsystem] = subsystem_bytes

    def _uncharge_owner_locked(self, call: _CleanupCall) -> ControlPlaneTicket | None:
        """Detach authoritative ownership and return the ticket for out-of-lock release."""
        subsystem = call.subsystem
        self._owned_index.pop(id(call), None)
        self._decrement_counter_locked("_owned_calls")
        self._decrement_counter_locked("_owned_bytes", call.retained_bytes)
        self._decrement_counter_locked("_owned_reserved_bytes", call.reserved_bytes)
        if self._subsystem_counts[subsystem] <= 0:
            self._protocol_violations += 1
        else:
            self._subsystem_counts[subsystem] -= 1
        if self._subsystem_bytes[subsystem] < call.retained_bytes:
            self._protocol_violations += 1
        else:
            self._subsystem_bytes[subsystem] -= call.retained_bytes
        ticket = call.control_ticket
        call.control_ticket = None
        return ticket

    def _enqueue_runnable_locked(self, call: _CleanupCall) -> None:
        """Publish runnable ownership transactionally."""
        subsystem = call.subsystem
        next_calls = self._queued_calls + 1
        next_bytes = self._queued_bytes + call.retained_bytes
        queue = self._queues.get(subsystem)
        created = queue is None
        if created:
            queue = deque((call,))
            self._queues[subsystem] = queue
            try:
                self._ready_subsystems.append(subsystem)
            except BaseException:
                self._queues.pop(subsystem, None)
                raise
        else:
            assert queue is not None
            queue.append(call)
        call.next_attempt_ns = 0
        call.state = _CleanupState.RUNNABLE
        self._queued_calls = next_calls
        self._queued_bytes = next_bytes

    def _enqueue_delayed_locked(self, call: _CleanupCall) -> None:
        """Move ACTIVE ownership to delayed state without publishing a heap node."""
        # Ordering no longer consumes a lifetime-monotonic namespace. The
        # call's exact control-plane token is bounded/reusable and remains live
        # for the entire cleanup owner lifetime.
        sequence = int(getattr(call.control_ticket, "token", 0) or 1)
        next_calls = self._delayed_calls + 1
        next_bytes = self._delayed_bytes + call.retained_bytes
        # Commit tail is allocation-free; authoritative rooting stays in _owned_index.
        call.sequence = sequence
        call.state = _CleanupState.DELAYED
        self._sequence = sequence
        self._delayed_calls = next_calls
        self._delayed_bytes = next_bytes

    def _promote_delayed_locked(self, *, force: bool = False) -> None:
        now = time.monotonic_ns()
        for call in self._owned_index.values():
            if call.state is not _CleanupState.DELAYED:
                continue
            if not force and call.next_attempt_ns > now:
                continue
            # Runnable queue publication is destination-first. If deque/dict
            # growth fails, DELAYED remains authoritative and will retry later.
            self._enqueue_runnable_locked(call)
            self._decrement_counter_locked("_delayed_calls")
            self._decrement_counter_locked("_delayed_bytes", call.retained_bytes)

    def submit(
        self,
        callback: Callable[..., None],
        *args: Any,
        retained_bytes: int = _DEFAULT_CALL_CHARGE,
        reserved_bytes: int = 0,
        start_worker: bool = True,
        subsystem: CleanupSubsystem = CleanupSubsystem.GENERIC,
    ) -> bool:
        self._ensure_process()
        ensure_runtime_fork_safe()
        if not callable(callback):
            raise TypeError("cleanup callback must be callable")
        if type(retained_bytes) is not int:
            raise TypeError("cleanup retained_bytes must be an exact integer")
        if retained_bytes < 0:
            raise ValueError("cleanup retained_bytes must be >= 0")
        if type(reserved_bytes) is not int:
            raise TypeError("cleanup reserved_bytes must be an exact integer")
        if reserved_bytes < 0:
            raise ValueError("cleanup reserved_bytes must be >= 0")
        if type(start_worker) is not bool:
            raise TypeError("cleanup start_worker must be an exact bool")
        # Cleanup queues may retain only compact callbacks/tokens. Rich owners
        # belong in bounded subsystem registries referenced by an integer token;
        # a byte claim is never trusted as proof that an arbitrary Python graph
        # is small enough.
        if callback_retains_hidden_owner(callback, tuple(args)):
            with self._condition:
                self._rejected_calls += 1
                self._rejected_hidden_owner_calls += 1
                self._mark_progress_locked()
            return False
        # Every call owns scheduler metadata even when the publisher claims a
        # tiny payload.  The caller-provided value may increase the charge but
        # can never understate this fixed floor.
        charge = max(_DEFAULT_CALL_CHARGE, retained_bytes)
        if not isinstance(subsystem, CleanupSubsystem):
            raise TypeError("cleanup subsystem must be CleanupSubsystem")
        call = _CleanupCall(callback, tuple(args), charge, reserved_bytes, subsystem)
        # Reserve global control-plane ownership before taking the dispatcher
        # mutex. This keeps the lock order one-way: control-plane -> dispatcher
        # is never nested with dispatcher -> control-plane.
        ticket = reserve_control_plane("cleanup_call", 384)
        call.control_ticket = ticket
        release_ticket: ControlPlaneTicket | None = None
        accepted = False
        try:
            with self._condition:
                self._reconcile_owner_counters_locked()
                rejected = False
                if self._closed or self._circuit_open or self._corrupted:
                    rejected = True
                elif (
                    self._active_calls >= _MAX_WORKERS
                    and self._oldest_active_ns
                    and time.monotonic_ns() - self._oldest_active_ns >= 60_000_000_000
                ):
                    self._circuit_open = True
                    self._rejected_calls += 1
                    self._mark_progress_locked()
                    rejected = True
                elif self._owned_calls >= _MAX_PENDING_CLEANUPS:
                    self._rejected_calls += 1
                    rejected = True
                elif self._subsystem_counts.get(subsystem, 0) >= _MAX_SUBSYSTEM_CLEANUPS:
                    self._rejected_calls += 1
                    rejected = True
                elif charge > _MAX_PENDING_BYTES - self._owned_bytes:
                    self._rejected_bytes += max(1, retained_bytes)
                    rejected = True
                elif charge > _MAX_SUBSYSTEM_BYTES - self._subsystem_bytes.get(subsystem, 0):
                    self._rejected_bytes += max(1, retained_bytes)
                    rejected = True

                if rejected:
                    release_ticket = call.control_ticket
                    call.control_ticket = None
                else:
                    self._charge_owner_locked(call)
                    try:
                        self._owned_index[id(call)] = call
                        try:
                            self._enqueue_runnable_locked(call)
                        except BaseException:
                            self._owned_index.pop(id(call), None)
                            raise
                    except BaseException:
                        release_ticket = self._uncharge_owner_locked(call)
                        raise
                    self._mark_progress_locked()
                    self._condition.notify_all()
                    accepted = True
        except BaseException:
            if release_ticket is not None:
                release_control_plane(release_ticket)
            raise
        if release_ticket is not None:
            release_control_plane(release_ticket)
        if not accepted:
            return False
        if start_worker:
            self._ensure_workers()
        return True

    def _has_failed_worker_leases_locked(self) -> bool:
        return bool(self._failed_worker_leases or self._terminal_failed_worker_lease is not None)

    def _retain_failed_worker_lease_locked(self, lease: Any) -> None:
        if any(existing is lease for existing in self._failed_worker_leases):
            return
        if self._terminal_failed_worker_lease is lease:
            return
        if len(self._failed_worker_leases) < _MAX_FAILED_WORKER_LEASES:
            self._failed_worker_leases.append(lease)
            return
        if self._terminal_failed_worker_lease is None:
            self._terminal_failed_worker_lease = lease
            self._worker_start_failures += 1
            return
        raise RuntimeError("cleanup worker lease ownership invariant exceeded")

    def _take_failed_worker_leases_locked(self) -> deque[Any]:
        failed = self._failed_worker_leases
        self._failed_worker_leases = deque()
        terminal = self._terminal_failed_worker_lease
        self._terminal_failed_worker_lease = None
        if terminal is not None:
            failed.append(terminal)
        return failed

    def _register_availability_locked(self) -> None:
        if self._availability_registered:
            return
        self._availability_registered = bool(
            register_project_thread_availability(AvailabilityEvent.CLEANUP_DISPATCHER)
        )

    def _unregister_availability(self) -> None:
        with self._condition:
            if not self._availability_registered:
                return
            self._availability_registered = False
        unregister_project_thread_availability(AvailabilityEvent.CLEANUP_DISPATCHER)

    def _availability_wakeup(self) -> None:
        # The notifier acknowledges the sealed event only after this returns.
        # Keep the registration live if a transient failure escapes.
        self._retry_start()

    def _schedule_retry_locked(self) -> None:
        if self._retry_scheduled or self._closed:
            return
        retry_token = id(self)
        with _CLEANUP_RETRY_OWNERS_LOCK:
            if (
                retry_token not in _CLEANUP_RETRY_OWNERS
                and len(_CLEANUP_RETRY_OWNERS) >= _MAX_CLEANUP_RETRY_OWNERS
            ):
                self._retry_scheduled = False
            else:
                _CLEANUP_RETRY_OWNERS[retry_token] = self
                self._retry_scheduled = schedule_retry(
                    ("cleanup-dispatcher", retry_token),
                    partial(_retry_cleanup_dispatcher_token, retry_token),
                    delay_seconds=_RETRY_SECONDS,
                    retained_bytes=512,
                    jitter_fraction=0.2,
                )
                if not self._retry_scheduled:
                    _CLEANUP_RETRY_OWNERS.pop(retry_token, None)
        if not self._retry_scheduled:
            self._register_availability_locked()

    def _ensure_workers(self) -> None:
        while True:
            with self._condition:
                self._promote_delayed_locked(force=self._closed)
                if self._has_failed_worker_leases_locked():
                    self._schedule_retry_locked()
                    return
                desired = min(_MAX_WORKERS, max(1, self._queued_calls))
                current = len(self._workers) + self._workers_starting
                if not self._queued_calls or current >= desired:
                    return
                self._workers_starting += 1
            try:
                if self._closed:
                    lease = acquire_teardown_project_threads(1, minimum=1)
                else:
                    try:
                        lease = acquire_project_threads(1, minimum=1)
                    except BaseException as public_error:
                        # A live operation may legitimately own the complete
                        # public envelope while it waits for terminal cleanup
                        # (for example, a remote staging callback).  Let that
                        # cleanup make progress through the bounded internal
                        # reserve instead of deadlocking behind its own owner.
                        clear_exception_traceback(public_error)
                        lease = acquire_teardown_project_threads(1, minimum=1)
            except BaseException as exc:
                clear_exception_traceback(exc)
                with self._condition:
                    self._decrement_counter_locked("_workers_starting")
                    self._worker_start_failures += 1
                    self._schedule_retry_locked()
                    self._condition.notify_all()
                return
            worker = threading.Thread(
                target=self._run,
                args=(lease,),
                name="schema-sanitizer-cleanup",
                daemon=True,
            )
            with self._condition:
                self._workers.add(worker)
                self._worker_leases[worker] = lease
            try:
                start_governed_thread(worker)
            except BaseException as exc:
                clear_exception_traceback(exc)
                with self._condition:
                    self._workers.discard(worker)
                    self._worker_leases.pop(worker, None)
                    self._decrement_counter_locked("_workers_starting")
                    self._worker_start_failures += 1
                    self._schedule_retry_locked()
                    self._condition.notify_all()
                try:
                    lease.release()
                except BaseException:
                    try:
                        adopted = adopt_failed_release(lease, retained_bytes=256)
                    except BaseException:
                        adopted = False
                    if not adopted:
                        with self._condition:
                            self._retain_failed_worker_lease_locked(lease)
                            self._schedule_retry_locked()
                return
            else:
                with self._condition:
                    self._decrement_counter_locked("_workers_starting")
                    self._retry_scheduled = False
                    self._condition.notify_all()
                self._unregister_availability()

    def _retry_start(self) -> None:
        self._unregister_availability()
        with self._condition:
            self._retry_scheduled = False
            failed = self._take_failed_worker_leases_locked()
        retry: deque[Any] = deque()
        while failed:
            lease = failed.popleft()
            try:
                lease.release()
            except BaseException:
                try:
                    adopted = adopt_failed_release(lease, retained_bytes=256)
                except BaseException:
                    adopted = False
                if not adopted:
                    retry.append(lease)
        if retry:
            with self._condition:
                existing = self._take_failed_worker_leases_locked()
                retry.extend(existing)
                while retry:
                    self._retain_failed_worker_lease_locked(retry.popleft())
        self._ensure_workers()
        with self._condition:
            if (
                (self._owned_calls or self._has_failed_worker_leases_locked())
                and not self._workers
                and not self._workers_starting
                and not self._closed
            ):
                self._schedule_retry_locked()

    def _take_call_locked(self) -> _CleanupCall | None:
        self._promote_delayed_locked(force=self._closed)
        visits = 0
        max_visits = max(1, len(self._ready_subsystems) * 64)
        while self._ready_subsystems and visits < max_visits:
            visits += 1
            subsystem = self._ready_subsystems.popleft()
            queue = self._queues.get(subsystem)
            if not queue:
                self._queues.pop(subsystem, None)
                self._deficits.pop(subsystem, None)
                continue
            deficit = self._deficits.get(subsystem, 0) + _DRR_QUANTUM_BYTES
            call = queue[0]
            if call.retained_bytes > deficit:
                self._deficits[subsystem] = deficit
                self._ready_subsystems.append(subsystem)
                continue
            queue.popleft()
            self._deficits[subsystem] = deficit - call.retained_bytes
            if queue:
                self._ready_subsystems.append(subsystem)
            else:
                self._queues.pop(subsystem, None)
                self._deficits.pop(subsystem, None)
            self._decrement_counter_locked("_queued_calls")
            self._decrement_counter_locked("_queued_bytes", call.retained_bytes)
            call.state = _CleanupState.ACTIVE
            return call
        return None

    def _publish_terminal_call_locked(self, call: _CleanupCall) -> None:
        """Publish terminal metadata without duplicating payload ownership."""
        if globals().get("_DISPATCHER") is self:
            publish_terminal_owner(
                "cleanup_dispatcher",
                id(call),
                retained_bytes=call.retained_bytes,
            )

    def _run(self, lease: Any) -> None:
        current = threading.current_thread()
        try:
            while True:
                with self._condition:
                    call = self._take_call_locked()
                    if call is None:
                        if self._closed or not (self._queued_calls or self._delayed_calls):
                            return
                        timeout = _IDLE_SECONDS
                        earliest = 0
                        for delayed in self._owned_index.values():
                            if delayed.state is _CleanupState.DELAYED and (
                                earliest == 0 or delayed.next_attempt_ns < earliest
                            ):
                                earliest = delayed.next_attempt_ns
                        if earliest:
                            timeout = min(
                                timeout,
                                max(0.001, (earliest - time.monotonic_ns()) / 1_000_000_000),
                            )
                        self._condition.wait(timeout=timeout)
                        continue
                    self._active_calls += 1
                    self._active_bytes += call.retained_bytes
                    self._active_reserved_bytes += call.reserved_bytes
                    started_ns = time.monotonic_ns()
                    self._active_started_ns[id(call)] = started_ns
                    self._oldest_active_ns = min(self._active_started_ns.values(), default=0)
                    self._mark_progress_locked()
                success = False
                failure = ""
                try:
                    call.callback(*call.args)
                    success = True
                except BaseException as exc:
                    failure = safe_exception_summary(exc, max_chars=512)
                    clear_exception_traceback(exc)
                payload_to_drop: tuple[Callable[..., None], tuple[Any, ...]] | None = None
                control_ticket_to_release: ControlPlaneTicket | None = None
                with self._condition:
                    self._decrement_counter_locked("_active_calls")
                    self._decrement_counter_locked("_active_bytes", call.retained_bytes)
                    self._decrement_counter_locked("_active_reserved_bytes", call.reserved_bytes)
                    self._active_started_ns.pop(id(call), None)
                    self._oldest_active_ns = min(self._active_started_ns.values(), default=0)
                    if (
                        self._circuit_open
                        and not self._corrupted
                        and self._active_calls < _MAX_WORKERS
                    ):
                        self._circuit_open = False
                    if success:
                        control_ticket_to_release = self._uncharge_owner_locked(call)
                        call.state = _CleanupState.FINISHED
                        payload_to_drop = (call.callback, call.args)
                        call.callback = lambda *_args: None
                        call.args = ()
                    else:
                        call.attempts += 1
                        if not call.first_failure:
                            call.first_failure = failure
                        call.last_failure = failure
                        if call.attempts >= _MAX_CLEANUP_ATTEMPTS:
                            if (
                                self._dead_letter_count < _MAX_DEAD_LETTER_CALLS
                                and call.retained_bytes
                                <= _MAX_DEAD_LETTER_BYTES - self._dead_letter_bytes
                            ):
                                index = self._dead_letter_count
                                next_dead_count = index + 1
                                next_dead_bytes = self._dead_letter_bytes + call.retained_bytes
                                self._dead_letters[index] = call
                                call.state = _CleanupState.DEAD_LETTER
                                self._dead_letter_count = next_dead_count
                                self._dead_letter_bytes = next_dead_bytes
                                self._publish_terminal_call_locked(call)
                            else:
                                call.parked = True
                                call.next_attempt_ns = _MAX_DEADLINE_NS
                                call.state = _CleanupState.PARKED
                                self._parked_calls += 1
                                self._publish_terminal_call_locked(call)
                        else:
                            delay = min(
                                _MAX_RETRY_SECONDS,
                                0.05 * (2 ** min(call.attempts, 9)),
                            )
                            call.next_attempt_ns = min(
                                _MAX_DEADLINE_NS,
                                time.monotonic_ns() + int(delay * 1_000_000_000),
                            )
                            self._enqueue_delayed_locked(call)
                    self._mark_progress_locked()
                    self._condition.notify_all()
                if control_ticket_to_release is not None:
                    release_control_plane(control_ticket_to_release)
                del payload_to_drop
        finally:
            with self._condition:
                owned_lease = self._worker_leases.get(current, lease)
                self._retiring_workers[current] = owned_lease
                needs_retry = bool((self._queued_calls or self._delayed_calls) and not self._closed)
                self._condition.notify_all()
            retired = defer_governed_thread_retirement(current, owned_lease.release)
            if not retired:
                with self._condition:
                    self._retain_failed_worker_lease_locked(owned_lease)
                    self._schedule_retry_locked()
            with self._condition:
                self._worker_leases.pop(current, None)
                self._retiring_workers.pop(current, None)
                self._workers.discard(current)
                self._condition.notify_all()
            if needs_retry:
                self._ensure_workers()

    def close(self, *, deadline_seconds: float = 1.0) -> bool:
        deadline = deadline_ns_from_timeout(
            deadline_seconds, name="cleanup dispatcher shutdown deadline"
        )
        with self._condition:
            self._closed = True
            self._promote_delayed_locked(force=True)
            self._condition.notify_all()
        cancel_retry(("cleanup-dispatcher", self._pid))
        self._unregister_availability()
        # Explicitly retry failed permit owners during close; normal scheduled
        # retries are intentionally disabled once _closed is set.
        self._retry_start()
        self._ensure_workers()
        while time.monotonic_ns() < deadline:
            self._retry_start()
            self._ensure_workers()
            with self._condition:
                workers = tuple(
                    worker
                    for worker in self._workers
                    if worker is not threading.current_thread() and worker.is_alive()
                )
                resources_drained = not (
                    self._owned_calls
                    or self._active_calls
                    or self._dead_letter_count
                    or self._parked_calls
                    or self._has_failed_worker_leases_locked()
                )
                workers_stopped = not workers and self._workers_starting == 0
                workers_stopped = workers_stopped and not self._retiring_workers
                if resources_drained and workers_stopped:
                    return self._protocol_violations == 0
                self._condition.notify_all()
            for worker in workers:
                worker.join(timeout=min(0.01, remaining_seconds(deadline)))
            with self._condition:
                self._condition.wait(timeout=min(0.01, remaining_seconds(deadline)))
        with self._condition:
            return not (
                self._owned_calls
                or self._active_calls
                or self._dead_letter_count
                or self._parked_calls
                or self._workers_starting
                or any(worker.is_alive() for worker in self._workers)
                or self._worker_leases
                or self._retiring_workers
                or self._has_failed_worker_leases_locked()
                or self._protocol_violations
            )

    def snapshot(self) -> CleanupDispatcherSnapshot:
        self._ensure_process()
        with self._condition:
            return CleanupDispatcherSnapshot(
                self._queued_calls + self._delayed_calls,
                self._queued_bytes + self._delayed_bytes,
                self._active_calls,
                self._active_bytes,
                sum(worker.is_alive() for worker in self._workers),
                self._workers_starting,
                self._worker_start_failures,
                self._rejected_calls,
                self._rejected_bytes,
                len(self._failed_worker_leases)
                + int(self._terminal_failed_worker_lease is not None),
                len(self._queues),
                self._progress_epoch,
                self._closed,
                self._dead_letter_count,
                self._dead_letter_bytes,
                self._parked_calls,
                min(
                    (
                        call.next_attempt_ns
                        for call in self._owned_index.values()
                        if call.state is _CleanupState.DELAYED
                    ),
                    default=0,
                ),
                self._delayed_calls,
                self._delayed_bytes,
                self._owned_calls,
                self._owned_bytes,
                self._circuit_open,
                self._oldest_active_ns,
                len(self._retiring_workers),
                self._owned_reserved_bytes,
                self._active_reserved_bytes,
                self._rejected_hidden_owner_calls,
                self._protocol_violations,
            )


_DISPATCHER = _CleanupDispatcher()


def dispatch_cleanup(
    callback: Callable[..., None],
    *args: Any,
    retained_bytes: int = _DEFAULT_CALL_CHARGE,
    reserved_bytes: int = 0,
    start_worker: bool = True,
    subsystem: CleanupSubsystem = CleanupSubsystem.GENERIC,
) -> bool:
    """Submit bounded cleanup work.

    ``retained_bytes`` is queue backpressure for memory kept alive by the call.
    ``reserved_bytes`` is diagnostic-only ownership that is already charged to
    an operation/process ledger and must therefore never be charged twice.
    ``start_worker=False`` is reserved for finalizer-safe publication: it may
    enqueue ownership but never acquires a thread permit or starts a worker.
    """
    return _DISPATCHER.submit(
        callback,
        *args,
        retained_bytes=retained_bytes,
        reserved_bytes=reserved_bytes,
        start_worker=start_worker,
        subsystem=subsystem,
    )


def cleanup_dispatcher_snapshot() -> CleanupDispatcherSnapshot:
    """Return current cleanup-dispatcher resource diagnostics."""
    return _DISPATCHER.snapshot()


def _reset_cleanup_after_fork() -> None:
    global _CLEANUP_RETRY_OWNERS_LOCK, _CLEANUP_RETRY_OWNERS
    from .fork_safety import fork_quarantine_generation

    if fork_quarantine_generation() > 1:
        return
    quarantine_inherited_state("cleanup-dispatcher", _DISPATCHER.__dict__)
    _DISPATCHER._reset(os.getpid(), cancel_old=False)
    _CLEANUP_RETRY_OWNERS_LOCK = threading.Lock()
    _CLEANUP_RETRY_OWNERS = {}


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("cleanup-dispatcher", mode="quarantine_only")


__all__ = [
    "CleanupDispatcherSnapshot",
    "CleanupSubsystem",
    "cleanup_dispatcher_snapshot",
    "dispatch_cleanup",
]
