"""Bounded process-wide execution for blocking terminal cleanup publishers."""

from __future__ import annotations

import heapq
import os
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from .diagnostic_epoch import diagnostic_transition
from .durations import deadline_ns_from_timeout, remaining_seconds
from .fork_safety import ensure_runtime_fork_safe, quarantine_inherited_state
from .process_resources import (
    AvailabilityEvent,
    acquire_project_threads,
    acquire_teardown_project_threads,
    register_project_thread_availability,
    unregister_project_thread_availability,
)
from .retry_scheduler import adopt_failed_release, cancel_retry, schedule_retry
from .safe_errors import clear_exception_traceback, safe_exception_summary

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


class CleanupSubsystem(Enum):
    """Classify cleanup work for bounded subsystem diagnostics."""

    GENERIC = auto()
    RETRY = auto()
    JANITOR = auto()
    REMOTE = auto()
    STORAGE = auto()


@dataclass(slots=True)
class _CleanupCall:
    callback: Callable[..., None]
    args: tuple[Any, ...]
    retained_bytes: int
    subsystem: CleanupSubsystem
    attempts: int = 0
    next_attempt_ns: int = 0
    first_failure: str = ""
    last_failure: str = ""
    parked: bool = False
    sequence: int = 0


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


class _CleanupDispatcher:
    """Governed cleanup with physically separate runnable and terminal states."""

    def __init__(self) -> None:
        self._reset(os.getpid())

    def _reset(self, pid: int, *, cancel_old: bool = True) -> None:
        old_pid = getattr(self, "_pid", None)
        if cancel_old and old_pid is not None:
            cancel_retry(("cleanup-dispatcher", old_pid))
        self._pid = pid
        self._condition = threading.Condition()
        self._queues: dict[CleanupSubsystem, deque[_CleanupCall]] = {}
        self._ready_subsystems: deque[CleanupSubsystem] = deque()
        self._deficits: dict[CleanupSubsystem, int] = {}
        self._delayed_heap: list[tuple[int, int, _CleanupCall]] = []
        self._delayed_calls = 0
        self._delayed_bytes = 0
        self._sequence = 0
        self._subsystem_counts: dict[CleanupSubsystem, int] = {}
        self._subsystem_bytes: dict[CleanupSubsystem, int] = {}
        self._queued_calls = 0
        self._queued_bytes = 0
        self._owned_calls = 0
        self._owned_bytes = 0
        self._active_calls = 0
        self._active_bytes = 0
        self._workers: set[threading.Thread] = set()
        self._worker_leases: dict[threading.Thread, Any] = {}
        self._retiring_workers: dict[threading.Thread, Any] = {}
        self._workers_starting = 0
        self._retry_scheduled = False
        self._availability_registered = False
        self._failed_worker_leases: deque[Any] = deque()
        self._terminal_failed_worker_lease: Any | None = None
        self._worker_start_failures = 0
        self._rejected_calls = 0
        self._rejected_bytes = 0
        self._closed = False
        self._progress_epoch = 0
        self._dead_letters: deque[_CleanupCall] = deque()
        self._dead_letter_bytes = 0
        self._parked: deque[_CleanupCall] = deque()
        self._parked_calls = 0
        self._oldest_active_ns = 0
        self._active_started_ns: dict[int, int] = {}
        self._circuit_open = False

    def _ensure_process(self) -> None:
        if self._pid != os.getpid():
            self._reset(os.getpid())

    def _mark_progress_locked(self) -> None:
        self._progress_epoch += 1
        diagnostic_transition()

    def _charge_owner_locked(self, call: _CleanupCall) -> None:
        subsystem = call.subsystem
        self._owned_calls += 1
        self._owned_bytes += call.retained_bytes
        self._subsystem_counts[subsystem] = self._subsystem_counts.get(subsystem, 0) + 1
        self._subsystem_bytes[subsystem] = (
            self._subsystem_bytes.get(subsystem, 0) + call.retained_bytes
        )

    def _uncharge_owner_locked(self, call: _CleanupCall) -> None:
        subsystem = call.subsystem
        self._owned_calls = max(0, self._owned_calls - 1)
        self._owned_bytes = max(0, self._owned_bytes - call.retained_bytes)
        count = self._subsystem_counts.get(subsystem, 0) - 1
        if count > 0:
            self._subsystem_counts[subsystem] = count
        else:
            self._subsystem_counts.pop(subsystem, None)
        total = self._subsystem_bytes.get(subsystem, 0) - call.retained_bytes
        if total > 0:
            self._subsystem_bytes[subsystem] = total
        else:
            self._subsystem_bytes.pop(subsystem, None)

    def _enqueue_runnable_locked(self, call: _CleanupCall) -> None:
        call.next_attempt_ns = 0
        subsystem = call.subsystem
        queue = self._queues.get(subsystem)
        if queue is None:
            queue = deque()
            self._queues[subsystem] = queue
            self._ready_subsystems.append(subsystem)
        queue.append(call)
        self._queued_calls += 1
        self._queued_bytes += call.retained_bytes

    def _enqueue_delayed_locked(self, call: _CleanupCall) -> None:
        self._sequence += 1
        call.sequence = self._sequence
        heapq.heappush(
            self._delayed_heap,
            (call.next_attempt_ns, call.sequence, call),
        )
        self._delayed_calls += 1
        self._delayed_bytes += call.retained_bytes

    def _promote_delayed_locked(self, *, force: bool = False) -> None:
        now = time.monotonic_ns()
        while self._delayed_heap and (force or self._delayed_heap[0][0] <= now):
            _deadline, _sequence, call = heapq.heappop(self._delayed_heap)
            self._delayed_calls = max(0, self._delayed_calls - 1)
            self._delayed_bytes = max(0, self._delayed_bytes - call.retained_bytes)
            self._enqueue_runnable_locked(call)

    def submit(
        self,
        callback: Callable[..., None],
        *args: Any,
        retained_bytes: int = _DEFAULT_CALL_CHARGE,
        subsystem: CleanupSubsystem = CleanupSubsystem.GENERIC,
    ) -> bool:
        self._ensure_process()
        ensure_runtime_fork_safe()
        if not callable(callback):
            raise TypeError("cleanup callback must be callable")
        if isinstance(retained_bytes, bool) or not isinstance(retained_bytes, int):
            raise TypeError("cleanup retained_bytes must be an integer")
        # Every call owns scheduler metadata even when the publisher claims a
        # tiny payload.  The caller-provided value may increase the charge but
        # can never understate this fixed floor.
        charge = max(_DEFAULT_CALL_CHARGE, retained_bytes)
        if not isinstance(subsystem, CleanupSubsystem):
            raise TypeError("cleanup subsystem must be CleanupSubsystem")
        call = _CleanupCall(callback, tuple(args), charge, subsystem)
        with self._condition:
            if self._closed or self._circuit_open:
                return False
            if (
                self._active_calls >= _MAX_WORKERS
                and self._oldest_active_ns
                and time.monotonic_ns() - self._oldest_active_ns >= 60_000_000_000
            ):
                self._circuit_open = True
                self._rejected_calls += 1
                self._mark_progress_locked()
                return False
            if self._owned_calls >= _MAX_PENDING_CLEANUPS:
                self._rejected_calls += 1
                return False
            if self._subsystem_counts.get(subsystem, 0) >= _MAX_SUBSYSTEM_CLEANUPS:
                self._rejected_calls += 1
                return False
            if charge > _MAX_PENDING_BYTES - self._owned_bytes:
                self._rejected_bytes += max(1, retained_bytes)
                return False
            if charge > _MAX_SUBSYSTEM_BYTES - self._subsystem_bytes.get(subsystem, 0):
                self._rejected_bytes += max(1, retained_bytes)
                return False
            self._charge_owner_locked(call)
            self._enqueue_runnable_locked(call)
            self._mark_progress_locked()
            self._condition.notify_all()
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
        self._retry_scheduled = schedule_retry(
            ("cleanup-dispatcher", self._pid),
            self._retry_start,
            delay_seconds=_RETRY_SECONDS,
            retained_bytes=512,
            jitter_fraction=0.2,
        )
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
                acquire = (
                    acquire_teardown_project_threads if self._closed else acquire_project_threads
                )
                lease = acquire(1, minimum=1)
            except BaseException as exc:
                clear_exception_traceback(exc)
                with self._condition:
                    self._workers_starting = max(0, self._workers_starting - 1)
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
                worker.start()
            except BaseException as exc:
                clear_exception_traceback(exc)
                with self._condition:
                    self._workers.discard(worker)
                    self._worker_leases.pop(worker, None)
                    self._workers_starting = max(0, self._workers_starting - 1)
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
                    self._workers_starting = max(0, self._workers_starting - 1)
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
            self._queued_calls = max(0, self._queued_calls - 1)
            self._queued_bytes = max(0, self._queued_bytes - call.retained_bytes)
            return call
        return None

    def _requeue_call_locked(self, call: _CleanupCall) -> None:
        """Compatibility helper: delayed work is never put in runnable queues."""
        if call.parked:
            self._parked.append(call)
            self._parked_calls += 1
        elif call.next_attempt_ns > time.monotonic_ns():
            self._enqueue_delayed_locked(call)
        else:
            self._enqueue_runnable_locked(call)

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
                        if self._delayed_heap:
                            timeout = min(
                                timeout,
                                max(
                                    0.001,
                                    (self._delayed_heap[0][0] - time.monotonic_ns())
                                    / 1_000_000_000,
                                ),
                            )
                        self._condition.wait(timeout=timeout)
                        continue
                    self._active_calls += 1
                    self._active_bytes += call.retained_bytes
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
                with self._condition:
                    self._active_calls = max(0, self._active_calls - 1)
                    self._active_bytes = max(0, self._active_bytes - call.retained_bytes)
                    self._active_started_ns.pop(id(call), None)
                    self._oldest_active_ns = min(self._active_started_ns.values(), default=0)
                    if self._circuit_open and self._active_calls < _MAX_WORKERS:
                        self._circuit_open = False
                    if success:
                        self._uncharge_owner_locked(call)
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
                                len(self._dead_letters) < _MAX_DEAD_LETTER_CALLS
                                and call.retained_bytes
                                <= _MAX_DEAD_LETTER_BYTES - self._dead_letter_bytes
                            ):
                                self._dead_letters.append(call)
                                self._dead_letter_bytes += call.retained_bytes
                            else:
                                call.parked = True
                                call.next_attempt_ns = _MAX_DEADLINE_NS
                                self._parked.append(call)
                                self._parked_calls += 1
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
                del payload_to_drop
        finally:
            with self._condition:
                owned_lease = self._worker_leases.get(current, lease)
                self._retiring_workers[current] = owned_lease
                needs_retry = bool((self._queued_calls or self._delayed_calls) and not self._closed)
                self._condition.notify_all()
            try:
                owned_lease.release()
            except BaseException:
                try:
                    adopted = adopt_failed_release(owned_lease, retained_bytes=256)
                except BaseException:
                    adopted = False
                if not adopted:
                    with self._condition:
                        self._retain_failed_worker_lease_locked(owned_lease)
                        self._schedule_retry_locked()
            finally:
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
                    or self._dead_letters
                    or self._parked
                    or self._has_failed_worker_leases_locked()
                )
                workers_stopped = not workers and self._workers_starting == 0
                workers_stopped = workers_stopped and not self._retiring_workers
                if resources_drained and workers_stopped:
                    return True
                self._condition.notify_all()
            for worker in workers:
                worker.join(timeout=min(0.01, remaining_seconds(deadline)))
            with self._condition:
                self._condition.wait(timeout=min(0.01, remaining_seconds(deadline)))
        with self._condition:
            return not (
                self._owned_calls
                or self._active_calls
                or self._dead_letters
                or self._parked
                or self._workers_starting
                or any(worker.is_alive() for worker in self._workers)
                or self._worker_leases
                or self._retiring_workers
                or self._has_failed_worker_leases_locked()
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
                len(self._dead_letters),
                self._dead_letter_bytes,
                len(self._parked),
                self._delayed_heap[0][0] if self._delayed_heap else 0,
                self._delayed_calls,
                self._delayed_bytes,
                self._owned_calls,
                self._owned_bytes,
                self._circuit_open,
                self._oldest_active_ns,
                len(self._retiring_workers),
            )


_DISPATCHER = _CleanupDispatcher()


def dispatch_cleanup(
    callback: Callable[..., None],
    *args: Any,
    retained_bytes: int = _DEFAULT_CALL_CHARGE,
    subsystem: CleanupSubsystem = CleanupSubsystem.GENERIC,
) -> bool:
    """Submit cleanup work to the process-wide bounded dispatcher."""
    return _DISPATCHER.submit(callback, *args, retained_bytes=retained_bytes, subsystem=subsystem)


def cleanup_dispatcher_snapshot() -> CleanupDispatcherSnapshot:
    """Return current cleanup-dispatcher resource diagnostics."""
    return _DISPATCHER.snapshot()


def _reset_cleanup_after_fork() -> None:
    from .fork_safety import fork_quarantine_generation

    if fork_quarantine_generation() > 1:
        return
    quarantine_inherited_state("cleanup-dispatcher", *tuple(_DISPATCHER.__dict__.values()))
    _DISPATCHER._reset(os.getpid(), cancel_old=False)


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_cleanup_after_fork)


__all__ = [
    "CleanupDispatcherSnapshot",
    "CleanupSubsystem",
    "cleanup_dispatcher_snapshot",
    "dispatch_cleanup",
]
