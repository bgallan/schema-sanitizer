"""Governed process-wide delayed retries without one thread per timer."""

from __future__ import annotations

import heapq
import math
import os
import random
import sys
import threading
import weakref
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from time import monotonic_ns
from typing import Any, Hashable, cast

from .diagnostic_epoch import diagnostic_transition
from .durations import deadline_ns_from_timeout, normalize_duration, remaining_seconds
from .fork_safety import ensure_runtime_fork_safe, quarantine_inherited_state
from .process_resources import (
    AvailabilityEvent,
    acquire_project_threads,
    acquire_release_guardian_thread,
    acquire_teardown_project_threads,
    is_release_guardian_thread_lease,
    register_project_thread_availability,
    unregister_project_thread_availability,
)
from .safe_errors import clear_exception_traceback, safe_exception_summary

_MAX_PENDING_RETRIES = 8192
_MAX_PENDING_BYTES = 32 * 1024 * 1024
_MAX_READY_RETRIES = 1024
_MAX_EXECUTION_WORKERS = 2
_MAX_SUBSYSTEM_RETRIES = 4096
_MAX_SUBSYSTEM_BYTES = 16 * 1024 * 1024
_MAX_EMERGENCY_RETRIES = 256
_MAX_EMERGENCY_BYTES = 2 * 1024 * 1024
_DEFAULT_RETAINED_BYTES = 512
_IDLE_SECONDS = 30.0
_HEAP_COMPACT_MIN = 64
_READY_COMPACT_MIN = 64
_MAX_GUARDED_RELEASES = 8192
_MAX_GUARDED_RELEASE_BYTES = 8 * 1024 * 1024
_MAX_RELEASE_GUARDIAN_WORKERS = 2
_RELEASE_RETRY_MAX_SECONDS = 1.0
_RELEASE_MAX_ATTEMPTS = 16
_MAX_DEAD_LETTERS = 256
_MAX_DEAD_LETTER_BYTES = 512 * 1024
_ADMISSION_HIGH_WATERMARK = 0.90
_ADMISSION_LOW_WATERMARK = 0.70
_MAX_DEADLINE_NS = (1 << 63) - 1
_MAX_FAILED_WORKER_LEASES = _MAX_EXECUTION_WORKERS + 1
_FORKED_RETRY_KEEPALIVE: list[tuple[object, ...]] = []
_FORKED_RETRY_GENERATIONS = 0
_SHUTDOWN_POLL_SECONDS = 0.01


class _RetryItemState(Enum):
    PENDING = auto()
    READY = auto()
    CLAIMED = auto()
    RUNNING = auto()
    SUCCESSOR = auto()
    CANCELLED = auto()
    FINISHED = auto()


class _LifecycleState(Enum):
    NEW = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()
    FAILED = auto()


class _GuardedReleaseState(Enum):
    READY = auto()
    ACTIVE = auto()
    DELAYED = auto()
    DEAD_LETTER = auto()
    PARKED = auto()
    RELEASED = auto()


def _noop() -> None:
    return None


class _StrongIdentityRetryKey:
    """Identity token for the exact tiny builtin ``object`` sentinel type."""

    __slots__ = ("owner", "owner_id", "_hash")

    def __init__(self, owner: object) -> None:
        self.owner = owner
        self.owner_id = id(owner)
        self._hash = hash(("builtin-object-identity", self.owner_id))

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _StrongIdentityRetryKey) and self.owner is other.owner


class _IdentityRetryKey:
    """Weak identity key that never retains or hashes user objects."""

    __slots__ = ("owner_ref", "owner_id", "type_id", "_hash")

    def __init__(self, owner: object) -> None:
        try:
            owner_ref = weakref.ref(owner)
        except TypeError as exc:
            raise TypeError(
                "custom retry keys must support weak references; use an immutable primitive key"
            ) from exc
        self.owner_ref = owner_ref
        self.owner_id = id(owner)
        self.type_id = id(type(owner))
        self._hash = hash(("identity", self.owner_id, self.type_id))

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _IdentityRetryKey):
            return False
        left = self.owner_ref()
        right = other.owner_ref()
        return left is not None and left is right


_MAX_RETRY_KEY_DEPTH = 16
_MAX_RETRY_KEY_ELEMENTS = 256
_MAX_RETRY_KEY_BYTES = 64 * 1024


def _normalize_retry_key(
    key: object,
    *,
    _depth: int = 0,
    _budget: list[int] | None = None,
) -> Hashable:
    """Build a type-tagged, bounded key without user hash/equality hooks."""
    if _depth > _MAX_RETRY_KEY_DEPTH:
        raise ValueError("retry key nesting exceeds the maximum depth")
    budget = [0, 0] if _budget is None else _budget
    budget[0] += 1
    if budget[0] > _MAX_RETRY_KEY_ELEMENTS:
        raise ValueError("retry key contains too many elements")
    kind = type(key)
    if kind is type(None):
        return ("none",)
    if kind is bool:
        return ("bool", key)
    if kind is int:
        integer_key = cast(int, key)
        budget[1] += max(1, (abs(integer_key).bit_length() + 7) // 8)
        if budget[1] > _MAX_RETRY_KEY_BYTES:
            raise ValueError("retry key exceeds the metadata budget")
        return ("int", integer_key)
    if kind is float:
        float_key = cast(float, key)
        if not math.isfinite(float_key):
            raise ValueError("retry key floats must be finite")
        return ("float", float_key.hex())
    if kind is str:
        string_key = cast(str, key)
        budget[1] += len(string_key.encode("utf-8", errors="surrogatepass"))
        if budget[1] > _MAX_RETRY_KEY_BYTES:
            raise ValueError("retry key exceeds the metadata budget")
        return ("str", string_key)
    if kind is bytes:
        bytes_key = cast(bytes, key)
        budget[1] += len(bytes_key)
        if budget[1] > _MAX_RETRY_KEY_BYTES:
            raise ValueError("retry key exceeds the metadata budget")
        return ("bytes", bytes_key)
    if kind is tuple:
        tuple_key = cast(tuple[object, ...], key)
        return (
            "tuple",
            tuple(
                _normalize_retry_key(part, _depth=_depth + 1, _budget=budget) for part in tuple_key
            ),
        )
    if kind is object:
        budget[1] += 32
        if budget[1] > _MAX_RETRY_KEY_BYTES:
            raise ValueError("retry key exceeds the metadata budget")
        return _StrongIdentityRetryKey(key)
    return _IdentityRetryKey(key)


def _subsystem_for(key: Hashable) -> Hashable:
    if isinstance(key, tuple) and len(key) == 2 and key[0] == "tuple":
        parts = key[1]
        if isinstance(parts, tuple) and parts:
            return parts[0]
    if isinstance(key, tuple) and key:
        return key[0]
    return type(key)


@dataclass(order=True, slots=True)
class _ScheduledRetry:
    deadline_ns: int
    sequence: int
    key: Hashable = field(compare=False)
    token: int = field(compare=False)
    subsystem: Hashable = field(compare=False)
    callback: Callable[[], None] = field(compare=False)
    retained_bytes: int = field(compare=False)
    state: _RetryItemState = field(default=_RetryItemState.PENDING, compare=False)
    started_ns: int = field(default=0, compare=False)

    def detach_payload(self) -> Callable[[], None]:
        """Make a tombstone while returning the user capture to its caller.

        The returned strong reference is deliberately destroyed after the scheduler
        lock is released.  Python finalizers are arbitrary code and must never run
        while queue/accounting invariants are being mutated.
        """
        callback = self.callback
        self.callback = _noop
        return callback

    def discard_payload(self) -> None:
        """Compatibility helper for already-detached/off-lock callers."""
        self.callback = _noop
        self.retained_bytes = 0


@dataclass(slots=True)
class _GuardedRelease:
    owner: Any
    method: str
    retained_bytes: int
    resource_reserved_bytes: int = 0
    attempts: int = 0
    next_attempt_ns: int = 0
    first_failure_ns: int = 0
    last_failure: str = ""
    first_failure: str = ""
    generation: int = 0
    parked: bool = False
    state: _GuardedReleaseState = _GuardedReleaseState.READY
    started_ns: int = 0


@dataclass(frozen=True, slots=True)
class ReleaseGuardianSnapshot:
    """Describe owners retained by the bounded release guardian."""

    pending_owners: int
    retained_bytes: int
    active_releases: int
    active_workers: int
    worker_start_failures: int
    rejected_owners: int
    rejected_bytes: int
    dead_letter_owners: int = 0
    dead_letter_bytes: int = 0
    stale_generation_drops: int = 0
    last_progress_ns: int = 0
    parked_owners: int = 0
    parked_bytes: int = 0
    generation_entries: int = 0
    lifecycle_state: str = "RUNNING"
    progress_epoch: int = 0
    oldest_failure_ns: int = 0
    resource_reserved_bytes: int = 0
    failed_worker_leases: int = 0
    circuit_open: bool = False
    oldest_active_ns: int = 0
    retiring_workers: int = 0
    active_owner_keys: int = 0


def _trusted_resource_reserved_bytes(owner: Any) -> int:
    """Read diagnostics only from exact internal lease classes already loaded."""
    trusted_types: tuple[tuple[str, str, str], ...] = (
        ("schema_sanitizer.core_impl.process_resources", "_Lease", "amount"),
        ("schema_sanitizer.core_impl.temporary_storage", "TemporaryStorageLease", "reserved_bytes"),
        (
            "schema_sanitizer.core_impl.temporary_storage",
            "StreamingStorageReservation",
            "reserved_bytes",
        ),
        ("schema_sanitizer.core_impl.memory_budget", "OperationMemoryLease", "reserved_bytes"),
        (
            "schema_sanitizer.core_impl.cross_process_memory",
            "CrossProcessMemoryLease",
            "reserved_bytes",
        ),
    )
    owner_type = type(owner)
    attribute = ""
    trusted = False
    for module_name, class_name, candidate_attribute in trusted_types:
        module = sys.modules.get(module_name)
        expected = getattr(module, class_name, None) if module is not None else None
        if expected is not None and owner_type is expected:
            trusted = True
            attribute = candidate_attribute
            break
    if not trusted:
        return 0
    try:
        value = getattr(owner, attribute)
    except BaseException as exc:
        clear_exception_traceback(exc)
        return 0
    return max(0, value) if type(value) is int else 0


class _ReleaseGuardian:
    """Bounded, governed and lifecycle-deduplicated failed-release owner."""

    def __init__(self) -> None:
        self._reset(os.getpid())

    def _reset(self, pid: int) -> None:
        self._pid = pid
        self._condition = threading.Condition()
        self._items: dict[int, _GuardedRelease] = {}
        self._owner_index: dict[int, _GuardedRelease] = {}
        self._generations: dict[int, int] = {}
        self._generation_sequence = 0
        self._dead_letters: deque[_GuardedRelease] = deque()
        self._dead_letter_bytes = 0
        self._order: deque[int] = deque()
        self._retained_bytes = 0
        self._active_releases = 0
        self._workers: set[threading.Thread] = set()
        self._worker_leases: dict[threading.Thread, Any] = {}
        self._retiring_workers: dict[threading.Thread, Any] = {}
        self._failed_worker_leases: deque[Any] = deque()
        self._workers_starting = 0
        self._worker_start_failures = 0
        self._rejected_owners = 0
        self._rejected_bytes = 0
        self._duplicate_owner_rejections = 0
        self._state = _LifecycleState.RUNNING
        self._last_progress_ns = monotonic_ns()
        self._progress_epoch = 0
        self._stale_generation_drops = 0
        self._oldest_active_ns = 0
        self._circuit_open = False
        self._active_owner_keys: set[int] = set()

    def _ensure_process(self) -> None:
        if self._pid != os.getpid():
            self._reset(os.getpid())

    def _mark_progress_locked(self) -> None:
        self._last_progress_ns = monotonic_ns()
        self._progress_epoch += 1
        diagnostic_transition()

    def adopt(self, owner: Any, *, method: str = "release", retained_bytes: int = 256) -> bool:
        self._ensure_process()
        ensure_runtime_fork_safe()
        if type(method) is not str or not method:
            raise TypeError("guardian release method must be a non-empty exact string")
        if isinstance(retained_bytes, bool) or not isinstance(retained_bytes, int):
            raise TypeError("guardian retained_bytes must be an integer")
        charge = max(1, retained_bytes)
        if is_release_guardian_thread_lease(owner):
            # Releasing the bootstrap permits through this guardian creates a
            # circular dependency when every permit is retained. The exact
            # worker lease is retried synchronously by its owning guardian.
            return False
        # All dynamic metadata is resolved before acquiring the guardian lock.
        resource_reserved_bytes = _trusted_resource_reserved_bytes(owner)
        owner_id = id(owner)
        with self._condition:
            if self._state is not _LifecycleState.RUNNING or self._circuit_open:
                return False
            if (
                self._active_releases >= _MAX_RELEASE_GUARDIAN_WORKERS
                and self._oldest_active_ns
                and monotonic_ns() - self._oldest_active_ns >= 60_000_000_000
            ):
                self._circuit_open = True
                self._rejected_owners += 1
                self._mark_progress_locked()
                return False
            existing = self._owner_index.get(owner_id)
            if existing is not None:
                if existing.owner is owner and existing.method == method:
                    return True
                self._duplicate_owner_rejections += 1
                return False
            if len(self._owner_index) >= _MAX_GUARDED_RELEASES:
                self._rejected_owners += 1
                return False
            if charge > _MAX_GUARDED_RELEASE_BYTES - self._retained_bytes:
                self._rejected_bytes += charge
                return False
            self._generation_sequence += 1
            generation = self._generation_sequence
            item = _GuardedRelease(
                owner,
                method,
                charge,
                resource_reserved_bytes=resource_reserved_bytes,
                generation=generation,
            )
            self._owner_index[owner_id] = item
            self._generations[owner_id] = generation
            self._items[owner_id] = item
            self._order.append(owner_id)
            self._retained_bytes += charge
            self._mark_progress_locked()
            self._condition.notify_all()
        self._ensure_workers()
        return True

    def _retain_failed_worker_lease(self, lease: Any) -> None:
        with self._condition:
            if any(existing is lease for existing in self._failed_worker_leases):
                return
            if len(self._failed_worker_leases) < _MAX_RELEASE_GUARDIAN_WORKERS:
                self._failed_worker_leases.append(lease)
            else:
                self._rejected_owners += 1
                raise RuntimeError("release guardian worker-lease invariant exceeded")
            self._condition.notify_all()

    def _release_worker_lease(self, lease: Any) -> None:
        """Release a bootstrap permit without recursively adopting into self."""
        try:
            lease.release()
            return
        except BaseException as exc:
            clear_exception_traceback(exc)
        self._retain_failed_worker_lease(lease)

    def _drain_failed_worker_leases_once(self) -> None:
        """Retry exact worker permits without recursively adopting into self."""
        with self._condition:
            pending = tuple(self._failed_worker_leases)
            self._failed_worker_leases.clear()
        retry: list[Any] = []
        for lease in pending:
            try:
                lease.release()
            except BaseException as exc:
                clear_exception_traceback(exc)
                retry.append(lease)
        if retry:
            with self._condition:
                for lease in retry:
                    if not any(existing is lease for existing in self._failed_worker_leases):
                        self._failed_worker_leases.append(lease)
                self._condition.notify_all()

    def _ensure_workers(self) -> None:
        # Bootstrap permits are never work items for this guardian. Retry them
        # synchronously before deciding whether another worker can start.
        self._drain_failed_worker_leases_once()
        starts = 0
        with self._condition:
            if self._state not in (_LifecycleState.RUNNING, _LifecycleState.STOPPING):
                return
            runnable = sum(
                item.state in (_GuardedReleaseState.READY, _GuardedReleaseState.DELAYED)
                for item in self._items.values()
            )
            live = sum(worker.is_alive() for worker in self._workers)
            desired = min(
                _MAX_RELEASE_GUARDIAN_WORKERS,
                max(1, runnable) if runnable else 0,
            )
            starts = max(0, desired - live - self._workers_starting)
            self._workers_starting += starts
        for _index in range(starts):
            try:
                lease = acquire_release_guardian_thread()
            except BaseException as exc:
                clear_exception_traceback(exc)
                with self._condition:
                    self._workers_starting = max(0, self._workers_starting - 1)
                    self._worker_start_failures += 1
                    self._condition.notify_all()
                continue
            worker = threading.Thread(
                target=self._run,
                args=(lease,),
                name="schema-sanitizer-retry-lease-guardian",
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
                    self._condition.notify_all()
                self._release_worker_lease(lease)
            else:
                with self._condition:
                    self._workers_starting = max(0, self._workers_starting - 1)
                    self._condition.notify_all()

    def _take_ready_locked(self) -> tuple[int, _GuardedRelease] | None:
        now_ns = monotonic_ns()
        examined = len(self._order)
        earliest: int | None = None
        for _index in range(examined):
            key = self._order.popleft()
            item = self._items.get(key)
            if item is None or item.state not in (
                _GuardedReleaseState.READY,
                _GuardedReleaseState.DELAYED,
            ):
                continue
            if item.next_attempt_ns <= now_ns:
                item.state = _GuardedReleaseState.ACTIVE
                item.started_ns = now_ns
                self._active_owner_keys.add(key)
                if self._active_releases == 0:
                    self._oldest_active_ns = now_ns
                self._active_releases += 1
                return key, item
            self._order.append(key)
            earliest = (
                item.next_attempt_ns if earliest is None else min(earliest, item.next_attempt_ns)
            )
        if earliest is not None:
            self._condition.wait(
                timeout=max(
                    0.001,
                    min((earliest - now_ns) / 1_000_000_000, 0.25),
                )
            )
        return None

    def _run(self, lease: Any) -> None:
        current = threading.current_thread()
        try:
            while True:
                with self._condition:
                    if self._state in (
                        _LifecycleState.STOPPED,
                        _LifecycleState.FAILED,
                    ):
                        return
                    claimed = self._take_ready_locked()
                    if claimed is None:
                        runnable = any(
                            item.state
                            in (
                                _GuardedReleaseState.READY,
                                _GuardedReleaseState.DELAYED,
                            )
                            for item in self._items.values()
                        )
                        if not runnable:
                            # Adoption is the producer and always calls
                            # _ensure_workers(); an idle emergency worker need
                            # not retain a scarce thread permit for 30 seconds.
                            return
                        continue
                key, item = claimed
                success = False
                failure = ""
                try:
                    release = getattr(item.owner, item.method)
                    release()
                    success = True
                except BaseException as exc:
                    failure = safe_exception_summary(exc, max_chars=512)
                    clear_exception_traceback(exc)
                owner_to_drop: Any | None = None
                with self._condition:
                    self._active_releases = max(0, self._active_releases - 1)
                    self._active_owner_keys.discard(key)
                    if self._active_releases == 0:
                        self._oldest_active_ns = 0
                    current_item = self._items.get(key)
                    if current_item is not item:
                        self._condition.notify_all()
                        continue
                    if self._generations.get(key) != item.generation:
                        self._stale_generation_drops += 1
                        success = True
                    if success:
                        self._items.pop(key, None)
                        self._owner_index.pop(key, None)
                        self._generations.pop(key, None)
                        self._retained_bytes = max(0, self._retained_bytes - item.retained_bytes)
                        item.state = _GuardedReleaseState.RELEASED
                        owner_to_drop = item.owner
                        item.owner = None
                    else:
                        item.attempts += 1
                        now_ns = monotonic_ns()
                        if item.first_failure_ns == 0:
                            item.first_failure_ns = now_ns
                            item.first_failure = failure
                        item.last_failure = failure
                        if item.attempts >= _RELEASE_MAX_ATTEMPTS:
                            if (
                                len(self._dead_letters) < _MAX_DEAD_LETTERS
                                and item.retained_bytes
                                <= _MAX_DEAD_LETTER_BYTES - self._dead_letter_bytes
                            ):
                                self._items.pop(key, None)
                                self._generations.pop(key, None)
                                item.state = _GuardedReleaseState.DEAD_LETTER
                                self._dead_letters.append(item)
                                self._dead_letter_bytes += item.retained_bytes
                            else:
                                item.parked = True
                                item.state = _GuardedReleaseState.PARKED
                                self._generations.pop(key, None)
                        else:
                            delay = min(
                                _RELEASE_RETRY_MAX_SECONDS,
                                0.01 * (2 ** min(item.attempts, 7)),
                            )
                            item.next_attempt_ns = min(
                                _MAX_DEADLINE_NS,
                                now_ns + int(delay * 1_000_000_000),
                            )
                            item.state = _GuardedReleaseState.DELAYED
                            self._order.append(key)
                    self._mark_progress_locked()
                    self._condition.notify_all()
                del owner_to_drop
        finally:
            with self._condition:
                owned_lease = self._worker_leases.pop(current, lease)
                self._retiring_workers[current] = owned_lease
                self._condition.notify_all()
            self._release_worker_lease(owned_lease)
            with self._condition:
                self._retiring_workers.pop(current, None)
                self._workers.discard(current)
                self._condition.notify_all()
            self._ensure_workers()

    def close(self, *, deadline_seconds: float = 1.0) -> bool:
        deadline = deadline_ns_from_timeout(
            deadline_seconds, name="release guardian shutdown deadline"
        )
        with self._condition:
            if self._state is _LifecycleState.STOPPED:
                return not (self._owner_index or self._dead_letters or self._failed_worker_leases)
            self._state = _LifecycleState.STOPPING
            # Shutdown drains every retryable owner immediately; it does not wait
            # for normal backoff intervals, but dead-letter/parked ownership stays
            # fail-closed and makes the result non-successful.
            for key, item in self._items.items():
                # ACTIVE owners are already executing. Re-enqueueing them here
                # allows a second worker to invoke the same release method.
                if item.state in (
                    _GuardedReleaseState.READY,
                    _GuardedReleaseState.DELAYED,
                ):
                    item.next_attempt_ns = 0
                    item.state = _GuardedReleaseState.READY
                    self._order.append(key)
            self._condition.notify_all()
        self._drain_failed_worker_leases_once()
        self._ensure_workers()
        while True:
            self._drain_failed_worker_leases_once()
            self._ensure_workers()
            with self._condition:
                live_workers = tuple(
                    worker
                    for worker in self._workers
                    if worker is not threading.current_thread() and worker.is_alive()
                )
                workers_quiescent = (
                    self._active_releases == 0
                    and self._workers_starting == 0
                    and not live_workers
                    and not self._retiring_workers
                )
                resources_drained = not (
                    self._owner_index or self._dead_letters or self._failed_worker_leases
                )
                if workers_quiescent and resources_drained:
                    self._state = _LifecycleState.STOPPED
                    self._condition.notify_all()
                    return True
                if monotonic_ns() >= deadline:
                    self._state = _LifecycleState.FAILED
                    self._condition.notify_all()
                    return False
                workers = live_workers
            for worker in workers:
                worker.join(timeout=min(0.01, remaining_seconds(deadline)))
            with self._condition:
                self._condition.wait(
                    timeout=min(_SHUTDOWN_POLL_SECONDS, remaining_seconds(deadline))
                )

    def snapshot(self) -> ReleaseGuardianSnapshot:
        self._ensure_process()
        with self._condition:
            parked = tuple(
                item for item in self._items.values() if item.state is _GuardedReleaseState.PARKED
            )
            failures = [
                item.first_failure_ns
                for item in (*self._items.values(), *self._dead_letters)
                if item.first_failure_ns
            ]
            return ReleaseGuardianSnapshot(
                len(self._items) + len(self._failed_worker_leases),
                self._retained_bytes,
                self._active_releases,
                sum(worker.is_alive() for worker in self._workers),
                self._worker_start_failures,
                self._rejected_owners + self._duplicate_owner_rejections,
                self._rejected_bytes,
                len(self._dead_letters),
                self._dead_letter_bytes,
                self._stale_generation_drops,
                self._last_progress_ns,
                len(parked),
                sum(item.retained_bytes for item in parked),
                len(self._generations),
                self._state.name,
                self._progress_epoch,
                min(failures, default=0),
                sum(
                    item.resource_reserved_bytes
                    for item in (*self._items.values(), *self._dead_letters)
                ),
                len(self._failed_worker_leases),
                self._circuit_open,
                self._oldest_active_ns,
                len(self._retiring_workers),
                len(self._active_owner_keys),
            )


_RELEASE_GUARDIAN = _ReleaseGuardian()


def adopt_failed_release(
    owner: Any,
    *,
    method: str = "release",
    retained_bytes: int = 256,
) -> bool:
    """Transfer a failed release to the bounded autonomous guardian."""
    return _RELEASE_GUARDIAN.adopt(owner, method=method, retained_bytes=retained_bytes)


def release_guardian_snapshot() -> ReleaseGuardianSnapshot:
    """Return current bounded release-guardian diagnostics."""
    return _RELEASE_GUARDIAN.snapshot()


@dataclass(frozen=True, slots=True)
class RetrySchedulerSnapshot:
    """Describe bounded delayed-retry work and worker ownership."""

    pending_retries: int
    pending_bytes: int
    active_retries: int
    active_bytes: int
    worker_alive: bool
    worker_start_failures: int
    rejected_retries: int
    rejected_bytes: int
    heap_entries: int = 0
    ready_retries: int = 0
    failed_worker_leases: int = 0
    execution_workers: int = 0
    ready_bytes: int = 0
    emergency_retries: int = 0
    emergency_bytes: int = 0
    lease_guardians: int = 0
    ready_queue_entries: int = 0
    guarded_release_owners: int = 0
    guardian_workers: int = 0
    guardian_start_failures: int = 0
    stale_generation_drops: int = 0
    admission_paused: bool = False
    last_progress_ns: int = 0
    active_keys: int = 0
    successor_retries: int = 0
    successor_bytes: int = 0
    generation_entries: int = 0
    lifecycle_state: str = "RUNNING"
    progress_epoch: int = 0
    oldest_deadline_ns: int = 0
    failed_lease_rejections: int = 0
    retiring_workers: int = 0


class _RetryScheduler:
    """Bounded keyed retry service with single-flight execution per key.

    A key can own at most one running callback and one coalesced successor.  The
    CLAIMED -> RUNNING transition and cancellation decision share the scheduler
    condition lock, closing the pass37 check-then-call race.
    """

    def __init__(self) -> None:
        self._reset(os.getpid())

    def _reset(self, pid: int) -> None:
        self._pid = pid
        self._condition = threading.Condition()
        self._heap: list[_ScheduledRetry] = []
        self._current: dict[Hashable, _ScheduledRetry] = {}
        self._ready: deque[Hashable] = deque()
        self._ready_queues: dict[Hashable, deque[_ScheduledRetry]] = {}
        self._ready_by_key: dict[Hashable, _ScheduledRetry] = {}
        self._active_by_key: dict[Hashable, _ScheduledRetry] = {}
        self._successors: dict[Hashable, _ScheduledRetry] = {}
        self._emergency: dict[Hashable, _ScheduledRetry] = {}
        self._emergency_bytes = 0
        self._successor_bytes = 0
        self._pending_bytes = 0
        self._ready_bytes = 0
        self._active_retries = 0
        self._active_bytes = 0
        self._token_sequence = 0
        self._heap_sequence = 0
        self._subsystem_counts: dict[Hashable, int] = {}
        self._subsystem_bytes: dict[Hashable, int] = {}
        self._timer_worker: threading.Thread | None = None
        self._timer_starting = False
        self._execution_workers: set[threading.Thread] = set()
        self._execution_starting = 0
        self._worker_leases: dict[threading.Thread, Any] = {}
        self._retiring_workers: dict[threading.Thread, Any] = {}
        self._failed_worker_leases: deque[Any] = deque()
        self._failed_lease_transfer_lock = threading.Lock()
        # Bounded fail-closed slot for an otherwise impossible extra failed
        # worker permit.  It prevents an invariant breach from discarding the
        # only release owner.
        self._terminal_failed_worker_lease: Any | None = None
        self._worker_start_failures = 0
        self._failed_lease_rejections = 0
        self._rejected_retries = 0
        self._rejected_bytes = 0
        self._closed = False
        self._availability_registered = False
        self._state = _LifecycleState.RUNNING
        self._key_generations: dict[Hashable, int] = {}
        self._stale_generation_drops = 0
        self._admission_paused = False
        self._last_progress_ns = monotonic_ns()
        self._progress_epoch = 0

    def _ensure_process(self) -> None:
        if self._pid != os.getpid():
            self._reset(os.getpid())

    def _mark_progress_locked(self) -> None:
        self._last_progress_ns = monotonic_ns()
        self._progress_epoch += 1
        diagnostic_transition()

    def _next_token_locked(self) -> int:
        self._token_sequence += 1
        return self._token_sequence

    @staticmethod
    def _deadline_from_delay(delay_seconds: int | float) -> int:
        return deadline_ns_from_timeout(delay_seconds, name="retry delay", allow_zero=True)

    def _drop_pending_charge_locked(self, item: _ScheduledRetry) -> None:
        self._pending_bytes = max(0, self._pending_bytes - item.retained_bytes)

    def _drop_subsystem_charge_locked(self, item: _ScheduledRetry) -> None:
        charge = item.retained_bytes
        count = self._subsystem_counts.get(item.subsystem, 0) - 1
        if count > 0:
            self._subsystem_counts[item.subsystem] = count
        else:
            self._subsystem_counts.pop(item.subsystem, None)
        total = self._subsystem_bytes.get(item.subsystem, 0) - charge
        if total > 0:
            self._subsystem_bytes[item.subsystem] = total
        else:
            self._subsystem_bytes.pop(item.subsystem, None)

    def _add_subsystem_charge_locked(self, item: _ScheduledRetry) -> None:
        self._subsystem_counts[item.subsystem] = self._subsystem_counts.get(item.subsystem, 0) + 1
        self._subsystem_bytes[item.subsystem] = (
            self._subsystem_bytes.get(item.subsystem, 0) + item.retained_bytes
        )

    @staticmethod
    def _detach_locked(
        item: _ScheduledRetry | None,
        detached: list[Callable[[], None]],
    ) -> None:
        if item is not None and item.callback is not _noop:
            item.state = _RetryItemState.FINISHED
            detached.append(item.detach_payload())

    def _has_key_locked(self, key: Hashable) -> bool:
        return any(
            key in mapping
            for mapping in (
                self._current,
                self._ready_by_key,
                self._active_by_key,
                self._successors,
                self._emergency,
            )
        )

    def _maybe_prune_generation_locked(self, key: Hashable) -> None:
        if not self._has_key_locked(key):
            self._key_generations.pop(key, None)

    def _compact_heap_locked(self, *, force: bool = False) -> None:
        live = len(self._current)
        if not force and len(self._heap) <= max(_HEAP_COMPACT_MIN, live * 2 + 16):
            return
        self._heap = [item for item in self._heap if self._current.get(item.key) is item]
        heapq.heapify(self._heap)

    def _compact_ready_locked(self, *, force: bool = False) -> None:
        live = len(self._ready_by_key)
        physical = sum(len(queue) for queue in self._ready_queues.values())
        if not force and physical <= max(_READY_COMPACT_MIN, live * 2 + 16):
            return
        rebuilt: dict[Hashable, deque[_ScheduledRetry]] = {}
        order: deque[Hashable] = deque()
        seen: set[Hashable] = set()
        for subsystem in tuple(self._ready) + tuple(self._ready_queues):
            if subsystem in seen:
                continue
            seen.add(subsystem)
            queue = self._ready_queues.get(subsystem)
            if queue is None:
                continue
            compacted = deque(item for item in queue if self._ready_by_key.get(item.key) is item)
            if compacted:
                rebuilt[subsystem] = compacted
                order.append(subsystem)
        self._ready_queues = rebuilt
        self._ready = order

    def _enqueue_ready_locked(self, item: _ScheduledRetry) -> None:
        item.state = _RetryItemState.READY
        queue = self._ready_queues.get(item.subsystem)
        if queue is None:
            queue = deque()
            self._ready_queues[item.subsystem] = queue
            self._ready.append(item.subsystem)
        queue.append(item)

    def _take_ready_locked(self) -> _ScheduledRetry | None:
        """Claim one callback fairly and publish single-flight ownership."""
        examined = len(self._ready)
        while examined > 0:
            examined -= 1
            subsystem = self._ready.popleft()
            queue = self._ready_queues.get(subsystem)
            if queue is None:
                continue
            item: _ScheduledRetry | None = None
            while queue:
                candidate = queue.popleft()
                if self._ready_by_key.get(candidate.key) is candidate:
                    item = candidate
                    break
            if queue:
                self._ready.append(subsystem)
            else:
                self._ready_queues.pop(subsystem, None)
            if item is None:
                continue
            self._ready_by_key.pop(item.key, None)
            self._ready_bytes = max(0, self._ready_bytes - item.retained_bytes)
            # A running callback for the same key must never coexist.  This can
            # only be reached by synthetic/private test manipulation; keep the
            # item ready rather than violating single-flight.
            if item.key in self._active_by_key:
                self._enqueue_ready_locked(item)
                self._ready_by_key[item.key] = item
                self._ready_bytes += item.retained_bytes
                return None
            item.state = _RetryItemState.CLAIMED
            self._active_by_key[item.key] = item
            self._active_retries += 1
            self._active_bytes += item.retained_bytes
            self._mark_progress_locked()
            return item
        return None

    def _begin_execution_locked(self, item: _ScheduledRetry) -> bool:
        """Atomically decide cancellation versus user-code execution."""
        if self._active_by_key.get(item.key) is not item:
            return False
        if item.state is not _RetryItemState.CLAIMED:
            return item.state is _RetryItemState.RUNNING
        if self._key_generations.get(item.key) != item.token:
            item.state = _RetryItemState.CANCELLED
            self._stale_generation_drops += 1
            self._mark_progress_locked()
            return False
        item.state = _RetryItemState.RUNNING
        item.started_ns = monotonic_ns()
        self._mark_progress_locked()
        return True

    def _emergency_fits_locked(self, key: Hashable, *, charge: int) -> bool:
        old = self._emergency.get(key)
        old_charge = old.retained_bytes if old is not None else 0
        if old is None and len(self._emergency) >= _MAX_EMERGENCY_RETRIES:
            return False
        return self._emergency_bytes - old_charge + charge <= _MAX_EMERGENCY_BYTES

    def _install_emergency_locked(
        self, item: _ScheduledRetry, detached: list[Callable[[], None]]
    ) -> None:
        old = self._emergency.get(item.key)
        old_charge = old.retained_bytes if old is not None else 0
        if old is not None:
            self._drop_subsystem_charge_locked(old)
        self._detach_locked(old, detached)
        item.state = _RetryItemState.PENDING
        self._emergency[item.key] = item
        self._emergency_bytes = self._emergency_bytes - old_charge + item.retained_bytes
        self._add_subsystem_charge_locked(item)

    def _promote_emergency_locked(self) -> None:
        if not self._emergency:
            return
        for key in tuple(self._emergency):
            item = self._emergency.get(key)
            if item is None or key in self._active_by_key:
                continue
            if len(self._current) >= _MAX_PENDING_RETRIES:
                break
            projected = (
                self._pending_bytes
                + self._ready_bytes
                + self._active_bytes
                + self._successor_bytes
                + self._emergency_bytes
            )
            if projected > _MAX_PENDING_BYTES:
                break
            # Charge already belongs to the emergency owner.
            self._emergency.pop(key, None)
            self._emergency_bytes = max(0, self._emergency_bytes - item.retained_bytes)
            item.state = _RetryItemState.PENDING
            self._current[key] = item
            self._pending_bytes += item.retained_bytes
            heapq.heappush(self._heap, item)
            self._mark_progress_locked()

    def _remove_scheduled_locked(self, key: Hashable, detached: list[Callable[[], None]]) -> bool:
        removed = False
        old = self._current.pop(key, None)
        if old is not None:
            removed = True
            self._drop_pending_charge_locked(old)
            self._drop_subsystem_charge_locked(old)
            self._detach_locked(old, detached)
        ready = self._ready_by_key.pop(key, None)
        if ready is not None:
            removed = True
            self._ready_bytes = max(0, self._ready_bytes - ready.retained_bytes)
            self._drop_subsystem_charge_locked(ready)
            self._detach_locked(ready, detached)
        emergency = self._emergency.pop(key, None)
        if emergency is not None:
            removed = True
            self._emergency_bytes = max(0, self._emergency_bytes - emergency.retained_bytes)
            self._drop_subsystem_charge_locked(emergency)
            self._detach_locked(emergency, detached)
        successor = self._successors.pop(key, None)
        if successor is not None:
            removed = True
            self._successor_bytes = max(0, self._successor_bytes - successor.retained_bytes)
            self._drop_subsystem_charge_locked(successor)
            self._detach_locked(successor, detached)
        return removed

    # Compatibility name retained for pass36/private tests.
    def _remove_existing_locked(self, key: Hashable, detached: list[Callable[[], None]]) -> None:
        self._remove_scheduled_locked(key, detached)

    def _install_successor_locked(
        self, item: _ScheduledRetry, detached: list[Callable[[], None]]
    ) -> None:
        old = self._successors.pop(item.key, None)
        if old is not None:
            self._successor_bytes = max(0, self._successor_bytes - old.retained_bytes)
            self._drop_subsystem_charge_locked(old)
            self._detach_locked(old, detached)
        item.state = _RetryItemState.SUCCESSOR
        self._successors[item.key] = item
        self._successor_bytes += item.retained_bytes
        self._add_subsystem_charge_locked(item)

    def _promote_successor_locked(self, key: Hashable) -> None:
        item = self._successors.pop(key, None)
        if item is None:
            self._maybe_prune_generation_locked(key)
            return
        self._successor_bytes = max(0, self._successor_bytes - item.retained_bytes)
        item.state = _RetryItemState.PENDING
        self._current[key] = item
        self._pending_bytes += item.retained_bytes
        heapq.heappush(self._heap, item)
        self._mark_progress_locked()

    def schedule(
        self,
        key: Hashable,
        callback: Callable[[], None],
        *,
        delay_seconds: float,
        retained_bytes: int = _DEFAULT_RETAINED_BYTES,
        jitter_fraction: float = 0.0,
    ) -> bool:
        self._ensure_process()
        ensure_runtime_fork_safe()
        key = _normalize_retry_key(key)
        if isinstance(retained_bytes, bool) or not isinstance(retained_bytes, int):
            raise TypeError("retry retained_bytes must be an integer")
        charge = max(1, retained_bytes)
        delay_value = normalize_duration(delay_seconds, name="retry delay", allow_zero=True)
        jitter_value = normalize_duration(
            jitter_fraction, name="retry jitter_fraction", allow_zero=True
        )
        assert delay_value is not None and jitter_value is not None
        delay = delay_value
        jitter = jitter_value
        if jitter > 1:
            raise ValueError("retry jitter_fraction must be between 0 and 1")
        if jitter:
            delay *= 1.0 + random.uniform(-jitter, jitter)
        deadline_ns = self._deadline_from_delay(delay)
        subsystem = _subsystem_for(key)
        detached: list[Callable[[], None]] = []
        accepted = False
        with self._condition:
            if self._state is not _LifecycleState.RUNNING or self._closed:
                return False
            active = self._active_by_key.get(key)
            old = self._current.get(key)
            old_ready = self._ready_by_key.get(key)
            old_emergency = self._emergency.get(key)
            old_successor = self._successors.get(key)
            replaced = tuple(
                item for item in (old, old_ready, old_emergency, old_successor) if item is not None
            )
            replaced_count = len(replaced)
            replaced_bytes = sum(item.retained_bytes for item in replaced)
            replaced_subsystem_count = sum(item.subsystem == subsystem for item in replaced)
            replaced_subsystem_bytes = sum(
                item.retained_bytes for item in replaced if item.subsystem == subsystem
            )
            live_count = (
                len(self._current)
                + len(self._ready_by_key)
                + len(self._active_by_key)
                + len(self._successors)
                + len(self._emergency)
                - replaced_count
            )
            subsystem_count = self._subsystem_counts.get(subsystem, 0) - replaced_subsystem_count
            subsystem_bytes = self._subsystem_bytes.get(subsystem, 0) - replaced_subsystem_bytes
            total_bytes = (
                self._pending_bytes
                + self._ready_bytes
                + self._active_bytes
                + self._successor_bytes
                + self._emergency_bytes
            )
            projected = total_bytes - replaced_bytes + charge
            utilization = projected / max(1, _MAX_PENDING_BYTES)
            if self._admission_paused and utilization <= _ADMISSION_LOW_WATERMARK:
                self._admission_paused = False
            elif utilization >= _ADMISSION_HIGH_WATERMARK:
                self._admission_paused = True
            over_count = (
                self._admission_paused
                or live_count >= _MAX_PENDING_RETRIES
                or subsystem_count >= _MAX_SUBSYSTEM_RETRIES
            )
            over_bytes = (
                charge > _MAX_PENDING_BYTES
                or projected > _MAX_PENDING_BYTES
                or charge > _MAX_SUBSYSTEM_BYTES
                or subsystem_bytes > _MAX_SUBSYSTEM_BYTES - charge
            )
            self._heap_sequence += 1
            token = self._next_token_locked()
            item = _ScheduledRetry(
                deadline_ns, self._heap_sequence, key, token, subsystem, callback, charge
            )
            if not over_count and not over_bytes:
                self._remove_scheduled_locked(key, detached)
                self._key_generations[key] = token
                if active is not None:
                    self._install_successor_locked(item, detached)
                else:
                    item.state = _RetryItemState.PENDING
                    self._current[key] = item
                    self._pending_bytes += charge
                    self._add_subsystem_charge_locked(item)
                    heapq.heappush(self._heap, item)
                accepted = True
            elif active is None and self._emergency_fits_locked(key, charge=charge):
                self._remove_scheduled_locked(key, detached)
                self._key_generations[key] = token
                self._install_emergency_locked(item, detached)
                accepted = True
            else:
                if over_count:
                    self._rejected_retries += 1
                if over_bytes:
                    self._rejected_bytes += charge
            if accepted:
                self._compact_heap_locked()
                self._compact_ready_locked()
                self._mark_progress_locked()
                self._condition.notify_all()
        detached.clear()
        if not accepted:
            return False
        self._ensure_workers()
        return True

    def cancel(self, key: Hashable) -> None:
        self._ensure_process()
        ensure_runtime_fork_safe()
        key = _normalize_retry_key(key)
        detached: list[Callable[[], None]] = []
        with self._condition:
            if not self._has_key_locked(key):
                return
            active = self._active_by_key.get(key)
            removed = self._remove_scheduled_locked(key, detached)
            if active is not None and active.state is _RetryItemState.CLAIMED:
                # The worker and canceller serialize on this lock.  Once RUNNING
                # is committed, cancellation is future-only; before that point the
                # generation invalidation makes the worker skip user code.
                self._key_generations[key] = self._next_token_locked()
                active.state = _RetryItemState.CANCELLED
                removed = True
            if removed:
                self._mark_progress_locked()
            self._maybe_prune_generation_locked(key)
            self._compact_heap_locked()
            self._compact_ready_locked()
            self._condition.notify_all()
        detached.clear()

    def _register_availability_locked(self) -> None:
        if not self._availability_registered:
            self._availability_registered = bool(
                register_project_thread_availability(AvailabilityEvent.RETRY_SCHEDULER)
            )

    def _unregister_availability_locked(self) -> None:
        if self._availability_registered:
            unregister_project_thread_availability(AvailabilityEvent.RETRY_SCHEDULER)
            self._availability_registered = False

    def _has_failed_worker_leases_locked(self) -> bool:
        return bool(self._failed_worker_leases or self._terminal_failed_worker_lease is not None)

    def _acquire_worker_lease(self) -> Any | None:
        with self._condition:
            if self._has_failed_worker_leases_locked():
                self._register_availability_locked()
                return None
        try:
            with self._condition:
                teardown = self._state in {_LifecycleState.STOPPING, _LifecycleState.STOPPED}
            acquire = acquire_teardown_project_threads if teardown else acquire_project_threads
            return acquire(1, minimum=1)
        except BaseException:
            with self._condition:
                self._worker_start_failures += 1
                self._register_availability_locked()
                self._condition.notify_all()
            return None

    def _adopt_failed_lease(self, lease: Any) -> None:
        if adopt_failed_release(lease, retained_bytes=256):
            return
        with self._condition:
            if any(existing is lease for existing in self._failed_worker_leases):
                return
            if self._terminal_failed_worker_lease is lease:
                return
            if len(self._failed_worker_leases) < _MAX_FAILED_WORKER_LEASES:
                self._failed_worker_leases.append(lease)
            elif self._terminal_failed_worker_lease is None:
                # Never discard the only release owner.  This slot is bounded
                # and worker admission remains blocked until it is drained.
                self._terminal_failed_worker_lease = lease
                self._failed_lease_rejections += 1
            else:
                # More unique failed leases than all possible live worker
                # permits is an internal invariant failure.  Keep the runtime
                # fail-closed and surface it immediately instead of silently
                # forgetting an owner.
                self._failed_lease_rejections += 1
                raise RuntimeError("retry worker lease ownership invariant exceeded")
            self._register_availability_locked()
            self._condition.notify_all()

    def _start_timer_worker(self) -> None:
        lease = self._acquire_worker_lease()
        if lease is None:
            with self._condition:
                self._timer_starting = False
                self._condition.notify_all()
            return
        worker = threading.Thread(
            target=self._run_timer, args=(lease,), name="schema-sanitizer-retry-timer", daemon=True
        )
        with self._condition:
            self._timer_worker = worker
            self._worker_leases[worker] = lease
        try:
            worker.start()
        except BaseException:
            with self._condition:
                if self._timer_worker is worker:
                    self._timer_worker = None
                self._timer_starting = False
                self._worker_leases.pop(worker, None)
                self._worker_start_failures += 1
                self._condition.notify_all()
            self._adopt_failed_lease(lease)
        else:
            with self._condition:
                self._timer_starting = False
                self._condition.notify_all()

    def _start_execution_worker(self) -> None:
        lease = self._acquire_worker_lease()
        if lease is None:
            with self._condition:
                self._execution_starting = max(0, self._execution_starting - 1)
                self._condition.notify_all()
            return
        worker = threading.Thread(
            target=self._run_execution,
            args=(lease,),
            name="schema-sanitizer-retry-executor",
            daemon=True,
        )
        with self._condition:
            self._execution_workers.add(worker)
            self._worker_leases[worker] = lease
        try:
            worker.start()
        except BaseException:
            with self._condition:
                self._execution_workers.discard(worker)
                self._execution_starting = max(0, self._execution_starting - 1)
                self._worker_leases.pop(worker, None)
                self._worker_start_failures += 1
                self._condition.notify_all()
            self._adopt_failed_lease(lease)
        else:
            with self._condition:
                self._execution_starting = max(0, self._execution_starting - 1)
                self._condition.notify_all()

    def _ensure_workers(self) -> None:
        self._ensure_process()
        start_timer = False
        execution_count = 0
        with self._condition:
            if self._closed or self._state is not _LifecycleState.RUNNING:
                return
            self._promote_emergency_locked()
            if self._has_failed_worker_leases_locked():
                self._register_availability_locked()
            else:
                timer = self._timer_worker
                if (
                    self._current
                    and not self._timer_starting
                    and not (timer is not None and timer.is_alive())
                ):
                    self._timer_starting = True
                    start_timer = True
                alive_execution = sum(worker.is_alive() for worker in self._execution_workers)
                needed = min(
                    _MAX_EXECUTION_WORKERS,
                    len(self._ready_by_key) + self._active_retries,
                )
                execution_count = max(0, needed - alive_execution - self._execution_starting)
                self._execution_starting += execution_count
            if start_timer or execution_count or self._has_failed_worker_leases_locked():
                self._register_availability_locked()
            elif not any((self._current, self._ready_by_key, self._emergency, self._successors)):
                self._unregister_availability_locked()
        if start_timer:
            self._start_timer_worker()
        for _index in range(execution_count):
            self._start_execution_worker()
        self._release_failed_leases()

    def _release_failed_leases(self) -> None:
        """Transfer permits one-by-one; ownership moves only after ACK."""
        with self._failed_lease_transfer_lock:
            while True:
                with self._condition:
                    if self._failed_worker_leases:
                        lease = self._failed_worker_leases[0]
                        terminal = False
                    elif self._terminal_failed_worker_lease is not None:
                        lease = self._terminal_failed_worker_lease
                        terminal = True
                    else:
                        self._unregister_availability_locked()
                        return
                try:
                    adopted = adopt_failed_release(lease, retained_bytes=256)
                except BaseException as exc:
                    clear_exception_traceback(exc)
                    adopted = False
                if not adopted:
                    with self._condition:
                        self._register_availability_locked()
                        self._condition.notify_all()
                    return
                with self._condition:
                    if terminal:
                        if self._terminal_failed_worker_lease is lease:
                            self._terminal_failed_worker_lease = None
                    else:
                        if self._failed_worker_leases and self._failed_worker_leases[0] is lease:
                            self._failed_worker_leases.popleft()
                        else:
                            try:
                                self._failed_worker_leases.remove(lease)
                            except ValueError:
                                pass
                    self._condition.notify_all()

    def _discard_stale_head_locked(self, detached: list[Callable[[], None]]) -> None:
        while self._heap:
            item = self._heap[0]
            if self._current.get(item.key) is item:
                return
            heapq.heappop(self._heap)
            self._detach_locked(item, detached)

    def _run_timer(self, lease: Any) -> None:
        current = threading.current_thread()
        try:
            while True:
                detached: list[Callable[[], None]] = []
                with self._condition:
                    self._discard_stale_head_locked(detached)
                    if self._closed or self._state in (
                        _LifecycleState.STOPPING,
                        _LifecycleState.STOPPED,
                    ):
                        return
                    if not self._heap:
                        self._condition.wait(timeout=_IDLE_SECONDS)
                        self._discard_stale_head_locked(detached)
                        if not self._heap and not self._current:
                            return
                        continue
                    item = self._heap[0]
                    remaining_ns = item.deadline_ns - monotonic_ns()
                    if remaining_ns > 0:
                        self._condition.wait(
                            timeout=min(remaining_ns / 1_000_000_000, _IDLE_SECONDS)
                        )
                        continue
                    if len(self._ready_by_key) >= _MAX_READY_RETRIES:
                        self._condition.wait(timeout=0.05)
                        continue
                    heapq.heappop(self._heap)
                    if self._current.get(item.key) is not item:
                        self._detach_locked(item, detached)
                        continue
                    self._current.pop(item.key, None)
                    self._drop_pending_charge_locked(item)
                    self._enqueue_ready_locked(item)
                    self._ready_by_key[item.key] = item
                    self._ready_bytes += item.retained_bytes
                    self._promote_emergency_locked()
                    self._mark_progress_locked()
                    self._condition.notify_all()
                detached.clear()
                self._ensure_workers()
        finally:
            self._finish_worker(current, lease, timer=True)

    def _run_execution(self, lease: Any) -> None:
        current = threading.current_thread()
        try:
            while True:
                with self._condition:
                    item = self._take_ready_locked()
                    if item is None:
                        self._condition.wait(timeout=_IDLE_SECONDS)
                        if not self._ready_by_key:
                            return
                        continue
                    should_run = self._begin_execution_locked(item)
                    self._condition.notify_all()
                try:
                    if should_run:
                        item.callback()
                except BaseException as exc:
                    try:
                        exc.__traceback__ = None
                    except BaseException:
                        pass
                finally:
                    retained_bytes = item.retained_bytes
                    callback = item.detach_payload()
                    with self._condition:
                        if self._active_by_key.get(item.key) is item:
                            self._active_by_key.pop(item.key, None)
                        self._active_retries = max(0, self._active_retries - 1)
                        self._active_bytes = max(0, self._active_bytes - retained_bytes)
                        self._drop_subsystem_charge_locked(item)
                        item.state = _RetryItemState.FINISHED
                        self._promote_successor_locked(item.key)
                        self._promote_emergency_locked()
                        self._maybe_prune_generation_locked(item.key)
                        self._mark_progress_locked()
                        self._condition.notify_all()
                    del callback
                    self._ensure_workers()
        finally:
            self._finish_worker(current, lease, timer=False)

    def _finish_worker(self, current: threading.Thread, lease: Any, *, timer: bool) -> None:
        with self._condition:
            owned_lease = self._worker_leases.get(current, lease)
            self._retiring_workers[current] = owned_lease
            self._condition.notify_all()
        try:
            owned_lease.release()
        except BaseException:
            self._adopt_failed_lease(owned_lease)
        finally:
            with self._condition:
                if timer and self._timer_worker is current:
                    self._timer_worker = None
                if not timer:
                    self._execution_workers.discard(current)
                self._worker_leases.pop(current, None)
                self._retiring_workers.pop(current, None)
                self._condition.notify_all()
        self._ensure_workers()

    def close(self, *, deadline_seconds: float = 1.0) -> bool:
        """Stop admission and wait for worker threads and their leases to retire."""
        deadline = deadline_ns_from_timeout(
            deadline_seconds, name="retry scheduler shutdown deadline"
        )
        detached: list[Callable[[], None]] = []
        with self._condition:
            if self._state is _LifecycleState.STOPPED:
                return True
            self._state = _LifecycleState.STOPPING
            self._closed = True
            keys = tuple(
                set(self._current)
                | set(self._ready_by_key)
                | set(self._emergency)
                | set(self._successors)
                | set(self._active_by_key)
            )
            for key in keys:
                self._remove_scheduled_locked(key, detached)
                active = self._active_by_key.get(key)
                if active is not None and active.state is _RetryItemState.CLAIMED:
                    self._key_generations[key] = self._next_token_locked()
                    active.state = _RetryItemState.CANCELLED
            self._compact_heap_locked(force=True)
            self._compact_ready_locked(force=True)
            self._condition.notify_all()
        detached.clear()

        # Workers release their permits in ``finally``. Join them outside the
        # scheduler lock so retirement can publish and notify progress.
        while monotonic_ns() < deadline:
            with self._condition:
                workers = tuple(
                    worker
                    for worker in (
                        *((self._timer_worker,) if self._timer_worker is not None else ()),
                        *self._execution_workers,
                    )
                    if worker is not threading.current_thread() and worker.is_alive()
                )
                quiescent = (
                    not self._active_by_key
                    and not workers
                    and not self._timer_starting
                    and self._execution_starting == 0
                    and not self._worker_leases
                    and not self._retiring_workers
                )
                if quiescent:
                    break
                self._condition.notify_all()
            for worker in workers:
                worker.join(timeout=min(_SHUTDOWN_POLL_SECONDS, remaining_seconds(deadline)))
            with self._condition:
                self._condition.wait(
                    timeout=min(_SHUTDOWN_POLL_SECONDS, remaining_seconds(deadline))
                )

        # Startup failures may still own exact permits without a worker. Transfer
        # them to the guardian while it is intentionally still running.
        try:
            self._release_failed_leases()
        except BaseException:
            pass
        with self._condition:
            live_workers = bool(
                (self._timer_worker is not None and self._timer_worker.is_alive())
                or any(worker.is_alive() for worker in self._execution_workers)
            )
            stopped = not (
                self._active_by_key
                or live_workers
                or self._timer_starting
                or self._execution_starting
                or self._worker_leases
                or self._retiring_workers
                or self._has_failed_worker_leases_locked()
            )
            self._state = _LifecycleState.STOPPED if stopped else _LifecycleState.FAILED
            for key in tuple(self._key_generations):
                self._maybe_prune_generation_locked(key)
            self._unregister_availability_locked()
            self._condition.notify_all()
            return stopped

    def snapshot(self) -> RetrySchedulerSnapshot:
        self._ensure_process()
        guardian = release_guardian_snapshot()
        with self._condition:
            timer = self._timer_worker
            execution_workers = sum(worker.is_alive() for worker in self._execution_workers)
            oldest = min(
                (
                    item.deadline_ns
                    for item in (
                        *self._current.values(),
                        *self._ready_by_key.values(),
                        *self._successors.values(),
                    )
                ),
                default=0,
            )
            return RetrySchedulerSnapshot(
                len(self._current) + len(self._emergency) + len(self._successors),
                self._pending_bytes + self._emergency_bytes + self._successor_bytes,
                self._active_retries,
                self._active_bytes,
                bool(timer is not None and timer.is_alive()),
                self._worker_start_failures,
                self._rejected_retries,
                self._rejected_bytes,
                len(self._heap),
                len(self._ready_by_key),
                len(self._failed_worker_leases)
                + int(self._terminal_failed_worker_lease is not None),
                execution_workers,
                self._ready_bytes,
                len(self._emergency),
                self._emergency_bytes,
                guardian.pending_owners,
                sum(len(queue) for queue in self._ready_queues.values()),
                guardian.pending_owners,
                guardian.active_workers,
                guardian.worker_start_failures,
                self._stale_generation_drops + guardian.stale_generation_drops,
                self._admission_paused,
                self._last_progress_ns,
                active_keys=len(self._active_by_key),
                successor_retries=len(self._successors),
                successor_bytes=self._successor_bytes,
                generation_entries=len(self._key_generations),
                lifecycle_state=self._state.name,
                progress_epoch=self._progress_epoch,
                oldest_deadline_ns=oldest,
                failed_lease_rejections=self._failed_lease_rejections,
                retiring_workers=len(self._retiring_workers),
            )


_SCHEDULER = _RetryScheduler()


def schedule_retry(
    key: Hashable,
    callback: Callable[[], None],
    *,
    delay_seconds: float,
    retained_bytes: int = _DEFAULT_RETAINED_BYTES,
    jitter_fraction: float = 0.0,
) -> bool:
    """Schedule one keyed bounded retry after the requested delay."""
    return _SCHEDULER.schedule(
        key,
        callback,
        delay_seconds=delay_seconds,
        retained_bytes=retained_bytes,
        jitter_fraction=jitter_fraction,
    )


def cancel_retry(key: Hashable) -> None:
    """Cancel pending retry work for one exact key."""
    _SCHEDULER.cancel(key)


def retry_scheduler_snapshot() -> RetrySchedulerSnapshot:
    """Return current bounded retry-scheduler diagnostics."""
    return _SCHEDULER.snapshot()


def release_guardian_dead_letter_diagnostics(*, limit: int = 32) -> tuple[dict[str, object], ...]:
    """Return bounded failure metadata without exposing or retaining owners."""
    guardian = _RELEASE_GUARDIAN
    guardian._ensure_process()
    with guardian._condition:
        items = list(guardian._dead_letters)
        items.extend(item for item in guardian._items.values() if item.parked)
        selected = items[: max(0, min(256, int(limit)))]
        return tuple(
            {
                "method": item.method,
                "retained_bytes": item.retained_bytes,
                "resource_reserved_bytes": item.resource_reserved_bytes,
                "attempts": item.attempts,
                "first_failure_ns": item.first_failure_ns,
                "first_failure": item.first_failure,
                "last_failure": item.last_failure,
                "parked": item.parked,
            }
            for item in selected
        )


def shutdown_retry_runtime(*, deadline_seconds: float = 1.0) -> bool:
    """Boundedly stop retry admission and autonomous release workers."""
    scheduler_stopped = _SCHEDULER.close(deadline_seconds=deadline_seconds)
    guardian_stopped = _RELEASE_GUARDIAN.close(deadline_seconds=deadline_seconds)
    return scheduler_stopped and guardian_stopped


def _reset_retry_runtime_after_fork() -> None:
    from .fork_safety import fork_quarantine_generation

    if fork_quarantine_generation() > 1:
        return
    # Never destroy inherited callbacks/owners in the child: their locks may
    # have been held by parent threads that no longer exist.  The supported
    # child model is fork+exec; these references are quarantined until exec.
    global _FORKED_RETRY_GENERATIONS
    quarantine_inherited_state(
        "retry-runtime",
        *(tuple(_RELEASE_GUARDIAN.__dict__.values()) + tuple(_SCHEDULER.__dict__.values())),
    )
    _FORKED_RETRY_GENERATIONS += 1
    pid = os.getpid()
    _RELEASE_GUARDIAN._reset(pid)
    _SCHEDULER._reset(pid)


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_retry_runtime_after_fork)


__all__ = [
    "ReleaseGuardianSnapshot",
    "RetrySchedulerSnapshot",
    "adopt_failed_release",
    "cancel_retry",
    "release_guardian_dead_letter_diagnostics",
    "release_guardian_snapshot",
    "retry_scheduler_snapshot",
    "schedule_retry",
    "shutdown_retry_runtime",
]
