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

from .bounded_generation import BoundedGenerationPool
from .compact_callback import callback_retains_hidden_owner
from .control_plane_budget import ControlPlaneTicket, release_control_plane, reserve_control_plane
from .diagnostic_epoch import diagnostic_transition
from .durations import deadline_ns_from_timeout, normalize_duration, remaining_seconds
from .fork_safety import ensure_runtime_fork_safe, quarantine_inherited_state
from .governed_thread import defer_governed_thread_retirement, start_governed_thread
from .process_resources import (
    AvailabilityEvent,
    acquire_project_threads,
    acquire_release_guardian_thread,
    acquire_teardown_project_threads,
    is_project_thread_lease,
    is_release_guardian_thread_lease,
    register_project_thread_availability,
    unregister_project_thread_availability,
)
from .safe_errors import clear_exception_traceback, safe_exception_summary
from .terminal_ownership import publish_terminal_owner, retire_terminal_category

_ORIGINAL_HEAPPUSH = heapq.heappush

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
    control_ticket: ControlPlaneTicket | None = field(default=None, compare=False)
    deadline_slot: int = field(default=-1, compare=False)

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
    control_ticket: ControlPlaneTicket | None = None
    # Primary owner.release() has committed; only the control-plane ticket may
    # remain.  Retry workers must never invoke user/resource code again.
    resource_released: bool = False


class _BoundedDeadlineIndex:
    """Preallocated indexed min-heap: one physical node per logical retry.

    The backing list never grows after construction. Every item stores its heap
    index in ``deadline_slot``, so replace/remove are O(log n), peek is O(1),
    and stale historical nodes are structurally impossible.
    """

    __slots__ = ("_slots", "_count", "_capacity")

    def __init__(self, capacity: int) -> None:
        self._slots: list[_ScheduledRetry | None] = [None] * capacity
        self._count = 0
        self._capacity = capacity

    def __len__(self) -> int:
        return self._count

    def __bool__(self) -> bool:
        return self._count != 0

    def __iter__(self):
        for index in range(self._count):
            item = self._slots[index]
            if item is not None:
                yield item

    def __getitem__(self, index: int) -> _ScheduledRetry:
        if index < 0 or index >= self._count:
            raise IndexError(index)
        item = self._slots[index]
        if item is None:
            raise RuntimeError("retry deadline heap contains an empty live slot")
        return item

    @staticmethod
    def _earlier(left: _ScheduledRetry, right: _ScheduledRetry) -> bool:
        if left.deadline_ns != right.deadline_ns:
            return left.deadline_ns < right.deadline_ns
        return left.sequence < right.sequence

    def _swap(self, left: int, right: int) -> None:
        a = self._slots[left]
        b = self._slots[right]
        if a is None or b is None:
            raise RuntimeError("retry deadline heap swap lost owner")
        self._slots[left], self._slots[right] = b, a
        b.deadline_slot = left
        a.deadline_slot = right

    def _sift_up(self, index: int) -> int:
        while index > 0:
            parent = (index - 1) // 2
            current = self._slots[index]
            ancestor = self._slots[parent]
            if current is None or ancestor is None:
                raise RuntimeError("retry deadline heap owner invariant violated")
            if not self._earlier(current, ancestor):
                break
            self._swap(index, parent)
            index = parent
        return index

    def _sift_down(self, index: int) -> int:
        count = self._count
        while True:
            left = index * 2 + 1
            if left >= count:
                return index
            right = left + 1
            best = left
            left_item = self._slots[left]
            if left_item is None:
                raise RuntimeError("retry deadline heap owner invariant violated")
            if right < count:
                right_item = self._slots[right]
                if right_item is None:
                    raise RuntimeError("retry deadline heap owner invariant violated")
                if self._earlier(right_item, left_item):
                    best = right
            current = self._slots[index]
            child = self._slots[best]
            if current is None or child is None:
                raise RuntimeError("retry deadline heap owner invariant violated")
            if not self._earlier(child, current):
                return index
            self._swap(index, best)
            index = best

    def peek_min(self) -> _ScheduledRetry | None:
        if self._count == 0:
            return None
        item = self._slots[0]
        if item is None:
            raise RuntimeError("retry deadline heap root invariant violated")
        return item

    def insert(self, item: _ScheduledRetry) -> None:
        if item.deadline_slot >= 0:
            raise RuntimeError("retry deadline owner already indexed")
        if self._count >= self._capacity:
            raise RuntimeError("retry deadline index capacity exhausted")
        # Keep the historical fault-injection seam entirely before commit.
        if heapq.heappush is not _ORIGINAL_HEAPPUSH:
            scratch: list[_ScheduledRetry] = []
            heapq.heappush(scratch, item)
        index = self._count
        self._slots[index] = item
        item.deadline_slot = index
        self._count = index + 1
        self._sift_up(index)

    def replace(self, old: _ScheduledRetry, new: _ScheduledRetry) -> None:
        index = old.deadline_slot
        if index < 0 or index >= self._count or self._slots[index] is not old:
            raise RuntimeError("retry deadline replacement lost source owner")
        if new.deadline_slot >= 0:
            raise RuntimeError("retry deadline replacement target already indexed")
        if heapq.heappush is not _ORIGINAL_HEAPPUSH:
            scratch: list[_ScheduledRetry] = []
            heapq.heappush(scratch, new)
        self._slots[index] = new
        new.deadline_slot = index
        old.deadline_slot = -1
        parent = (index - 1) // 2 if index else -1
        if parent >= 0:
            ancestor = self._slots[parent]
            if ancestor is not None and self._earlier(new, ancestor):
                self._sift_up(index)
                return
        self._sift_down(index)

    def remove(self, item: _ScheduledRetry) -> bool:
        index = item.deadline_slot
        if index < 0 or index >= self._count or self._slots[index] is not item:
            return False
        last_index = self._count - 1
        last = self._slots[last_index]
        next_count = last_index
        if index != last_index:
            if last is None:
                raise RuntimeError("retry deadline heap tail invariant violated")
            self._slots[index] = last
            last.deadline_slot = index
        self._slots[last_index] = None
        item.deadline_slot = -1
        self._count = next_count
        if index < next_count:
            moved = self._slots[index]
            if moved is None:
                raise RuntimeError("retry deadline heap removal lost moved owner")
            parent = (index - 1) // 2 if index else -1
            if parent >= 0:
                ancestor = self._slots[parent]
                if ancestor is not None and self._earlier(moved, ancestor):
                    self._sift_up(index)
                    return True
            self._sift_down(index)
        return True

    def pop_min(self) -> _ScheduledRetry | None:
        item = self.peek_min()
        if item is None:
            return None
        self.remove(item)
        return item


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
    protocol_violations: int = 0


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
        if globals().get("_RELEASE_GUARDIAN") is self:
            retire_terminal_category("release_guardian")
        self._pid = pid
        self._condition = threading.Condition()
        self._items: dict[int, _GuardedRelease] = {}
        # One authoritative owner map; the compatibility alias cannot diverge.
        self._owner_index = self._items
        self._generations: dict[int, int] = {}
        self._generation_sequence = 0
        self._dead_letters: list[_GuardedRelease | None] = [None] * _MAX_DEAD_LETTERS
        self._dead_letter_count = 0
        self._dead_letter_bytes = 0
        self._order: deque[int] = deque()  # compatibility only; pass51 scans owner states
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
        self._corrupted = False
        self._active_owner_keys: set[int] = set()
        self._protocol_violations = 0

    def _ensure_process(self) -> None:
        if self._pid != os.getpid():
            self._reset(os.getpid())

    def _mark_progress_locked(self) -> None:
        try:
            self._last_progress_ns = monotonic_ns()
            self._progress_epoch = min((1 << 63) - 1, self._progress_epoch + 1)
            diagnostic_transition()
        except BaseException:
            pass

    def _reconcile_retained_bytes_locked(self) -> bool:
        """Rebuild retained-byte admission from exact guarded owners."""
        exact = 0
        try:
            for item in self._items.values():
                if not item.resource_released:
                    exact += item.retained_bytes
        except BaseException:
            self._corrupted = True
            self._circuit_open = True
            self._protocol_violations += 1
            return False
        if exact != self._retained_bytes:
            self._retained_bytes = exact
            self._corrupted = True
            self._circuit_open = True
            self._protocol_violations += 1
            self._mark_progress_locked()
        return not self._corrupted

    def adopt(self, owner: Any, *, method: str = "release", retained_bytes: int = 256) -> bool:
        self._ensure_process()
        ensure_runtime_fork_safe()
        if type(method) is not str or not method:
            raise TypeError("guardian release method must be a non-empty exact string")
        if type(retained_bytes) is not int:
            raise TypeError("guardian retained_bytes must be an exact integer")
        if retained_bytes < 0:
            raise ValueError("guardian retained_bytes must be non-negative")
        charge = max(1, retained_bytes)
        if is_release_guardian_thread_lease(owner):
            return False
        resource_reserved_bytes = _trusted_resource_reserved_bytes(owner)
        owner_id = id(owner)
        # Every dynamic guardian owner is represented in the global control-plane
        # envelope. The reusable ticket token doubles as bounded generation.
        control_ticket = reserve_control_plane("release_guardian_owner", 384)
        accepted = False
        try:
            with self._condition:
                self._reconcile_retained_bytes_locked()
                if (
                    self._state is not _LifecycleState.RUNNING
                    or self._circuit_open
                    or self._corrupted
                ):
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
                existing = self._items.get(owner_id)
                if existing is not None:
                    if existing.owner is owner and existing.method == method:
                        return True
                    self._duplicate_owner_rejections += 1
                    return False
                if len(self._items) >= _MAX_GUARDED_RELEASES:
                    self._rejected_owners += 1
                    return False
                if charge > _MAX_GUARDED_RELEASE_BYTES - self._retained_bytes:
                    self._rejected_bytes += charge
                    return False
                generation = control_ticket.token
                item = _GuardedRelease(
                    owner,
                    method,
                    charge,
                    resource_reserved_bytes=resource_reserved_bytes,
                    generation=generation,
                    control_ticket=control_ticket,
                )
                # Prepare advisory generation mapping first; owner visibility is
                # the single _items publication. Roll back if that publication fails.
                self._generations[owner_id] = generation
                try:
                    self._items[owner_id] = item
                except BaseException:
                    self._generations.pop(owner_id, None)
                    raise
                self._generation_sequence = max(self._generation_sequence, generation)
                self._retained_bytes += charge
                accepted = True
                self._mark_progress_locked()
                self._condition.notify_all()
        finally:
            if not accepted:
                release_control_plane(control_ticket)
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

    def _finish_worker_start_locked(self) -> None:
        if self._workers_starting <= 0:
            self._protocol_violations += 1
            return
        self._workers_starting -= 1

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
                    self._finish_worker_start_locked()
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
                start_governed_thread(worker)
            except BaseException as exc:
                clear_exception_traceback(exc)
                with self._condition:
                    self._workers.discard(worker)
                    self._worker_leases.pop(worker, None)
                    self._finish_worker_start_locked()
                    self._worker_start_failures += 1
                    self._condition.notify_all()
                self._release_worker_lease(lease)
            else:
                with self._condition:
                    self._finish_worker_start_locked()
                    self._condition.notify_all()

    def _take_ready_locked(self) -> tuple[int, _GuardedRelease] | None:
        now_ns = monotonic_ns()
        selected_key = -1
        selected: _GuardedRelease | None = None
        earliest: int | None = None
        for key, item in self._items.items():
            if item.state not in (_GuardedReleaseState.READY, _GuardedReleaseState.DELAYED):
                continue
            if item.next_attempt_ns <= now_ns:
                if selected is None or item.next_attempt_ns < selected.next_attempt_ns:
                    selected_key = key
                    selected = item
                continue
            if earliest is None or item.next_attempt_ns < earliest:
                earliest = item.next_attempt_ns
        if selected is not None:
            next_active = self._active_releases + 1
            selected.state = _GuardedReleaseState.ACTIVE
            selected.started_ns = now_ns
            self._active_owner_keys.add(selected_key)
            if self._active_releases == 0:
                self._oldest_active_ns = now_ns
            self._active_releases = next_active
            return selected_key, selected
        if earliest is not None:
            self._condition.wait(timeout=max(0.001, min((earliest - now_ns) / 1_000_000_000, 0.25)))
        return None

    def _append_dead_letter_locked(self, item: _GuardedRelease) -> bool:
        if self._dead_letter_count >= _MAX_DEAD_LETTERS:
            return False
        index = self._dead_letter_count
        next_count = index + 1
        next_bytes = self._dead_letter_bytes + item.retained_bytes
        self._dead_letters[index] = item
        self._dead_letter_count = next_count
        self._dead_letter_bytes = next_bytes
        return True

    def _publish_terminal_item_locked(self, key: int, item: _GuardedRelease) -> None:
        """Publish metadata only for the process-wide singleton guardian."""
        if globals().get("_RELEASE_GUARDIAN") is self:
            publish_terminal_owner("release_guardian", key, retained_bytes=item.retained_bytes)

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
                success = item.resource_released
                failure = ""
                if not success:
                    try:
                        release = getattr(item.owner, item.method)
                        release()
                        success = True
                    except BaseException as exc:
                        failure = safe_exception_summary(exc, max_chars=512)
                        clear_exception_traceback(exc)
                owner_to_drop: Any | None = None
                with self._condition:
                    if self._active_releases <= 0:
                        # Never turn an underflow into apparent shutdown
                        # quiescence. Keep a sticky protocol diagnostic.
                        self._protocol_violations += 1
                    else:
                        self._active_releases -= 1
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
                        if not item.resource_released:
                            # Exact owners are authoritative. Repair any low/high
                            # cache drift before cleanup, latch quarantine, then
                            # retire only this authenticated item's charge.
                            self._reconcile_retained_bytes_locked()
                            if item.retained_bytes > self._retained_bytes:
                                self._corrupted = True
                                self._circuit_open = True
                                self._protocol_violations += 1
                            else:
                                self._retained_bytes -= item.retained_bytes
                            owner_to_drop = item.owner
                            item.owner = None
                            item.retained_bytes = 0
                            item.resource_reserved_bytes = 0
                            item.resource_released = True
                        # Keep the exact item/generation rooted until secondary
                        # control-plane retirement commits. RELEASED is a
                        # transient state while the worker performs that tail.
                        item.state = _GuardedReleaseState.RELEASED
                    else:
                        item.attempts += 1
                        now_ns = monotonic_ns()
                        if item.first_failure_ns == 0:
                            item.first_failure_ns = now_ns
                            item.first_failure = failure
                        item.last_failure = failure
                        if item.attempts >= _RELEASE_MAX_ATTEMPTS:
                            if (
                                self._dead_letter_count < _MAX_DEAD_LETTERS
                                and item.retained_bytes
                                <= _MAX_DEAD_LETTER_BYTES - self._dead_letter_bytes
                            ):
                                # Destination slab first; source map is retired
                                # only after terminal ownership is rooted.
                                if self._append_dead_letter_locked(item):
                                    # Keep the same identity in the authoritative
                                    # owner map. The fixed dead-letter slab is a
                                    # diagnostic/terminal index, not a second
                                    # authority; dedup therefore remains exact.
                                    item.state = _GuardedReleaseState.DEAD_LETTER
                                    self._publish_terminal_item_locked(key, item)
                                else:
                                    item.parked = True
                                    item.state = _GuardedReleaseState.PARKED
                                    self._publish_terminal_item_locked(key, item)
                            else:
                                item.parked = True
                                item.state = _GuardedReleaseState.PARKED
                                self._publish_terminal_item_locked(key, item)
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
                    self._mark_progress_locked()
                    self._condition.notify_all()
                if success:
                    ticket = item.control_ticket
                    control_retired = ticket is None
                    if ticket is not None:
                        try:
                            control_retired = bool(release_control_plane(ticket))
                        except BaseException as exc:
                            clear_exception_traceback(exc)
                            control_retired = False
                    with self._condition:
                        current_item = self._items.get(key)
                        if current_item is item and self._generations.get(key) == item.generation:
                            if control_retired:
                                item.control_ticket = None
                                self._items.pop(key, None)
                                self._generations.pop(key, None)
                                item.state = _GuardedReleaseState.RELEASED
                            else:
                                # Primary release is already committed. Requeue
                                # only the exact control-ticket ACK; the next
                                # worker iteration skips owner.release().
                                item.next_attempt_ns = min(
                                    _MAX_DEADLINE_NS,
                                    monotonic_ns() + 10_000_000,
                                )
                                item.state = _GuardedReleaseState.DELAYED
                            self._mark_progress_locked()
                            self._condition.notify_all()
                del owner_to_drop
        finally:
            with self._condition:
                owned_lease = self._worker_leases.pop(current, lease)
                self._retiring_workers[current] = owned_lease
                self._condition.notify_all()
            if not defer_governed_thread_retirement(current, owned_lease.release):
                self._retain_failed_worker_lease(owned_lease)
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
                return not (self._items or self._dead_letter_count or self._failed_worker_leases)
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
                    self._items or self._dead_letter_count or self._failed_worker_leases
                )
                if workers_quiescent and resources_drained:
                    if self._protocol_violations:
                        self._state = _LifecycleState.FAILED
                        self._condition.notify_all()
                        return False
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
                item.first_failure_ns for item in self._items.values() if item.first_failure_ns
            ]
            return ReleaseGuardianSnapshot(
                sum(
                    item.state is not _GuardedReleaseState.DEAD_LETTER
                    for item in self._items.values()
                )
                + len(self._failed_worker_leases),
                self._retained_bytes,
                self._active_releases,
                sum(worker.is_alive() for worker in self._workers),
                self._worker_start_failures,
                self._rejected_owners + self._duplicate_owner_rejections,
                self._rejected_bytes,
                self._dead_letter_count,
                self._dead_letter_bytes,
                self._stale_generation_drops,
                self._last_progress_ns,
                len(parked),
                sum(item.retained_bytes for item in parked),
                len(self._generations),
                self._state.name,
                self._progress_epoch,
                min(failures, default=0),
                sum(item.resource_reserved_bytes for item in self._items.values()),
                len(self._failed_worker_leases),
                self._circuit_open,
                self._oldest_active_ns,
                len(self._retiring_workers),
                len(self._active_owner_keys),
                self._protocol_violations,
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
    protocol_violations: int = 0


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
        self._heap = _BoundedDeadlineIndex(_MAX_PENDING_RETRIES)
        self._current: dict[Hashable, _ScheduledRetry] = {}
        self._ready: deque[Hashable] = deque()
        self._ready_queues: dict[Hashable, deque[_ScheduledRetry]] = {}
        self._ready_by_key: dict[Hashable, _ScheduledRetry] = {}
        self._ready_last_subsystem: Hashable | None = None
        self._active_by_key: dict[Hashable, _ScheduledRetry] = {}
        self._successors: dict[Hashable, _ScheduledRetry] = {}
        self._emergency: dict[Hashable, _ScheduledRetry] = {}
        self._emergency_bytes = 0
        self._successor_bytes = 0
        self._pending_bytes = 0
        self._ready_bytes = 0
        self._active_retries = 0
        self._active_bytes = 0
        self._generation_pool = BoundedGenerationPool(_MAX_PENDING_RETRIES)
        # Compatibility diagnostics: these mirror the latest reusable token;
        # they are no longer lifetime-monotonic namespaces.
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
        self._rejected_hidden_owner_retries = 0
        self._closed = False
        self._availability_registered = False
        self._state = _LifecycleState.RUNNING
        self._key_generations: dict[Hashable, int] = {}
        self._stale_generation_drops = 0
        self._admission_paused = False
        self._admission_corrupted = False
        self._last_progress_ns = monotonic_ns()
        self._progress_epoch = 0
        self._protocol_violations = 0

    def _reconcile_admission_counters_locked(self) -> bool:
        """Validate every admission cache against exact retry owner mappings."""
        try:
            pending = sum(item.retained_bytes for item in self._current.values())
            ready = sum(item.retained_bytes for item in self._ready_by_key.values())
            active = sum(item.retained_bytes for item in self._active_by_key.values())
            successor = sum(item.retained_bytes for item in self._successors.values())
            emergency = sum(item.retained_bytes for item in self._emergency.values())
            counts: dict[Hashable, int] = {}
            bytes_by_subsystem: dict[Hashable, int] = {}
            for mapping in (
                self._current,
                self._ready_by_key,
                self._active_by_key,
                self._successors,
                self._emergency,
            ):
                for item in mapping.values():
                    counts[item.subsystem] = counts.get(item.subsystem, 0) + 1
                    bytes_by_subsystem[item.subsystem] = (
                        bytes_by_subsystem.get(item.subsystem, 0) + item.retained_bytes
                    )
        except BaseException:
            self._admission_corrupted = True
            self._admission_paused = True
            self._protocol_violations += 1
            return False
        mismatch = (
            self._pending_bytes != pending
            or self._ready_bytes != ready
            or self._active_bytes != active
            or self._successor_bytes != successor
            or self._emergency_bytes != emergency
            or self._subsystem_counts != counts
            or self._subsystem_bytes != bytes_by_subsystem
        )
        if mismatch:
            self._pending_bytes = pending
            self._ready_bytes = ready
            self._active_bytes = active
            self._successor_bytes = successor
            self._emergency_bytes = emergency
            self._subsystem_counts = counts
            self._subsystem_bytes = bytes_by_subsystem
            self._admission_corrupted = True
            self._admission_paused = True
            self._protocol_violations += 1
            self._mark_progress_locked()
        return not self._admission_corrupted

    def _checked_byte_decrement_locked(self, current: int, amount: int) -> int:
        """Return a conservative post-release byte count or latch corruption."""
        if amount < 0 or current < amount:
            self._protocol_violations += 1
            return current
        return current - amount

    def _decrement_protocol_counter_locked(self, name: str, amount: int = 1) -> None:
        current = int(getattr(self, name))
        if amount < 0 or current < amount:
            self._protocol_violations += 1
            return
        setattr(self, name, current - amount)

    def _ensure_process(self) -> None:
        if self._pid != os.getpid():
            self._reset(os.getpid())

    def _mark_progress_locked(self) -> None:
        try:
            self._last_progress_ns = monotonic_ns()
            self._progress_epoch = min((1 << 63) - 1, self._progress_epoch + 1)
            diagnostic_transition()
        except BaseException:
            pass

    def _next_token_locked(self, *, commit: bool = True) -> int:
        token = self._generation_pool.acquire()
        if token is None:
            raise RuntimeError("retry token generation capacity exhausted")
        if commit:
            self._token_sequence = token
        return token

    def _release_retry_generation_locked(self, item: _ScheduledRetry) -> None:
        token = item.token
        if token and self._generation_pool.release_for(item):
            item.token = 0

    def _release_generation_token_noexcept_locked(self, token: int) -> None:
        """Return one unpublished generation without masking a primary failure."""
        try:
            self._generation_pool.release(token)
        except BaseException as exc:
            clear_exception_traceback(exc)

    @staticmethod
    def _deadline_from_delay(delay_seconds: int | float) -> int:
        return deadline_ns_from_timeout(delay_seconds, name="retry delay", allow_zero=True)

    def _drop_pending_charge_locked(self, item: _ScheduledRetry) -> None:
        # Owner retirement must never be undone by diagnostic/accounting OOM.
        try:
            next_pending = self._checked_byte_decrement_locked(
                self._pending_bytes, item.retained_bytes
            )
            self._pending_bytes = next_pending
        except BaseException:
            # Conservatively retain an over-count; it can only reduce admission.
            pass

    def _drop_subsystem_charge_locked(self, item: _ScheduledRetry) -> None:
        """Retire advisory counters/ticket without throwing after ownership commit."""
        charge = item.retained_bytes
        try:
            current_count = self._subsystem_counts.get(item.subsystem, 0)
            current_bytes = self._subsystem_bytes.get(item.subsystem, 0)
            if current_count <= 0 or current_bytes < charge:
                self._admission_corrupted = True
                self._admission_paused = True
                self._protocol_violations += 1
            else:
                count = current_count - 1
                if count > 0:
                    self._subsystem_counts[item.subsystem] = count
                else:
                    self._subsystem_counts.pop(item.subsystem, None)
                total = current_bytes - charge
                if total > 0:
                    self._subsystem_bytes[item.subsystem] = total
                else:
                    self._subsystem_bytes.pop(item.subsystem, None)
        except BaseException:
            # Admission accounting may remain conservatively high. Never make an
            # already-committed retry replacement fail because telemetry shrank.
            pass
        ticket = item.control_ticket
        if ticket is not None:
            try:
                if release_control_plane(ticket):
                    item.control_ticket = None
            except BaseException:
                pass
        try:
            self._release_retry_generation_locked(item)
        except BaseException:
            pass

    def _add_subsystem_charge_locked(self, item: _ScheduledRetry) -> None:
        """Prepare one subsystem charge transactionally before publication."""
        ticket = item.control_ticket
        created = False
        if ticket is None:
            ticket = reserve_control_plane("retry_item", 384)
            created = True
        old_count_present = item.subsystem in self._subsystem_counts
        old_bytes_present = item.subsystem in self._subsystem_bytes
        old_count = self._subsystem_counts.get(item.subsystem, 0)
        old_bytes = self._subsystem_bytes.get(item.subsystem, 0)
        next_count = old_count + 1
        next_bytes = old_bytes + item.retained_bytes
        try:
            self._subsystem_counts[item.subsystem] = next_count
            try:
                self._subsystem_bytes[item.subsystem] = next_bytes
            except BaseException:
                if old_count_present:
                    self._subsystem_counts[item.subsystem] = old_count
                else:
                    self._subsystem_counts.pop(item.subsystem, None)
                raise
            item.control_ticket = ticket
        except BaseException:
            if old_bytes_present:
                try:
                    self._subsystem_bytes[item.subsystem] = old_bytes
                except BaseException:
                    pass
            else:
                self._subsystem_bytes.pop(item.subsystem, None)
            if created:
                try:
                    release_control_plane(ticket)
                except BaseException:
                    pass
            raise

    @staticmethod
    def _detach_locked(
        item: _ScheduledRetry | None,
        detached: list[Callable[[], None]],
    ) -> None:
        """Detach only after the off-lock destruction list accepts ownership."""
        if item is None or item.callback is _noop:
            return
        callback = item.callback
        try:
            detached.append(callback)
        except BaseException:
            # Leave the callback attached. The removed retry item remains rooted
            # by the caller until the scheduler lock is released, so arbitrary
            # callback finalizers still run off-lock.
            item.state = _RetryItemState.FINISHED
            return
        item.callback = _noop
        item.state = _RetryItemState.FINISHED

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
        """Compatibility hook: pass51 deadline storage never contains stale nodes."""
        # pass35/pass50 compatibility: self._heap.insert(item) used to
        # require stale-node compaction. The bounded deadline index has exactly
        # one physical node per current key, so there is nothing to rebuild.
        return

    def _compact_ready_locked(self, *, force: bool = False) -> None:
        """Compatibility hook: the ready map is the sole physical ready index."""
        return

    def _enqueue_ready_locked(self, item: _ScheduledRetry) -> None:
        # ``_ready_by_key`` is authoritative in pass51. No growable deque/index
        # publication occurs after a retry leaves the deadline index.
        item.state = _RetryItemState.READY

    def _take_ready_locked(self) -> _ScheduledRetry | None:
        """Claim one ready owner without deque rotation/reinsertion allocation."""
        item: _ScheduledRetry | None = None
        fallback: _ScheduledRetry | None = None
        for candidate in self._ready_by_key.values():
            if candidate.key in self._active_by_key:
                continue
            if fallback is None or candidate.sequence < fallback.sequence:
                fallback = candidate
            if candidate.subsystem == self._ready_last_subsystem:
                continue
            if item is None or candidate.sequence < item.sequence:
                item = candidate
        if item is None:
            item = fallback
        if item is None:
            return None
        next_ready_bytes = self._checked_byte_decrement_locked(
            self._ready_bytes, item.retained_bytes
        )
        next_active_retries = self._active_retries + 1
        next_active_bytes = self._active_bytes + item.retained_bytes
        # Publish the destination before retiring READY. Dict insertion is the
        # only growable step and therefore happens while READY remains intact.
        self._active_by_key[item.key] = item
        try:
            if self._ready_by_key.get(item.key) is not item:
                self._active_by_key.pop(item.key, None)
                return None
            self._ready_by_key.pop(item.key, None)
        except BaseException:
            self._active_by_key.pop(item.key, None)
            raise
        item.state = _RetryItemState.CLAIMED
        self._ready_last_subsystem = item.subsystem
        self._ready_bytes = next_ready_bytes
        self._active_retries = next_active_retries
        self._active_bytes = next_active_bytes
        self._mark_progress_locked()
        return item

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
        next_bytes = self._emergency_bytes - old_charge + item.retained_bytes
        item.state = _RetryItemState.PENDING
        self._add_subsystem_charge_locked(item)
        try:
            self._emergency[item.key] = item
        except BaseException:
            self._drop_subsystem_charge_locked(item)
            raise
        self._emergency_bytes = next_bytes
        if old is not None:
            self._drop_subsystem_charge_locked(old)
            try:
                self._detach_locked(old, detached)
            except BaseException:
                # Caller retains the replaced owner through the commit tail; a
                # callback finalizer may run only after the scheduler lock exits.
                pass

    def _promote_emergency_locked(self) -> None:
        if not self._emergency:
            return
        for key in tuple(self._emergency):
            item = self._emergency.get(key)
            if item is None or key in self._active_by_key or key in self._current:
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
            # Materialize all integer accounting before publishing the heap node.
            next_emergency_bytes = self._checked_byte_decrement_locked(
                self._emergency_bytes, item.retained_bytes
            )
            next_pending_bytes = self._pending_bytes + item.retained_bytes
            try:
                # Heap publication is provisional until _current points at the
                # same identity, so an allocation failure cannot strand an
                # authoritative emergency retry outside the deadline index.
                self._heap.insert(item)
                try:
                    self._current[key] = item
                except BaseException:
                    self._remove_heap_item_identity_noexcept_locked(item)
                    raise
            except BaseException:
                break
            # Commit tail contains only non-growing mutations/assignments.
            self._emergency.pop(key, None)
            self._emergency_bytes = next_emergency_bytes
            item.state = _RetryItemState.PENDING
            self._pending_bytes = next_pending_bytes
            self._mark_progress_locked()

    def _remove_scheduled_locked(self, key: Hashable, detached: list[Callable[[], None]]) -> bool:
        removed = False
        old = self._current.pop(key, None)
        if old is not None:
            self._heap.remove(old)
            removed = True
            self._drop_pending_charge_locked(old)
            self._drop_subsystem_charge_locked(old)
            self._detach_locked(old, detached)
        ready = self._ready_by_key.pop(key, None)
        if ready is not None:
            removed = True
            self._ready_bytes = self._checked_byte_decrement_locked(
                self._ready_bytes, ready.retained_bytes
            )
            self._drop_subsystem_charge_locked(ready)
            self._detach_locked(ready, detached)
        emergency = self._emergency.pop(key, None)
        if emergency is not None:
            removed = True
            self._emergency_bytes = self._checked_byte_decrement_locked(
                self._emergency_bytes, emergency.retained_bytes
            )
            self._drop_subsystem_charge_locked(emergency)
            self._detach_locked(emergency, detached)
        successor = self._successors.pop(key, None)
        if successor is not None:
            removed = True
            self._successor_bytes = self._checked_byte_decrement_locked(
                self._successor_bytes, successor.retained_bytes
            )
            self._drop_subsystem_charge_locked(successor)
            self._detach_locked(successor, detached)
        return removed

    # Compatibility name retained for pass36/private tests.
    def _remove_existing_locked(self, key: Hashable, detached: list[Callable[[], None]]) -> None:
        self._remove_scheduled_locked(key, detached)

    def _install_successor_locked(
        self, item: _ScheduledRetry, detached: list[Callable[[], None]]
    ) -> None:
        old = self._successors.get(item.key)
        next_bytes = (
            self._successor_bytes
            - (old.retained_bytes if old is not None else 0)
            + item.retained_bytes
        )
        item.state = _RetryItemState.SUCCESSOR
        self._add_subsystem_charge_locked(item)
        try:
            self._successors[item.key] = item
        except BaseException:
            self._drop_subsystem_charge_locked(item)
            raise
        self._successor_bytes = next_bytes
        if old is not None:
            self._drop_subsystem_charge_locked(old)
            try:
                self._detach_locked(old, detached)
            except BaseException:
                # Caller retains the replaced owner through the commit tail; a
                # callback finalizer may run only after the scheduler lock exits.
                pass

    def _promote_successor_locked(self, key: Hashable) -> None:
        item = self._successors.get(key)
        if item is None:
            self._maybe_prune_generation_locked(key)
            return
        if key in self._current:
            return
        next_successor_bytes = self._checked_byte_decrement_locked(
            self._successor_bytes, item.retained_bytes
        )
        next_pending_bytes = self._pending_bytes + item.retained_bytes
        try:
            # Preserve the successor representation until both growable
            # structures for the pending representation are prepared.
            self._heap.insert(item)
            try:
                self._current[key] = item
            except BaseException:
                self._remove_heap_item_identity_noexcept_locked(item)
                raise
        except BaseException:
            return
        self._successors.pop(key, None)
        self._successor_bytes = next_successor_bytes
        item.state = _RetryItemState.PENDING
        self._pending_bytes = next_pending_bytes
        self._mark_progress_locked()

    def _retire_mapping_item_noexcept_locked(
        self,
        mapping: dict[Hashable, _ScheduledRetry],
        key: Hashable,
        expected: _ScheduledRetry | None,
        bytes_attr: str,
        detached: list[Callable[[], None]],
    ) -> None:
        """Retire one replaced representation without invalidating the new commit."""
        if expected is None or mapping.get(key) is not expected:
            return
        if mapping is self._current:
            try:
                self._heap.remove(expected)
            except BaseException:
                return
        try:
            mapping.pop(key, None)
        except BaseException:
            return
        try:
            current_bytes = getattr(self, bytes_attr)
            setattr(
                self,
                bytes_attr,
                self._checked_byte_decrement_locked(current_bytes, expected.retained_bytes),
            )
        except BaseException:
            # Conservative stale-high accounting only reduces future admission.
            pass
        self._drop_subsystem_charge_locked(expected)
        try:
            self._detach_locked(expected, detached)
        except BaseException:
            pass

    def _restore_generation_noexcept_locked(
        self, key: Hashable, *, present: bool, value: int | None
    ) -> None:
        try:
            if present:
                self._key_generations[key] = value  # type: ignore[assignment]
            else:
                self._key_generations.pop(key, None)
        except BaseException:
            # A stale/newer generation is fail-closed: workers validate exact
            # item identity and generation before user-code execution.
            pass

    def _remove_heap_item_identity_noexcept_locked(self, item: _ScheduledRetry) -> None:
        """Remove one provisional deadline owner from fixed storage."""
        try:
            self._heap.remove(item)
        except BaseException:
            pass

    def schedule(
        self,
        key: Hashable,
        callback: Callable[[], None],
        *,
        delay_seconds: float,
        retained_bytes: int = _DEFAULT_RETAINED_BYTES,
        jitter_fraction: float = 0.0,
    ) -> bool:
        """Install or replace one retry with a prepare -> publish -> retire protocol."""
        # pass50 compatibility breadcrumb: heapq.heappush(self._heap, item)
        # preceded self._current[key] = item; pass51 uses the fixed index insert.
        self._ensure_process()
        ensure_runtime_fork_safe()
        key = _normalize_retry_key(key)
        if type(retained_bytes) is not int:
            raise TypeError("retry retained_bytes must be an exact integer")
        if retained_bytes < 0:
            raise ValueError("retry retained_bytes must be non-negative")
        charge = max(1, retained_bytes)
        if callback_retains_hidden_owner(callback):
            with self._condition:
                self._rejected_retries += 1
                self._rejected_hidden_owner_retries += 1
                self._mark_progress_locked()
            return False
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
            self._reconcile_admission_counters_locked()
            if (
                self._state is not _LifecycleState.RUNNING
                or self._closed
                or self._admission_corrupted
            ):
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
            regular_ok = not over_count and not over_bytes
            emergency_ok = (
                not regular_ok
                and active is None
                and self._emergency_fits_locked(key, charge=charge)
            )
            if not regular_ok and not emergency_ok:
                if over_count:
                    self._rejected_retries += 1
                if over_bytes:
                    self._rejected_bytes += charge
                self._mark_progress_locked()
                return False
            # Pass85 owner-first generation admission. Construct the retry owner
            # before the namespace commits; an interrupted token handoff can be
            # rolled back by item identity even when ``token = CALL`` never stored.
            control_ticket = reserve_control_plane("retry_item", 384)
            try:
                item = _ScheduledRetry(deadline_ns, 0, key, 0, subsystem, callback, charge)
            except BaseException:
                try:
                    release_control_plane(control_ticket)
                except BaseException as cleanup_exc:
                    clear_exception_traceback(cleanup_exc)
                raise
            item.control_ticket = control_ticket
            try:
                token = self._generation_pool.acquire_for(item)
                if token is None:
                    raise RuntimeError("retry token generation capacity exhausted")
                item.token = token
                item.sequence = token
            except BaseException:
                try:
                    self._generation_pool.release_for(item)
                except BaseException as cleanup_exc:
                    clear_exception_traceback(cleanup_exc)
                try:
                    release_control_plane(control_ticket)
                except BaseException as cleanup_exc:
                    clear_exception_traceback(cleanup_exc)
                item.control_ticket = None
                raise

            old_generation_present = key in self._key_generations
            old_generation = self._key_generations.get(key)

            if regular_ok and active is not None:
                # Prepare generation before publishing successor ownership.
                try:
                    self._key_generations[key] = token
                except BaseException:
                    release_control_plane(item.control_ticket)
                    item.control_ticket = None
                    self._release_retry_generation_locked(item)
                    raise
                try:
                    self._install_successor_locked(item, detached)
                except BaseException:
                    self._restore_generation_noexcept_locked(
                        key, present=old_generation_present, value=old_generation
                    )
                    if item.control_ticket is not None:
                        release_control_plane(item.control_ticket)
                        item.control_ticket = None
                    self._release_retry_generation_locked(item)
                    raise

                # New successor is authoritative. Retire every older pending
                # representation best-effort; no tail failure may undo success.
                self._retire_mapping_item_noexcept_locked(
                    self._current, key, old, "_pending_bytes", detached
                )
                self._retire_mapping_item_noexcept_locked(
                    self._ready_by_key, key, old_ready, "_ready_bytes", detached
                )
                self._retire_mapping_item_noexcept_locked(
                    self._emergency, key, old_emergency, "_emergency_bytes", detached
                )
                accepted = True

            elif regular_ok:
                # Prepare bounded charge first. It is rolled back if either map
                # publication or heap insertion fails.
                try:
                    self._add_subsystem_charge_locked(item)
                except BaseException:
                    if item.control_ticket is not None:
                        release_control_plane(item.control_ticket)
                        item.control_ticket = None
                    self._release_retry_generation_locked(item)
                    raise
                old_current = self._current.get(key)
                current_written = False
                generation_written = False
                heap_published = False
                try:
                    # The heap node is only a provisional root until _current
                    # points at the same identity. Workers identity-check every
                    # popped node, so publication here cannot execute user code.
                    # Preparing this growable structure first prevents a failed
                    # heap resize from replacing the authoritative retry.
                    replaced_deadline = False
                    if old_current is not None and old_current.deadline_slot >= 0:
                        replaced_deadline = True
                        self._heap.replace(old_current, item)
                    else:
                        self._heap.insert(item)
                    heap_published = True
                    self._current[key] = item
                    current_written = True
                    self._key_generations[key] = token
                    generation_written = True
                except BaseException:
                    # heappush is permitted to mutate before raising under fault
                    # injection; identity removal handles both possibilities.
                    if replaced_deadline and old_current is not None:
                        try:
                            self._heap.replace(item, old_current)
                        except BaseException:
                            pass
                    else:
                        self._remove_heap_item_identity_noexcept_locked(item)
                    if current_written:
                        try:
                            if old_current is None:
                                if self._current.get(key) is item:
                                    self._current.pop(key, None)
                            else:
                                self._current[key] = old_current
                        except BaseException:
                            pass
                    if generation_written:
                        self._restore_generation_noexcept_locked(
                            key, present=old_generation_present, value=old_generation
                        )
                    self._drop_subsystem_charge_locked(item)
                    raise

                # Commit point: current + generation + heap all identify new item.
                try:
                    self._pending_bytes = self._pending_bytes + charge
                except BaseException:
                    # Conservative under-accounting is not acceptable. Remove the
                    # just-published owner while all rollback roots still exist.
                    if locals().get("replaced_deadline", False) and old_current is not None:
                        try:
                            self._heap.replace(item, old_current)
                        except BaseException:
                            pass
                    else:
                        self._remove_heap_item_identity_noexcept_locked(item)
                    try:
                        if old_current is None:
                            self._current.pop(key, None)
                        else:
                            self._current[key] = old_current
                    except BaseException:
                        pass
                    self._restore_generation_noexcept_locked(
                        key, present=old_generation_present, value=old_generation
                    )
                    self._drop_subsystem_charge_locked(item)
                    raise

                self._retire_mapping_item_noexcept_locked(
                    self._ready_by_key, key, old_ready, "_ready_bytes", detached
                )
                self._retire_mapping_item_noexcept_locked(
                    self._emergency, key, old_emergency, "_emergency_bytes", detached
                )
                self._retire_mapping_item_noexcept_locked(
                    self._successors, key, old_successor, "_successor_bytes", detached
                )
                if old_current is not None and old_current is not item:
                    # It was replaced in-place, so retire its charge/payload only.
                    self._drop_pending_charge_locked(old_current)
                    self._drop_subsystem_charge_locked(old_current)
                    try:
                        self._detach_locked(old_current, detached)
                    except BaseException:
                        pass
                accepted = True

            else:
                # Emergency representation has no heap entry yet, but generation
                # must be prepared before the dict publication becomes authority.
                try:
                    self._key_generations[key] = token
                except BaseException:
                    release_control_plane(item.control_ticket)
                    item.control_ticket = None
                    self._release_retry_generation_locked(item)
                    raise
                try:
                    self._install_emergency_locked(item, detached)
                except BaseException:
                    self._restore_generation_noexcept_locked(
                        key, present=old_generation_present, value=old_generation
                    )
                    if item.control_ticket is not None:
                        release_control_plane(item.control_ticket)
                        item.control_ticket = None
                    self._release_retry_generation_locked(item)
                    raise
                self._retire_mapping_item_noexcept_locked(
                    self._current, key, old, "_pending_bytes", detached
                )
                self._retire_mapping_item_noexcept_locked(
                    self._ready_by_key, key, old_ready, "_ready_bytes", detached
                )
                self._retire_mapping_item_noexcept_locked(
                    self._successors, key, old_successor, "_successor_bytes", detached
                )
                accepted = True

            if accepted:
                self._token_sequence = token
                self._heap_sequence = token
                try:
                    self._compact_heap_locked()
                except BaseException:
                    pass
                try:
                    self._compact_ready_locked()
                except BaseException:
                    pass
                self._mark_progress_locked()
                try:
                    self._condition.notify_all()
                except BaseException:
                    pass

        # Drop user callbacks only after leaving the scheduler lock.
        detached.clear()
        if not accepted:
            return False
        try:
            self._ensure_workers()
        except BaseException as exc:
            clear_exception_traceback(exc)
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
                self._key_generations[key] = 0
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
            start_governed_thread(worker)
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
                self._decrement_protocol_counter_locked("_execution_starting")
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
            start_governed_thread(worker)
        except BaseException:
            with self._condition:
                self._execution_workers.discard(worker)
                self._decrement_protocol_counter_locked("_execution_starting")
                self._worker_leases.pop(worker, None)
                self._worker_start_failures += 1
                self._condition.notify_all()
            self._adopt_failed_lease(lease)
        else:
            with self._condition:
                self._decrement_protocol_counter_locked("_execution_starting")
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
        item = self._heap.peek_min()
        if item is None or self._current.get(item.key) is item:
            return
        # Defensive repair for synthetic/private state corruption.
        self._heap.remove(item)
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
                    if self._current.get(item.key) is not item:
                        self._heap.remove(item)
                        self._detach_locked(item, detached)
                        continue
                    next_pending_bytes = self._checked_byte_decrement_locked(
                        self._pending_bytes, item.retained_bytes
                    )
                    next_ready_bytes = self._ready_bytes + item.retained_bytes
                    # Destination map first: an allocation failure leaves the
                    # deadline/current representation completely authoritative.
                    self._ready_by_key[item.key] = item
                    self._enqueue_ready_locked(item)
                    self._heap.remove(item)
                    self._current.pop(item.key, None)
                    self._pending_bytes = next_pending_bytes
                    self._ready_bytes = next_ready_bytes
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
                        self._decrement_protocol_counter_locked("_active_retries")
                        self._decrement_protocol_counter_locked("_active_bytes", retained_bytes)
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
        if is_project_thread_lease(owned_lease):
            if not defer_governed_thread_retirement(current, owned_lease.release):
                self._adopt_failed_lease(owned_lease)
        else:
            # Focused test/control leases are not physical project permits and
            # preserve the historical synchronous release contract.
            try:
                owned_lease.release()
            except BaseException:
                self._adopt_failed_lease(owned_lease)
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
                    self._key_generations[key] = 0
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
                or self._protocol_violations
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
                len(self._ready_by_key),
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
                protocol_violations=self._protocol_violations,
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
        items = [item for item in guardian._dead_letters if item is not None]
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
    quarantine_inherited_state("retry-runtime", _RELEASE_GUARDIAN.__dict__, _SCHEDULER.__dict__)
    _FORKED_RETRY_GENERATIONS += 1
    pid = os.getpid()
    _RELEASE_GUARDIAN._reset(pid)
    _SCHEDULER._reset(pid)


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("retry-scheduler", mode="quarantine_only")


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
