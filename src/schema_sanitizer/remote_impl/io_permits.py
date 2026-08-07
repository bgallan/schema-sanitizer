"""Fair process-wide weighted admission for remote I/O coroutines."""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections import OrderedDict, deque
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from itertools import islice
from threading import Lock, local
from typing import Final

from schema_sanitizer.core_impl.safe_errors import add_bounded_note

from ..core_impl.cancellation import check_operation_cancelled
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.system_pressure import system_pressure_snapshot
from ..errors import SchemaSanitizerResourceError

_MAX_HEAD_BYPASSES: Final = 4
_MAX_LOCAL_BYPASS_SCAN: Final = 32
_DEFAULT_CAPACITY: Final = min(256, max(4, (os.cpu_count() or 1) * 4))
_DEFAULT_MAX_WAITERS: Final = 4096
_DEFAULT_MAX_PENDING_SUBMISSIONS: Final = 4096
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


@dataclass(slots=True)
class _Waiter:
    """One loop-affine weighted request queued under the process lock."""

    loop: asyncio.AbstractEventLoop
    future: asyncio.Future["RemoteIoPermit"]
    requested_weight: int
    label: str
    operation_id: str
    bypasses: int = 0
    granted_weight: int = 0
    state: str = "queued"
    indexed: bool = False


class _WaiterMirror:
    """O(1)-removal compatibility view of currently queued waiters."""

    def __init__(self, on_external_mutation: Callable[[], object] | None = None) -> None:
        self._items: dict[int, _Waiter] = {}
        self._on_external_mutation = on_external_mutation

    def append(self, waiter: _Waiter) -> None:
        self._items[id(waiter)] = waiter
        callback = self._on_external_mutation
        if callback is not None and not waiter.indexed:
            callback()

    def extend(self, waiters: Iterable[_Waiter]) -> None:
        for waiter in waiters:
            self.append(waiter)

    def remove(self, waiter: _Waiter) -> None:
        try:
            del self._items[id(waiter)]
        except KeyError as exc:
            raise ValueError("waiter is not queued") from exc

    def discard(self, waiter: _Waiter) -> None:
        self._items.pop(id(waiter), None)

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[_Waiter]:
        return iter(self._items.values())

    def __getitem__(self, index: int) -> _Waiter:
        if index < 0:
            index += len(self._items)
        if index < 0:
            raise IndexError(index)
        for current, waiter in enumerate(self._items.values()):
            if current == index:
                return waiter
        raise IndexError(index)


class RemoteIoSubmissionReservation:
    """Exactly-once process-wide slot for one submitted remote coroutine."""

    def __init__(self, governor: "RemoteIoPermitGovernor", *, _active: bool = True) -> None:
        """Initialize an active reservation or inert pre-publication owner."""
        self._governor = governor
        self._pid = os.getpid()
        self._lock = Lock()
        self._released = not _active

    def _activate(self) -> None:
        """Publish this owner after submission accounting commits."""
        self._released = False

    def release(self) -> None:
        """Return this submission slot exactly once."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._released:
                return
            self._released = True
        try:
            self._governor._release_submission()
        except BaseException:
            with self._lock:
                self._released = False
            raise

    close = release

    def __del__(self) -> None:
        """Release owned resources during finalization."""
        try:
            if runtime_is_finalizing():
                return
            self.release()
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
        self._governor = governor
        self._token = token
        self._pid = os.getpid()
        self._lock = Lock()
        self._released = not _active

    def _activate(self) -> None:
        """Publish this owner after capacity registration commits."""
        self._released = False

    def release(self) -> None:
        """Remove this coordinator's requested capacity exactly once."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._released:
                return
            self._released = True
        try:
            self._governor._unregister_capacity(self._token)
        except BaseException:
            with self._lock:
                self._released = False
            raise

    close = release

    def __del__(self) -> None:
        """Release owned resources during finalization."""
        try:
            if runtime_is_finalizing():
                return
            self.release()
        except BaseException:
            pass


class RemoteIoPermit:
    """Exactly-once weighted permit returned by the shared governor."""

    def __init__(self, governor: "RemoteIoPermitGovernor", weight: int, label: str) -> None:
        """Initialize this helper."""
        self._governor = governor
        self.weight = weight
        self.label = label
        self._pid = os.getpid()
        self._lock = Lock()
        self._released = False

    def release(self) -> None:
        """Return this permit exactly once from any thread or event loop."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._released:
                return
            self._released = True
        try:
            self._governor._release(self.weight)
        except BaseException:
            with self._lock:
                self._released = False
            raise

    close = release

    async def __aenter__(self) -> "RemoteIoPermit":
        """Enter this managed resource."""
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Exit this managed resource."""
        self.release()

    def __del__(self) -> None:
        """Release owned resources during finalization."""
        try:
            if runtime_is_finalizing():
                return
            self.release()
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
    ) -> None:
        """Initialize this helper."""
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
        self._lock = Lock()
        self._delivery_local = local()
        self._base_capacity = capacity
        self._pressure_scale = 1.0
        self._capacity = capacity
        self._registrations: dict[int, int] = {}
        self._next_registration_token = 0
        self._in_use = 0
        self._peak_in_use = 0
        # ``_waiters`` remains a compatibility/tombstone mirror for older
        # diagnostics and fault-injection tests. Scheduling uses per-operation
        # queues plus a rotating active-operation ring.
        self._legacy_waiters_dirty = False
        self._waiters = _WaiterMirror(self._mark_legacy_waiters_dirty)
        self._operation_waiters: dict[str, OrderedDict[int, _Waiter]] = {}
        self._operation_order: OrderedDict[str, None] = OrderedDict()
        self._weight_buckets: dict[int, OrderedDict[str, None]] = {}
        self._weight_order: OrderedDict[int, None] = OrderedDict()
        self._operation_weights: dict[str, int] = {}
        self._bucket_capacity = capacity
        self._waiting_count = 0
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

    def configure_capacity(self, requested: int) -> None:
        """Raise the permanent base ceiling for explicitly configured governors."""
        self._validate_capacity(requested)
        pressure_scale = system_pressure_snapshot().scale
        with self._lock:
            self._pressure_scale = pressure_scale
            self._base_capacity = max(self._base_capacity, requested)
            self._recompute_capacity_locked()
            deliveries = self._grant_ready_locked()
        self._deliver(deliveries)

    def register_capacity(self, requested: int) -> RemoteIoCapacityRegistration:
        """Register one live coordinator without publishing ownerless capacity."""
        self._validate_capacity(requested)
        pressure_scale = system_pressure_snapshot().scale
        with self._lock:
            self._pressure_scale = pressure_scale
            token = self._next_registration_token + 1
            registration = RemoteIoCapacityRegistration(self, token, _active=False)
            self._next_registration_token = token
            self._registrations[token] = requested
            self._recompute_capacity_locked()
            deliveries = self._grant_ready_locked()
            registration._activate()
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
            if self._pending_submissions >= self._max_pending_submissions:
                self._rejected_submissions += 1
                raise SchemaSanitizerResourceError(
                    "remote I/O submission capacity exhausted",
                    detail={
                        "stage": "remote_io_submission",
                        "limit_name": "remote_io_pending_submissions",
                        "limit_items": self._max_pending_submissions,
                        "actual_items": self._pending_submissions + 1,
                    },
                )
            self._pending_submissions += 1
            self._peak_pending_submissions = max(
                self._peak_pending_submissions, self._pending_submissions
            )
            reservation._activate()
        return reservation

    def _release_submission(self) -> None:
        """Return one submitted-coroutine slot without underflow."""
        with self._lock:
            self._pending_submissions = max(0, self._pending_submissions - 1)

    @staticmethod
    def _validate_capacity(requested: int) -> None:
        """Implement the internal _validate_capacity helper."""
        if isinstance(requested, bool) or not isinstance(requested, int):
            raise TypeError("remote I/O permit capacity must be an integer")
        if requested <= 0:
            raise ValueError("remote I/O permit capacity must be > 0")

    def _unregister_capacity(self, token: int) -> None:
        """Implement the internal _unregister_capacity helper."""
        with self._lock:
            self._registrations.pop(token, None)
            self._recompute_capacity_locked()
            deliveries = self._grant_ready_locked()
        self._deliver(deliveries)

    def _recompute_capacity_locked(self) -> None:
        """Recompute from the last pressure sample without performing I/O."""
        requested = max((self._base_capacity, *self._registrations.values()))
        pressured = max(1, int(requested * self._pressure_scale))
        previous = self._capacity
        self._capacity = max(self._in_use, pressured)
        if self._capacity != previous:
            self._rebuild_weight_buckets_locked()

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
        rejected = False
        pressure_scale = system_pressure_snapshot().scale
        with self._lock:
            self._pressure_scale = pressure_scale
            deliveries = self._grant_ready_locked()
            if self._waiting_count >= self._max_waiters:
                self._rejected_waiters += 1
                rejected = True
            else:
                self._enqueue_waiter_locked(waiter)
                self._peak_waiting = max(self._peak_waiting, self._waiting_count)
                deliveries.extend(self._grant_ready_locked())
        self._deliver(deliveries)
        if rejected:
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
            permit = await future
            check_operation_cancelled(stage="remote_io_admission")
            return permit
        except BaseException as exc:
            # Cancellation is not the only control-flow exception that can
            # abandon an await. KeyboardInterrupt, SystemExit, or an injected
            # BaseException must remove queued waiters and reclaim committed
            # grants by the same exactly-once path.
            cancellation_deliveries: list[_Waiter] = []
            delivered_permit: RemoteIoPermit | None = None
            with self._lock:
                if waiter.state == "queued":
                    waiter.state = "cancelled"
                    self._cancellations += 1
                    self._remove_waiter_locked(waiter)
                    cancellation_deliveries = self._grant_ready_locked()
                elif waiter.state == "granted":
                    waiter.state = "cancelled"
                    self._cancellations += 1
                    self._in_use = max(0, self._in_use - waiter.granted_weight)
                    self._recompute_capacity_locked()
                    deliveries = self._grant_ready_locked()
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

    def snapshot(self) -> RemoteIoPermitSnapshot:
        """Implement the internal snapshot helper."""
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
            )

    def _release(self, weight: int) -> None:
        """Implement the internal _release helper."""
        with self._lock:
            amount = max(0, weight)
            excess = max(0, amount - self._in_use)
            if excess:
                self._over_release_count += 1
                self._over_release_weight += excess
            self._in_use = max(0, self._in_use - amount)
            self._recompute_capacity_locked()
            deliveries = self._grant_ready_locked()
        self._deliver(deliveries)

    def _effective_weight(self, waiter: _Waiter) -> int:
        """Clamp requested weight against the capacity at admission time."""
        return max(1, min(waiter.requested_weight, self._capacity))

    def _mark_legacy_waiters_dirty(self) -> None:
        """Record direct compatibility-view mutation outside scheduler APIs."""
        self._legacy_waiters_dirty = True

    def _enqueue_waiter_locked(self, waiter: _Waiter) -> None:
        """Index one waiter in O(1) amortized operation-fair structures."""
        if waiter.indexed or waiter.state != "queued":
            return
        queue = self._operation_waiters.get(waiter.operation_id)
        if queue is None:
            queue = OrderedDict()
            self._operation_waiters[waiter.operation_id] = queue
            self._operation_order[waiter.operation_id] = None
        queue[id(waiter)] = waiter
        waiter.indexed = True
        self._waiting_count += 1
        self._waiters.append(waiter)
        self._refresh_operation_weight_locked(waiter.operation_id)

    def _sync_legacy_waiters_locked(self) -> None:
        """Index direct compatibility mutations only when explicitly dirtied."""
        if not self._legacy_waiters_dirty:
            return
        self._legacy_waiters_dirty = False
        for waiter in self._waiters:
            if waiter.state == "queued" and not waiter.indexed:
                queue = self._operation_waiters.get(waiter.operation_id)
                if queue is None:
                    queue = OrderedDict()
                    self._operation_waiters[waiter.operation_id] = queue
                    self._operation_order[waiter.operation_id] = None
                queue[id(waiter)] = waiter
                waiter.indexed = True
                self._waiting_count += 1
                self._refresh_operation_weight_locked(waiter.operation_id)

    def _compact_legacy_waiters_locked(self) -> None:
        """Bound tombstone retention without touching the scheduler hot path."""
        return

    def _operation_bucket_weight_locked(self, queue: OrderedDict[int, _Waiter]) -> int | None:
        """Return the lightest currently selectable local request."""
        queued = [
            waiter
            for waiter in islice(queue.values(), _MAX_LOCAL_BYPASS_SCAN + 1)
            if waiter.state == "queued"
        ]
        if not queued:
            return None
        head = queued[0]
        weight = self._effective_weight(head)
        if head.bypasses >= _MAX_HEAD_BYPASSES:
            return weight
        for waiter in queued[1:]:
            weight = min(weight, self._effective_weight(waiter))
        return weight

    def _remove_operation_weight_locked(self, operation_id: str) -> None:
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

    def _remove_waiter_locked(self, waiter: _Waiter) -> None:
        """Remove one queued waiter from its operation-local queue."""
        if not waiter.indexed:
            return
        queue = self._operation_waiters.get(waiter.operation_id)
        if queue is not None and queue.pop(id(waiter), None) is not None:
            self._waiting_count = max(0, self._waiting_count - 1)
        waiter.indexed = False
        self._waiters.discard(waiter)
        self._refresh_operation_weight_locked(waiter.operation_id)
        self._remove_operation_if_empty_locked(waiter.operation_id)
        self._compact_legacy_waiters_locked()

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

    def _take_candidate_locked(self, available: int) -> _Waiter | None:
        """Select through eligible-weight rings without scanning all operations."""
        weight_count = len(self._weight_order)
        for skipped in range(weight_count):
            weight, _unused = self._weight_order.popitem(last=False)
            bucket = self._weight_buckets.get(weight)
            if not bucket:
                self._weight_buckets.pop(weight, None)
                continue
            if weight > available:
                self._weight_order[weight] = None
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
            candidate = self._operation_candidate_locked(queue, available)
            if candidate is None:
                self._refresh_operation_weight_locked(operation_id)
                if bucket:
                    self._weight_order[weight] = None
                continue
            waiter, bypassed_head = candidate
            head = next(iter(queue.values()))
            if waiter is head:
                queue.popitem(last=False)
            else:
                queue.pop(id(waiter), None)
                next(iter(queue.values())).bypasses += 1
                self._bounded_bypasses += 1
            waiter.indexed = False
            self._waiters.discard(waiter)
            self._waiting_count = max(0, self._waiting_count - 1)
            self._operation_order.pop(operation_id, None)
            if queue:
                self._operation_order[operation_id] = None
            else:
                self._operation_waiters.pop(operation_id, None)
            self._refresh_operation_weight_locked(operation_id)
            if bucket:
                self._weight_order[weight] = None
            elif not self._weight_buckets.get(weight):
                self._weight_buckets.pop(weight, None)
            if skipped or bypassed_head:
                self._bounded_bypasses += skipped
            self._compact_legacy_waiters_locked()
            return waiter
        return None

    def _grant_ready_locked(self) -> list[_Waiter]:
        """Grant waiters through the incremental operation scheduler."""
        self._sync_legacy_waiters_locked()
        deliveries: list[_Waiter] = []
        while self._waiting_count:
            self._recompute_capacity_locked()
            available = self._capacity - self._in_use
            if available <= 0:
                break
            waiter = self._take_candidate_locked(available)
            if waiter is None:
                break
            waiter.state = "granted"
            waiter.granted_weight = self._effective_weight(waiter)
            self._in_use += waiter.granted_weight
            self._peak_in_use = max(self._peak_in_use, self._in_use)
            self._grants += 1
            self._last_granted_operation = waiter.operation_id
            deliveries.append(waiter)
        return deliveries

    def _deliver(self, waiters: list[_Waiter]) -> None:
        """Deliver grants without recursion, including synchronous loop doubles."""
        active = getattr(self._delivery_local, "pending", None)
        if active is not None:
            active.extend(waiters)
            return
        pending: deque[_Waiter] = deque(waiters)
        self._delivery_local.pending = pending
        try:
            while pending:
                waiter = pending.popleft()

                def deliver(current: _Waiter = waiter) -> None:
                    """Publish one grant only if cancellation has not reclaimed it."""
                    deliveries: list[_Waiter] = []
                    permit: RemoteIoPermit | None = None
                    construction_error: BaseException | None = None
                    with self._lock:
                        if current.state != "granted":
                            return
                        if current.future.cancelled() or current.future.done():
                            current.state = "cancelled"
                            self._cancellations += 1
                            self._in_use = max(0, self._in_use - current.granted_weight)
                            self._recompute_capacity_locked()
                            deliveries = self._grant_ready_locked()
                        else:
                            try:
                                permit = RemoteIoPermit(self, current.granted_weight, current.label)
                            except BaseException as exc:
                                construction_error = exc
                                current.state = "cancelled"
                                self._delivery_failures += 1
                                self._cancellations += 1
                                self._in_use = max(0, self._in_use - current.granted_weight)
                                self._recompute_capacity_locked()
                                deliveries = self._grant_ready_locked()
                            else:
                                current.state = "delivered"
                                construction_error = None
                    if construction_error is not None:
                        try:
                            current.future.set_exception(construction_error)
                        except BaseException as publication_error:
                            if not isinstance(
                                publication_error,
                                (asyncio.InvalidStateError, RuntimeError),
                            ):
                                raise
                    if permit is not None:
                        try:
                            current.future.set_result(permit)
                        except BaseException as publication_error:
                            with self._lock:
                                current.state = "cancelled"
                                self._cancellations += 1
                            try:
                                permit.release()
                            except BaseException as cleanup_error:
                                add_bounded_note(
                                    publication_error,
                                    "remote-I/O permit rollback also failed after delivery publication",
                                    cleanup_error,
                                )
                            if not isinstance(
                                publication_error,
                                (asyncio.InvalidStateError, RuntimeError),
                            ):
                                raise
                    self._deliver(deliveries)

                try:
                    waiter.loop.call_soon_threadsafe(deliver)
                except BaseException as delivery_error:
                    with self._lock:
                        if waiter.state == "granted":
                            waiter.state = "cancelled"
                            self._delivery_failures += 1
                            self._cancellations += 1
                            self._in_use = max(0, self._in_use - waiter.granted_weight)
                            self._recompute_capacity_locked()
                            pending.extend(self._grant_ready_locked())
                    try:
                        if not waiter.future.done():
                            waiter.future.set_exception(delivery_error)
                    except BaseException as publication_error:
                        if isinstance(delivery_error, (KeyboardInterrupt, SystemExit)):
                            add_bounded_note(
                                delivery_error,
                                "remote-I/O waiter failure publication also failed",
                                publication_error,
                            )
                    if isinstance(delivery_error, (KeyboardInterrupt, SystemExit)):
                        raise
        finally:
            try:
                del self._delivery_local.pending
            except AttributeError:
                pass

    def reset_after_fork(self) -> None:
        """Rebuild every derived queue, bucket, counter, and lock in the child."""
        self._lock = Lock()
        self._delivery_local = local()
        self._registrations = {}
        self._next_registration_token = 0
        self._pressure_scale = 1.0
        self._capacity = self._base_capacity
        self._in_use = 0
        self._peak_in_use = 0
        self._legacy_waiters_dirty = False
        self._waiters = _WaiterMirror(self._mark_legacy_waiters_dirty)
        self._operation_waiters = {}
        self._operation_order = OrderedDict()
        self._weight_buckets = {}
        self._weight_order = OrderedDict()
        self._operation_weights = {}
        self._bucket_capacity = self._capacity
        self._waiting_count = 0
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


_SHARED_REMOTE_IO_GOVERNOR = RemoteIoPermitGovernor(1)


def default_remote_io_permit_capacity() -> int:
    """Implement the internal default_remote_io_permit_capacity helper."""
    return _DEFAULT_CAPACITY


def shared_remote_io_permit_governor() -> RemoteIoPermitGovernor:
    """Implement the internal shared_remote_io_permit_governor helper."""
    return _SHARED_REMOTE_IO_GOVERNOR


def process_remote_io_permit_snapshot() -> RemoteIoPermitSnapshot:
    """Implement the internal process_remote_io_permit_snapshot helper."""
    return _SHARED_REMOTE_IO_GOVERNOR.snapshot()


def _reset_shared_after_fork() -> None:
    """Implement the internal _reset_shared_after_fork helper."""
    _SHARED_REMOTE_IO_GOVERNOR.reset_after_fork()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_shared_after_fork)


__all__ = [
    "RemoteIoCapacityRegistration",
    "RemoteIoPermit",
    "RemoteIoPermitGovernor",
    "RemoteIoPermitSnapshot",
    "RemoteIoSubmissionReservation",
    "default_remote_io_permit_capacity",
    "process_remote_io_permit_snapshot",
    "shared_remote_io_permit_governor",
]
