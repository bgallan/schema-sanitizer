"""Schedule asynchronous work under bounded memory and task admission.

This module coordinates fairness, cancellation, retry and result ownership, terminal debt,
diagnostics, and orderly shutdown for shared asynchronous execution.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum
from itertools import islice
from secrets import SystemRandom
from threading import Condition, Lock
from time import monotonic
from typing import AbstractSet, Any, TypeVar, cast

from ..errors import SchemaSanitizerCancelledError, SchemaSanitizerResourceError
from .cancellation import cancellable_async_sleep, check_operation_cancelled
from .memory_budget import GovernedResultOwnership

T = TypeVar("T")

_JITTER_RANDOM = SystemRandom()
_MAX_ASYNC_RETRIES = 32
_MAX_PROCESS_ASYNC_TASK_SLOTS = 256
_ASYNC_SLOT_CONTROL_BYTES = 4096
_ASYNC_ADMISSION_LOCK = Lock()
_ASYNC_ADMISSION_CONDITION = Condition(_ASYNC_ADMISSION_LOCK)
_ASYNC_TASK_SLOTS_IN_USE = 0
_ASYNC_PEAK_TASK_SLOTS = 0
_ASYNC_ACTIVE_OPERATIONS = 0
_ASYNC_REJECTIONS = 0
_ASYNC_PROTOCOL_VIOLATIONS = 0
_ASYNC_ADMISSION_CLOSED = False
_ASYNC_CORRUPTED = False
_ASYNC_TERMINAL_DEBT_CAPACITY = _MAX_PROCESS_ASYNC_TASK_SLOTS
_ASYNC_TERMINAL_DEBT_COUNT = 0
_ASYNC_TERMINAL_DEBT_REJECTIONS = 0
_ASYNC_CANCEL_TIMEOUT_SECONDS = 5.0
_ASYNC_RESULT_ESTIMATE_MAX_ITEMS = 64
_ASYNC_RESULT_ESTIMATE_MAX_DEPTH = 3
_ASYNC_DEBT_FREE = 0
_ASYNC_DEBT_ACTIVE = 1
_ASYNC_DEBT_CLAIMED = 2
_ASYNC_DEBT_RETRY_PENDING = 3
_ASYNC_DEBT_BUILDING = 4
_ASYNC_DEBT_MAX_GENERATION = (1 << 63) - 1
_ASYNC_TERMINAL_REAP_CURSOR = 0
_ASYNC_TERMINAL_REAP_FAILURES = 0
_ASYNC_RESULT_EMPTY = 0
_ASYNC_RESULT_READY = 1
_EMPTY_ASYNC_TASKS: frozenset[asyncio.Task[None]] = frozenset()


@dataclass(frozen=True, slots=True)
class AsyncSchedulerSnapshot:
    """Immutable process-wide async scheduler accounting snapshot."""

    capacity: int
    in_use: int
    peak_in_use: int
    active_operations: int
    rejected_slots: int
    admission_closed: bool = False
    terminal_debts: int = 0
    terminal_debt_rejections: int = 0
    terminal_retry_pending: int = 0
    terminal_reap_failures: int = 0
    protocol_violations: int = 0
    corrupted: bool = False


class AsyncResultOwnershipMode(Enum):
    """Describe which subsystem owns bytes after a scheduler result is handed off."""

    SCHEDULER = "scheduler"
    EXTERNALLY_GOVERNED = "externally_governed"


@dataclass(frozen=True, slots=True)
class AsyncResultMemoryContract:
    """Explicit pre/post materialization bounds and post-yield ownership model.

    ``preflight_bytes`` gates concurrent materialization. ``postflight_bytes``
    can prove the retained size after fetch. EXTERNALLY_GOVERNED means the
    payload's long-lived ownership is charged by another governor; the scheduler
    still bridges the result until the consumer adopts it.
    """

    preflight_bytes: Callable[[Any], int] | int | None
    postflight_bytes: Callable[[Any], int] | None = None
    ownership_mode: AsyncResultOwnershipMode = AsyncResultOwnershipMode.SCHEDULER
    external_ownership_capability: Callable[[Any], GovernedResultOwnership | None] | None = None


def _contract_estimators(
    memory_contract: AsyncResultMemoryContract | None,
) -> tuple[Callable[[Any], int] | None, Callable[[Any], int] | int | None]:
    """Return the estimators supplied by an asynchronous memory contract."""
    if memory_contract is None:
        return None, None
    if not isinstance(memory_contract.ownership_mode, AsyncResultOwnershipMode):
        raise TypeError("async result memory contract has an invalid ownership mode")
    return memory_contract.postflight_bytes, memory_contract.preflight_bytes


def _assert_async_result_ownership(
    value: Any, memory_contract: AsyncResultMemoryContract | None
) -> None:
    """Require proof before scheduler bridge ownership is released externally."""
    if memory_contract is None:
        return
    mode = memory_contract.ownership_mode
    if mode is AsyncResultOwnershipMode.SCHEDULER:
        return
    proof = memory_contract.external_ownership_capability
    if proof is None:
        raise RuntimeError(
            "externally governed async results require an authenticated ownership capability"
        )
    try:
        capability = proof(value)
    except BaseException:
        # The scheduler bridge lease remains live in the caller's finally path.
        raise
    if not isinstance(capability, GovernedResultOwnership):
        raise RuntimeError(
            "external async result governor did not return a runtime-issued ownership capability"
        )
    if not capability.proves_result_ownership():
        raise RuntimeError("external async result ownership capability is not live")


def _mark_async_corrupted_locked() -> None:
    """Irreversibly quarantine new admission after authoritative disagreement."""
    global _ASYNC_PROTOCOL_VIOLATIONS, _ASYNC_ADMISSION_CLOSED, _ASYNC_CORRUPTED
    if _ASYNC_PROTOCOL_VIOLATIONS < (1 << 63) - 1:
        _ASYNC_PROTOCOL_VIOLATIONS += 1
    _ASYNC_CORRUPTED = True
    _ASYNC_ADMISSION_CLOSED = True


class _AsyncTaskDomainLease:
    """Exact async-task domain owned by one StageConcurrencyAdmission.

    Slot and operation ownership retire independently.  If either authoritative
    counter disagrees, the runtime closes new admission but keeps the unresolved
    component live so an explicit cleanup retry can still consume the exact
    capability after diagnostics/reconciliation.
    """

    __slots__ = (
        "amount",
        "_counts_operation",
        "_released",
        "_slots_released",
        "_operation_released",
    )

    def __init__(self, amount: int, *, counts_operation: bool = True) -> None:
        """Initialize the async task domain lease and its owned runtime state."""
        self.amount = amount
        self._counts_operation = counts_operation
        self._released = False
        self._slots_released = False
        self._operation_released = not counts_operation

    def release(self) -> None:
        """Release resources owned by this async task domain lease."""
        global _ASYNC_TASK_SLOTS_IN_USE, _ASYNC_ACTIVE_OPERATIONS
        with _ASYNC_ADMISSION_CONDITION:
            if self._released:
                return
            if not self._slots_released:
                if self.amount < 0 or _ASYNC_TASK_SLOTS_IN_USE < self.amount:
                    _mark_async_corrupted_locked()
                else:
                    next_task_slots = _ASYNC_TASK_SLOTS_IN_USE - self.amount
                    _ASYNC_TASK_SLOTS_IN_USE = next_task_slots
                    self._slots_released = True
            if not self._operation_released:
                if _ASYNC_ACTIVE_OPERATIONS <= 0:
                    _mark_async_corrupted_locked()
                else:
                    next_active_operations = _ASYNC_ACTIVE_OPERATIONS - 1
                    _ASYNC_ACTIVE_OPERATIONS = next_active_operations
                    self._operation_released = True
            # Exactly-once commit: only destroy the aggregate capability after
            # every owned component has actually retired.
            if self._slots_released and self._operation_released:
                self._released = True
            _ASYNC_ADMISSION_CONDITION.notify_all()
            if not self._released:
                # StageConcurrencyAdmission removes a domain capability only
                # after release() returns successfully. Raising here therefore
                # keeps this exact partially-retired lease rooted for retry.
                raise RuntimeError(
                    "async task-domain cleanup did not commit; admission quarantined"
                )


@dataclass(slots=True)
class _AsyncSchedulerAdmission:
    slots: int
    stage_admission: object | None = None
    borrowed_stage_admission: object | None = None
    _close_lock: Lock = field(default_factory=Lock, repr=False)

    def close(self) -> None:
        """Release each owned generation once; keep failed ownership retryable."""
        with self._close_lock:
            borrowed = self.borrowed_stage_admission
            if borrowed is not None:
                close = getattr(borrowed, "close", None)
                if callable(close):
                    close()
                # Clear only after cleanup commits. A throwing close retains the
                # exact capability for a later terminal/shutdown retry.
                self.borrowed_stage_admission = None
            stage = self.stage_admission
            if stage is not None:
                close = getattr(stage, "close", None)
                if callable(close):
                    close()
                self.stage_admission = None
            self.slots = 0


def _reap_one_async_terminal_debt() -> bool:
    """Implemented after the preallocated terminal-debt banks are defined."""
    return _reap_one_async_terminal_debt_impl()


def _reap_async_terminal_debts() -> None:
    """Strictly reap debts; cleanup failures remain visible to explicit callers."""
    while _reap_one_async_terminal_debt():
        pass


def _try_reap_async_terminal_debts() -> int:
    """Best-effort reaping for admission/diagnostics; never let cleanup poison them."""
    reaped = 0
    # Round-robin claiming means one permanently failing debt cannot starve
    # completed debts behind it. Bound the pass by the fixed debt bank size.
    for _attempt in range(_ASYNC_TERMINAL_DEBT_CAPACITY):
        try:
            if not _reap_one_async_terminal_debt():
                break
            reaped += 1
        except BaseException:
            continue
    return reaped


def _fair_async_candidate(requested: int) -> int:
    """Return the guaranteed fair share before opportunistic borrowing."""
    with _ASYNC_ADMISSION_CONDITION:
        if _ASYNC_ADMISSION_CLOSED or _ASYNC_CORRUPTED:
            raise SchemaSanitizerResourceError(
                "async scheduler admission is closed",
                detail={
                    "stage": "async_scheduler",
                    "limit_name": "async_scheduler_admission",
                    "limit_items": 0,
                    "actual_items": requested,
                },
            )
        available = max(0, _MAX_PROCESS_ASYNC_TASK_SLOTS - _ASYNC_TASK_SLOTS_IN_USE)
        if available <= 0:
            return 0
        if _MAX_PROCESS_ASYNC_TASK_SLOTS <= 2:
            return min(requested, available)
        contenders = max(2, _ASYNC_ACTIVE_OPERATIONS + 1)
        fair_share = max(1, _MAX_PROCESS_ASYNC_TASK_SLOTS // contenders)
        return min(requested, available, fair_share)


def _acquire_async_task_domain_exact(
    slots: int, *, counts_operation: bool = True
) -> _AsyncTaskDomainLease:
    """Acquire async task domain exact."""
    global _ASYNC_TASK_SLOTS_IN_USE, _ASYNC_PEAK_TASK_SLOTS, _ASYNC_ACTIVE_OPERATIONS
    with _ASYNC_ADMISSION_CONDITION:
        if _ASYNC_ADMISSION_CLOSED or _ASYNC_CORRUPTED:
            raise SchemaSanitizerResourceError("async scheduler admission is closed")
        available = max(0, _MAX_PROCESS_ASYNC_TASK_SLOTS - _ASYNC_TASK_SLOTS_IN_USE)
        if slots <= 0 or slots > available:
            raise SchemaSanitizerResourceError(
                "async task-slot capacity exhausted",
                detail={
                    "stage": "async_scheduler",
                    "limit_items": available,
                    "actual_items": slots,
                },
            )
        _ASYNC_TASK_SLOTS_IN_USE += slots
        if counts_operation:
            _ASYNC_ACTIVE_OPERATIONS += 1
        _ASYNC_PEAK_TASK_SLOTS = max(_ASYNC_PEAK_TASK_SLOTS, _ASYNC_TASK_SLOTS_IN_USE)
    return _AsyncTaskDomainLease(slots, counts_operation=counts_operation)


def _acquire_async_scheduler_admission(requested: int) -> _AsyncSchedulerAdmission:
    """Reserve async Tasks through the process-wide multi-domain stage broker."""
    global _ASYNC_REJECTIONS
    _try_reap_async_terminal_debts()
    requested = max(1, min(int(requested), _MAX_PROCESS_ASYNC_TASK_SLOTS))
    candidate = _fair_async_candidate(requested)
    if candidate <= 0:
        with _ASYNC_ADMISSION_CONDITION:
            _ASYNC_REJECTIONS += requested
        return _AsyncSchedulerAdmission(0)
    from .memory_budget import acquire_stage_concurrency_admission

    while candidate > 0:
        try:
            stage = acquire_stage_concurrency_admission(
                candidate,
                per_slot_bytes=_ASYNC_SLOT_CONTROL_BYTES,
                reserve_bytes=0,
                stage="async_scheduler",
                physical_threads=False,
                domain_acquirers={"async_task": _acquire_async_task_domain_exact},
            )
        except SchemaSanitizerResourceError:
            candidate //= 2
            continue
        granted = int(getattr(stage, "slots", 0))
        if granted > 0:
            return _AsyncSchedulerAdmission(granted, stage)
        close = getattr(stage, "close", None)
        if callable(close):
            close()
        candidate //= 2
    with _ASYNC_ADMISSION_CONDITION:
        _ASYNC_REJECTIONS += requested
    return _AsyncSchedulerAdmission(0)


def _record_async_admission_shortfall(requested: int, granted: int) -> None:
    """Count only capacity that remains unavailable after fair idle borrowing."""
    global _ASYNC_REJECTIONS
    shortfall = max(0, int(requested) - max(0, int(granted)))
    if shortfall <= 0:
        return
    with _ASYNC_ADMISSION_CONDITION:
        _ASYNC_REJECTIONS += shortfall


def _borrow_idle_async_capacity(admission: _AsyncSchedulerAdmission, requested: int) -> None:
    """Borrow idle slots after queued contenders had one scheduling turn.

    Borrowing happens before worker Tasks are created, so the extra capability
    never needs unsafe preemption. Concurrent operations that entered during
    the scheduling turn retain their guaranteed shares; otherwise the sole
    operation becomes fully work-conserving.
    """
    if admission.slots <= 0 or admission.borrowed_stage_admission is not None:
        return
    with _ASYNC_ADMISSION_CONDITION:
        if _ASYNC_ADMISSION_CLOSED or _ASYNC_CORRUPTED:
            return
        active = max(1, _ASYNC_ACTIVE_OPERATIONS)
        available = max(0, _MAX_PROCESS_ASYNC_TASK_SLOTS - _ASYNC_TASK_SLOTS_IN_USE)
        if available <= 0:
            return
        if active == 1:
            target = requested
        else:
            target = min(requested, max(1, _MAX_PROCESS_ASYNC_TASK_SLOTS // active))
        borrow = min(available, max(0, target - admission.slots))
    if borrow <= 0:
        return
    from .memory_budget import acquire_stage_concurrency_admission

    try:
        stage = acquire_stage_concurrency_admission(
            borrow,
            per_slot_bytes=_ASYNC_SLOT_CONTROL_BYTES,
            reserve_bytes=0,
            stage="async_scheduler_borrow",
            physical_threads=False,
            domain_acquirers={
                "async_task": lambda slots: _acquire_async_task_domain_exact(
                    slots, counts_operation=False
                )
            },
        )
    except SchemaSanitizerResourceError:
        return
    granted = int(getattr(stage, "slots", 0))
    if granted <= 0:
        close = getattr(stage, "close", None)
        if callable(close):
            close()
        return
    admission.borrowed_stage_admission = stage
    admission.slots += granted


async def _bounded_async_event_wait(event: asyncio.Event, *, stage: str) -> None:
    """Wait in bounded slices so shutdown/cancellation can always be observed."""
    while not event.is_set():
        check_operation_cancelled(stage=stage)
        waiter = event.wait
        try:
            # Keep the Event wait in this Task. On Python 3.11, wait_for()
            # delegates to an inner Task whose simultaneous completion and
            # outer cancellation can delay worker shutdown until the terminal
            # debt deadline. The structured timeout distinguishes its own
            # expiry from an external cancellation and propagates the latter.
            async with asyncio.timeout(0.25):
                await waiter()
        except TimeoutError:
            continue


class _AsyncWorkerResultSlot:
    """Preallocated worker-owned result envelope.

    Publication performs only field mutation plus ``Event.set``.  The worker may
    not materialize a second result until the consumer releases this exact slot,
    so retained result ownership is bounded by the admitted worker window.
    """

    __slots__ = (
        "index",
        "value",
        "error",
        "retained_lease",
        "state",
        "available",
        "ready_event",
    )

    def __init__(self, ready_event: asyncio.Event) -> None:
        """Initialize the async worker result slot and its owned runtime state."""
        self.index = -1
        self.value: Any = None
        self.error: BaseException | None = None
        self.retained_lease: object | None = None
        self.state = _ASYNC_RESULT_EMPTY
        self.available = asyncio.Event()
        self.available.set()
        self.ready_event = ready_event

    async def claim_empty(self) -> None:
        """Claim the empty result slot for publication."""
        await _bounded_async_event_wait(self.available, stage="async_result_slot")
        self.available.clear()
        if self.state != _ASYNC_RESULT_EMPTY:
            raise RuntimeError("async worker result slot ownership mismatch")

    def publish(
        self,
        index: int,
        value: Any,
        error: BaseException | None,
        retained_lease: object | None,
    ) -> None:
        """Publish the prepared value."""
        if self.state != _ASYNC_RESULT_EMPTY:
            raise RuntimeError("async worker result slot double publication")
        # Commit state last: a scanner can never observe READY with partially
        # initialized fields.
        self.index = index
        self.value = value
        self.error = error
        self.retained_lease = retained_lease
        self.state = _ASYNC_RESULT_READY
        self.ready_event.set()

    def take(self) -> tuple[int, Any, BaseException | None, object | None]:
        """Remove and return the retained value."""
        if self.state != _ASYNC_RESULT_READY:
            raise RuntimeError("async worker result slot is not ready")
        index = self.index
        value = self.value
        error = self.error
        lease = self.retained_lease
        self.index = -1
        self.value = None
        self.error = None
        self.retained_lease = None
        self.state = _ASYNC_RESULT_EMPTY
        self.available.set()
        return index, value, error, lease

    def terminal_release(self) -> bool:
        """Release late retained ownership; clear only after close commits."""
        if self.state != _ASYNC_RESULT_READY:
            return True
        lease = self.retained_lease
        if lease is not None:
            _release_async_result_lease(lease)
        self.index = -1
        self.value = None
        self.error = None
        self.retained_lease = None
        self.state = _ASYNC_RESULT_EMPTY
        self.available.set()
        return True


class _AsyncTerminalTaskSlot:
    """One preallocated task root used by exactly one terminal debt group."""

    __slots__ = ("task", "next_index", "group_index", "active")

    def __init__(self) -> None:
        """Initialize the async terminal task slot and its owned runtime state."""
        self.task: asyncio.Task[None] | None = None
        self.next_index = -1
        self.group_index = -1
        self.active = False

    def clear(self) -> None:
        """Clear values and ownership retained by this async terminal task slot."""
        self.task = None
        self.next_index = -1
        self.group_index = -1
        self.active = False


class _AsyncTerminalDebt:
    """Transactional owner for one cancellation-resistant worker group.

    ACTIVE/RETRY_PENDING are published but unclaimed. CLAIMED has exactly one
    reaper generation. Cleanup executes outside the scheduler lock, then either
    commits FREE or rolls back to RETRY_PENDING without exposing a second owner.
    """

    __slots__ = (
        "head_task_slot",
        "task_count",
        "admission",
        "result_slots",
        "pending_slots",
        "building_tasks",
        "state",
        "generation",
        "retry_count",
    )

    def __init__(self) -> None:
        """Initialize the async terminal debt and its owned runtime state."""
        self.head_task_slot = -1
        self.task_count = 0
        self.admission: _AsyncSchedulerAdmission | None = None
        self.result_slots: list[_AsyncWorkerResultSlot] | None = None
        self.pending_slots: list[_AsyncPendingResultSlot] | None = None
        self.building_tasks: AbstractSet[asyncio.Task[None]] | None = None
        self.state = _ASYNC_DEBT_FREE
        self.generation = 0
        self.retry_count = 0

    @property
    def active(self) -> bool:
        """Return whether the governed state is active."""
        return self.state != _ASYNC_DEBT_FREE

    def all_done(self) -> bool:
        """Return whether all tracked operations have completed."""
        building = self.building_tasks
        if self.state == _ASYNC_DEBT_BUILDING and building is not None:
            for building_task in building:
                if not building_task.done():
                    return False
        cursor = self.head_task_slot
        seen = 0
        while cursor >= 0:
            slot = _ASYNC_TERMINAL_TASK_BANK[cursor]
            task = slot.task
            if task is not None and not task.done():
                return False
            next_seen = seen + 1
            cursor = slot.next_index
            seen = next_seen
            if seen > self.task_count:
                raise RuntimeError("async terminal task chain corruption")
        return seen == self.task_count

    def try_claim_reaping(self) -> int | None:
        """Attempt to claim responsibility for reaping completed work."""
        if self.state not in (_ASYNC_DEBT_BUILDING, _ASYNC_DEBT_ACTIVE, _ASYNC_DEBT_RETRY_PENDING):
            return None
        if not self.all_done():
            return None
        self.state = _ASYNC_DEBT_CLAIMED
        return self.generation

    def rollback_reaping(self, generation: int) -> None:
        """Return reaping responsibility after an interrupted claim."""
        if self.state != _ASYNC_DEBT_CLAIMED or self.generation != generation:
            raise RuntimeError("async terminal debt claim corruption")
        next_retry_count = self.retry_count + 1
        self.retry_count = next_retry_count
        self.state = _ASYNC_DEBT_RETRY_PENDING

    def commit_free(self, generation: int) -> None:
        """Commit completion and release this terminal-debt slot."""
        if self.state != _ASYNC_DEBT_CLAIMED or self.generation != generation:
            raise RuntimeError("async terminal debt state corruption")
        next_generation = (self.generation + 1) & _ASYNC_DEBT_MAX_GENERATION
        cursor = self.head_task_slot
        seen = 0
        while cursor >= 0:
            slot = _ASYNC_TERMINAL_TASK_BANK[cursor]
            next_index = slot.next_index
            next_seen = seen + 1
            slot.clear()
            cursor = next_index
            seen = next_seen
            if seen > self.task_count:
                raise RuntimeError("async terminal task chain corruption")
        self.head_task_slot = -1
        self.task_count = 0
        self.admission = None
        self.result_slots = None
        self.pending_slots = None
        self.building_tasks = None
        self.retry_count = 0
        self.generation = next_generation
        self.state = _ASYNC_DEBT_FREE


_ASYNC_TERMINAL_TASK_BANK: list[_AsyncTerminalTaskSlot] = [
    _AsyncTerminalTaskSlot() for _ in range(_MAX_PROCESS_ASYNC_TASK_SLOTS)
]
_ASYNC_TERMINAL_DEBTS: list[_AsyncTerminalDebt] = [
    _AsyncTerminalDebt() for _ in range(_ASYNC_TERMINAL_DEBT_CAPACITY)
]
if _ASYNC_TERMINAL_DEBT_CAPACITY < _MAX_PROCESS_ASYNC_TASK_SLOTS:
    raise RuntimeError("async terminal debt capacity must cover every live task slot")

# Explicitly account the fixed terminal banks from their actual Python object
# footprints instead of a coarse per-slot multiplier. The per-operation result
# slots remain charged separately by async scheduler admission.
_ASYNC_TERMINAL_BANK_STATIC_BYTES = (
    sys.getsizeof(_ASYNC_TERMINAL_TASK_BANK)
    + sum(sys.getsizeof(slot) for slot in _ASYNC_TERMINAL_TASK_BANK)
    + sys.getsizeof(_ASYNC_TERMINAL_DEBTS)
    + sum(sys.getsizeof(debt) for debt in _ASYNC_TERMINAL_DEBTS)
)
try:
    from .static_control_plane import register_static_control_plane as _register_async_static

    _register_async_static(
        "async_terminal_ownership_banks",
        _ASYNC_TERMINAL_BANK_STATIC_BYTES,
    )
except (ImportError, AttributeError):
    pass


def _reap_one_async_terminal_debt_impl() -> bool:
    """Claim and transactionally reap exactly one completed debt generation."""
    global _ASYNC_TERMINAL_DEBT_COUNT, _ASYNC_TERMINAL_REAP_CURSOR
    global _ASYNC_TERMINAL_REAP_FAILURES, _ASYNC_PROTOCOL_VIOLATIONS
    debt: _AsyncTerminalDebt | None = None
    claim_generation: int | None = None
    with _ASYNC_ADMISSION_CONDITION:
        for offset in range(_ASYNC_TERMINAL_DEBT_CAPACITY):
            index = (_ASYNC_TERMINAL_REAP_CURSOR + offset) % _ASYNC_TERMINAL_DEBT_CAPACITY
            next_cursor = (index + 1) % _ASYNC_TERMINAL_DEBT_CAPACITY
            candidate = _ASYNC_TERMINAL_DEBTS[index]
            generation = candidate.try_claim_reaping()
            if generation is not None:
                debt = candidate
                claim_generation = generation
                _ASYNC_TERMINAL_REAP_CURSOR = next_cursor
                break
    if debt is None or claim_generation is None:
        return False

    try:
        slots = debt.result_slots
        if slots is not None:
            for result_slot in slots:
                result_slot.terminal_release()
        pending_slots = debt.pending_slots
        if pending_slots is not None:
            for pending_slot in pending_slots:
                pending_slot.terminal_release()
        admission = debt.admission
        if admission is not None:
            admission.close()
    except BaseException:
        with _ASYNC_ADMISSION_CONDITION:
            debt.rollback_reaping(claim_generation)
            _ASYNC_TERMINAL_REAP_FAILURES += 1
            _ASYNC_ADMISSION_CONDITION.notify_all()
        raise

    with _ASYNC_ADMISSION_CONDITION:
        if _ASYNC_TERMINAL_DEBT_COUNT <= 0:
            _ASYNC_PROTOCOL_VIOLATIONS += 1
            debt.rollback_reaping(claim_generation)
            _ASYNC_ADMISSION_CONDITION.notify_all()
            return False
        next_debt_count = _ASYNC_TERMINAL_DEBT_COUNT - 1
        debt.commit_free(claim_generation)
        _ASYNC_TERMINAL_DEBT_COUNT = next_debt_count
        _ASYNC_ADMISSION_CONDITION.notify_all()
    return True


def _park_async_terminal_debt(
    tasks: AbstractSet[asyncio.Task[None]],
    admission: _AsyncSchedulerAdmission,
    result_slots: list[_AsyncWorkerResultSlot] | None,
    pending_slots: list[_AsyncPendingResultSlot] | None = None,
    *,
    reap_completed: bool = True,
) -> bool:
    """Transfer terminal ownership into fixed debt/task banks without growth."""
    global _ASYNC_TERMINAL_DEBT_COUNT, _ASYNC_TERMINAL_DEBT_REJECTIONS
    global _ASYNC_TERMINAL_REAP_FAILURES
    if (
        not tasks
        and result_slots is None
        and pending_slots is None
        and admission.slots <= 0
        and admission.stage_admission is None
        and admission.borrowed_stage_admission is None
    ):
        return False
    # Publication is the ownership commit. Never make it conditional on
    # unrelated cleanup: an old poison debt cannot prevent the new owner from
    # entering the fixed bank. Reaping is opportunistic only after commit.
    with _ASYNC_ADMISSION_CONDITION:
        debt_index = -1
        for index, candidate in enumerate(_ASYNC_TERMINAL_DEBTS):
            if candidate.state == _ASYNC_DEBT_FREE:
                debt_index = index
                break
        if debt_index < 0:
            _ASYNC_TERMINAL_DEBT_REJECTIONS += 1
            raise RuntimeError("async terminal debt capacity invariant violated")
        free_count = 0
        for slot in _ASYNC_TERMINAL_TASK_BANK:
            if not slot.active:
                free_count += 1
        if free_count < len(tasks):
            _ASYNC_TERMINAL_DEBT_REJECTIONS += 1
            raise RuntimeError("async terminal task-bank capacity invariant violated")

        debt = _ASYNC_TERMINAL_DEBTS[debt_index]
        next_debt_count = _ASYNC_TERMINAL_DEBT_COUNT + 1
        # BUILDING is already an authoritative owner. Root the caller's existing
        # task set before touching individual task-bank slots so any later
        # MemoryError leaves a complete recoverable owner graph.
        debt.head_task_slot = -1
        debt.task_count = 0
        debt.admission = admission
        debt.result_slots = result_slots
        debt.pending_slots = pending_slots
        debt.building_tasks = tasks
        debt.state = _ASYNC_DEBT_BUILDING
        _ASYNC_TERMINAL_DEBT_COUNT = next_debt_count

        tail = -1
        published = 0
        search_from = 0
        try:
            for task in tasks:
                chosen = -1
                for offset in range(_MAX_PROCESS_ASYNC_TASK_SLOTS):
                    candidate_index = (search_from + offset) % _MAX_PROCESS_ASYNC_TASK_SLOTS
                    if not _ASYNC_TERMINAL_TASK_BANK[candidate_index].active:
                        chosen = candidate_index
                        search_from = (candidate_index + 1) % _MAX_PROCESS_ASYNC_TASK_SLOTS
                        break
                if chosen < 0:
                    raise RuntimeError("async terminal task-bank publication invariant violated")
                next_published = published + 1
                task_slot = _ASYNC_TERMINAL_TASK_BANK[chosen]
                task_slot.task = task
                task_slot.group_index = debt_index
                task_slot.next_index = -1
                task_slot.active = True
                if tail >= 0:
                    _ASYNC_TERMINAL_TASK_BANK[tail].next_index = chosen
                else:
                    debt.head_task_slot = chosen
                tail = chosen
                published = next_published
                debt.task_count = published
            debt.building_tasks = None
            debt.state = _ASYNC_DEBT_ACTIVE
        except BaseException:
            # Ownership already committed in BUILDING. Do not re-raise and cause
            # the caller to run a second cleanup path against the same owners.
            _ASYNC_TERMINAL_REAP_FAILURES += 1
        _ASYNC_ADMISSION_CONDITION.notify_all()
    # Do not immediately reap the just-published generation. This keeps
    # publication independent from cleanup and preserves explicit reaper error
    # observability; admission/snapshot/shutdown perform best-effort reaping.
    return True


def close_async_scheduler_admission() -> None:
    """Prevent new async worker pools from entering during runtime shutdown."""
    global _ASYNC_ADMISSION_CLOSED
    with _ASYNC_ADMISSION_CONDITION:
        _ASYNC_ADMISSION_CLOSED = True
        _ASYNC_ADMISSION_CONDITION.notify_all()


def wait_async_scheduler_quiescent(timeout_seconds: float) -> bool:
    """Wait within a caller-owned deadline and reap completed terminal debts."""
    deadline = monotonic() + max(0.0, float(timeout_seconds))
    while True:
        _try_reap_async_terminal_debts()
        with _ASYNC_ADMISSION_CONDITION:
            if (
                not _ASYNC_ACTIVE_OPERATIONS
                and not _ASYNC_TERMINAL_DEBT_COUNT
                and not _ASYNC_PROTOCOL_VIOLATIONS
            ):
                return True
            remaining = deadline - monotonic()
            if remaining <= 0:
                return False
            _ASYNC_ADMISSION_CONDITION.wait(timeout=min(0.05, remaining))


def reopen_async_scheduler_for_tests() -> None:
    """Reopen admission only when no live operation/debt exists."""
    global _ASYNC_ADMISSION_CLOSED
    with _ASYNC_ADMISSION_CONDITION:
        if (
            _ASYNC_ACTIVE_OPERATIONS
            or _ASYNC_TERMINAL_DEBT_COUNT
            or _ASYNC_PROTOCOL_VIOLATIONS
            or _ASYNC_CORRUPTED
        ):
            raise RuntimeError(
                "cannot reopen async scheduler with live operations or protocol violations"
            )
        _ASYNC_ADMISSION_CLOSED = False
        _ASYNC_ADMISSION_CONDITION.notify_all()


def async_scheduler_snapshot() -> AsyncSchedulerSnapshot:
    """Return scheduler capacity, usage, debt, and corruption diagnostics."""
    _try_reap_async_terminal_debts()
    with _ASYNC_ADMISSION_CONDITION:
        retry_pending = sum(
            debt.state == _ASYNC_DEBT_RETRY_PENDING for debt in _ASYNC_TERMINAL_DEBTS
        )
        return AsyncSchedulerSnapshot(
            _MAX_PROCESS_ASYNC_TASK_SLOTS,
            _ASYNC_TASK_SLOTS_IN_USE,
            _ASYNC_PEAK_TASK_SLOTS,
            _ASYNC_ACTIVE_OPERATIONS,
            _ASYNC_REJECTIONS,
            _ASYNC_ADMISSION_CLOSED,
            _ASYNC_TERMINAL_DEBT_COUNT,
            _ASYNC_TERMINAL_DEBT_REJECTIONS,
            retry_pending,
            _ASYNC_TERMINAL_REAP_FAILURES,
            _ASYNC_PROTOCOL_VIOLATIONS,
            _ASYNC_CORRUPTED,
        )


def retry_delay(attempt: int) -> float:
    """Return jittered exponential backoff delay for remote I/O retries."""
    bounded_attempt = min(max(attempt, 0), 16)
    return min(8.0, 0.25 * (2**bounded_attempt)) + _JITTER_RANDOM.uniform(0.0, 0.25)


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    *,
    retries: int,
    should_retry: Callable[[Exception], bool] | None = None,
    throttle_key: str | None = None,
) -> T:
    """Run one async operation with bounded retry/backoff."""
    bounded_retries = min(max(int(retries), 0), _MAX_ASYNC_RETRIES)
    for attempt in range(bounded_retries + 1):
        check_operation_cancelled(stage="async_retry")
        lease = None
        try:
            if throttle_key is not None:
                from ..remote_impl.provider_throttle import acquire_provider_request

                lease = await acquire_provider_request(throttle_key)
            result = await operation()
        except SchemaSanitizerCancelledError:
            if lease is not None:
                lease.release()
            raise
        except asyncio.CancelledError:
            if lease is not None:
                lease.release()
            raise
        except Exception as exc:
            if lease is not None:
                lease.failure(exc)
            retryable = should_retry(exc) if should_retry is not None else True
            if attempt >= bounded_retries or not retryable:
                raise
            await cancellable_async_sleep(retry_delay(attempt), stage="async_retry_backoff")
            continue
        except BaseException:
            if lease is not None:
                lease.release()
            raise

        # The user operation has already completed successfully.  Keep success
        # accounting outside the retry exception handler so an instrumentation
        # failure cannot repeat a non-idempotent operation.
        if lease is not None:
            lease.success()
        # Success is the irreversible delivery commit. A cancellation arriving
        # after the provider/user operation completed must not turn that success
        # into an apparent failure that an outer layer could retry.
        return result
    raise RuntimeError("unreachable async retry state")


def _estimate_async_result_bytes(value: object, *, _depth: int = 0) -> int:
    """Conservative bounded-sampling diagnostic estimate; never hard admission."""
    if value is None:
        return 32
    kind = type(value)
    if kind is bytes or kind is bytearray:
        return 64 + len(cast(bytes | bytearray, value))
    if kind is memoryview:
        return 128 + cast(memoryview, value).nbytes
    if kind is str:
        return 64 + len(cast(str, value)) * 4
    if kind in (int, float, bool):
        return 64
    if _depth >= _ASYNC_RESULT_ESTIMATE_MAX_DEPTH:
        if kind in (tuple, list):
            return 128 + len(cast(tuple[object, ...] | list[object], value)) * 128
        if kind is dict:
            return 256 + len(cast(dict[object, object], value)) * 256
        return 128
    if kind is tuple or kind is list:
        sequence = cast(tuple[object, ...] | list[object], value)
        length = len(sequence)
        total = 128 + length * 16
        sampled = 0
        sampled_bytes = 0
        for item in sequence:
            if sampled >= _ASYNC_RESULT_ESTIMATE_MAX_ITEMS:
                break
            charge = _estimate_async_result_bytes(item, _depth=_depth + 1)
            total += charge
            sampled_bytes += charge
            sampled += 1
        remaining = length - sampled
        if remaining > 0:
            average = max(128, (sampled_bytes + max(1, sampled) - 1) // max(1, sampled))
            total += remaining * average
        return total
    if kind is dict:
        mapping = cast(dict[object, object], value)
        length = len(mapping)
        total = 256 + length * 32
        sampled = 0
        sampled_bytes = 0
        for key, item in mapping.items():
            if sampled >= _ASYNC_RESULT_ESTIMATE_MAX_ITEMS:
                break
            charge = _estimate_async_result_bytes(key, _depth=_depth + 1)
            charge += _estimate_async_result_bytes(item, _depth=_depth + 1)
            total += charge
            sampled_bytes += charge
            sampled += 1
        remaining = length - sampled
        if remaining > 0:
            average = max(256, (sampled_bytes + max(1, sampled) - 1) // max(1, sampled))
            total += remaining * average
        return total
    return 128


def _known_async_result_upper_bound(value: object, *, _depth: int = 0) -> int | None:
    """Return a safe bound only for bounded, fully inspected builtin graphs."""
    if value is None:
        return 32
    kind = type(value)
    if kind is bytes or kind is bytearray:
        return 64 + len(cast(bytes | bytearray, value))
    if kind is memoryview:
        return 128 + cast(memoryview, value).nbytes
    if kind is str:
        return 64 + len(cast(str, value)) * 4
    if kind in (int, float, bool):
        return 64
    if _depth >= _ASYNC_RESULT_ESTIMATE_MAX_DEPTH:
        return None
    if kind is tuple or kind is list:
        sequence = cast(tuple[object, ...] | list[object], value)
        length = len(sequence)
        if length > _ASYNC_RESULT_ESTIMATE_MAX_ITEMS:
            return None
        total = 128 + length * 16
        for item in sequence:
            charge = _known_async_result_upper_bound(item, _depth=_depth + 1)
            if charge is None:
                return None
            total += charge
        return total
    if kind is dict:
        mapping = cast(dict[object, object], value)
        length = len(mapping)
        if length > _ASYNC_RESULT_ESTIMATE_MAX_ITEMS:
            return None
        total = 256 + length * 32
        for key, item in mapping.items():
            key_charge = _known_async_result_upper_bound(key, _depth=_depth + 1)
            value_charge = _known_async_result_upper_bound(item, _depth=_depth + 1)
            if key_charge is None or value_charge is None:
                return None
            total += key_charge + value_charge
        return total
    return None


def _preflight_async_result_bytes(index: int, estimator: Callable[[int], int] | int | None) -> int:
    """Estimate bytes that must be admitted before an asynchronous result runs."""
    if estimator is None:
        return 0
    retained = estimator(index) if callable(estimator) else estimator
    if isinstance(retained, bool) or not isinstance(retained, int) or retained < 0:
        raise SchemaSanitizerResourceError(
            "async result preflight_bytes must return a non-negative integer"
        )
    return retained


def _release_async_result_lease(lease: object | None) -> None:
    """Release async result lease."""
    if lease is None:
        return
    close = getattr(lease, "close", None)
    if callable(close):
        close()


def _async_result_postflight_bound(
    value: Any, estimator: Callable[[Any], int] | None
) -> int | None:
    """Measure and validate the retained size of an asynchronous result."""
    if estimator is None:
        return _known_async_result_upper_bound(value)
    retained = estimator(value)
    if isinstance(retained, bool) or not isinstance(retained, int) or retained < 0:
        raise SchemaSanitizerResourceError(
            "async result postflight_bytes must return a non-negative integer"
        )
    return retained


async def _fetch_with_result_admission(
    index: int,
    fetch: Callable[[int], Awaitable[Any]],
    postflight_bytes: Callable[[Any], int] | None,
    preflight_bytes: Callable[[int], int] | int | None,
) -> tuple[Any, object | None]:
    """Fetch one value; concurrent callers require a pre-materialization bound."""
    from .memory_budget import acquire_operation_memory

    result_lease: object | None = None
    value: Any = None
    expected = _preflight_async_result_bytes(index, preflight_bytes)
    if preflight_bytes is not None and expected > 0:
        result_lease = acquire_operation_memory(expected, stage="async_result_preflight")
    try:
        value = await fetch(index)
        retained_bound = _async_result_postflight_bound(value, postflight_bytes)
        if preflight_bytes is not None:
            # preflight_bytes is an explicit upper-bound contract. When
            # the result is inspectable, reject/expand a violated declaration.
            if retained_bound is not None and retained_bound > expected:
                if result_lease is None:
                    result_lease = acquire_operation_memory(
                        retained_bound, stage="async_result_contract_violation"
                    )
                else:
                    resize = getattr(result_lease, "resize", None)
                    if callable(resize):
                        resize(retained_bound)
            return value, result_lease
        # No preflight contract means the scheduler has already degraded to the
        # single caller coroutine. Charge only a proven post-materialization
        # bound; UNKNOWN is deliberately not represented as a fake 128-byte cap.
        if retained_bound is not None and retained_bound > 0:
            result_lease = acquire_operation_memory(retained_bound, stage="async_result_single")
        return value, result_lease
    except BaseException:
        _release_async_result_lease(result_lease)
        value = None
        raise


async def _indexed_worker(
    indices: asyncio.Queue[int],
    result_slot: _AsyncWorkerResultSlot,
    fetch: Callable[[int], Awaitable[Any]],
    postflight_bytes: Callable[[Any], int] | None = None,
    preflight_bytes: Callable[[int], int] | int | None = None,
) -> None:
    """Fetch and publish into one preallocated terminal-safe result envelope."""
    while True:
        check_operation_cancelled(stage="async_worker")
        await result_slot.claim_empty()
        try:
            index = await indices.get()
        except BaseException:
            result_slot.available.set()
            raise
        result_lease: object | None = None
        try:
            try:
                value, result_lease = await _fetch_with_result_admission(
                    index, fetch, postflight_bytes, preflight_bytes
                )
            except asyncio.CancelledError:
                result_slot.available.set()
                raise
            except BaseException as exc:
                result_slot.publish(index, None, exc, None)
            else:
                try:
                    result_slot.publish(index, value, None, result_lease)
                    result_lease = None
                except BaseException:
                    _release_async_result_lease(result_lease)
                    result_slot.available.set()
                    raise
        finally:
            indices.task_done()


def _start_indexed_workers(
    worker_count: int,
    indices: asyncio.Queue[int],
    result_slots: list[_AsyncWorkerResultSlot],
    fetch: Callable[[int], Awaitable[Any]],
    postflight_bytes: Callable[[Any], int] | None = None,
    preflight_bytes: Callable[[int], int] | int | None = None,
) -> list[asyncio.Task[None]]:
    """Start a fixed worker pool with one preallocated result slot per worker."""
    return [
        asyncio.create_task(
            _indexed_worker(
                indices,
                result_slots[index],
                fetch,
                postflight_bytes,
                preflight_bytes,
            )
        )
        for index in range(worker_count)
    ]


async def _await_async_result(
    result_slots: list[_AsyncWorkerResultSlot], ready_event: asyncio.Event
) -> tuple[int, Any, BaseException | None, object | None]:
    """Take any ready preallocated result without creating waiter Tasks."""
    while True:
        for slot in result_slots:
            if slot.state == _ASYNC_RESULT_READY:
                return slot.take()
        ready_event.clear()
        # Recheck after clear to close producer-set vs consumer-clear races.
        for slot in result_slots:
            if slot.state == _ASYNC_RESULT_READY:
                ready_event.set()
                return slot.take()
        await _bounded_async_event_wait(ready_event, stage="async_result_ready")


async def _stop_workers(
    workers: list[asyncio.Task[None]],
    admission: _AsyncSchedulerAdmission,
    result_slots: list[_AsyncWorkerResultSlot] | None = None,
    pending_slots: list[_AsyncPendingResultSlot] | None = None,
) -> bool:
    """Cancel within a deadline; resistant workers move to transactional debt."""
    for worker in workers:
        worker.cancel()
    if not workers:
        return False
    done, pending = await asyncio.wait(workers, timeout=_ASYNC_CANCEL_TIMEOUT_SECONDS)
    if done:
        await asyncio.gather(*done, return_exceptions=True)
    if not pending:
        return False
    if _park_async_terminal_debt(pending, admission, result_slots, pending_slots=pending_slots):
        return True
    raise RuntimeError("async terminal-debt publication failed")


@dataclass(slots=True)
class _AsyncPendingResultSlot:
    """One slot in the fixed O(window) ordered-result ring."""

    index: int = -1
    value: Any = None
    error: BaseException | None = None
    retained_lease: object | None = None

    def store(
        self, index: int, value: Any, error: BaseException | None, retained_lease: object | None
    ) -> None:
        """Store a value in the bounded result slot."""
        if self.index >= 0:
            raise RuntimeError("async ordered-result ring collision")
        self.value = value
        self.error = error
        self.retained_lease = retained_lease
        self.index = index

    def take(self, expected: int) -> tuple[Any, BaseException | None, object | None]:
        """Remove and return the retained value."""
        if self.index != expected:
            raise RuntimeError("async ordered-result ring ownership mismatch")
        value = self.value
        error = self.error
        retained_lease = self.retained_lease
        self.index = -1
        self.value = None
        self.error = None
        self.retained_lease = None
        return value, error, retained_lease

    def terminal_release(self) -> bool:
        """Release one pending lease transactionally; retain fields on failure."""
        lease = self.retained_lease
        if lease is not None:
            _release_async_result_lease(lease)
        self.index = -1
        self.value = None
        self.error = None
        self.retained_lease = None
        return True


def _release_or_park_async_terminal_ownership(
    admission: _AsyncSchedulerAdmission,
    result_slots: list[_AsyncWorkerResultSlot] | None,
    pending_slots: list[_AsyncPendingResultSlot] | None = None,
) -> None:
    """Commit terminal cleanup or publish exact retry ownership before raising."""
    try:
        if result_slots is not None:
            for result_slot in result_slots:
                result_slot.terminal_release()
        if pending_slots is not None:
            for pending_slot in pending_slots:
                pending_slot.terminal_release()
        admission.close()
    except BaseException:
        # No live Task is required: a cleanup-only debt remains retryable
        # and roots the exact admission/lease fields that have not committed.
        _park_async_terminal_debt(
            _EMPTY_ASYNC_TASKS,
            admission,
            result_slots,
            pending_slots=pending_slots,
            reap_completed=False,
        )
        raise


async def ordered_indexed_results(
    count: int,
    fetch: Callable[[int], Awaitable[Any]],
    *,
    window: int,
    memory_contract: AsyncResultMemoryContract | None = None,
) -> AsyncIterator[tuple[int, Any]]:
    """Yield in order under an explicit result-memory ownership contract."""
    if count <= 0:
        return
    postflight_bytes, preflight_bytes = _contract_estimators(memory_contract)
    # Concurrent materialization is allowed only with an explicit preflight
    # upper-bound contract. Unknown-size results use the caller coroutine so N
    # workers can never materialize N uncharged payloads simultaneously.
    requested_workers = min(count, max(1, int(window))) if preflight_bytes is not None else 0
    admission = (
        _acquire_async_scheduler_admission(requested_workers)
        if requested_workers > 0
        else _AsyncSchedulerAdmission(0)
    )
    if admission.slots > 0:
        await asyncio.sleep(0)
        _borrow_idle_async_capacity(admission, requested_workers)
        _record_async_admission_shortfall(requested_workers, admission.slots)
    worker_count = admission.slots
    if worker_count <= 0:
        # No process-global Task/control credit is available. The current
        # coroutine remains the forward-progress slot and performs no prefetch.
        for index in range(count):
            check_operation_cancelled(stage="ordered_async_results")
            value, result_lease = await _fetch_with_result_admission(
                index, fetch, postflight_bytes, preflight_bytes
            )
            try:
                _assert_async_result_ownership(value, memory_contract)
                yield index, value
            finally:
                _release_async_result_lease(result_lease)
        return
    indices: asyncio.Queue[int] = asyncio.Queue(maxsize=worker_count)
    ready_event = asyncio.Event()
    result_slots = [_AsyncWorkerResultSlot(ready_event) for _ in range(worker_count)]
    for index in range(worker_count):
        indices.put_nowait(index)
    next_to_schedule = worker_count
    workers = _start_indexed_workers(
        worker_count, indices, result_slots, fetch, postflight_bytes, preflight_bytes
    )
    if worker_count > _MAX_PROCESS_ASYNC_TASK_SLOTS:
        raise RuntimeError("async worker-count capacity invariant violated")
    pending = [_AsyncPendingResultSlot() for _ in range(worker_count)]

    try:
        for expected in range(count):
            check_operation_cancelled(stage="ordered_async_results")
            slot = pending[expected % worker_count]
            while slot.index != expected:
                index, value, error, result_lease = await _await_async_result(
                    result_slots, ready_event
                )
                target = pending[index % worker_count]
                target.store(index, value, error, result_lease)
            value, error, result_lease = slot.take(expected)
            try:
                if error is not None:
                    raise error
                _assert_async_result_ownership(value, memory_contract)
                yield expected, value
            finally:
                _release_async_result_lease(result_lease)
            if next_to_schedule < count:
                indices.put_nowait(next_to_schedule)
                next_to_schedule += 1
    finally:
        parked = await _stop_workers(workers, admission, result_slots, pending_slots=pending)
        if not parked:
            _release_or_park_async_terminal_ownership(
                admission, result_slots, pending_slots=pending
            )


async def unordered_indexed_results(
    count: int,
    fetch: Callable[[int], Awaitable[Any]],
    *,
    window: int,
    memory_contract: AsyncResultMemoryContract | None = None,
) -> AsyncIterator[tuple[int, Any]]:
    """Yield completion-order results under an explicit memory contract."""
    if count <= 0:
        return
    postflight_bytes, preflight_bytes = _contract_estimators(memory_contract)
    # Concurrent materialization is allowed only with an explicit preflight
    # upper-bound contract. Unknown-size results use the caller coroutine so N
    # workers can never materialize N uncharged payloads simultaneously.
    requested_workers = min(count, max(1, int(window))) if preflight_bytes is not None else 0
    admission = (
        _acquire_async_scheduler_admission(requested_workers)
        if requested_workers > 0
        else _AsyncSchedulerAdmission(0)
    )
    if admission.slots > 0:
        await asyncio.sleep(0)
        _borrow_idle_async_capacity(admission, requested_workers)
        _record_async_admission_shortfall(requested_workers, admission.slots)
    worker_count = admission.slots
    if worker_count <= 0:
        for index in range(count):
            check_operation_cancelled(stage="unordered_async_results")
            value, result_lease = await _fetch_with_result_admission(
                index, fetch, postflight_bytes, preflight_bytes
            )
            try:
                _assert_async_result_ownership(value, memory_contract)
                yield index, value
            finally:
                _release_async_result_lease(result_lease)
        return
    indices: asyncio.Queue[int] = asyncio.Queue(maxsize=worker_count)
    ready_event = asyncio.Event()
    result_slots = [_AsyncWorkerResultSlot(ready_event) for _ in range(worker_count)]
    for index in range(worker_count):
        indices.put_nowait(index)
    next_to_schedule = worker_count
    workers = _start_indexed_workers(
        worker_count, indices, result_slots, fetch, postflight_bytes, preflight_bytes
    )

    try:
        for _ in range(count):
            check_operation_cancelled(stage="unordered_async_results")
            index, value, error, result_lease = await _await_async_result(result_slots, ready_event)
            try:
                if error is not None:
                    raise error
                _assert_async_result_ownership(value, memory_contract)
                yield index, value
            finally:
                _release_async_result_lease(result_lease)
            if next_to_schedule < count:
                indices.put_nowait(next_to_schedule)
                next_to_schedule += 1
    finally:
        parked = await _stop_workers(workers, admission, result_slots)
        if not parked:
            _release_or_park_async_terminal_ownership(admission, result_slots)


async def drain_ordered_indexed_results(
    count: int,
    fetch: Callable[[int], Awaitable[Any]],
    *,
    window: int,
    memory_contract: AsyncResultMemoryContract | None = None,
) -> None:
    """Run indexed async work with optional pre-materialization byte admission."""
    async for _index, _result in ordered_indexed_results(
        count,
        fetch,
        window=window,
        memory_contract=memory_contract,
    ):
        continue


async def drain_ordered_iterable_results(
    values: Iterable[T],
    fetch: Callable[[T], Awaitable[Any]],
    *,
    window: int,
    memory_contract: AsyncResultMemoryContract | None = None,
) -> None:
    """Drain an iterable with only O(window) auxiliary references.

    This is intended for large metadata maps where materialising all keys merely
    to feed ``ordered_indexed_results`` would temporarily duplicate O(n) Python
    references.  Each bounded batch inherits the process-global async admission
    used by the indexed scheduler.
    """
    iterator = iter(values)
    batch_size = max(1, min(int(window), _MAX_PROCESS_ASYNC_TASK_SLOTS))
    while True:
        batch = tuple(islice(iterator, batch_size))
        if not batch:
            return

        async def fetch_index(index: int) -> Any:
            """Fetch and return the result for the indexed batch value."""
            return await fetch(batch[index])

        preflight_for_index: Callable[[int], int] | int | None = None
        if memory_contract is not None and callable(memory_contract.preflight_bytes):
            estimator = memory_contract.preflight_bytes

            def preflight_for_batch_index(index: int) -> int:
                """Return the preflight memory bound for a scheduled batch."""
                return estimator(batch[index])

            preflight_for_index = preflight_for_batch_index
        elif memory_contract is not None:
            preflight_for_index = memory_contract.preflight_bytes
        batch_contract = (
            None
            if memory_contract is None
            else AsyncResultMemoryContract(
                preflight_bytes=preflight_for_index,
                postflight_bytes=memory_contract.postflight_bytes,
                ownership_mode=memory_contract.ownership_mode,
                external_ownership_capability=memory_contract.external_ownership_capability,
            )
        )
        await drain_ordered_indexed_results(
            len(batch),
            fetch_index,
            window=min(batch_size, len(batch)),
            memory_contract=batch_contract,
        )


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "async-scheduler",
    mode="quarantine_only",
)


__all__ = [
    "AsyncResultMemoryContract",
    "AsyncResultOwnershipMode",
    "AsyncSchedulerSnapshot",
    "async_scheduler_snapshot",
    "close_async_scheduler_admission",
    "drain_ordered_indexed_results",
    "drain_ordered_iterable_results",
    "ordered_indexed_results",
    "retry_async",
    "retry_delay",
    "reopen_async_scheduler_for_tests",
    "unordered_indexed_results",
    "wait_async_scheduler_quiescent",
]
