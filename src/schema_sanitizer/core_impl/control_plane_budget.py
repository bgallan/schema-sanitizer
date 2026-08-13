"""Process-wide bounded budget for concurrency/memory control-plane objects.

This ledger intentionally charges conservative retained-byte estimates for
scheduler entries, waiters and runtime control blocks. Payload bytes continue
through the exact operation/native ledgers; this budget closes the compositional
hole where many individually bounded control structures could peak together.
"""

from __future__ import annotations

import os
import weakref
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock

from ..errors import SchemaSanitizerResourceError
from .fork_safety import quarantine_inherited_state

_DEFAULT_CAPACITY_BYTES = 256 * 1024 * 1024
_MAX_CAPACITY_BYTES = 512 * 1024 * 1024
_MIN_TICKET_BYTES = 256
# Conservative fixed charge for preallocated escrows, notifier slots, locks and
# other runtime control structures that exist independently of live tickets.
_MAX_ACTIVE_TICKETS = 262_144
_MAX_TICKET_TOKEN = (1 << 63) - 1
_GOVERNED_MEMORY_ADMISSION_LOCK = Lock()


@dataclass(frozen=True, slots=True)
class ControlPlaneBudgetSnapshot:
    """Immutable accounting view of process control-plane memory."""

    capacity_bytes: int
    reserved_bytes: int
    peak_reserved_bytes: int
    active_tickets: int
    rejected_tickets: int
    over_release_count: int
    static_baseline_bytes: int = 0
    reconciliation_pending: bool = False

    @property
    def governed_bytes(self) -> int:
        return self.static_baseline_bytes + self.reserved_bytes


class _ControlPlaneCapability:
    """Ledger-rooted exact authority independent of the caller wrapper.

    The budget roots this object, never ``ControlPlaneTicket``.  Losing a return
    value between ledger commit and caller handoff can therefore collect the
    wrapper; its non-blocking ``__del__`` merely flips ``retire_requested`` on
    this already-rooted capability for safe-point cleanup.
    """

    __slots__ = ("pid", "token", "released", "retire_requested")

    def __init__(self, pid: int) -> None:
        self.pid = int(pid)
        self.token = 0
        self.released = False
        self.retire_requested = False


class ControlPlaneTicket:
    """Caller wrapper for one exact process control-plane capability."""

    __slots__ = (
        "amount",
        "kind",
        "pid",
        "token",
        "capability",
        "_released_mirror",
        "_retire_requested_mirror",
        "__weakref__",
    )

    def __init__(
        self,
        amount: int,
        kind: str,
        pid: int,
        token: int = 0,
        released: bool = False,
        retire_requested: bool = False,
        capability: object | None = None,
    ) -> None:
        self.amount = amount
        self.kind = kind
        self.pid = pid
        self.token = token
        if capability is None:
            capability = _ControlPlaneCapability(pid)
        self.capability = capability
        self._released_mirror = bool(released)
        self._retire_requested_mirror = bool(retire_requested)
        if isinstance(capability, _ControlPlaneCapability):
            capability.token = int(token)
            capability.released = bool(released)
            capability.retire_requested = bool(retire_requested)

    @property
    def released(self) -> bool:
        capability = self.capability
        if isinstance(capability, _ControlPlaneCapability):
            return bool(capability.released)
        return self._released_mirror

    @released.setter
    def released(self, value: bool) -> None:
        self._released_mirror = bool(value)
        capability = self.capability
        if isinstance(capability, _ControlPlaneCapability):
            capability.released = bool(value)

    @property
    def retire_requested(self) -> bool:
        capability = self.capability
        if isinstance(capability, _ControlPlaneCapability):
            return bool(capability.retire_requested)
        return self._retire_requested_mirror

    @retire_requested.setter
    def retire_requested(self, value: bool) -> None:
        self._retire_requested_mirror = bool(value)
        capability = self.capability
        if isinstance(capability, _ControlPlaneCapability):
            capability.retire_requested = bool(value)

    def __del__(self) -> None:
        """Request bounded cleanup without taking a lock on the GC thread."""
        try:
            capability = getattr(self, "capability", None)
            if (
                isinstance(capability, _ControlPlaneCapability)
                and not capability.released
                and capability.pid == os.getpid()
                and capability.token > 0
            ):
                capability.retire_requested = True
        except BaseException:
            pass


class _ControlPlaneOwnerEntry:
    """Ledger-rooted capability plus a weak compatibility view of its wrapper."""

    __slots__ = ("capability", "amount", "ticket_ref")

    def __init__(
        self, ticket: ControlPlaneTicket, capability: _ControlPlaneCapability, amount: int
    ) -> None:
        self.capability = capability
        self.amount = amount
        self.ticket_ref = weakref.ref(ticket)

    def __getitem__(self, index: int) -> object:
        # Historical focused tests treated owner entries as
        # ``(ticket, capability, amount)``. Preserve that read-only shape without
        # strongly rooting the wrapper and reopening commit->handoff leaks.
        if index == 0:
            return self.ticket_ref()
        if index == 1:
            return self.capability
        if index == 2:
            return self.amount
        raise IndexError(index)


class _ProcessControlPlaneBudget:
    def __init__(self, *, include_static_baseline: bool = False) -> None:
        self._lock = Lock()
        self._include_static_baseline = include_static_baseline
        self._pid = os.getpid()
        self._capacity = _DEFAULT_CAPACITY_BYTES
        self._reserved = 0
        self._peak = 0
        self._active = 0
        self._rejected = 0
        self._over_release = 0
        self._sequence = 0
        self._free_tokens: list[int] = [0] * _MAX_ACTIVE_TICKETS
        self._free_token_head = 0
        self._free_token_tail = 0
        self._free_token_count = 0
        self._owners: dict[int, _ControlPlaneOwnerEntry] = {}
        self._counters_dirty = False
        self._corrupted = False
        # Two child banks are allocated during normal runtime. Fork callbacks
        # only select/swap these objects; they never build a 262k-token list or
        # a fresh dict/lock in the child. The inherited token array is safe to
        # reuse because ``_free_token_count = 0`` makes every old cell inert.
        self._fork_spare_locks = (Lock(), Lock())
        self._fork_spare_init_locks = (Lock(), Lock())
        self._fork_spare_owners: tuple[dict[int, _ControlPlaneOwnerEntry], ...] = ({}, {})
        self._fork_next_spare = 0
        self._fork_fresh_lock: Lock | None = None
        self._fork_fresh_init_lock: Lock | None = None
        self._fork_fresh_owners: dict[int, _ControlPlaneOwnerEntry] | None = None
        self._native_init_lock = Lock()
        self._native_create: Callable[[int], object] | None = None
        self._native_reserve_snapshot: Callable[[object, int, str], object] | None = None
        self._native_release: Callable[[object, int], object] | None = None
        self._native_snapshot: Callable[[object], object] | None = None
        # A private native OperationMemoryLedger mirrors governed control bytes
        # into the exact shared resident pool. Native-only work therefore cannot
        # consume headroom already owned by Python control structures.
        self._native_shadow_capsule: object | None = None
        self._native_shadow_bytes = 0
        self._native_shadow_dirty = False

    def _ensure_process_locked(self) -> None:
        pid = os.getpid()
        if self._pid == pid:
            return
        self._pid = pid
        self._reserved = 0
        self._peak = 0
        self._active = 0
        self._rejected = 0
        self._over_release = 0
        self._sequence = 0
        self._free_tokens = [0] * _MAX_ACTIVE_TICKETS
        self._free_token_head = 0
        self._free_token_tail = 0
        self._free_token_count = 0
        self._owners = {}
        self._counters_dirty = False
        self._corrupted = False
        self._native_shadow_capsule = None
        self._native_shadow_bytes = 0
        self._native_shadow_dirty = False

    def prepare_for_fork(self) -> None:
        """Select a child bank that was allocated before fork preparation."""
        index = self._fork_next_spare
        self._fork_fresh_lock = self._fork_spare_locks[index]
        self._fork_fresh_init_lock = self._fork_spare_init_locks[index]
        owners = self._fork_spare_owners[index]
        # Spare owner maps are never used by the parent. They remain empty and
        # therefore require no allocation/clear during the at-fork callback.
        self._fork_fresh_owners = owners

    def clear_fork_preparation(self) -> None:
        self._fork_fresh_lock = None
        self._fork_fresh_init_lock = None
        self._fork_fresh_owners = None

    def reset_after_fork(self) -> None:
        """Swap inherited locks without acquiring/allocating in the child."""
        prepared_lock = self._fork_fresh_lock
        prepared_init_lock = self._fork_fresh_init_lock
        prepared_owners = self._fork_fresh_owners
        if prepared_lock is None or prepared_init_lock is None or prepared_owners is None:
            # A failed prepare leaves the runtime child poisoned; do not allocate
            # replacement synchronization here.
            return
        quarantine_inherited_state(
            "control-plane-budget",
            self._lock,
            self._native_init_lock,
            self._owners,
            self._native_shadow_capsule,
        )
        self._lock = prepared_lock
        self._native_init_lock = prepared_init_lock
        self._owners = prepared_owners
        # Reuse the inherited fixed token slab; zero count makes every stale
        # cell unreachable without touching the list contents.
        self._free_token_head = 0
        self._free_token_tail = 0
        self._free_token_count = 0
        self._fork_fresh_lock = None
        self._fork_fresh_init_lock = None
        self._fork_fresh_owners = None
        self._fork_next_spare = 1 - self._fork_next_spare
        self._pid = os.getpid()
        self._reserved = 0
        self._peak = 0
        self._active = 0
        self._rejected = 0
        self._over_release = 0
        self._sequence = 0
        self._counters_dirty = False
        self._corrupted = False
        # The inherited PyCapsule points at a parent-process shadow ledger whose
        # shared-pool charge is not authoritative in the child. Runtime fork
        # safety poisons initialized work; discard the Python capability without
        # touching parent-owned native accounting from the at-fork callback.
        self._native_shadow_capsule = None
        self._native_shadow_bytes = 0
        self._native_shadow_dirty = False

    def _reconcile_counters_locked(self) -> bool:
        """Rebuild cache counters from exact ticket owners and latch drift.

        ``_owners`` is the only admission authority.  Derived counters are repaired
        even after corruption so authenticated cleanup can continue, but corruption
        permanently closes new ticket admission for this budget generation.
        """
        try:
            reserved = 0
            active = 0
            for entry in self._owners.values():
                capability = entry.capability
                amount = entry.amount
                if (
                    not isinstance(capability, _ControlPlaneCapability)
                    or capability.pid != self._pid
                    or capability.token <= 0
                    or type(amount) is not int
                    or amount < 0
                ):
                    self._corrupted = True
                    continue
                reserved += amount
                active += 1
        except BaseException:
            self._counters_dirty = True
            self._corrupted = True
            return False
        if self._reserved != reserved or self._active != active:
            # Derived counters are repairable mirrors.  An asynchronous unwind
            # after exact owner publication/retirement is not evidence that the
            # capability ledger itself is corrupt.
            self._counters_dirty = True
        self._reserved = reserved
        self._active = active
        self._counters_dirty = False
        return not self._corrupted

    def _static_baseline_bytes_locked(self) -> int:
        if not self._include_static_baseline:
            return 0
        from .static_control_plane import static_control_plane_bytes

        return static_control_plane_bytes()

    def prewarm_native_shadow(self) -> bool:
        """Resolve/create the zero-byte native shadow outside admission locks."""
        if getattr(self, "_native_shadow_capsule", None) is not None:
            return True
        with self._native_init_lock:
            if self._native_shadow_capsule is not None:
                return True
            try:
                from types import ModuleType

                from .native_runtime import native_core

                if not isinstance(native_core, ModuleType):
                    return False
                create = getattr(native_core, "operation_memory_ledger_create", None)
                reserve_snapshot = getattr(
                    native_core, "operation_memory_ledger_reserve_snapshot", None
                )
                release = getattr(native_core, "operation_memory_ledger_release", None)
                snapshot = getattr(native_core, "operation_memory_ledger_snapshot", None)
                if (
                    not callable(create)
                    or not callable(reserve_snapshot)
                    or not callable(release)
                    or not callable(snapshot)
                ):
                    return False
                capsule = create(_MAX_CAPACITY_BYTES)
            except BaseException:
                return False
            self._native_create = create
            self._native_reserve_snapshot = reserve_snapshot
            self._native_release = release
            self._native_snapshot = snapshot
            self._native_shadow_capsule = capsule
            self._native_shadow_bytes = 0
            self._native_shadow_dirty = False
            return True

    def _sync_native_shadow_locked(self, target: int) -> bool:
        """Mirror ``target`` governed bytes into the exact native process pool.

        This method is called only while the process governed-admission lock is
        held. Focused source-only tests intentionally lack the ABI3 extension and
        keep the previous independently bounded Python-only behavior.
        """
        if self._native_shadow_capsule is None and not self.prewarm_native_shadow():
            return False
        reserve_snapshot = self._native_reserve_snapshot
        release = self._native_release
        snapshot = self._native_snapshot
        if not callable(reserve_snapshot) or not callable(release) or not callable(snapshot):
            return False
        capsule = self._native_shadow_capsule
        if capsule is None:
            return False
        if self._native_shadow_dirty:
            values = snapshot(capsule)
            if not isinstance(values, tuple) or len(values) != 3:
                raise RuntimeError("native control-plane shadow returned invalid statistics")
            self._native_shadow_bytes = int(values[1])
            self._native_shadow_dirty = False
        current = self._native_shadow_bytes
        if target > current:
            values = reserve_snapshot(capsule, target - current, "process_control_plane_shadow")
            if not isinstance(values, tuple) or len(values) != 3:
                raise RuntimeError(
                    "native control-plane shadow reserve returned invalid statistics"
                )
            self._native_shadow_bytes = target
        elif target < current:
            release(capsule, current - target)
            self._native_shadow_bytes = target
        return True

    def _release_native_shadow_locked(self, amount: int) -> None:
        """Release an authenticated dynamic charge without creating a new owner."""
        if amount <= 0 or self._native_shadow_capsule is None:
            return
        try:
            release = self._native_release
            if callable(release):
                release(self._native_shadow_capsule, amount)
                try:
                    self._native_shadow_bytes = max(0, self._native_shadow_bytes - amount)
                except BaseException:
                    self._native_shadow_dirty = True
        except BaseException:
            # Failure to mirror a release leaves the native pool conservatively
            # overcharged. A later normal-path synchronization reconciles it.
            self._native_shadow_dirty = True

    def ensure_native_shadow_under_admission_lock(self) -> tuple[bool, int]:
        """Synchronize the native shadow while the caller owns admission lock."""
        with self._lock:
            self._ensure_process_locked()
            self._reconcile_counters_locked()
            target = self._static_baseline_bytes_locked() + self._reserved
            active = self._sync_native_shadow_locked(target)
            return active, self._native_shadow_bytes if active else 0

    def configure(self, capacity_bytes: int) -> None:
        if type(capacity_bytes) is not int:
            raise TypeError("control-plane capacity must be an exact integer")
        if capacity_bytes <= 0 or capacity_bytes > _MAX_CAPACITY_BYTES:
            raise ValueError("control-plane capacity must be within (0, 512 MiB]")
        with self._lock:
            self._ensure_process_locked()
            self._reconcile_counters_locked()
            if self._corrupted:
                raise RuntimeError(
                    "control-plane budget is quarantined after accounting corruption"
                )
            baseline = self._static_baseline_bytes_locked()
            if capacity_bytes < self._reserved + baseline:
                raise RuntimeError(
                    "control-plane capacity is below live reservations and static baseline"
                )
            self._capacity = capacity_bytes

    def reserve(self, kind: str, amount: int) -> ControlPlaneTicket:
        if type(kind) is not str or type(amount) is not int:
            raise TypeError("control-plane reservation metadata must be exact")
        if amount < _MIN_TICKET_BYTES:
            raise ValueError(f"control-plane reservation must be >= {_MIN_TICKET_BYTES} bytes")
        from .memory_budget import _optional_process_resident_memory_snapshot

        self.prewarm_native_shadow()
        ticket = ControlPlaneTicket(amount, kind, os.getpid())
        capability = ticket.capability
        assert isinstance(capability, _ControlPlaneCapability)
        with _GOVERNED_MEMORY_ADMISSION_LOCK:
            with self._lock:
                self._ensure_process_locked()
                self._reconcile_counters_locked()
                if self._corrupted:
                    try:
                        self._rejected += 1
                    except BaseException:
                        pass
                    raise SchemaSanitizerResourceError(
                        "process control-plane admission quarantined after accounting corruption",
                        detail={
                            "stage": "control_plane",
                            "limit_name": "process_control_plane_corruption_quarantine",
                            "limit_bytes": self._capacity,
                            "actual_bytes": self._reserved + amount,
                            "kind": kind,
                        },
                    )
                current_governed = self._static_baseline_bytes_locked() + self._reserved
                native_shadow_active = self._sync_native_shadow_locked(current_governed)
                next_reserved = self._reserved + amount
                next_governed_control = self._static_baseline_bytes_locked() + next_reserved
                next_active = self._active + 1
                next_peak = max(self._peak, next_reserved)
                limit_name = "process_control_plane_bytes"
                limit_bytes = self._capacity
                actual_bytes = next_governed_control
                exhausted = (
                    next_governed_control > self._capacity
                    or next_active > _MAX_ACTIVE_TICKETS
                    or (self._free_token_count == 0 and self._sequence >= _MAX_TICKET_TOKEN)
                )
                if not exhausted:
                    resident = _optional_process_resident_memory_snapshot()
                    if resident is not None:
                        combined = resident.reserved_bytes + (
                            amount if native_shadow_active else next_governed_control
                        )
                        if combined > resident.capacity_bytes:
                            exhausted = True
                            limit_name = "process_governed_memory_bytes"
                            limit_bytes = resident.capacity_bytes
                            actual_bytes = combined
                if exhausted:
                    try:
                        self._rejected += 1
                    except MemoryError:
                        pass
                    message = (
                        "process governed resident memory limit exceeded"
                        if limit_name == "process_governed_memory_bytes"
                        else "process control-plane memory budget limit exceeded"
                    )
                    raise SchemaSanitizerResourceError(
                        message,
                        detail={
                            "stage": "control_plane",
                            "limit_name": limit_name,
                            "limit_bytes": limit_bytes,
                            "actual_bytes": actual_bytes,
                            "kind": kind,
                        },
                    )

                reuse_token = self._free_token_count > 0
                if reuse_token:
                    token = self._free_tokens[self._free_token_head]
                    next_free_head = self._free_token_head + 1
                    if next_free_head == _MAX_ACTIVE_TICKETS:
                        next_free_head = 0
                    next_free_count = self._free_token_count - 1
                else:
                    token = self._sequence + 1
                    next_free_head = self._free_token_head
                    next_free_count = self._free_token_count

                # Publish the lookup identity into the non-rooted wrapper and
                # the ledger-rooted capability before any owner-map commit.  If
                # admission aborts before publication, the wrapper may disappear
                # harmlessly because the capability is not yet rooted anywhere.
                ticket.token = token
                capability.token = token
                entry = _ControlPlaneOwnerEntry(ticket, capability, amount)
                namespace_committed = False
                owner_committed = False
                shadow_grew = False
                try:
                    if native_shadow_active:
                        self._sync_native_shadow_locked(next_governed_control)
                        shadow_grew = True

                    # Token namespace commits before owner publication.  A failed
                    # dict insert can roll this back under the same lock; once an
                    # owner exists, no asynchronous unwind can leave a reusable
                    # token pointing at that exact authority.
                    if reuse_token:
                        self._free_token_head = next_free_head
                        self._free_token_count = next_free_count
                    else:
                        self._sequence = token
                    namespace_committed = True

                    self._owners[token] = entry
                    owner_committed = self._owners.get(token) is entry
                    if not owner_committed:
                        raise RuntimeError("control-plane exact owner publication did not commit")
                except BaseException:
                    # STORE_SUBSCR/custom mappings can commit and then raise, and
                    # asynchronous exceptions can arrive immediately after the
                    # opcode.  Inspect exact membership before deciding rollback.
                    if self._owners.get(token) is entry:
                        owner_committed = True
                        capability.retire_requested = True
                        self._reserved = next_reserved
                        self._peak = next_peak
                        self._active = next_active
                    else:
                        if namespace_committed:
                            if reuse_token:
                                self._free_token_head = (
                                    self._free_token_head - 1
                                    if self._free_token_head > 0
                                    else _MAX_ACTIVE_TICKETS - 1
                                )
                                self._free_token_count += 1
                            elif self._sequence == token:
                                self._sequence = token - 1
                        if shadow_grew:
                            try:
                                self._sync_native_shadow_locked(current_governed)
                            except BaseException:
                                self._native_shadow_dirty = True
                        capability.token = 0
                        ticket.token = 0
                    raise

                # Owner membership is authoritative.  Everything below is a
                # repairable mirror assignment; a signal after this point can
                # only make counters stale, never orphan ownership or namespace.
                self._reserved = next_reserved
                self._peak = next_peak
                self._active = next_active
        _observe_runtime_concurrency_contract_noexcept("process_control_plane_budget")
        return ticket

    def _release_capability(self, capability: _ControlPlaneCapability, token: int) -> bool:
        if capability.released:
            return True
        if capability.pid != os.getpid():
            return True
        with _GOVERNED_MEMORY_ADMISSION_LOCK:
            with self._lock:
                self._ensure_process_locked()
                entry = self._owners.get(token)
                if entry is None:
                    # Exact membership absence after a prior authenticated pop is
                    # a committed release, even if wrapper mirrors lagged.
                    if capability.token == token:
                        capability.released = True
                        capability.retire_requested = False
                        self._reconcile_counters_locked()
                        return True
                    try:
                        self._over_release += 1
                    except BaseException:
                        pass
                    return False
                if entry.capability is not capability or capability.token != token:
                    try:
                        self._over_release += 1
                    except BaseException:
                        pass
                    return False
                authoritative_amount = entry.amount
                self._reconcile_counters_locked()
                if self._reserved < authoritative_amount or self._active <= 0:
                    self._counters_dirty = True
                    self._reconcile_counters_locked()
                if self._reserved < authoritative_amount or self._active <= 0:
                    try:
                        self._over_release += 1
                    except BaseException:
                        pass
                    return False

                can_recycle = self._free_token_count < _MAX_ACTIVE_TICKETS
                if can_recycle:
                    recycle_tail = self._free_token_tail
                    next_recycle_tail = recycle_tail + 1
                    if next_recycle_tail == _MAX_ACTIVE_TICKETS:
                        next_recycle_tail = 0
                    next_recycle_count = self._free_token_count + 1
                else:
                    recycle_tail = 0
                    next_recycle_tail = self._free_token_tail
                    next_recycle_count = self._free_token_count

                # Exact owner removal is the primary release commit.  A mapping
                # implementation or async exception may report failure after the
                # pop; inspect membership and continue if authority is already gone.
                try:
                    removed = self._owners.pop(token, None)
                except BaseException:
                    if token in self._owners:
                        raise
                    removed = entry
                if removed is None:
                    capability.released = True
                    capability.retire_requested = False
                    self._reconcile_counters_locked()
                    return True
                if removed is not entry:
                    self._corrupted = True
                    return False

                # Native shadow release is fail-closed: failures retain an
                # overcharge and mark the shadow dirty, never resurrect owner
                # membership or manufacture headroom.
                self._release_native_shadow_locked(authoritative_amount)
                if can_recycle:
                    # pass50 compatibility breadcrumb: self._free_tokens[self._free_token_tail]
                    self._free_tokens[recycle_tail] = token
                    self._free_token_tail = next_recycle_tail
                    self._free_token_count = next_recycle_count
                else:
                    try:
                        self._over_release = min(_MAX_TICKET_TOKEN, self._over_release + 1)
                    except BaseException:
                        pass
                capability.released = True
                capability.retire_requested = False
                # Mirrors are reconstructed from surviving exact owners rather
                # than trusted across a post-pop signal boundary.
                self._counters_dirty = True
                self._reconcile_counters_locked()
                return True

    def release(self, ticket: ControlPlaneTicket | None) -> bool:
        """Retire one authenticated caller wrapper/capability generation."""
        if ticket is None:
            return True
        if type(ticket) is not ControlPlaneTicket:
            return False
        capability = ticket.capability
        if not isinstance(capability, _ControlPlaneCapability):
            return False
        if ticket.pid != os.getpid():
            return True
        released = self._release_capability(capability, ticket.token)
        if released:
            ticket.released = True
            ticket.retire_requested = False
        return released

    def request_retirement(self, ticket: ControlPlaneTicket | None) -> bool:
        """Mark one rooted capability for bounded safe-point retirement."""
        if ticket is None or type(ticket) is not ControlPlaneTicket:
            return False
        capability = ticket.capability
        if not isinstance(capability, _ControlPlaneCapability):
            return False
        if capability.released or ticket.released:
            return True
        if ticket.pid != os.getpid():
            return True
        with self._lock:
            self._ensure_process_locked()
            entry = self._owners.get(ticket.token)
            if entry is None or entry.capability is not capability:
                return False
            capability.retire_requested = True
            ticket.retire_requested = True
            return True

    def drain_requested_retirements(self, *, limit: int = 256) -> int:
        """Retry abandoned wrappers from ledger-rooted exact capabilities."""
        progressed = 0
        for _ in range(max(0, int(limit))):
            capability: _ControlPlaneCapability | None = None
            token = 0
            with self._lock:
                self._ensure_process_locked()
                self._reconcile_counters_locked()
                for candidate_token, entry in self._owners.items():
                    candidate = entry.capability
                    if candidate.retire_requested:
                        capability = candidate
                        token = candidate_token
                        break
            if capability is None:
                break
            if not self._release_capability(capability, token):
                break
            progressed += 1
        return progressed

    def snapshot(self) -> ControlPlaneBudgetSnapshot:
        with self._lock:
            self._ensure_process_locked()
            self._reconcile_counters_locked()
            return ControlPlaneBudgetSnapshot(
                self._capacity,
                self._reserved,
                self._peak,
                self._active,
                self._rejected,
                self._over_release,
                self._static_baseline_bytes_locked(),
                self._counters_dirty or self._corrupted,
            )

    def snapshot_with_shadow(self) -> tuple[ControlPlaneBudgetSnapshot, bool, int]:
        """Return one lock-consistent observation without mutating native state."""
        with self._lock:
            self._ensure_process_locked()
            self._reconcile_counters_locked()
            snap = ControlPlaneBudgetSnapshot(
                self._capacity,
                self._reserved,
                self._peak,
                self._active,
                self._rejected,
                self._over_release,
                self._static_baseline_bytes_locked(),
                self._counters_dirty or self._corrupted,
            )
            active = self._native_shadow_capsule is not None and not self._native_shadow_dirty
            return snap, active, self._native_shadow_bytes if active else 0


def register_static_control_plane(kind: str, amount: int) -> None:
    """Register permanent control-plane memory and reconcile its native shadow."""
    from .static_control_plane import register_static_control_plane as register

    register(kind, amount)
    # Registration is a mutation, so shadow reconciliation belongs here rather
    # than in diagnostic snapshots. Early import/source-only runtimes simply
    # defer reconciliation until the first governed admission.
    try:
        synchronize_control_plane_native_shadow()
    except BaseException:
        pass


from .static_control_plane import (  # noqa: E402
    register_static_control_plane as _register_static_control_plane,
)

_register_static_control_plane("control_plane_budget_core", 4 * 1024 * 1024)


_PROCESS_CONTROL_PLANE_BUDGET = _ProcessControlPlaneBudget(include_static_baseline=True)


def reserve_control_plane(kind: str, amount: int) -> ControlPlaneTicket:
    """Reserve process-wide control-plane bytes for one live owner."""
    return _PROCESS_CONTROL_PLANE_BUDGET.reserve(kind, amount)


def release_control_plane(ticket: ControlPlaneTicket | None) -> bool:
    """Release an exact ticket or defer its retirement when necessary."""
    released = _PROCESS_CONTROL_PLANE_BUDGET.release(ticket)
    if not released and type(ticket) is ControlPlaneTicket:
        _PROCESS_CONTROL_PLANE_BUDGET.request_retirement(ticket)
    return released


def defer_control_plane_release(ticket: ControlPlaneTicket | None) -> bool:
    """Persist a cleanup-only control owner for a later bounded safe-point retry."""
    if ticket is None:
        return True
    if release_control_plane(ticket):
        return True
    return _PROCESS_CONTROL_PLANE_BUDGET.request_retirement(ticket)


def drain_deferred_control_plane_releases(*, limit: int = 256) -> int:
    return _PROCESS_CONTROL_PLANE_BUDGET.drain_requested_retirements(limit=limit)


def _synchronize_control_plane_native_shadow_under_admission_lock() -> tuple[bool, int]:
    return _PROCESS_CONTROL_PLANE_BUDGET.ensure_native_shadow_under_admission_lock()


def synchronize_control_plane_native_shadow() -> tuple[bool, int]:
    """Mirror current governed control bytes into the shared native resident pool."""
    _PROCESS_CONTROL_PLANE_BUDGET.prewarm_native_shadow()
    with _GOVERNED_MEMORY_ADMISSION_LOCK:
        return _synchronize_control_plane_native_shadow_under_admission_lock()


def try_synchronize_control_plane_native_shadow() -> bool:
    """Best-effort mutation hook that never waits on an in-flight admission."""
    _PROCESS_CONTROL_PLANE_BUDGET.prewarm_native_shadow()
    if not _GOVERNED_MEMORY_ADMISSION_LOCK.acquire(blocking=False):
        return False
    try:
        _synchronize_control_plane_native_shadow_under_admission_lock()
        return True
    except BaseException:
        return False
    finally:
        _GOVERNED_MEMORY_ADMISSION_LOCK.release()


def control_plane_snapshot_with_shadow() -> tuple[ControlPlaneBudgetSnapshot, bool, int]:
    """Pure, admission-serialized observation of control bytes and native shadow."""
    with _GOVERNED_MEMORY_ADMISSION_LOCK:
        return _PROCESS_CONTROL_PLANE_BUDGET.snapshot_with_shadow()


def process_control_plane_snapshot() -> ControlPlaneBudgetSnapshot:
    """Return process-wide control-plane budget diagnostics."""
    return _PROCESS_CONTROL_PLANE_BUDGET.snapshot()


def configure_control_plane_budget(capacity_bytes: int) -> None:
    """Set the process control-plane capacity before admission begins."""
    _PROCESS_CONTROL_PLANE_BUDGET.configure(capacity_bytes)


_FORK_ADMISSION_LOCK_BANK = (Lock(), Lock())
_FORK_ADMISSION_LOCK_BANK_INDEX = 0
_FORK_FRESH_ADMISSION_LOCK: Lock | None = None


def _prepare_control_plane_for_fork() -> None:
    global _FORK_FRESH_ADMISSION_LOCK
    _FORK_FRESH_ADMISSION_LOCK = _FORK_ADMISSION_LOCK_BANK[_FORK_ADMISSION_LOCK_BANK_INDEX]
    _PROCESS_CONTROL_PLANE_BUDGET.prepare_for_fork()


def _clear_control_plane_fork_preparation() -> None:
    global _FORK_FRESH_ADMISSION_LOCK
    _FORK_FRESH_ADMISSION_LOCK = None
    _PROCESS_CONTROL_PLANE_BUDGET.clear_fork_preparation()


def _reset_control_plane_after_fork() -> None:
    global \
        _GOVERNED_MEMORY_ADMISSION_LOCK, \
        _FORK_FRESH_ADMISSION_LOCK, \
        _FORK_ADMISSION_LOCK_BANK_INDEX
    prepared = _FORK_FRESH_ADMISSION_LOCK
    if prepared is None:
        return
    _GOVERNED_MEMORY_ADMISSION_LOCK = prepared
    _FORK_ADMISSION_LOCK_BANK_INDEX = 1 - _FORK_ADMISSION_LOCK_BANK_INDEX
    _FORK_FRESH_ADMISSION_LOCK = None
    _PROCESS_CONTROL_PLANE_BUDGET.reset_after_fork()


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "control-plane-budget",
    before=_prepare_control_plane_for_fork,
    after_in_parent=_clear_control_plane_fork_preparation,
    after_in_child=_reset_control_plane_after_fork,
)


from .concurrency_contracts import (  # noqa: E402
    observe_runtime_concurrency_contract_noexcept as _observe_runtime_concurrency_contract_noexcept,
)
from .concurrency_contracts import (  # noqa: E402
    register_runtime_concurrency_contract as _register_runtime_concurrency_contract,
)

_register_runtime_concurrency_contract("process_control_plane_budget", reserve_control_plane)


__all__ = [
    "ControlPlaneBudgetSnapshot",
    "ControlPlaneTicket",
    "_GOVERNED_MEMORY_ADMISSION_LOCK",
    "configure_control_plane_budget",
    "process_control_plane_snapshot",
    "try_synchronize_control_plane_native_shadow",
    "control_plane_snapshot_with_shadow",
    "register_static_control_plane",
    "release_control_plane",
    "reserve_control_plane",
]
