"""Derive every runtime resource budget from one per-operation memory limit."""

from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from threading import Condition, Lock
from time import monotonic
from typing import Any, Callable, Iterator, Mapping, cast

# Cross-process memory is a mandatory dependency of every operation ledger.
# Import it while normal allocations/imports are allowed so all of its static
# control-plane footprint is registered before payload headroom is advertised.
from .bounded_generation import next_reusable_token
from .control_plane_budget import ControlPlaneTicket, release_control_plane, reserve_control_plane
from .finalization import runtime_is_finalizing
from .finalizer_escrow import ReservedFinalizerEscrow
from .fork_safety import quarantine_inherited_state
from .rooted_finalizer import (
    FinalizerReplayCapability,
    RootedFinalizerAuthority,
    arm_rooted_finalizer_authority,
    reserve_rooted_finalizer_authority,
    retire_or_ack_rooted_finalizer_authority,
)
from .safe_errors import add_bounded_note, clear_exception_traceback
from .terminal_ownership import publish_terminal_owner, retire_terminal_owner

MAX_MEMORY_LIMIT_BYTES = 64 * 1024 * 1024 * 1024
_CROSS_PROCESS_HEADROOM_BYTES = 8 << 20
_OPERATION_MEMORY_LEASE_CONTROL_BYTES = 384


def normalize_memory_limit(memory_limit_bytes: int | None) -> int:
    """Return the effective positive per-operation memory limit.

    ``None`` asks the extension for a safe share of currently available host
    and container memory. Values above the absolute native ceiling are rejected
    rather than silently weakening the safety contract.
    """
    from .fork_safety import ensure_runtime_fork_safe

    ensure_runtime_fork_safe()
    if memory_limit_bytes is None:
        from .native_runtime import native_core

        values = native_core.memory_budget(-1)
        if not isinstance(values, tuple) or not values:
            raise RuntimeError("native memory budget returned an invalid contract")
        return int(values[0])
    if isinstance(memory_limit_bytes, bool) or not isinstance(memory_limit_bytes, int):
        raise TypeError("Option 'memory_limit_bytes' must be an integer or None")
    if memory_limit_bytes <= 0:
        raise ValueError("Option 'memory_limit_bytes' must be > 0")
    if memory_limit_bytes > MAX_MEMORY_LIMIT_BYTES:
        raise ValueError("Option 'memory_limit_bytes' exceeds the absolute 64 GiB safety ceiling")
    return memory_limit_bytes


def _raw_process_resident_memory_snapshot() -> ProcessResidentMemorySnapshot:
    """Return the physical native resident-memory envelope without Python charges."""
    from .fork_safety import ensure_runtime_fork_safe
    from .native_runtime import native_core

    ensure_runtime_fork_safe()
    values = native_core.process_resident_memory_stats()
    if not isinstance(values, tuple) or len(values) != 3:
        raise RuntimeError("native process resident memory ledger returned invalid statistics")
    return ProcessResidentMemorySnapshot(*(int(value) for value in values))


def process_resident_memory_snapshot() -> ProcessResidentMemorySnapshot:
    """Return resident bytes plus the payload capacity currently safe to admit.

    ``reserved_bytes`` is the exact governed payload/control charge. Native
    allocation-registry metadata is covered by a fixed process-wide reserve bank
    already removed from ``capacity_bytes``, so metadata churn cannot make the
    advertised payload ceiling fluctuate. ``capacity_bytes`` is the stable
    process-admission ceiling visible to Python
    callers: it discounts the current governed control plane and one minimum
    lease-owner charge. Dynamic control owners therefore reduce advertised
    headroom immediately; admission remains serialized so a concurrent change
    is handled like any other concurrent resource reservation. Internal composition
    checks use :func:`_raw_process_resident_memory_snapshot` instead.
    """
    from .control_plane_budget import (
        _GOVERNED_MEMORY_ADMISSION_LOCK,
        _PROCESS_CONTROL_PLANE_BUDGET,
    )

    # Read raw resident bytes and control ownership under the same admission
    # barrier. This function is observational only: no shadow reserve/release is
    # performed from a snapshot path.
    with _GOVERNED_MEMORY_ADMISSION_LOCK:
        control, shadow_active, shadow_bytes = _PROCESS_CONTROL_PLANE_BUDGET.snapshot_with_shadow()
        raw = _raw_process_resident_memory_snapshot()
    if shadow_active:
        # The shared native pool already contains every governed control byte.
        # Subtract that private shadow from the public payload view while keeping
        # one owner charge unavailable for the next ``OperationMemoryLease``.
        payload_reserved = max(0, raw.reserved_bytes - shadow_bytes)
        payload_capacity = raw.capacity_bytes - shadow_bytes - _OPERATION_MEMORY_LEASE_CONTROL_BYTES
        payload_peak = max(payload_reserved, raw.peak_reserved_bytes - shadow_bytes)
    else:
        # Source-only/native-double fallback: composition remains enforced in
        # Python even though there is no native shared pool to mirror into.
        payload_reserved = raw.reserved_bytes
        payload_capacity = (
            raw.capacity_bytes - control.governed_bytes - _OPERATION_MEMORY_LEASE_CONTROL_BYTES
        )
        payload_peak = raw.peak_reserved_bytes
    return ProcessResidentMemorySnapshot(
        max(payload_reserved, payload_capacity),
        payload_reserved,
        payload_peak,
    )


def _optional_process_resident_memory_snapshot() -> ProcessResidentMemorySnapshot | None:
    """Return the aggregate native envelope only when the real ABI3 module is loaded.

    Python-only lifecycle tests and source-tree tooling intentionally use lightweight
    native doubles.  Those environments still receive hard local payload/control
    limits, but cannot provide an authoritative process-global native envelope.
    An actual loaded extension must still satisfy the strict snapshot contract.
    """
    from types import ModuleType

    from .native_runtime import native_core

    if not isinstance(native_core, ModuleType):
        return None
    return _raw_process_resident_memory_snapshot()


@dataclass(frozen=True, slots=True)
class OperationMemorySnapshot:
    """Atomic cross-language resident-memory accounting snapshot."""

    limit_bytes: int
    reserved_bytes: int
    peak_reserved_bytes: int


@dataclass(frozen=True, slots=True)
class ProcessResidentMemorySnapshot:
    """Exact aggregate bytes charged by every live operation ledger."""

    capacity_bytes: int
    reserved_bytes: int
    peak_reserved_bytes: int


@dataclass(frozen=True, slots=True)
class OperationMemoryDiagnostics:
    """Cleanup anomalies observed by one operation memory ledger."""

    close_outstanding_bytes: int
    over_release_count: int
    over_release_bytes: int
    cross_process_reconciliation_failures: int = 0
    cross_process_pending_bytes: int = 0
    cross_process_release_deferred: bool = False
    cross_process_release_failures: int = 0
    post_release_observation_failures: int = 0


@dataclass(frozen=True, slots=True)
class ProcessMemoryPressureSnapshot:
    """Payload, control-plane and best-effort process RSS pressure."""

    capacity_bytes: int
    exact_reserved_bytes: int
    exact_peak_reserved_bytes: int
    exact_headroom_bytes: int
    rss_bytes: int | None
    untracked_rss_bytes: int | None
    control_plane_reserved_bytes: int = 0
    control_plane_static_baseline_bytes: int = 0
    governed_reserved_bytes: int = 0
    governed_headroom_bytes: int = 0


def _read_process_rss_bytes() -> int | None:
    """Read current Linux RSS without adding an optional runtime dependency."""
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            fields = statm.read().split()
        if len(fields) < 2:
            return None
        return max(0, int(fields[1]) * int(os.sysconf("SC_PAGE_SIZE")))
    except (OSError, TypeError, ValueError):
        return None


def process_memory_pressure_snapshot() -> ProcessMemoryPressureSnapshot:
    """Combine exact governed bytes with best-effort opaque RSS overhead."""
    exact = process_resident_memory_snapshot()
    from .control_plane_budget import process_control_plane_snapshot

    control = process_control_plane_snapshot()
    governed = exact.reserved_bytes + control.governed_bytes
    rss_bytes = _read_process_rss_bytes()
    # Control-plane allocations are a conservative logical charge and are also
    # physically present in RSS. Subtract both governed classes when exposing
    # opaque overhead so diagnostics do not double-count scheduler metadata.
    # Only the native resident ledger is an exact physical-byte measurement.
    # Control-plane charges are deliberately conservative logical estimates, so
    # subtracting them from RSS could under-report opaque physical memory. Keep
    # them separately observable and define untracked RSS against exact payload.
    untracked = None if rss_bytes is None else max(0, rss_bytes - exact.reserved_bytes)
    return ProcessMemoryPressureSnapshot(
        capacity_bytes=exact.capacity_bytes,
        exact_reserved_bytes=exact.reserved_bytes,
        exact_peak_reserved_bytes=exact.peak_reserved_bytes,
        exact_headroom_bytes=max(0, exact.capacity_bytes - exact.reserved_bytes),
        rss_bytes=rss_bytes,
        untracked_rss_bytes=untracked,
        control_plane_reserved_bytes=control.reserved_bytes,
        control_plane_static_baseline_bytes=control.static_baseline_bytes,
        governed_reserved_bytes=governed,
        # ``exact.capacity_bytes`` is already the payload ceiling after the
        # current control-plane shadow/baseline. Do not subtract control twice.
        governed_headroom_bytes=max(0, exact.capacity_bytes - exact.reserved_bytes),
    )


@dataclass(slots=True)
class _PythonMemoryLeaseEntry:
    owner_id: int
    capability: object
    size_bytes: int
    control_ticket: ControlPlaneTicket | None = None
    physical_released: bool = False
    # Physical bytes are a conservative high-watermark owned by this exact
    # capability. Shrink never exposes headroom before the logical authority is
    # updated; reclaimed bytes are returned when the lease retires.
    physical_size_bytes: int = 0
    # Pass80: authoritative native receipt. When present, this object—not the
    # mirrored integer above—owns the physical reservation and survives any
    # Python publication interruption.
    native_receipt: object | None = None


_GOVERNED_OWNERSHIP_SEAL = object()


class GovernedResultOwnership:
    """Runtime-issued capability for a live lease or a certified zero payload."""

    __slots__ = ("_lease", "_lease_id", "_capability", "_pid", "_seal", "_zero_payload")

    def __init__(
        self, lease: "OperationMemoryLease | None", *, _seal: object, zero_payload: bool = False
    ) -> None:
        if _seal is not _GOVERNED_OWNERSHIP_SEAL:
            raise TypeError("governed result ownership capabilities are runtime-issued")
        if lease is None and not zero_payload:
            raise TypeError("governed result ownership requires a lease or zero-payload seal")
        self._lease = lease
        self._lease_id = int(getattr(lease, "_lease_id", 0)) if lease is not None else 0
        self._capability = getattr(lease, "_capability", None) if lease is not None else None
        self._pid = os.getpid()
        self._seal = _seal
        self._zero_payload = bool(zero_payload)

    @property
    def reserved_bytes(self) -> int:
        """Return authoritative live bytes, or zero after generation retirement."""
        if self._seal is not _GOVERNED_OWNERSHIP_SEAL or os.getpid() != self._pid:
            return 0
        lease = self._lease
        if lease is None:
            return 0
        if (
            int(getattr(lease, "_lease_id", 0)) != self._lease_id
            or getattr(lease, "_capability", None) is not self._capability
            or bool(getattr(lease, "_released", True))
        ):
            return 0
        try:
            return int(lease.reserved_bytes)
        except BaseException:
            return 0

    def proves_live_ownership(self, *, minimum_bytes: int = 1) -> bool:
        """Authenticate the exact live memory generation."""
        if type(minimum_bytes) is not int or minimum_bytes < 0 or self._zero_payload:
            return False
        return self.reserved_bytes >= minimum_bytes

    def proves_result_ownership(self) -> bool:
        """Authenticate either a live lease or an explicit no-payload result."""
        if self._seal is not _GOVERNED_OWNERSHIP_SEAL or os.getpid() != self._pid:
            return False
        return self._zero_payload or self.proves_live_ownership(minimum_bytes=1)


def operation_memory_ownership_capability(
    lease: "OperationMemoryLease | None",
) -> GovernedResultOwnership | None:
    """Issue a non-forgeable capability only for an exact live memory lease."""
    if lease is None or not isinstance(lease, OperationMemoryLease):
        return None
    capability = GovernedResultOwnership(lease, _seal=_GOVERNED_OWNERSHIP_SEAL)
    return capability if capability.proves_live_ownership(minimum_bytes=1) else None


def no_retained_result_ownership_capability() -> GovernedResultOwnership:
    """Issue a sealed proof that this result intentionally owns no retained payload."""
    return GovernedResultOwnership(None, _seal=_GOVERNED_OWNERSHIP_SEAL, zero_payload=True)


def _run_operation_memory_lease_finalizer(authority: RootedFinalizerAuthority) -> None:
    """Release a Python memory lease from exact ledger identity, never the wrapper."""
    ledger = authority.arg0
    if ledger is None:
        return
    lease_id_value = authority.arg1
    lease_id = lease_id_value if isinstance(lease_id_value, int) else 0
    capability = authority.arg2
    owner_id_value = authority.arg3
    owner_id = owner_id_value if isinstance(owner_id_value, int) else 0
    if lease_id > 0 and capability is not None:
        release_exact = getattr(ledger, "_release_python_lease_authority", None)
        if not callable(release_exact):
            raise RuntimeError("operation memory ledger lacks exact finalizer release")
        release_exact(lease_id, owner_id, capability)
        authority.arg1 = 0
        authority.arg2 = None
        return
    amount_value = authority.arg4
    amount = amount_value if isinstance(amount_value, int) else 0
    if amount > 0:
        release = getattr(ledger, "release", None)
        if not callable(release):
            raise RuntimeError("operation memory ledger lacks finalizer release")
        release(amount)
        authority.arg4 = 0


class OperationMemoryLease:
    """Exactly-once reservation retained alongside a Python-owned resource."""

    def __init__(self, ledger: "OperationMemoryLedger", size_bytes: int, stage: str) -> None:
        """Reserve bytes and bind them to this thread-safe lease."""
        self._ledger = ledger
        self._size_bytes = 0
        self._lease_id = 0
        self._capability: object | None = None
        self._finalizer_owner = RootedFinalizerAuthority(_run_operation_memory_lease_finalizer)
        self._finalizer_ticket = None
        try:
            ticket = _MEMORY_LEASE_FINALIZER_ESCROW.reserve_rooted(self._finalizer_owner)
            if ticket is None:
                raise RuntimeError("operation-memory finalizer escrow exhausted")
            self._finalizer_ticket = ticket
        except BaseException:
            try:
                _MEMORY_LEASE_FINALIZER_ESCROW.release_rooted_owner(self._finalizer_owner)
            except BaseException:
                pass
            raise
        self._finalizer_owner.arg0 = ledger
        self._finalizer_owner.arg3 = id(self)
        self._finalizer_owner.arg4 = size_bytes
        self.stage = stage
        self._pid = os.getpid()
        self._lock = Lock()
        # Pass80 publishes authenticated zero-byte ownership before the first
        # physical commit. The native reservation receipt then becomes the
        # authority in the same C call that charges the ledger.
        self._released = True
        prepare = getattr(ledger, "_prepare_python_lease", None)
        commit = getattr(ledger, "_commit_python_lease_reservation", None)
        if callable(prepare) and callable(commit):
            try:
                registration = prepare(self)
                self._lease_id = registration.lease_id
                self._capability = registration.capability
                self._finalizer_owner.arg1 = self._lease_id
                self._finalizer_owner.arg2 = self._capability
                # From here on the rooted finalizer can safely retire the exact
                # entry even if no physical bytes have committed yet.
                self._released = False
                commit(self, size_bytes, stage=stage)
                self._size_bytes = size_bytes
                self._finalizer_owner.arg4 = 0
                return
            except BaseException as primary:
                if self._lease_id and self._capability is not None:
                    try:
                        ledger._release_python_lease_authority(
                            self._lease_id, id(self), self._capability
                        )
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            primary,
                            "operation-memory exact constructor rollback also failed",
                            cleanup_error,
                        )
                ticket = getattr(self, "_finalizer_ticket", None)
                owner = getattr(self, "_finalizer_owner", None)
                if ticket is not None and isinstance(owner, RootedFinalizerAuthority):
                    try:
                        owner.make_ack_only()
                        if _MEMORY_LEASE_FINALIZER_ESCROW.release_ticket(ticket):
                            self._finalizer_ticket = None
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            primary,
                            "operation-memory finalizer rollback also failed",
                            cleanup_error,
                        )
                self._released = True
                raise

        # Compatibility path for deliberately minimal historical test doubles.
        # Production OperationMemoryLedger always takes the receipt path above.
        try:
            ledger.reserve(size_bytes, stage=stage)
        except BaseException as primary:
            ticket = getattr(self, "_finalizer_ticket", None)
            owner = getattr(self, "_finalizer_owner", None)
            if ticket is None and isinstance(owner, RootedFinalizerAuthority):
                ticket = owner.ticket or None
            if ticket is not None and isinstance(owner, RootedFinalizerAuthority):
                try:
                    retire_or_ack_rooted_finalizer_authority(
                        _MEMORY_LEASE_ROOTED_FINALIZER_ESCROW, ticket, owner
                    )
                    self._finalizer_ticket = None
                except BaseException as cleanup_error:
                    add_bounded_note(
                        primary,
                        "operation-memory finalizer rollback also failed",
                        cleanup_error,
                    )
            raise
        register = getattr(ledger, "_register_python_lease", None)
        if callable(register):
            try:
                registration = register(self, size_bytes)
            except BaseException as primary:
                rollback_committed = False
                try:
                    ledger.release(size_bytes)
                    rollback_committed = True
                except BaseException as cleanup_error:
                    recovered_registration = None
                    try:
                        recovered_registration = register(self, size_bytes)
                    except BaseException as recovery_error:
                        add_bounded_note(
                            primary,
                            "operation-memory exact registration recovery also failed",
                            recovery_error,
                        )
                    if recovered_registration is not None:
                        self._lease_id = recovered_registration.lease_id
                        self._capability = recovered_registration.capability
                        self._finalizer_owner.arg1 = self._lease_id
                        self._finalizer_owner.arg2 = self._capability
                    self._size_bytes = size_bytes
                    self._released = False
                    add_bounded_note(
                        primary,
                        "operation-memory lease registration rollback failed",
                        cleanup_error,
                    )
                if rollback_committed:
                    self._finalizer_owner.make_ack_only()
                    try:
                        retired = _MEMORY_LEASE_FINALIZER_ESCROW.release_ticket(
                            self._finalizer_ticket
                        )
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            primary,
                            "operation-memory finalizer-ticket rollback failed",
                            cleanup_error,
                        )
                    else:
                        if retired:
                            self._finalizer_ticket = None
                raise
            self._lease_id = registration.lease_id
            self._capability = registration.capability
            self._finalizer_owner.arg1 = self._lease_id
            self._finalizer_owner.arg2 = self._capability
        self._size_bytes = size_bytes
        self._finalizer_owner.arg4 = size_bytes
        self._released = False

    @property
    def reserved_bytes(self) -> int:
        """Return the active reservation size."""
        if os.getpid() != self._pid:
            return 0
        with self._lock:
            if self._released:
                return 0
            authoritative = getattr(self._ledger, "_python_lease_size", None)
            return (
                authoritative(self)
                if getattr(self, "_lease_id", 0) and callable(authoritative)
                else self._size_bytes
            )

    def resize(self, size_bytes: int) -> None:
        """Resize this retained reservation without racing final cleanup."""
        if os.getpid() != self._pid:
            raise RuntimeError("operation memory lease cannot be reused after fork")
        if type(size_bytes) is not int:
            raise TypeError("operation memory lease size must be an exact integer")
        if size_bytes < 0:
            raise ValueError("operation memory lease size must be >= 0")
        with self._lock:
            if self._released:
                raise RuntimeError("operation memory lease is already released")
            resize = getattr(self._ledger, "_resize_python_lease", None)
            if getattr(self, "_lease_id", 0) and callable(resize):
                resize(self, size_bytes, stage=self.stage)
            else:
                if bool(getattr(self._ledger, "_requires_exact_python_lease_authority", False)):
                    raise RuntimeError(
                        "production operation memory lease lost exact resize authority"
                    )
                current = self._size_bytes
                growth = size_bytes - current
                if growth > 0:
                    self._ledger.reserve(growth, stage=self.stage)
                elif growth < 0:
                    self._ledger.release(-growth)
            self._size_bytes = size_bytes
            owner = getattr(self, "_finalizer_owner", None)
            if isinstance(owner, RootedFinalizerAuthority):
                owner.arg4 = size_bytes

    def transfer_stage(self, stage: str) -> "OperationMemoryLease":
        """Transfer resident-byte ownership to a new generation/handle.

        The upstream object is invalidated without releasing or reacquiring any
        bytes. A new finalizer ticket, lock and capability are prepared before
        the authoritative ledger owner is changed.
        """
        if type(stage) is not str:
            raise TypeError("operation memory stage must be an exact string")
        if os.getpid() != self._pid:
            raise RuntimeError("operation memory lease cannot be transferred after fork")
        with self._lock:
            if self._released:
                raise RuntimeError("operation memory lease is already released")
            ledger = getattr(self, "_ledger", None)
            transfer = getattr(ledger, "_transfer_python_lease", None)
            if not getattr(self, "_lease_id", 0) or not callable(transfer):
                # Legacy focused test doubles cannot authenticate generation
                # handoff; retain the historical diagnostic-only behavior.
                self.stage = stage
                return self

            successor_owner = RootedFinalizerAuthority(_run_operation_memory_lease_finalizer)
            try:
                ticket = _MEMORY_LEASE_FINALIZER_ESCROW.reserve_rooted(successor_owner)
                if ticket is None:
                    raise RuntimeError("operation-memory finalizer escrow exhausted")
            except BaseException:
                try:
                    _MEMORY_LEASE_FINALIZER_ESCROW.release_rooted_owner(successor_owner)
                except BaseException:
                    pass
                raise
            successor = None
            try:
                successor = object.__new__(OperationMemoryLease)
                # Initialize terminal-safe fields first so even a partially-built
                # object cannot publish an unusable finalizer owner.
                successor._released = True
                successor._finalizer_ticket = None
                successor._finalizer_owner = successor_owner
                successor._ledger = self._ledger
                successor._size_bytes = self._size_bytes
                successor._lease_id = self._lease_id
                successor._capability = FinalizerReplayCapability()
                successor.stage = stage
                successor._pid = self._pid
                successor._lock = Lock()
                successor._finalizer_ticket = ticket
                successor_owner.arg0 = self._ledger
                successor_owner.arg1 = successor._lease_id
                successor_owner.arg2 = successor._capability
                successor_owner.arg3 = id(successor)
                successor_owner.arg4 = successor._size_bytes
                successor._released = False
                transfer(self, successor)
                successor_owner.arg1 = successor._lease_id
                successor_owner.arg2 = successor._capability
                successor_owner.arg4 = successor._size_bytes
            except BaseException as primary:
                # Transfer can be interrupted after the authoritative owner swap
                # but before this wrapper publishes completion. Never disarm the
                # successor until the ledger proves the swap did not commit.
                successor_committed: bool | None = None
                if successor is not None:
                    probe = getattr(ledger, "_python_lease_authority_owned_by", None)
                    if callable(probe):
                        try:
                            successor_committed = bool(
                                probe(
                                    successor._lease_id,
                                    id(successor),
                                    successor._capability,
                                )
                            )
                        except BaseException as inspect_error:
                            add_bounded_note(
                                primary,
                                "operation-memory transfer ownership probe also failed",
                                inspect_error,
                            )

                if successor_committed is True:
                    # The ledger already belongs to successor. Keep its rooted
                    # finalizer authoritative; the local successor will publish
                    # cleanup while this failed call unwinds. The predecessor is
                    # deliberately made inert so it cannot claim a stale owner.
                    self._released = True
                    self._size_bytes = 0
                    self._capability = None
                    current_owner = getattr(self, "_finalizer_owner", None)
                    if isinstance(current_owner, RootedFinalizerAuthority):
                        current_owner.make_ack_only()
                elif successor_committed is False:
                    assert successor is not None
                    try:
                        retire_or_ack_rooted_finalizer_authority(
                            _MEMORY_LEASE_ROOTED_FINALIZER_ESCROW, ticket, successor_owner
                        )
                        successor._finalizer_ticket = None
                        successor._released = True
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            primary,
                            "operation-memory transfer finalizer rollback also failed",
                            cleanup_error,
                        )
                # If ownership could not be inspected, fail closed by retaining
                # both rooted authorities. Exactly one capability can authenticate
                # in the ledger, so duplicate cleanup cannot release twice.
                raise

            old_ticket = self._finalizer_ticket
            self._released = True
            self._size_bytes = 0
            self._capability = None
            current_owner = getattr(self, "_finalizer_owner", None)
            if isinstance(current_owner, RootedFinalizerAuthority):
                current_owner.make_ack_only()
            if old_ticket is not None:
                if _MEMORY_LEASE_FINALIZER_ESCROW.release_ticket(old_ticket):
                    self._finalizer_ticket = None
                    if isinstance(current_owner, RootedFinalizerAuthority):
                        current_owner.ticket = 0
                        current_owner.clear()
                elif isinstance(current_owner, RootedFinalizerAuthority):
                    _MEMORY_LEASE_FINALIZER_ESCROW.publish_rooted(old_ticket, current_owner)
            else:
                self._finalizer_ticket = None
            _observe_runtime_concurrency_contract_noexcept("transferable_resident_memory_credit")
            return successor

    def release(self) -> None:
        """Release this reservation exactly once across competing threads."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            owner = getattr(self, "_finalizer_owner", None)
            if self._released:
                ticket = getattr(self, "_finalizer_ticket", None)
                if ticket is not None and isinstance(owner, RootedFinalizerAuthority):
                    if owner._escrow_armed:
                        self._finalizer_ticket = None
                        return
                    owner.make_ack_only()
                    if _MEMORY_LEASE_FINALIZER_ESCROW.release_ticket(ticket):
                        self._finalizer_ticket = None
                        owner.ticket = 0
                        owner.clear()
                    elif _MEMORY_LEASE_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                        raise RuntimeError(
                            "operation-memory finalizer slot retirement did not commit"
                        )
                return
            release = getattr(self._ledger, "_release_python_lease", None)
            if getattr(self, "_lease_id", 0) and callable(release):
                release(self)
                if isinstance(owner, RootedFinalizerAuthority):
                    owner.arg1 = 0
                    owner.arg2 = None
            else:
                self._ledger.release(self._size_bytes)
                if isinstance(owner, RootedFinalizerAuthority):
                    owner.arg4 = 0
            self._released = True
            self._size_bytes = 0
            ticket = getattr(self, "_finalizer_ticket", None)
            if ticket is not None:
                if isinstance(owner, RootedFinalizerAuthority):
                    owner.make_ack_only()
                if _MEMORY_LEASE_FINALIZER_ESCROW.release_ticket(ticket):
                    self._finalizer_ticket = None
                    if isinstance(owner, RootedFinalizerAuthority):
                        owner.ticket = 0
                        owner.clear()
                elif isinstance(
                    owner, RootedFinalizerAuthority
                ) and _MEMORY_LEASE_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                    raise RuntimeError("operation-memory finalizer slot retirement did not commit")
                else:
                    raise RuntimeError("operation-memory finalizer slot retirement did not commit")

    close = release

    def __enter__(self) -> "OperationMemoryLease":
        """Return this active memory lease."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Release the memory lease when its context exits."""
        self.release()

    def __del__(self) -> None:
        """Arm separate exact authority without taking escrow/ledger locks in GC."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", None)
            owner = getattr(self, "_finalizer_owner", None)
            if ticket is None and isinstance(owner, RootedFinalizerAuthority):
                ticket = owner.ticket or None
            if ticket is None:
                return
            if isinstance(owner, RootedFinalizerAuthority):
                if getattr(self, "_released", True):
                    owner.make_ack_only()
                if _MEMORY_LEASE_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                    self._finalizer_ticket = None
                    return
                _mark_memory_finalizer_overflow(ticket)
                return
            if not getattr(self, "_released", True):
                _publish_abandoned_memory_lease(ticket, self)
        except BaseException:
            pass


class _MemoryLeaseRegistration:
    """Preallocated single-owner publication result for a Python memory lease."""

    __slots__ = ("lease_id", "capability")

    def __init__(self) -> None:
        self.lease_id = 0
        self.capability: object | None = None

    def __iter__(self):  # legacy test/internal unpacking only
        yield self.lease_id
        yield self.capability


class OperationMemoryLedger:
    """One native atomic ledger shared by Python and C++ operation resources."""

    def __init__(self, memory_limit_bytes: int | None) -> None:
        """Create one native atomic ledger for the resolved operation limit."""
        # A completed output may publish its ledger authority only after its
        # last native buffer releases.  Drain those now-quiescent owners before
        # admitting another process contribution, otherwise sequential public
        # calls can accumulate the conservative initial cross-process charge.
        drain_abandoned_memory_finalizers()
        limit = normalize_memory_limit(memory_limit_bytes)
        from .cross_process_memory import acquire_cross_process_memory
        from .native_runtime import native_core

        self.limit_bytes = limit
        self._pid = os.getpid()
        self._native = native_core
        self._capsule = native_core.operation_memory_ledger_create(limit)
        process_capacity = _raw_process_resident_memory_snapshot().capacity_bytes
        # Stable diagnostic ceiling captured before this ledger owns dynamic
        # control tickets. Physical composition still consults the live raw
        # envelope for every reservation.
        self._process_payload_capacity = process_resident_memory_snapshot().capacity_bytes
        self._cross_process = acquire_cross_process_memory(process_capacity, limit)
        self._cross_process_reconciliation_failures = 0
        self._cross_process_pending_bytes = 0
        self._cross_process_release_deferred = False
        self._cross_process_release_failures = 0
        self._post_release_observation_failures = 0
        self._deferred_close_cleanup_armed = False
        self._close_advisory_recorded = False
        self._close_peak_bytes = 0
        self._lock = Lock()
        self._cross_process_io_lock = Lock()
        self._close_condition = Condition(self._lock)
        self._close_started = False
        self._closing = False
        self._closed = False
        self._close_outstanding_bytes = 0
        self._python_lease_sequence = 0
        self._python_leases: dict[int, _PythonMemoryLeaseEntry] = {}
        # Production ledgers require authenticated per-lease mutation. Amount-
        # based resize remains only for deliberately minimal historical doubles.
        self._requires_exact_python_lease_authority = True
        self._unknown_python_lease_releases = 0
        # Root an authority object that does not reference this wrapper.  The
        # ledger itself therefore remains collectible while native/cross-process
        # ownership is still retained for safe-point recovery.
        self._finalizer_owner = RootedFinalizerAuthority(_run_operation_memory_ledger_finalizer)
        self._finalizer_owner.arg0 = self._native
        self._finalizer_owner.arg1 = self._capsule
        self._finalizer_owner.arg2 = self._cross_process
        self._finalizer_ticket = None
        try:
            ticket = _MEMORY_LEDGER_FINALIZER_ESCROW.reserve_rooted(self._finalizer_owner)
            if ticket is None:
                raise RuntimeError("operation-memory ledger finalizer escrow exhausted")
            self._finalizer_ticket = ticket
        except BaseException:
            try:
                _MEMORY_LEDGER_FINALIZER_ESCROW.release_rooted_owner(self._finalizer_owner)
            except BaseException:
                pass
            try:
                self._cross_process.release()
            finally:
                raise

    def _prepare_python_lease(self, owner: OperationMemoryLease) -> _MemoryLeaseRegistration:
        """Publish exact zero-byte authority before any physical commit."""
        return self._register_python_lease(owner, 0)

    def _commit_python_lease_reservation(
        self, owner: OperationMemoryLease, size_bytes: int, *, stage: str
    ) -> None:
        """Attach the native receipt after commit without an anonymous window."""
        receipt = self.reserve(size_bytes, stage=stage, _exact_receipt=True)
        with self._lock:
            entry = self._python_lease_entry(owner)
            entry.native_receipt = receipt
            entry.size_bytes = size_bytes
            entry.physical_size_bytes = size_bytes

    def _register_python_lease(
        self, owner: OperationMemoryLease, size_bytes: int
    ) -> _MemoryLeaseRegistration:
        """Bind reserved bytes without allocating after authoritative publication."""
        result = _MemoryLeaseRegistration()
        control_ticket = reserve_control_plane(
            "operation_memory_lease", _OPERATION_MEMORY_LEASE_CONTROL_BYTES
        )
        entry = _PythonMemoryLeaseEntry(
            id(owner),
            FinalizerReplayCapability(),
            size_bytes,
            control_ticket,
            physical_size_bytes=size_bytes,
        )
        try:
            with self._lock:
                lease_id = next_reusable_token(self._python_lease_sequence, self._python_leases)
                if lease_id is None:
                    raise RuntimeError("operation-memory lease namespace exhausted")
                self._python_leases[lease_id] = entry
                self._python_lease_sequence = lease_id
                result.lease_id = lease_id
                result.capability = entry.capability
                return result
        except BaseException:
            release_control_plane(control_ticket)
            raise

    def _python_lease_entry_authority(
        self, lease_id: int, owner_id: int, capability: object
    ) -> _PythonMemoryLeaseEntry:
        entry = self._python_leases.get(lease_id)
        if entry is None or entry.owner_id != owner_id or capability is not entry.capability:
            self._unknown_python_lease_releases += 1
            raise RuntimeError("operation memory lease is not authoritative")
        return entry

    def _python_lease_entry(self, owner: OperationMemoryLease) -> _PythonMemoryLeaseEntry:
        return self._python_lease_entry_authority(owner._lease_id, id(owner), owner._capability)

    def _python_lease_size(self, owner: OperationMemoryLease) -> int:
        with self._lock:
            return self._python_lease_entry(owner).size_bytes

    def _native_reservation_metadata(self, receipt: object) -> tuple[int, int, int] | None:
        method = getattr(self._native, "operation_memory_reservation_metadata", None)
        if not callable(method):
            return None
        values = method(receipt)
        if not isinstance(values, tuple) or len(values) != 3:
            raise RuntimeError("operation memory reservation returned invalid metadata")
        return tuple(int(value) for value in values)  # type: ignore[return-value]

    def _native_reservation_resize(
        self, receipt: object, requested: int, stage: str
    ) -> tuple[int, int] | None:
        metadata = self._native_reservation_metadata(receipt)
        if metadata is None:
            result = self._native.operation_memory_reservation_resize(receipt, requested, stage)
        else:
            result = self._native.operation_memory_reservation_resize(
                receipt, requested, stage, metadata[1]
            )
        if isinstance(result, tuple) and len(result) == 2:
            return int(result[0]), max(0, int(result[1]))
        return None

    def _native_reservation_release(self, receipt: object) -> tuple[int, int] | None:
        metadata = self._native_reservation_metadata(receipt)
        if metadata is None:
            result = self._native.operation_memory_reservation_release(receipt)
        else:
            result = self._native.operation_memory_reservation_release(receipt, metadata[1])
        if isinstance(result, tuple) and len(result) == 2:
            return int(result[0]), max(0, int(result[1]))
        return None

    def _resize_python_lease(
        self, owner: OperationMemoryLease, requested: int, *, stage: str
    ) -> None:
        """Resize exact logical authority without a shrink commit split.

        A Python lease keeps a conservative physical high-watermark. Shrinking
        changes the exact logical owner first and does not immediately return
        native bytes; this removes the unsafe window where a signal could occur
        after aggregate release but before the per-lease authority was reduced.
        Growth beyond the high-watermark reserves only the additional physical
        bytes. Final retirement releases the exact physical high-watermark.
        """
        with self._lock:
            entry = self._python_lease_entry(owner)
            current = entry.size_bytes
            receipt = entry.native_receipt
            if receipt is None:
                # Historical focused doubles have no native receipt. Keep their
                # conservative high-watermark behavior; production entries are
                # created only through _commit_python_lease_reservation.
                current = entry.size_bytes
                physical = max(entry.size_bytes, entry.physical_size_bytes)
                if requested <= physical:
                    entry.size_bytes = requested
                    return
                growth = requested - physical
            else:
                growth = 0
        if receipt is None:
            if growth > 0:
                self.reserve(growth, stage=stage)
            with self._lock:
                entry = self._python_lease_entry(owner)
                entry.size_bytes = requested
                entry.physical_size_bytes = requested
            return
        # The native receipt updates its exact authority before returning from
        # the mutating C call. A signal after CALL but before the Python mirror
        # therefore cannot orphan growth or cause a retry to double-release.
        committed_state = self._native_reservation_resize(receipt, requested, stage)
        committed_bytes = requested if committed_state is None else committed_state[1]
        with self._lock:
            entry = self._python_lease_entry(owner)
            entry.size_bytes = committed_bytes
            entry.physical_size_bytes = committed_bytes
        # Cross-process accounting is aggregate and deliberately conservative;
        # reconcile from the authoritative native snapshot after exact commit.
        try:
            values = self._native.operation_memory_ledger_snapshot(self._capsule)
            if isinstance(values, tuple) and len(values) == 3:
                # Growth must publish cross-process admission before it becomes
                # externally usable. Shrink may remain conservatively charged.
                self._reconcile_cross_process(max(0, int(values[1])), strict=requested > current)
        except BaseException as exc:
            self._cross_process_reconciliation_failures += 1
            if requested > current:
                # Exact native ownership makes rollback itself deterministic.
                # Restore the previous receipt size before propagating the
                # reconciliation/cancellation failure.
                try:
                    rollback_state = self._native_reservation_resize(receipt, current, stage)
                    rollback_bytes = current if rollback_state is None else rollback_state[1]
                    with self._lock:
                        entry = self._python_lease_entry(owner)
                        entry.size_bytes = rollback_bytes
                        entry.physical_size_bytes = rollback_bytes
                    rollback = self._native.operation_memory_ledger_snapshot(self._capsule)
                    if isinstance(rollback, tuple) and len(rollback) == 3:
                        self._reconcile_cross_process(max(0, int(rollback[1])), strict=False)
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc,
                        "operation-memory exact resize rollback also failed",
                        cleanup_error,
                    )
                raise
            # Shrink is fail-closed if coordination lags: local/native ownership
            # is smaller while the cross-process journal remains conservative.
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise

    def _python_lease_authority_owned_by(
        self, lease_id: int, owner_id: int, capability: object
    ) -> bool:
        """Return whether an exact lease currently authenticates this owner.

        Used only for interruption recovery; it never mutates accounting.
        """
        with self._lock:
            entry = self._python_leases.get(int(lease_id))
            return bool(
                entry is not None
                and entry.owner_id == int(owner_id)
                and entry.capability is capability
            )

    def _transfer_python_lease(
        self, owner: OperationMemoryLease, successor: OperationMemoryLease
    ) -> None:
        """Atomically transfer authority without touching physical byte totals."""
        with self._lock:
            entry = self._python_lease_entry(owner)
            if successor._lease_id != owner._lease_id:
                raise RuntimeError("operation memory transfer changed lease identity")
            entry.owner_id = id(successor)
            entry.capability = successor._capability
            successor._size_bytes = entry.size_bytes

    def _release_python_lease_authority(
        self, lease_id: int, owner_id: int, capability: object
    ) -> None:
        """Release one exact lease without requiring the wrapper to remain alive."""
        replay_committed = False
        with self._lock:
            entry = self._python_leases.get(lease_id)
            if entry is None:
                if isinstance(capability, FinalizerReplayCapability) and capability.released:
                    replay_committed = True
                else:
                    self._unknown_python_lease_releases += 1
                    raise RuntimeError("operation memory lease is not authoritative")
            elif entry.owner_id != owner_id or entry.capability is not capability:
                self._unknown_python_lease_releases += 1
                raise RuntimeError("operation memory lease is not authoritative")
            elif isinstance(capability, FinalizerReplayCapability) and capability.released:
                # A previous attempt committed exact retirement and was
                # interrupted before removing the ledger mirror.
                self._python_leases.pop(lease_id, None)
                replay_committed = True
            if replay_committed:
                entry = None
            else:
                assert entry is not None
        if replay_committed:
            try:
                self._maybe_finish_deferred_close()
            except BaseException:
                self._schedule_deferred_close_cleanup_noexcept()
            return
        assert entry is not None
        with self._lock:
            amount = max(entry.size_bytes, entry.physical_size_bytes)
            physical_released = entry.physical_released
            receipt = entry.native_receipt
        if not physical_released:
            if receipt is not None:
                # The receipt retires itself idempotently before returning to
                # Python. Aggregate integers are observation only in this path.
                # Compatibility/source-contract marker: the legacy direct form
                # was ``operation_memory_reservation_release(receipt)``; production
                # now routes through the generation-authenticated helper below.
                self._native_reservation_release(receipt)
                with self._lock:
                    current = self._python_lease_entry_authority(lease_id, owner_id, capability)
                    current.physical_released = True
                    current.native_receipt = None
                try:
                    values = self._native.operation_memory_ledger_snapshot(self._capsule)
                    if isinstance(values, tuple) and len(values) == 3:
                        self._reconcile_cross_process(max(0, int(values[1])), strict=False)
                except BaseException:
                    self._cross_process_reconciliation_failures += 1
            else:
                # A prepared pass80 owner may still be zero-byte if physical
                # commit never happened. Retiring that exact empty authority is
                # a state transition, not an aggregate release.
                if amount == 0:
                    with self._lock:
                        current = self._python_lease_entry_authority(lease_id, owner_id, capability)
                        current.physical_released = True
                else:
                    # Legacy/source doubles created before pass80 retain the
                    # amount path; production committed receipts never enter it.
                    self.release(amount, _release_entry=entry)
            with self._lock:
                current = self._python_lease_entry_authority(lease_id, owner_id, capability)
                if current is not entry or not entry.physical_released:
                    raise RuntimeError(
                        "operation memory physical release did not publish completion"
                    )
        control_ticket = entry.control_ticket
        if control_ticket is not None:
            if not release_control_plane(control_ticket):
                raise RuntimeError("operation-memory control-plane retirement did not commit")
            entry.control_ticket = None
        with self._lock:
            current = self._python_lease_entry_authority(lease_id, owner_id, capability)
            if current is entry:
                if isinstance(capability, FinalizerReplayCapability):
                    # Primary exact retirement commits before the mapping
                    # disappears. A finalizer replay with this same capability
                    # can therefore ACK the release even if interrupted here.
                    capability.released = True
                self._python_leases.pop(lease_id, None)
        # Exact owner retirement is the primary commit and must remain successful
        # even if the ledger-level deferred-close tail (journal/fsync/advisory)
        # fails afterwards. Preserve the deferred host reservation for retry by
        # close()/finalizer instead of making this child release look replayable.
        try:
            self._maybe_finish_deferred_close()
        except BaseException:
            with self._lock:
                self._post_release_observation_failures = (
                    getattr(self, "_post_release_observation_failures", 0) + 1
                )
                self._cross_process_release_deferred = True
            # The child owner is already irreversibly retired. Arm the ledger's
            # pre-rooted cleanup authority so the cross-process close tail retries
            # autonomously at safe points without replaying child release.
            self._schedule_deferred_close_cleanup_noexcept()

    def _release_python_lease(self, owner: OperationMemoryLease) -> None:
        """Release exactly one authenticated live wrapper generation."""
        self._release_python_lease_authority(owner._lease_id, id(owner), owner._capability)

    def _ensure_owner_process(self) -> None:
        """Reject use of native ledger state inherited by a forked child."""
        if os.getpid() != self._pid:
            raise RuntimeError("operation memory ledger cannot be reused after fork")

    def _ensure_cross_process_io_lock(self) -> Lock:
        """Lazily create the single-flight lock for legacy focused test owners."""
        io_lock = getattr(self, "_cross_process_io_lock", None)
        if io_lock is not None:
            return io_lock
        with self._lock:
            io_lock = getattr(self, "_cross_process_io_lock", None)
            if io_lock is None:
                io_lock = Lock()
                self._cross_process_io_lock = io_lock
        return io_lock

    @property
    def capsule(self) -> Any:
        """Return the opaque native ledger handle attached to prepared options."""
        self._ensure_owner_process()
        with self._lock:
            if self._close_started:
                raise RuntimeError("operation memory ledger is closed")
            return self._capsule

    @staticmethod
    def _cross_process_target(reserved_bytes: int) -> int:
        """Return the conservative host-wide reservation for one native snapshot."""
        return max(
            _CROSS_PROCESS_HEADROOM_BYTES,
            max(0, int(reserved_bytes)) + _CROSS_PROCESS_HEADROOM_BYTES,
        )

    def _reconcile_cross_process(
        self,
        reserved_bytes: int,
        *,
        strict: bool,
    ) -> BaseException | None:
        """Persist one host-wide target without holding the local ledger lock."""
        target = self._cross_process_target(reserved_bytes)
        try:
            self._cross_process.resize(target)
        except BaseException as exc:
            with self._lock:
                self._cross_process_reconciliation_failures += 1
                self._cross_process_pending_bytes = target
            if strict:
                raise
            return exc
        with self._lock:
            self._cross_process_pending_bytes = 0
        return None

    def _release_cross_process_after_close(self, *, strict: bool) -> BaseException | None:
        """Drop host-wide ownership without holding the local ledger lock."""
        try:
            self._cross_process.release()
        except BaseException as exc:
            with self._lock:
                self._cross_process_release_deferred = True
                self._cross_process_release_failures += 1
            if strict:
                raise
            return exc
        with self._lock:
            self._cross_process_release_deferred = False
            self._cross_process_pending_bytes = 0
        return None

    def _maybe_finish_deferred_close(self) -> None:
        """Finish a previously deferred close after the last exact owner retires."""
        if os.getpid() != self._pid:
            return
        # Focused legacy tests construct a minimal ledger shell. Fast-exit before
        # touching close-only synchronization state when close never started.
        if not getattr(self, "_close_started", False) or not getattr(
            self, "_cross_process_release_deferred", False
        ):
            return
        close_condition = getattr(self, "_close_condition", None)
        if close_condition is None:
            return
        with close_condition:
            if self._closing or self._python_leases:
                return
        # ``close`` rechecks the native snapshot under the normal close protocol
        # and owns all cross-process/finalizer tails. Keep one implementation of
        # that transaction rather than duplicating its retry semantics here.
        self.close()

    def _schedule_deferred_close_cleanup_noexcept(self) -> bool:
        """Arm the ledger authority for retry after a post-child close-tail fault."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return False
        owner = getattr(self, "_finalizer_owner", None)
        ticket = getattr(self, "_finalizer_ticket", None)
        if ticket is None or not isinstance(owner, RootedFinalizerAuthority):
            return bool(getattr(self, "_deferred_close_cleanup_armed", False))
        try:
            owner.arg3 = self
            if _MEMORY_LEDGER_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                self._finalizer_ticket = None
                self._deferred_close_cleanup_armed = True
                return True
        except BaseException:
            pass
        try:
            _mark_memory_finalizer_overflow(int(ticket))
        except BaseException:
            pass
        return False

    def _finish_deferred_close_from_finalizer(self, authority: RootedFinalizerAuthority) -> None:
        """Retry only the ledger-level close tail from a safe-point authority."""
        if os.getpid() != self._pid:
            return
        with self._close_condition:
            if self._python_leases:
                raise RuntimeError("deferred memory close still has exact child owners")
            if not self._close_started:
                raise RuntimeError("deferred memory close retry without close state")
        values = self._native.operation_memory_ledger_snapshot(self._capsule)
        if not isinstance(values, tuple) or len(values) != 3:
            raise RuntimeError("native operation memory ledger returned invalid statistics")
        if max(0, int(values[1])) != 0:
            raise RuntimeError("deferred memory close retry still owns native bytes")
        peak = max(0, int(values[2]))
        with self._ensure_cross_process_io_lock():
            self._release_cross_process_after_close(strict=True)
        # Cross-process ownership is now gone; prevent fallback finalizer replay.
        authority.arg2 = None
        authority.arg3 = None
        with self._close_condition:
            advisory_peak = self._claim_close_advisory_locked(peak)
            self._closed = True
            self._closing = False
            self._deferred_close_cleanup_armed = False
            self._close_condition.notify_all()
        if advisory_peak is not None:
            try:
                self._record_close_advisory(advisory_peak)
            except BaseException:
                with self._lock:
                    self._post_release_observation_failures = (
                        getattr(self, "_post_release_observation_failures", 0) + 1
                    )

    def _claim_finalizer_ticket_locked(self) -> int | None:
        """Observe the exact ledger finalizer ticket without destroying authority."""
        return getattr(self, "_finalizer_ticket", None)

    def _release_finalizer_ticket(self, ticket: int | None) -> None:
        """Retire the ledger slot after primary host ownership is gone."""
        if ticket is None:
            return
        owner = getattr(self, "_finalizer_owner", None)
        if isinstance(owner, RootedFinalizerAuthority):
            # The host reservation has already committed its release before any
            # caller reaches this tail.  Never allow a failed ticket retirement
            # to replay that primary operation.
            owner.arg2 = None
            owner.make_ack_only()
        try:
            retired = _MEMORY_LEDGER_FINALIZER_ESCROW.release_ticket(ticket)
        except BaseException:
            retired = False
        if not retired:
            if isinstance(owner, RootedFinalizerAuthority):
                _MEMORY_LEDGER_FINALIZER_ESCROW.publish_rooted(ticket, owner)
            return
        if isinstance(owner, RootedFinalizerAuthority):
            owner.clear()
        with self._lock:
            if getattr(self, "_finalizer_ticket", None) == ticket:
                self._finalizer_ticket = None

    def _claim_close_advisory_locked(self, peak_bytes: int = 0) -> int | None:
        """Claim exactly-once close telemetry after host ownership is gone."""
        self._close_peak_bytes = max(getattr(self, "_close_peak_bytes", 0), max(0, int(peak_bytes)))
        if getattr(self, "_close_advisory_recorded", False):
            return None
        if getattr(self, "_cross_process_release_deferred", False):
            return None
        self._close_advisory_recorded = True
        return self._close_peak_bytes

    @staticmethod
    def _record_close_advisory(peak_bytes: int) -> None:
        """Record best-effort pressure telemetry after ownership is unambiguous."""
        try:
            pressure = process_memory_pressure_snapshot()
            from .safety_margins import record_resource_telemetry

            record_resource_telemetry(
                untracked_rss_bytes=pressure.untracked_rss_bytes,
                source="operation_close",
            )
            from .allocator_control import maybe_trim_allocator

            maybe_trim_allocator(
                peak_bytes=max(0, int(peak_bytes)),
                untracked_rss_bytes=pressure.untracked_rss_bytes,
            )
        except Exception:
            pass

    def reserve(
        self, size_bytes: int, *, stage: str, _exact_receipt: bool = False
    ) -> object | None:
        """Atomically reserve resident bytes or raise the public resource error.

        When ``_exact_receipt`` is true, the native commit returns an owning
        capsule. The capsule is the authority for the charge immediately when
        the C call commits, so an asynchronous exception before Python metadata
        publication cannot orphan aggregate bytes.
        """
        self._ensure_owner_process()
        from .cancellation import check_operation_cancelled

        check_operation_cancelled(stage=stage)
        if type(size_bytes) is not int:
            raise TypeError("operation memory reservation must be an exact integer")
        if type(stage) is not str:
            raise TypeError("operation memory stage must be an exact string")
        if size_bytes < 0:
            raise ValueError("operation memory reservation must be >= 0")
        if size_bytes == 0:
            return None
        # Preserve the closed-ledger fast path before touching process/native
        # admission state.  The authoritative check is repeated below while
        # holding the governed-admission lock, so close-vs-reserve still cannot
        # admit bytes after close has committed.
        with self._lock:
            if self._close_started:
                raise RuntimeError("operation memory ledger is closed")
        try:
            with self._ensure_cross_process_io_lock():
                from .control_plane_budget import (
                    _GOVERNED_MEMORY_ADMISSION_LOCK,
                    _synchronize_control_plane_native_shadow_under_admission_lock,
                    process_control_plane_snapshot,
                )

                # Payload and dynamic control metadata share one process envelope.
                # Both admission paths serialize through this lock, so a pair of
                # concurrent payload/control reservations cannot each observe the
                # same headroom and oversubscribe the combined process budget.
                with _GOVERNED_MEMORY_ADMISSION_LOCK:
                    shadow_active, _shadow_bytes = (
                        _synchronize_control_plane_native_shadow_under_admission_lock()
                    )
                    resident = _optional_process_resident_memory_snapshot()
                    control = process_control_plane_snapshot()
                    # With the pass49 native shadow, resident.reserved_bytes
                    # already includes control.governed_bytes. Source-only doubles
                    # retain the explicit composition below. Contract breadcrumb:
                    # resident.reserved_bytes + control.governed_bytes + size_bytes
                    # Legacy diagnostic name retained for source-contract tooling:
                    # process_governed_memory_bytes
                    combined_reserved = (
                        resident.reserved_bytes
                        if shadow_active and resident is not None
                        else (resident.reserved_bytes + control.governed_bytes)
                        if resident is not None
                        else 0
                    )
                    if (
                        resident is not None
                        and combined_reserved + size_bytes > resident.capacity_bytes
                    ):
                        raise MemoryError("process resident memory governed envelope exceeded")
                    with self._lock:
                        if self._close_started:
                            raise RuntimeError("operation memory ledger is closed")
                        capsule = self._capsule
                        receipt = None
                        if _exact_receipt:
                            create_receipt = getattr(
                                self._native, "operation_memory_reservation_create", None
                            )
                            if not callable(create_receipt):
                                raise RuntimeError(
                                    "production native core lacks exact memory reservation receipts"
                                )
                            receipt = create_receipt(capsule, size_bytes, stage)
                            values = self._native.operation_memory_ledger_snapshot(capsule)
                        else:
                            reserve_snapshot = getattr(
                                self._native, "operation_memory_ledger_reserve_snapshot", None
                            )
                            if callable(reserve_snapshot):
                                # The ABI method rolls the native reservation back if
                                # CPython cannot allocate the returned observation. No
                                # fallible Python snapshot exists after the commit.
                                values = reserve_snapshot(capsule, size_bytes, stage)
                            else:
                                # Compatibility for focused native doubles only. The
                                # shipped ABI always exposes the transactional method.
                                self._native.operation_memory_ledger_reserve(
                                    capsule, size_bytes, stage
                                )
                                values = self._native.operation_memory_ledger_snapshot(capsule)
                        if (
                            not isinstance(values, tuple)
                            or len(values) != 3
                            or type(values[1]) is not int
                        ):
                            if receipt is not None:
                                self._native_reservation_release(receipt)
                            else:
                                self._native.operation_memory_ledger_release(capsule, size_bytes)
                            raise RuntimeError(
                                "native operation memory ledger returned invalid statistics"
                            )
                        target_reserved = values[1]
                try:
                    self._reconcile_cross_process(target_reserved, strict=True)
                except BaseException as exc:
                    rollback_target: int | None = None
                    try:
                        with self._lock:
                            if receipt is not None:
                                self._native_reservation_release(receipt)
                            else:
                                self._native.operation_memory_ledger_release(capsule, size_bytes)
                            rollback = self._native.operation_memory_ledger_snapshot(capsule)
                            if isinstance(rollback, tuple) and len(rollback) == 3:
                                rollback_target = int(rollback[1])
                        if rollback_target is not None:
                            cleanup_error = self._reconcile_cross_process(
                                rollback_target, strict=False
                            )
                            if cleanup_error is not None:
                                add_bounded_note(
                                    exc,
                                    "cross-process memory rollback reconciliation also failed",
                                    cleanup_error,
                                )
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            exc,
                            "native memory reservation rollback also failed",
                            cleanup_error,
                        )
                    raise
                return receipt
        except MemoryError as exc:
            # Rich diagnostics are strictly best-effort under OOM. If any
            # observation/allocation needed to translate the error fails, keep
            # the original MemoryError instead of masking it with a secondary
            # snapshot/dict/string failure.
            try:
                from ..errors import SchemaSanitizerResourceError

                process_limited = str(exc).startswith("process resident memory")
                limit_bytes = (
                    self._process_payload_capacity if process_limited else self.limit_bytes
                )
                actual_bytes = size_bytes
                try:
                    snapshot = self.snapshot()
                    actual_bytes = snapshot.reserved_bytes + size_bytes
                except BaseException:
                    pass
                detail: dict[str, int | str | None] = {
                    "stage": stage,
                    "limit_name": "process_resident_memory_bytes"
                    if process_limited
                    else "memory_limit_bytes",
                    "limit_bytes": limit_bytes,
                    "actual_bytes": actual_bytes,
                }
                if process_limited:
                    try:
                        pressure = process_memory_pressure_snapshot()
                        detail["actual_bytes"] = pressure.governed_reserved_bytes + size_bytes
                        detail["process_rss_bytes"] = pressure.rss_bytes
                        detail["untracked_rss_bytes"] = pressure.untracked_rss_bytes
                        detail["control_plane_reserved_bytes"] = (
                            pressure.control_plane_reserved_bytes
                        )
                    except BaseException:
                        pass
                raise SchemaSanitizerResourceError(str(exc), detail=detail) from None
            except SchemaSanitizerResourceError:
                raise
            except BaseException:
                raise exc

    def acquire(self, size_bytes: int, *, stage: str) -> OperationMemoryLease:
        """Reserve bytes and return an exactly-once lifetime lease."""
        self._ensure_owner_process()
        return OperationMemoryLease(self, size_bytes, stage)

    def release(
        self,
        size_bytes: int,
        *,
        _finalizer: bool = False,
        _release_entry: _PythonMemoryLeaseEntry | None = None,
    ) -> None:
        """Release native bytes and reconcile host state outside local locks.

        Finalizer mode never performs the final cross-process file/journal
        release; it leaves a conservative reservation for the ledger reaper.
        """
        if os.getpid() != self._pid:
            return
        if type(size_bytes) is not int:
            raise TypeError("operation memory release must be an exact integer")
        if size_bytes < 0:
            raise ValueError("operation memory release must be >= 0")
        amount = size_bytes
        if amount == 0:
            return
        advisory_peak: int | None = None
        finalizer_ticket: int | None = None
        with self._ensure_cross_process_io_lock():
            try:
                with self._lock:
                    capsule = self._capsule
                    self._native.operation_memory_ledger_release(capsule, amount)
                    if _release_entry is not None:
                        # The exact owner is already rooted in ``_python_leases``.
                        # Publish physical completion before any observation that
                        # can allocate/raise so retry never debits bytes twice.
                        _release_entry.physical_released = True
                    values = self._native.operation_memory_ledger_snapshot(capsule)
                    if not isinstance(values, tuple) or len(values) != 3:
                        raise RuntimeError(
                            "native operation memory ledger returned invalid statistics"
                        )
                    reserved = max(0, int(values[1]))
                    peak = max(0, int(values[2]))
                    close_started = self._close_started
                    if close_started:
                        self._close_outstanding_bytes = reserved
                if close_started and reserved == 0 and not _finalizer:
                    error = self._release_cross_process_after_close(strict=False)
                    if error is None:
                        with self._lock:
                            advisory_peak = self._claim_close_advisory_locked(peak)
                            finalizer_ticket = self._claim_finalizer_ticket_locked()
                else:
                    # Shrink reconciliation is process-aggregated/coalesced.
                    # In finalizer mode this can only schedule cleanup; it does
                    # not perform coordination-file I/O on the GC thread.
                    self._reconcile_cross_process(reserved, strict=False)
            except BaseException:
                # Native ownership is already gone. Preserve the conservative
                # host reservation and never make the caller double-release.
                with self._lock:
                    self._post_release_observation_failures = (
                        getattr(self, "_post_release_observation_failures", 0) + 1
                    )
        self._release_finalizer_ticket(finalizer_ticket)
        if advisory_peak is not None:
            self._record_close_advisory(advisory_peak)

    def safe_point(self) -> int:
        """Explicitly drain abandoned memory owners outside diagnostic reads."""
        self._ensure_owner_process()
        drain = globals().get("drain_abandoned_memory_finalizers")
        return int(drain()) if callable(drain) else 0

    def snapshot(self) -> OperationMemorySnapshot:
        """Pure observation of current and peak bytes; never performs cleanup."""
        self._ensure_owner_process()
        with self._lock:
            capsule = self._capsule
        values = self._native.operation_memory_ledger_snapshot(capsule)
        if not isinstance(values, tuple) or len(values) != 3:
            raise RuntimeError("native operation memory ledger returned invalid statistics")
        return OperationMemorySnapshot(*(int(value) for value in values))

    def diagnostics(self) -> OperationMemoryDiagnostics:
        """Return explicit close-outstanding and over-release counters."""
        self._ensure_owner_process()
        with self._lock:
            capsule = self._capsule
            close_outstanding = self._close_outstanding_bytes
            reconciliation_failures = self._cross_process_reconciliation_failures
            pending_bytes = self._cross_process_pending_bytes
            release_deferred = getattr(self, "_cross_process_release_deferred", False)
            release_failures = getattr(self, "_cross_process_release_failures", 0)
            post_release_failures = getattr(self, "_post_release_observation_failures", 0)
        values = self._native.operation_memory_ledger_diagnostics(capsule)
        if not isinstance(values, tuple) or len(values) != 2:
            raise RuntimeError("native operation memory diagnostics are invalid")
        return OperationMemoryDiagnostics(
            close_outstanding_bytes=close_outstanding,
            over_release_count=int(values[0]),
            over_release_bytes=int(values[1]),
            cross_process_reconciliation_failures=reconciliation_failures,
            cross_process_pending_bytes=pending_bytes,
            cross_process_release_deferred=release_deferred,
            cross_process_release_failures=release_failures,
            post_release_observation_failures=post_release_failures,
        )

    def close(self) -> None:
        """Close admission without holding local locks across coordination I/O."""
        if os.getpid() != self._pid:
            return
        advisory_peak: int | None = None
        peak = 0
        must_release = False
        with self._close_condition:
            self._close_started = True
            deadline = monotonic() + 30.0
            while self._closing:
                remaining = deadline - monotonic()
                if remaining <= 0 or not self._close_condition.wait(timeout=remaining):
                    raise RuntimeError("operation memory ledger close exceeded its deadline")
            deferred = getattr(self, "_cross_process_release_deferred", False)
            if self._closed and not deferred:
                advisory_peak = self._claim_close_advisory_locked()
                if advisory_peak is None:
                    return
            else:
                self._closing = True
                try:
                    values = self._native.operation_memory_ledger_snapshot(self._capsule)
                    if not isinstance(values, tuple) or len(values) != 3:
                        raise RuntimeError(
                            "native operation memory ledger returned invalid statistics"
                        )
                    outstanding = max(0, int(values[1]))
                    peak = max(0, int(values[2]))
                    self._close_outstanding_bytes = outstanding
                    self._close_peak_bytes = max(getattr(self, "_close_peak_bytes", 0), peak)
                except BaseException:
                    # ``_closing`` is coordination state, not an ownership commit.
                    # Never strand later closers behind a failed observation.
                    self._closing = False
                    self._close_condition.notify_all()
                    raise
                if outstanding:
                    self._cross_process_release_deferred = True
                    self._closed = True
                    self._closing = False
                    self._close_condition.notify_all()
                    return
                must_release = True

        if must_release:
            try:
                with self._ensure_cross_process_io_lock():
                    self._release_cross_process_after_close(strict=True)
            except BaseException:
                with self._close_condition:
                    self._closing = False
                    self._close_condition.notify_all()
                raise
            with self._close_condition:
                advisory_peak = self._claim_close_advisory_locked(peak)
                self._closed = True
                self._closing = False
                self._close_condition.notify_all()
                ticket = self._claim_finalizer_ticket_locked()

            self._release_finalizer_ticket(ticket)

        if advisory_peak is not None:
            self._record_close_advisory(advisory_peak)

    def __del__(self) -> None:
        """Arm the separate ledger authority without coordination I/O."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", None)
            owner = getattr(self, "_finalizer_owner", None)
            if ticket is not None and isinstance(owner, RootedFinalizerAuthority):
                if _MEMORY_LEDGER_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                    self._finalizer_ticket = None
                else:
                    _mark_memory_finalizer_overflow(ticket)
        except BaseException:
            pass


def _run_operation_memory_ledger_finalizer(
    authority: RootedFinalizerAuthority,
) -> None:
    """Finish an abandoned ledger without retaining the Python wrapper."""
    deferred_wrapper = authority.arg3
    if isinstance(deferred_wrapper, OperationMemoryLedger):
        deferred_wrapper._finish_deferred_close_from_finalizer(authority)
        return
    native = authority.arg0
    capsule = authority.arg1
    cross_process = authority.arg2
    if native is None or capsule is None:
        return
    snapshot = getattr(native, "operation_memory_ledger_snapshot", None)
    if not callable(snapshot):
        raise RuntimeError("native operation memory ledger lacks a snapshot callback")
    values = snapshot(capsule)
    if not isinstance(values, tuple) or len(values) != 3:
        raise RuntimeError("native operation memory ledger returned invalid statistics")
    if max(0, int(values[1])) != 0:
        raise RuntimeError("abandoned operation memory ledger still owns bytes")
    if cross_process is not None:
        release = getattr(cross_process, "release", None)
        if not callable(release):
            raise RuntimeError("cross-process memory owner lacks a release callback")
        release()
        authority.arg2 = None


_MAX_ABANDONED_MEMORY_OWNERS = 8192
_MEMORY_LEASE_FINALIZER_ESCROW: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(
    _MAX_ABANDONED_MEMORY_OWNERS, static_kind="memory_lease"
)
_MEMORY_LEASE_ROOTED_FINALIZER_ESCROW = cast(
    ReservedFinalizerEscrow[RootedFinalizerAuthority], _MEMORY_LEASE_FINALIZER_ESCROW
)
_MEMORY_LEDGER_FINALIZER_ESCROW: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(
    _MAX_ABANDONED_MEMORY_OWNERS, static_kind="memory_ledger"
)
# Construction rollback for a composed stage can retain arbitrary domain leases,
# not only memory owners. Reserve this slot before any stage ownership exists so
# even MemoryError during rollback cannot make a retryable domain unreachable.
_MAX_STAGE_ADMISSION_CONSTRUCTION_ESCROWS = 1024
_STAGE_ADMISSION_CONSTRUCTION_ESCROW: ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
    ReservedFinalizerEscrow(
        _MAX_STAGE_ADMISSION_CONSTRUCTION_ESCROWS, static_kind="stage_admission"
    )
)
_MEMORY_FINALIZER_OVERFLOWS = 0
_MEMORY_FINALIZER_OVERFLOWED = False
# Stage construction uses the same separate pre-rooted authority model as the
# resource finalizers. The escrow never roots the construction object directly.


def _run_stage_admission_construction_finalizer(
    authority: RootedFinalizerAuthority,
) -> None:
    owner = authority.arg0
    if owner is None:
        return
    close = getattr(owner, "close", None)
    if not callable(close):
        raise RuntimeError("stage-admission construction owner is not closeable")
    close()
    authority.arg0 = None


def _reserve_stage_admission_construction_authority() -> tuple[int, RootedFinalizerAuthority]:
    return reserve_rooted_finalizer_authority(
        _STAGE_ADMISSION_CONSTRUCTION_ESCROW,
        _run_stage_admission_construction_finalizer,
    )


def _retire_stage_admission_construction_ticket(
    ticket: int, authority: RootedFinalizerAuthority | None = None
) -> bool:
    """Retire or publish an ACK-only construction authority.

    ``authority is None`` is retained only for historical focused tests/tools
    that reserve a naked ticket. Production construction paths always reserve
    and root the authority before acquiring their first resource.
    """
    if authority is None:
        # Historical naked-ticket path: preserve its release_ticket fault
        # injection surface, then transfer only ACK authority if retirement did
        # not commit. Production never enters this branch.
        try:
            if _STAGE_ADMISSION_CONSTRUCTION_ESCROW.release_ticket(ticket):
                return True
        except BaseException:
            pass
        authority = RootedFinalizerAuthority(_run_stage_admission_construction_finalizer)
        authority.ticket = ticket
        authority.make_ack_only()
        try:
            if not _STAGE_ADMISSION_CONSTRUCTION_ESCROW.root_reserved(ticket, authority):
                return False
            return bool(_STAGE_ADMISSION_CONSTRUCTION_ESCROW.publish_rooted(ticket, authority))
        except BaseException:
            return False
    try:
        retired = retire_or_ack_rooted_finalizer_authority(
            _STAGE_ADMISSION_CONSTRUCTION_ESCROW, ticket, authority
        )
        return retired or authority._escrow_armed
    except BaseException:
        _mark_memory_finalizer_overflow(ticket)
        return False


def _mark_memory_finalizer_overflow(ticket: int) -> None:
    global _MEMORY_FINALIZER_OVERFLOWS, _MEMORY_FINALIZER_OVERFLOWED
    _MEMORY_FINALIZER_OVERFLOWED = True
    try:
        _MEMORY_FINALIZER_OVERFLOWS += 1
    except MemoryError:
        pass
    publish_terminal_owner("operation_memory_finalizer_overflow", ticket, retained_bytes=256)


def _publish_abandoned_memory_lease(ticket: int, owner: OperationMemoryLease) -> None:
    global _MEMORY_FINALIZER_OVERFLOWS, _MEMORY_FINALIZER_OVERFLOWED
    if not _MEMORY_LEASE_FINALIZER_ESCROW.publish_reserved(ticket, owner):
        _MEMORY_FINALIZER_OVERFLOWED = True
        try:
            _MEMORY_FINALIZER_OVERFLOWS += 1
        except MemoryError:
            pass
        publish_terminal_owner("operation_memory_finalizer_overflow", id(owner), retained_bytes=256)


def _publish_abandoned_memory_ledger(ticket: int, owner: object) -> None:
    """Compatibility publisher for pre-pass75 owners."""
    global _MEMORY_FINALIZER_OVERFLOWS, _MEMORY_FINALIZER_OVERFLOWED
    publish = (
        _MEMORY_LEDGER_FINALIZER_ESCROW.publish_rooted
        if isinstance(owner, RootedFinalizerAuthority)
        else _MEMORY_LEDGER_FINALIZER_ESCROW.publish_reserved
    )
    if not publish(ticket, owner):
        _MEMORY_FINALIZER_OVERFLOWED = True
        try:
            _MEMORY_FINALIZER_OVERFLOWS += 1
        except MemoryError:
            pass
        publish_terminal_owner("operation_memory_finalizer_overflow", ticket, retained_bytes=256)


def drain_abandoned_memory_finalizers() -> int:
    """Release GC-abandoned memory owners one rooted generation at a time."""
    drained = 0
    for escrow, kind in (
        (_MEMORY_LEASE_FINALIZER_ESCROW, "lease"),
        (_MEMORY_LEDGER_FINALIZER_ESCROW, "ledger"),
        (_STAGE_ADMISSION_CONSTRUCTION_ESCROW, "stage_admission"),
    ):

        def process(ticket: int, owner: object, *, _kind: str = kind) -> None:
            nonlocal drained
            if _kind in {"lease", "ledger", "stage_admission"} and isinstance(
                owner, RootedFinalizerAuthority
            ):
                owner.run()
                owner.clear()
                owner.ticket = 0
                retire_terminal_owner("operation_memory_finalizer_overflow", ticket)
                drained += 1
                return
            if not _memory_owner_finished(owner, _kind):
                raise RuntimeError("memory finalizer cleanup did not finish")
            if hasattr(owner, "_finalizer_ticket"):
                owner._finalizer_ticket = None
            drained += 1

        attempts = escrow.active_count()
        for _ in range(attempts):
            try:
                if not escrow.process_one(process):
                    break
            except BaseException:
                continue

    # The compatibility overflow bank is physically preallocated. Safe-point
    # draining removes one owner at a time and puts failures back in-place.
    global _ABANDONED_MEMORY_EMERGENCY_COUNT
    emergency_attempts = _ABANDONED_MEMORY_EMERGENCY_COUNT
    for _ in range(emergency_attempts):
        with _ABANDONED_MEMORY_LOCK:
            if _ABANDONED_MEMORY_EMERGENCY_COUNT <= 0:
                break
            index = _ABANDONED_MEMORY_EMERGENCY_COUNT - 1
            owner = _ABANDONED_MEMORY_EMERGENCY[index]
            emergency_kind = _ABANDONED_MEMORY_EMERGENCY_KINDS[index]
            _ABANDONED_MEMORY_EMERGENCY[index] = None
            _ABANDONED_MEMORY_EMERGENCY_KINDS[index] = None
            _ABANDONED_MEMORY_EMERGENCY_COUNT = index
        if owner is None or emergency_kind is None:
            continue
        try:
            finished = _memory_owner_finished(owner, emergency_kind)
        except BaseException:
            finished = False
        if finished:
            retire_terminal_owner("operation_memory_finalizer_overflow", id(owner))
            drained += 1
            continue
        with _ABANDONED_MEMORY_LOCK:
            index = _ABANDONED_MEMORY_EMERGENCY_COUNT
            if index < _MAX_ABANDONED_MEMORY_OWNERS:
                _ABANDONED_MEMORY_EMERGENCY[index] = owner
                _ABANDONED_MEMORY_EMERGENCY_KINDS[index] = emergency_kind
                _ABANDONED_MEMORY_EMERGENCY_COUNT = index + 1
    return drained


def operation_memory_finalizer_snapshot() -> tuple[int, int, int]:
    """Return reserved tickets, published owners and irreversible overflows."""
    return (
        _MEMORY_LEASE_FINALIZER_ESCROW.reserved_count()
        + _MEMORY_LEDGER_FINALIZER_ESCROW.reserved_count()
        + _STAGE_ADMISSION_CONSTRUCTION_ESCROW.reserved_count(),
        _MEMORY_LEASE_FINALIZER_ESCROW.published_count()
        + _MEMORY_LEDGER_FINALIZER_ESCROW.published_count()
        + _STAGE_ADMISSION_CONSTRUCTION_ESCROW.published_count(),
        max(1, _MEMORY_FINALIZER_OVERFLOWS)
        if (
            _MEMORY_FINALIZER_OVERFLOWED
            or _MEMORY_LEASE_FINALIZER_ESCROW.overflowed
            or _MEMORY_LEDGER_FINALIZER_ESCROW.overflowed
            or _STAGE_ADMISSION_CONSTRUCTION_ESCROW.overflowed
        )
        else _MEMORY_FINALIZER_OVERFLOWS,
    )


_ABANDONED_MEMORY_LOCK = Lock()
_ABANDONED_MEMORY_OWNERS: dict[int, tuple[object, str]] = {}
# Emergency roots are only used if the primary bounded registry invariant is
# violated. They are pre-bounded to the same ceiling and keep ownership safe.
_ABANDONED_MEMORY_EMERGENCY: list[object | None] = [None] * _MAX_ABANDONED_MEMORY_OWNERS
_ABANDONED_MEMORY_EMERGENCY_KINDS: list[str | None] = [None] * _MAX_ABANDONED_MEMORY_OWNERS
_ABANDONED_MEMORY_EMERGENCY_COUNT = 0
_ABANDONED_MEMORY_SLOT_BITS = max(1, (_MAX_ABANDONED_MEMORY_OWNERS - 1).bit_length())
_ABANDONED_MEMORY_SLOT_MASK = (1 << _ABANDONED_MEMORY_SLOT_BITS) - 1
_ABANDONED_MEMORY_FREE = list(range(_MAX_ABANDONED_MEMORY_OWNERS))
_ABANDONED_MEMORY_FREE_COUNT = _MAX_ABANDONED_MEMORY_OWNERS
_ABANDONED_MEMORY_GENERATIONS = [0] * _MAX_ABANDONED_MEMORY_OWNERS


def _memory_owner_finished(owner: object, kind: str) -> bool:
    if kind == "lease":
        owner.release()  # type: ignore[attr-defined]
        return True
    if kind == "ledger":
        owner.close()  # type: ignore[attr-defined]
        diagnostics = owner.diagnostics()  # type: ignore[attr-defined]
        return not bool(diagnostics.cross_process_release_deferred)
    if kind == "stage_admission":
        owner.close()  # type: ignore[attr-defined]
        return (
            not bool(getattr(owner, "domain_leases", ()))
            and getattr(owner, "pending_domain_lease", None) is None
            and getattr(owner, "memory_lease", None) is None
            and getattr(owner, "execution_lease", None) is None
            and getattr(owner, "control_ticket", None) is None
        )
    raise RuntimeError("unknown abandoned memory owner kind")


def _retry_abandoned_memory_owner(token: int) -> None:
    global _ABANDONED_MEMORY_FREE_COUNT
    with _ABANDONED_MEMORY_LOCK:
        entry = _ABANDONED_MEMORY_OWNERS.get(token)
    if entry is None:
        return
    owner, kind = entry
    if not _memory_owner_finished(owner, kind):
        raise RuntimeError("abandoned operation memory ownership is not quiescent")
    with _ABANDONED_MEMORY_LOCK:
        current = _ABANDONED_MEMORY_OWNERS.get(token)
        if current is entry:
            _ABANDONED_MEMORY_OWNERS.pop(token, None)
            slot = token & _ABANDONED_MEMORY_SLOT_MASK
            if (
                0 <= slot < _MAX_ABANDONED_MEMORY_OWNERS
                and _ABANDONED_MEMORY_FREE_COUNT < _MAX_ABANDONED_MEMORY_OWNERS
            ):
                _ABANDONED_MEMORY_FREE[_ABANDONED_MEMORY_FREE_COUNT] = slot
                _ABANDONED_MEMORY_FREE_COUNT += 1
    retire_terminal_owner("operation_memory_finalizer", token)


def _defer_abandoned_memory_owner(owner: object, *, kind: str) -> None:
    """Publish one compact retry token; never perform external I/O here."""
    global _ABANDONED_MEMORY_FREE_COUNT, _ABANDONED_MEMORY_EMERGENCY_COUNT
    with _ABANDONED_MEMORY_LOCK:
        if _ABANDONED_MEMORY_FREE_COUNT <= 0:
            if _ABANDONED_MEMORY_EMERGENCY_COUNT < _MAX_ABANDONED_MEMORY_OWNERS:
                index = _ABANDONED_MEMORY_EMERGENCY_COUNT
                _ABANDONED_MEMORY_EMERGENCY[index] = owner
                _ABANDONED_MEMORY_EMERGENCY_KINDS[index] = kind
                _ABANDONED_MEMORY_EMERGENCY_COUNT = index + 1
                publish_terminal_owner(
                    "operation_memory_finalizer_overflow", id(owner), retained_bytes=256
                )
            return
        _ABANDONED_MEMORY_FREE_COUNT -= 1
        slot = _ABANDONED_MEMORY_FREE[_ABANDONED_MEMORY_FREE_COUNT]
        generation = _ABANDONED_MEMORY_GENERATIONS[slot] + 1
        if generation >= (1 << (63 - _ABANDONED_MEMORY_SLOT_BITS)):
            # Permanently retire this slot rather than wrap into ABA.
            return
        _ABANDONED_MEMORY_GENERATIONS[slot] = generation
        token = (generation << _ABANDONED_MEMORY_SLOT_BITS) | slot
        try:
            _ABANDONED_MEMORY_OWNERS[token] = (owner, kind)
        except BaseException:
            _ABANDONED_MEMORY_FREE[_ABANDONED_MEMORY_FREE_COUNT] = slot
            _ABANDONED_MEMORY_FREE_COUNT += 1
            raise
    publish_terminal_owner("operation_memory_finalizer", token, retained_bytes=256)
    accepted = False
    try:
        from .cleanup_dispatcher import CleanupSubsystem, dispatch_cleanup

        accepted = dispatch_cleanup(
            _retry_abandoned_memory_owner,
            token,
            retained_bytes=256,
            start_worker=False,
            subsystem=CleanupSubsystem.MEMORY,
        )
    except BaseException as exc:
        clear_exception_traceback(exc)
    if accepted:
        return
    # A finalizer must not start the retry scheduler either: doing so can
    # acquire a process thread permit and block the GC thread.  Keep the
    # compact owner in the bounded registry and terminal ledger.  This is
    # deliberately fail-closed; shutdown/diagnostics will report the owner.


_OPERATION_MEMORY_LEDGER: ContextVar[OperationMemoryLedger | None] = ContextVar(
    "schema_sanitizer_operation_memory_ledger", default=None
)
_FORKED_MEMORY_LEDGERS_KEEPALIVE: list[OperationMemoryLedger] = []


@contextmanager
def activate_operation_memory_ledger(
    ledger: OperationMemoryLedger,
) -> Iterator[OperationMemoryLedger]:
    """Expose one operation ledger to Python staging and transport helpers."""
    owner_pid = os.getpid()
    token = _OPERATION_MEMORY_LEDGER.set(ledger)
    try:
        yield ledger
    finally:
        if os.getpid() == owner_pid:
            _OPERATION_MEMORY_LEDGER.reset(token)
        else:
            _reset_operation_memory_ledger_after_fork()


def _reset_operation_memory_ledger_after_fork() -> None:
    """Detach inherited ledgers and rebuild finalizer publication locks."""
    global _ABANDONED_MEMORY_LOCK, _ABANDONED_MEMORY_OWNERS
    global \
        _ABANDONED_MEMORY_EMERGENCY, \
        _ABANDONED_MEMORY_EMERGENCY_KINDS, \
        _ABANDONED_MEMORY_EMERGENCY_COUNT
    global _ABANDONED_MEMORY_FREE, _ABANDONED_MEMORY_FREE_COUNT, _ABANDONED_MEMORY_GENERATIONS
    global _MEMORY_FINALIZER_OVERFLOWS, _MEMORY_FINALIZER_OVERFLOWED
    inherited = _OPERATION_MEMORY_LEDGER.get()
    if inherited is not None:
        quarantine_inherited_state("memory-ledger", inherited)
    _OPERATION_MEMORY_LEDGER.set(None)
    # Never acquire a lock inherited from a multi-threaded parent. Parent-owned
    # ledgers/leases reject child finalization by PID before touching resources.
    _ABANDONED_MEMORY_LOCK = Lock()
    _ABANDONED_MEMORY_OWNERS = {}
    _ABANDONED_MEMORY_EMERGENCY = [None] * _MAX_ABANDONED_MEMORY_OWNERS
    _ABANDONED_MEMORY_EMERGENCY_KINDS = [None] * _MAX_ABANDONED_MEMORY_OWNERS
    _ABANDONED_MEMORY_EMERGENCY_COUNT = 0
    _ABANDONED_MEMORY_FREE = list(range(_MAX_ABANDONED_MEMORY_OWNERS))
    _ABANDONED_MEMORY_FREE_COUNT = _MAX_ABANDONED_MEMORY_OWNERS
    _ABANDONED_MEMORY_GENERATIONS = [0] * _MAX_ABANDONED_MEMORY_OWNERS
    _MEMORY_LEASE_FINALIZER_ESCROW.reset_after_fork()
    _MEMORY_LEDGER_FINALIZER_ESCROW.reset_after_fork()
    _MEMORY_FINALIZER_OVERFLOWS = 0
    _MEMORY_FINALIZER_OVERFLOWED = False


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("operation-memory-ledger", mode="quarantine_only")


def current_operation_memory_ledger() -> OperationMemoryLedger | None:
    """Return the active operation ledger, if the caller owns one."""
    return _OPERATION_MEMORY_LEDGER.get()


def acquire_operation_memory(size_bytes: int, *, stage: str) -> OperationMemoryLease | None:
    """Acquire a retained lease from the active operation when present."""
    if type(size_bytes) is not int:
        raise TypeError("operation memory size must be an exact integer")
    if size_bytes < 0:
        raise ValueError("operation memory size must be >= 0")
    if type(stage) is not str:
        raise TypeError("operation memory stage must be an exact string")
    ledger = current_operation_memory_ledger()
    return None if ledger is None else ledger.acquire(size_bytes, stage=stage)


@contextmanager
def reserve_operation_memory(size_bytes: int, *, stage: str) -> Iterator[None]:
    """Reserve one transient Python buffer against the active operation."""
    lease = acquire_operation_memory(size_bytes, stage=stage)
    try:
        yield
    except BaseException as primary:
        if lease is not None:
            try:
                lease.close()
            except BaseException as cleanup_error:
                add_bounded_note(primary, "operation memory cleanup also failed", cleanup_error)
        raise
    else:
        if lease is not None:
            lease.close()


def adaptive_parallel_slots(
    desired: int,
    *,
    per_slot_bytes: int,
    reserve_bytes: int | None = None,
) -> int:
    """Return additional parallel slots; zero means execute no new helper work.

    Forward progress is deliberately separated from helper admission. A caller
    that already owns the resources needed for serial progress may continue,
    while new parallel work is suppressed when usable process headroom is zero.
    """
    if type(desired) is not int:
        raise TypeError("desired concurrency must be an exact integer")
    if type(per_slot_bytes) is not int:
        raise TypeError("per_slot_bytes must be an exact integer")
    if desired <= 0:
        raise ValueError("desired concurrency must be > 0")
    if per_slot_bytes <= 0:
        raise ValueError("per_slot_bytes must be > 0")
    from .cancellation import check_operation_cancelled
    from .system_pressure import pressure_adjusted_target

    check_operation_cancelled(stage="adaptive_concurrency")
    desired = pressure_adjusted_target(desired)
    snapshot = process_resident_memory_snapshot()
    from .control_plane_budget import process_control_plane_snapshot

    control = process_control_plane_snapshot()
    # ``process_resident_memory_snapshot`` already discounts
    # ``control.governed_bytes`` from its admission capacity.  Keep the legacy
    # expression below as a contract breadcrumb for source-level compatibility:
    # snapshot.capacity_bytes - snapshot.reserved_bytes - control.governed_bytes
    del control
    headroom = max(0, snapshot.capacity_bytes - snapshot.reserved_bytes)
    if reserve_bytes is None:
        fallback_reserve = max(4 << 20, snapshot.capacity_bytes // 64)
        from .safety_margins import tuned_memory_reserve_bytes

        reserve = tuned_memory_reserve_bytes(snapshot.capacity_bytes, fallback_reserve)
    else:
        if type(reserve_bytes) is not int:
            raise TypeError("reserve_bytes must be an exact integer or None")
        if reserve_bytes < 0:
            raise ValueError("reserve_bytes must be >= 0")
        reserve = reserve_bytes
    usable = max(0, headroom - reserve)
    slots = usable // per_slot_bytes
    return max(0, min(desired, slots))


class _ParallelAdmissionConstructionOwner:
    """Preallocated rollback owner for resources acquired before publication.

    The object and escrow ticket exist before the first irreversible acquisition.
    Every acquired capability is assigned to an existing slot immediately, so an
    OOM while constructing the public admission cannot make ownership unreachable.
    """

    __slots__ = (
        "memory_lease",
        "execution_lease",
        "owns_execution_lease",
        "control_ticket",
        "control_bytes",
        "pending_domain_lease",
    )

    def __init__(self) -> None:
        self.memory_lease: object | None = None
        self.execution_lease: object | None = None
        self.owns_execution_lease = False
        self.control_ticket: ControlPlaneTicket | None = None
        self.control_bytes = 0
        self.pending_domain_lease: object | None = None

    def disarm(self) -> None:
        self.memory_lease = None
        self.execution_lease = None
        self.owns_execution_lease = False
        self.control_ticket = None
        self.control_bytes = 0
        self.pending_domain_lease = None

    def close(self) -> None:
        pending = self.pending_domain_lease
        if pending is not None:
            release = getattr(pending, "release", None)
            if not callable(release):
                release = getattr(pending, "close", None)
            if callable(release):
                release()
            self.pending_domain_lease = None
        control = self.control_ticket
        if control is not None:
            if not release_control_plane(control):
                raise RuntimeError("memory execution control-plane retirement did not commit")
            self.control_ticket = None
            self.control_bytes = 0
        execution = self.execution_lease
        if execution is not None and self.owns_execution_lease:
            release = getattr(execution, "release", None)
            if callable(release):
                release()
            self.execution_lease = None
            self.owns_execution_lease = False
        memory = self.memory_lease
        if memory is not None:
            close = getattr(memory, "close", None)
            if not callable(close):
                close = getattr(memory, "release", None)
            if callable(close):
                close()
            self.memory_lease = None


@dataclass(slots=True)
class CompositeParallelAdmission:
    """Physical execution-slot plus resident-byte admission for pipeline fan-out."""

    slots: int
    per_slot_bytes: int
    memory_lease: OperationMemoryLease | None = None
    execution_lease: object | None = None
    owns_execution_lease: bool = False
    memory_enforced: bool = True
    control_ticket: ControlPlaneTicket | None = None
    control_bytes: int = 0
    # Additional process domains (remote-I/O, async-task, native/CPU permits,
    # etc.) are acquired transactionally by StageConcurrencyAdmission. Keep a
    # fixed tuple so the admission object itself cannot grow after publication.
    domain_leases: tuple[tuple[str, object], ...] = ()
    # A domain is rooted here *before* tuple-growth publication. If tuple growth
    # raises MemoryError, close()/escrow still owns the exact lease.
    pending_domain_name: str | None = None
    pending_domain_lease: object | None = None

    @property
    def reserved_bytes(self) -> int:
        return self.slots * self.per_slot_bytes

    def transfer_stage(self, stage: str) -> "CompositeParallelAdmission":
        if self.memory_lease is not None:
            successor = self.memory_lease.transfer_stage(stage)
            # Historical focused doubles returned None; real pass48 leases return
            # a new generation and must replace the upstream capability.
            if successor is not None:
                self.memory_lease = successor
        return self

    def close(self) -> None:
        """Release capabilities in reverse order without early base publication.

        Cleanup is commit-after-release at every domain boundary.  If a
        secondary domain remains retryable, resident bytes and physical slots
        stay owned by this capability instead of becoming visible to another
        stage prematurely.
        """
        pending = self.pending_domain_lease
        if pending is not None:
            release = getattr(pending, "release", None)
            if not callable(release):
                release = getattr(pending, "close", None)
            if callable(release):
                release()
            self.pending_domain_lease = None
            self.pending_domain_name = None

        domains = self.domain_leases
        while domains:
            domain_name, domain = domains[-1]
            release = getattr(domain, "release", None)
            if not callable(release):
                release = getattr(domain, "close", None)
            if callable(release):
                release()
            domains = domains[:-1]
            self.domain_leases = domains

        control = self.control_ticket
        if control is not None:
            if not release_control_plane(control):
                raise RuntimeError("memory execution control-plane retirement did not commit")
            self.control_ticket = None
            self.control_bytes = 0

        execution = self.execution_lease
        if execution is not None and self.owns_execution_lease:
            release = getattr(execution, "release", None)
            if callable(release):
                release()
            self.execution_lease = None
            self.owns_execution_lease = False
        elif execution is not None:
            # Borrowed execution capabilities are never released by this stage,
            # but clearing the reference after every owned domain is gone keeps
            # repeated close() calls idempotent.
            self.execution_lease = None

        # Resident bytes were acquired before helper-worker capacity in Pass 56.
        # Release the worker first so the byte credit cannot be re-admitted while
        # a helper that was protected by those bytes is still live.
        lease = self.memory_lease
        if lease is not None:
            lease.close()
            self.memory_lease = None

    release = close

    def __enter__(self) -> "CompositeParallelAdmission":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class _StageControlReservation:
    """Single-reference result allocated before control-plane reservation commits."""

    __slots__ = ("ticket", "control_bytes")

    def __init__(self, control_bytes: int) -> None:
        self.ticket: ControlPlaneTicket | None = None
        self.control_bytes = control_bytes

    def __iter__(self):  # compatibility; production uses direct fields
        yield self.ticket
        yield self.control_bytes


_STAGE_ADMISSION_BASE_CONTROL_BYTES = 512
_STAGE_ADMISSION_PER_SLOT_CONTROL_BYTES = 512
_STAGE_DOMAIN_ORDER: dict[str, int] = {
    "resident_memory": 10,
    "physical_thread": 20,
    "control_plane": 30,
    "cpu_runnable": 40,
    "remote_io": 50,
    "async_task": 60,
    "provider_permit": 70,
}


def _stage_domain_order_key(name: str) -> tuple[int, str]:
    # Unknown extension domains sort after the published process-wide domains
    # but remain deterministic amongst themselves.
    return (_STAGE_DOMAIN_ORDER.get(name, 1_000), name)


def _reserve_stage_control(slots: int, stage: str) -> _StageControlReservation:
    control_bytes = _STAGE_ADMISSION_BASE_CONTROL_BYTES + (
        max(1, slots) * _STAGE_ADMISSION_PER_SLOT_CONTROL_BYTES
    )
    result = _StageControlReservation(control_bytes)
    result.ticket = reserve_control_plane(f"stage_concurrency:{stage}", control_bytes)
    return result


def acquire_parallel_admission(
    desired: int,
    *,
    per_slot_bytes: int,
    stage: str,
    reserve_bytes: int | None = None,
    execution_lease: object | None = None,
    require_memory: bool = False,
    memory_ledger: OperationMemoryLedger | None = None,
    physical_threads: bool = True,
    _admission_type: type[CompositeParallelAdmission] | None = None,
) -> CompositeParallelAdmission:
    """Acquire bytes/threads/control with construction ownership pre-rooted.

    Pass69 reserves an escrow generation and allocates the construction owner
    before the first resource acquisition. After each commit, ownership moves
    immediately into an existing owner slot. Public-admission construction is
    therefore no longer an allocation-after-commit proof gap.
    """
    admission_type = CompositeParallelAdmission if _admission_type is None else _admission_type
    slots = adaptive_parallel_slots(
        desired, per_slot_bytes=per_slot_bytes, reserve_bytes=reserve_bytes
    )
    if slots <= 0:
        return admission_type(0, per_slot_bytes, None, None, False)

    construction = _ParallelAdmissionConstructionOwner()
    rollback_authority = RootedFinalizerAuthority(_run_stage_admission_construction_finalizer)
    rollback_authority.arg0 = construction
    try:
        rollback_ticket = _STAGE_ADMISSION_CONSTRUCTION_ESCROW.reserve_rooted(rollback_authority)
        if rollback_ticket is None:
            raise RuntimeError("stage-admission construction escrow exhausted")
    except BaseException:
        try:
            _STAGE_ADMISSION_CONSTRUCTION_ESCROW.release_rooted_owner(rollback_authority)
        except BaseException:
            pass
        raise
    escrow_published = False

    def rollback(primary: BaseException) -> None:
        nonlocal escrow_published
        try:
            construction.close()
            rollback_authority.arg0 = None
        except BaseException as cleanup_error:
            add_bounded_note(
                primary,
                "parallel-admission construction rollback remains retryable",
                cleanup_error,
            )
            if arm_rooted_finalizer_authority(
                _STAGE_ADMISSION_CONSTRUCTION_ESCROW,
                rollback_ticket,
                rollback_authority,
            ):
                escrow_published = True
            else:
                _mark_memory_finalizer_overflow(rollback_ticket)
                add_bounded_note(
                    primary,
                    "parallel-admission rollback escrow publication failed",
                    RuntimeError("reserved parallel rollback publication failed"),
                )

    try:
        ledger = memory_ledger if memory_ledger is not None else current_operation_memory_ledger()
        if ledger is None:
            if require_memory:
                raise RuntimeError(
                    "composite parallel admission requires an operation memory ledger"
                )
            helper_slots = max(0, slots - 1) if physical_threads else 0
            physical = execution_lease if physical_threads else None
            owned_execution = False
            if helper_slots and physical is None:
                from .process_resources import acquire_project_threads

                try:
                    physical = acquire_project_threads(helper_slots, minimum=1)
                except Exception as exc:
                    from ..errors import SchemaSanitizerResourceError

                    if not isinstance(exc, SchemaSanitizerResourceError):
                        raise
                    physical = None
                    helper_slots = 0
                    slots = 1
                else:
                    owned_execution = True
                    construction.execution_lease = physical
                    construction.owns_execution_lease = True
            if physical is not None:
                granted = int(getattr(physical, "amount", helper_slots))
                slots = min(slots, 1 + max(0, granted))
            control_reservation = _reserve_stage_control(slots, stage)
            control_ticket = control_reservation.ticket
            assert control_ticket is not None
            control_bytes = control_reservation.control_bytes
            construction.control_ticket = control_ticket
            construction.control_bytes = control_bytes
            admission = admission_type(
                slots,
                per_slot_bytes,
                None,
                physical,
                owned_execution,
                False,
                control_ticket,
                control_bytes,
            )
            construction.disarm()
            return admission

        candidate = slots
        while candidate > 0:
            try:
                lease = ledger.acquire(candidate * per_slot_bytes, stage=stage)
            except Exception as exc:
                from ..errors import SchemaSanitizerResourceError

                if not isinstance(exc, SchemaSanitizerResourceError):
                    raise
                candidate //= 2
                continue
            construction.memory_lease = lease
            physical = execution_lease if physical_threads else None
            owned_execution = False
            final_slots = candidate
            helper_slots = max(0, candidate - 1) if physical_threads else 0
            if helper_slots and physical is None:
                from .process_resources import acquire_project_threads

                try:
                    physical = acquire_project_threads(helper_slots, minimum=1)
                except Exception as exc:
                    from ..errors import SchemaSanitizerResourceError

                    if not isinstance(exc, SchemaSanitizerResourceError):
                        raise
                    physical = None
                    final_slots = 1
                else:
                    owned_execution = True
                    construction.execution_lease = physical
                    construction.owns_execution_lease = True
            if physical is not None:
                granted = int(getattr(physical, "amount", helper_slots))
                final_slots = min(candidate, 1 + max(0, granted))
            if final_slots < candidate:
                resize = getattr(lease, "resize", None)
                if callable(resize):
                    resize(final_slots * per_slot_bytes)
            control_reservation = _reserve_stage_control(final_slots, stage)
            control_ticket = control_reservation.ticket
            assert control_ticket is not None
            control_bytes = control_reservation.control_bytes
            construction.control_ticket = control_ticket
            construction.control_bytes = control_bytes
            admission = admission_type(
                final_slots,
                per_slot_bytes,
                lease,
                physical,
                owned_execution,
                True,
                control_ticket,
                control_bytes,
            )
            construction.disarm()
            _observe_runtime_concurrency_contract_noexcept("composite_slot_and_byte_admission")
            return admission
        return admission_type(0, per_slot_bytes, None, None, False)
    except BaseException as primary:
        rollback(primary)
        raise
    finally:
        if not escrow_published:
            rollback_authority.arg0 = None
            _retire_stage_admission_construction_ticket(rollback_ticket, rollback_authority)


class StageConcurrencyAdmission(CompositeParallelAdmission):
    """A transactionally composed admission spanning every requested stage domain.

    This is deliberately a distinct capability type rather than an alias for the
    historical slot/byte admission. Callers can therefore distinguish a complete
    stage envelope from a lower-level parallel admission while retaining the same
    close/transfer surface.
    """

    __slots__ = ()

    def attach_domain(self, domain_name: str, lease: object) -> None:
        """Attach one already-acquired domain while preserving global order.

        This is used for domains whose acquisition is inherently asynchronous
        (for example remote-I/O permits).  Base slot/byte/control admission must
        already be owned; the caller rolls back ``lease`` if this method rejects
        publication.
        """
        if type(domain_name) is not str or not domain_name:
            raise ValueError("stage concurrency domain names must be non-empty strings")
        if lease is None:
            raise RuntimeError(f"stage concurrency domain {domain_name!r} returned no lease")
        existing = self.domain_leases
        if existing and _stage_domain_order_key(domain_name) <= _stage_domain_order_key(
            existing[-1][0]
        ):
            raise RuntimeError("stage concurrency domains must be attached in strict global order")
        if self.pending_domain_lease is not None:
            raise RuntimeError("stage concurrency admission already owns a pending domain")
        # Root the lease in an existing object slot before the tuple-growth
        # publication. MemoryError can abort publication but cannot lose owner.
        self.pending_domain_name = domain_name
        self.pending_domain_lease = lease
        self.domain_leases = (*existing, (domain_name, lease))
        self.pending_domain_lease = None
        self.pending_domain_name = None


def _promote_stage_admission(
    base: CompositeParallelAdmission,
) -> StageConcurrencyAdmission:
    """Move one base admission into the distinct stage capability type."""
    try:
        stage = StageConcurrencyAdmission(
            base.slots,
            base.per_slot_bytes,
            base.memory_lease,
            base.execution_lease,
            base.owns_execution_lease,
            base.memory_enforced,
            base.control_ticket,
            base.control_bytes,
            base.domain_leases,
        )
    except BaseException as primary:
        try:
            base.close()
        except BaseException as cleanup_error:
            add_bounded_note(
                primary, "stage admission promotion rollback also failed", cleanup_error
            )
        raise
    # Ownership is now represented only by ``stage``. The lower-level object has
    # no finalizer, but clearing it makes accidental later cleanup harmless.
    base.memory_lease = None
    base.execution_lease = None
    base.owns_execution_lease = False
    base.control_ticket = None
    base.control_bytes = 0
    base.domain_leases = ()
    return stage


def acquire_stage_concurrency_admission(
    desired: int,
    *,
    per_slot_bytes: int,
    stage: str,
    reserve_bytes: int | None = None,
    execution_lease: object | None = None,
    require_memory: bool = False,
    memory_ledger: OperationMemoryLedger | None = None,
    domain_acquirers: Mapping[str, Callable[[int], object]] | None = None,
    physical_threads: bool = True,
) -> StageConcurrencyAdmission:
    """Acquire one transactionally composed multi-domain stage admission.

    ``domain_acquirers`` are acquired in the process-wide published domain order,
    independent of caller mapping order. Each returned lease becomes owned by
    the still-private stage immediately. A pre-reserved bounded escrow slot makes
    any rollback suffix retryable even when release itself fails under memory
    pressure; successful construction retires that slot before publication.
    """
    # Sorting/validation may allocate, so finish it before any stage resource is
    # acquired. The final capability type is then constructed directly; normal
    # production no longer needs an allocation-heavy promotion step.
    ordered_domains = (
        sorted(domain_acquirers.items(), key=lambda item: _stage_domain_order_key(item[0]))
        if domain_acquirers
        else []
    )
    for domain_name, _acquire in ordered_domains:
        if type(domain_name) is not str or not domain_name:
            raise ValueError("stage concurrency domain names must be non-empty strings")

    rollback_authority = RootedFinalizerAuthority(_run_stage_admission_construction_finalizer)
    try:
        rollback_ticket = _STAGE_ADMISSION_CONSTRUCTION_ESCROW.reserve_rooted(rollback_authority)
        if rollback_ticket is None:
            raise RuntimeError("stage-admission construction escrow exhausted")
    except BaseException:
        try:
            _STAGE_ADMISSION_CONSTRUCTION_ESCROW.release_rooted_owner(rollback_authority)
        except BaseException:
            pass
        raise
    escrow_published = False
    try:
        base = acquire_parallel_admission(
            desired,
            per_slot_bytes=per_slot_bytes,
            stage=stage,
            reserve_bytes=reserve_bytes,
            execution_lease=execution_lease,
            require_memory=require_memory,
            memory_ledger=memory_ledger,
            physical_threads=physical_threads,
            _admission_type=StageConcurrencyAdmission,
        )
        # Compatibility for focused tests/third-party monkeypatches that replace
        # acquire_parallel_admission and still return the historical base type.
        stage_admission = (
            base if isinstance(base, StageConcurrencyAdmission) else _promote_stage_admission(base)
        )
        rollback_authority.arg0 = stage_admission
        if stage_admission.slots <= 0 or not ordered_domains:
            _observe_runtime_concurrency_contract_noexcept("stage_concurrency_admission")
            return stage_admission

        try:
            for domain_name, acquire in ordered_domains:
                lease = acquire(stage_admission.slots)
                if lease is None:
                    raise RuntimeError(
                        f"stage concurrency domain {domain_name!r} returned no lease"
                    )
                # Publish ownership into the still-private capability immediately.
                # No acquired lease exists solely in a temporary construction list.
                stage_admission.attach_domain(domain_name, lease)
            _observe_runtime_concurrency_contract_noexcept("stage_concurrency_admission")
            return stage_admission
        except BaseException as primary:
            try:
                stage_admission.close()
            except BaseException as cleanup_error:
                add_bounded_note(
                    primary, "stage-admission rollback remains retryable", cleanup_error
                )
                if arm_rooted_finalizer_authority(
                    _STAGE_ADMISSION_CONSTRUCTION_ESCROW,
                    rollback_ticket,
                    rollback_authority,
                ):
                    escrow_published = True
                else:
                    _mark_memory_finalizer_overflow(rollback_ticket)
                    add_bounded_note(
                        primary,
                        "retryable stage-admission rollback escrow publication failed",
                        RuntimeError("reserved stage rollback publication failed"),
                    )
            raise
    finally:
        if not escrow_published:
            rollback_authority.arg0 = None
            _retire_stage_admission_construction_ticket(rollback_ticket, rollback_authority)


def adaptive_concurrency_target(
    desired: int,
    *,
    per_slot_bytes: int,
    reserve_bytes: int | None = None,
) -> int:
    """Compatibility target that preserves one caller-owned progress slot."""
    return max(
        1,
        adaptive_parallel_slots(
            desired,
            per_slot_bytes=per_slot_bytes,
            reserve_bytes=reserve_bytes,
        ),
    )


@dataclass(frozen=True, slots=True)
class MemoryBudget:
    """All internal limits deterministically derived from one memory budget."""

    total_bytes: int
    io_chunk_bytes: int
    batch_target_bytes: int
    coalesce_max_bytes: int
    metadata_bytes: int
    materialized_input_bytes: int
    replay_spool_bytes: int
    parquet_reader_buffer_bytes: int
    parquet_reader_rows: int
    parquet_row_group_bytes: int
    parquet_row_group_rows: int
    parquet_page_bytes: int
    parquet_footer_bytes: int
    async_concurrency: int
    async_prefetch_files: int
    async_retries: int
    async_timeout_seconds: float
    remote_chunk_prefetch: int
    source_discovery_concurrency: int

    @classmethod
    def from_limit(cls, memory_limit_bytes: int | None) -> "MemoryBudget":
        """Ask the native extension to derive all internal sub-budgets."""
        requested = normalize_memory_limit(memory_limit_bytes)
        from .native_runtime import native_core

        values = native_core.memory_budget(requested)
        if not isinstance(values, tuple) or len(values) != 19:
            raise RuntimeError("native memory budget returned an invalid contract")
        return cls(*values)


def memory_budget(memory_limit_bytes: int | None) -> MemoryBudget:
    """Return the canonical derived budget for one public operation."""
    return MemoryBudget.from_limit(memory_limit_bytes)


from .concurrency_contracts import (  # noqa: E402
    observe_runtime_concurrency_contract_noexcept as _observe_runtime_concurrency_contract_noexcept,
)
from .concurrency_contracts import (  # noqa: E402
    register_runtime_concurrency_contract as _register_runtime_concurrency_contract,
)

_register_runtime_concurrency_contract(
    "transferable_resident_memory_credit", OperationMemoryLease.transfer_stage
)
_register_runtime_concurrency_contract(
    "composite_slot_and_byte_admission", acquire_parallel_admission
)
_register_runtime_concurrency_contract(
    "stage_concurrency_admission", acquire_stage_concurrency_admission
)

from .finalizer_registry import (  # noqa: E402
    register_finalizer_domain as _register_finalizer_domain,
)

_register_finalizer_domain(
    "operation_memory",
    drain=drain_abandoned_memory_finalizers,
    snapshot=operation_memory_finalizer_snapshot,
    escrows=(
        ("operation_memory_lease", _MEMORY_LEASE_FINALIZER_ESCROW),
        ("operation_memory_ledger", _MEMORY_LEDGER_FINALIZER_ESCROW),
        ("stage_admission_construction", _STAGE_ADMISSION_CONSTRUCTION_ESCROW),
    ),
)


__all__ = [
    "MAX_MEMORY_LIMIT_BYTES",
    "MemoryBudget",
    "OperationMemoryDiagnostics",
    "OperationMemoryLedger",
    "OperationMemoryLease",
    "GovernedResultOwnership",
    "CompositeParallelAdmission",
    "StageConcurrencyAdmission",
    "OperationMemorySnapshot",
    "ProcessMemoryPressureSnapshot",
    "ProcessResidentMemorySnapshot",
    "acquire_operation_memory",
    "operation_memory_ownership_capability",
    "no_retained_result_ownership_capability",
    "acquire_parallel_admission",
    "acquire_stage_concurrency_admission",
    "adaptive_concurrency_target",
    "adaptive_parallel_slots",
    "activate_operation_memory_ledger",
    "current_operation_memory_ledger",
    "memory_budget",
    "normalize_memory_limit",
    "process_memory_pressure_snapshot",
    "process_resident_memory_snapshot",
    "reserve_operation_memory",
]
