"""Derive every runtime resource budget from one per-operation memory limit."""

from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from threading import Condition, Lock
from time import monotonic
from typing import Any, Iterator

from .finalization import runtime_is_finalizing
from .fork_safety import quarantine_inherited_state
from .safe_errors import add_bounded_note

MAX_MEMORY_LIMIT_BYTES = 64 * 1024 * 1024 * 1024
_CROSS_PROCESS_HEADROOM_BYTES = 8 << 20


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


def process_resident_memory_snapshot() -> ProcessResidentMemorySnapshot:
    """Return aggregate resident-memory accounting across live operations."""
    from .fork_safety import ensure_runtime_fork_safe
    from .native_runtime import native_core

    ensure_runtime_fork_safe()

    values = native_core.process_resident_memory_stats()
    if not isinstance(values, tuple) or len(values) != 3:
        raise RuntimeError("native process resident memory ledger returned invalid statistics")
    return ProcessResidentMemorySnapshot(*(int(value) for value in values))


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
    """Exact accounting plus best-effort process RSS overhead telemetry."""

    capacity_bytes: int
    exact_reserved_bytes: int
    exact_peak_reserved_bytes: int
    exact_headroom_bytes: int
    rss_bytes: int | None
    untracked_rss_bytes: int | None


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
    rss_bytes = _read_process_rss_bytes()
    untracked = None if rss_bytes is None else max(0, rss_bytes - exact.reserved_bytes)
    return ProcessMemoryPressureSnapshot(
        capacity_bytes=exact.capacity_bytes,
        exact_reserved_bytes=exact.reserved_bytes,
        exact_peak_reserved_bytes=exact.peak_reserved_bytes,
        exact_headroom_bytes=max(0, exact.capacity_bytes - exact.reserved_bytes),
        rss_bytes=rss_bytes,
        untracked_rss_bytes=untracked,
    )


class OperationMemoryLease:
    """Exactly-once reservation retained alongside a Python-owned resource."""

    def __init__(self, ledger: "OperationMemoryLedger", size_bytes: int, stage: str) -> None:
        """Reserve bytes and bind them to this thread-safe lease."""
        self._ledger = ledger
        self._size_bytes = 0
        self.stage = stage
        self._pid = os.getpid()
        self._lock = Lock()
        # A failed constructor is still finalized by CPython. Keep it inert
        # until the native reservation succeeds so it cannot release bytes
        # owned by a different concurrent lease.
        self._released = True
        ledger.reserve(size_bytes, stage=stage)
        self._size_bytes = size_bytes
        self._released = False

    @property
    def reserved_bytes(self) -> int:
        """Return the active reservation size."""
        if os.getpid() != self._pid:
            return 0
        with self._lock:
            return 0 if self._released else self._size_bytes

    def resize(self, size_bytes: int) -> None:
        """Resize this retained reservation without racing final cleanup."""
        if os.getpid() != self._pid:
            raise RuntimeError("operation memory lease cannot be reused after fork")
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int):
            raise TypeError("operation memory lease size must be an integer")
        if size_bytes < 0:
            raise ValueError("operation memory lease size must be >= 0")
        with self._lock:
            if self._released:
                raise RuntimeError("operation memory lease is already released")
            current = self._size_bytes
            growth = size_bytes - current
            if growth > 0:
                self._ledger.reserve(growth, stage=self.stage)
            elif growth < 0:
                # Do not publish the smaller local ownership until the native
                # ledger confirms the release. A transient native failure must
                # leave the lease retryable at its original size.
                self._ledger.release(-growth)
            self._size_bytes = size_bytes

    def release(self) -> None:
        """Release this reservation exactly once across competing threads."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._released:
                return
            size_bytes = self._size_bytes
            # Commit local ownership only after the native ledger accepts the
            # release. This mirrors temporary-storage leases and permits an
            # explicit close retry after a transient runtime failure.
            self._ledger.release(size_bytes)
            self._released = True
            self._size_bytes = 0

    close = release

    def __enter__(self) -> "OperationMemoryLease":
        """Return this active memory lease."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Release the memory lease when its context exits."""
        self.release()

    def __del__(self) -> None:
        """Return an abandoned reservation unless Python is shutting down."""
        try:
            if runtime_is_finalizing():
                return
            self.release()
        except BaseException:
            pass


class OperationMemoryLedger:
    """One native atomic ledger shared by Python and C++ operation resources."""

    def __init__(self, memory_limit_bytes: int | None) -> None:
        """Create one native atomic ledger for the resolved operation limit."""
        limit = normalize_memory_limit(memory_limit_bytes)
        from .cross_process_memory import acquire_cross_process_memory
        from .native_runtime import native_core

        self.limit_bytes = limit
        self._pid = os.getpid()
        self._native = native_core
        self._capsule = native_core.operation_memory_ledger_create(limit)
        process_capacity = process_resident_memory_snapshot().capacity_bytes
        self._cross_process = acquire_cross_process_memory(process_capacity, limit)
        self._cross_process_reconciliation_failures = 0
        self._cross_process_pending_bytes = 0
        self._cross_process_release_deferred = False
        self._cross_process_release_failures = 0
        self._post_release_observation_failures = 0
        self._close_advisory_recorded = False
        self._close_peak_bytes = 0
        self._lock = Lock()
        self._cross_process_io_lock = Lock()
        self._close_condition = Condition(self._lock)
        self._close_started = False
        self._closing = False
        self._closed = False
        self._close_outstanding_bytes = 0

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

    def reserve(self, size_bytes: int, *, stage: str) -> None:
        """Atomically reserve resident bytes or raise the public resource error."""
        self._ensure_owner_process()
        from .cancellation import check_operation_cancelled

        check_operation_cancelled(stage=stage)
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int):
            raise TypeError("operation memory reservation must be an integer")
        if size_bytes < 0:
            raise ValueError("operation memory reservation must be >= 0")
        if size_bytes == 0:
            return
        try:
            with self._ensure_cross_process_io_lock():
                with self._lock:
                    if self._close_started:
                        raise RuntimeError("operation memory ledger is closed")
                    capsule = self._capsule
                    self._native.operation_memory_ledger_reserve(capsule, size_bytes, stage)
                    values = self._native.operation_memory_ledger_snapshot(capsule)
                    if not isinstance(values, tuple) or len(values) != 3:
                        raise RuntimeError(
                            "native operation memory ledger returned invalid statistics"
                        )
                    target_reserved = int(values[1])
                try:
                    self._reconcile_cross_process(target_reserved, strict=True)
                except BaseException as exc:
                    rollback_target: int | None = None
                    try:
                        with self._lock:
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
        except MemoryError as exc:
            from ..errors import SchemaSanitizerResourceError

            snapshot = self.snapshot()
            process_snapshot = process_resident_memory_snapshot()
            operation_actual = snapshot.reserved_bytes + size_bytes
            process_limited = str(exc).startswith("process resident memory")
            detail: dict[str, int | str | None] = {
                "stage": stage,
                "limit_name": (
                    "process_resident_memory_bytes" if process_limited else "memory_limit_bytes"
                ),
                "limit_bytes": (
                    process_snapshot.capacity_bytes if process_limited else snapshot.limit_bytes
                ),
                "actual_bytes": (
                    process_snapshot.reserved_bytes + size_bytes
                    if process_limited
                    else operation_actual
                ),
            }
            if process_limited:
                pressure = process_memory_pressure_snapshot()
                detail["process_rss_bytes"] = pressure.rss_bytes
                detail["untracked_rss_bytes"] = pressure.untracked_rss_bytes
            raise SchemaSanitizerResourceError(str(exc), detail=detail) from None

    def acquire(self, size_bytes: int, *, stage: str) -> OperationMemoryLease:
        """Reserve bytes and return an exactly-once lifetime lease."""
        self._ensure_owner_process()
        return OperationMemoryLease(self, size_bytes, stage)

    def release(self, size_bytes: int) -> None:
        """Release native bytes and reconcile host state outside local locks."""
        if os.getpid() != self._pid:
            return
        amount = max(0, int(size_bytes))
        if amount == 0:
            return
        advisory_peak: int | None = None
        with self._ensure_cross_process_io_lock():
            try:
                with self._lock:
                    capsule = self._capsule
                    self._native.operation_memory_ledger_release(capsule, amount)
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
                if close_started and reserved == 0:
                    error = self._release_cross_process_after_close(strict=False)
                    if error is None:
                        with self._lock:
                            advisory_peak = self._claim_close_advisory_locked(peak)
                else:
                    self._reconcile_cross_process(reserved, strict=False)
            except BaseException:
                # Native ownership is already gone. Preserve the conservative
                # host reservation and never make the caller double-release.
                with self._lock:
                    self._post_release_observation_failures = (
                        getattr(self, "_post_release_observation_failures", 0) + 1
                    )
        if advisory_peak is not None:
            self._record_close_advisory(advisory_peak)

    def snapshot(self) -> OperationMemorySnapshot:
        """Return current and peak bytes across Python and native allocators."""
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
                values = self._native.operation_memory_ledger_snapshot(self._capsule)
                if isinstance(values, tuple) and len(values) == 3:
                    outstanding = max(0, int(values[1]))
                    peak = max(0, int(values[2]))
                    self._close_outstanding_bytes = outstanding
                    self._close_peak_bytes = max(getattr(self, "_close_peak_bytes", 0), peak)
                else:
                    outstanding = 0
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

        if advisory_peak is not None:
            self._record_close_advisory(advisory_peak)

    def __del__(self) -> None:
        """Release an abandoned ledger unless interpreter teardown has begun."""
        try:
            if runtime_is_finalizing():
                return
            self.close()
        except BaseException:
            pass


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
    """Detach inherited native ledgers without invoking capsule finalizers."""
    inherited = _OPERATION_MEMORY_LEDGER.get()
    if inherited is not None:
        quarantine_inherited_state("memory-ledger", inherited)
    _OPERATION_MEMORY_LEDGER.set(None)


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_operation_memory_ledger_after_fork)


def current_operation_memory_ledger() -> OperationMemoryLedger | None:
    """Return the active operation ledger, if the caller owns one."""
    return _OPERATION_MEMORY_LEDGER.get()


def acquire_operation_memory(size_bytes: int, *, stage: str) -> OperationMemoryLease | None:
    """Acquire a retained lease from the active operation when present."""
    ledger = current_operation_memory_ledger()
    return None if ledger is None else ledger.acquire(max(0, int(size_bytes)), stage=stage)


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


def adaptive_concurrency_target(
    desired: int,
    *,
    per_slot_bytes: int,
    reserve_bytes: int | None = None,
) -> int:
    """Narrow a live concurrency window using current process headroom.

    The result is always at least one so callers preserve forward progress. A
    small untracked reserve is held back for interpreter, TLS, SDK, and thread
    stack growth outside the exact resident ledger.
    """
    if isinstance(desired, bool) or not isinstance(desired, int):
        raise TypeError("desired concurrency must be an integer")
    if isinstance(per_slot_bytes, bool) or not isinstance(per_slot_bytes, int):
        raise TypeError("per_slot_bytes must be an integer")
    if desired <= 0:
        raise ValueError("desired concurrency must be > 0")
    if per_slot_bytes <= 0:
        raise ValueError("per_slot_bytes must be > 0")
    from .cancellation import check_operation_cancelled
    from .system_pressure import pressure_adjusted_target

    check_operation_cancelled(stage="adaptive_concurrency")
    desired = pressure_adjusted_target(desired)
    snapshot = process_resident_memory_snapshot()
    headroom = max(0, snapshot.capacity_bytes - snapshot.reserved_bytes)
    if reserve_bytes is None:
        fallback_reserve = max(4 << 20, snapshot.capacity_bytes // 64)
        from .safety_margins import tuned_memory_reserve_bytes

        reserve = tuned_memory_reserve_bytes(snapshot.capacity_bytes, fallback_reserve)
    else:
        if isinstance(reserve_bytes, bool) or not isinstance(reserve_bytes, int):
            raise TypeError("reserve_bytes must be an integer or None")
        if reserve_bytes < 0:
            raise ValueError("reserve_bytes must be >= 0")
        reserve = reserve_bytes
    usable = max(0, headroom - reserve)
    slots = max(1, usable // per_slot_bytes)
    return max(1, min(desired, slots))


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


__all__ = [
    "MAX_MEMORY_LIMIT_BYTES",
    "MemoryBudget",
    "OperationMemoryDiagnostics",
    "OperationMemoryLedger",
    "OperationMemoryLease",
    "OperationMemorySnapshot",
    "ProcessMemoryPressureSnapshot",
    "ProcessResidentMemorySnapshot",
    "acquire_operation_memory",
    "adaptive_concurrency_target",
    "activate_operation_memory_ledger",
    "current_operation_memory_ledger",
    "memory_budget",
    "normalize_memory_limit",
    "process_memory_pressure_snapshot",
    "process_resident_memory_snapshot",
    "reserve_operation_memory",
]
