"""Coordinate single-flight phased shutdown for process-wide concurrency services.

One authoritative caller drains finalizers and registered services under a deadline while
secondary callers share its eventual result or error.
"""

from __future__ import annotations

import sys
import threading
from dataclasses import dataclass
from time import monotonic_ns

from . import async_scheduler as _async_scheduler_module
from . import cleanup_dispatcher as _cleanup_module
from . import control_plane_budget as _control_plane_module
from . import process_resources as _resources_module
from . import retry_scheduler as _retry_module
from . import runtime_registry as _registry_module
from . import temporary_janitor as _janitor_module
from .control_plane_budget import process_control_plane_snapshot
from .durations import deadline_ns_from_timeout, remaining_seconds
from .finalizer_admission import finalizer_admission_snapshot
from .finalizer_registry import (
    finalizer_activity_buffer_size,
    finalizer_domains,
    freeze_finalizer_registry,
    write_finalizer_activity_into,
)
from .fork_safety import ensure_runtime_fork_safe, quarantine_inherited_state
from .governed_thread import reap_governed_thread_retirements
from .process_resources import (
    process_file_descriptor_snapshot,
    process_thread_snapshot,
)
from .runtime_registry import _RUNTIME_SERVICES, RuntimeServicePhase, runtime_service_snapshot
from .safe_errors import clear_exception_traceback
from .shutdown_observers import freeze_shutdown_observers
from .static_control_plane import freeze_static_control_plane


@dataclass(frozen=True, slots=True)
class ConcurrencyShutdownResult:
    """Summarize the terminal state of every concurrency subsystem."""

    retry_scheduler_stopped: bool
    janitor_stopped: bool
    cleanup_dispatcher_stopped: bool
    release_guardian_stopped: bool
    deadline_exhausted: bool
    registered_services_stopped: bool = True
    remaining_registered_services: int = 0
    thread_leases_remaining: int = 0
    fd_leases_remaining: int = 0
    admission_closed: bool = False
    resources_drained: bool = False
    owners_parked: int = 0
    workers_stopped: bool = False
    services_quiescent: bool = False
    retry_work_drained: bool = False
    cleanup_work_drained: bool = False
    owners_released: bool = False
    terminal_success: bool = False
    shutdown_generation: int = 0
    availability_notifier_stopped: bool = True
    native_reaper_stopped: bool = True
    observability_complete: bool = True
    observability_failures: tuple[str, ...] = ()
    terminal_owners_remaining: int = 0
    terminal_retained_bytes: int = 0
    terminal_publication_rejections: int = 0
    finalizer_quiescent: bool = True
    finalizer_redrain_rounds: int = 0

    @property
    def stopped(self) -> bool:
        """Return whether shutdown has completed."""
        return self.terminal_success


_MAX_SHUTDOWN_FAILURES = 256
_SHUTDOWN_FAILURE_FALLBACK = ("shutdown:diagnostics_unavailable",)


class _BoundedShutdownFailures:
    """Preallocated failure sink used by terminal correctness paths."""

    __slots__ = ("_items", "_count", "_overflowed")

    def __init__(self) -> None:
        """Initialize the bounded shutdown failures and its owned runtime state."""
        self._items: list[str | None] = [None] * _MAX_SHUTDOWN_FAILURES
        self._count = 0
        self._overflowed = False

    def reset(self) -> None:
        """Clear recorded shutdown failures for reuse in the current process."""
        for index in range(self._count):
            self._items[index] = None
        self._count = 0
        self._overflowed = False

    def append(self, message: str) -> None:
        """Append one value to the bounded collection."""
        if self._count >= _MAX_SHUTDOWN_FAILURES:
            self._overflowed = True
            return
        self._items[self._count] = message
        self._count += 1

    def __bool__(self) -> bool:
        """Return whether the instance currently carries a value."""
        return self._count > 0 or self._overflowed

    def __iter__(self):
        """Iterate over the retained values."""
        for index in range(self._count):
            value = self._items[index]
            if value is not None:
                yield value
        if self._overflowed:
            yield "shutdown:failure_buffer_overflow"

    def freeze(self) -> tuple[str, ...]:
        """Freeze accumulated shutdown failures into an immutable view."""
        try:
            return tuple(self)
        except BaseException:
            return _SHUTDOWN_FAILURE_FALLBACK


_SHUTDOWN_FAILURES = _BoundedShutdownFailures()

_SHUTDOWN_CONDITION = threading.Condition()
_SHUTDOWN_CHILD_CONDITIONS = (threading.Condition(), threading.Condition())
_SHUTDOWN_CHILD_CONDITION_INDEX = 0
_SHUTDOWN_FORK_FRESH_CONDITION: threading.Condition | None = None
_SHUTDOWN_IN_PROGRESS = False
_SHUTDOWN_OWNER: int | None = None
_SHUTDOWN_RESULT: ConcurrencyShutdownResult | None = None
_SHUTDOWN_ERROR: BaseException | None = None
_SHUTDOWN_GENERATION = 0


def _raise_shared_shutdown_error(error: BaseException) -> None:
    """Raise one shared failure while stripping frames before propagation ends."""
    try:
        raise error
    finally:
        clear_exception_traceback(error)


def _current_shutdown_error() -> BaseException | None:
    """Read the shared error after a condition wait without stale narrowing."""
    return _SHUTDOWN_ERROR


def _wait_for_shared_shutdown(deadline_ns: int) -> ConcurrencyShutdownResult:
    """Wait under the shutdown condition for the active owner's publication."""
    while _SHUTDOWN_IN_PROGRESS and _SHUTDOWN_RESULT is None and _SHUTDOWN_ERROR is None:
        remaining = remaining_seconds(deadline_ns)
        if remaining <= 0:
            return _timed_out_result(deadline_ns, _SHUTDOWN_GENERATION)
        _SHUTDOWN_CONDITION.wait(timeout=min(0.05, remaining))
    waited_error = _current_shutdown_error()
    if waited_error is not None:
        _raise_shared_shutdown_error(waited_error)
    return _SHUTDOWN_RESULT or _timed_out_result(deadline_ns, _SHUTDOWN_GENERATION)


def _timed_out_result(deadline_ns: int, generation: int) -> ConcurrencyShutdownResult:
    # A secondary caller that exhausts its own wait budget must not invent the
    # primary shutdown state.  Read only process-wide gates that are safe here.
    """Build a bounded shutdown result for a caller whose deadline expired."""
    try:
        admission_closed = bool(
            _async_scheduler_module.async_scheduler_snapshot().admission_closed
            and process_thread_snapshot().admission_closed
            and process_file_descriptor_snapshot().admission_closed
            and runtime_service_snapshot().admission_closed
        )
    except Exception:
        admission_closed = False
    return ConcurrencyShutdownResult(
        False,
        False,
        False,
        False,
        monotonic_ns() >= deadline_ns,
        registered_services_stopped=False,
        admission_closed=admission_closed,
        shutdown_generation=generation,
    )


def _shutdown_native_cleanup_reaper(deadline_ns: int) -> bool:
    """Best-effort integration with the optional native cleanup reaper."""
    native = sys.modules.get("schema_sanitizer._core_abi3")
    if native is None:
        return True
    method = getattr(native, "operation_task_arena_reaper_shutdown", None)
    if not callable(method):
        return True
    timeout_ms = max(0, min((1 << 31) - 1, int(remaining_seconds(deadline_ns) * 1000)))
    try:
        return bool(method(timeout_ms))
    except Exception:
        return False


def _perform_shutdown(deadline_ns: int, generation: int) -> ConcurrencyShutdownResult:
    # Resolve already-imported module attributes now (no imports/initialization).
    # This preserves runtime instrumentation/monkeypatching without capturing
    # stale singleton objects at module-import time.
    """Run the authoritative phased shutdown for one generation."""
    _DISPATCHER = _cleanup_module._DISPATCHER
    _SCHEDULER = _retry_module._SCHEDULER
    _RELEASE_GUARDIAN = _retry_module._RELEASE_GUARDIAN
    _JANITOR = _janitor_module._JANITOR
    _RUNTIME_SERVICES = _registry_module._RUNTIME_SERVICES
    close_process_resource_external_admission = (
        _resources_module.close_process_resource_external_admission
    )
    close_process_resource_admission = _resources_module.close_process_resource_admission
    close_release_guardian_thread_admission = (
        _resources_module.close_release_guardian_thread_admission
    )
    shutdown_availability_notifier = _resources_module.shutdown_availability_notifier
    availability_notifier_snapshot = _resources_module.availability_notifier_snapshot
    availability_notifier_thread_snapshot = _resources_module.availability_notifier_thread_snapshot
    process_file_descriptor_snapshot = _resources_module.process_file_descriptor_snapshot
    process_thread_snapshot = _resources_module.process_thread_snapshot
    release_guardian_thread_snapshot = _resources_module.release_guardian_thread_snapshot
    uncertain_fd_close_snapshot = _resources_module.uncertain_fd_close_snapshot

    # Freeze static/control and finalizer discovery before closing any producer.
    # Immutable views are built while ordinary runtime allocation is still permitted.
    freeze_static_control_plane()
    freeze_finalizer_registry()
    # Allocate both quiescence buffers before teardown starts. From Phase 1 on,
    # the correctness barrier only mutates these fixed-size buffers.
    activity_size = finalizer_activity_buffer_size()
    finalizer_activity_a = bytearray(activity_size)
    finalizer_activity_b = bytearray(activity_size)
    terminal_observers = freeze_shutdown_observers()
    # Phase 1: stop external producers, but keep the dedicated teardown
    # reserve open.  Cleanup workers and descriptor-relative janitor work may
    # still need threads/FDs to complete the shutdown.
    _RUNTIME_SERVICES.close_admission()
    close_process_resource_external_admission()
    _async_scheduler_module.close_async_scheduler_admission()
    remote_io_module = sys.modules.get("schema_sanitizer.remote_impl.io_permits")
    if remote_io_module is not None:
        close_remote_io = getattr(remote_io_module, "close_remote_io_permit_admission", None)
        if callable(close_remote_io):
            close_remote_io()
    # Give already-running Tasks a short grace period, but never let async drain
    # consume the whole caller deadline before the producers those Tasks depend
    # on have even entered shutdown.
    initial_async_grace = min(0.25, max(0.0, remaining_seconds(deadline_ns) * 0.10))
    _async_scheduler_module.wait_async_scheduler_quiescent(initial_async_grace)

    # Phase 2: close ordinary producers while cleanup/retry consumers live.
    _RUNTIME_SERVICES.close_all(deadline_ns=deadline_ns, phase=RuntimeServicePhase.PRODUCER)
    # Producer close can unblock cancellation-resistant provider Tasks. Drain a
    # second time with a bounded share; final certification below uses the final
    # authoritative snapshot rather than sticking a transient timeout forever.
    _async_scheduler_module.wait_async_scheduler_quiescent(
        min(1.0, max(0.0, remaining_seconds(deadline_ns) * 0.25))
    )

    # Phase 3: transfer GC-published owners while cleanup/retry hosts live.
    # Closing services can itself drop the last strong reference to an owner, so
    # shutdown uses repeated quiescence epochs rather than one optimistic drain.
    observability_failures = _SHUTDOWN_FAILURES
    observability_failures.reset()
    finalizer_drain_failures = observability_failures
    finalizer_redrain_rounds = 0

    def drain_finalizer_epoch() -> None:
        """Drain one bounded epoch of published finalizer work."""
        nonlocal finalizer_redrain_rounds
        finalizer_redrain_rounds += 1
        # Domains are frozen before Phase 1. Do not construct diagnostic
        # snapshots here: they are irrelevant to the correctness barrier and
        # can allocate under the very memory pressure shutdown is handling.
        for domain in finalizer_domains():
            try:
                domain.drain()
            except BaseException:
                finalizer_drain_failures.append("finalizer_domain:drain_failed")
        try:
            _control_plane_module.drain_deferred_control_plane_releases(limit=256)
        except BaseException:
            finalizer_drain_failures.append("control_plane:deferred_release_failed")

    def quiesce_finalizers(*, max_rounds: int = 8) -> bool:
        """Drain finalizers until two bounded activity samples prove quiescence."""
        previous = finalizer_activity_a
        current = finalizer_activity_b
        have_previous = False
        for _ in range(max_rounds):
            if remaining_seconds(deadline_ns) <= 0:
                return False
            drain_finalizer_epoch()
            # Fixed-width publication/progress epochs close ABA; zero active
            # owners closes the stable-RESERVED hole. Both buffers were
            # allocated before teardown, so this loop performs no container
            # growth merely to prove quiescence.
            quiescent = write_finalizer_activity_into(current)
            if have_previous and current == previous and quiescent:
                return True
            previous, current = current, previous
            have_previous = True
        return False

    finalizer_quiescent = quiesce_finalizers()
    # Continue with registered cleanup producers and internal cleanup while
    # the retry scheduler remains available.
    _RUNTIME_SERVICES.close_all(deadline_ns=deadline_ns, phase=RuntimeServicePhase.CLEANUP_PRODUCER)
    finalizer_quiescent = quiesce_finalizers() and finalizer_quiescent
    janitor_stopped = _JANITOR.close(deadline_seconds=remaining_seconds(deadline_ns))
    dispatcher_stopped = _DISPATCHER.close(deadline_seconds=remaining_seconds(deadline_ns))

    # Phase 4: no producer may now create a new delayed retry.
    retry_stopped = _SCHEDULER.close(deadline_seconds=remaining_seconds(deadline_ns))
    try:
        reap_governed_thread_retirements()
    except BaseException:
        finalizer_drain_failures.append("governed_thread:reap_failed")

    # Phase 5: close any registered consumers that depended on producers.
    _RUNTIME_SERVICES.close_all(deadline_ns=deadline_ns, phase=RuntimeServicePhase.CONSUMER)
    finalizer_quiescent = quiesce_finalizers() and finalizer_quiescent

    # Phase 6: release ownership is the final consumer to stop.
    guardian_stopped = _RELEASE_GUARDIAN.close(deadline_seconds=remaining_seconds(deadline_ns))
    # Availability hooks can be published while the final leases are returned.
    # Drain that host before closing its dedicated admission.
    notifier_stopped = shutdown_availability_notifier(
        deadline_seconds=remaining_seconds(deadline_ns)
    )
    try:
        reap_governed_thread_retirements()
    except BaseException:
        finalizer_drain_failures.append("governed_thread:final_reap_failed")
    # The emergency budgets remain available until their hosts are quiescent.
    close_release_guardian_thread_admission()

    native_reaper_stopped = _shutdown_native_cleanup_reaper(deadline_ns)

    _async_scheduler_module.wait_async_scheduler_quiescent(remaining_seconds(deadline_ns))

    # Only after every cleanup host has had a chance to drain do we close the
    # internal teardown reserve.  This turns the admission split into a strict
    # two-phase commit instead of starving shutdown itself.
    close_process_resource_admission()

    if not finalizer_quiescent:
        observability_failures.append("finalizers:quiescence_not_reached")

    def snapshot_or_none(service: object, name: str) -> object | None:
        """Return a bounded snapshot when runtime state is available."""
        snapshot = getattr(service, "snapshot", None)
        if not callable(snapshot):
            observability_failures.append("runtime_service:missing_snapshot")
            return None
        try:
            return snapshot()
        except Exception:
            observability_failures.append("runtime_service:snapshot_failed")
            return None

    thread_snapshot = process_thread_snapshot()
    guardian_thread_snapshot = release_guardian_thread_snapshot()
    notifier_thread_snapshot = availability_notifier_thread_snapshot()
    notifier_snapshot = availability_notifier_snapshot()
    fd_snapshot = process_file_descriptor_snapshot()
    uncertain_fd_snapshot = uncertain_fd_close_snapshot()
    async_snapshot = _async_scheduler_module.async_scheduler_snapshot()
    async_scheduler_quiescent = not (
        async_snapshot.active_operations
        or async_snapshot.terminal_debts
        or getattr(async_snapshot, "protocol_violations", 0)
    )
    if getattr(async_snapshot, "protocol_violations", 0):
        observability_failures.append("async_scheduler:protocol_violation")
    retry_snapshot = snapshot_or_none(_SCHEDULER, "retry_scheduler")
    cleanup_snapshot = snapshot_or_none(_DISPATCHER, "cleanup_dispatcher")
    janitor_snapshot = snapshot_or_none(_JANITOR, "temporary_janitor")
    guardian_snapshot = snapshot_or_none(_RELEASE_GUARDIAN, "release_guardian")
    service_snapshot = _RUNTIME_SERVICES.snapshot()
    # Optional terminal ownership domains were registered during normal import.
    # Never import a new subsystem merely to observe it under terminal pressure.
    observer_snapshots: dict[str, object | None] = {}
    for observer in terminal_observers:
        try:
            observer_snapshots[observer.name] = observer.snapshot()
        except BaseException:
            observer_snapshots[observer.name] = None
            observability_failures.append("shutdown_observer:snapshot_failed")

    native_snapshot = observer_snapshots.get("native_arena")
    if native_snapshot is None:
        native_snapshot = {"available": False}
    elif (
        isinstance(native_snapshot, dict)
        and native_snapshot.get("available")
        and native_snapshot.get("snapshot_failed")
    ):
        observability_failures.append("native_arena:snapshot_failed")
    native_fd_snapshot = observer_snapshots.get("native_file_descriptors")
    if native_fd_snapshot is None:
        native_fd_snapshot = {"available": False, "reserved": 0, "opened": 0}
    elif (
        isinstance(native_fd_snapshot, dict)
        and native_fd_snapshot.get("available")
        and native_fd_snapshot.get("snapshot_failed")
    ):
        observability_failures.append("native_file_descriptors:snapshot_failed")
    failed_bridge_snapshot = observer_snapshots.get("failed_bridge_runners")
    orphaned_startup_snapshot = observer_snapshots.get("orphaned_remote_startups")
    terminal_snapshot = observer_snapshots.get("terminal_ownership")

    # Unloaded optional modules cannot own state; loaded modules are represented
    # by observers.
    authoritative: dict[str, object] = {}

    # Finalizer domains are already registered; observing them must not import
    # modules or initialize subsystems during terminal memory pressure.
    registered_finalizer_snapshots: dict[str, object] = {}
    for domain in finalizer_domains():
        try:
            registered_finalizer_snapshots[domain.name] = domain.snapshot()
        except Exception:
            registered_finalizer_snapshots[domain.name] = None
            observability_failures.append("finalizer_domain:snapshot_failed")

    authoritative["generic_finalizer_cleanup"] = registered_finalizer_snapshots.get(
        "finalizer_cleanup", (0, 0)
    )
    authoritative["operation_memory_finalizers"] = registered_finalizer_snapshots.get(
        "operation_memory", (0, 0, 0)
    )
    authoritative["temporary_storage_finalizers"] = registered_finalizer_snapshots.get(
        "temporary_storage", (0, 0, 0)
    )
    authoritative["path_claim_finalizers"] = registered_finalizer_snapshots.get(
        "path_claim", (0, 0)
    )
    authoritative["operation_finalizers"] = registered_finalizer_snapshots.get(
        "operation_context", (0, 0, 0)
    )
    authoritative["partition_lookahead_finalizers"] = registered_finalizer_snapshots.get(
        "partition_lookahead", (0, 0)
    )
    try:
        authoritative["finalizer_admission"] = finalizer_admission_snapshot()
    except Exception:
        authoritative["finalizer_admission"] = None
        observability_failures.append("finalizer_admission:snapshot_failed")
    try:
        authoritative["control_plane_budget"] = process_control_plane_snapshot()
    except Exception:
        authoritative["control_plane_budget"] = None
        observability_failures.append("control_plane_budget:snapshot_failed")

    for name in (
        "cross_process_memory",
        "governed_thread_retirement",
        "temporary_storage_authoritative",
        "remote_io_permits",
        "provider_throttle",
        "external_runtime_pools",
    ):
        if name in observer_snapshots:
            authoritative[name] = observer_snapshots[name]
        else:
            # Not loaded before freeze => incapable of having admitted an owner.
            authoritative[name] = None

    cross_memory = authoritative.get("cross_process_memory")
    if isinstance(cross_memory, dict):
        cross_overflows = int(cross_memory.get("finalizer_overflows", 0) or 0)
        cross_unknown = int(cross_memory.get("unknown_releases", 0) or 0)
        if cross_overflows:
            observability_failures.append("cross_process_memory:finalizer_release_overflow")
        if cross_unknown:
            observability_failures.append("cross_process_memory:unknown_release")

    remote_io_authoritative = authoritative.get("remote_io_permits")
    provider_authoritative = authoritative.get("provider_throttle")
    external_runtime_authoritative = authoritative.get("external_runtime_pools")
    if remote_io_authoritative is not None:
        remote_unknown = (
            int(getattr(remote_io_authoritative, "unknown_permit_releases", 0))
            + int(getattr(remote_io_authoritative, "unknown_submission_releases", 0))
            + int(getattr(remote_io_authoritative, "unknown_capacity_releases", 0))
        )
        if remote_unknown:
            observability_failures.append("remote_io:unknown_release")
        if int(getattr(remote_io_authoritative, "protocol_violations", 0)):
            observability_failures.append("remote_io:protocol_violation")
    if provider_authoritative is not None:
        provider_unknown = int(getattr(provider_authoritative, "unknown_lease_releases", 0))
        if provider_unknown:
            observability_failures.append("provider_throttle:unknown_release")

    def field(snapshot: object | None, name: str, default: int | bool = 0):
        # Missing snapshots are tracked above and therefore can never be
        # interpreted as terminal success even though arithmetic needs a
        # bounded placeholder value. Unknown is not equivalent to zero.
        """Return one normalized field from the diagnostic snapshot."""
        return getattr(snapshot, name, default) if snapshot is not None else default

    remaining_services = service_snapshot.registered_services
    parked_owners = (
        int(field(guardian_snapshot, "parked_owners"))
        + int(field(guardian_snapshot, "dead_letter_owners"))
        + int(field(cleanup_snapshot, "parked_calls"))
        + int(field(cleanup_snapshot, "dead_letter_calls"))
        + int(field(janitor_snapshot, "pending_artifacts"))
        + int(field(notifier_snapshot, "parked_callbacks"))
    )
    terminal_hosts_remaining = int(field(failed_bridge_snapshot, "hosts")) + int(
        field(orphaned_startup_snapshot, "hosts")
    )
    native_hosts_remaining = (
        sum(
            int(native_snapshot.get(name, 0) or 0)
            for name in (
                "live_arenas",
                "detached_workers",
                "reaper_workers",
                "reaper_queued_states",
                "reaper_active_states",
                "reaper_reserved_states",
                "reaper_parked_states",
            )
        )
        if isinstance(native_snapshot, dict) and native_snapshot.get("available")
        else 0
    )
    terminal_owners_remaining = int(field(terminal_snapshot, "owners"))
    terminal_retained_bytes = int(field(terminal_snapshot, "retained_bytes")) + int(
        field(terminal_snapshot, "metadata_bytes")
    )
    terminal_publication_rejections = int(field(terminal_snapshot, "rejected"))
    terminal_generation_exhausted = bool(field(terminal_snapshot, "generation_exhausted", False))
    if terminal_generation_exhausted:
        observability_failures.append("terminal_ownership:generation_exhausted")
    if terminal_publication_rejections:
        # Once terminal metadata publication has overflowed, the process can no
        # longer prove that every terminal owner is represented even if later
        # entries retire. Treat the lost observation as fail-closed.
        observability_failures.append("terminal_ownership:publication_rejected")

    def tuple_value(name: str, index: int) -> int:
        """Return the diagnostic value normalized as a tuple."""
        value = authoritative.get(name)
        if isinstance(value, tuple) and len(value) > index:
            try:
                return int(value[index])
            except Exception:
                return 1
        return 0

    generic_finalizer_published = tuple_value("generic_finalizer_cleanup", 0)
    generic_finalizer_overflows = tuple_value("generic_finalizer_cleanup", 1)
    finalizer_admission = authoritative.get("finalizer_admission")
    prepared_finalizer_active = 0
    if finalizer_admission is not None:
        for domain in getattr(finalizer_admission, "domains", ()):
            if getattr(domain, "name", "") == "prepared_cleanup":
                prepared_finalizer_active = int(getattr(domain, "active", 0))
                break
    finalizer_admission_active = int(
        getattr(finalizer_admission, "active", 0) if finalizer_admission is not None else 0
    )
    if finalizer_admission is not None and not bool(
        getattr(finalizer_admission, "invariant_ok", False)
    ):
        observability_failures.append("finalizer_admission:capacity_invariant_failed")
    finalizer_publication_failures = int(
        getattr(finalizer_admission, "publication_failures", 0)
        if finalizer_admission is not None
        else 0
    )
    if finalizer_publication_failures:
        observability_failures.append("finalizer_admission:publication_failures")
    # Admission rejection is intentionally *not* terminal corruption. It means
    # no external owner was admitted because teardown capacity was temporarily full.
    control_plane = authoritative.get("control_plane_budget")
    control_plane_active = int(getattr(control_plane, "active_tickets", 0))
    control_plane_reserved = int(getattr(control_plane, "reserved_bytes", 0))
    control_plane_over_releases = int(getattr(control_plane, "over_release_count", 0))
    control_plane_reconciliation_pending = bool(
        getattr(control_plane, "reconciliation_pending", False)
    )
    if control_plane_over_releases:
        observability_failures.append("control_plane_budget:over_release")
    if control_plane_reconciliation_pending:
        observability_failures.append("control_plane_budget:reconciliation_pending")
    retirement_debts = tuple_value("governed_thread_retirement", 0)
    retirement_overflows = tuple_value("governed_thread_retirement", 1)
    memory_finalizer_reserved = tuple_value("operation_memory_finalizers", 0)
    memory_finalizer_published = tuple_value("operation_memory_finalizers", 1)
    memory_finalizer_overflows = tuple_value("operation_memory_finalizers", 2)
    storage_finalizer_reserved = tuple_value("temporary_storage_finalizers", 0)
    storage_finalizer_published = tuple_value("temporary_storage_finalizers", 1)
    storage_finalizer_overflows = tuple_value("temporary_storage_finalizers", 2)
    operation_finalizer_reserved = tuple_value("operation_finalizers", 0)
    operation_finalizer_published = tuple_value("operation_finalizers", 1)
    operation_finalizer_overflows = tuple_value("operation_finalizers", 2)
    path_finalizer_published = tuple_value("path_claim_finalizers", 0)
    path_finalizer_overflows = tuple_value("path_claim_finalizers", 1)
    lookahead_finalizer_published = tuple_value("partition_lookahead_finalizers", 0)
    lookahead_finalizer_overflows = tuple_value("partition_lookahead_finalizers", 1)

    irreversible_overflows = (
        generic_finalizer_overflows
        + retirement_overflows
        + memory_finalizer_overflows
        + storage_finalizer_overflows
        + operation_finalizer_overflows
        + path_finalizer_overflows
        + lookahead_finalizer_overflows
    )
    if irreversible_overflows:
        observability_failures.append("authoritative_finalizers:overflow")

    temporary_authoritative = authoritative.get("temporary_storage_authoritative")
    temporary_logical_bytes = int(
        getattr(temporary_authoritative, "reserved_bytes", 0)
        if temporary_authoritative is not None
        else 0
    )
    temporary_logical_inodes = int(
        getattr(temporary_authoritative, "reserved_inodes", 0)
        if temporary_authoritative is not None
        else 0
    )
    temporary_cross_bytes = int(
        getattr(temporary_authoritative, "cross_reserved_bytes", 0)
        if temporary_authoritative is not None
        else 0
    )
    temporary_cross_inodes = int(
        getattr(temporary_authoritative, "cross_reserved_inodes", 0)
        if temporary_authoritative is not None
        else 0
    )
    temporary_protocol_violations = int(
        getattr(temporary_authoritative, "protocol_violations", 0)
        if temporary_authoritative is not None
        else 0
    )
    if temporary_protocol_violations:
        observability_failures.append("temporary_storage:protocol_violation")

    cross_logical_contributions = (
        int(cross_memory.get("logical_contributions", 0) or 0)
        if isinstance(cross_memory, dict)
        else 0
    )
    cross_logical_bytes = (
        int(cross_memory.get("logical_bytes", 0) or 0) if isinstance(cross_memory, dict) else 0
    )
    cross_deferred_finalizers = (
        int(cross_memory.get("deferred_finalizers", 0) or 0)
        + int(cross_memory.get("direct_finalizers", 0) or 0)
        if isinstance(cross_memory, dict)
        else 0
    )
    cross_physical_bytes = (
        int(cross_memory.get("physical_bytes", 0) or 0) if isinstance(cross_memory, dict) else 0
    )
    cross_direct_live_bytes = (
        int(cross_memory.get("direct_live_bytes", 0) or 0) if isinstance(cross_memory, dict) else 0
    )

    remote_io_in_use = int(getattr(remote_io_authoritative, "in_use", 0))
    remote_io_waiting = int(getattr(remote_io_authoritative, "waiting", 0))
    remote_io_sync_waiters = int(getattr(remote_io_authoritative, "sync_waiters", 0))
    remote_io_protocol_violations = int(getattr(remote_io_authoritative, "protocol_violations", 0))
    remote_io_permits = int(getattr(remote_io_authoritative, "active_permits", 0))
    remote_io_submissions = int(getattr(remote_io_authoritative, "pending_submissions", 0))
    remote_io_submission_capabilities = int(
        getattr(remote_io_authoritative, "active_submission_reservations", 0)
    )
    remote_io_registrations = int(
        getattr(remote_io_authoritative, "active_capacity_registrations", 0)
    )
    remote_io_capacity_capabilities = int(
        getattr(remote_io_authoritative, "active_capacity_capabilities", 0)
    )
    provider_active_leases = int(getattr(provider_authoritative, "active_leases", 0))
    external_runtime_physical_claims = (
        int(external_runtime_authoritative.get("claims", 0) or 0)
        if isinstance(external_runtime_authoritative, dict)
        else 0
    )
    external_runtime_logical_claims = (
        int(external_runtime_authoritative.get("logical_claims", 0) or 0)
        if isinstance(external_runtime_authoritative, dict)
        else 0
    )
    external_runtime_physical_permits = (
        int(external_runtime_authoritative.get("physical_permits", 0) or 0)
        if isinstance(external_runtime_authoritative, dict)
        else 0
    )
    external_runtime_logical_width = (
        int(external_runtime_authoritative.get("logical_width", 0) or 0)
        if isinstance(external_runtime_authoritative, dict)
        else 0
    )
    external_runtime_configuration_inflight = (
        int(external_runtime_authoritative.get("configuration_inflight", 0) or 0)
        if isinstance(external_runtime_authoritative, dict)
        else 0
    )
    external_runtime_configuration_uncertain = (
        int(external_runtime_authoritative.get("configuration_uncertain", 0) or 0)
        if isinstance(external_runtime_authoritative, dict)
        else 0
    )
    if external_runtime_configuration_uncertain:
        observability_failures.append("external_runtime_pools:configuration_uncertain")

    observability_complete = not observability_failures
    workers_stopped = not (
        async_snapshot.active_operations
        or async_snapshot.terminal_debts
        or field(retry_snapshot, "worker_alive", False)
        or field(retry_snapshot, "execution_workers")
        or field(retry_snapshot, "retiring_workers")
        or field(cleanup_snapshot, "active_workers")
        or field(cleanup_snapshot, "workers_starting")
        or field(cleanup_snapshot, "retiring_workers")
        or field(guardian_snapshot, "active_workers")
        or field(guardian_snapshot, "retiring_workers")
        or guardian_thread_snapshot.in_use
        or notifier_thread_snapshot.in_use
        or notifier_snapshot.worker_alive
        or notifier_snapshot.worker_starting
        or getattr(notifier_snapshot, "retiring_worker", False)
        or field(janitor_snapshot, "worker_alive", False)
        or field(janitor_snapshot, "worker_starting", False)
        or terminal_hosts_remaining
        or native_hosts_remaining
        or retirement_debts
    )
    retry_work_drained = not (
        field(retry_snapshot, "pending_retries")
        or field(retry_snapshot, "ready_retries")
        or field(retry_snapshot, "active_retries")
        or field(retry_snapshot, "successor_retries")
        or field(retry_snapshot, "emergency_retries")
    )
    cleanup_work_drained = not (
        field(cleanup_snapshot, "owned_calls")
        or field(cleanup_snapshot, "active_calls")
        or field(cleanup_snapshot, "dead_letter_calls")
        or field(cleanup_snapshot, "parked_calls")
        or field(janitor_snapshot, "pending_artifacts")
    )
    owners_released = not (
        field(guardian_snapshot, "pending_owners")
        or field(guardian_snapshot, "dead_letter_owners")
        or field(guardian_snapshot, "parked_owners")
    )
    admission_closed = (
        async_snapshot.admission_closed
        and thread_snapshot.admission_closed
        and fd_snapshot.admission_closed
        and guardian_thread_snapshot.admission_closed
        and notifier_thread_snapshot.admission_closed
        and thread_snapshot.teardown_admission_closed
        and fd_snapshot.teardown_admission_closed
        and service_snapshot.admission_closed
        and (
            remote_io_authoritative is None
            or bool(getattr(remote_io_authoritative, "admission_closed", False))
        )
    )
    services_quiescent = remaining_services == 0
    native_fd_reserved = (
        int(native_fd_snapshot.get("reserved", 0) or 0)
        if isinstance(native_fd_snapshot, dict)
        else 0
    )
    native_fd_opened = (
        int(native_fd_snapshot.get("opened", 0) or 0) if isinstance(native_fd_snapshot, dict) else 0
    )
    resources_drained = not (
        (not async_scheduler_quiescent)
        or async_snapshot.active_operations
        or async_snapshot.terminal_debts
        or remaining_services
        or thread_snapshot.in_use
        or fd_snapshot.in_use
        or native_fd_reserved
        or native_fd_opened
        or uncertain_fd_snapshot.debts
        or guardian_thread_snapshot.in_use
        or notifier_thread_snapshot.in_use
        or notifier_snapshot.pending_callbacks
        or notifier_snapshot.delayed_callbacks
        or notifier_snapshot.parked_callbacks
        or notifier_snapshot.failed_worker_leases
        or terminal_hosts_remaining
        or native_hosts_remaining
        or terminal_owners_remaining
        or retirement_debts
        or generic_finalizer_published
        or prepared_finalizer_active
        or finalizer_admission_active
        or memory_finalizer_reserved
        or memory_finalizer_published
        or storage_finalizer_reserved
        or storage_finalizer_published
        or operation_finalizer_reserved
        or operation_finalizer_published
        or path_finalizer_published
        or lookahead_finalizer_published
        or temporary_logical_bytes
        or temporary_logical_inodes
        or temporary_cross_bytes
        or temporary_cross_inodes
        or cross_logical_contributions
        or cross_logical_bytes
        or cross_physical_bytes
        or cross_direct_live_bytes
        or cross_deferred_finalizers
        or remote_io_in_use
        or remote_io_waiting
        or remote_io_sync_waiters
        or remote_io_protocol_violations
        or remote_io_permits
        or remote_io_submissions
        or remote_io_submission_capabilities
        or remote_io_registrations
        or remote_io_capacity_capabilities
        or provider_active_leases
        or external_runtime_physical_claims
        or external_runtime_logical_claims
        or external_runtime_physical_permits
        or external_runtime_logical_width
        or external_runtime_configuration_inflight
        or external_runtime_configuration_uncertain
        # Resident-only pools are allowed to survive shutdown: their threads
        # belong to process-global third-party runtimes, not schema-sanitizer
        # operation ownership. Active claims/permits above must be zero.
        or control_plane_active
        or control_plane_reserved
        or not finalizer_quiescent
        or not retry_work_drained
        or not cleanup_work_drained
        or not owners_released
    )
    deadline_exhausted = monotonic_ns() >= deadline_ns
    terminal_success = all(
        (
            admission_closed,
            services_quiescent,
            retry_stopped,
            janitor_stopped,
            dispatcher_stopped,
            guardian_stopped,
            notifier_stopped,
            native_reaper_stopped,
            workers_stopped,
            resources_drained,
            observability_complete,
            finalizer_quiescent,
            not deadline_exhausted,
        )
    )
    return ConcurrencyShutdownResult(
        retry_stopped,
        janitor_stopped,
        dispatcher_stopped,
        guardian_stopped,
        deadline_exhausted,
        services_quiescent,
        remaining_services,
        thread_snapshot.in_use + guardian_thread_snapshot.in_use + notifier_thread_snapshot.in_use,
        max(fd_snapshot.in_use, native_fd_reserved, native_fd_opened),
        admission_closed,
        resources_drained,
        parked_owners,
        workers_stopped,
        services_quiescent,
        retry_work_drained,
        cleanup_work_drained,
        owners_released,
        terminal_success,
        generation,
        notifier_stopped,
        native_reaper_stopped,
        observability_complete,
        observability_failures.freeze(),
        terminal_owners_remaining,
        terminal_retained_bytes,
        terminal_publication_rejections,
        finalizer_quiescent,
        finalizer_redrain_rounds,
    )


def shutdown_concurrency_runtime(*, deadline_seconds: float = 5.0) -> ConcurrencyShutdownResult:
    """Execute one terminal shutdown; concurrent callers share its result."""
    ensure_runtime_fork_safe()
    try:
        deadline_ns = deadline_ns_from_timeout(
            deadline_seconds, name="concurrency runtime shutdown deadline"
        )
    except (TypeError, ValueError, OverflowError):
        raise ValueError("shutdown deadline must be finite and non-negative") from None

    global _SHUTDOWN_IN_PROGRESS, _SHUTDOWN_OWNER
    global _SHUTDOWN_RESULT, _SHUTDOWN_ERROR, _SHUTDOWN_GENERATION
    caller = threading.get_ident()
    with _SHUTDOWN_CONDITION:
        if _SHUTDOWN_RESULT is not None and _SHUTDOWN_RESULT.terminal_success:
            return _SHUTDOWN_RESULT
        if _SHUTDOWN_RESULT is not None:
            _SHUTDOWN_RESULT = None
        if _SHUTDOWN_ERROR is not None:
            shared_shutdown_error = _SHUTDOWN_ERROR
            _SHUTDOWN_ERROR = None
            _raise_shared_shutdown_error(shared_shutdown_error)
        if _SHUTDOWN_IN_PROGRESS:
            if _SHUTDOWN_OWNER == caller:
                raise RuntimeError("concurrency runtime shutdown is not reentrant")
            return _wait_for_shared_shutdown(deadline_ns)
        _SHUTDOWN_IN_PROGRESS = True
        _SHUTDOWN_OWNER = caller
        _SHUTDOWN_GENERATION += 1
        generation = _SHUTDOWN_GENERATION

    error: BaseException | None = None
    result: ConcurrencyShutdownResult | None = None
    try:
        result = _perform_shutdown(deadline_ns, generation)
    except BaseException as exc:
        error = exc
        clear_exception_traceback(exc)
    finally:
        with _SHUTDOWN_CONDITION:
            _SHUTDOWN_IN_PROGRESS = False
            _SHUTDOWN_OWNER = None
            _SHUTDOWN_RESULT = result
            _SHUTDOWN_ERROR = error
            _SHUTDOWN_CONDITION.notify_all()
    if error is not None:
        _raise_shared_shutdown_error(error)
    assert result is not None
    return result


def _reset_runtime_shutdown_for_tests() -> None:
    """Reset only the terminal admission/result gate for isolated unit tests."""
    from .async_scheduler import reopen_async_scheduler_for_tests
    from .finalizer_registry import _reset_finalizer_registry_for_tests
    from .process_resources import _reopen_process_resource_admission_for_tests
    from .shutdown_observers import _reset_shutdown_observers_for_tests
    from .static_control_plane import _reset_static_control_plane_for_tests

    global _SHUTDOWN_IN_PROGRESS, _SHUTDOWN_OWNER, _SHUTDOWN_RESULT, _SHUTDOWN_ERROR
    with _SHUTDOWN_CONDITION:
        _SHUTDOWN_IN_PROGRESS = False
        _SHUTDOWN_OWNER = None
        _SHUTDOWN_RESULT = None
        _SHUTDOWN_ERROR = None
        _SHUTDOWN_CONDITION.notify_all()
    _reset_finalizer_registry_for_tests()
    _reset_shutdown_observers_for_tests()
    _reset_static_control_plane_for_tests()
    _RUNTIME_SERVICES.reopen_admission_for_tests()
    _reopen_process_resource_admission_for_tests()
    reopen_async_scheduler_for_tests()
    remote_io_module = sys.modules.get("schema_sanitizer.remote_impl.io_permits")
    if remote_io_module is not None:
        reopen_remote_io = getattr(
            remote_io_module, "_reopen_remote_io_permit_admission_for_tests", None
        )
        if callable(reopen_remote_io):
            reopen_remote_io()


def _prepare_runtime_shutdown_for_fork() -> None:
    """Prepare runtime shutdown for fork."""
    global _SHUTDOWN_FORK_FRESH_CONDITION
    _SHUTDOWN_FORK_FRESH_CONDITION = _SHUTDOWN_CHILD_CONDITIONS[_SHUTDOWN_CHILD_CONDITION_INDEX]


def _clear_runtime_shutdown_fork_preparation() -> None:
    """Clear runtime shutdown fork preparation."""
    global _SHUTDOWN_FORK_FRESH_CONDITION
    _SHUTDOWN_FORK_FRESH_CONDITION = None


def _reset_runtime_shutdown_after_fork() -> None:
    """Reset runtime shutdown after fork."""
    global _SHUTDOWN_CONDITION, _SHUTDOWN_FORK_FRESH_CONDITION, _SHUTDOWN_CHILD_CONDITION_INDEX
    global _SHUTDOWN_IN_PROGRESS, _SHUTDOWN_OWNER, _SHUTDOWN_RESULT, _SHUTDOWN_ERROR
    prepared = _SHUTDOWN_FORK_FRESH_CONDITION
    if prepared is None:
        return
    quarantine_inherited_state(
        "runtime-shutdown",
        _SHUTDOWN_CONDITION,
        _SHUTDOWN_OWNER,
        _SHUTDOWN_RESULT,
        _SHUTDOWN_ERROR,
    )
    _SHUTDOWN_CONDITION = prepared
    _SHUTDOWN_CHILD_CONDITION_INDEX = 1 - _SHUTDOWN_CHILD_CONDITION_INDEX
    _SHUTDOWN_FORK_FRESH_CONDITION = None
    _SHUTDOWN_IN_PROGRESS = False
    _SHUTDOWN_OWNER = None
    _SHUTDOWN_RESULT = None
    _SHUTDOWN_ERROR = None


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "runtime-shutdown",
    before=_prepare_runtime_shutdown_for_fork,
    after_in_parent=_clear_runtime_shutdown_fork_preparation,
    after_in_child=_reset_runtime_shutdown_after_fork,
)


__all__ = ["ConcurrencyShutdownResult", "shutdown_concurrency_runtime"]
