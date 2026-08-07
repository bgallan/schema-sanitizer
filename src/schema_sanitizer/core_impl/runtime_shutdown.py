"""Single-flight phased shutdown for process-wide concurrency services."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from time import monotonic_ns

from .durations import deadline_ns_from_timeout, remaining_seconds
from .fork_safety import ensure_runtime_fork_safe
from .safe_errors import clear_exception_traceback


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

    @property
    def stopped(self) -> bool:
        return self.terminal_success


_SHUTDOWN_CONDITION = threading.Condition()
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
    try:
        from .process_resources import (
            process_file_descriptor_snapshot,
            process_thread_snapshot,
        )
        from .runtime_registry import runtime_service_snapshot

        admission_closed = bool(
            process_thread_snapshot().admission_closed
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
    try:
        from schema_sanitizer import _core_abi3 as native
    except ImportError:
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
    from .cleanup_dispatcher import _DISPATCHER
    from .process_resources import (
        availability_notifier_snapshot,
        availability_notifier_thread_snapshot,
        close_process_resource_admission,
        close_process_resource_external_admission,
        close_release_guardian_thread_admission,
        process_file_descriptor_snapshot,
        process_thread_snapshot,
        release_guardian_thread_snapshot,
        shutdown_availability_notifier,
        uncertain_fd_close_snapshot,
    )
    from .retry_scheduler import _RELEASE_GUARDIAN, _SCHEDULER
    from .runtime_registry import _RUNTIME_SERVICES, RuntimeServicePhase
    from .temporary_janitor import _JANITOR

    # Phase 1: stop external producers, but keep the dedicated teardown
    # reserve open.  Cleanup workers and descriptor-relative janitor work may
    # still need threads/FDs to complete the shutdown.
    _RUNTIME_SERVICES.close_admission()
    close_process_resource_external_admission()

    # Phase 2: close ordinary producers while cleanup/retry consumers live.
    _RUNTIME_SERVICES.close_all(deadline_ns=deadline_ns, phase=RuntimeServicePhase.PRODUCER)

    # Phase 3: drain registered cleanup producers and internal cleanup while
    # the retry scheduler remains available.
    _RUNTIME_SERVICES.close_all(deadline_ns=deadline_ns, phase=RuntimeServicePhase.CLEANUP_PRODUCER)
    janitor_stopped = _JANITOR.close(deadline_seconds=remaining_seconds(deadline_ns))
    dispatcher_stopped = _DISPATCHER.close(deadline_seconds=remaining_seconds(deadline_ns))

    # Phase 4: no producer may now create a new delayed retry.
    retry_stopped = _SCHEDULER.close(deadline_seconds=remaining_seconds(deadline_ns))

    # Phase 5: close any registered consumers that depended on producers.
    _RUNTIME_SERVICES.close_all(deadline_ns=deadline_ns, phase=RuntimeServicePhase.CONSUMER)

    # Phase 6: release ownership is the final consumer to stop.
    guardian_stopped = _RELEASE_GUARDIAN.close(deadline_seconds=remaining_seconds(deadline_ns))
    # Availability hooks can be published while the final leases are returned.
    # Drain that host before closing its dedicated admission.
    notifier_stopped = shutdown_availability_notifier(
        deadline_seconds=remaining_seconds(deadline_ns)
    )
    # The emergency budgets remain available until their hosts are quiescent.
    close_release_guardian_thread_admission()

    native_reaper_stopped = _shutdown_native_cleanup_reaper(deadline_ns)

    # Only after every cleanup host has had a chance to drain do we close the
    # internal teardown reserve.  This turns the admission split into a strict
    # two-phase commit instead of starving shutdown itself.
    close_process_resource_admission()

    def snapshot_or_none(service: object) -> object | None:
        snapshot = getattr(service, "snapshot", None)
        if not callable(snapshot):
            return None
        try:
            return snapshot()
        except Exception:
            return None

    thread_snapshot = process_thread_snapshot()
    guardian_thread_snapshot = release_guardian_thread_snapshot()
    notifier_thread_snapshot = availability_notifier_thread_snapshot()
    notifier_snapshot = availability_notifier_snapshot()
    fd_snapshot = process_file_descriptor_snapshot()
    uncertain_fd_snapshot = uncertain_fd_close_snapshot()
    retry_snapshot = snapshot_or_none(_SCHEDULER)
    cleanup_snapshot = snapshot_or_none(_DISPATCHER)
    janitor_snapshot = snapshot_or_none(_JANITOR)
    guardian_snapshot = snapshot_or_none(_RELEASE_GUARDIAN)
    service_snapshot = _RUNTIME_SERVICES.snapshot()
    try:
        from .runtime_diagnostics import _native_arena_snapshot

        native_snapshot = _native_arena_snapshot()
    except Exception:
        native_snapshot = {"available": False, "snapshot_failed": True}
    try:
        from ..remote_impl import async_bridge, io_coordinator

        failed_bridge_snapshot = async_bridge._FAILED_BRIDGE_RUNNERS.snapshot()
        orphaned_startup_snapshot = io_coordinator._ORPHANED_STARTUPS.snapshot()
    except Exception:
        failed_bridge_snapshot = None
        orphaned_startup_snapshot = None

    def field(snapshot: object | None, name: str, default: int | bool = 0):
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
        if native_snapshot.get("available")
        else 0
    )
    workers_stopped = not (
        field(retry_snapshot, "worker_alive", False)
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
        thread_snapshot.admission_closed
        and fd_snapshot.admission_closed
        and guardian_thread_snapshot.admission_closed
        and notifier_thread_snapshot.admission_closed
        and thread_snapshot.teardown_admission_closed
        and fd_snapshot.teardown_admission_closed
        and service_snapshot.admission_closed
    )
    services_quiescent = remaining_services == 0
    resources_drained = not (
        remaining_services
        or thread_snapshot.in_use
        or fd_snapshot.in_use
        or uncertain_fd_snapshot.debts
        or guardian_thread_snapshot.in_use
        or notifier_thread_snapshot.in_use
        or notifier_snapshot.pending_callbacks
        or notifier_snapshot.delayed_callbacks
        or notifier_snapshot.parked_callbacks
        or notifier_snapshot.failed_worker_leases
        or terminal_hosts_remaining
        or native_hosts_remaining
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
        fd_snapshot.in_use,
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
    from .process_resources import _reopen_process_resource_admission_for_tests
    from .runtime_registry import _RUNTIME_SERVICES

    global _SHUTDOWN_IN_PROGRESS, _SHUTDOWN_OWNER, _SHUTDOWN_RESULT, _SHUTDOWN_ERROR
    with _SHUTDOWN_CONDITION:
        _SHUTDOWN_IN_PROGRESS = False
        _SHUTDOWN_OWNER = None
        _SHUTDOWN_RESULT = None
        _SHUTDOWN_ERROR = None
        _SHUTDOWN_CONDITION.notify_all()
    _RUNTIME_SERVICES.reopen_admission_for_tests()
    _reopen_process_resource_admission_for_tests()


__all__ = ["ConcurrencyShutdownResult", "shutdown_concurrency_runtime"]
