"""Produce bounded, callback-free diagnostics for concurrency support and watchdogs.

Native arena metrics and Python service snapshots are combined into one debug record without
executing user callbacks or exposing unbounded retained state.
"""

from __future__ import annotations

from dataclasses import asdict
from time import monotonic_ns
from typing import Any

_MAX_AGE_NS = (1 << 63) - 1


def _age(now: int, then: int) -> int:
    """Return the elapsed age for the supplied timestamp."""
    if then <= 0 or then > now:
        return 0
    return min(_MAX_AGE_NS, now - then)


def _native_arena_snapshot() -> dict[str, int | bool]:
    """Return a bounded snapshot of native arena."""
    try:
        from schema_sanitizer import _core_abi3 as native
    except ImportError:
        return {"available": False}
    method = getattr(native, "operation_task_arena_runtime_snapshot", None)
    if not callable(method):
        return {"available": False}
    try:
        values = tuple(method())
    except Exception:
        return {"available": True, "snapshot_failed": True}
    if len(values) != 30:
        return {"available": True, "snapshot_failed": True}
    names = (
        "live_arenas",
        "detached_workers",
        "reaper_workers",
        "reaper_queued_states",
        "reaper_active_states",
        "reaper_reserved_states",
        "reaper_parked_states",
        "counter_underflows",
        "reaper_queued_bytes",
        "reaper_active_bytes",
        "reaper_reserved_bytes",
        "reaper_parked_bytes",
        "oldest_parked_since_ns",
        "reaper_thread_permits",
        "reaper_thread_start_failures",
        "reaper_over_capacity",
        "reaper_terminal_states",
        "reaper_terminal_bytes",
        "oldest_terminal_since_ns",
        "reaper_stopping_lanes",
        "native_physical_threads",
        "native_physical_thread_capacity",
        "native_physical_thread_rejections",
        "external_runtime_thread_permits",
        "completion_memory_protocol_violations",
        "total_physical_thread_permits",
        "external_runtime_resident_threads",
        "thread_permit_snapshot_stable",
        "external_runtime_resident_protocol_violations",
        "external_runtime_stack_debt_threads",
    )
    selected = names[: len(values)]
    return {
        "available": True,
        "snapshot_schema_fields": len(values),
        **dict(zip(selected, map(int, values), strict=True)),
    }


def concurrency_runtime_debug_snapshot() -> dict[str, Any]:
    """Return a bounded cross-component snapshot with optimistic consistency."""
    from . import path_identity, retry_scheduler
    from .cleanup_dispatcher import cleanup_dispatcher_snapshot
    from .diagnostic_epoch import diagnostic_epoch
    from .fork_safety import fork_inherited_capsule_snapshot, runtime_fork_poisoned
    from .process_resources import (
        availability_notifier_snapshot,
        availability_notifier_thread_snapshot,
        external_runtime_pool_snapshot,
        native_file_descriptor_snapshot,
        process_file_descriptor_snapshot,
        process_thread_snapshot,
        release_guardian_thread_snapshot,
        uncertain_fd_close_snapshot,
    )
    from .runtime_registry import runtime_service_snapshot
    from .temporary_janitor import temporary_janitor_snapshot

    def capture() -> tuple[Any, ...]:
        """Capture the current bounded runtime diagnostics."""
        try:
            from ..remote_impl import async_bridge, io_coordinator

            terminal_hosts: dict[str, Any] = {
                "async_bridges": asdict(async_bridge._FAILED_BRIDGE_RUNNERS.snapshot()),
                "remote_startups": asdict(io_coordinator._ORPHANED_STARTUPS.snapshot()),
            }
        except Exception:
            terminal_hosts = {"snapshot_failed": True}
        return (
            retry_scheduler.retry_scheduler_snapshot(),
            retry_scheduler.release_guardian_snapshot(),
            cleanup_dispatcher_snapshot(),
            temporary_janitor_snapshot(),
            process_thread_snapshot(),
            external_runtime_pool_snapshot(),
            process_file_descriptor_snapshot(),
            native_file_descriptor_snapshot(),
            release_guardian_thread_snapshot(),
            availability_notifier_thread_snapshot(),
            availability_notifier_snapshot(),
            runtime_service_snapshot(),
            _native_arena_snapshot(),
            terminal_hosts,
            fork_inherited_capsule_snapshot(),
            uncertain_fd_close_snapshot(),
        )

    first_epoch = diagnostic_epoch()
    first = capture()
    consistent = False
    final_epoch = first_epoch
    for _attempt in range(3):
        second = capture()
        final_epoch = diagnostic_epoch()
        if not (first_epoch & 1) and first_epoch == final_epoch and first == second:
            consistent = True
            first = second
            break
        first_epoch = final_epoch
        first = second
    (
        retry,
        guardian,
        cleanup,
        janitor,
        threads,
        external_pools,
        descriptors,
        native_descriptors,
        guardian_threads,
        notifier_threads,
        notifier,
        services,
        native_arena,
        terminal_hosts,
        fork_capsule,
        uncertain_fd_closes,
    ) = first
    now = monotonic_ns()
    capture_epoch = (
        f"{final_epoch}:{retry.progress_epoch}:{guardian.progress_epoch}:"
        f"{cleanup.progress_epoch}:{services.progress_epoch}"
    )
    common = {
        "capture_epoch": capture_epoch,
        "consistent": consistent,
        "fork_quarantine_generations": retry_scheduler._FORKED_RETRY_GENERATIONS,
    }

    return {
        "version": 8,
        "captured_at_monotonic_ns": now,
        "fork_poisoned": runtime_fork_poisoned(),
        "fork_inherited_capsule": fork_capsule,
        "retry_scheduler": {
            **asdict(retry),
            **common,
            "path_fork_quarantine_generations": path_identity._FORKED_PATH_GENERATIONS,
            "progress_age_ns": _age(now, retry.last_progress_ns),
        },
        "release_guardian": {
            **asdict(guardian),
            **common,
            "progress_age_ns": _age(now, guardian.last_progress_ns),
        },
        "cleanup_dispatcher": asdict(cleanup),
        "temporary_janitor": asdict(janitor),
        "process_threads": asdict(threads),
        "external_runtime_pools": external_pools,
        "process_file_descriptors": asdict(descriptors),
        "native_process_file_descriptors": native_descriptors,
        "uncertain_fd_closes": asdict(uncertain_fd_closes),
        "release_guardian_threads": asdict(guardian_threads),
        "availability_notifier_threads": asdict(notifier_threads),
        "availability_notifier": asdict(notifier),
        "runtime_services": {
            **asdict(services),
            "progress_age_ns": _age(now, services.last_progress_ns),
        },
        "native_operation_arenas": native_arena,
        "terminal_runtime_hosts": terminal_hosts,
    }


from .shutdown_observers import (  # noqa: E402
    register_shutdown_observer as _register_shutdown_observer,
)

_register_shutdown_observer("native_arena", _native_arena_snapshot)


__all__ = [
    "concurrency_runtime_debug_snapshot",
]
