"""Protects privileged notifier and cleanup paths with terminal close, event-retry
acknowledgements, guardian permits, per-item scheduler transfer, quarantine roots,
seqlock diagnostics, bounded registries, fork capsules, and native reaper lanes. Caller
identity cannot be spoofed through callback metadata; only explicit subsystems publish
work, and every post-fork owner uses the single bounded capsule."""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS


def _delivery(module: object, governor: object, event: object) -> object:
    """Deliver the notifier callback under the spoofed caller context."""
    del module
    return governor._availability_events[event]


def test_notifier_close_is_terminal_and_rejects_late_publication() -> None:
    """Verify notifier close is terminal and rejects late publication."""
    from schema_sanitizer.core_impl import process_resources as module

    notifier = module._AvailabilityNotifier()
    governor = module._Governor(
        1, "spoofed-module-cannot-enter-privileged-notifier-terminal-notifier"
    )
    assert governor.register_availability_event(module.AvailabilityEvent.RETRY_SCHEDULER)
    delivery = _delivery(module, governor, module.AvailabilityEvent.RETRY_SCHEDULER)
    assert notifier.close(deadline_seconds=0.2)
    assert notifier.snapshot().lifecycle_state == "STOPPED"
    assert not notifier.publish_one(delivery)
    assert notifier.snapshot().worker_alive is False


def test_notifier_retries_failed_event_and_acks_only_after_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify notifier retries failed event and acks only after success."""
    from schema_sanitizer.core_impl import process_resources as module

    notifier = module._AvailabilityNotifier()
    local_threads = module._Governor(
        1, "spoofed-module-cannot-enter-privileged-notifier-notifier-thread"
    )
    monkeypatch.setattr(module, "_NOTIFIER_THREAD_GOVERNOR", local_threads)
    attempts = 0
    completed = threading.Event()

    def dispatch(event: module.AvailabilityEvent) -> None:
        """Dispatch work through the controlled scheduling path."""
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient")
        assert event is module.AvailabilityEvent.RETRY_SCHEDULER
        completed.set()

    governor = module._Governor(
        1, "spoofed-module-cannot-enter-privileged-notifier-ack", availability_dispatcher=dispatch
    )
    assert governor.register_availability_event(module.AvailabilityEvent.RETRY_SCHEDULER)
    delivery = _delivery(module, governor, module.AvailabilityEvent.RETRY_SCHEDULER)
    assert notifier.publish_one(delivery)
    assert completed.wait(SCHEDULER_TIMEOUT_SECONDS)
    deadline = time.monotonic() + SCHEDULER_TIMEOUT_SECONDS
    while governor.snapshot().availability_callbacks and time.monotonic() < deadline:
        time.sleep(0.01)
    assert attempts == 2
    assert governor.snapshot().availability_callbacks == 0
    assert notifier.close(deadline_seconds=1.0)


def test_guardian_worker_permits_are_never_recursively_adopted() -> None:
    """Verify guardian worker permits are never recursively adopted."""
    from schema_sanitizer.core_impl.retry_scheduler import _ReleaseGuardian

    class Lease:
        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.attempts = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.attempts += 1
            raise RuntimeError("still held")

    guardian = _ReleaseGuardian()
    first = Lease()
    second = Lease()
    guardian._release_worker_lease(first)
    guardian._release_worker_lease(second)
    guardian._ensure_workers()
    snapshot = guardian.snapshot()
    assert snapshot.failed_worker_leases == 2
    assert snapshot.active_workers == 0
    assert first.attempts >= 2
    assert second.attempts >= 2


def test_guardian_rejects_its_exact_bootstrap_lease() -> None:
    """Verify guardian rejects its exact bootstrap lease."""
    from schema_sanitizer.core_impl import process_resources
    from schema_sanitizer.core_impl.retry_scheduler import _ReleaseGuardian

    # Recognition is a type/capability contract and must not depend on a free
    # slot in the process-global emergency pool left by unrelated work.
    lease = process_resources._Lease(
        process_resources._GUARDIAN_THREAD_GOVERNOR,
        1,
        _active=False,
    )
    assert not _ReleaseGuardian().adopt(lease)


def test_scheduler_failed_lease_transfer_is_per_item_transactional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify scheduler failed lease transfer is per item transactional."""
    from schema_sanitizer.core_impl import retry_scheduler as module

    scheduler = module._RetryScheduler()
    first = object()
    second = object()
    with scheduler._condition:
        scheduler._failed_worker_leases.extend((first, second))

    def fail(*_args: object, **_kwargs: object) -> bool:
        """Raise the deliberate failure injected by the test."""
        raise MemoryError("guardian unavailable")

    monkeypatch.setattr(module, "adopt_failed_release", fail)
    scheduler._release_failed_leases()
    with scheduler._condition:
        assert tuple(scheduler._failed_worker_leases) == (first, second)


@pytest.mark.skipif(os.name == "nt", reason="POSIX quarantine ownership descriptor required")
def test_quarantine_root_owner_survives_guardian_rejection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Verify quarantine root owner survives guardian rejection."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    class Lease:
        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.fail = True

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            if self.fail:
                raise RuntimeError("lease busy")

    descriptor = os.open(tmp_path, os.O_RDONLY)
    metadata = os.fstat(descriptor)
    lease = Lease()
    handle = module._QuarantineRootHandle(
        tmp_path, descriptor, lease, metadata.st_dev, metadata.st_ino, os.getpid()
    )
    monkeypatch.setattr(module, "_ROOT_HANDLE", handle)
    monkeypatch.setattr(module, "_RETIRED_ROOT_HANDLES", [])
    monkeypatch.setattr(module, "_CLOSING_ROOT_OWNERS", module.deque())

    def reject(*_args: object, **_kwargs: object) -> bool:
        """Raise the deliberate failure for the reject path."""
        raise MemoryError("guardian full")

    monkeypatch.setattr(module, "adopt_failed_release", reject)
    assert not module._close_root_handle()
    assert len(module._CLOSING_ROOT_OWNERS) == 1
    lease.fail = False
    assert module._close_root_handle()
    assert not module._CLOSING_ROOT_OWNERS


def test_diagnostic_epoch_remains_odd_until_last_writer_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify diagnostic epoch remains odd until last writer exits."""
    from schema_sanitizer.core_impl import diagnostic_epoch as module

    monkeypatch.setattr(module, "_EPOCH", 0)
    monkeypatch.setattr(module, "_ACTIVE_WRITERS", 0)
    module.diagnostic_write_begin()
    module.diagnostic_write_begin()
    module.diagnostic_transition()
    assert module.diagnostic_epoch() & 1
    module.diagnostic_write_end()
    assert module.diagnostic_epoch() & 1
    module.diagnostic_write_end()
    assert module.diagnostic_epoch() % 2 == 0


def test_runtime_registry_is_bounded_and_opens_circuit() -> None:
    """Verify runtime registry is bounded and opens circuit."""
    from schema_sanitizer.core_impl.runtime_registry import _RuntimeServiceRegistry

    class Service:
        def close(self, *, deadline_seconds: float = 0.0) -> bool:
            """Close the resources owned by the service test double."""
            return True

    registry = _RuntimeServiceRegistry()
    registry._capacity = 2
    first = registry.reserve(Service(), kind="a", close_name="close")
    second = registry.reserve(Service(), kind="b", close_name="close")
    with pytest.raises(RuntimeError, match="capacity"):
        registry.reserve(Service(), kind="c", close_name="close")
    snapshot = registry.snapshot()
    assert snapshot.registered_services == 2
    assert snapshot.circuit_open
    assert snapshot.rejected_services == 1
    first.close()
    second.close()


def test_cleanup_subsystem_is_explicit_not_callback_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cleanup subsystem is explicit not callback metadata."""
    from schema_sanitizer.core_impl.cleanup_dispatcher import (
        CleanupSubsystem,
        _CleanupDispatcher,
    )

    dispatcher = _CleanupDispatcher()
    monkeypatch.setattr(dispatcher, "_ensure_workers", lambda: None)

    def callback() -> None:
        """Return no value without exposing callback metadata."""
        return None

    callback.__module__ = "schema_sanitizer.fake_many_callbacks"
    callback.__qualname__ = "forged"
    assert dispatcher.submit(callback)
    with dispatcher._condition:
        assert tuple(dispatcher._queues) == (CleanupSubsystem.GENERIC,)


def test_terminal_host_markers_are_bounded_without_retaining_owners() -> None:
    """Verify terminal host markers are bounded without retaining owners."""
    from schema_sanitizer.core_impl.terminal_hosts import TerminalHostMarkers

    markers = TerminalHostMarkers(2)
    owners = [object(), object(), object()]
    assert markers.add(owners[0])
    assert markers.add(owners[1])
    assert not markers.add(owners[2])
    snapshot = markers.snapshot()
    assert snapshot.hosts == 2
    assert snapshot.circuit_open
    assert snapshot.rejected == 1


def test_fork_capsule_is_single_and_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify fork capsule is single and bounded."""
    from schema_sanitizer.core_impl import fork_safety as module

    monkeypatch.setattr(module, "_FORK_GENERATION", 1)
    monkeypatch.setattr(module, "_MAX_FORK_CAPSULE_ENTRIES", 2)
    monkeypatch.setattr(module, "_FORK_CAPSULE_COUNTS", [0, 0])
    monkeypatch.setattr(module, "_FORK_CAPSULE_COUNT", 0)
    monkeypatch.setattr(module, "_FORK_LABELS", [None] * 4)
    monkeypatch.setattr(module, "_FORK_OWNERS", [None] * 32)
    monkeypatch.setattr(module, "_REJECTED_FORK_CAPSULE_ENTRIES", 0)
    monkeypatch.setattr(module, "_REJECTED_FORK_CAPSULE_OVERFLOWED", False)
    assert module.quarantine_inherited_state("a", object())
    assert module.quarantine_inherited_state("b", object())
    assert not module.quarantine_inherited_state("c", object())
    snapshot = module.fork_inherited_capsule_snapshot()
    assert snapshot == {"entries": 2, "capacity": 4, "rejected": 1, "generation": 1}


def test_native_reaper_reserves_lane_before_start_and_promotes_parking() -> None:
    """Verify native reaper reserves lane before start and promotes parking."""
    source = Path("cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    header = Path("cpp/src/internal/runtime/operation_task_arena.hh").read_text()
    abi = Path("cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc").read_text()
    assert "EnsureLaneStarted" in source
    assert "TryAcquireReaperThreadPermit" in source
    assert "PromoteParked(index)" in source
    assert "kLaneCount * kMaxQueuedStates" in source
    assert "reaper_thread_start_failures" in header
    assert "PyTuple_New(30)" in abi
    shutdown = source[source.index("void OperationTaskArena::Shutdown() noexcept") :]
    assert "Every accepted arena reserved" in shutdown
    assert (
        "slot->abandoned_tasks.clear();"
        not in shutdown[
            shutdown.index("if (!ArenaCleanupReaper::Instance().Park(state))") : shutdown.index(
                "for (auto &slot : state->slots) {",
                shutdown.index("if (!ArenaCleanupReaper::Instance().Park(state))"),
            )
        ]
    )


def test_native_accounting_uses_saturating_subtraction_for_retained_bytes() -> None:
    """Verify native accounting uses saturating subtraction for retained bytes."""
    source = Path("cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    runtime = Path("cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text()
    assert "retained_bytes_total.fetch_sub" not in source
    assert "queued_bytes.fetch_sub" not in source
    assert "queued_bytes.fetch_sub" not in runtime
    assert "SaturatingAtomicSubtract(state_->retained_bytes_total, bytes_)" in runtime


def test_runtime_registry_reopens_circuit_after_capacity_drains() -> None:
    """Verify runtime registry reopens circuit after capacity drains."""
    from schema_sanitizer.core_impl.runtime_registry import _RuntimeServiceRegistry

    class Service:
        def close(self, *, deadline_seconds: float = 0.0) -> bool:
            """Close the resources owned by the service test double."""
            return True

    registry = _RuntimeServiceRegistry()
    registry._capacity = 1
    registration = registry.reserve(Service(), kind="one", close_name="close")
    with pytest.raises(RuntimeError, match="capacity"):
        registry.reserve(Service(), kind="overflow", close_name="close")
    assert registry.snapshot().circuit_open
    registration.close()
    assert not registry.snapshot().circuit_open
    replacement = registry.reserve(Service(), kind="replacement", close_name="close")
    replacement.close()


def test_all_post_fork_owners_use_single_capsule() -> None:
    """Verify all post fork owners use single capsule."""
    root = Path("src/schema_sanitizer")
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        text = path.read_text()
        if "_FORKED_" in text and "KEEPALIVE.append" in text:
            offenders.append(str(path))
    assert offenders == []


def test_shutdown_accounts_for_terminal_notifier_hosts_and_native_states() -> None:
    """Verify shutdown accounts for terminal notifier hosts and native states."""
    source = Path("src/schema_sanitizer/core_impl/runtime_shutdown.py").read_text()
    assert "notifier_snapshot.delayed_callbacks" in source
    assert "notifier_snapshot.parked_callbacks" in source
    assert "terminal_hosts_remaining" in source
    assert "native_hosts_remaining" in source
    native = Path("cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    promote = native[native.index("void PromoteParked") : native.index("const bool enabled_")]
    assert "for (std::size_t i = 0; i < parked_.size(); ++i)" in promote
