"""Regression coverage for memory guardian close never requeues active owner."""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS


def test_guardian_close_never_requeues_active_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import retry_scheduler as module

    class Permit:
        def release(self) -> None:
            return None

    monkeypatch.setattr(module, "acquire_release_guardian_thread", lambda: Permit())
    guardian = module._ReleaseGuardian()
    close_wait_entered = threading.Event()
    close_thread: threading.Thread | None = None

    class CloseObservedCondition(threading.Condition):
        def wait(self, timeout: float | None = None) -> bool:
            if threading.current_thread() is close_thread:
                close_wait_entered.set()
            return super().wait(timeout)

    guardian._condition = CloseObservedCondition()
    entered = threading.Event()
    resume = threading.Event()
    calls = 0
    concurrent = 0
    peak = 0
    lock = threading.Lock()

    class BlockingOwner:
        def release(self) -> None:
            nonlocal calls, concurrent, peak
            with lock:
                calls += 1
                concurrent += 1
                peak = max(peak, concurrent)
            entered.set()
            assert resume.wait(SCHEDULER_TIMEOUT_SECONDS)
            with lock:
                concurrent -= 1

    class QuickOwner:
        def release(self) -> None:
            return None

    owner = BlockingOwner()
    assert guardian.adopt(owner)
    assert guardian.adopt(QuickOwner())
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    result: list[bool] = []
    close_thread = threading.Thread(
        target=lambda: result.append(guardian.close(deadline_seconds=SCHEDULER_TIMEOUT_SECONDS))
    )
    close_thread.start()
    assert close_wait_entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert calls == 1
    assert peak == 1
    resume.set()
    close_thread.join(SCHEDULER_TIMEOUT_SECONDS)
    assert result == [True]
    assert calls == 1


def test_retry_worker_remains_visible_until_permit_release_commits() -> None:
    from schema_sanitizer.core_impl.retry_scheduler import _RetryScheduler

    scheduler = _RetryScheduler()
    release_entered = threading.Event()
    release_resume = threading.Event()
    registered = threading.Event()

    class Lease:
        def release(self) -> None:
            release_entered.set()
            assert release_resume.wait(2)

    lease = Lease()

    def retire() -> None:
        current = threading.current_thread()
        with scheduler._condition:
            scheduler._execution_workers.add(current)
            scheduler._worker_leases[current] = lease
            registered.set()
        scheduler._finish_worker(current, lease, timer=False)

    worker = threading.Thread(target=retire)
    worker.start()
    assert registered.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert release_entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert not scheduler.close(deadline_seconds=0.03)
    assert scheduler.snapshot().retiring_workers == 1
    release_resume.set()
    worker.join(2)
    assert not worker.is_alive()
    assert scheduler.snapshot().retiring_workers == 0


def test_notifier_hard_deadline_is_dispatch_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    notifier = module._AvailabilityNotifier()
    local_threads = module._Governor(
        1, "guardian-close-never-requeues-active-owner-notifier-thread"
    )
    monkeypatch.setattr(module, "_NOTIFIER_THREAD_GOVERNOR", local_threads)
    called = threading.Event()
    governor = module._Governor(
        1,
        "guardian-close-never-requeues-active-owner-notifier-deadline",
        availability_dispatcher=lambda _event: called.set(),
    )
    event = module.AvailabilityEvent.RETRY_SCHEDULER
    assert governor.register_availability_event(event)
    delivery = governor._availability_events[event]
    delivery.next_attempt_ns = time.monotonic_ns() + 150_000_000
    assert notifier.publish_one(delivery)
    assert not notifier.close(deadline_seconds=0.01)
    with notifier._condition:
        assert notifier._condition.wait_for(
            lambda: len(notifier._parked) == 1,
            timeout=SCHEDULER_TIMEOUT_SECONDS,
        )
    assert not called.is_set()
    assert notifier.snapshot().parked_callbacks == 1


def test_level_triggered_availability_closes_release_before_register_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(
        1,
        "guardian-close-never-requeues-active-owner-level",
        level_triggered_availability=True,
        availability_dispatcher=lambda _event: None,
    )
    notifier = module._AVAILABILITY_NOTIFIER
    real_publish_one = notifier.publish_one
    published: list[Any] = []

    def capture_target_and_forward_others(delivery: Any) -> bool:
        if delivery.governor is governor:
            published.append(delivery)
            return True
        return real_publish_one(delivery)

    # Existing process services may publish concurrently.  Route their work to
    # the real notifier while accepting only this governor's canonical delivery
    # synchronously; replacing the module-global notifier would let unrelated
    # owners contaminate this unit test's queue and shutdown result.
    monkeypatch.setattr(notifier, "publish_one", capture_target_and_forward_others)

    lease = governor.acquire(1, timeout_seconds=0)
    lease.release()
    event = module.AvailabilityEvent.RETRY_SCHEDULER
    assert governor.register_availability_event(event)
    delivery = governor._availability_events[event]
    assert published == [delivery]
    assert delivery.governor is governor
    assert delivery.event is event
    governor.unregister_availability_event(event)


def test_notifier_rearm_during_execution_is_not_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    notifier = module._AvailabilityNotifier()
    local_threads = module._Governor(1, "guardian-close-never-requeues-active-owner-rearm-thread")
    monkeypatch.setattr(module, "_NOTIFIER_THREAD_GOVERNOR", local_threads)
    event = module.AvailabilityEvent.RETRY_SCHEDULER
    attempts = 0
    completed = threading.Event()
    delivery: module._AvailabilityDelivery

    def dispatch(_event: module.AvailabilityEvent) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            assert notifier.publish_one(delivery)
        else:
            completed.set()

    governor = module._Governor(
        1, "guardian-close-never-requeues-active-owner-rearm", availability_dispatcher=dispatch
    )
    assert governor.register_availability_event(event)
    delivery = governor._availability_events[event]
    assert notifier.publish_one(delivery)
    assert completed.wait(SCHEDULER_TIMEOUT_SECONDS)
    deadline = time.monotonic() + 1
    while governor.snapshot().availability_callbacks and time.monotonic() < deadline:
        time.sleep(0.005)
    assert attempts == 2
    assert governor.snapshot().availability_callbacks == 0
    assert notifier.close(deadline_seconds=1.0)


def test_uncertain_fd_close_retains_capacity_debt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import path_identity, process_resources

    monkeypatch.setattr(
        process_resources,
        "_UNCERTAIN_FD_CLOSE_DEBTS",
        [
            process_resources._UncertainFdCloseDebtSlot()
            for _ in range(process_resources._FD_GOVERNOR.capacity)
        ],
    )
    monkeypatch.setattr(process_resources, "_UNCERTAIN_FD_CLOSE_REJECTED", 0)

    governor = process_resources._FD_GOVERNOR
    baseline = governor.snapshot().in_use
    lease = process_resources.acquire_file_descriptors(1, timeout_seconds=0.1)
    owner = path_identity._IdentityDescriptorOwner(123, lease)
    monkeypatch.setattr(
        path_identity.os, "close", lambda _fd: (_ for _ in ()).throw(OSError("uncertain"))
    )
    with pytest.raises(OSError, match="uncertain"):
        owner.release()
    assert governor.snapshot().in_use == baseline + 1
    snapshot = process_resources.uncertain_fd_close_snapshot()
    assert snapshot.debts == 1
    assert snapshot.oldest_debt_ns > 0


def test_runtime_registry_cancels_reserved_thread_before_start() -> None:
    from schema_sanitizer.core_impl.runtime_registry import _RuntimeServiceRegistry

    class Service:
        def close(self, *, deadline_seconds: float = 0.0) -> bool:
            return True

    registry = _RuntimeServiceRegistry()
    registration = registry.reserve(
        Service(), kind="guardian-close-never-requeues-active-owner", close_name="close"
    )
    ran = threading.Event()
    thread = threading.Thread(target=ran.set)
    registry.close_admission()
    with pytest.raises(RuntimeError, match="admission closed"):
        registration.start_thread(thread)
    assert not thread.is_alive()
    assert not ran.is_set()


def test_dispatcher_watchdog_tracks_real_active_call_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import cleanup_dispatcher as module

    class Lease:
        def release(self) -> None:
            return None

    monkeypatch.setattr(module, "acquire_project_threads", lambda *_a, **_k: Lease())
    dispatcher = module._CleanupDispatcher()
    entered = threading.Event()
    resume = threading.Event()

    def blocked() -> None:
        entered.set()
        assert resume.wait(2)

    assert dispatcher.submit(blocked)
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    snapshot = dispatcher.snapshot()
    assert snapshot.active_calls == 1
    assert snapshot.oldest_active_ns > 0
    resume.set()
    assert dispatcher.close(deadline_seconds=1.0)
    assert dispatcher.snapshot().oldest_active_ns == 0


def test_runtime_snapshot_includes_fd_debt_and_retirement() -> None:
    source = Path("src/schema_sanitizer/core_impl/runtime_diagnostics.py").read_text()
    shutdown = Path("src/schema_sanitizer/core_impl/runtime_shutdown.py").read_text()
    # Separately conserved external resident-stack debt is part of the
    # current integral diagnostic schema.
    assert '"version": 8' in source
    assert '"uncertain_fd_closes"' in source
    assert 'field(retry_snapshot, "retiring_workers")' in shutdown
    assert 'field(guardian_snapshot, "retiring_workers")' in shutdown


def test_native_reaper_shutdown_is_biphasic_and_terminal_states_are_visible() -> None:
    source = Path("cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    header = Path("cpp/src/internal/runtime/operation_task_arena.hh").read_text()
    abi = Path("cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc").read_text()
    assert "DrainAndShutdownFor" in source
    drain = source[source.index("DrainAndShutdownFor") : source.index("void Shutdown() noexcept")]
    assert "producers_quiescent" in drain
    assert "Keep consumers alive after a timed-out attempt" in drain
    assert "Terminalize" in source
    assert "reaper_terminal_states" in header
    # The current native snapshot publishes every terminal/reaper field plus
    # the unified physical-thread and external resident-stack authorities.
    assert "PyTuple_New(30)" in abi
    assert "snapshot.external_runtime_stack_debt_threads" in abi
    assert (
        "SaturatingAtomicSubtract(state_->active, 1U)"
        in Path("cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text()
    )
