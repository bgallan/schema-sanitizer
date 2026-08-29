"""Tests immutable process-lease amounts alongside mutable retry keys, cancellation
durations, guardian or dispatcher lock boundaries, callback ownership, shared shutdown
deadlines, snapshots, fork behavior, and native metrics. The ledger remains
authoritative, cleanup workers retain failed owners, and child or runtime shutdown
cannot rebuild unsafe dynamic state."""

from __future__ import annotations

import os
import threading
from pathlib import Path

import pytest
from _support.synchronization import (
    SCHEDULER_TIMEOUT_SECONDS,
    join_thread_or_fail,
    wait_for_process_exit,
)


def test_process_lease_amount_is_immutable_and_ledger_authoritative() -> None:
    """Verify process lease amount is immutable and ledger authoritative."""
    from schema_sanitizer.core_impl.process_resources import _Governor

    governor = _Governor(2, "process-lease-amount-is-immutable-and")
    first = governor.acquire(1)
    second = governor.acquire(1)
    with pytest.raises(AttributeError):
        first.amount = 2  # type: ignore[misc]
    # Capability fields are immutable after publication and the ledger remains authoritative.
    with pytest.raises(AttributeError):
        first._amount = 2
    first.release()
    snapshot = governor.snapshot()
    assert snapshot.in_use == 1
    assert snapshot.active_leases == 1
    with pytest.raises(Exception):
        governor.acquire(2, timeout_seconds=0)
    second.release()
    assert governor.snapshot().in_use == 0


def test_mutable_hash_retry_key_can_be_cancelled_and_pruned() -> None:
    """Verify mutable hash retry key can be cancelled and pruned."""
    from schema_sanitizer.core_impl.retry_scheduler import _RetryScheduler

    class MutableHash:
        def __init__(self) -> None:
            """Initialize the mutable hash test double."""
            self.value = 1

        def __hash__(self) -> int:
            """Return the mutable hash selected by the retry-key test."""
            return self.value

        def __eq__(self, other: object) -> bool:
            """Compare mutable-hash identities for the retry-key test."""
            return self is other

    scheduler = _RetryScheduler()
    key = MutableHash()
    assert scheduler.schedule(key, lambda: None, delay_seconds=60)
    key.value = 999
    scheduler.cancel(key)
    snapshot = scheduler.snapshot()
    assert snapshot.pending_retries == 0
    assert snapshot.generation_entries == 0
    assert scheduler.close(deadline_seconds=1.0)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1.0, True])
def test_operation_cancellation_rejects_invalid_durations(value: object) -> None:
    """Verify operation cancellation rejects invalid durations."""
    from schema_sanitizer.core_impl.cancellation import operation_cancellation

    with pytest.raises((TypeError, ValueError)):
        with operation_cancellation(timeout_seconds=value):  # type: ignore[arg-type]
            pass


def test_guardian_reads_owner_metadata_outside_its_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify guardian reads owner metadata outside its lock."""
    import schema_sanitizer.core_impl.retry_scheduler as module

    entered = threading.Event()
    resume = threading.Event()

    from schema_sanitizer.core_impl import temporary_storage

    class Owner:
        @property
        def reserved_bytes(self) -> int:
            """Fail the first cleanup attempt, then signal a successful retry."""
            entered.set()
            assert resume.wait(SCHEDULER_TIMEOUT_SECONDS)
            return 1024

        def release(self) -> None:
            """Release the resource held by the owner test double."""
            return None

    monkeypatch.setattr(temporary_storage, "TemporaryStorageLease", Owner)
    guardian = module._ReleaseGuardian()

    class FailOnContentionCondition(threading.Condition):
        def __enter__(self) -> FailOnContentionCondition:
            """Enter the context managed by the fail on contention condition test double."""
            if not self.acquire(blocking=False):
                raise AssertionError("guardian snapshot waited on owner metadata")
            return self

        def __exit__(self, *_args: object) -> None:
            """Exit the context managed by the fail on contention condition test double and run cleanup."""
            self.release()

    guardian._condition = FailOnContentionCondition()
    monkeypatch.setattr(guardian, "_ensure_workers", lambda: None)
    thread = threading.Thread(
        target=lambda: guardian.adopt(Owner(), retained_bytes=128), daemon=True
    )
    thread.start()
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    try:
        guardian.snapshot()
    finally:
        resume.set()
    join_thread_or_fail(thread)


def test_cleanup_dispatcher_retries_instead_of_dropping_failed_owner() -> None:
    """Verify cleanup dispatcher retries instead of dropping failed owner."""
    from schema_sanitizer.core_impl.cleanup_dispatcher import _CleanupDispatcher

    dispatcher = _CleanupDispatcher()
    completed = threading.Event()
    attempts = 0

    def cleanup() -> None:
        """Signal the metadata read, wait for release, and report 1 KiB reserved."""
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("transient")
        completed.set()

    assert dispatcher.submit(cleanup, retained_bytes=256)
    assert completed.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert attempts == 2
    assert dispatcher.close(deadline_seconds=2.0)


def test_cleanup_close_retains_pending_callback_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cleanup close retains pending callback owner."""
    from schema_sanitizer.core_impl.cleanup_dispatcher import _CleanupDispatcher

    dispatcher = _CleanupDispatcher()
    monkeypatch.setattr(dispatcher, "_ensure_workers", lambda: None)
    owner = object()
    assert dispatcher.submit(lambda retained: None, owner, retained_bytes=64)
    assert not dispatcher.close(deadline_seconds=0)
    snapshot = dispatcher.snapshot()
    assert snapshot.pending_calls == 1
    queue = next(iter(dispatcher._queues.values()))
    assert queue[0].args[0] is owner


def test_runtime_registry_closes_services_with_one_shared_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify runtime registry closes services with one shared deadline."""
    from schema_sanitizer.core_impl import runtime_registry as module

    monkeypatch.setattr(module, "remaining_seconds", lambda _deadline_ns: 0.75)

    registry = module._RuntimeServiceRegistry()
    calls: list[float] = []

    class Service:
        def close(self, *, deadline_seconds: float) -> bool:
            """Close the resources owned by the service test double."""
            calls.append(deadline_seconds)
            return True

    service = Service()
    registration = registry.register(
        service, kind="process-lease-amount-is-immutable-and", close_name="close"
    )
    closed, remaining = registry.close_all(deadline_ns=1)
    assert closed == 1
    assert remaining == 0
    assert calls == [0.75]
    registration.close()


def test_debug_snapshot_covers_integral_runtime() -> None:
    """Verify debug snapshot covers integral runtime."""
    from schema_sanitizer.core_impl.runtime_diagnostics import (
        concurrency_runtime_debug_snapshot,
    )

    snapshot = concurrency_runtime_debug_snapshot()
    assert snapshot["version"] >= 2
    for key in (
        "retry_scheduler",
        "release_guardian",
        "cleanup_dispatcher",
        "temporary_janitor",
        "process_threads",
        "process_file_descriptors",
        "runtime_services",
        "fork_poisoned",
    ):
        assert key in snapshot


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_post_fork_child_cannot_reinitialize_retry_runtime() -> None:
    """Verify post fork child cannot reinitialize retry runtime."""
    from schema_sanitizer.core_impl.retry_scheduler import _RetryScheduler

    scheduler = _RetryScheduler()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - isolated child
        try:
            scheduler.schedule("child", lambda: None, delay_seconds=0)
        except RuntimeError:
            os._exit(0)
        os._exit(3)
    status = wait_for_process_exit(pid)
    assert os.waitstatus_to_exitcode(status) == 0
    assert scheduler.close(deadline_seconds=1.0)


def test_native_shutdown_path_uses_preallocated_detached_metrics() -> None:
    """Verify native shutdown path uses preallocated detached metrics."""
    source = Path("cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    assert "std::unordered_map" not in source
    assert "std::function<void()> mark_detached" not in source
    assert "compare_exchange_strong" in source
    assert "wrapper allocation failed" in source
