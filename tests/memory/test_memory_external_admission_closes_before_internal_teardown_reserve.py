"""Tests shutdown sequencing across external admission, teardown reserves, ledger
acknowledgements, notifier restart, dispatcher or guardian lease retry, single-flight
close, quarantine, snapshots, asynchronous bridges, and native reaping. External work
closes before internal teardown reserve; failed cleanup remains rooted and visible,
deadlines stay bounded, and snapshots remain coherent."""

from __future__ import annotations

import gc
import os
import threading
import weakref
from pathlib import Path

import pytest
from _support.synchronization import (
    SCHEDULER_TIMEOUT_SECONDS,
    WaitObservedCondition,
    join_thread_or_fail,
)


def test_external_admission_closes_before_internal_teardown_reserve() -> None:
    """Verify external admission closes before internal teardown reserve."""
    from schema_sanitizer.core_impl.process_resources import _Governor
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    governor = _Governor(1, "external-admission-closes-before-internal-teardown-split-admission")
    governor.close_external_admission()
    with pytest.raises(SchemaSanitizerResourceError, match="external admission"):
        governor.acquire(1)
    lease = governor.acquire(1, _teardown=True)
    lease.release()
    governor.close_admission()
    with pytest.raises(SchemaSanitizerResourceError, match="teardown admission"):
        governor.acquire(1, _teardown=True)


def test_lease_commits_released_only_after_ledger_acknowledges() -> None:
    """Verify lease commits released only after ledger acknowledges."""
    from schema_sanitizer.core_impl.process_resources import _Governor

    governor = _Governor(1, "external-admission-closes-before-internal-teardown-release-ack")
    lease = governor.acquire(1)
    original = lease._capability
    object.__setattr__(lease, "_capability", object())
    with pytest.raises(RuntimeError, match="unknown or corrupted"):
        lease.release()
    assert lease._released is False
    assert governor.snapshot().in_use == 1
    assert governor.snapshot().active_leases == 1
    object.__setattr__(lease, "_capability", original)
    lease.release()
    assert governor.snapshot().in_use == 0


def test_notifier_rejection_reinstalls_accepted_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify notifier rejection reinstalls accepted callback."""
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(
        1, "external-admission-closes-before-internal-teardown-notifier-transaction"
    )
    lease = governor.acquire(1)
    assert governor.register_availability_event(module.AvailabilityEvent.RETRY_SCHEDULER)
    monkeypatch.setattr(
        module._AVAILABILITY_NOTIFIER,
        "publish_one",
        lambda _delivery: False,
    )
    lease.release()
    snapshot = governor.snapshot()
    assert snapshot.availability_callbacks == 1
    assert snapshot.in_use == 0


def test_notifier_schedules_autonomous_restart_after_start_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify notifier schedules autonomous restart after start failure."""
    from schema_sanitizer.core_impl import process_resources as module
    from schema_sanitizer.core_impl import retry_scheduler

    notifier = module._AvailabilityNotifier()
    governor = module._Governor(1, "external-admission-closes-before-internal-teardown-restart")
    assert governor.register_availability_event(module.AvailabilityEvent.RETRY_SCHEDULER)
    delivery = governor._availability_events[module.AvailabilityEvent.RETRY_SCHEDULER]
    scheduled = threading.Event()

    class Exhausted:
        def try_acquire_up_to(self, *_args: object, **_kwargs: object) -> object:
            """Raise the deliberate failure for the try acquire up to path."""
            raise RuntimeError("no emergency thread")

    def schedule(*_args: object, **_kwargs: object) -> bool:
        """Schedule the callback through the controlled executor."""
        scheduled.set()
        return True

    monkeypatch.setattr(module, "_NOTIFIER_THREAD_GOVERNOR", Exhausted())
    monkeypatch.setattr(retry_scheduler, "schedule_retry", schedule)
    assert notifier.publish_one(delivery)
    assert scheduled.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert notifier.snapshot().pending_callbacks == 1
    assert notifier.snapshot().worker_start_failures == 1


def test_dispatcher_close_retries_failed_worker_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify dispatcher close retries failed worker lease."""
    from schema_sanitizer.core_impl import cleanup_dispatcher as module

    def adopt(owner: object, **_kwargs: object) -> bool:
        """Adopt the teardown lease into the controlled lifecycle owner."""
        getattr(owner, "release")()
        return True

    monkeypatch.setattr(module, "adopt_failed_release", adopt)
    dispatcher = module._CleanupDispatcher()

    class Lease:
        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.attempts = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("transient")

    lease = Lease()
    with dispatcher._condition:
        dispatcher._failed_worker_leases.append(lease)
    assert dispatcher.close(deadline_seconds=1)
    assert lease.attempts >= 2
    assert dispatcher.snapshot().failed_worker_leases == 0


def test_guardian_close_retries_failed_worker_lease() -> None:
    """Verify guardian close retries failed worker lease."""
    from schema_sanitizer.core_impl.retry_scheduler import _ReleaseGuardian

    guardian = _ReleaseGuardian()

    class Lease:
        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.attempts = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("transient")

    lease = Lease()
    with guardian._condition:
        guardian._failed_worker_leases.append(lease)
    assert guardian.close(deadline_seconds=1)
    assert lease.attempts >= 2
    assert guardian.snapshot().failed_worker_leases == 0


def test_shutdown_single_flight_propagates_same_failure_to_waiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify shutdown single flight propagates same failure to waiters."""
    from schema_sanitizer.core_impl import runtime_shutdown as module

    module._reset_runtime_shutdown_for_tests()
    monkeypatch.setattr(module, "_SHUTDOWN_CONDITION", WaitObservedCondition())
    entered = threading.Event()
    resume = threading.Event()
    failure = RuntimeError("external-admission-closes-before-internal-teardown shutdown failure")

    def fail(_deadline_ns: int, _generation: int) -> object:
        """Inject the expected failure at the controlled test point."""
        entered.set()
        assert resume.wait(SCHEDULER_TIMEOUT_SECONDS)
        raise failure

    monkeypatch.setattr(module, "_perform_shutdown", fail)
    observed: list[BaseException] = []

    def invoke() -> None:
        """Invoke the callback at the controlled failure point."""
        try:
            module.shutdown_concurrency_runtime(deadline_seconds=SCHEDULER_TIMEOUT_SECONDS)
        except BaseException as exc:  # noqa: BLE001 - contract under test
            observed.append(exc)

    first = threading.Thread(target=invoke)
    second = threading.Thread(target=invoke)
    first.start()
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    second.start()
    assert module._SHUTDOWN_CONDITION.wait_entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    resume.set()
    join_thread_or_fail(first)
    join_thread_or_fail(second)
    try:
        assert observed == [failure, failure]
    finally:
        module._reset_runtime_shutdown_for_tests()


def test_registry_retains_strong_control_block_until_quiescence() -> None:
    """Verify registry retains strong control block until quiescence."""
    from schema_sanitizer.core_impl.durations import deadline_ns_from_timeout
    from schema_sanitizer.core_impl.runtime_registry import _RuntimeServiceRegistry

    registry = _RuntimeServiceRegistry()

    class Service:
        def close(self, *, deadline_seconds: float) -> bool:
            """Close the resources owned by the service test double."""
            return deadline_seconds >= 0

    service = Service()
    reference = weakref.ref(service)
    registration = registry.register(
        service,
        kind="external-admission-closes-before-internal-teardown-strong",
        close_name="close",
    )
    del service
    gc.collect()
    assert reference() is not None
    closed, remaining = registry.close_all(
        deadline_ns=deadline_ns_from_timeout(
            1, name="external-admission-closes-before-internal-teardown registry"
        )
    )
    assert (closed, remaining) == (1, 0)
    registration.close()
    gc.collect()
    assert reference() is None


@pytest.mark.skipif(os.name == "nt", reason="POSIX quarantine root descriptor required")
def test_quarantine_rejects_replaced_root_without_targeting_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify quarantine rejects replaced root without targeting replacement."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    module._close_root_handle()
    monkeypatch.setenv(module._ENV_DIRECTORY, str(tmp_path))
    handle = module._root_handle()
    pinned = tmp_path / "pinned-root"
    os.rename(handle.path, pinned)
    handle.path.mkdir(mode=0o700)
    source = tmp_path / "source.tmp"
    source.write_bytes(b"payload")

    janitor = module._TemporaryArtifactJanitor()
    monkeypatch.setattr(janitor, "_ensure_worker", lambda: None)

    class Lease:
        def release(self) -> None:
            """Release the resource held by the lease test double."""
            return None

    assert janitor.quarantine(source, is_dir=False, lease=Lease())
    assert source.exists()
    assert not tuple(handle.path.glob("artifact-*"))
    assert not tuple(pinned.glob("artifact-*"))
    source.unlink()
    janitor.sweep()
    module._close_root_handle()


def test_cleanup_exception_notes_use_only_no_throw_helper() -> None:
    """Verify cleanup exception notes use only no throw helper."""
    root = Path("src/schema_sanitizer")
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        if path.name == "safe_errors.py":
            continue
        source = path.read_text()
        if ".add_note(" in source or "cleanup_error!r" in source or "cleanup_error!s" in source:
            offenders.append(str(path))
    assert offenders == []


def test_integral_snapshot_uses_even_seqlock_epoch_and_native_section() -> None:
    """Verify integral snapshot uses even seqlock epoch and native section."""
    from schema_sanitizer.core_impl.runtime_diagnostics import (
        concurrency_runtime_debug_snapshot,
    )

    snapshot = concurrency_runtime_debug_snapshot()
    assert snapshot["version"] >= 4
    epoch = int(snapshot["retry_scheduler"]["capture_epoch"].split(":", 1)[0])
    assert epoch % 2 == 0
    assert "native_operation_arenas" in snapshot


def test_async_bridge_has_no_post_deadline_unbounded_drain() -> None:
    """Verify async bridge has no post deadline unbounded drain."""
    source = Path("src/schema_sanitizer/remote_impl/async_bridge.py").read_text()
    assert "while pending_after_deadline" not in source
    assert "_terminal_non_cooperative" in source


def test_native_reaper_reservation_and_exit_are_bounded() -> None:
    """Verify native reaper reservation and exit are bounded."""
    source = Path("cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    assert "class TeardownReservationGuard" in source
    assert "reservation_guard.Commit()" in source
    assert "ShutdownFor(100U)" in source
    assert "g_detached_workers" in source
    assert "g_live_arena_states" in source
    assert "SaturatingAtomicSubtract(state->queued_total, abandoned)" in source
    assert "ArenaCleanupReaper::Instance().Park(state)" in source
