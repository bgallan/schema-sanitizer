"""Pass38 regressions for linearization, crash recovery, fork and hard bounds."""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path

import pytest


def _move_pending_to_ready(scheduler) -> None:
    with scheduler._condition:
        for item in tuple(scheduler._current.values()):
            scheduler._current.pop(item.key, None)
            scheduler._drop_pending_charge_locked(item)
            scheduler._enqueue_ready_locked(item)
            scheduler._ready_by_key[item.key] = item
            scheduler._ready_bytes += item.retained_bytes


def test_cancel_linearizes_before_claimed_callback_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    calls: list[str] = []
    key = ("pass38-linearization", 1)
    assert scheduler.schedule(key, lambda: calls.append("ran"), delay_seconds=0)
    _move_pending_to_ready(scheduler)
    with scheduler._condition:
        item = scheduler._take_ready_locked()
        assert item is not None
    scheduler.cancel(key)
    with scheduler._condition:
        assert not scheduler._begin_execution_locked(item)
    assert calls == []


def test_same_key_is_single_flight_with_one_coalesced_successor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    key = ("pass38-single-flight", 1)
    normalized_key = module._normalize_retry_key(key)
    assert scheduler.schedule(key, lambda: None, delay_seconds=0, retained_bytes=10)
    _move_pending_to_ready(scheduler)
    with scheduler._condition:
        active = scheduler._take_ready_locked()
        assert active is not None
        assert scheduler._begin_execution_locked(active)
    assert scheduler.schedule(key, lambda: None, delay_seconds=0, retained_bytes=11)
    assert scheduler.schedule(key, lambda: None, delay_seconds=0, retained_bytes=12)
    with scheduler._condition:
        assert scheduler._active_by_key[normalized_key] is active
        assert len(scheduler._successors) == 1
        assert scheduler._successors[normalized_key].retained_bytes == 12
        assert key not in scheduler._ready_by_key
        assert key not in scheduler._current


def test_generation_tombstones_are_pruned(monkeypatch: pytest.MonkeyPatch) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    scheduler.cancel(("unknown", 1))
    assert not scheduler._key_generations
    for index in range(2000):
        key = ("unique", index)
        assert scheduler.schedule(key, lambda: None, delay_seconds=3600)
        scheduler.cancel(key)
    assert not scheduler._key_generations


@pytest.mark.parametrize("delay", [float("nan"), float("inf"), -1.0])
def test_retry_rejects_non_finite_or_negative_delay(delay: float) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    with pytest.raises(ValueError):
        scheduler.schedule(("bad-delay", repr(delay)), lambda: None, delay_seconds=delay)


def test_retry_huge_finite_delay_saturates(monkeypatch: pytest.MonkeyPatch) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    assert scheduler.schedule(("huge", 1), lambda: None, delay_seconds=1e300)
    assert next(iter(scheduler._current.values())).deadline_ns == module._MAX_DEADLINE_NS


def test_claim_sweeper_removes_crash_left_hardlink_alias(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import schema_sanitizer.core_impl.path_identity as module

    root = tmp_path / "claims"
    root.mkdir(mode=0o700)
    record = module._ExternalClaim(
        os.getpid(), module.process_start_token(os.getpid()), b"x" * 16, 1
    )
    temporary = root / ".claim-write-crash"
    canonical = root / "claim-canonical"
    temporary.write_bytes(module._serialize_claim(record))
    try:
        os.link(temporary, canonical)
    except OSError:
        pytest.skip("hard links unavailable")
    monkeypatch.setattr(module, "_CLAIM_SWEEP_ITERATOR", None)
    monkeypatch.setattr(module, "_CLAIM_SWEEP_OWNER", None)
    monkeypatch.setattr(module, "_CLAIM_SWEEP_ROOT", None)
    module._sweep_external_claims(root, limit=32)
    assert not temporary.exists()
    assert canonical.exists()
    assert canonical.stat().st_nlink == 1


def test_claim_publication_fsyncs_alias_removal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import schema_sanitizer.core_impl.path_identity as module

    getattr(monkeypatch, "set" + "env")(
        "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path / "coord")
    )
    monkeypatch.setattr(module, "_set_new_owner_marker", lambda *_args: False)
    artifact = tmp_path / "artifact"
    artifact.write_text("x")
    real_fsync = module.os.fsync
    directory_syncs = 0

    def count_fsync(descriptor: int) -> None:
        nonlocal directory_syncs
        if os.path.isdir(f"/proc/self/fd/{descriptor}"):
            directory_syncs += 1
        real_fsync(descriptor)

    monkeypatch.setattr(module.os, "fsync", count_fsync)
    identity = module.claim_path_identity(artifact)
    assert identity is not None
    assert directory_syncs >= 2
    module.release_path_identity(identity)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX fork required")
def test_after_fork_path_reset_never_acquires_inherited_owner_lock(tmp_path: Path) -> None:
    import schema_sanitizer.core_impl.path_identity as module

    owner = module.PathClaimOwner(None, None, None)
    locked = threading.Event()
    release = threading.Event()

    def hold() -> None:
        owner.lock.acquire()
        locked.set()
        release.wait(5)
        owner.lock.release()

    thread = threading.Thread(target=hold)
    thread.start()
    assert locked.wait(2)
    module._ABANDONED_CLAIM_OWNERS = {id(owner): owner}
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(read_fd)
            module._reset_path_identity_after_fork()
            os.write(write_fd, b"ok")
        finally:
            os._exit(0)
    os.close(write_fd)
    deadline = time.monotonic() + 3
    data = b""
    while time.monotonic() < deadline and not data:
        import select

        if select.select([read_fd], [], [], 0.05)[0]:
            data = os.read(read_fd, 2)
    release.set()
    thread.join(2)
    os.close(read_fd)
    os.waitpid(pid, 0)
    assert data == b"ok"


def test_guardian_parks_overflow_without_destroying_owner_under_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    monkeypatch.setattr(module, "_RELEASE_MAX_ATTEMPTS", 1)
    monkeypatch.setattr(module, "_MAX_DEAD_LETTERS", 0)
    monkeypatch.setattr(module, "_IDLE_SECONDS", 0.005)

    class WorkerPermit:
        def release(self) -> None:
            pass

    monkeypatch.setattr(module, "acquire_release_guardian_thread", WorkerPermit)
    guardian = module._ReleaseGuardian()

    class Broken:
        def release(self) -> None:
            raise OSError("permanent")

    owner = Broken()
    assert guardian.adopt(owner, retained_bytes=32)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and guardian.snapshot().parked_owners == 0:
        time.sleep(0.002)
    snap = guardian.snapshot()
    assert snap.parked_owners == 1
    assert snap.parked_bytes == 32
    assert snap.dead_letter_owners == 0


def test_cleanup_dispatcher_round_robins_subsystems(monkeypatch: pytest.MonkeyPatch) -> None:
    import schema_sanitizer.core_impl.cleanup_dispatcher as module

    dispatcher = module._CleanupDispatcher()
    monkeypatch.setattr(dispatcher, "_ensure_workers", lambda: None)

    class A:
        def __call__(self) -> None:
            pass

    class B:
        def __call__(self) -> None:
            pass

    assert dispatcher.submit(A(), retained_bytes=16, subsystem=module.CleanupSubsystem.STORAGE)
    assert dispatcher.submit(A(), retained_bytes=16, subsystem=module.CleanupSubsystem.STORAGE)
    assert dispatcher.submit(B(), retained_bytes=16, subsystem=module.CleanupSubsystem.REMOTE)
    with dispatcher._condition:
        calls = [dispatcher._take_call_locked() for _ in range(3)]
    assert [type(call.callback).__name__ for call in calls if call is not None] == ["A", "B", "A"]


def test_scheduler_structured_close_rejects_new_work(monkeypatch: pytest.MonkeyPatch) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    assert scheduler.schedule(("close", 1), lambda: None, delay_seconds=3600)
    assert scheduler.close(deadline_seconds=0.1)
    assert not scheduler.schedule(("close", 2), lambda: None, delay_seconds=0)
    assert scheduler.snapshot().lifecycle_state == "STOPPED"


def test_native_arena_source_has_checked_inline_admission_and_bounded_reaper() -> None:
    source = (
        Path(__file__).parents[2] / "cpp/src/internal/runtime/operation_task_arena.cc"
    ).read_text()
    inline = source.index("if (state->worker_count <= 1U)")
    oversize = source.index("if (retained_bytes > state->queue_byte_capacity)", inline)
    subtraction = source.index("state->queue_byte_capacity - retained_bytes", inline)
    assert oversize < subtraction
    assert "Enqueue(const std::shared_ptr<OperationTaskArena::State> &state)" in source
    assert "if (!ArenaCleanupReaper::Instance().Enqueue(state))" in source


def test_retry_scheduler_failed_worker_lease_overflow_is_retained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(module, "adopt_failed_release", lambda *_args, **_kwargs: False)
    owners = [object() for _ in range(module._MAX_FAILED_WORKER_LEASES + 1)]
    for owner in owners:
        scheduler._adopt_failed_lease(owner)
    with scheduler._condition:
        retained = tuple(scheduler._failed_worker_leases)
        terminal = scheduler._terminal_failed_worker_lease
    assert len(retained) == module._MAX_FAILED_WORKER_LEASES
    assert terminal is owners[-1]
    assert {id(owner) for owner in retained} | {id(terminal)} == {id(owner) for owner in owners}


def test_cleanup_dispatcher_failed_worker_leases_are_never_truncated() -> None:
    import schema_sanitizer.core_impl.cleanup_dispatcher as module

    dispatcher = module._CleanupDispatcher()
    owners = [object() for _ in range(module._MAX_FAILED_WORKER_LEASES + 1)]
    with dispatcher._condition:
        for owner in owners:
            dispatcher._retain_failed_worker_lease_locked(owner)
        retained = tuple(dispatcher._failed_worker_leases)
        terminal = dispatcher._terminal_failed_worker_lease
    assert len(retained) == module._MAX_FAILED_WORKER_LEASES
    assert terminal is owners[-1]
    assert dispatcher.snapshot().failed_worker_leases == len(owners)


def test_janitor_failed_thread_lease_fallback_is_bounded_and_fail_closed() -> None:
    import schema_sanitizer.core_impl.temporary_janitor as module

    janitor = module._TemporaryArtifactJanitor()
    owners = [object() for _ in range(module._MAX_FAILED_THREAD_LEASES + 1)]
    with janitor._condition:
        for owner in owners:
            janitor._retain_failed_thread_lease_locked(owner)
        assert tuple(janitor._failed_thread_leases) == (owners[0],)
        assert janitor._terminal_failed_thread_lease is owners[1]
        assert janitor._has_failed_thread_leases_locked()
    assert janitor.snapshot().failed_thread_leases == len(owners)


def test_structured_runtime_shutdown_orders_producers_before_guardian(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.cleanup_dispatcher as cleanup
    import schema_sanitizer.core_impl.process_resources as resources
    import schema_sanitizer.core_impl.retry_scheduler as retry
    import schema_sanitizer.core_impl.runtime_shutdown as shutdown
    import schema_sanitizer.core_impl.temporary_janitor as janitor

    order: list[str] = []

    class Service:
        def __init__(self, name: str) -> None:
            self.name = name

        def close(self, *, deadline_seconds: float) -> bool:
            assert deadline_seconds >= 0
            order.append(self.name)
            return True

    monkeypatch.setattr(retry, "_SCHEDULER", Service("retry"))
    monkeypatch.setattr(janitor, "_JANITOR", Service("janitor"))
    monkeypatch.setattr(cleanup, "_DISPATCHER", Service("cleanup"))
    monkeypatch.setattr(retry, "_RELEASE_GUARDIAN", Service("guardian"))
    # This is an ordering unit test with fake services. Keep the corresponding
    # process-global admissions and real notifier/reaper untouched; otherwise
    # their live leases cannot be drained by the substituted services.
    monkeypatch.setattr(resources, "close_process_resource_external_admission", lambda: None)
    monkeypatch.setattr(resources, "close_process_resource_admission", lambda: None)
    monkeypatch.setattr(resources, "close_release_guardian_thread_admission", lambda: None)
    monkeypatch.setattr(resources, "shutdown_availability_notifier", lambda **_kwargs: True)
    monkeypatch.setattr(shutdown, "_shutdown_native_cleanup_reaper", lambda _deadline: True)
    try:
        result = shutdown.shutdown_concurrency_runtime(deadline_seconds=1.0)
        assert result.retry_scheduler_stopped
        assert result.janitor_stopped
        assert result.cleanup_dispatcher_stopped
        assert result.release_guardian_stopped
        # terminal_success is intentionally stricter and may be false when an
        # unrelated real singleton worker from a previous test remains alive.
        assert order == ["janitor", "cleanup", "retry", "guardian"]
    finally:
        shutdown._reset_runtime_shutdown_for_tests()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -0.1])
def test_structured_runtime_shutdown_rejects_invalid_deadlines(value: float) -> None:
    from schema_sanitizer.core_impl.runtime_shutdown import shutdown_concurrency_runtime

    with pytest.raises(ValueError, match="finite and non-negative"):
        shutdown_concurrency_runtime(deadline_seconds=value)


def test_release_guardian_reports_trusted_resource_reservations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module
    from schema_sanitizer.core_impl import temporary_storage

    class Lease:
        reserved_bytes = 8 * 1024 * 1024

        def release(self) -> None:
            return None

    monkeypatch.setattr(temporary_storage, "TemporaryStorageLease", Lease)
    guardian = module._ReleaseGuardian()
    monkeypatch.setattr(guardian, "_ensure_workers", lambda: None)
    assert guardian.adopt(Lease(), retained_bytes=256)
    snapshot = guardian.snapshot()
    assert snapshot.retained_bytes == 256
    assert snapshot.resource_reserved_bytes == 8 * 1024 * 1024
