"""Regression coverage for memory cancelled bridge retains submission until real task terminal."""

from __future__ import annotations

import asyncio
import errno
import os
from concurrent.futures import Future
from pathlib import Path
from threading import Condition, Event, Lock, Thread
from types import SimpleNamespace
from typing import Any

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS

from benchmarks.concurrency.assets import load_probe

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="POSIX descriptor-relative filesystem hardening suite"
)

_NATIVE_STUB_MODULES = (
    "schema_sanitizer.core_impl.native_options",
    "schema_sanitizer.core_impl.execution_policy",
    "schema_sanitizer.api_impl.operation_context",
    "schema_sanitizer.api_impl.input.directory_preparation",
    "schema_sanitizer.api_impl.source_plan.attached",
    "schema_sanitizer.pipeline.partition_lookahead",
)


def test_cancelled_bridge_retains_submission_until_real_task_terminal() -> None:
    from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator

    started = Event()
    cancellation_observed = Event()
    allow_terminal = Event()
    coordinator = RemoteIoCoordinator(
        shutdown_timeout_seconds=2.0,
        permit_capacity=1,
        operation_id="cancelled-bridge-retains-submission-until-real-real-terminal",
    )

    async def operation(_context: Any) -> str:
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancellation_observed.set()
            while not allow_terminal.is_set():
                await asyncio.sleep(0.002)
            return "late-success"

    future = coordinator.submit(operation)
    assert started.wait(SCHEDULER_TIMEOUT_SECONDS)
    owner = getattr(future, "_schema_sanitizer_remote_submission")
    future.cancel()
    assert future.cancelled()
    assert cancellation_observed.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert not owner.terminal.is_set()
    assert coordinator.permit_snapshot().pending_submissions == 1
    allow_terminal.set()
    assert owner.terminal.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert coordinator.permit_snapshot().pending_submissions == 0
    coordinator.close()


def test_staged_path_never_deletes_public_replacement_after_rename_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.remote_impl import staging_paths as module

    path = tmp_path / "owned"
    path.write_text("owned")

    class Lease:
        released = False

        def release(self) -> None:
            self.released = True

    lease = Lease()
    owner = module.StagedPath(str(path), storage_lease=lease)
    real_replace = module.os.replace

    def raced_replace(source: Any, target: Any) -> None:
        if Path(source) == path:
            path.unlink()
            path.write_text("replacement")
            raise PermissionError("injected transfer failure")
        real_replace(source, target)

    monkeypatch.setattr(module.os, "replace", raced_replace)
    with pytest.raises(OSError, match="transferred privately"):
        owner.close()
    assert path.read_text() == "replacement"
    assert not lease.released
    assert not owner._closed


def test_claim_path_identity_does_not_block_on_fifo(tmp_path: Path) -> None:
    from schema_sanitizer.core_impl.path_identity import (
        claim_path_identity,
        release_path_identity,
    )

    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    identities: list[Any] = []
    errors: list[BaseException] = []

    def claim() -> None:
        try:
            identities.append(claim_path_identity(fifo))
        except BaseException as exc:
            errors.append(exc)

    thread = Thread(target=claim)
    thread.start()
    thread.join(SCHEDULER_TIMEOUT_SECONDS)
    assert not thread.is_alive()
    assert not errors
    assert identities[0] is not None
    release_path_identity(identities[0])


def test_path_identity_uses_exclusive_external_claim_without_xattrs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.core_impl import path_identity as module

    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path / "coord"))
    path = tmp_path / "dangling"
    path.symlink_to(tmp_path / "missing")

    def unsupported(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError(errno.ENOTSUP, "xattrs unsupported")

    monkeypatch.setattr(module.os, "setxattr", unsupported, raising=False)
    monkeypatch.setattr(module.os, "getxattr", unsupported, raising=False)
    identity = module.claim_path_identity(path)
    assert identity is not None
    assert identity.external_claim_path is not None
    with pytest.raises(OSError, match="already owned"):
        module.claim_path_identity(path)
    module.release_path_identity(identity)


def test_path_identity_never_installs_owner_marker_by_public_pathname(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.core_impl import path_identity as module

    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path / "coord"))
    path = tmp_path / "owned"
    path.write_text("data")
    targets: list[Any] = []

    def unsupported(target: Any, _marker: bytes) -> bool:
        targets.append(target)
        return False

    monkeypatch.setattr(module, "_set_new_owner_marker", unsupported)
    identity = module.claim_path_identity(path)
    assert identity is not None
    assert targets and all(isinstance(target, int) for target in targets)
    assert identity.external_claim_path is not None
    module.release_path_identity(identity)


def test_session_entry_waits_for_acceptance_acknowledgement() -> None:
    from schema_sanitizer.remote_impl.session_lifecycle import (
        enter_shared_download_session,
    )

    loop = asyncio.new_event_loop()
    loop_started = Event()
    block_started = Event()
    allow_loop = Event()

    def run_loop() -> None:
        asyncio.set_event_loop(loop)
        loop_started.set()
        loop.run_forever()

    loop_thread = Thread(target=run_loop)
    loop_thread.start()
    assert loop_started.wait(SCHEDULER_TIMEOUT_SECONDS)

    class Coordinator:
        def submit(self, operation: Any) -> Future[Any]:
            async def invoke() -> Any:
                return await operation(None)

            return asyncio.run_coroutine_threadsafe(invoke(), loop)

    class Session:
        async def __aenter__(self) -> "Session":
            def block() -> None:
                block_started.set()
                allow_loop.wait(SCHEDULER_TIMEOUT_SECONDS)

            asyncio.get_running_loop().call_soon(block)
            return self

        async def __aexit__(self, *_args: Any) -> None:
            return None

    errors: list[BaseException] = []
    caller = Thread(
        target=lambda: enter_shared_download_session(Coordinator(), Session(), timeout_seconds=1.0)
    )
    try:
        caller.start()
        assert block_started.wait(SCHEDULER_TIMEOUT_SECONDS)
        assert caller.is_alive()
        allow_loop.set()
        caller.join(SCHEDULER_TIMEOUT_SECONDS)
        assert not caller.is_alive()
        assert not errors
    finally:
        allow_loop.set()
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(SCHEDULER_TIMEOUT_SECONDS)
        loop.close()


def test_permit_delivery_failure_rolls_back_any_baseexception() -> None:
    from schema_sanitizer.remote_impl import io_permits as module

    governor = module.RemoteIoPermitGovernor(1)
    event_loop = asyncio.new_event_loop()

    class BrokenLoop:
        def call_soon_threadsafe(self, _callback: Any) -> None:
            raise MemoryError("delivery failed")

    try:
        future = event_loop.create_future()
        waiter = module._Waiter(
            BrokenLoop(), future, 1, "cancelled-bridge-retains-submission-until-real", "operation"
        )
        with governor._lock:
            governor._enqueue_waiter_locked(waiter)
            deliveries = governor._grant_ready_locked()
        governor._deliver(deliveries)
        assert governor.snapshot().in_use == 0
        assert future.done()
        assert isinstance(future.exception(), MemoryError)
    finally:
        event_loop.close()


def test_lookahead_retries_context_only_cleanup(native_stub: None) -> None:
    from schema_sanitizer.pipeline import partition_lookahead as module

    class Context:
        calls = 0

        def close(self) -> None:
            self.calls += 1
            if self.calls == 1:
                raise OSError("retry")

    context = Context()
    owner = object.__new__(module.PartitionSourceLookahead)
    owner._pid = os.getpid()
    owner.enabled = True
    owner._close_lock = Lock()
    owner._close_condition = Condition(owner._close_lock)
    owner._close_in_progress = False
    owner._submissions_inflight = 0
    owner._protocol_violations = 0
    owner._consumer_inflight = False
    owner._close_started = False
    owner._late_close_registered = False
    owner._closed = False
    owner._armed = None
    owner._future = None
    owner._future_context = context
    owner._executor = None
    owner._close_timeout_seconds = 0.5

    with pytest.raises(OSError, match="retry"):
        owner.close()
    assert owner._future_context is context
    assert not owner._closed
    owner.close()
    assert context.calls == 2
    assert owner._future_context is None
    assert owner._closed


def test_prepared_partition_attempts_both_cleanups_and_keeps_primary(
    native_stub: None,
) -> None:
    from schema_sanitizer.pipeline import partition_lookahead as module

    class Prepared:
        def close(self) -> None:
            raise ValueError("prepared-primary")

    class Context:
        def close(self) -> None:
            raise OSError("context-secondary")

    packet = module._PreparedPartition(
        plan=object(),
        options=object(),
        prepared_input=Prepared(),
        operation_context=Context(),
        allow_early_lookahead=False,
    )
    with pytest.raises(ValueError, match="prepared-primary") as caught:
        packet.close()
    assert any("context-secondary" in note for note in caught.value.__notes__)


def test_provider_lease_restores_active_state_after_release_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl.provider_throttle import ProviderThrottleGovernor

    governor = ProviderThrottleGovernor()
    lease, _delay = governor.try_acquire("provider")
    assert lease is not None
    original_release = governor._release_lease
    calls = 0

    def release(*args: Any, **kwargs: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("transient")
        original_release(*args, **kwargs)

    monkeypatch.setattr(governor, "_release_lease", release)
    with pytest.raises(OSError, match="transient"):
        lease.release()
    assert lease._state == "active"
    lease.release()
    assert lease._state == "released"
    assert calls == 2


def test_janitor_retries_worker_start_without_external_activity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.core_impl import temporary_janitor as module

    source = tmp_path / "source"
    source.write_text("owned")

    class StorageLease:
        released = Event()

        def release(self) -> None:
            self.released.set()

    class ThreadLease:
        def release(self) -> None:
            return None

    calls = 0

    def acquire(*_args: Any, **_kwargs: Any) -> ThreadLease:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporarily unavailable")
        return ThreadLease()

    janitor = module._TemporaryArtifactJanitor()
    monkeypatch.setattr(module, "acquire_project_threads", acquire)
    monkeypatch.setattr(module, "_RETRY_SECONDS", 0.01)
    lease = StorageLease()
    assert janitor.quarantine(source, is_dir=False, lease=lease)
    assert lease.released.wait(2)
    assert calls >= 2
    snapshot = janitor.snapshot()
    assert snapshot.pending_artifacts == 0
    assert snapshot.worker_start_failures >= 1
    janitor.close()


def test_remote_coordinator_construction_runs_outside_resource_lock(
    native_stub: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.api_impl import operation_context as module

    entered = Event()
    allow = Event()
    sentinel = object()

    def construct(*_args: Any, **_kwargs: Any) -> object:
        entered.set()
        assert allow.wait(2)
        return sentinel

    monkeypatch.setattr(module, "RemoteIoCoordinator", construct)
    owner = object.__new__(module._OperationExecutionResources)
    owner.pid = os.getpid()
    owner.policy = SimpleNamespace(is_single=False, async_concurrency=2)
    owner.operation_id = "cancelled-bridge-retains-submission-until-real"
    owner._lock = Lock()
    owner._close_condition = Condition(owner._lock)
    owner._remote_coordinator = None
    owner._remote_coordinator_building = False
    owner._close_started = False
    owner._finalizer_owner = SimpleNamespace(arg0=None)

    results: list[Any] = []
    builder = Thread(target=lambda: results.append(owner.remote_coordinator()))
    builder.start()
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert owner._lock.acquire(timeout=0.1)
    owner._lock.release()
    allow.set()
    builder.join(SCHEDULER_TIMEOUT_SECONDS)
    assert results == [sentinel]


def test_task_arena_plan_no_longer_owns_runtime_state() -> None:
    root = Path(__file__).resolve().parents[2]
    header = (root / "cpp/src/internal/runtime/operation_task_arena.hh").read_text()
    source = (root / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    assert "std::shared_ptr<void> state_owner" not in header
    assert "std::atomic<std::size_t> *cursor" not in header
    assert "cursor_lane" in header
    assert "abandoned_queued_tasks" in source
    clear_position = source.index("slot->tasks.clear();")
    detach_position = source.index("worker->detach();")
    assert clear_position < detach_position
    assert "detached_worker_age_millis" in header
    assert "benchmarks/" not in source
    probe = load_probe("lifecycle/arena-lifecycle-tsan.cc")
    assert "VerifyQueuedClosuresAreReleasedBeforeWorkerDetach" in probe
    assert "VerifyConcurrentPublicCallsAgainstShutdown" in probe
    assert "VerifyInlineAdmissionOutlivesBoundedShutdownSafely" in probe


def test_session_ack_timeout_revokes_transfer_and_self_closes() -> None:
    from schema_sanitizer.remote_impl.session_lifecycle import (
        enter_shared_download_session,
    )

    loop = asyncio.new_event_loop()
    loop_started = Event()
    blocker_started = Event()
    allow_loop = Event()
    exited = Event()

    def run_loop() -> None:
        asyncio.set_event_loop(loop)
        loop_started.set()
        loop.run_forever()

    loop_thread = Thread(target=run_loop)
    loop_thread.start()
    assert loop_started.wait(SCHEDULER_TIMEOUT_SECONDS)

    class Coordinator:
        def submit(self, operation: Any) -> Future[Any]:
            async def invoke() -> Any:
                return await operation(None)

            return asyncio.run_coroutine_threadsafe(invoke(), loop)

    class Session:
        async def __aenter__(self) -> "Session":
            def block() -> None:
                blocker_started.set()
                allow_loop.wait(2)

            asyncio.get_running_loop().call_soon(block)
            return self

        async def __aexit__(self, *_args: Any) -> None:
            exited.set()

    errors: list[BaseException] = []

    def enter() -> None:
        try:
            enter_shared_download_session(Coordinator(), Session(), timeout_seconds=0.05)
        except BaseException as exc:
            errors.append(exc)

    caller = Thread(target=enter)
    try:
        caller.start()
        assert blocker_started.wait(SCHEDULER_TIMEOUT_SECONDS)
        caller.join(SCHEDULER_TIMEOUT_SECONDS)
        assert not caller.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], TimeoutError)
        allow_loop.set()
        assert exited.wait(SCHEDULER_TIMEOUT_SECONDS)
    finally:
        allow_loop.set()
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(SCHEDULER_TIMEOUT_SECONDS)
        loop.close()


def test_provider_registry_evicts_inactive_keys_without_full_scan() -> None:
    from schema_sanitizer.remote_impl.provider_throttle import (
        ProviderThrottleGovernor,
    )

    governor = ProviderThrottleGovernor(max_tracked_keys=2)
    first, _ = governor.try_acquire("first")
    second, _ = governor.try_acquire("second")
    assert first is not None and second is not None
    first.release()
    second.release()
    third, _ = governor.try_acquire("third")
    assert third is not None
    snapshot = governor.registry_snapshot()
    assert snapshot.tracked_keys == 2
    assert snapshot.evictions == 1
    third.release()


def test_remote_permit_restores_ownership_after_release_failure() -> None:
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    governor = RemoteIoPermitGovernor(1)
    permit = asyncio.run(governor.acquire(label="cancelled-bridge-retains-submission-until-real"))
    real_release = governor._release_permit
    calls = 0

    def flaky_release(owner: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("retry")
        real_release(owner)  # type: ignore[arg-type]

    governor._release_permit = flaky_release  # type: ignore[method-assign]
    with pytest.raises(OSError, match="retry"):
        permit.release()
    assert permit._released is False
    permit.release()
    assert permit._released is True
    assert calls == 2


class _CallbackRejectingCancelledFuture(Future[Any]):
    """Bridge double that rejects callbacks and cancels before coroutine start."""

    def __init__(self, coroutine: Any) -> None:
        super().__init__()
        self.coroutine = coroutine

    def add_done_callback(self, fn: Any, *, context: Any = None) -> None:
        raise RuntimeError("callback registration failed before start")

    def cancel(self) -> bool:
        self.coroutine.close()
        return super().cancel()


def test_callbackless_cancelled_before_start_releases_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl import io_coordinator as module

    class Reservation:
        def __init__(self) -> None:
            self.release_calls = 0

        def release(self) -> None:
            self.release_calls += 1

    class Governor:
        def __init__(self, reservation: Reservation) -> None:
            self.reservation = reservation

        def reserve_submission(self) -> Reservation:
            return self.reservation

        async def acquire(self, *_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("cancelled coroutine must never start")

    class StoppedLoop:
        pass

    created: list[_CallbackRejectingCancelledFuture] = []

    def submit_bridge(coroutine: Any, _loop: Any) -> Future[Any]:
        future = _CallbackRejectingCancelledFuture(coroutine)
        created.append(future)
        return future

    reservation = Reservation()
    monkeypatch.setattr(module.asyncio, "run_coroutine_threadsafe", submit_bridge)
    owner = object.__new__(module.RemoteIoCoordinator)
    owner._pid = os.getpid()
    owner._operation_id = "cancelled-bridge-retains-submission-until-real-callbackless-cancelled"
    owner._permit_governor = Governor(reservation)
    owner._shutdown_timeout_seconds = 0.5
    owner._lock = Lock()
    owner._release_lock = Lock()
    owner._close_condition = Condition(owner._lock)
    owner._loop = StoppedLoop()
    owner._context = None
    owner._futures = set()
    owner._submissions = {}
    owner._failed_submissions = module.deque()
    owner._callbackless_submissions = {}
    owner._submission_callbacks_inflight = 0
    owner._closed = False

    with pytest.raises(RuntimeError, match="callback registration failed before start"):
        owner.submit(lambda _context: asyncio.sleep(0))

    assert len(created) == 1
    assert created[0].cancelled()
    assert reservation.release_calls == 1
    assert not owner._futures
    assert not owner._submissions
    assert not owner._callbackless_submissions
    assert owner._submission_callbacks_inflight == 0


def test_lookahead_timed_out_close_auto_resumes_after_last_admission(
    native_stub: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.pipeline import partition_lookahead as module

    entered = Event()
    allow = Event()
    sentinel = object()
    owner = object.__new__(module.PartitionSourceLookahead)
    owner._pid = os.getpid()
    owner.enabled = True
    owner._close_lock = Lock()
    owner._close_condition = Condition(owner._close_lock)
    owner._close_in_progress = False
    owner._submissions_inflight = 0
    owner._protocol_violations = 0
    owner._consumer_inflight = False
    owner._close_started = False
    owner._late_close_registered = False
    owner._closed = False
    owner._armed = None
    owner._future = None
    owner._future_context = None
    owner._executor = None
    owner._close_timeout_seconds = 0.03

    monkeypatch.setattr(module.PartitionSourceLookahead, "_current_options", lambda self: object())

    def prepare(self: Any, _plan: Any, _options: Any) -> object:
        entered.set()
        assert allow.wait(SCHEDULER_TIMEOUT_SECONDS)
        return sentinel

    monkeypatch.setattr(module.PartitionSourceLookahead, "_prepare_with_new_context", prepare)
    monkeypatch.setattr(
        module.PartitionSourceLookahead,
        "_materialize_deferred",
        lambda self, value, options=None: value,
    )

    results: list[Any] = []
    worker = Thread(target=lambda: results.append(owner.prepare_first(object())))
    worker.start()
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    with pytest.raises(RuntimeError, match="admissions exceeded"):
        owner.close()
    assert owner._close_started
    assert not owner._closed

    allow.set()
    worker.join(SCHEDULER_TIMEOUT_SECONDS)
    assert not worker.is_alive()
    assert results == [sentinel]
    assert owner._closed
    assert owner._submissions_inflight == 0
