"""Regression coverage for memory coordinator close waits for submission callback registration."""

from __future__ import annotations

import ast
import asyncio
import os
import subprocess
import sys
from collections import deque
from concurrent.futures import Future
from pathlib import Path
from threading import Condition, Event, Lock, RLock, Thread
from types import SimpleNamespace
from typing import Any

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS, WaitObservedCondition

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="POSIX descriptor-relative filesystem hardening suite"
)

_NATIVE_STUB_MODULES = (
    "schema_sanitizer.core_impl.native_options",
    "schema_sanitizer.core_impl.execution_policy",
    "schema_sanitizer.api_impl.operation_context",
    "schema_sanitizer.api_impl.input.directory_preparation",
    "schema_sanitizer.api_impl.source_plan.attached",
    "schema_sanitizer.api_impl.source_plan.remote",
    "schema_sanitizer.pipeline.partition_lookahead",
)


class _DeadThread:
    ident = None

    def is_alive(self) -> bool:
        return False

    def join(self, timeout: float | None = None) -> None:
        return None


class _SubmissionOwner:
    def __init__(self) -> None:
        self.released = False

    def release(self) -> None:
        self.released = True


class _Governor:
    def __init__(self, owner: _SubmissionOwner) -> None:
        self.owner = owner

    def reserve_submission(self) -> _SubmissionOwner:
        return self.owner

    async def acquire(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("test coroutine must not execute")


class _StoppedLoop:
    def is_running(self) -> bool:
        return False

    def is_closed(self) -> bool:
        return True


class _BlockingCallbackFuture(Future[Any]):
    def __init__(self, coroutine: Any) -> None:
        super().__init__()
        self.coroutine = coroutine
        self.registration_entered = Event()
        self.allow_registration = Event()

    def add_done_callback(self, fn: Any, *, context: Any = None) -> None:
        self.registration_entered.set()
        assert self.allow_registration.wait(SCHEDULER_TIMEOUT_SECONDS)
        super().add_done_callback(fn)

    def cancel(self) -> bool:
        self.coroutine.close()
        return super().cancel()


def test_coordinator_close_waits_for_submission_callback_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl import io_coordinator as module

    owner = _SubmissionOwner()
    created: list[_BlockingCallbackFuture] = []
    future_created = Event()

    def submit_bridge(coroutine: Any, _loop: Any) -> _BlockingCallbackFuture:
        future = _BlockingCallbackFuture(coroutine)
        created.append(future)
        future_created.set()
        return future

    monkeypatch.setattr(module.asyncio, "run_coroutine_threadsafe", submit_bridge)
    coordinator = object.__new__(module.RemoteIoCoordinator)
    coordinator._pid = os.getpid()
    coordinator._operation_id = "coordinator-close-waits-for-submission-callback"
    coordinator._permit_governor = _Governor(owner)
    coordinator._shutdown_timeout_seconds = SCHEDULER_TIMEOUT_SECONDS
    coordinator._lock = Lock()
    coordinator._release_lock = Lock()
    coordinator._close_condition = WaitObservedCondition(coordinator._lock)
    coordinator._loop = _StoppedLoop()
    coordinator._context = None
    coordinator._futures = set()
    coordinator._submissions = {}
    coordinator._failed_submissions = module.deque()
    coordinator._failed_permits = module.deque()
    coordinator._callbackless_submissions = {}
    coordinator._deferred_terminal_callbacks = module.deque()
    coordinator._terminal_callback_owners = set()
    coordinator._failed_terminal_callbacks = module.deque()
    coordinator._shutdown_future = None
    coordinator._submission_callbacks_inflight = 0
    coordinator._closed = False
    coordinator._closing = False
    coordinator._close_complete = Event()
    coordinator._close_generation = 0
    coordinator._completed_close_generation = 0
    coordinator._close_results = {}
    coordinator._close_waiters = {}
    coordinator._close_error = None
    coordinator._protocol_violations = 0
    coordinator._permit_registration = None
    coordinator._thread_lease = None
    coordinator._runtime_registration = None
    coordinator._thread = _DeadThread()

    submitted: list[Future[Any]] = []
    submitter = Thread(
        target=lambda: submitted.append(coordinator.submit(lambda _context: asyncio.sleep(0)))
    )
    submitter.start()
    assert future_created.wait(SCHEDULER_TIMEOUT_SECONDS)
    future = created[0]
    assert future.registration_entered.wait(SCHEDULER_TIMEOUT_SECONDS)

    close_errors: list[BaseException] = []
    closer = Thread(target=lambda: _capture_error(coordinator.close, close_errors))
    closer.start()
    assert coordinator._close_condition.enter_observed.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert coordinator._submission_callbacks_inflight == 1
    assert closer.is_alive()
    assert not owner.released
    future.allow_registration.set()
    submitter.join(SCHEDULER_TIMEOUT_SECONDS)
    closer.join(SCHEDULER_TIMEOUT_SECONDS)
    assert not close_errors
    assert owner.released
    assert coordinator._submission_callbacks_inflight == 0


def _capture_error(call: Any, errors: list[BaseException]) -> None:
    try:
        call()
    except BaseException as exc:
        errors.append(exc)


def test_staged_path_refuses_to_delete_replacement(tmp_path: Path) -> None:
    from schema_sanitizer.remote_impl.staging_paths import StagedPath

    path = tmp_path / "owned"
    path.write_text("original")

    class Lease:
        released = False

        def release(self) -> None:
            self.released = True

    lease = Lease()
    owner = StagedPath(str(path), storage_lease=lease)
    path.unlink()
    path.write_text("replacement")
    with pytest.raises(OSError, match="ownership changed"):
        owner.close()
    assert path.read_text() == "replacement"
    assert not lease.released
    assert not owner._closed


def test_janitor_deletes_dangling_symlink_before_releasing_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.core_impl import temporary_janitor as module

    link = tmp_path / "dangling"
    link.symlink_to(tmp_path / "missing")

    class Lease:
        calls = 0

        def release(self) -> None:
            self.calls += 1

    lease = Lease()
    janitor = module._TemporaryArtifactJanitor()
    quarantine = janitor.root()
    monkeypatch.setattr(janitor, "_ensure_worker", lambda: None)
    assert janitor.quarantine(link, is_dir=False, lease=lease)
    assert lease.calls == 0
    assert janitor.snapshot().pending_artifacts == 1
    janitor.sweep()
    assert lease.calls == 1
    assert janitor.snapshot().pending_artifacts == 0
    assert not any(quarantine.iterdir())


def test_janitor_retains_lease_when_quarantined_inode_is_replaced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.core_impl import temporary_janitor as module

    source = tmp_path / "source"
    source.write_text("owned")

    class Lease:
        released = False

        def release(self) -> None:
            self.released = True

    lease = Lease()
    janitor = module._TemporaryArtifactJanitor()
    quarantine = janitor.root()
    monkeypatch.setattr(janitor, "_ensure_worker", lambda: None)
    assert janitor.quarantine(source, is_dir=False, lease=lease)
    retained = next(iter(quarantine.iterdir()))
    retained.unlink()
    retained.write_text("replacement")
    janitor.sweep()
    snapshot = janitor.snapshot()
    assert retained.read_text() == "replacement"
    assert not lease.released
    assert snapshot.pending_artifacts == 1
    assert snapshot.identity_mismatches >= 1


class _Prepared:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _SynchronousCompletionFuture:
    def __init__(self, prepared: _Prepared) -> None:
        self.prepared = prepared
        self.completed = False

    def cancel(self) -> bool:
        return False

    def done(self) -> bool:
        return self.completed

    def add_done_callback(self, callback: Any) -> None:
        self.completed = True
        callback(self)

    def result(self) -> _Prepared:
        return self.prepared


class _Executor:
    def __init__(self) -> None:
        self.closed = False

    def shutdown(self, **_kwargs: Any) -> None:
        self.closed = True


def test_lookahead_close_does_not_reenter_under_callback_registration(
    native_stub: None,
) -> None:
    from schema_sanitizer.pipeline import partition_lookahead as module

    prepared = _Prepared()
    future = _SynchronousCompletionFuture(prepared)
    executor = _Executor()
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
    owner._future = future
    owner._future_context = None
    owner._executor = executor
    owner._close_timeout_seconds = SCHEDULER_TIMEOUT_SECONDS
    closer = Thread(target=owner.close)
    closer.start()
    closer.join(SCHEDULER_TIMEOUT_SECONDS)
    assert not closer.is_alive()
    assert prepared.closed
    assert executor.closed
    assert owner._closed


def test_session_entry_timeout_self_closes_without_second_submission() -> None:
    from schema_sanitizer.remote_impl.session_lifecycle import (
        enter_shared_download_session,
    )

    loop = asyncio.new_event_loop()
    started = Event()

    def run_loop() -> None:
        asyncio.set_event_loop(loop)
        started.set()
        loop.run_forever()

    thread = Thread(target=run_loop)
    thread.start()
    assert started.wait(SCHEDULER_TIMEOUT_SECONDS)

    class Coordinator:
        submissions = 0

        def submit(self, operation: Any) -> Future[Any]:
            self.submissions += 1

            async def invoke() -> Any:
                return await operation(None)

            return asyncio.run_coroutine_threadsafe(invoke(), loop)

    allow_entry = Event()
    exited = Event()

    class Session:
        async def __aenter__(self) -> object:
            await asyncio.get_running_loop().run_in_executor(None, allow_entry.wait)
            return self

        async def __aexit__(self, *_args: Any) -> None:
            exited.set()

    coordinator = Coordinator()
    try:
        with pytest.raises(TimeoutError):
            enter_shared_download_session(coordinator, Session(), timeout_seconds=0.02)
        allow_entry.set()
        assert exited.wait(SCHEDULER_TIMEOUT_SECONDS)
        assert coordinator.submissions == 1
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(SCHEDULER_TIMEOUT_SECONDS)
        loop.close()


def test_staged_result_concurrent_abandon_closes_once() -> None:
    from schema_sanitizer.remote_impl.staged_ownership import StagedResultOwnership

    entered = Event()
    release = Event()

    class Staged:
        calls = 0

        def close(self) -> None:
            self.calls += 1
            entered.set()
            assert release.wait(2)

    staged = Staged()
    owner = StagedResultOwnership()
    owner.publish(staged)
    owner._condition = WaitObservedCondition()
    owner._lock = owner._condition
    results: list[bool] = []
    first = Thread(target=lambda: results.append(owner.abandon()))
    second = Thread(target=lambda: results.append(owner.abandon()))
    first.start()
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    second.start()
    assert owner._condition.wait_entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    release.set()
    first.join(SCHEDULER_TIMEOUT_SECONDS)
    second.join(SCHEDULER_TIMEOUT_SECONDS)
    assert staged.calls == 1
    assert results == [True, True]


def test_provider_lease_construction_failure_does_not_consume_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl import provider_throttle as module

    governor = module.ProviderThrottleGovernor()

    class BrokenLease:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            raise MemoryError("lease allocation failed")

    monkeypatch.setattr(module, "ProviderRequestLease", BrokenLease)
    with pytest.raises(MemoryError):
        governor.try_acquire("provider")
    assert governor.snapshot("provider").in_flight == 0
    assert governor.registry_snapshot().construction_rollbacks == 1


def test_permit_scheduler_grants_many_operations_without_quadratic_scan() -> None:
    from schema_sanitizer.remote_impl import io_permits as module

    count = 4096
    governor = module.RemoteIoPermitGovernor(count, max_waiters=count + 1)
    loop = asyncio.new_event_loop()
    try:
        with governor._lock:
            for index in range(count):
                waiter = module._Waiter(
                    loop,
                    loop.create_future(),
                    1,
                    "coordinator-close-waits-for-submission-callback",
                    f"operation-{index}",
                )
                governor._enqueue_waiter_locked(waiter)
            candidate_calls = 0
            effective_weight_calls = 0
            original_candidate = governor._operation_candidate_locked
            original_effective_weight = governor._effective_weight

            def counted_candidate(*args: Any, **kwargs: Any) -> Any:
                nonlocal candidate_calls
                candidate_calls += 1
                return original_candidate(*args, **kwargs)

            def counted_effective_weight(*args: Any, **kwargs: Any) -> int:
                nonlocal effective_weight_calls
                effective_weight_calls += 1
                return original_effective_weight(*args, **kwargs)

            governor._operation_candidate_locked = counted_candidate  # type: ignore[method-assign]
            governor._effective_weight = counted_effective_weight  # type: ignore[method-assign]
            deliveries = governor._grant_ready_locked()
        assert len(deliveries) == count
        assert candidate_calls == count
        assert effective_weight_calls <= 4 * count
    finally:
        loop.close()


def test_primary_cleanup_gate_covers_coordinator_callback_boundaries() -> None:
    completed = subprocess.run(
        [sys.executable, "meta/ci/quality/check_primary_cleanup.py"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


class _RejectingCallbackFuture(Future[Any]):
    def __init__(self, coroutine: Any) -> None:
        super().__init__()
        self.coroutine = coroutine

    def add_done_callback(self, fn: Any, *, context: Any = None) -> None:
        raise RuntimeError("callback registration failed")

    def cancel(self) -> bool:
        return False

    def finish(self) -> None:
        self.coroutine.close()
        self.set_result(None)


def test_coordinator_retains_owner_when_callback_registration_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl import io_coordinator as module

    owner = _SubmissionOwner()
    created: list[_RejectingCallbackFuture] = []

    def submit_bridge(coroutine: Any, _loop: Any) -> _RejectingCallbackFuture:
        future = _RejectingCallbackFuture(coroutine)
        created.append(future)
        return future

    monkeypatch.setattr(module.asyncio, "run_coroutine_threadsafe", submit_bridge)
    coordinator = object.__new__(module.RemoteIoCoordinator)
    coordinator._pid = os.getpid()
    coordinator._operation_id = (
        "coordinator-close-waits-for-submission-callback-registration-failure"
    )
    coordinator._permit_governor = _Governor(owner)
    coordinator._shutdown_timeout_seconds = SCHEDULER_TIMEOUT_SECONDS
    coordinator._lock = Lock()
    coordinator._release_lock = Lock()
    coordinator._close_condition = WaitObservedCondition(coordinator._lock)
    coordinator._loop = _StoppedLoop()
    coordinator._context = None
    coordinator._futures = set()
    coordinator._submissions = {}
    coordinator._failed_submissions = module.deque()
    coordinator._failed_permits = module.deque()
    coordinator._callbackless_submissions = {}
    coordinator._deferred_terminal_callbacks = module.deque()
    coordinator._terminal_callback_owners = set()
    coordinator._failed_terminal_callbacks = module.deque()
    coordinator._shutdown_future = None
    coordinator._submission_callbacks_inflight = 0
    coordinator._closed = False
    coordinator._closing = False
    coordinator._close_complete = Event()
    coordinator._close_generation = 0
    coordinator._completed_close_generation = 0
    coordinator._close_results = {}
    coordinator._close_waiters = {}
    coordinator._close_error = None
    coordinator._protocol_violations = 0
    coordinator._permit_registration = None
    coordinator._thread_lease = None
    coordinator._runtime_registration = None
    coordinator._thread = _DeadThread()

    with pytest.raises(RuntimeError, match="callback registration failed"):
        coordinator.submit(lambda _context: asyncio.sleep(0))
    assert len(created) == 1
    assert not owner.released
    assert coordinator._submission_callbacks_inflight == 1
    coordinator._close_condition.enter_observed.clear()

    close_errors: list[BaseException] = []
    closer = Thread(target=lambda: _capture_error(coordinator.close, close_errors))
    closer.start()
    assert coordinator._close_condition.enter_observed.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert coordinator._submission_callbacks_inflight == 1
    assert closer.is_alive()
    created[0].finish()
    closer.join(SCHEDULER_TIMEOUT_SECONDS)
    assert not closer.is_alive()
    assert not close_errors
    assert owner.released
    assert not coordinator._callbackless_submissions
    assert coordinator._submission_callbacks_inflight == 0


def test_path_identity_survives_owned_content_updates(tmp_path: Path) -> None:
    from schema_sanitizer.core_impl.path_identity import (
        claim_path_identity,
        lstat_identity,
    )

    path = tmp_path / "mutable-owned-artifact"
    path.write_text("before")
    identity = claim_path_identity(path)
    assert identity is not None
    path.write_text("after")
    assert lstat_identity(path) == identity
    path.unlink()
    path.write_text("replacement")
    assert lstat_identity(path) != identity


def test_lookahead_close_waits_for_trigger_commit(
    native_stub: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.pipeline import partition_lookahead as module

    submit_entered = Event()
    allow_submit = Event()
    submitted_future: Future[Any] = Future()

    class ChildContext:
        closed = False

        def close(self) -> None:
            self.closed = True

    child = ChildContext()

    class ParentContext:
        memory_ledger = None

        def fork(self) -> ChildContext:
            return child

    class BlockingExecutor:
        shutdown_called = False

        def submit(self, *_args: Any, **_kwargs: Any) -> Future[Any]:
            submit_entered.set()
            assert allow_submit.wait(2)
            return submitted_future

        def shutdown(self, **_kwargs: Any) -> None:
            self.shutdown_called = True

    executor = BlockingExecutor()
    executor._thread_lease = object()
    owner = object.__new__(module.PartitionSourceLookahead)
    owner._pid = os.getpid()
    owner.enabled = True
    owner._memory_limit_bytes = 64 << 20
    owner._close_lock = Lock()
    owner._close_condition = WaitObservedCondition(owner._close_lock)
    owner._close_in_progress = False
    owner._submissions_inflight = 0
    owner._protocol_violations = 0
    owner._consumer_inflight = False
    owner._close_started = False
    owner._late_close_registered = False
    owner._closed = False
    owner._armed = (object(), ParentContext())
    owner._future = None
    owner._future_context = None
    owner._executor = executor
    owner._close_timeout_seconds = SCHEDULER_TIMEOUT_SECONDS
    owner._current_options = lambda: object()
    monkeypatch.setattr(module, "_adaptive_parallel_slots", lambda *_a, **_k: 2)
    monkeypatch.setattr(
        module,
        "acquire_stage_concurrency_admission",
        lambda *_a, **_k: SimpleNamespace(slots=2, close=lambda: None),
    )

    trigger = Thread(target=owner.trigger)
    trigger.start()
    assert submit_entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    closer = Thread(target=owner.close)
    closer.start()
    assert owner._close_condition.wait_entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert closer.is_alive()
    allow_submit.set()
    trigger.join(SCHEDULER_TIMEOUT_SECONDS)
    closer.join(SCHEDULER_TIMEOUT_SECONDS)
    assert not trigger.is_alive()
    assert not closer.is_alive()
    assert submitted_future.cancelled()
    assert child.closed
    assert executor.shutdown_called
    assert owner._closed
    assert owner._future is None
    assert owner._future_context is None


def test_prefetch_close_waits_for_external_storage_admission(
    native_stub: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.api_impl.source_plan import remote as module

    acquire_entered = Event()
    allow_acquire = Event()

    class Manifest:
        files = ("one",)
        chunk_size = 1

        @staticmethod
        def estimated_chunk_bytes(_start: int) -> int:
            return 1

        def try_acquire_storage_lease(self, _start: int) -> None:
            acquire_entered.set()
            assert allow_acquire.wait(2)
            return None

    owner = object.__new__(module.RemoteChunkPrefetchIterator)
    owner._pid = os.getpid()
    owner._manifest = Manifest()
    owner._policy = SimpleNamespace(async_concurrency=1)
    owner._prefetch_chunks = 1
    owner._remote_timeout_seconds = 1.0
    owner._io_chunk_bytes = 1
    owner._coordinator = SimpleNamespace(shutdown_timeout_seconds=1.0)
    owner._owns_coordinator = False
    owner._download_session = None
    owner._futures = deque()
    owner._failed_storage_leases = deque()
    owner._callbackless_storage_futures = {}
    owner._next_start = 0
    owner._close_lock = RLock()
    owner._close_condition = WaitObservedCondition(owner._close_lock)
    owner._close_in_progress = False
    owner._cleanup_callbacks_inflight = 0
    owner._admissions_inflight = 0
    owner._consumers_inflight = 0
    owner._protocol_violations = 0
    owner._starting = False
    owner._fill_in_progress = False
    owner._close_started = False
    owner._session_closer = None
    owner._finalizer_ticket = None
    owner._finalizer_capsule = None
    owner._closed = False
    owner._started = True
    monkeypatch.setattr(module, "_adaptive_parallel_slots", lambda *_a, **_k: 1)

    filler = Thread(target=owner._fill_prefetch_window)
    filler.start()
    assert acquire_entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    closer = Thread(target=owner.close)
    closer.start()
    assert owner._close_condition.wait_entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert closer.is_alive()
    allow_acquire.set()
    filler.join(SCHEDULER_TIMEOUT_SECONDS)
    closer.join(SCHEDULER_TIMEOUT_SECONDS)
    assert not filler.is_alive()
    assert not closer.is_alive()
    assert owner._closed
    assert owner._admissions_inflight == 0


def test_prefetch_close_waits_for_claimed_consumer(native_stub: None) -> None:
    from schema_sanitizer.api_impl.source_plan import remote as module
    from schema_sanitizer.remote_impl.staged_ownership import StagedResultOwnership

    class Staged:
        closed = False

        def close(self) -> None:
            self.closed = True

    staged = Staged()
    ownership = StagedResultOwnership()
    ownership.publish(staged)
    consumer_claimed = Event()

    class ClaimedFuture(Future[Any]):
        def result(self, timeout: float | None = None) -> Any:
            consumer_claimed.set()
            return super().result(timeout=timeout)

    future: Future[Any] = ClaimedFuture()
    setattr(future, "_schema_sanitizer_staged_ownership", ownership)

    owner = object.__new__(module.RemoteChunkPrefetchIterator)
    owner._pid = os.getpid()
    owner._remote_timeout_seconds = 1.0
    owner._coordinator = None
    owner._owns_coordinator = False
    owner._download_session = None
    owner._futures = deque((future,))
    owner._failed_storage_leases = deque()
    owner._callbackless_storage_futures = {}
    owner._close_lock = RLock()
    owner._close_condition = WaitObservedCondition(owner._close_lock)
    owner._close_in_progress = False
    owner._cleanup_callbacks_inflight = 0
    owner._admissions_inflight = 0
    owner._consumers_inflight = 0
    owner._protocol_violations = 0
    owner._starting = False
    owner._fill_in_progress = False
    owner._close_started = False
    owner._session_closer = None
    owner._finalizer_ticket = None
    owner._finalizer_capsule = None
    owner._closed = False
    owner._started = True
    owner._ensure_started = lambda: None
    owner._fill_prefetch_window = lambda: None

    consumed: list[Any] = []
    consumer = Thread(target=lambda: consumed.append(next(owner)))
    consumer.start()
    assert consumer_claimed.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert owner._consumers_inflight == 1

    closer = Thread(target=owner.close)
    closer.start()
    assert owner._close_condition.wait_entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert closer.is_alive()
    future.set_result(staged)
    consumer.join(SCHEDULER_TIMEOUT_SECONDS)
    closer.join(SCHEDULER_TIMEOUT_SECONDS)
    assert not consumer.is_alive()
    assert not closer.is_alive()
    assert consumed == [staged]
    assert not staged.closed
    assert owner._closed


def test_primary_cleanup_gate_inspects_error_branch_beside_helper() -> None:
    from meta.ci.quality import check_primary_cleanup as gate

    tree = ast.parse(
        """
def close_boundary(primary_error, owner):
    try:
        work()
    finally:
        if primary_error is None:
            _cleanup_with_note(primary_error, owner)
        else:
            owner.close()
"""
    )
    function = tree.body[0]
    assert isinstance(function, ast.FunctionDef)
    guarded = function.body[0]
    assert isinstance(guarded, ast.Try)
    unsafe = gate._unsafe_finally_calls(guarded.finalbody)
    assert [ast.unparse(call.func) for call in unsafe] == ["owner.close"]


class _CallbackRejectingFuture(Future[Any]):
    def add_done_callback(self, fn: Any, *, context: Any = None) -> None:
        raise RuntimeError("callback registration rejected")

    def cancel(self) -> bool:
        return False


def test_prefetch_retains_storage_owner_when_callback_registration_fails(
    native_stub: None,
) -> None:
    from schema_sanitizer.api_impl.source_plan import remote as module

    future = _CallbackRejectingFuture()

    class Coordinator:
        def submit(self, *_args: Any, **_kwargs: Any) -> Future[Any]:
            return future

    class Manifest:
        @staticmethod
        def estimated_chunk_bytes(_start: int) -> int:
            return 1

        async def stage_chunk_async(self, *_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("the synthetic Future owns execution")

    class Lease:
        calls = 0

        def release(self) -> None:
            self.calls += 1

    lease = Lease()
    owner = object.__new__(module.RemoteChunkPrefetchIterator)
    owner._pid = os.getpid()
    owner._manifest = Manifest()
    owner._policy = SimpleNamespace(async_concurrency=1)
    owner._io_chunk_bytes = 1
    owner._coordinator = Coordinator()
    owner._download_session = object()
    owner._failed_storage_leases = deque()
    owner._callbackless_storage_futures = {}
    owner._close_lock = RLock()
    owner._close_condition = Condition(owner._close_lock)
    owner._close_in_progress = False
    owner._cleanup_callbacks_inflight = 0
    owner._admissions_inflight = 0
    owner._consumers_inflight = 0
    owner._protocol_violations = 0
    owner._starting = False
    owner._fill_in_progress = False

    returned = owner._submit_stage(0, lease)
    assert returned is future
    assert future in owner._callbackless_storage_futures
    assert lease.calls == 0
    future.set_exception(RuntimeError("staging failed"))
    owner._complete_callbackless_storage_futures()
    assert lease.calls == 1
    assert not owner._callbackless_storage_futures


def test_lookahead_callback_registration_failure_remains_retryable(
    native_stub: None,
) -> None:
    from schema_sanitizer.pipeline import partition_lookahead as module

    prepared = _Prepared()
    future = _CallbackRejectingFuture()
    executor = _Executor()
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
    owner._future = future
    owner._future_context = None
    owner._executor = executor
    owner._close_timeout_seconds = SCHEDULER_TIMEOUT_SECONDS

    with pytest.raises(RuntimeError, match="callback registration rejected"):
        owner.close()
    assert not owner._late_close_registered
    assert owner._future is future
    assert not owner._closed
    future.set_result(prepared)
    owner.close()
    assert prepared.closed
    assert executor.closed
    assert owner._closed


def test_session_entry_interruption_publishes_abandon_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl import session_lifecycle as module

    class InterruptedEvent:
        def wait(self, timeout: float | None = None) -> bool:
            raise KeyboardInterrupt

        def set(self) -> None:
            return None

    attempt = SimpleNamespace(
        entered=InterruptedEvent(),
        decision=Future(),
        terminal=Event(),
        error=None,
    )
    monkeypatch.setattr(module, "_SessionEntryAttempt", lambda: attempt)

    class Coordinator:
        def submit(self, _operation: Any) -> Future[Any]:
            return Future()

    with pytest.raises(KeyboardInterrupt):
        module.enter_shared_download_session(Coordinator(), object(), timeout_seconds=1.0)
    assert attempt.decision.result(timeout=0) is False


def test_path_identity_rejects_second_owner_claim(tmp_path: Path) -> None:
    from schema_sanitizer.core_impl.path_identity import claim_path_identity

    path = tmp_path / "single-owner"
    path.write_text("owned")
    first = claim_path_identity(path)
    assert first is not None
    with pytest.raises(OSError, match="already owned"):
        claim_path_identity(path)


def test_staged_path_restores_replacement_captured_by_cleanup_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.remote_impl import staging_paths as module

    path = tmp_path / "owned-race"
    path.write_text("original")

    class Lease:
        released = False

        def release(self) -> None:
            self.released = True

    lease = Lease()
    owner = module.StagedPath(str(path), storage_lease=lease)
    real_replace = module.os.replace
    raced = False

    def replace_after_substitution(source: Any, target: Any) -> None:
        nonlocal raced
        if not raced:
            raced = True
            Path(source).unlink()
            Path(source).write_text("replacement")
        real_replace(source, target)

    monkeypatch.setattr(module.os, "replace", replace_after_substitution)
    with pytest.raises(OSError, match="replaced during cleanup transfer"):
        owner.close()
    assert path.read_text() == "replacement"
    assert not lease.released
    assert not owner._closed
    assert not list(tmp_path.glob(".schema-sanitizer-delete-*"))
