"""Exercises iterative closed-loop delivery plus asynchronous retry accounting,
cancellation-graph walking, registry compaction, prefetch cursor or lease cleanup,
native option caches, inline storage release, and pressure reset after fork. Delivery
and cancellation never recurse, successful work is not replayed after telemetry failure,
and consumed or pending chunks close without losing cursor state."""

from __future__ import annotations

import asyncio
import os
import select
from collections import OrderedDict, deque
from concurrent.futures import Future
from threading import Condition, RLock
from types import SimpleNamespace
from typing import Any

import pytest
from _support.synchronization import (
    SCHEDULER_TIMEOUT_SECONDS,
    run_isolated_python_probe,
    wait_for_process_exit,
)

_NATIVE_STUB_MODULES = (
    "schema_sanitizer.core_impl.native_options",
    "schema_sanitizer.core_impl.native_symbols",
    "schema_sanitizer.api_impl.input.directory_preparation",
    "schema_sanitizer.api_impl.source_plan.attached",
    "schema_sanitizer.api_impl.source_plan.remote",
)


def test_remote_io_closed_loop_delivery_storm_is_iterative() -> None:
    """Thousands of failed cross-loop deliveries must not recurse through release."""
    from schema_sanitizer.remote_impl import io_permits as module

    class ClosedLoop:
        def call_soon_threadsafe(self, *_args: object, **_kwargs: object) -> None:
            """Reject scheduling through the forbidden thread-safe callback."""
            raise RuntimeError("event loop is closed")

    governor = module.RemoteIoPermitGovernor(capacity=1, max_waiters=1200)
    with governor._lock:
        for index in range(1100):
            governor._enqueue_waiter_locked(
                module._Waiter(ClosedLoop(), SimpleNamespace(), 1, "label", f"operation-{index}")
            )
        deliveries = governor._grant_ready_locked()

    governor._deliver(deliveries)
    snapshot = governor.snapshot()
    assert snapshot.in_use == 0
    assert snapshot.waiting == 0
    assert snapshot.delivery_failures == 1100


@pytest.mark.parametrize("failure", [asyncio.CancelledError, KeyboardInterrupt])
def test_retry_async_neutrally_releases_throttle_on_base_exception(
    monkeypatch: pytest.MonkeyPatch,
    failure: type[BaseException],
) -> None:
    """Task cancellation and control-flow exceptions cannot retain endpoint slots."""
    from schema_sanitizer.core_impl import async_scheduler
    from schema_sanitizer.remote_impl import provider_throttle

    class Lease:
        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.releases = 0
            self.failures = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.releases += 1

        def failure(self, _exc: BaseException) -> None:
            """Count a throttle failure notification."""
            self.failures += 1

        def success(self) -> None:
            """Reject an unexpected success-path invocation."""
            raise AssertionError("operation did not succeed")

    lease = Lease()

    async def acquire(_key: str) -> Lease:
        """Return the controlled permit acquisition result."""
        return lease

    async def operation() -> None:
        """Raise the deliberate failure for the operation path."""
        raise failure()

    monkeypatch.setattr(provider_throttle, "acquire_provider_request", acquire)
    with pytest.raises(failure):
        asyncio.run(
            async_scheduler.retry_async(
                operation,
                retries=3,
                throttle_key="provider",
            )
        )
    assert lease.releases == 1
    assert lease.failures == 0


def test_retry_async_does_not_repeat_success_when_success_accounting_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A telemetry failure after success cannot replay a non-idempotent operation."""
    from schema_sanitizer.core_impl import async_scheduler
    from schema_sanitizer.remote_impl import provider_throttle

    calls = 0

    class Lease:
        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.failures = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            raise AssertionError("successful operations are not neutrally released")

        def failure(self, _exc: BaseException) -> None:
            """Count a throttle failure notification."""
            self.failures += 1

        def success(self) -> None:
            """Reject an unexpected success-path invocation."""
            raise RuntimeError("success telemetry failed")

    lease = Lease()

    async def acquire(_key: str) -> Lease:
        """Return the controlled permit acquisition result."""
        return lease

    async def operation() -> str:
        """Run the controlled operation under test."""
        nonlocal calls
        calls += 1
        return "committed"

    monkeypatch.setattr(provider_throttle, "acquire_provider_request", acquire)
    with pytest.raises(RuntimeError, match="success telemetry failed"):
        asyncio.run(
            async_scheduler.retry_async(
                operation,
                retries=3,
                throttle_key="provider",
            )
        )
    assert calls == 1
    assert lease.failures == 0


def test_bounded_wait_checks_external_cancellation_once() -> None:
    """One wait poll must not duplicate arbitrary external-event callbacks."""
    from schema_sanitizer.core_impl.cancellation import (
        OperationCancellationToken,
        activate_operation_cancellation_token,
        bounded_wait_timeout,
    )

    class EventProbe:
        def __init__(self) -> None:
            """Initialize the event probe test double."""
            self.calls = 0

        def is_set(self) -> bool:
            """Count the cancellation probe and report that cancellation is unset."""
            self.calls += 1
            return False

    event = EventProbe()
    token = OperationCancellationToken(external_event=event)
    with activate_operation_cancellation_token(token):
        assert bounded_wait_timeout(0.25) == 0.25
    assert event.calls == 1


def test_cancellation_parent_walk_is_iterative_and_cycle_safe() -> None:
    """Deep or malformed public token chains cannot overflow the Python stack."""
    from schema_sanitizer.core_impl.cancellation import OperationCancellationToken

    root = OperationCancellationToken()
    current = root
    for _index in range(5000):
        current = OperationCancellationToken(_parent=current)
    root.cancel()
    assert current.cancelled()

    cyclic = OperationCancellationToken()
    cyclic._parent = cyclic
    assert cyclic.cancelled()


def test_operation_registry_snapshot_uses_the_exported_type() -> None:
    """The snapshot constructor and exported dataclass must have one identity."""
    from schema_sanitizer.core_impl import operation_diagnostics as module

    snapshot = module.operation_diagnostic_registry_snapshot()
    assert isinstance(snapshot, module.OperationDiagnosticRegistrySnapshot)


def test_operation_registry_compacts_attacker_sized_ids() -> None:
    """Live callbacks and completed history must not retain huge operation IDs."""
    from schema_sanitizer.core_impl import operation_diagnostics as module

    module._reset_after_fork()
    huge = "operation-" + ("x" * 2_000_000)

    class Source:
        def snapshot(self) -> dict[str, object]:
            """Return a snapshot of the state recorded by the test double."""
            return {"state": "running"}

    source = Source()
    module.register_operation(huge, source.snapshot)
    assert len(module._LIVE) == 1
    retained_key = next(iter(module._LIVE))
    assert len(retained_key) <= module._MAX_RETAINED_OPERATION_ID_CHARS
    live = module.process_operation_diagnostics(huge)
    assert live[0]["operation_id"] == retained_key

    module.complete_operation(huge, {"operation_id": huge, "state": "done"})
    completed = module.process_operation_diagnostics(huge)
    assert completed[-1]["operation_id"] == retained_key


def _bare_remote_prefetch_iterator(module: Any, manifest: Any) -> Any:
    """Construct a remote prefetch iterator without starting background work."""
    iterator = object.__new__(module.RemoteChunkPrefetchIterator)
    iterator._pid = os.getpid()
    iterator._manifest = manifest
    iterator._policy = SimpleNamespace(async_concurrency=1)
    iterator._prefetch_chunks = 1
    iterator._io_chunk_bytes = 1
    iterator._coordinator = object()
    iterator._owns_coordinator = False
    iterator._download_session = None
    iterator._futures = deque()
    iterator._failed_storage_leases = deque()
    iterator._callbackless_storage_futures = {}
    iterator._next_start = 0
    iterator._close_lock = RLock()
    iterator._close_condition = Condition(iterator._close_lock)
    iterator._close_in_progress = False
    iterator._cleanup_callbacks_inflight = 0
    iterator._admissions_inflight = 0
    iterator._consumers_inflight = 0
    iterator._protocol_violations = 0
    iterator._starting = False
    iterator._fill_in_progress = False
    iterator._close_started = False
    iterator._session_closer = None
    iterator._closed = False
    iterator._started = True
    iterator._remote_timeout_seconds = 0.1
    iterator._finalizer_ticket = None
    iterator._finalizer_capsule = None
    return iterator


def test_remote_prefetch_next_chunk_failure_releases_lease_and_preserves_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Planning failure cannot leak disk capacity or skip a chunk on retry."""
    from schema_sanitizer.api_impl.source_plan import remote as module

    class Lease:
        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.releases = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.releases += 1

    lease = Lease()
    manifest = SimpleNamespace(
        files=(object(),),
        try_acquire_storage_lease=lambda _start: lease,
        next_chunk_start=lambda _start: (_ for _ in ()).throw(RuntimeError("planning failed")),
    )
    iterator = _bare_remote_prefetch_iterator(module, manifest)
    monkeypatch.setattr(module, "_adaptive_parallel_slots", lambda *_a, **_k: 1)

    with pytest.raises(RuntimeError, match="planning failed"):
        iterator._fill_prefetch_window()
    assert lease.releases == 1
    assert iterator._next_start == 0
    assert not iterator._futures


def test_remote_prefetch_submission_failure_preserves_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Admission overload cannot advance the manifest before submission commits."""
    from schema_sanitizer.api_impl.source_plan import remote as module

    manifest = SimpleNamespace(
        files=(object(),),
        chunk_size=1,
        next_chunk_start=lambda start: start + 1,
    )
    iterator = _bare_remote_prefetch_iterator(module, manifest)
    iterator._coordinator = None
    monkeypatch.setattr(module, "_adaptive_parallel_slots", lambda *_a, **_k: 1)
    monkeypatch.setattr(
        iterator,
        "_submit_stage",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("submission failed")),
    )

    with pytest.raises(RuntimeError, match="submission failed"):
        iterator._fill_prefetch_window()
    assert iterator._next_start == 0
    assert not iterator._futures


def test_remote_prefetch_cancelled_drain_still_exits_shared_session() -> None:
    """Cancelling the drain task must execute the separate session's __aexit__."""
    from schema_sanitizer.api_impl.source_plan import remote as module
    from schema_sanitizer.remote_impl.staged_ownership import StagedResultOwnership

    captured: list[Any] = []

    class Coordinator:
        shutdown_timeout_seconds = 0.001

        def submit(self, operation: Any, **_kwargs: object) -> Future[Any]:
            """Submit work through the coordinator test double."""
            captured.append(operation)
            return Future()

    class Session:
        def __init__(self) -> None:
            """Initialize the session test double."""
            self.exits = 0

        async def __aexit__(self, *_exc: object) -> None:
            """Exit the asynchronous context managed by the session test double and run cleanup."""
            self.exits += 1

    running: Future[Any] = Future()
    running.set_running_or_notify_cancel()
    setattr(running, "_schema_sanitizer_staged_ownership", StagedResultOwnership())
    session = Session()
    iterator = _bare_remote_prefetch_iterator(module, SimpleNamespace(files=()))
    iterator._futures = deque([running])
    iterator._coordinator = Coordinator()
    iterator._owns_coordinator = False
    iterator._download_session = session

    iterator.close()
    operation = captured[0]

    async def exercise() -> None:
        """Cancel prefetch during drain and verify shared-session cleanup."""
        task = asyncio.create_task(operation(None))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(exercise())
    assert session.exits == 1


def test_native_options_cache_reset_discards_inherited_capsules(
    native_stub: None,
) -> None:
    """A fork child must not retain an inherited locked cache or native capsules."""
    from schema_sanitizer.core_impl import native_options as module

    old_lock = module._PREPARED_OPTIONS_CACHE_LOCK
    module._PREPARED_OPTIONS_CACHE = OrderedDict([(b"one", object())])
    module._PREPARED_OPTIONS_CACHE_BYTES = 3
    module._prepare_options_cache_for_fork()
    module._reset_prepared_options_cache_after_fork()

    assert module._PREPARED_OPTIONS_CACHE == OrderedDict()
    assert module._PREPARED_OPTIONS_CACHE_BYTES == 0
    assert module._PREPARED_OPTIONS_CACHE_LOCK is not old_lock


def test_remote_inline_stage_base_exception_releases_storage_lease() -> None:
    """Inline control-flow failures must return their pre-acquired disk reservation."""
    from schema_sanitizer.api_impl.source_plan import remote as module

    class Lease:
        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.releases = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.releases += 1

    class Manifest:
        def stage_chunk(self, _start: int) -> object:
            """Stage one chunk through the controlled session."""
            raise KeyboardInterrupt("inline stage interrupted")

    lease = Lease()
    iterator = object.__new__(module.RemoteChunkPrefetchIterator)
    iterator._manifest = Manifest()
    iterator._coordinator = None

    future = iterator._submit_stage(0, lease)
    with pytest.raises(KeyboardInterrupt, match="inline stage interrupted"):
        future.result()
    assert lease.releases == 1


def _probe_native_options_actual_fork() -> None:
    """Exercise the native-options at-fork hook in a disposable process."""
    from schema_sanitizer.core_impl import native_runtime

    native_runtime.native_core = SimpleNamespace(options_catalog=lambda: ())
    from schema_sanitizer.core_impl import native_options as module

    module._PREPARED_OPTIONS_CACHE = OrderedDict([(b"inherited", object())])
    module._PREPARED_OPTIONS_CACHE_BYTES = 9
    module._PREPARED_OPTIONS_CACHE_LOCK.acquire()
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no branch - child has one bounded exit path
        os.close(read_fd)
        try:
            acquired = module._PREPARED_OPTIONS_CACHE_LOCK.acquire(timeout=0.5)
            if acquired:
                module._PREPARED_OPTIONS_CACHE_LOCK.release()
            payload = (
                f"{len(module._PREPARED_OPTIONS_CACHE)}:"
                f"{module._PREPARED_OPTIONS_CACHE_BYTES}:"
                f"{int(acquired)}"
            ).encode("ascii")
            os.write(write_fd, payload)
        except BaseException as exc:
            os.write(write_fd, f"error:{type(exc).__name__}".encode("ascii"))
        finally:
            os.close(write_fd)
            os._exit(0)

    os.close(write_fd)
    module._PREPARED_OPTIONS_CACHE_LOCK.release()
    try:
        readable, _, _ = select.select([read_fd], [], [], SCHEDULER_TIMEOUT_SECONDS)
        assert readable, "fork child did not report cache state"
        payload = os.read(read_fd, 128).decode("ascii")
    finally:
        os.close(read_fd)
        status = wait_for_process_exit(pid)
    assert os.waitstatus_to_exitcode(status) == 0
    assert payload == "0:0:1"


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_native_options_actual_fork_replaces_inherited_locked_cache() -> None:
    """The registered at-fork hook must replace a lock held by another thread."""
    run_isolated_python_probe(__file__, "_probe_native_options_actual_fork")


def test_remote_prefetch_refill_failure_closes_consumed_chunk() -> None:
    """A chunk not yet returned to the caller must retain a cleanup owner."""
    from schema_sanitizer.api_impl.source_plan import remote as module
    from schema_sanitizer.remote_impl.staged_ownership import StagedResultOwnership

    class Staged:
        def __init__(self) -> None:
            """Initialize the staged test double."""
            self.closes = 0

        def close(self) -> None:
            """Close the resources owned by the staged test double."""
            self.closes += 1

    staged = Staged()
    ownership = StagedResultOwnership()
    future: Future[Any] = Future()
    future.set_result(ownership.publish(staged))
    setattr(future, "_schema_sanitizer_staged_ownership", ownership)

    iterator = _bare_remote_prefetch_iterator(module, SimpleNamespace(files=()))
    iterator._futures = deque([future])
    iterator._ensure_started = lambda: None
    iterator._fill_prefetch_window = lambda: (_ for _ in ()).throw(RuntimeError("refill failed"))
    close_calls = 0

    def close() -> None:
        """Close the resource at the synchronization point under test."""
        nonlocal close_calls
        close_calls += 1
        iterator._closed = True

    iterator.close = close
    with pytest.raises(RuntimeError, match="refill failed"):
        next(iterator)
    assert staged.closes == 1
    assert close_calls == 1


def test_remote_prefetch_unexpected_stop_iteration_still_closes() -> None:
    """A provider-originated StopIteration cannot bypass iterator cleanup."""
    from schema_sanitizer.api_impl.source_plan import remote as module
    from schema_sanitizer.remote_impl.staged_ownership import StagedResultOwnership

    future: Future[Any] = Future()
    future.set_exception(StopIteration("provider stopped"))
    setattr(future, "_schema_sanitizer_staged_ownership", StagedResultOwnership())

    iterator = _bare_remote_prefetch_iterator(module, SimpleNamespace(files=()))
    iterator._futures = deque([future])
    iterator._ensure_started = lambda: None
    close_calls = 0

    def close() -> None:
        """Close the resource at the synchronization point under test."""
        nonlocal close_calls
        close_calls += 1
        iterator._closed = True

    iterator.close = close
    with pytest.raises(StopIteration, match="provider stopped"):
        next(iterator)
    assert close_calls == 1


def _probe_system_pressure_actual_fork() -> None:
    """Exercise the pressure sampler at-fork hook in a disposable process."""
    from schema_sanitizer.core_impl import system_pressure as module

    previous = (
        module._cached_at,
        module._cached,
        module._last_high,
        module._last_oom,
        module._last_scale_change,
    )
    module._cached_at = 123.0
    module._cached = module.SystemPressureSnapshot(0.125, 9.0, 2.0, 4, 1, 0.99)
    module._last_high = 4
    module._last_oom = 1
    module._last_scale_change = 122.0
    module._lock.acquire()
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no branch - child has one bounded exit path
        os.close(read_fd)
        try:
            acquired = module._lock.acquire(timeout=0.5)
            if acquired:
                module._lock.release()
            payload = (
                f"{module._cached.scale}:"
                f"{module._cached_at}:"
                f"{module._last_high}:"
                f"{module._last_oom}:"
                f"{module._last_scale_change}:"
                f"{int(acquired)}"
            ).encode("ascii")
            os.write(write_fd, payload)
        except BaseException as exc:
            os.write(write_fd, f"error:{type(exc).__name__}".encode("ascii"))
        finally:
            os.close(write_fd)
            os._exit(0)

    os.close(write_fd)
    module._lock.release()
    try:
        readable, _, _ = select.select([read_fd], [], [], SCHEDULER_TIMEOUT_SECONDS)
        assert readable, "fork child did not report pressure state"
        payload = os.read(read_fd, 256).decode("ascii")
    finally:
        os.close(read_fd)
        status = wait_for_process_exit(pid)
        with module._lock:
            (
                module._cached_at,
                module._cached,
                module._last_high,
                module._last_oom,
                module._last_scale_change,
            ) = previous
    assert os.waitstatus_to_exitcode(status) == 0
    assert payload == "1.0:0.0:0:0:0.0:1"


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_system_pressure_actual_fork_replaces_lock_and_hysteresis() -> None:
    """A fork child must not inherit a locked sampler or parent pressure history."""
    run_isolated_python_probe(__file__, "_probe_system_pressure_actual_fork")
