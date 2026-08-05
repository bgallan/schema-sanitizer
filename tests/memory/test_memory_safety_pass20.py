"""Regressions for recursive delivery, cancellation, and transactional prefetch."""

from __future__ import annotations

import asyncio
import os
import select
import sys
from collections import OrderedDict, deque
from concurrent.futures import Future
from types import SimpleNamespace
from typing import Any

import pytest

_NATIVE_STUB_MODULES = (
    "schema_sanitizer.core_impl.native_options",
    "schema_sanitizer.core_impl.native_symbols",
    "schema_sanitizer.api_impl.input.directory_preparation",
    "schema_sanitizer.api_impl.source_plan.attached",
    "schema_sanitizer.api_impl.source_plan.remote",
)


def _purge_module(name: str) -> None:
    """Remove one module and its cached parent-package attribute."""
    sys.modules.pop(name, None)
    parent_name, _, attribute = name.rpartition(".")
    parent = sys.modules.get(parent_name)
    if parent is not None and hasattr(parent, attribute):
        delattr(parent, attribute)


@pytest.fixture
def native_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide isolated import-time native metadata for Python-only lifecycle tests."""
    from schema_sanitizer.core_impl import native_runtime

    class Stub:
        """Minimal native metadata provider."""

        def options_catalog(self) -> tuple[object, ...]:
            """Return an empty option catalog."""
            return ()

        def __getattr__(self, _name: str) -> Any:
            """Return no-op native entry points."""
            return lambda *_args, **_kwargs: None

    real_native = native_runtime.native_core
    preexisting_modules = set(sys.modules)
    sentinel = object()
    saved = {name: sys.modules.get(name, sentinel) for name in _NATIVE_STUB_MODULES}
    monkeypatch.setattr(native_runtime, "native_core", Stub())
    for name in reversed(_NATIVE_STUB_MODULES):
        _purge_module(name)
    try:
        yield
    finally:
        native_runtime.native_core = real_native
        created_modules = sorted(
            (
                name
                for name in tuple(sys.modules)
                if name.startswith("schema_sanitizer.") and name not in preexisting_modules
            ),
            key=lambda name: name.count("."),
            reverse=True,
        )
        for name in created_modules:
            _purge_module(name)
        for name in reversed(_NATIVE_STUB_MODULES):
            _purge_module(name)
        for name, module in saved.items():
            if module is sentinel:
                continue
            sys.modules[name] = module
            parent_name, _, attribute = name.rpartition(".")
            parent = sys.modules.get(parent_name)
            if parent is not None:
                setattr(parent, attribute, module)


def test_remote_io_closed_loop_delivery_storm_is_iterative() -> None:
    """Thousands of failed cross-loop deliveries must not recurse through release."""
    from schema_sanitizer.remote_impl import io_permits as module

    class ClosedLoop:
        def call_soon_threadsafe(self, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError("event loop is closed")

    governor = module.RemoteIoPermitGovernor(capacity=1, max_waiters=1200)
    with governor._lock:
        for index in range(1100):
            governor._waiters.append(
                module._Waiter(
                    ClosedLoop(),
                    SimpleNamespace(),
                    1,
                    "label",
                    f"operation-{index}",
                )
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
            self.releases = 0
            self.failures = 0

        def release(self) -> None:
            self.releases += 1

        def failure(self, _exc: BaseException) -> None:
            self.failures += 1

        def success(self) -> None:
            raise AssertionError("operation did not succeed")

    lease = Lease()

    async def acquire(_key: str) -> Lease:
        return lease

    async def operation() -> None:
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
            self.failures = 0

        def release(self) -> None:
            raise AssertionError("successful operations are not neutrally released")

        def failure(self, _exc: BaseException) -> None:
            self.failures += 1

        def success(self) -> None:
            raise RuntimeError("success telemetry failed")

    lease = Lease()

    async def acquire(_key: str) -> Lease:
        return lease

    async def operation() -> str:
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
            self.calls = 0

        def is_set(self) -> bool:
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
    iterator = object.__new__(module.RemoteChunkPrefetchIterator)
    iterator._manifest = manifest
    iterator._policy = SimpleNamespace(async_concurrency=1)
    iterator._prefetch_chunks = 1
    iterator._io_chunk_bytes = 1
    iterator._coordinator = object()
    iterator._owns_coordinator = False
    iterator._download_session = None
    iterator._futures = deque()
    iterator._next_start = 0
    iterator._closed = False
    iterator._started = True
    return iterator


def test_remote_prefetch_next_chunk_failure_releases_lease_and_preserves_cursor(
    monkeypatch: pytest.MonkeyPatch,
    native_stub: None,
) -> None:
    """Planning failure cannot leak disk capacity or skip a chunk on retry."""
    from schema_sanitizer.api_impl.source_plan import remote as module

    class Lease:
        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    lease = Lease()
    manifest = SimpleNamespace(
        files=(object(),),
        try_acquire_storage_lease=lambda _start: lease,
        next_chunk_start=lambda _start: (_ for _ in ()).throw(RuntimeError("planning failed")),
    )
    iterator = _bare_remote_prefetch_iterator(module, manifest)
    monkeypatch.setattr(module, "adaptive_concurrency_target", lambda *_a, **_k: 1)

    with pytest.raises(RuntimeError, match="planning failed"):
        iterator._fill_prefetch_window()
    assert lease.releases == 1
    assert iterator._next_start == 0
    assert not iterator._futures


def test_remote_prefetch_submission_failure_preserves_cursor(
    monkeypatch: pytest.MonkeyPatch,
    native_stub: None,
) -> None:
    """Admission overload cannot advance the manifest before submission commits."""
    from schema_sanitizer.api_impl.source_plan import remote as module

    manifest = SimpleNamespace(files=(object(),), chunk_size=1)
    iterator = _bare_remote_prefetch_iterator(module, manifest)
    monkeypatch.setattr(module, "adaptive_concurrency_target", lambda *_a, **_k: 1)
    monkeypatch.setattr(
        iterator,
        "_submit_stage",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("submission failed")),
    )

    with pytest.raises(RuntimeError, match="submission failed"):
        iterator._fill_prefetch_window()
    assert iterator._next_start == 0
    assert not iterator._futures


def test_remote_prefetch_cancelled_drain_still_exits_shared_session(
    native_stub: None,
) -> None:
    """Cancelling the drain task must execute the separate session's __aexit__."""
    from schema_sanitizer.api_impl.source_plan import remote as module
    from schema_sanitizer.remote_impl.io_coordinator import StagedResultOwnership

    captured: list[Any] = []

    class Coordinator:
        shutdown_timeout_seconds = 0.001

        def submit(self, operation: Any, **_kwargs: object) -> Future[Any]:
            captured.append(operation)
            return Future()

    class Session:
        def __init__(self) -> None:
            self.exits = 0

        async def __aexit__(self, *_exc: object) -> None:
            self.exits += 1

    running: Future[Any] = Future()
    running.set_running_or_notify_cancel()
    setattr(running, "_schema_sanitizer_staged_ownership", StagedResultOwnership())
    session = Session()
    iterator = object.__new__(module.RemoteChunkPrefetchIterator)
    iterator._closed = False
    iterator._futures = deque([running])
    iterator._coordinator = Coordinator()
    iterator._owns_coordinator = False
    iterator._download_session = session

    iterator.close()
    operation = captured[0]

    async def exercise() -> None:
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
    module._reset_prepared_options_cache_after_fork()

    assert module._PREPARED_OPTIONS_CACHE == OrderedDict()
    assert module._PREPARED_OPTIONS_CACHE_BYTES == 0
    assert module._PREPARED_OPTIONS_CACHE_LOCK is not old_lock


def test_remote_inline_stage_base_exception_releases_storage_lease(
    native_stub: None,
) -> None:
    """Inline control-flow failures must return their pre-acquired disk reservation."""
    from schema_sanitizer.api_impl.source_plan import remote as module

    class Lease:
        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    class Manifest:
        def stage_chunk(self, _start: int) -> object:
            raise KeyboardInterrupt("inline stage interrupted")

    lease = Lease()
    iterator = object.__new__(module.RemoteChunkPrefetchIterator)
    iterator._manifest = Manifest()
    iterator._coordinator = None

    future = iterator._submit_stage(0, lease)
    with pytest.raises(KeyboardInterrupt, match="inline stage interrupted"):
        future.result()
    assert lease.releases == 1


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_native_options_actual_fork_replaces_inherited_locked_cache(
    native_stub: None,
) -> None:
    """The registered at-fork hook must replace a lock held by another thread."""
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
        readable, _, _ = select.select([read_fd], [], [], 2.0)
        assert readable, "fork child did not report cache state"
        payload = os.read(read_fd, 128).decode("ascii")
    finally:
        os.close(read_fd)
        waited_pid, status = os.waitpid(pid, 0)
    assert waited_pid == pid
    assert os.waitstatus_to_exitcode(status) == 0
    assert payload == "0:0:1"


def test_remote_prefetch_refill_failure_closes_consumed_chunk(
    native_stub: None,
) -> None:
    """A chunk not yet returned to the caller must retain a cleanup owner."""
    from schema_sanitizer.api_impl.source_plan import remote as module
    from schema_sanitizer.remote_impl.io_coordinator import StagedResultOwnership

    class Staged:
        def __init__(self) -> None:
            self.closes = 0

        def close(self) -> None:
            self.closes += 1

    staged = Staged()
    ownership = StagedResultOwnership()
    future: Future[Any] = Future()
    future.set_result(ownership.publish(staged))
    setattr(future, "_schema_sanitizer_staged_ownership", ownership)

    iterator = object.__new__(module.RemoteChunkPrefetchIterator)
    iterator._started = True
    iterator._closed = False
    iterator._futures = deque([future])
    iterator._remote_timeout_seconds = 0.1
    iterator._ensure_started = lambda: None
    iterator._fill_prefetch_window = lambda: (_ for _ in ()).throw(RuntimeError("refill failed"))
    close_calls = 0

    def close() -> None:
        nonlocal close_calls
        close_calls += 1
        iterator._closed = True

    iterator.close = close
    with pytest.raises(RuntimeError, match="refill failed"):
        next(iterator)
    assert staged.closes == 1
    assert close_calls == 1


def test_remote_prefetch_unexpected_stop_iteration_still_closes(
    native_stub: None,
) -> None:
    """A provider-originated StopIteration cannot bypass iterator cleanup."""
    from schema_sanitizer.api_impl.source_plan import remote as module
    from schema_sanitizer.remote_impl.io_coordinator import StagedResultOwnership

    future: Future[Any] = Future()
    future.set_exception(StopIteration("provider stopped"))
    setattr(future, "_schema_sanitizer_staged_ownership", StagedResultOwnership())

    iterator = object.__new__(module.RemoteChunkPrefetchIterator)
    iterator._started = True
    iterator._closed = False
    iterator._futures = deque([future])
    iterator._remote_timeout_seconds = 0.1
    iterator._ensure_started = lambda: None
    close_calls = 0

    def close() -> None:
        nonlocal close_calls
        close_calls += 1
        iterator._closed = True

    iterator.close = close
    with pytest.raises(StopIteration, match="provider stopped"):
        next(iterator)
    assert close_calls == 1


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_system_pressure_actual_fork_replaces_lock_and_hysteresis() -> None:
    """A fork child must not inherit a locked sampler or parent pressure history."""
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
        readable, _, _ = select.select([read_fd], [], [], 2.0)
        assert readable, "fork child did not report pressure state"
        payload = os.read(read_fd, 256).decode("ascii")
    finally:
        os.close(read_fd)
        waited_pid, status = os.waitpid(pid, 0)
        with module._lock:
            (
                module._cached_at,
                module._cached,
                module._last_high,
                module._last_oom,
                module._last_scale_change,
            ) = previous
    assert waited_pid == pid
    assert os.waitstatus_to_exitcode(status) == 0
    assert payload == "1.0:0.0:0:0:0.0:1"
