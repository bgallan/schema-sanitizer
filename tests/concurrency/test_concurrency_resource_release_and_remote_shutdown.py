"""Regression coverage for concurrency resource release and remote shutdown."""

from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator
from schema_sanitizer.core_impl.memory_budget import (
    OperationMemoryLedger,
    process_resident_memory_snapshot,
)
from schema_sanitizer.core_impl.temporary_storage import TemporaryStoragePermitPool
from schema_sanitizer.errors import SchemaSanitizerResourceError
from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator
from schema_sanitizer.remote_impl.provider_session_pool import RemoteProviderSessionPool


def test_operation_memory_lease_release_is_thread_safe() -> None:
    """Competing cleanup threads release one resident reservation exactly once."""
    baseline = process_resident_memory_snapshot().reserved_bytes
    ledger = OperationMemoryLedger(64 << 20)
    lease = ledger.acquire(8 << 20, stage="lease_race")
    barrier = threading.Barrier(17)

    def release() -> None:
        """Race one lease release after every thread reaches the barrier."""
        barrier.wait()
        lease.release()

    threads = [threading.Thread(target=release) for _ in range(16)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()

    assert lease.reserved_bytes == 0
    assert ledger.snapshot().reserved_bytes == 0
    assert process_resident_memory_snapshot().reserved_bytes == baseline
    ledger.close()


def test_temporary_storage_lease_release_is_thread_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent release cannot underflow bytes or active-lease accounting."""
    monkeypatch.setattr(
        TemporaryStoragePermitPool,
        "_ensure_filesystem_capacity",
        staticmethod(lambda *_args, **_kwargs: None),
    )
    pool = TemporaryStoragePermitPool(64 << 20)
    lease = pool.acquire(8 << 20, label="release race")
    barrier = threading.Barrier(17)

    def release() -> None:
        """Race one temporary-storage release."""
        barrier.wait()
        lease.release()

    threads = [threading.Thread(target=release) for _ in range(16)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()

    snapshot = pool.snapshot()
    assert snapshot.reserved_bytes == 0
    assert snapshot.active_leases == 0
    assert lease.reserved_bytes == 0
    pool.close()


def test_temporary_storage_resize_and_release_are_serialized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A resize racing final cleanup leaves no retained permit or drift."""
    monkeypatch.setattr(
        TemporaryStoragePermitPool,
        "_ensure_filesystem_capacity",
        staticmethod(lambda *_args, **_kwargs: None),
    )
    for _ in range(64):
        pool = TemporaryStoragePermitPool(64 << 20)
        lease = pool.acquire(4 << 20, label="resize race")
        barrier = threading.Barrier(3)
        resize_errors: list[BaseException] = []

        def resize() -> None:
            """Race growth against release."""
            barrier.wait()
            try:
                lease.resize(12 << 20)
            except RuntimeError as exc:
                resize_errors.append(exc)

        def release() -> None:
            """Race release against growth."""
            barrier.wait()
            lease.release()

        resize_thread = threading.Thread(target=resize)
        release_thread = threading.Thread(target=release)
        resize_thread.start()
        release_thread.start()
        barrier.wait()
        resize_thread.join(timeout=2)
        release_thread.join(timeout=2)
        assert not resize_thread.is_alive()
        assert not release_thread.is_alive()
        assert len(resize_errors) <= 1
        snapshot = pool.snapshot()
        assert snapshot.reserved_bytes == 0
        assert snapshot.active_leases == 0
        pool.close()


def test_process_resident_ledger_blocks_cross_operation_oversubscription() -> None:
    """Independent operation limits cannot jointly exceed safe process memory."""
    baseline = process_resident_memory_snapshot()
    available = baseline.capacity_bytes - baseline.reserved_bytes
    if available < 4:
        pytest.skip("process resident ledger has no test headroom")
    first_amount = max(1, (available * 3) // 4)
    second_amount = available - first_amount + 1
    first = OperationMemoryLedger(baseline.capacity_bytes)
    second = OperationMemoryLedger(baseline.capacity_bytes)
    first_lease = first.acquire(first_amount, stage="first_operation")
    try:
        with pytest.raises(SchemaSanitizerResourceError) as raised:
            second.acquire(second_amount, stage="second_operation")
        assert raised.value.detail["limit_name"] == "process_resident_memory_bytes"
        assert raised.value.detail["limit_bytes"] == baseline.capacity_bytes
        assert process_resident_memory_snapshot().reserved_bytes == (
            baseline.reserved_bytes + first_amount
        )
    finally:
        first_lease.release()
        first.close()
        second.close()
    assert process_resident_memory_snapshot().reserved_bytes == baseline.reserved_bytes


def test_remote_close_deadline_includes_cancelled_future_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancellation-resistant coroutine cannot block close before shutdown."""
    from schema_sanitizer.remote_impl import io_coordinator as io_coordinator_module

    started = threading.Event()
    release = threading.Event()
    coordinator = RemoteIoCoordinator(shutdown_timeout_seconds=1.0)
    coordinator._shutdown_timeout_seconds = 0.05  # noqa: SLF001

    async def stubborn(_context: object) -> None:
        """Ignore cancellation until the test permits final thread cleanup."""
        started.set()
        while not release.is_set():
            try:
                await asyncio.sleep(0.005)
            except asyncio.CancelledError:
                continue

    coordinator.submit(stubborn)
    assert started.wait(timeout=1)
    original_monotonic = io_coordinator_module.monotonic
    clock = iter((100.0, 101.0))
    observed: list[float] = []

    def expired_clock() -> float:
        """Advance directly from deadline creation to its expired state."""
        value = next(clock)
        observed.append(value)
        return value

    monkeypatch.setattr(io_coordinator_module, "monotonic", expired_clock)
    try:
        with pytest.raises(RuntimeError, match="shutdown exceeded its deadline"):
            coordinator.close()
    finally:
        monkeypatch.setattr(io_coordinator_module, "monotonic", original_monotonic)
    assert observed == [100.0, 101.0]
    assert coordinator._thread.is_alive()  # noqa: SLF001
    assert not release.is_set()
    release.set()
    coordinator.close()
    coordinator._thread.join(timeout=1)  # noqa: SLF001
    assert not coordinator._thread.is_alive()  # noqa: SLF001


def test_remote_close_from_owned_thread_fails_without_deadlock() -> None:
    """A provider callback cannot self-join the coordinator host thread."""
    coordinator = RemoteIoCoordinator()

    async def close_from_loop(_context: object) -> str:
        """Attempt the forbidden self-close on the owned loop."""
        with pytest.raises(RuntimeError, match="cannot close from its owned thread"):
            coordinator.close()
        return "alive"

    assert coordinator.submit(close_from_loop).result(timeout=1) == "alive"
    coordinator.close()


def test_concurrent_remote_close_waits_for_the_owner() -> None:
    """A second close caller observes completed provider cleanup, not early return."""
    exit_started = threading.Event()
    exit_finished = threading.Event()
    release_exit = asyncio.Event()

    @asynccontextmanager
    async def context():
        """Delay provider cleanup long enough for close calls to overlap."""
        try:
            yield object()
        finally:
            exit_started.set()
            await release_exit.wait()
            exit_finished.set()

    coordinator = RemoteIoCoordinator(context, shutdown_timeout_seconds=1.0)
    errors: list[BaseException] = []
    completions: list[bool] = []
    barrier = threading.Barrier(3)

    def close() -> None:
        """Race one coordinator close call."""
        barrier.wait()
        try:
            coordinator.close()
            completions.append(exit_finished.is_set())
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=close) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    assert exit_started.wait(timeout=1)
    assert completions == []
    coordinator._loop.call_soon_threadsafe(release_exit.set)  # noqa: SLF001
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()

    assert errors == []
    assert completions == [True, True]


class _ClosingClient:
    """Track cleanup of a client created during pool shutdown."""

    def __init__(self) -> None:
        """Initialize close accounting."""
        self.close_calls = 0

    async def close(self) -> None:
        """Record final close."""
        self.close_calls += 1


class _ClosingManager:
    """Track entry and exit of a manager created during pool shutdown."""

    def __init__(self) -> None:
        """Initialize lifecycle accounting."""
        self.enter_calls = 0
        self.exit_calls = 0

    async def __aenter__(self) -> object:
        """Record entry and return one value."""
        self.enter_calls += 1
        return object()

    async def __aexit__(self, *_exc: object) -> None:
        """Record final exit."""
        self.exit_calls += 1


def test_provider_client_created_after_shutdown_is_closed_immediately() -> None:
    """A factory resuming after pool close cannot publish or leak its client."""

    async def exercise() -> _ClosingClient:
        """Close a pool while one key-local client factory is suspended."""
        pool = RemoteProviderSessionPool()
        await pool.__aenter__()
        started = asyncio.Event()
        release = asyncio.Event()
        client = _ClosingClient()

        async def factory() -> _ClosingClient:
            """Wait until shutdown has made the pool terminal."""
            started.set()
            await release.wait()
            return client

        borrow = asyncio.create_task(pool.borrow_client(("http", "late"), factory))
        await started.wait()
        await pool.__aexit__(None, None, None)
        release.set()
        with pytest.raises(RuntimeError, match="pool is closed"):
            await borrow
        return client

    client = asyncio.run(exercise())
    assert client.close_calls == 1


def test_provider_manager_entered_after_shutdown_is_exited_immediately() -> None:
    """A late manager entry is rolled back instead of escaping the closed pool."""

    async def exercise() -> _ClosingManager:
        """Close a pool while one manager factory is suspended."""
        pool = RemoteProviderSessionPool()
        await pool.__aenter__()
        started = asyncio.Event()
        release = asyncio.Event()
        manager = _ClosingManager()

        async def factory() -> _ClosingManager:
            """Wait until shutdown has made the pool terminal."""
            started.set()
            await release.wait()
            return manager

        borrow = asyncio.create_task(pool.borrow_manager(("s3", "late"), factory))
        await started.wait()
        await pool.__aexit__(None, None, None)
        release.set()
        with pytest.raises(RuntimeError, match="pool is closed"):
            await borrow
        return manager

    manager = asyncio.run(exercise())
    assert manager.enter_calls == 1
    assert manager.exit_calls == 1


def test_remote_prefetch_abandonment_is_bounded_and_closes_late_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Closing a shared prefetcher does not wait forever or leak late staging."""
    from schema_sanitizer.api_impl.source_plan import remote as remote_source

    started = threading.Event()
    release = threading.Event()
    staged_closed = threading.Event()
    coordinator = RemoteIoCoordinator(shutdown_timeout_seconds=0.05)

    class Staged:
        """Record cleanup of one staging result completed after abandonment."""

        def close(self) -> None:
            """Record staging cleanup."""
            staged_closed.set()

    class Session:
        """Provide a lightweight shared staging session."""

        async def __aenter__(self) -> Session:
            """Return the session."""
            return self

        async def __aexit__(self, *_exc: object) -> None:
            """Close the session."""

    class Manifest:
        """Expose one cancellation-resistant remote chunk."""

        files = (object(),)
        chunk_size = 1
        memory_limit_bytes = 64 << 20
        threading_mode = "multi"
        operation_context = SimpleNamespace(remote_coordinator=coordinator)

        def open_staging_session(self) -> Session:
            """Create the explicit shared session."""
            return Session()

        async def stage_chunk_async(self, _start: int, _session: Session) -> Staged:
            """Delay successful staging until after iterator close returns."""
            started.set()
            while not release.is_set():
                try:
                    await asyncio.sleep(0.005)
                except asyncio.CancelledError:
                    continue
            return Staged()

    iterator = RemoteChunkPrefetchIterator(Manifest())
    iterator.__enter__()
    assert started.wait(timeout=1)
    close_timeouts: list[float] = []

    def bounded_close(_closer: object, *, timeout_seconds: float) -> bool:
        """Model an exhausted session deadline without waiting on wall time."""
        close_timeouts.append(timeout_seconds)
        return False

    monkeypatch.setattr(remote_source, "remaining_seconds", lambda _deadline: 0.05)
    monkeypatch.setattr(remote_source.SharedDownloadSessionCloser, "close", bounded_close)
    iterator.close()
    assert close_timeouts == [0.05]
    assert not staged_closed.is_set()
    assert not release.is_set()
    release.set()
    assert staged_closed.wait(timeout=1)
    coordinator.close()
