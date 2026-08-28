"""Regression coverage for concurrency resource finalization and deadlines."""

from __future__ import annotations

import asyncio
import gc
import threading
from pathlib import Path
from time import monotonic, sleep
from types import SimpleNamespace

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS

import schema_sanitizer as ss
from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator
from schema_sanitizer.core_impl.async_scheduler import ordered_indexed_results
from schema_sanitizer.core_impl.memory_budget import (
    OperationMemoryLedger,
    activate_operation_memory_ledger,
    process_resident_memory_snapshot,
)
from schema_sanitizer.core_impl.temporary_storage import (
    _PROCESS_TEMPORARY_STORAGE,
    TemporaryStoragePermitPool,
    process_temporary_storage_snapshot,
)
from schema_sanitizer.core_impl.temporary_storage_governor import _MINIMUM_FREE_BYTES
from schema_sanitizer.errors import SchemaSanitizerResourceError
from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator
from schema_sanitizer.remote_impl.transport import read_bounded_response_text
from schema_sanitizer.remote_impl.upload_policy import (
    read_upload_range,
    release_upload_payload,
)


def test_operation_memory_lease_resize_racing_release_has_no_drift() -> None:
    """Concurrent resize and cleanup leave both ledgers at their baseline."""
    baseline = process_resident_memory_snapshot().reserved_bytes
    for _ in range(64):
        ledger = OperationMemoryLedger(64 << 20)
        lease = ledger.acquire(4 << 20, stage="memory_resize_race")
        barrier = threading.Barrier(3)
        resize_errors: list[BaseException] = []

        def resize() -> None:
            """Race one growth against final release."""
            barrier.wait()
            try:
                lease.resize(12 << 20)
            except RuntimeError as exc:
                resize_errors.append(exc)

        def release() -> None:
            """Race final release against growth."""
            barrier.wait()
            lease.release()

        threads = [threading.Thread(target=resize), threading.Thread(target=release)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=2)
            assert not thread.is_alive()
        assert len(resize_errors) <= 1
        assert ledger.snapshot().reserved_bytes == 0
        ledger.close()
    assert process_resident_memory_snapshot().reserved_bytes == baseline


def test_abandoned_operation_memory_lease_releases_during_finalization() -> None:
    """Dropping the final lease reference cannot pin process memory forever."""
    baseline = process_resident_memory_snapshot().reserved_bytes
    ledger = OperationMemoryLedger(64 << 20)
    lease = ledger.acquire(4 << 20, stage="abandoned_lease")
    assert ledger.snapshot().reserved_bytes == 4 << 20
    del lease
    gc.collect()
    # Snapshots stay observationally pure; abandoned-finalizer work is
    # drained only at an explicit operation safe point.
    ledger.safe_point()
    assert ledger.snapshot().reserved_bytes == 0
    ledger.close()
    # ``safe_point`` drains process-global finalizer debt, including stale debt
    # that may predate this test. The leak invariant is therefore that this
    # operation never leaves the process above its entry baseline.
    assert process_resident_memory_snapshot().reserved_bytes <= baseline


def test_operation_memory_close_is_a_barrier_for_inflight_reserve() -> None:
    """Closing waits for a reserve already inside the ledger critical section."""
    ledger = OperationMemoryLedger(64 << 20)
    original = ledger._native  # noqa: SLF001
    entered = threading.Event()
    release = threading.Event()
    closed = threading.Event()
    close_contention_checked = threading.Event()
    close_lock_acquired = threading.Event()
    close_thread: threading.Thread | None = None
    contention_observations: list[bool] = []

    class ObservedCloseCondition(threading.Condition):
        """Authenticate the close thread's failed non-blocking lock acquisition."""

        def __enter__(self) -> bool:
            if threading.current_thread() is close_thread:
                acquired_without_wait = self.acquire(blocking=False)
                contention_observations.append(not acquired_without_wait)
                if acquired_without_wait:
                    self.release()
                close_contention_checked.set()
            acquired = super().__enter__()
            if threading.current_thread() is close_thread:
                close_lock_acquired.set()
            return acquired

    ledger._close_condition = ObservedCloseCondition(ledger._lock)  # noqa: SLF001

    def reserve(capsule: object, size: int, stage: str) -> tuple[int, int, int]:
        """Pause the native reserve while the Python ledger lock is held."""
        entered.set()
        assert release.wait(timeout=2)
        return original.operation_memory_ledger_reserve_snapshot(capsule, size, stage)

    ledger._native = SimpleNamespace(  # noqa: SLF001
        operation_memory_ledger_reserve_snapshot=reserve,
        operation_memory_ledger_release=original.operation_memory_ledger_release,
        operation_memory_ledger_snapshot=original.operation_memory_ledger_snapshot,
    )
    reserve_thread = threading.Thread(target=lambda: ledger.reserve(1024, stage="close_barrier"))
    reserve_thread.start()
    assert entered.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)

    def close() -> None:
        """Record when close crosses the in-flight reserve barrier."""
        ledger.close()
        closed.set()

    close_thread = threading.Thread(target=close, name="observed-ledger-close")
    close_thread.start()
    assert close_contention_checked.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
    assert contention_observations == [True]
    assert not close_lock_acquired.is_set()
    assert not closed.is_set()
    release.set()
    reserve_thread.join(timeout=2)
    close_thread.join(timeout=2)
    assert close_lock_acquired.is_set()
    assert closed.is_set()
    ledger.release(1024)
    assert ledger.snapshot().reserved_bytes == 0


def test_process_temporary_storage_blocks_cross_operation_oversubscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Independent operation pools share one filesystem-level byte ceiling."""
    device = 8_675_309
    free_bytes = _MINIMUM_FREE_BYTES + 100
    monkeypatch.setattr(
        _PROCESS_TEMPORARY_STORAGE,
        "filesystem",
        lambda _path: (device, Path("/tmp"), free_bytes),
    )
    first_pool = TemporaryStoragePermitPool(16 << 20)
    second_pool = TemporaryStoragePermitPool(16 << 20)
    first = first_pool.acquire(60, label="first-process-spool")
    try:
        with pytest.raises(SchemaSanitizerResourceError) as raised:
            second_pool.acquire(41, label="second-process-spool")
        assert raised.value.detail["limit_name"] == "process_temporary_storage_bytes"
        snapshot = process_temporary_storage_snapshot()
        assert snapshot.capacity_bytes == 100
        assert snapshot.reserved_bytes == 60
    finally:
        first.release()
        first_pool.close()
    second = second_pool.acquire(41, label="second-process-spool")
    second.release()
    second_pool.close()
    assert process_temporary_storage_snapshot().reserved_bytes == 0


def test_zero_byte_temporary_lease_can_grow_without_reselecting_filesystem(
    tmp_path: Path,
) -> None:
    """A zero-byte lease retains its filesystem identity for later growth."""
    pool = TemporaryStoragePermitPool(16 << 20)
    lease = pool.acquire(0, label="deferred-spool", path=tmp_path)
    lease.resize(4096)
    assert lease.reserved_bytes == 4096
    assert pool.snapshot().reserved_bytes == 4096
    lease.release()
    pool.close()
    assert process_temporary_storage_snapshot(tmp_path).reserved_bytes == 0


def test_abandoned_temporary_storage_lease_releases_during_finalization(
    tmp_path: Path,
) -> None:
    """Dropping a staging lease returns operation and filesystem capacity."""
    pool = TemporaryStoragePermitPool(16 << 20)
    lease = pool.acquire(4096, label="abandoned-spool", path=tmp_path)
    assert pool.snapshot().reserved_bytes == 4096
    del lease
    gc.collect()
    assert pool.snapshot().reserved_bytes == 0
    assert process_temporary_storage_snapshot(tmp_path).reserved_bytes == 0
    pool.close()


def test_upload_read_rejects_unbudgeted_transient_copy(tmp_path: Path) -> None:
    """Multipart reads reserve source and retained immutable copies up front."""
    source = tmp_path / "transient-part.bin"
    size = 1 << 20
    source.write_bytes(b"x" * size)
    ledger = OperationMemoryLedger(size + (size // 2))
    with activate_operation_memory_ledger(ledger):
        with pytest.raises(SchemaSanitizerResourceError):
            read_upload_range(str(source), 0, size, size)
    assert ledger.snapshot().reserved_bytes == 0
    ledger.close()


def test_upload_payload_retains_and_releases_operation_memory(tmp_path: Path) -> None:
    """A multipart byte body remains charged until its provider call finishes."""
    source = tmp_path / "part.bin"
    source.write_bytes(b"x" * (1 << 20))
    ledger = OperationMemoryLedger(8 << 20)
    with activate_operation_memory_ledger(ledger):
        payload = read_upload_range(str(source), 0, source.stat().st_size, source.stat().st_size)
    assert payload == source.read_bytes()
    assert ledger.snapshot().reserved_bytes >= len(payload)
    release_upload_payload(payload)
    assert ledger.snapshot().reserved_bytes == 0
    ledger.close()


def test_bounded_response_text_accounts_final_unicode_object() -> None:
    """HTTP decoding charges transient copies and retains only the final text."""

    class Content:
        """Return one bounded UTF-8 response body."""

        async def read(self, _maximum: int) -> bytes:
            return "áβ中".encode() * 64

        def at_eof(self) -> bool:
            return True

    response = SimpleNamespace(content=Content(), charset="utf-8")
    ledger = OperationMemoryLedger(8 << 20)

    async def run() -> str:
        """Decode the fake response under the active operation ledger."""
        with activate_operation_memory_ledger(ledger):
            return await read_bounded_response_text(
                response,
                maximum_bytes=4096,
                stage="response_text_test",
            )

    text = asyncio.run(run())
    assert text == "áβ中" * 64
    assert ledger.snapshot().reserved_bytes > 0
    text.close()  # type: ignore[attr-defined]
    assert ledger.snapshot().reserved_bytes == 0
    ledger.close()


def test_async_scheduler_surfaces_non_exception_failure_without_hanging() -> None:
    """A worker BaseException becomes an ordered outcome instead of a deadlock."""

    class WorkerFatal(BaseException):
        """Synthetic non-Exception worker failure."""

    async def run() -> None:
        """Consume one scheduler whose first fetch fails fatally."""

        async def fetch(index: int) -> int:
            """Raise a BaseException for the first ordinal."""
            if index == 0:
                raise WorkerFatal("fatal worker")
            return index

        async for _index, _value in ordered_indexed_results(2, fetch, window=2):
            pass

    with pytest.raises(WorkerFatal, match="fatal worker"):
        asyncio.run(asyncio.wait_for(run(), timeout=1))


def test_remote_close_forcibly_stops_cancellation_resistant_host_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A resistant coroutine keeps its host owned until real termination."""
    from schema_sanitizer.remote_impl import io_coordinator as io_coordinator_module

    started = threading.Event()
    release = threading.Event()
    coordinator = RemoteIoCoordinator(
        thread_name="schema-sanitizer-stubborn-close-test",
        shutdown_timeout_seconds=0.05,
    )

    async def stubborn(_context: object) -> None:
        """Ignore every cancellation request forever."""
        started.set()
        while not release.is_set():
            try:
                await asyncio.sleep(0.005)
            except asyncio.CancelledError:
                continue

    coordinator.submit(stubborn)
    assert started.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
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
    coordinator._thread.join(timeout=SCHEDULER_TIMEOUT_SECONDS)  # noqa: SLF001
    assert not coordinator._thread.is_alive()  # noqa: SLF001


def test_abandoned_remote_startup_has_a_hard_thread_lifetime_bound() -> None:
    """An abandoned context stays owned until its real startup task terminates."""
    thread_name = "schema-sanitizer-stubborn-startup-test"
    release = threading.Event()

    class Context:
        """Async manager whose entry ignores cancellation forever."""

        async def __aenter__(self) -> object:
            while not release.is_set():
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    continue

        async def __aexit__(self, *_exc: object) -> None:
            pass

    with pytest.raises(RuntimeError, match="startup exceeded its deadline"):
        RemoteIoCoordinator(
            Context,
            thread_name=thread_name,
            shutdown_timeout_seconds=0.01,
        )
    release.set()
    deadline = monotonic() + 2.0
    while monotonic() < deadline:
        if not any(
            thread.name == thread_name and thread.is_alive() for thread in threading.enumerate()
        ):
            break
        sleep(0.02)
    assert not any(
        thread.name == thread_name and thread.is_alive() for thread in threading.enumerate()
    )


def test_native_and_python_reservations_share_one_process_counter(tmp_path: Path) -> None:
    """A virtual Python charge leaves no hidden independent native capacity."""
    # Resolve the lazy public entrypoint before exhausting process headroom so
    # the assertion exercises conversion admission, not import machinery.
    to_jsonl = ss.to_jsonl
    process_snapshot = process_resident_memory_snapshot()
    blocker = OperationMemoryLedger(process_snapshot.capacity_bytes)
    live = process_resident_memory_snapshot()
    available = live.capacity_bytes - live.reserved_bytes
    if available < (2 << 20):
        blocker.close()
        pytest.skip("process resident ledger has insufficient integration-test headroom")
    blocked_bytes = available - 1
    lease = blocker.acquire(blocked_bytes, stage="python_process_blocker")
    lease_id = lease._lease_id  # noqa: SLF001
    lease_capability = lease._capability  # noqa: SLF001
    assert lease_capability is not None
    assert lease.reserved_bytes == blocked_bytes
    assert blocker.snapshot().reserved_bytes == blocked_bytes
    assert blocker._python_lease_authority_owned_by(  # noqa: SLF001
        lease_id, id(lease), lease_capability
    )
    source = tmp_path / "tiny.jsonl"
    source.write_text('{"value":1}\n', encoding="utf-8")
    try:
        with pytest.raises(
            Exception,
            match="out of memory|resident memory|limit exceeded|bootstrap concurrency window",
        ):
            to_jsonl(
                source,
                tmp_path / "out.jsonl",
                input_format="jsonl",
                memory_limit_bytes=8 << 20,
            )
    finally:
        lease.release()
        blocker.close()
    assert lease._released is True  # noqa: SLF001
    assert lease.reserved_bytes == 0
    assert not blocker._python_lease_authority_owned_by(  # noqa: SLF001
        lease_id, id(lease), lease_capability
    )
    assert lease_id not in blocker._python_leases  # noqa: SLF001
    assert blocker.snapshot().reserved_bytes == 0
    assert blocker._unknown_python_lease_releases == 0  # noqa: SLF001
    assert lease._finalizer_ticket is None  # noqa: SLF001
    assert blocker._closed is True  # noqa: SLF001
    assert blocker._finalizer_ticket is None  # noqa: SLF001


def test_operation_remote_wait_has_a_hard_transport_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The synchronous adapter forwards its exact deadline and cancels on timeout."""
    from concurrent.futures import TimeoutError as FutureTimeoutError

    from schema_sanitizer.api_impl import operation_context as operation_context_module

    context = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=64 << 20,
    )
    context._resources.remote_timeout_seconds = 0.02  # noqa: SLF001
    observed_timeouts: list[float | None] = []

    class TimedOutFuture:
        """Record the bounded wait and model its exact timeout branch."""

        def result(self, *, timeout: float | None = None) -> None:
            observed_timeouts.append(timeout)
            raise FutureTimeoutError

        def cancel(self) -> bool:
            observed_timeouts.append(-1.0)
            return True

    async def block() -> None:
        """Wait until the bounded caller cancels this operation."""
        await asyncio.Event().wait()

    monkeypatch.setattr(context, "submit_remote", lambda *_args, **_kwargs: TimedOutFuture())
    monkeypatch.setattr(
        operation_context_module,
        "bounded_wait_timeout",
        lambda default: default,
    )
    try:
        with pytest.raises(TimeoutError, match="bounded transport deadline"):
            context.run_remote(block)
        assert observed_timeouts == [0.02, -1.0]
    finally:
        context.close()


def test_operation_remote_timeout_cancels_a_live_coordinator_coroutine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The timeout branch cancels work already running on the real coordinator."""
    from concurrent.futures import Future
    from concurrent.futures import TimeoutError as FutureTimeoutError

    from schema_sanitizer.api_impl import operation_context as operation_context_module

    context = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=64 << 20,
    )
    context._resources.remote_timeout_seconds = 0.02  # noqa: SLF001
    real_submit = context.submit_remote
    operation_started = threading.Event()
    operation_cancelled = threading.Event()
    submitted: list[Future[None]] = []
    observed_timeouts: list[float | None] = []

    async def block() -> None:
        """Publish live execution, then record coordinator-delivered cancellation."""
        operation_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            operation_cancelled.set()
            raise

    def submit_with_controlled_result(*args: object, **kwargs: object) -> Future[None]:
        """Keep the real Future but control when its synchronous result path times out."""
        future = real_submit(*args, **kwargs)  # type: ignore[arg-type]
        assert isinstance(future, Future)
        submitted.append(future)

        def timeout_after_start(*, timeout: float | None = None) -> None:
            assert operation_started.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
            observed_timeouts.append(timeout)
            raise FutureTimeoutError

        monkeypatch.setattr(future, "result", timeout_after_start)
        return future

    monkeypatch.setattr(context, "submit_remote", submit_with_controlled_result)
    monkeypatch.setattr(
        operation_context_module,
        "bounded_wait_timeout",
        lambda default: default,
    )
    try:
        with pytest.raises(TimeoutError, match="bounded transport deadline"):
            context.run_remote(block)
        assert observed_timeouts == [0.02]
        assert len(submitted) == 1
        assert submitted[0].cancelled()
        assert operation_cancelled.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
    finally:
        context.close()


def test_shared_staging_session_late_entry_is_closed_after_timeout() -> None:
    """A cancellation-resistant late session entry cannot leak its manager."""
    release = threading.Event()
    exited = threading.Event()
    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=64 << 20,
    )

    class Session:
        """Delay entry beyond the iterator deadline and record cleanup."""

        async def __aenter__(self) -> Session:
            while not release.is_set():
                try:
                    await asyncio.sleep(0.005)
                except asyncio.CancelledError:
                    continue
            return self

        async def __aexit__(self, *_exc: object) -> None:
            exited.set()

    class Manifest:
        """Expose one chunk through the operation-owned coordinator."""

        files = (object(),)
        chunk_size = 1
        memory_limit_bytes = 64 << 20
        threading_mode = "multi"
        operation_context = operation

        @staticmethod
        def open_staging_session() -> Session:
            return Session()

        @staticmethod
        async def stage_chunk_async(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("staging should not start")

    iterator = RemoteChunkPrefetchIterator(Manifest())
    iterator._remote_timeout_seconds = 0.02  # noqa: SLF001
    try:
        with pytest.raises(TimeoutError, match="session startup"):
            iterator.__enter__()
        release.set()
        assert exited.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
    finally:
        iterator.close()
        operation.close()
