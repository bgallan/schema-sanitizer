"""Integrated regressions for concurrency and memory hardening pass 2."""

from __future__ import annotations

import asyncio
import gc
import threading
from pathlib import Path
from time import monotonic, sleep
from types import SimpleNamespace

import pytest

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
    _MINIMUM_FREE_BYTES,
    _PROCESS_TEMPORARY_STORAGE,
    TemporaryStoragePermitPool,
    process_temporary_storage_snapshot,
)
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
    # pass51 keeps snapshots observationally pure; abandoned-finalizer work is
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

    def reserve(capsule: object, size: int, stage: str) -> None:
        """Pause the native reserve while the Python ledger lock is held."""
        entered.set()
        assert release.wait(timeout=2)
        original.operation_memory_ledger_reserve(capsule, size, stage)

    ledger._native = SimpleNamespace(  # noqa: SLF001
        operation_memory_ledger_reserve=reserve,
        operation_memory_ledger_release=original.operation_memory_ledger_release,
        operation_memory_ledger_snapshot=original.operation_memory_ledger_snapshot,
    )
    reserve_thread = threading.Thread(target=lambda: ledger.reserve(1024, stage="close_barrier"))
    reserve_thread.start()
    assert entered.wait(timeout=1)

    def close() -> None:
        """Record when close crosses the in-flight reserve barrier."""
        ledger.close()
        closed.set()

    close_thread = threading.Thread(target=close)
    close_thread.start()
    sleep(0.02)
    assert not closed.is_set()
    release.set()
    reserve_thread.join(timeout=2)
    close_thread.join(timeout=2)
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
            """Return deterministic multibyte text."""
            return "áβ中".encode() * 64

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


def test_remote_close_forcibly_stops_cancellation_resistant_host_thread() -> None:
    """A resistant coroutine keeps its host owned until real termination."""
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
    assert started.wait(timeout=1)
    before = monotonic()
    with pytest.raises(RuntimeError, match="shutdown exceeded its deadline"):
        coordinator.close()
    assert monotonic() - before < 0.5
    assert coordinator._thread.is_alive()  # noqa: SLF001
    release.set()
    coordinator.close()
    coordinator._thread.join(timeout=0.5)  # noqa: SLF001
    assert not coordinator._thread.is_alive()  # noqa: SLF001


def test_abandoned_remote_startup_has_a_hard_thread_lifetime_bound() -> None:
    """An abandoned context stays owned until its real startup task terminates."""
    thread_name = "schema-sanitizer-stubborn-startup-test"
    release = threading.Event()

    class Context:
        """Async manager whose entry ignores cancellation forever."""

        async def __aenter__(self) -> object:
            """Never finish entering the provider context."""
            while not release.is_set():
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    continue

        async def __aexit__(self, *_exc: object) -> None:
            """No-op exit for completeness."""

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
    baseline = process_resident_memory_snapshot()
    available = baseline.capacity_bytes - baseline.reserved_bytes
    if available < (2 << 20):
        pytest.skip("process resident ledger has insufficient integration-test headroom")
    blocker = OperationMemoryLedger(baseline.capacity_bytes)
    lease = blocker.acquire(available - 1, stage="python_process_blocker")
    source = tmp_path / "tiny.jsonl"
    source.write_text('{"value":1}\n', encoding="utf-8")
    try:
        with pytest.raises(Exception, match="out of memory|resident memory|limit exceeded"):
            ss.to_jsonl(
                source,
                tmp_path / "out.jsonl",
                input_format="jsonl",
                memory_limit_bytes=8 << 20,
            )
    finally:
        lease.release()
        blocker.close()
    assert process_resident_memory_snapshot().reserved_bytes == baseline.reserved_bytes


def test_operation_remote_wait_has_a_hard_transport_deadline() -> None:
    """A cooperative remote coroutine cannot block its synchronous caller forever."""
    context = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=64 << 20,
    )
    context._resources.remote_timeout_seconds = 0.02  # noqa: SLF001

    async def block() -> None:
        """Wait until the bounded caller cancels this operation."""
        await asyncio.Event().wait()

    try:
        before = monotonic()
        with pytest.raises(TimeoutError, match="bounded transport deadline"):
            context.run_remote(block)
        assert monotonic() - before < 0.5
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
            """Ignore cancellation until the test releases the entry."""
            while not release.is_set():
                try:
                    await asyncio.sleep(0.005)
                except asyncio.CancelledError:
                    continue
            return self

        async def __aexit__(self, *_exc: object) -> None:
            """Record closure of the late-entered manager."""
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
            """Return the delayed session."""
            return Session()

        @staticmethod
        async def stage_chunk_async(*_args: object, **_kwargs: object) -> None:
            """Never stage because session startup times out first."""
            raise AssertionError("staging should not start")

    iterator = RemoteChunkPrefetchIterator(Manifest())
    iterator._remote_timeout_seconds = 0.02  # noqa: SLF001
    try:
        with pytest.raises(TimeoutError, match="session startup"):
            iterator.__enter__()
        release.set()
        assert exited.wait(timeout=1)
    finally:
        iterator.close()
        operation.close()
