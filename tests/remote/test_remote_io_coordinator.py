"""Operation-scoped remote I/O coordinator contracts."""

from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager
from time import monotonic, sleep
from types import SimpleNamespace

import pytest

from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator
from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator


def test_remote_io_coordinator_reuses_one_thread_and_context() -> None:
    """All submitted work shares one event loop thread and one context."""
    entered = 0
    exited = 0
    thread_ids: set[int] = set()

    @asynccontextmanager
    async def context():
        """Track one shared context lifetime."""
        nonlocal entered, exited
        entered += 1
        try:
            yield object()
        finally:
            exited += 1

    coordinator = RemoteIoCoordinator(context)

    async def operation(_context: object) -> int:
        """Return the worker thread after one scheduling point."""
        await asyncio.sleep(0)
        ident = threading.get_ident()
        thread_ids.add(ident)
        return ident

    futures = [coordinator.submit(operation) for _ in range(8)]
    assert len({future.result() for future in futures}) == 1
    coordinator.close()

    assert entered == 1
    assert exited == 1
    assert thread_ids == {coordinator.thread_ident}


def test_remote_io_coordinator_does_not_lock_on_already_completed_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Synchronous done callbacks are registered outside the bookkeeping lock."""
    from concurrent.futures import Future

    from schema_sanitizer.remote_impl import io_coordinator

    coordinator = RemoteIoCoordinator()
    real_submit = io_coordinator.asyncio.run_coroutine_threadsafe

    def completed(coroutine, _loop):
        """Return an already-finished future and close the unused coroutine."""
        coroutine.close()
        future: Future[int] = Future()
        future.set_result(7)
        return future

    monkeypatch.setattr(io_coordinator.asyncio, "run_coroutine_threadsafe", completed)
    try:
        future = coordinator.submit(lambda _context: asyncio.sleep(0))
        monkeypatch.setattr(
            io_coordinator.asyncio,
            "run_coroutine_threadsafe",
            real_submit,
        )
        assert future.result() == 7
    finally:
        coordinator.close()


def test_remote_io_coordinator_close_has_a_bounded_provider_deadline() -> None:
    """A stuck provider close fails clearly instead of blocking forever."""
    exit_started = threading.Event()

    @asynccontextmanager
    async def context():
        """Expose a provider whose close waits until coordinator cancellation."""
        try:
            yield object()
        finally:
            exit_started.set()
            await asyncio.Event().wait()

    coordinator = RemoteIoCoordinator(context, shutdown_timeout_seconds=0.05)
    with pytest.raises(RuntimeError, match="shutdown exceeded its deadline"):
        coordinator.close()
    assert exit_started.wait(timeout=1)


def test_remote_io_coordinator_abandons_a_late_startup_cleanly() -> None:
    """A provider startup is cancelled and its host terminates after timeout."""
    exited = threading.Event()
    thread_name = "schema-sanitizer-late-startup-test"

    @asynccontextmanager
    async def context():
        """Enter too late, then record cleanup of the abandoned provider."""
        await asyncio.sleep(0.5)
        try:
            yield object()
        finally:
            exited.set()

    with pytest.raises(RuntimeError, match="startup exceeded its deadline"):
        RemoteIoCoordinator(
            context,
            thread_name=thread_name,
            shutdown_timeout_seconds=0.01,
        )
    deadline = monotonic() + 1.0
    while monotonic() < deadline and any(
        thread.name == thread_name and thread.is_alive() for thread in threading.enumerate()
    ):
        sleep(0.01)
    assert not exited.is_set()
    assert not any(
        thread.name == thread_name and thread.is_alive() for thread in threading.enumerate()
    )


def test_multi_remote_prefetch_cleans_unconsumed_staged_chunks() -> None:
    """Closing multi prefetch drains tasks and removes unconsumed staging."""

    class FakeStaged:
        """Track staged-resource cleanup."""

        def __init__(self, start: int) -> None:
            """Store the chunk ordinal."""
            self.start = start
            self.closed = False

        def close(self) -> None:
            """Mark this staging resource as released."""
            self.closed = True

    class Manifest:
        """Provide an async remote-manifest test double."""

        files = (object(), object(), object())
        chunk_size = 1
        memory_limit_bytes = 512 * 1024 * 1024
        threading_mode = "multi"

        def __init__(self) -> None:
            """Initialize lifecycle tracking."""
            self.entered = 0
            self.exited = 0
            self.thread_ids: set[int] = set()
            self.staged: dict[int, FakeStaged] = {}

        @asynccontextmanager
        async def open_staging_session(self):
            """Open one shared fake provider session."""
            self.entered += 1
            try:
                yield SimpleNamespace()
            finally:
                self.exited += 1

        async def stage_chunk_async(self, start: int, _session: object) -> FakeStaged:
            """Complete chunks out of order on the same I/O thread."""
            self.thread_ids.add(threading.get_ident())
            await asyncio.sleep(0.02 if start == 0 else 0)
            staged = FakeStaged(start)
            self.staged[start] = staged
            return staged

    manifest = Manifest()
    iterator = RemoteChunkPrefetchIterator(manifest)
    first = next(iterator)
    assert first.start == 0
    iterator.close()

    assert manifest.entered == 1
    assert manifest.exited == 1
    assert len(manifest.thread_ids) == 1
    assert first.closed is False
    assert all(staged.closed for start, staged in manifest.staged.items() if start != 0)


def test_directory_session_applies_one_global_transfer_limit(
    monkeypatch,
    tmp_path,
) -> None:
    """Concurrent staged chunks share one client and one transfer semaphore."""
    from schema_sanitizer.core_impl.execution_policy import execution_policy
    from schema_sanitizer.remote_impl import directory_downloads
    from schema_sanitizer.sources import RemoteFile

    files = [RemoteFile(f"s3://bucket/{index}.jsonl", f"{index}.jsonl", 8) for index in range(12)]
    opens = 0
    closes = 0
    active = 0
    max_active = 0

    async def fake_open(_files, **_kwargs):
        """Open one stand-in provider context."""
        nonlocal opens
        opens += 1
        return object()

    async def fake_close(_context):
        """Close the one stand-in provider context."""
        nonlocal closes
        closes += 1

    async def fake_download(_context, file, local_path):
        """Track transfer overlap and create one local file."""
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        try:
            await asyncio.sleep(0.01)
            with open(local_path, "wb") as handle:
                handle.write(file.name.encode())
        finally:
            active -= 1

    monkeypatch.setattr(directory_downloads, "provider_client_for_downloads", fake_open)
    monkeypatch.setattr(directory_downloads, "close_provider_client", fake_close)
    monkeypatch.setattr(directory_downloads, "download_file_to_path", fake_download)

    async def run() -> None:
        """Download three chunks concurrently through one session."""
        directories = [tmp_path / f"chunk-{index}" for index in range(3)]
        for directory in directories:
            directory.mkdir()
        async with directory_downloads.RemoteDirectoryDownloadSession(
            files,
            memory_limit_bytes=64 * 1024 * 1024,
            threading_mode="multi",
        ) as session:
            await asyncio.gather(
                *(
                    session.download_files(files[index : index + 4], str(directory))
                    for index, directory in zip(range(0, 12, 4), directories, strict=True)
                )
            )

    asyncio.run(run())

    policy = execution_policy("multi", 64 * 1024 * 1024)
    assert opens == 1
    assert closes == 1
    assert max_active == policy.async_concurrency


def test_coordinator_cancellation_removes_partial_staging(monkeypatch, tmp_path) -> None:
    """Closing the coordinator drains cancellation before leaving temp files."""
    from schema_sanitizer.remote_impl import staging
    from schema_sanitizer.sources import RemoteFile

    target = tmp_path / "cancelled-stage"
    started = threading.Event()

    class BlockingSession:
        """Block a staged chunk until coordinator cancellation."""

        async def download_files(self, _files, _directory):
            """Wait indefinitely after exposing that the transfer started."""
            started.set()
            await asyncio.Event().wait()

    @asynccontextmanager
    async def context():
        """Provide one blocking shared session."""
        yield BlockingSession()

    def create_temp_directory() -> staging.StagedPath:
        """Create a predictable owned staging directory."""
        target.mkdir()
        return staging.StagedPath(str(target), is_dir=True)

    monkeypatch.setattr(staging, "create_temp_directory_path", create_temp_directory)
    coordinator = RemoteIoCoordinator(context)
    files = [RemoteFile("s3://bucket/a.jsonl", "a.jsonl", 8)]

    async def stage(session):
        """Stage one chunk through the blocking session."""
        return await staging.stage_remote_files_to_directory_async(
            files,
            memory_limit_bytes=64 * 1024 * 1024,
            threading_mode="multi",
            download_session=session,
        )

    coordinator.submit(stage)
    assert started.wait(timeout=2.0)
    coordinator.close()
    assert not target.exists()


def test_remote_operation_context_reuses_one_loop_across_pipeline_stages() -> None:
    """Discovery, transfer, and upload share one multi-mode event-loop host."""
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext

    context = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=64 * 1024 * 1024,
    )

    async def stage(label: str) -> tuple[str, int]:
        """Return the stage label and operation-owned remote thread."""
        await asyncio.sleep(0)
        return label, threading.get_ident()

    try:
        results = [
            context.run_remote(lambda label=label: stage(label))
            for label in ("discovery", "download", "upload")
        ]
        assert [label for label, _thread in results] == [
            "discovery",
            "download",
            "upload",
        ]
        assert {thread for _label, thread in results} == {
            context.remote_coordinator.thread_ident  # type: ignore[union-attr]
        }
    finally:
        context.close()


def test_single_remote_operation_context_stays_inline(monkeypatch) -> None:
    """Single mode never constructs the project-owned remote host thread."""
    from schema_sanitizer.api_impl import operation_context as operation_context_module

    class ForbiddenCoordinator:
        """Fail if single mode attempts to construct a coordinator."""

        def __init__(self, *_args, **_kwargs) -> None:
            """Reject construction."""
            raise AssertionError("single mode created a remote coordinator")

    monkeypatch.setattr(operation_context_module, "RemoteIoCoordinator", ForbiddenCoordinator)
    context = operation_context_module.OperationExecutionContext(
        threading_mode="single",
        memory_limit_bytes=64 * 1024 * 1024,
    )
    caller_thread = threading.get_ident()

    def operation() -> int:
        """Return the thread executing strict blocking remote work."""
        return threading.get_ident()

    async def forbidden_async_operation() -> int:
        """Model an async provider operation forbidden in strict single mode."""
        return threading.get_ident()

    try:
        assert context.run_remote_sync(operation) == caller_thread
        with pytest.raises(RuntimeError, match="strict single-mode"):
            context.run_remote(forbidden_async_operation)
        assert context.remote_coordinator is None
    finally:
        context.close()


def test_prefetch_borrows_operation_coordinator_without_closing_it() -> None:
    """A staged iterator closes its provider session but not the operation loop."""
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext

    class FakeStaged:
        """Minimal staged result."""

        def close(self) -> None:
            """Release the fake result."""

    class Session:
        """Track one provider-session lifetime."""

        def __init__(self) -> None:
            """Initialize enter and exit counters."""
            self.entered = 0
            self.exited = 0

        async def __aenter__(self):
            """Enter the fake provider session."""
            self.entered += 1
            return self

        async def __aexit__(self, *_exc: object) -> None:
            """Exit the fake provider session."""
            self.exited += 1

    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=64 * 1024 * 1024,
    )
    session = Session()

    class Manifest:
        """Expose one remote packet through the shared operation context."""

        files = (object(),)
        chunk_size = 1
        memory_limit_bytes = 64 * 1024 * 1024
        threading_mode = "multi"
        operation_context = operation

        @staticmethod
        def next_chunk_start(start: int) -> int:
            """Advance by the single fake file."""
            return start + 1

        @staticmethod
        def open_staging_session() -> Session:
            """Return the shared fake provider session."""
            return session

        @staticmethod
        async def stage_chunk_async(_start: int, _session: Session) -> FakeStaged:
            """Return one staged fake packet."""
            return FakeStaged()

    try:
        iterator = RemoteChunkPrefetchIterator(Manifest())
        staged = next(iterator)
        iterator.close()
        assert session.entered == 1
        assert session.exited == 1
        assert operation.run_remote(lambda: asyncio.sleep(0, result="alive")) == "alive"
        staged.close()
    finally:
        operation.close()
