from __future__ import annotations

import asyncio
import gc
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"


def _source(relative: str) -> str:
    return (SRC / relative).read_text(encoding="utf-8")


def test_governed_file_gc_closes_physical_stream_before_releasing_credit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module
    from schema_sanitizer.core_impl.finalizer_cleanup import drain_finalizer_cleanup

    events: list[str] = []

    class Lease:
        def release(self) -> None:
            events.append("lease.release")

    class Stream:
        closed = False

        def close(self) -> None:
            events.append("stream.close")
            self.closed = True

    stream = Stream()
    monkeypatch.setattr(module, "acquire_file_descriptors", lambda _amount: Lease())
    monkeypatch.setattr("builtins.open", lambda *_args, **_kwargs: stream)

    governed = module.open_governed_file("ignored", "rb")
    del governed
    gc.collect()
    assert drain_finalizer_cleanup() >= 1
    assert events == ["stream.close", "lease.release"]
    assert stream.closed


def test_buffered_generated_bytes_close_is_allocation_free() -> None:
    source = _source("core_impl/generated_bytes.py")
    discard = source[
        source.index("def _discard_buffer") : source.index(
            "def close", source.index("def _discard_buffer")
        )
    ]
    assert "bytearray()" not in discard
    assert ".clear()" in discard
    close = source[source.index("def close", source.index("class BufferedGeneratedBytesReader")) :]
    assert close.index("self._discard_buffer()") < close.index("self._closed = True")


def test_sync_cleanup_escrow_retries_exact_owner_and_fd_credit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl import sync_cleanup_escrow as module

    class Lease:
        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    class Owner:
        def __init__(self) -> None:
            self.calls = 0
            self.fail = True

        def close(self) -> None:
            self.calls += 1
            if self.fail:
                raise RuntimeError("physical close failed")

    lease = Lease()
    monkeypatch.setattr(module, "acquire_file_descriptors", lambda _amount: lease)
    escrow = module._SyncCleanupEscrow()
    reservation = escrow.reserve(label="test", network_fds=1)
    owner = Owner()
    reservation.bind_owner(owner)
    with pytest.raises(RuntimeError, match="physical close failed"):
        reservation.close_and_commit()
    snap = escrow.snapshot()
    assert snap.live == 1
    assert lease.releases == 0
    owner.fail = False
    reservation.close_and_commit()
    assert escrow.snapshot().active == 0
    assert owner.calls == 2
    assert lease.releases == 1


def test_sync_transports_reserve_cleanup_before_resource_construction() -> None:
    azure = _source("remote_impl/providers/azure_sync.py")
    s3 = _source("remote_impl/providers/s3_sync.py")
    http = _source("remote_impl/sync_http.py")
    assert azure.index("reserve_sync_cleanup") < azure.index("DefaultAzureCredential()")
    assert s3.index("reserve_sync_cleanup") < s3.index('create_client("s3"')
    request = http[http.index("def _request_once") : http.index("def _headers")]
    assert request.index("reserve_sync_cleanup") < request.index("_connection(url, timeout)")
    assert "abandon_to_escrow" in http


def test_remote_cleanup_timeout_retains_same_cleanup_task() -> None:
    from schema_sanitizer.remote_impl.io_shutdown import RemoteIoCleanupOwner

    class Manager:
        def __init__(self) -> None:
            self.calls = 0
            self.release = asyncio.Event()

        async def __aexit__(self, *_exc: object) -> None:
            self.calls += 1
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                await self.release.wait()

    async def run() -> None:
        owner = RemoteIoCleanupOwner()
        manager = Manager()
        loop = asyncio.get_running_loop()
        with pytest.raises(TimeoutError):
            await owner.close(manager, deadline=loop.time() + 0.01)
        first = owner.task
        assert first is not None
        assert manager.calls == 1
        manager.release.set()
        await asyncio.sleep(0)
        await owner.close(manager, deadline=loop.time() + 0.5)
        assert manager.calls == 1
        assert owner.task is None
        assert owner.manager is None

    asyncio.run(run())


def test_remote_permit_post_commit_failure_is_level_trigger_repaired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    async def run() -> None:
        governor = RemoteIoPermitGovernor(1, max_waiters=8)
        holder = await governor.acquire(label="holder")
        waiter_task = asyncio.create_task(governor.acquire(label="waiter"))
        await asyncio.sleep(0)
        assert governor.snapshot().waiting == 1
        original = governor._grant_ready_locked
        armed = True

        def fail_once():
            nonlocal armed
            if armed:
                armed = False
                raise MemoryError("injected post-commit scheduler failure")
            return original()

        monkeypatch.setattr(governor, "_grant_ready_locked", fail_once)
        holder.release()
        permit = await asyncio.wait_for(waiter_task, timeout=0.5)
        assert permit is not None
        permit.release()
        assert governor.snapshot().in_use == 0
        assert governor.snapshot().post_commit_failures >= 1

    asyncio.run(run())


def test_remote_sync_and_async_share_authoritative_waiter_queue() -> None:
    source = _source("remote_impl/io_permits.py")
    sync = source[
        source.index("def acquire_sync") : source.index(
            "def snapshot", source.index("def acquire_sync")
        )
    ]
    assert "_enqueue_waiter_locked(waiter)" in sync
    assert "sync_event=event" in sync
    assert "_repair_waiter_progress_noexcept(waiter)" in sync
    assert "cancellable_sleep" not in sync


def test_fd_capacity_distinguishes_reserved_from_physically_open() -> None:
    py = _source("core_impl/process_resources.py")
    native = (
        _source("../cpp/src/internal/runtime/operation_task_arena.cc")
        if False
        else (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    )
    assert "_PYTHON_GOVERNED_FDS_OPENED" in py
    assert "external_open = max(0, open_now - opened_governed)" in py
    assert "g_process_file_descriptors_opened" in native
    assert "const auto external = *observed > opened ? *observed - opened : 0U" in native
    assert "current permits and physically-open" not in native


def test_native_fd_governor_waits_is_fork_safe_and_shutdown_visible() -> None:
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    header = (CPP / "internal/runtime/process_fd_governor.hh").read_text(encoding="utf-8")
    shutdown = _source("core_impl/runtime_shutdown.py")
    diagnostics = _source("core_impl/runtime_diagnostics.py")
    assert "acquire_process_file_descriptor_permits_wait" in arena
    assert "g_process_fd_wait_cv.wait_until" in arena
    assert "!runtime_owner_process()" in arena
    assert "mark_process_file_descriptors_opened" in header
    assert "native_fd_reserved" in shutdown and "native_fd_opened" in shutdown
    assert '"native_process_file_descriptors"' in diagnostics


def test_remote_footprint_underdeclaration_fails_before_recursive_fd_wait() -> None:
    source = _source("remote_impl/io_footprint.py")
    assert "footprint under-declared local file descriptor" in source
    start = source.index("def borrow_local_file_descriptor")
    method = source[start : source.index("_ACTIVE_REMOTE_IO_FOOTPRINT", start)]
    assert "acquire_file_descriptors" not in method


def test_coordination_and_atomic_control_fds_use_teardown_authority() -> None:
    journal = _source("core_impl/coordination_journal.py")
    atomic = _source("core_impl/atomic_output.py")
    assert "open_governed_stream(_opener, teardown=True)" in journal
    assert "governed_os_descriptor" in journal
    assert 'label="coordination-directory"' in journal
    assert "governed_os_descriptor" in atomic
    assert "teardown=True" in atomic


def test_directory_file_objects_carry_metadata_owner_after_charge() -> None:
    from threading import Lock

    from schema_sanitizer.input_impl.directory_inputs import FolderFile
    from schema_sanitizer.input_impl.directory_metadata_budget import (
        DirectoryMetadataBudget,
        RetainedDirectoryMetadata,
    )

    # Build the compatibility-path budget without invoking the unavailable
    # native memory-budget extension in this source-only test environment.
    budget = object.__new__(DirectoryMetadataBudget)
    budget.limit_bytes = 2 * 1024 * 1024
    budget._operation_memory_ledger = None
    budget._retention_owner = RetainedDirectoryMetadata()
    budget._used_bytes = 0
    budget._lock = Lock()
    budget._close_started = False
    file = FolderFile("a", "a", 1, lambda: None)  # type: ignore[arg-type]
    budget.charge_file(file)
    assert file._metadata_owner is budget.retention_owner


def test_discovery_external_ownership_checks_each_escapable_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.input_impl.directory_inputs import (
        DiscoveredDirectoryInput,
        FolderFile,
    )
    from schema_sanitizer.pipeline import source_discovery as module
    from schema_sanitizer.sources.models import RemoteFile

    class Owner:
        def __init__(self) -> None:
            self.lease = object()

        def live_lease(self) -> object:
            return self.lease

    owner = Owner()
    foreign_owner = Owner()
    capability = object()

    def issue_capability(lease: object) -> object | None:
        return capability if lease is owner.lease else None

    monkeypatch.setattr(module, "operation_memory_ownership_capability", issue_capability)

    local_files = (
        FolderFile("a", "a", 1, lambda: None, _metadata_owner=owner),  # type: ignore[arg-type]
        FolderFile("b", "b", 1, lambda: None, _metadata_owner=owner),  # type: ignore[arg-type]
    )
    remote_files = (
        RemoteFile("s3://bucket/a", "a", _metadata_owner=owner),
        RemoteFile("s3://bucket/b", "b", _metadata_owner=owner),
    )
    discovered = DiscoveredDirectoryInput(
        "parquet",
        local_files=local_files,
        remote_files=remote_files,
        _metadata_owner=owner,
    )
    assert module._discovery_result_external_ownership_capability((0, discovered)) is capability

    mismatched_local = DiscoveredDirectoryInput(
        "parquet",
        local_files=(
            local_files[0],
            FolderFile(
                "foreign",
                "foreign",
                1,
                lambda: None,  # type: ignore[arg-type]
                _metadata_owner=foreign_owner,
            ),
        ),
        remote_files=remote_files,
        _metadata_owner=owner,
    )
    assert module._discovery_result_external_ownership_capability((0, mismatched_local)) is None

    mismatched_remote = DiscoveredDirectoryInput(
        "parquet",
        local_files=local_files,
        remote_files=(
            remote_files[0],
            RemoteFile(
                "s3://bucket/foreign",
                "foreign",
                _metadata_owner=foreign_owner,
            ),
        ),
        _metadata_owner=owner,
    )
    assert module._discovery_result_external_ownership_capability((0, mismatched_remote)) is None


def test_native_tsan_probe_covers_process_fd_reserved_opened_wait() -> None:
    source = (ROOT / "cpp/tests/ordered_executor_tsan.cc").read_text(encoding="utf-8")
    assert "bool run_process_fd_governor_round()" in source
    assert 'selected_case == "process_fd_governor"' in source
    assert '"process_fd_governor"' in source
    assert "mark_process_file_descriptors_opened" in source
    assert "mark_process_file_descriptors_closed" in source
    assert "acquire_process_file_descriptor_permits_wait" in source


def test_pass61_primary_cleanup_gate_remains_green() -> None:
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "meta/ci/check_primary_cleanup.py"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
