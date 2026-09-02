"""Exercises failed provider close with physical descriptors, logical retry, memory or
entry caps, runtime pairs, directory and Azure owners, hard asynchronous boundaries,
source metadata, cross-process capacity, streaming parsers, and backpressure metrics.
Physical close never repeats after commit; native credit precedes workers or queues,
waiters stay bounded, and every full-object helper declares a materialization ceiling."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from _support.source_contracts import package_source_text

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"
CPP_TESTS = ROOT / "cpp/tests"


def test_provider_pool_failed_close_keeps_physical_and_descriptor_owner_for_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify provider pool failed close keeps physical and descriptor owner for retry."""
    from schema_sanitizer.remote_impl import provider_session_pool as module

    class Lease:
        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.releases = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.releases += 1

    class Client:
        def __init__(self) -> None:
            """Initialize the client test double."""
            self.fail = True
            self.closes = 0

        async def close(self) -> None:
            """Close the resources owned by the client test double."""
            self.closes += 1
            if self.fail:
                raise RuntimeError("close failed")

    lease = Lease()
    client = Client()
    monkeypatch.setattr(module, "acquire_file_descriptors", lambda *_a, **_k: lease)
    monkeypatch.setattr(module, "acquire_operation_memory", lambda *_a, **_k: None)

    async def run() -> None:
        """Fail one provider close, then retry until its escrow entry is freed."""
        pool = module.RemoteProviderSessionPool()
        await pool.__aenter__()
        await pool.borrow_client(("client",), lambda: asyncio.sleep(0, result=client))
        with pytest.raises(RuntimeError, match="close failed"):
            await pool.__aexit__(None, None, None)
        assert len(pool._entries) == 1
        assert lease.releases == 0
        client.fail = False
        await pool.__aexit__(None, None, None)
        assert not pool._entries

    asyncio.run(run())
    assert client.closes == 2
    assert lease.releases == 1


def test_provider_pool_does_not_repeat_physical_close_if_only_logical_release_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify provider pool does not repeat physical close if only logical release failed."""
    from schema_sanitizer.remote_impl import provider_session_pool as module

    class Lease:
        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.fail = True
            self.releases = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.releases += 1
            if self.fail:
                raise RuntimeError("lease failed")

    class Client:
        def __init__(self) -> None:
            """Initialize the client test double."""
            self.closes = 0

        async def close(self) -> None:
            """Close the resources owned by the client test double."""
            self.closes += 1

    descriptor = Lease()
    client = Client()
    monkeypatch.setattr(module, "acquire_file_descriptors", lambda *_a, **_k: descriptor)
    monkeypatch.setattr(module, "acquire_operation_memory", lambda *_a, **_k: None)

    async def run() -> None:
        """Retry logical release without repeating the committed physical close."""
        pool = module.RemoteProviderSessionPool()
        await pool.__aenter__()
        await pool.borrow_client(("client",), lambda: asyncio.sleep(0, result=client))
        with pytest.raises(RuntimeError, match="lease failed"):
            await pool.__aexit__(None, None, None)
        descriptor.fail = False
        await pool.__aexit__(None, None, None)

    asyncio.run(run())
    assert client.closes == 1
    assert descriptor.releases == 2


def test_provider_pool_has_memory_charge_and_hard_entry_ceiling() -> None:
    """Verify provider pool has memory charge and hard entry ceiling."""
    source = package_source_text("remote_impl/provider_session_pool.py")
    assert "_MAX_POOL_ENTRIES = 1024" in source
    assert "_MAX_PENDING_KEY_GATES = 1024" in source
    assert 'acquire_operation_memory(control_bytes, stage="remote_provider_pool_entry")' in source
    assert "control_bytes = min(1 << 20, (16 << 10) + descriptor_weight * (8 << 10))" in source
    assert "self._ensure_entry_capacity()" in source
    assert "self._entry_escrow" in source


def test_runtime_pair_close_retries_failed_admission_instead_of_false_commit() -> None:
    """Verify runtime pair close retries failed admission instead of false commit."""
    from schema_sanitizer.core_impl import concurrency_contracts as module

    class Admission:
        def __init__(self) -> None:
            """Initialize the admission test double."""
            self.fail = True
            self.calls = 0

        def close(self) -> None:
            """Close the resources owned by the admission test double."""
            self.calls += 1
            if self.fail:
                raise RuntimeError("admission close failed")

    token = module.activate_runtime_concurrency_pair("json", "parquet")
    admission = Admission()
    owner = module.RuntimeConcurrencyPairAdmission(token, admission, object())
    with pytest.raises(RuntimeError, match="admission close failed"):
        owner.close()
    assert owner.admission is admission
    assert not owner._closed
    admission.fail = False
    owner.close()
    assert owner.admission is None
    assert owner._closed
    assert admission.calls == 2


def test_directory_session_and_azure_owner_use_commit_after_cleanup() -> None:
    """Verify directory session and azure owner use commit after cleanup."""
    directory = package_source_text("remote_impl/directory_downloads.py")
    close_pos = directory.index("await close_provider_client(context)")
    clear_pos = directory.index("self._context = None", close_pos)
    assert close_pos < clear_pos
    sem_pos = directory.index("semaphore = asyncio.Semaphore")
    provider_pos = directory.index("context = await provider_client_for_downloads", sem_pos)
    assert sem_pos < provider_pos

    azure = package_source_text("remote_impl/providers/azure.py")
    block = azure[
        azure.index("class _AzureServiceOwner") : azure.index(
            "@dataclass", azure.index("class _AzureServiceOwner")
        )
    ]
    assert "self._closed = self._service_closed and self._credential_closed" in block
    assert "setattr(self, flag_name, True)" in block
    assert "reservation = _reserve_azure_rollback_slot()" in azure
    assert "_publish_azure_credential_rollback(reservation, credential)" in azure


def test_io_shutdown_uses_hard_task_boundary_not_wait_for() -> None:
    """Verify I/O shutdown uses hard task boundary not wait for."""
    source = package_source_text("remote_impl/io_shutdown.py")
    assert "loop.create_task(manager.__aexit__" in source
    assert "await asyncio.wait({task}, timeout=remaining)" in source
    assert "RemoteIoCleanupOwner" in source
    assert "asyncio.wait_for" not in source


def test_azure_sdk_fanout_is_disabled_for_async_blob_transfers() -> None:
    """Verify azure sdk fanout is disabled for async blob transfers."""
    source = package_source_text("remote_impl/providers/azure.py")
    assert "max_concurrency=policy.async_concurrency" not in source
    assert "max_concurrency=tuning.concurrency" not in source
    assert source.count("max_concurrency=1") >= 3


def test_async_external_ownership_requires_runtime_issued_capability() -> None:
    """Verify async external ownership requires runtime issued capability."""
    from schema_sanitizer.core_impl.async_scheduler import (
        AsyncResultMemoryContract,
        AsyncResultOwnershipMode,
        _assert_async_result_ownership,
    )
    from schema_sanitizer.core_impl.memory_budget import (
        no_retained_result_ownership_capability,
    )

    value = object()
    missing = AsyncResultMemoryContract(
        preflight_bytes=1,
        ownership_mode=AsyncResultOwnershipMode.EXTERNALLY_GOVERNED,
    )
    with pytest.raises(RuntimeError, match="authenticated ownership capability"):
        _assert_async_result_ownership(value, missing)

    zero_payload = AsyncResultMemoryContract(
        preflight_bytes=1,
        ownership_mode=AsyncResultOwnershipMode.EXTERNALLY_GOVERNED,
        external_ownership_capability=lambda _candidate: no_retained_result_ownership_capability(),
    )
    _assert_async_result_ownership(None, zero_payload)


def test_source_discovery_proves_metadata_owner_before_scheduler_bridge_release() -> None:
    """Verify source discovery proves metadata owner before scheduler bridge release."""
    source = package_source_text("pipeline/source_discovery.py")
    assert "external_ownership_capability=_discovery_result_external_ownership_capability" in source
    assert "operation_memory_ownership_capability(live_lease())" in source
    assert "no_retained_result_ownership_capability()" in source


def test_cross_process_effective_capacity_tracks_only_live_owner_ceilings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cross process effective capacity tracks only live owner ceilings."""
    from schema_sanitizer.core_impl.cross_process_memory import _ProcessCrossMemoryCoordinator

    class FakePhysical:
        def __init__(self) -> None:
            """Initialize the fake physical test double."""
            self.capacity = 0
            self.size = 0

        def _set_capacity(self, value: int) -> None:
            """Set the governor capacity for the contention scenario."""
            self.capacity = value

        def resize(self, value: int) -> None:
            """Resize the resource represented by the fake physical test double."""
            self.size = value

    coordinator = _ProcessCrossMemoryCoordinator(1000)
    physical = FakePhysical()
    coordinator._physical = physical  # type: ignore[assignment]
    coordinator._physical_bytes = 0
    monkeypatch.setattr(coordinator, "_schedule_reconcile_locked", lambda *, start_worker: None)

    high = coordinator.acquire(1, capacity_bytes=1000)
    high.resize(200)
    assert physical.capacity == 1000
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    with pytest.raises(SchemaSanitizerResourceError, match="live-owner ceiling"):
        coordinator.acquire(1, capacity_bytes=100)
    # Failed low-ceiling publication rolls back its cap as well as contribution.
    assert physical.capacity == 1000
    low = coordinator.acquire(1, capacity_bytes=300)
    assert physical.capacity == 300
    low.close()
    assert physical.capacity == 1000
    high.close()


def test_procfs_cgroup_parser_is_streaming_and_hard_bounded(tmp_path: Path) -> None:
    """Verify procfs cgroup parser is streaming and hard bounded."""
    from schema_sanitizer.core_impl import cgroup_view

    path = tmp_path / "proc"
    path.write_bytes(b"a\n" * 4)
    assert (
        list(
            cgroup_view._iter_bounded_proc_lines(
                path, max_line_bytes=8, max_total_bytes=16, max_records=4
            )
        )
        == ["a"] * 4
    )
    path.write_bytes(b"0123456789\n")
    with pytest.raises(cgroup_view._ProcReadLimitExceeded):
        list(
            cgroup_view._iter_bounded_proc_lines(
                path, max_line_bytes=4, max_total_bytes=32, max_records=4
            )
        )
    source = package_source_text("core_impl/cgroup_view.py")
    assert 'Path("/proc/self/mountinfo").read_text()' not in source
    assert 'Path("/proc/self/cgroup").read_text()' not in source


def test_full_object_byte_helpers_require_explicit_materialization_ceiling() -> None:
    """Verify full object byte helpers require explicit materialization ceiling."""
    s3 = package_source_text("remote_impl/providers/s3.py")
    azure = package_source_text("remote_impl/providers/azure.py")
    gcs = package_source_text("remote_impl/providers/gcs.py")
    assert "file: RemoteFile, *, maximum_bytes: int" in s3
    assert "async def download_bytes(uri: str, *, maximum_bytes: int)" in azure
    assert "file: RemoteFile, *, maximum_bytes: int" in gcs
    assert "return await body.read()" not in s3
    assert (
        "data.extend(chunk)"
        not in azure[azure.index("async def download_bytes") : azure.index("async def upload_file")]
    )


def test_native_backpressure_waiters_are_bounded_and_do_not_consume_queue_slots() -> None:
    """Verify native backpressure waiters are bounded and do not consume queue slots."""
    source = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    helper_start = source.index("sanitize::Status AcquireRetainedSubmitCredit")
    helper_end = source.index("class ArenaCleanupReaper", helper_start)
    helper = source[helper_start:helper_end]
    assert "backpressure_waiters" in helper
    assert "rejected_backpressure_waiters" in helper
    assert "retained_ready.wait_until" in helper
    assert "queued_total" not in helper
    assert "ensure_worker_started" not in helper
    assert "retained_ready.notify_all()" in source


def test_native_memory_credit_precedes_worker_start_and_queue_publication() -> None:
    """Verify native memory credit precedes worker start and queue publication."""
    source = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    start = source.index("sanitize::Status OperationTaskArena::SubmitCharged")
    end = source.index("std::size_t OperationTaskArena::worker_count", start)
    body = source[start:end]
    credit = body.index("AcquireRetainedSubmitCredit(state, retained_bytes)")
    worker = body.index("ensure_worker_started")
    queue = body.index("state->queued_total.compare_exchange_weak")
    publish = body.index("slot.tasks.push_back")
    assert credit < worker < queue < publish


def test_native_backpressure_deadline_is_wired_at_every_arena_creation_site() -> None:
    """Verify native backpressure deadline is wired at every arena creation site."""
    paths = [
        CPP / "ingest/prepare/prepare.cc",
        CPP / "api/python_abi3/registry/arrow_source_sinks.cc",
        CPP / "api/python_abi3/registry/path_source_sinks.cc",
        CPP / "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.cc",
    ]
    for path in paths:
        source = path.read_text(encoding="utf-8")
        assert "SetBackpressureDeadlineMillis(" in source, path
        assert "backpressure_deadline_millis_from(arena_budget)" in source, path
    budget = (CPP / "internal/memory/memory_budget.hh").read_text(encoding="utf-8")
    assert "backpressure_deadline_millis_from" in budget
    assert "86'400'000.0" in budget


def test_native_exposes_separate_backpressure_waiter_rejection_metric() -> None:
    """Verify native exposes separate backpressure waiter rejection metric."""
    header = (CPP / "internal/runtime/operation_task_arena.hh").read_text(encoding="utf-8")
    source = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    assert "rejected_backpressure_waiters()" in header
    assert "OperationTaskArena::rejected_backpressure_waiters()" in source


def test_tsan_probe_covers_heterogeneous_backpressure_and_phantom_slots() -> None:
    """Verify TSan probe covers heterogeneous backpressure and phantom slots."""
    source = (CPP_TESTS / "ordered_executor_tsan.cc").read_text(encoding="utf-8")
    assert "run_arena_heterogeneous_backpressure_round" in source
    assert "arena_heterogeneous_backpressure" in source
    assert "waiters_do_not_publish_queue_slots" in source
    assert "size_aware_progress" in source
    assert 'selected_case == "arena_heterogeneous_backpressure"' in source
