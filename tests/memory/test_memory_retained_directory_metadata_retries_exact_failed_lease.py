"""Regression coverage for memory retained directory metadata retries exact failed lease."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"
CPP_TESTS = ROOT / "cpp/tests"


def _source(relative: str) -> str:
    return (SRC / relative).read_text(encoding="utf-8")


def test_retained_directory_metadata_retries_exact_failed_lease() -> None:
    from schema_sanitizer.input_impl.directory_metadata_budget import RetainedDirectoryMetadata

    class Lease:
        def __init__(self) -> None:
            self.fail = True
            self.calls = 0
            self.reserved_bytes = 777

        def close(self) -> None:
            self.calls += 1
            if self.fail:
                raise RuntimeError("lease close failed")

    owner = RetainedDirectoryMetadata()
    lease = Lease()
    owner._adopt(lease)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="lease close failed"):
        owner.close()
    assert owner.live_lease() is lease
    assert owner.reserved_bytes == 777
    lease.fail = False
    owner.close()
    assert owner.live_lease() is None
    assert owner.reserved_bytes == 0
    assert lease.calls == 2


def test_provider_client_insertion_failure_keeps_preallocated_cleanup_escrow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl import provider_session_pool as module

    class Lease:
        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    class Client:
        def __init__(self) -> None:
            self.fail_close = True
            self.closes = 0

        async def close(self) -> None:
            self.closes += 1
            if self.fail_close:
                raise RuntimeError("client close failed")

    class ExplodingDict(dict):
        def __setitem__(self, key, value) -> None:  # type: ignore[no-untyped-def]
            raise MemoryError("publish failed")

    descriptor = Lease()
    control_leases: list[Lease] = []

    def memory_lease(*_a, **_k):  # type: ignore[no-untyped-def]
        lease = Lease()
        control_leases.append(lease)
        return lease

    monkeypatch.setattr(module, "acquire_file_descriptors", lambda *_a, **_k: descriptor)
    monkeypatch.setattr(module, "acquire_operation_memory", memory_lease)
    client = Client()

    async def run() -> None:
        pool = module.RemoteProviderSessionPool()
        await pool.__aenter__()
        pool._entries = ExplodingDict()
        with pytest.raises(MemoryError, match="publish failed"):
            await pool.borrow_client(("escrow",), lambda: asyncio.sleep(0, result=client))
        live = [slot for slot in pool._entry_escrow if slot.resource is client]
        assert len(live) == 1
        assert live[0].kind == "client"
        assert live[0].descriptor_lease is descriptor
        assert descriptor.releases == 0
        client.fail_close = False
        await pool.__aexit__(None, None, None)
        assert live[0].kind == "free"

    asyncio.run(run())
    assert client.closes == 2
    assert descriptor.releases == 1
    assert all(lease.releases == 1 for lease in control_leases)


def test_provider_manager_partial_enter_stays_owned_until_exit_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl import provider_session_pool as module

    class Lease:
        def release(self) -> None:
            return None

    class Manager:
        def __init__(self) -> None:
            self.exit_fail = True
            self.exit_calls = 0

        async def __aenter__(self):
            raise RuntimeError("partial enter")

        async def __aexit__(self, *_exc):
            self.exit_calls += 1
            if self.exit_fail:
                raise RuntimeError("rollback exit failed")

    monkeypatch.setattr(module, "acquire_file_descriptors", lambda *_a, **_k: Lease())
    monkeypatch.setattr(module, "acquire_operation_memory", lambda *_a, **_k: Lease())
    manager = Manager()

    async def run() -> None:
        pool = module.RemoteProviderSessionPool()
        await pool.__aenter__()
        with pytest.raises(RuntimeError, match="rollback exit failed"):
            await pool.borrow_manager(("manager",), lambda: asyncio.sleep(0, result=manager))
        live = [slot for slot in pool._entry_escrow if slot.resource is manager]
        assert len(live) == 1
        assert live[0].kind == "manager"
        manager.exit_fail = False
        await pool.__aexit__(None, None, None)
        assert live[0].kind == "free"

    asyncio.run(run())
    assert manager.exit_calls == 2


def test_azure_credential_terminal_slot_is_reserved_before_constructor() -> None:
    source = _source("remote_impl/providers/azure.py")
    start = source.index("async def _open_service_unpooled")
    body = source[start : source.index("\n\nasync def", start + 10)]
    reserve = body.index("reservation = _reserve_azure_rollback_slot()")
    construct = body.index("credential = identity.DefaultAzureCredential")
    assert reserve < construct
    assert "_publish_azure_credential_rollback(reservation, credential)" in body
    assert "drain_azure_credential_rollbacks" in source


def test_remote_io_waiter_self_expires_at_operation_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import cancellation as cancellation_module
    from schema_sanitizer.errors import SchemaSanitizerCancelledError
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    class StepClock:
        def __init__(self) -> None:
            self.reads = 0

        def __call__(self) -> float:
            self.reads += 1
            return 0.0 if self.reads <= 4 else 2.0

    clock = StepClock()
    monkeypatch.setattr(cancellation_module, "monotonic", clock)

    async def run() -> tuple[int, int]:
        governor = RemoteIoPermitGovernor(capacity=1)
        first = await governor.acquire(1, operation_id="holder")
        try:
            with cancellation_module.operation_cancellation(timeout_seconds=1.0):
                with pytest.raises(SchemaSanitizerCancelledError):
                    await governor.acquire(1, operation_id="deadline")
        finally:
            first.release()
        snapshot = governor.snapshot()
        return snapshot.cancellations, snapshot.waiting

    cancellations, waiting = asyncio.run(run())
    assert clock.reads == 6
    assert cancellations == 1
    assert waiting == 0


def test_sync_and_async_remote_waiters_share_one_process_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    async def run() -> None:
        governor = RemoteIoPermitGovernor(capacity=1)
        first = await governor.acquire(1, operation_id="async-holder")
        acquired = threading.Event()
        enqueued = threading.Event()
        release = threading.Event()
        errors: list[BaseException] = []
        original_enqueue = governor._enqueue_waiter_locked

        def observe_enqueue(waiter: object) -> None:
            original_enqueue(waiter)  # type: ignore[arg-type]
            if getattr(waiter, "sync_event", None) is not None:
                enqueued.set()

        monkeypatch.setattr(governor, "_enqueue_waiter_locked", observe_enqueue)

        def worker() -> None:
            try:
                permit = governor.acquire_sync(1, operation_id="sync-waiter")
                acquired.set()
                release.wait(timeout=1.0)
                permit.release()
            except BaseException as exc:
                errors.append(exc)
                acquired.set()

        thread = threading.Thread(target=worker)
        thread.start()
        assert await asyncio.to_thread(enqueued.wait, SCHEDULER_TIMEOUT_SECONDS), (
            "synchronous waiter was not authoritatively enqueued"
        )
        assert not acquired.is_set()
        first.release()
        assert await asyncio.to_thread(acquired.wait, SCHEDULER_TIMEOUT_SECONDS)
        release.set()
        thread.join(timeout=SCHEDULER_TIMEOUT_SECONDS)
        assert not thread.is_alive()
        assert not errors

    asyncio.run(run())


def test_remote_footprint_separates_logical_network_and_local_fd_weights(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl import io_footprint as module

    acquired: list[int] = []

    class Lease:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

    monkeypatch.setattr(
        module,
        "acquire_file_descriptors",
        lambda amount: acquired.append(amount) or Lease(),
    )
    footprint = module.RemoteIoFootprint(remote_weight=9, network_fds=2, local_file_fds=1)
    assert footprint.total_file_descriptors == 3
    owner = module.ActiveRemoteIoFootprint(footprint)
    with module.activate_remote_io_footprint(owner):
        with module.reserve_remote_local_file_descriptor():
            assert acquired == []
            # Footprint under-declaration is a contract violation. Reacquiring from
            # the same FD authority while already holding part of the footprint
            # could create a circular wait under low capacity.
            with pytest.raises(RuntimeError, match="under-declared"):
                with module.reserve_remote_local_file_descriptor():
                    pass
            assert acquired == []


def test_async_provider_pool_charges_transport_capacity_once_not_per_operation() -> None:
    context = _source("api_impl/operation_context.py")
    coordinator = _source("remote_impl/io_coordinator.py")
    assert "default_descriptor_weight=max(1, self.policy.async_concurrency)" in context
    assert "RemoteIoFootprint(remote_weight=permit_weight, network_fds=0)" in coordinator
    transport = _source("remote_impl/transport.py")
    assert "descriptor_weight=max" not in transport


def test_simple_remote_uploads_consume_pre_admitted_local_fd_credit() -> None:
    paths = [
        "remote_impl/providers/s3.py",
        "remote_impl/providers/s3_sync.py",
        "remote_impl/providers/azure.py",
        "remote_impl/providers/azure_sync.py",
        "remote_impl/providers/gcs.py",
        "remote_impl/upload_policy.py",
        "remote_impl/transport.py",
        "remote_impl/sync_http.py",
    ]
    for relative in paths:
        source = _source(relative)
        assert "open_remote_local_file" in source, relative


def test_python_local_user_file_streams_use_governed_open() -> None:
    for relative in (
        "input_impl/selection.py",
        "input_impl/directory_inputs.py",
        "api_impl/output_diagnostics.py",
    ):
        source = _source(relative)
        assert "open_governed_file" in source, relative


def test_python_fd_lease_bridges_to_native_process_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import native_runtime, process_resources

    class NativeFdApi:
        def __init__(self) -> None:
            self.acquires: list[tuple[int, int]] = []
            self.releases: list[int] = []

        def process_file_descriptor_permits_acquire(self, desired: int, minimum: int) -> int:
            self.acquires.append((desired, minimum))
            return desired

        def process_file_descriptor_permits_release(self, amount: int) -> None:
            self.releases.append(amount)

    fake = NativeFdApi()
    monkeypatch.setattr(native_runtime, "native_core", fake)
    lease = process_resources.acquire_file_descriptors(1, timeout_seconds=0.1)
    lease.release()
    assert fake.acquires[-1] == (1, 1)
    assert fake.releases[-1] == 1


def test_native_fd_abi_and_raii_cover_user_data_file_handles() -> None:
    header = (CPP / "internal/runtime/process_fd_governor.hh").read_text(encoding="utf-8")
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    methods = (CPP / "internal/abi/python_abi3/methods.hh").read_text(encoding="utf-8")
    module = (CPP / "api/python_abi3/_core_abi3_module.cc").read_text(encoding="utf-8")
    assert "class ProcessFdPermitLease" in header
    assert "acquire_process_file_descriptor_permits" in arena
    assert "process_file_descriptor_permits_acquire" in methods
    assert "process_file_descriptor_permits_acquire" in module

    for relative in (
        "ingest/chunk_source_file.cc",
        "ingest/transcoding/chunk_source.cc",
        "internal/parquet/footer_reader/footer_reader.cc",
        "api/python_abi3/json/output_adapters/output_adapters.cc",
        "api/python_abi3/csv/_core_abi3_csv_writer.cc",
        "api/python_abi3/parquet/_core_abi3_parquet_writer.cc",
    ):
        source = (CPP / relative).read_text(encoding="utf-8")
        assert "ProcessFdPermitLease" in source or (
            relative.endswith("footer_reader.cc") and "process_fd_governor.hh" in source
        ), relative

    parquet_state = (
        CPP / "internal/parquet/footer_reader/native_stream/schema/native_stream_arrow_state.cc.inc"
    ).read_text(encoding="utf-8")
    parquet_public = (
        CPP / "internal/parquet/footer_reader/reporting/footer_reader_public.cc.inc"
    ).read_text(encoding="utf-8")
    assert parquet_state.count("ProcessFdPermitLease") >= 2
    assert parquet_public.count("ProcessFdPermitLease") >= 4


def test_externally_governed_results_reject_boolean_proofs_and_require_capability() -> None:
    from schema_sanitizer.core_impl.async_scheduler import (
        AsyncResultMemoryContract,
        AsyncResultOwnershipMode,
        _assert_async_result_ownership,
    )
    from schema_sanitizer.core_impl.memory_budget import no_retained_result_ownership_capability

    forged = AsyncResultMemoryContract(
        preflight_bytes=1,
        ownership_mode=AsyncResultOwnershipMode.EXTERNALLY_GOVERNED,
        external_ownership_proof=lambda _value: True,
    )
    with pytest.raises(RuntimeError, match="runtime-issued"):
        _assert_async_result_ownership(object(), forged)

    authenticated = AsyncResultMemoryContract(
        preflight_bytes=1,
        ownership_mode=AsyncResultOwnershipMode.EXTERNALLY_GOVERNED,
        external_ownership_capability=lambda _value: no_retained_result_ownership_capability(),
    )
    _assert_async_result_ownership(None, authenticated)


def test_cgroup_resolver_prefers_complete_root_mount_over_subtree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import cgroup_view

    lines = [
        "25 1 0:20 /tenant /sys/fs/cgroup/sub rw - cgroup2 cgroup rw",
        "26 1 0:21 / /sys/fs/cgroup rw - cgroup2 cgroup rw",
    ]
    monkeypatch.setattr(cgroup_view, "_iter_bounded_proc_lines", lambda *_a, **_k: iter(lines))
    view = cgroup_view._resolve_linux_cgroup_view_once(("/tenant", {}))
    assert view.version == 2
    assert view.hierarchy_complete
    assert view.root == Path("/sys/fs/cgroup/tenant")


def test_cgroup_small_value_reader_rejects_truncation(tmp_path: Path) -> None:
    from schema_sanitizer.core_impl.cgroup_view import _read_text_path

    path = tmp_path / "memory.max"
    path.write_text("123456", encoding="ascii")
    assert _read_text_path(path, limit=4) is None
    assert _read_text_path(path, limit=8) == "123456"


def test_empty_cross_process_reconciliation_failure_rebases_without_losing_owner() -> None:
    source = _source("core_impl/cross_process_memory.py")
    start = source.index("def _get_process_coordinator")
    body = source[start : source.index("\n\ndef acquire_cross_process_memory", start)]
    assert "coordinator.reconcile_pending()" in body
    assert "coordinator.rebase_empty_capacity(capacity_bytes)" in body
    assert "return coordinator" in body


def test_native_backpressure_uses_independent_ticket_bank_and_bounded_bypass() -> None:
    source = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    header = (CPP / "internal/runtime/operation_task_arena.hh").read_text(encoding="utf-8")
    assert "ProducerWaiterCapacity" in source
    assert "backpressure_tickets" in source
    assert "kMaxOldestBypasses = 4U" in source
    assert "starvation_preventions" in source
    assert "producer_waiter_capacity()" in header
    assert "oldest_backpressure_waiter_age_millis()" in header


def test_provider_key_gates_are_bounded_and_memory_charged() -> None:
    source = _source("remote_impl/provider_session_pool.py")
    assert "_MAX_PENDING_KEY_GATES = 1024" in source
    assert "provider_pending_key_gates" in source
    assert 'acquire_operation_memory(512, stage="remote_provider_key_gate")' in source
    assert "control_bytes = min(1 << 20" in source


def test_native_probe_contains_starvation_prevention_case() -> None:
    source = (CPP_TESTS / "ordered_executor_tsan.cc").read_text(encoding="utf-8")
    assert "run_arena_backpressure_starvation_round" in source
    assert 'selected_case == "arena_backpressure_starvation"' in source
    assert "starvation_preventions()" in source
