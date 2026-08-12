"""Regression coverage for memory fd capability rejects open after release linearizes."""

from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"


def _source(relative: str) -> str:
    return (SRC / relative).read_text(encoding="utf-8")


def test_fd_capability_rejects_open_after_release_linearizes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    entered = threading.Event()
    continue_release = threading.Event()

    class Lease:
        def release(self) -> None:
            entered.set()
            assert continue_release.wait(2)

    capability = module.FileDescriptorCapability(
        Lease(), 1, label="fd-capability-rejects-open-after-release"
    )
    monkeypatch.setattr(module, "record_physical_file_descriptors_opened", lambda _n=1: None)
    thread = threading.Thread(target=capability.release)
    thread.start()
    assert entered.wait(2)
    with pytest.raises(RuntimeError, match="being released"):
        capability._mark_opened()
    continue_release.set()
    thread.join(2)
    assert not thread.is_alive()
    assert capability._lease is None
    assert capability.opened == 0


def test_physical_file_owner_close_commits_once_under_two_closers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    entered = threading.Event()
    continue_close = threading.Event()
    closed_commits = 0
    lease_releases = 0

    class Stream:
        closed = False

        def close(self) -> None:
            entered.set()
            assert continue_close.wait(2)
            self.closed = True

    class Lease:
        def release(self) -> None:
            nonlocal lease_releases
            lease_releases += 1

    def record_closed(_amount: int = 1) -> None:
        nonlocal closed_commits
        closed_commits += 1

    monkeypatch.setattr(module, "record_physical_file_descriptors_opened", lambda _n=1: None)
    monkeypatch.setattr(module, "record_physical_file_descriptors_closed", record_closed)
    owner = module._PhysicalFileOwner()
    owner.bind(Stream(), Lease())
    first = threading.Thread(target=owner.close)
    second = threading.Thread(target=owner.close)
    first.start()
    assert entered.wait(2)
    second.start()
    continue_close.set()
    first.join(2)
    second.join(2)
    assert closed_commits == 1
    assert lease_releases == 1


def test_path_identity_does_not_release_credit_before_close_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import path_identity as module

    entered = threading.Event()
    continue_close = threading.Event()
    lease_released = threading.Event()

    class Lease:
        def release(self) -> None:
            lease_released.set()

    def blocked_close(_fd: int) -> None:
        entered.set()
        assert continue_close.wait(2)

    monkeypatch.setattr(module.os, "close", blocked_close)
    monkeypatch.setattr(module, "record_physical_file_descriptors_closed", lambda _n=1: None)
    owner = module._IdentityDescriptorOwner(123, Lease())
    first = threading.Thread(target=owner.release)
    second = threading.Thread(target=owner.release)
    first.start()
    assert entered.wait(2)
    second.start()
    assert not lease_released.wait(0.1)
    continue_close.set()
    first.join(2)
    second.join(2)
    assert lease_released.is_set()


def test_operation_thread_budget_is_borrowed_not_reacquired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(5, "fd-capability-rejects-open-after-release_threads")
    operation = governor.try_acquire_up_to(5, minimum=5)
    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: None)
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: operation)

    runtime = module.acquire_external_runtime_threads(10, allow_parallel=True)
    assert runtime.parallel
    assert runtime.workers == 4
    assert runtime._lease is None
    assert runtime._parent_lease is operation
    operation_lease_id = operation.lease_id
    with governor._condition:
        operation_entry = governor._active_leases[operation_lease_id]
        assert operation_entry.owner_id == id(operation)
        assert operation_entry.capability is operation._capability
        assert operation_entry.amount == 5
        assert not operation_entry.resource_released
    with pytest.raises(RuntimeError, match="external runtime workers are borrowed"):
        operation.release()
    runtime.close()
    operation.release()
    with governor._condition:
        assert operation_lease_id not in governor._active_leases


def test_replay_artifact_retains_storage_until_reader_closes(tmp_path: Path) -> None:
    from schema_sanitizer.api_impl.parquet.replay_stream import _ReplayArtifactOwner

    releases: list[str] = []

    class Lease:
        def release(self) -> None:
            releases.append("lease")

    class Pool:
        def close(self) -> None:
            releases.append("pool")

    path = tmp_path / "replay.arrow"
    path.write_bytes(b"x")
    owner = _ReplayArtifactOwner(Lease(), Pool())
    owner.bind_path(str(path))
    reader = owner.acquire_reader()
    owner.close()
    assert not path.exists()
    assert releases == []
    reader.close()
    assert releases == ["lease", "pool"]
    assert owner.released


def test_factory_construction_failure_closes_previously_published_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.adapters.parquet import record_batch_factory as module

    class Artifact:
        path = None
        closed = False

        def close(self) -> None:
            self.closed = True

    artifact = Artifact()
    prepared = module.PreparedParquetFactorySource(
        local_path=None, staged_path=None, staged_artifact=artifact, native_source_kind="text"
    )
    monkeypatch.setattr(module, "prepare_parquet_factory_source", lambda *_a, **_k: prepared)
    monkeypatch.setattr(
        module, "ensure_pyarrow", lambda **_k: (_ for _ in ()).throw(MemoryError("boom"))
    )
    with pytest.raises(MemoryError, match="boom"):
        module.ParquetRecordBatchStreamFactory(b"x", source="text", feature="test")
    assert artifact.closed


def test_parquet_factory_keeps_each_published_stream_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    from schema_sanitizer.adapters.parquet import record_batch_factory as module

    class Runtime:
        parallel = False
        workers = 1
        closed = False

        def close(self) -> None:
            self.closed = True

    class Reader:
        def __arrow_c_stream__(self) -> object:
            return object()

        def close(self) -> None:
            pass

    class Scanner:
        def to_reader(self) -> Reader:
            return Reader()

        def close(self) -> None:
            pass

    class Dataset:
        def scanner(self, **_kwargs: object) -> Scanner:
            return Scanner()

    factory = SimpleNamespace(
        _filters=None,
        _dataset=Dataset(),
        _columns=None,
        _batch_size=16,
        _dataset_error=None,
        _pending_parquet_file=None,
        _pending_opened_file=None,
        _keepalive=[],
        _pa=SimpleNamespace(),
    )
    runtimes: list[Runtime] = []

    def runtime(_factory: object) -> Runtime:
        value = Runtime()
        runtimes.append(value)
        return value

    monkeypatch.setattr(module, "_external_runtime_threads", runtime)
    monkeypatch.setattr(module, "record_parquet_fallback_attempt", lambda *_a, **_k: None)
    monkeypatch.setattr(module, "record_parquet_fallback_success", lambda *_a, **_k: None)
    module.pyarrow_fallback_arrow_stream(
        factory,
        record_batch_reader_from_iterable=lambda *_a, **_k: None,
        logger=SimpleNamespace(debug=lambda *_a, **_k: None),
    )
    module.pyarrow_fallback_arrow_stream(
        factory,
        record_batch_reader_from_iterable=lambda *_a, **_k: None,
        logger=SimpleNamespace(debug=lambda *_a, **_k: None),
    )
    assert len(factory._keepalive) == 2
    assert all(not item.closed for item in runtimes)


def test_transient_directory_grouping_credit_is_returned_on_finish() -> None:
    from threading import Condition, Lock

    from schema_sanitizer.input_impl.directory_inputs import DirectoryDiscoveryBuilder
    from schema_sanitizer.input_impl.directory_metadata_budget import (
        DirectoryMetadataBudget,
        RetainedDirectoryMetadata,
    )

    budget = object.__new__(DirectoryMetadataBudget)
    budget.limit_bytes = 8 * 1024 * 1024
    budget._operation_memory_ledger = None
    budget._retention_owner = RetainedDirectoryMetadata()
    budget._used_bytes = 0
    budget._admission_lock = Lock()
    budget._lock = Lock()
    budget._close_condition = Condition(budget._lock)
    budget._close_started = False
    budget._closing = False
    budget._closed = False
    discovery = DirectoryDiscoveryBuilder.from_uris(["s3://bucket/a"], metadata_budget=budget)
    baseline = budget.used_bytes
    groups: list[str] = []
    discovery.publish_group_association(lambda: groups.append("x"))
    assert budget.used_bytes > baseline
    discovery.finish()
    assert budget.used_bytes == baseline
    budget.close()


def test_external_runtime_pool_cap_is_monotonic() -> None:
    from schema_sanitizer.core_impl.process_resources import constrain_external_runtime_worker_pool

    class Runtime:
        value = 16

        @classmethod
        def cpu_count(cls) -> int:
            return cls.value

        @classmethod
        def set_cpu_count(cls, value: int) -> None:
            cls.value = value

    assert constrain_external_runtime_worker_pool(Runtime, 4) == 4
    assert Runtime.value == 4
    assert constrain_external_runtime_worker_pool(Runtime, 8) == 4
    assert Runtime.value == 4


def test_native_parquet_fd_bundle_is_preadmitted_before_arena_workers() -> None:
    header = (ROOT / "cpp/src/internal/runtime/process_fd_governor.hh").read_text(encoding="utf-8")
    parallel = (
        ROOT
        / "cpp/src/internal/parquet/footer_reader/native_stream/materialization/row_group/native_stream_parallel_columns.cc.inc"
    ).read_text(encoding="utf-8")
    assert "TryAcquireUpTo" in header
    assert "ProcessFdPermitLease split" in header
    assert "auto fd_bundle" in parallel
    assert "effective_workers" in parallel
    assert "fd_bundle.split(1U)" in parallel
    section = parallel[
        parallel.index("sanitize::Status materialize_native_row_group_columns_parallel") :
    ]
    assert section.index("auto fd_bundle") < section.index(
        "native_parquet_grouped_column_decode_eligible"
    )


def test_scandir_owner_does_not_release_credit_before_iterator_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import path_identity as module

    entered = threading.Event()
    continue_close = threading.Event()
    lease_released = threading.Event()

    class Iterator:
        def close(self) -> None:
            entered.set()
            assert continue_close.wait(2)

    class Lease:
        def release(self) -> None:
            lease_released.set()

    monkeypatch.setattr(module, "record_physical_file_descriptors_closed", lambda _n=1: None)
    owner = module._ScandirCleanupOwner(Iterator(), Lease())
    first = threading.Thread(target=owner.release)
    second = threading.Thread(target=owner.release)
    first.start()
    assert entered.wait(2)
    second.start()
    assert not lease_released.wait(0.1)
    continue_close.set()
    first.join(2)
    second.join(2)
    assert lease_released.is_set()


def test_fd_hard_capacity_subtracts_native_governed_opened(monkeypatch: pytest.MonkeyPatch) -> None:
    from schema_sanitizer.core_impl import native_runtime
    from schema_sanitizer.core_impl import process_resources as module

    class Native:
        @staticmethod
        def process_file_descriptor_permits_snapshot() -> tuple[int, int, int, int]:
            return (10, 10, 100, 0)

    class Resource:
        RLIMIT_NOFILE = object()

        @staticmethod
        def getrlimit(kind: object) -> tuple[int, int]:
            assert kind is Resource.RLIMIT_NOFILE
            return (100, 100)

    monkeypatch.setattr(module, "_fd_requested_capacity", lambda: 100)
    monkeypatch.setattr(module, "_open_fd_count", lambda: 20)
    monkeypatch.setattr(module, "_python_governed_fds_opened", lambda: 2)
    monkeypatch.setattr(module, "resource", Resource())
    monkeypatch.setattr(native_runtime, "native_core", Native())
    # reserve=16, external=20-10 native-governed = 10 => 100-16-10
    assert module._fd_hard_capacity() == 74


def test_external_runtime_lease_preacquires_native_physical_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(3, "fd-capability-rejects-open-after-release_native_threads")
    calls: list[tuple[str, int]] = []

    class Native:
        def process_physical_thread_permits_acquire(self, desired: int, minimum: int) -> int:
            assert desired == minimum
            calls.append(("acquire", desired))
            return desired

        def process_physical_thread_permits_release(self, amount: int) -> None:
            calls.append(("release", amount))

    native = Native()
    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: None)
    runtime = module.acquire_external_runtime_threads(2, allow_parallel=True)
    assert runtime.parallel and runtime.workers == 2
    logical_claim = runtime._lease
    assert logical_claim is not None
    logical_claim_id = logical_claim.lease_id
    with governor._condition:
        logical_entry = governor._active_leases[logical_claim_id]
        assert logical_entry.owner_id == id(logical_claim)
        assert logical_entry.capability is logical_claim._capability
        assert logical_entry.amount == 2
        assert not logical_entry.resource_released
    assert calls == [("acquire", 2)]
    runtime.close()
    assert calls == [("acquire", 2), ("release", 2)]
    with governor._condition:
        assert logical_claim_id not in governor._active_leases


def test_operation_lease_finalizer_capsule_refuses_live_external_borrow() -> None:
    from types import SimpleNamespace

    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(2, "fd-capability-rejects-open-after-release_finalizer_borrow")
    lease = governor.try_acquire_up_to(2, minimum=2)
    assert lease is not None
    budget = module._OperationThreadBorrowBudget(1)
    assert budget.try_borrow(1)
    capsule = SimpleNamespace(
        arg0=governor,
        arg1=lease.lease_id,
        arg2=lease._capability,
        arg3=budget,
    )
    before = governor.snapshot().in_use
    with pytest.raises(RuntimeError, match="external runtime workers are borrowed"):
        module._release_process_lease_capsule(capsule)
    assert governor.snapshot().in_use == before == 2
    budget.release(1)
    lease.release()
    assert governor.snapshot().in_use == 0


def test_external_runtime_close_retries_component_release_without_losing_owner() -> None:
    from schema_sanitizer.core_impl import process_resources as module

    calls: list[str] = []

    class Native:
        attempts = 0

        def process_physical_thread_permits_release(self, amount: int) -> None:
            assert amount == 2
            self.attempts += 1
            calls.append(f"native:{self.attempts}")
            if self.attempts == 1:
                raise RuntimeError("retry native release")

    class Lease:
        released = 0

        def release(self) -> None:
            self.released += 1
            calls.append("lease")

    native = Native()
    lease = Lease()
    runtime = module.ExternalRuntimeConcurrencyLease(
        lease, workers=2, parallel=True, native=native, native_amount=2
    )
    with pytest.raises(RuntimeError, match="retry native release"):
        runtime.close()
    assert lease.released == 0
    assert runtime._native is native
    assert runtime._native_amount == 2
    assert runtime._lease is lease

    runtime.close()
    assert calls == ["native:1", "native:2", "lease"]
    assert lease.released == 1
    assert runtime._native is None
    assert runtime._native_amount == 0
    assert runtime._lease is None
