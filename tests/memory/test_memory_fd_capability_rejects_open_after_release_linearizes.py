"""Exercises concurrent physical close, path identity, borrowed thread budgets, replay
artifacts, construction rollback, directory grouping, external pools, Parquet bundles,
scandir, and finalizer capsules. Once release linearizes, no opener can consume the
capability; credit waits for physical close, and live external borrows block premature
finalizer release."""

from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS, join_thread_or_fail

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"


def test_physical_file_owner_close_commits_once_under_two_closers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify physical file owner close commits once under two closers."""
    from schema_sanitizer.core_impl import process_resources as module

    entered = threading.Event()
    continue_close = threading.Event()
    closed_commits = 0
    lease_releases = 0

    class Stream:
        closed = False

        def close(self) -> None:
            """Close the resources owned by the stream test double."""
            entered.set()
            assert continue_close.wait(SCHEDULER_TIMEOUT_SECONDS)
            self.closed = True

    class Lease:
        def release(self) -> None:
            """Release the resource held by the lease test double."""
            nonlocal lease_releases
            lease_releases += 1

    def record_closed(_amount: int = 1) -> None:
        """Record that the physical descriptor has closed."""
        nonlocal closed_commits
        closed_commits += 1

    monkeypatch.setattr(module, "record_physical_file_descriptors_opened", lambda _n=1: None)
    monkeypatch.setattr(module, "record_physical_file_descriptors_closed", record_closed)
    owner = module._PhysicalFileOwner()
    owner.bind(Stream(), Lease())
    first = threading.Thread(target=owner.close)
    second = threading.Thread(target=owner.close)
    first.start()
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    second.start()
    continue_close.set()
    join_thread_or_fail(first)
    join_thread_or_fail(second)
    assert closed_commits == 1
    assert lease_releases == 1


def test_path_identity_does_not_release_credit_before_close_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify path identity does not release credit before close finishes."""
    from schema_sanitizer.core_impl import path_identity as module

    entered = threading.Event()
    continue_close = threading.Event()
    lease_released = threading.Event()

    class Lease:
        def release(self) -> None:
            """Release the resource held by the lease test double."""
            lease_released.set()

    def blocked_close(_fd: int) -> None:
        """Pause at the blocked close synchronization point."""
        entered.set()
        assert continue_close.wait(SCHEDULER_TIMEOUT_SECONDS)

    monkeypatch.setattr(module.os, "close", blocked_close)
    monkeypatch.setattr(module, "record_physical_file_descriptors_closed", lambda _n=1: None)
    owner = module._IdentityDescriptorOwner(123, Lease())
    waiter_entered = threading.Event()

    class TrackingCondition(threading.Condition):
        def wait(self, timeout: float | None = None) -> bool:
            """Wait for the tracking condition test double to reach its terminal state."""
            waiter_entered.set()
            return super().wait(timeout)

    owner._condition = TrackingCondition(owner.lock)
    first = threading.Thread(target=owner.release)
    second = threading.Thread(target=owner.release)
    first.start()
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    second.start()
    assert waiter_entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    with owner._condition:
        assert owner._state == owner._CLOSING
        assert owner.fd_lease is not None
    assert not lease_released.is_set()
    continue_close.set()
    join_thread_or_fail(first)
    join_thread_or_fail(second)
    assert lease_released.is_set()


def test_operation_thread_budget_is_borrowed_not_reacquired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify operation thread budget is borrowed not reacquired."""
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
    """Verify replay artifact retains storage until reader closes."""
    from schema_sanitizer.api_impl.parquet.replay_stream import _ReplayArtifactOwner

    releases: list[str] = []

    class Lease:
        def release(self) -> None:
            """Release the resource held by the lease test double."""
            releases.append("lease")

    class Pool:
        def close(self) -> None:
            """Close the resources owned by the pool test double."""
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
    """Verify factory construction failure closes previously published stage."""
    from schema_sanitizer.adapters.parquet import record_batch_factory as module

    class Artifact:
        path = None
        closed = False

        def close(self) -> None:
            """Close the resources owned by the artifact test double."""
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


def test_transient_directory_grouping_credit_is_returned_on_finish() -> None:
    """Verify transient directory grouping credit is returned on finish."""
    from schema_sanitizer.input_impl.directory_inputs import DirectoryDiscoveryBuilder
    from schema_sanitizer.input_impl.directory_metadata_budget import DirectoryMetadataBudget

    budget = DirectoryMetadataBudget(8 * 1024 * 1024)
    discovery = DirectoryDiscoveryBuilder.from_uris(["s3://bucket/a"], metadata_budget=budget)
    baseline = budget.used_bytes
    groups: list[str] = []
    discovery.publish_group_association(lambda: groups.append("x"))
    assert budget.used_bytes > baseline
    discovery.finish()
    assert budget.used_bytes == baseline
    budget.close()


def test_external_runtime_pool_cap_is_monotonic() -> None:
    """Verify external runtime pool cap is monotonic."""
    from schema_sanitizer.core_impl.process_resources import constrain_external_runtime_worker_pool

    class Runtime:
        value = 16

        @classmethod
        def cpu_count(cls) -> int:
            """Return the controlled CPU count reported by the runtime."""
            return cls.value

        @classmethod
        def set_cpu_count(cls, value: int) -> None:
            """Record the CPU count selected by the controlled runtime."""
            cls.value = value

    assert constrain_external_runtime_worker_pool(Runtime, 4) == 4
    assert Runtime.value == 4
    assert constrain_external_runtime_worker_pool(Runtime, 8) == 4
    assert Runtime.value == 4


def test_native_parquet_fd_bundle_is_preadmitted_before_arena_workers() -> None:
    """Verify native Parquet FD bundle is preadmitted before arena workers."""
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
    """Verify scandir owner does not release credit before iterator close."""
    from schema_sanitizer.core_impl import path_identity as module

    entered = threading.Event()
    continue_close = threading.Event()
    lease_released = threading.Event()

    class Iterator:
        def close(self) -> None:
            """Close the resources owned by the iterator test double."""
            entered.set()
            assert continue_close.wait(SCHEDULER_TIMEOUT_SECONDS)

    class Lease:
        def release(self) -> None:
            """Release the resource held by the lease test double."""
            lease_released.set()

    monkeypatch.setattr(module, "record_physical_file_descriptors_closed", lambda _n=1: None)
    owner = module._ScandirCleanupOwner(Iterator(), Lease())
    waiter_entered = threading.Event()

    class TrackingCondition(threading.Condition):
        def wait(self, timeout: float | None = None) -> bool:
            """Wait for the tracking condition test double to reach its terminal state."""
            waiter_entered.set()
            return super().wait(timeout)

    owner._condition = TrackingCondition(owner.lock)
    first = threading.Thread(target=owner.release)
    second = threading.Thread(target=owner.release)
    first.start()
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    second.start()
    assert waiter_entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    with owner._condition:
        assert owner._state == owner._CLOSING
        assert owner.lease is not None
    assert not lease_released.is_set()
    continue_close.set()
    join_thread_or_fail(first)
    join_thread_or_fail(second)
    assert lease_released.is_set()


def test_fd_hard_capacity_subtracts_native_governed_opened(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify FD hard capacity subtracts native governed opened."""
    from schema_sanitizer.core_impl import native_runtime
    from schema_sanitizer.core_impl import process_resources as module

    class Native:
        @staticmethod
        def process_file_descriptor_permits_snapshot() -> tuple[int, int, int, int, int, int]:
            """Return the current FD permit ledger snapshot."""
            return (10, 10, 100, 0, 0, 0)

    class Resource:
        RLIMIT_NOFILE = object()

        @staticmethod
        def getrlimit(kind: object) -> tuple[int, int]:
            """Return hard and soft descriptor limits of one hundred."""
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
    """Verify external runtime lease preacquires native physical threads."""
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(3, "fd-capability-rejects-open-after-release_native_threads")
    calls: list[tuple[str, int]] = []

    class Native:
        def acquire_exact_permit_lease(self, desired: int, minimum: int):
            """Acquire the fake exact-permit lease requested by the resource owner."""
            assert desired == minimum
            calls.append(("acquire", desired))
            return SimpleNamespace(amount=desired), desired

        @staticmethod
        def exact_permit_lease_amount(receipt: object) -> int:
            """Return the exact permit amount tracked by the fake lease."""
            return int(receipt.amount)  # type: ignore[attr-defined]

        @staticmethod
        def resize_exact_permit_lease(receipt: object, target: int) -> int:
            """Resize the fake exact-permit lease to the requested amount."""
            previous = int(receipt.amount)  # type: ignore[attr-defined]
            receipt.amount = target  # type: ignore[attr-defined]
            calls.append(("release", previous - target))
            return target

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
    """Verify operation lease finalizer capsule refuses live external borrow."""
    from types import SimpleNamespace

    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(2, "fd-capability-rejects-open-after-release_finalizer_borrow")
    lease = governor.try_acquire_up_to(2, minimum=2)
    assert lease is not None
    budget = module._OperationThreadBorrowBudget(1)
    borrow = budget.try_borrow_up_to_exact(1, minimum=1, exact=True)
    assert borrow is not None
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
    borrow.release()
    lease.release()
    assert governor.snapshot().in_use == 0
