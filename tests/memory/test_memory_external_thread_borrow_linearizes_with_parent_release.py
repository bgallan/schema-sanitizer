"""Regression coverage for memory external thread borrow linearizes with parent release."""

from __future__ import annotations

import gc
import os
import threading
import time
import weakref
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"
CPP = ROOT / "cpp/src"


def test_external_thread_borrow_linearizes_with_parent_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(4, "external-thread-borrow-linearizes-with-parent_atomic_borrow")
    operation = governor.try_acquire_up_to(4, minimum=4)
    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: None)
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: operation)

    entered = threading.Event()
    continue_borrow = threading.Event()
    original = module._OperationThreadBorrowBudget.try_borrow_up_to

    def blocked(self: object, desired: int, *, minimum: int = 1) -> int:
        entered.set()
        assert continue_borrow.wait(2)
        return original(self, desired, minimum=minimum)

    monkeypatch.setattr(module._OperationThreadBorrowBudget, "try_borrow_up_to", blocked)
    result: list[object] = []
    acquire_thread = threading.Thread(
        target=lambda: result.append(
            module.acquire_external_runtime_threads(3, allow_parallel=True)
        )
    )
    release_errors: list[BaseException] = []
    release_thread = threading.Thread(
        target=lambda: _capture_error(operation.release, release_errors)
    )
    acquire_thread.start()
    assert entered.wait(2)
    release_thread.start()
    time.sleep(0.05)
    assert release_thread.is_alive(), "parent release must block behind atomic child publication"
    continue_borrow.set()
    acquire_thread.join(2)
    release_thread.join(2)
    assert len(result) == 1
    runtime = result[0]
    assert getattr(runtime, "workers") == 3
    assert getattr(runtime, "_lease") is None
    assert getattr(runtime, "_parent_lease") is operation
    operation_lease_id = operation.lease_id
    with governor._condition:
        operation_entry = governor._active_leases[operation_lease_id]
        assert operation_entry.owner_id == id(operation)
        assert operation_entry.capability is operation._capability
        assert operation_entry.amount == 4
        assert not operation_entry.resource_released
    assert release_errors and "external runtime workers are borrowed" in str(release_errors[0])
    runtime.close()
    operation.release()
    with governor._condition:
        assert operation_lease_id not in governor._active_leases


def _capture_error(fn: object, errors: list[BaseException]) -> None:
    try:
        fn()  # type: ignore[misc]
    except BaseException as exc:
        errors.append(exc)


def test_parent_shrink_cannot_invalidate_live_external_borrow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(5, "external-thread-borrow-linearizes-with-parent_shrink")
    operation = governor.try_acquire_up_to(5, minimum=5)
    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: None)
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: operation)
    runtime = module.acquire_external_runtime_threads(3, allow_parallel=True)
    assert runtime.workers == 3
    with pytest.raises(RuntimeError, match="below live external borrows"):
        operation.shrink(2)
    assert operation.amount == 5
    runtime.close()
    operation.shrink(2)
    assert operation.amount == 2
    budget = operation.__dict__["_external_runtime_borrow_budget"]
    assert budget.capacity == 1
    operation.release()


def test_fd_opening_state_prevents_credit_release_before_opener_returns(tmp_path: Path) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    released = threading.Event()
    entered = threading.Event()
    continue_open = threading.Event()

    class Lease:
        def release(self) -> None:
            released.set()

    capability = module.FileDescriptorCapability(
        Lease(), 1, label="external-thread-borrow-linearizes-with-parent-opening"
    )
    path = tmp_path / "x"
    path.write_bytes(b"x")

    def opener() -> int:
        fd = os.open(path, os.O_RDONLY)
        entered.set()
        assert continue_open.wait(2)
        return fd

    errors: list[BaseException] = []

    def use() -> None:
        try:
            with capability.open_descriptor(opener):
                pass
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=use)
    thread.start()
    assert entered.wait(2)
    assert capability.opening == 1
    with pytest.raises(RuntimeError, match="descriptors are opening"):
        capability.release()
    assert not released.is_set()
    continue_open.set()
    thread.join(2)
    assert not errors
    assert capability.opening == 0
    assert capability.opened == 0
    capability.release()
    assert released.is_set()


def test_external_runtime_gc_finalizer_returns_all_components() -> None:
    from schema_sanitizer.core_impl import process_resources as module

    calls: list[str] = []

    class Native:
        def process_physical_thread_permits_release(self, amount: int) -> None:
            calls.append(f"native:{amount}")

    class Lease:
        def release(self) -> None:
            calls.append("lease")

    budget = module._OperationThreadBorrowBudget(2)
    assert budget.try_borrow(2)
    runtime = module.ExternalRuntimeConcurrencyLease(
        Lease(),
        workers=2,
        parallel=True,
        parent_lease=object(),
        borrow_budget=budget,
        borrowed=2,
        native=Native(),
        native_amount=2,
    )
    ref = weakref.ref(runtime)
    del runtime
    gc.collect()
    module.drain_finalizer_cleanup()
    assert ref() is None
    assert budget.borrowed == 0
    assert calls == ["native:2", "lease"]


def test_external_runtime_shrinks_logical_and_native_envelope_to_real_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(8, "external-thread-borrow-linearizes-with-parent_pool_width")
    native_calls: list[tuple[str, int]] = []

    class Native:
        def process_physical_thread_permits_acquire(self, desired: int, minimum: int) -> int:
            native_calls.append(("acquire", desired))
            return desired

        def process_physical_thread_permits_release(self, amount: int) -> None:
            native_calls.append(("release", amount))

    class Runtime:
        @staticmethod
        def cpu_count() -> int:
            return 2

        @staticmethod
        def set_cpu_count(_value: int) -> None:
            raise AssertionError("pool already at physical width")

    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_refresh_thread_governor_capacity", lambda: None)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: Native())
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: None)
    lease = module.acquire_external_runtime_threads(8, allow_parallel=True)
    logical_claim = lease._lease
    assert logical_claim is not None
    assert logical_claim.amount == 8
    logical_claim_id = logical_claim.lease_id
    configured = module.constrain_external_runtime_worker_pool(Runtime, lease.workers)
    lease.shrink_to(configured)
    assert configured == 2
    assert lease.workers == 2
    assert lease._lease is logical_claim
    assert logical_claim.amount == 2
    with governor._condition:
        logical_entry = governor._active_leases[logical_claim_id]
        assert logical_entry.owner_id == id(logical_claim)
        assert logical_entry.capability is logical_claim._capability
        assert logical_entry.amount == 2
        assert not logical_entry.resource_released
    assert native_calls[:2] == [("acquire", 8), ("release", 6)]
    lease.close()
    assert lease._lease is None
    assert logical_claim._released
    with governor._condition:
        assert logical_claim_id not in governor._active_leases
    assert native_calls[-1] == ("release", 2)


def test_dataset_lifetime_retains_fd_and_stage_until_last_stream_reference() -> None:
    from schema_sanitizer.adapters.parquet import record_batch_factory as module

    events: list[str] = []

    class Dataset:
        def __del__(self) -> None:
            events.append("dataset")

    class Capability:
        def close(self) -> None:
            events.append("capability")

    class Stage:
        def close(self) -> None:
            events.append("stage")

    owner = module._DatasetLifetimeOwner(Dataset(), Capability(), Stage())
    stream_ref = owner.acquire()
    owner.close()  # release factory reference only
    assert events == []
    stream_ref.close()
    assert events == ["dataset", "capability", "stage"]


def test_stream_scoped_owner_retires_when_batch_iterator_finishes() -> None:
    from schema_sanitizer.adapters.parquet import record_batch_factory as module

    events: list[str] = []

    class Resource:
        def close(self) -> None:
            events.append("close")

    owner = module._ParquetStreamKeepaliveOwner()
    owner.add(Resource())
    registry: list[object] = []
    registration = weakref.ref(owner)
    registry.append(registration)
    batches = module._OwnedParquetBatchIterator(iter(()), owner, registry, registration)
    with pytest.raises(StopIteration):
        next(batches)
    assert events == ["close"]
    assert registry == []


def test_close_waits_have_real_deadlines(monkeypatch: pytest.MonkeyPatch) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    monkeypatch.setattr(module, "_RESOURCE_CLOSE_WAIT_TIMEOUT_SECONDS", 0.05)
    entered = threading.Event()
    continue_close = threading.Event()

    class Stream:
        closed = False

        def close(self) -> None:
            entered.set()
            assert continue_close.wait(2)
            self.closed = True

    class Lease:
        def release(self) -> None:
            pass

    monkeypatch.setattr(module, "record_physical_file_descriptors_opened", lambda _n=1: None)
    monkeypatch.setattr(module, "record_physical_file_descriptors_closed", lambda _n=1: None)
    owner = module._PhysicalFileOwner()
    owner.bind(Stream(), Lease())
    first = threading.Thread(target=owner.close)
    first.start()
    assert entered.wait(2)
    clock_values = iter((100.0, 101.0))
    clock_reads = 0

    def advancing_clock() -> float:
        nonlocal clock_reads
        clock_reads += 1
        return next(clock_values)

    monkeypatch.setattr(module, "monotonic", advancing_clock)
    with pytest.raises(module.SchemaSanitizerResourceError, match="timed out"):
        owner.close()
    assert clock_reads == 2
    continue_close.set()
    first.join(2)

    path_src = (SRC / "core_impl/path_identity.py").read_text(encoding="utf-8")
    replay_src = (SRC / "api_impl/parquet/replay_stream.py").read_text(encoding="utf-8")
    assert "deadline = monotonic() + _OWNER_CLOSE_WAIT_TIMEOUT_SECONDS" in path_src
    assert "deadline = monotonic() + _REPLAY_CLOSE_WAIT_TIMEOUT_SECONDS" in replay_src


def test_native_thread_authority_is_owner_process_guarded() -> None:
    source = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    acquire = source[source.index("TryAcquireProcessPhysicalThreadPermitsUpTo") :]
    acquire = acquire[: acquire.index("TryAcquireProcessExternalRuntimeThreadPermitsUpTo")]
    assert "!runtime_owner_process()" in acquire
    release = source[source.index("void release_process_physical_thread_permits") :]
    release = release[: release.index("std::size_t acquire_process_file_descriptor_permits")]
    assert "!runtime_owner_process()" in release


def test_native_fd_protocol_violation_and_uncertain_debt_are_observable() -> None:
    header = (CPP / "internal/runtime/process_fd_governor.hh").read_text(encoding="utf-8")
    runtime = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    abi = (CPP / "api/python_abi3/runtime/ordered_executor_probe.cc").read_text(encoding="utf-8")
    assert "amount > available" in header
    assert "record_process_file_descriptor_protocol_violation" in header
    assert "record_process_file_descriptor_uncertain_close_debt(amount_)" in header
    assert "g_process_file_descriptor_protocol_violations" in runtime
    assert "g_process_file_descriptor_uncertain_close_debts" in runtime
    assert "PyTuple_New(6)" in abi


def test_native_fd_snapshot_accepts_extended_telemetry(monkeypatch: pytest.MonkeyPatch) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    class Native:
        @staticmethod
        def process_file_descriptor_permits_snapshot() -> tuple[int, int, int, int, int, int]:
            return (3, 2, 100, 4, 5, 6)

    monkeypatch.setattr(module, "_native_file_descriptor_api", lambda: Native())
    snapshot = module.native_file_descriptor_snapshot()
    assert snapshot["reserved"] == 3
    assert snapshot["opened"] == 2
    assert snapshot["protocol_violations"] == 5
    assert snapshot["uncertain_close_debts"] == 6


def test_dataset_lifetime_cleanup_failure_keeps_exact_owner_for_retry() -> None:
    from schema_sanitizer.adapters.parquet import record_batch_factory as module

    events: list[str] = []

    class Dataset:
        def __del__(self) -> None:
            events.append("dataset")

    class Capability:
        attempts = 0

        def close(self) -> None:
            self.attempts += 1
            events.append(f"capability:{self.attempts}")
            if self.attempts == 1:
                raise RuntimeError("retry")

    class Stage:
        def close(self) -> None:
            events.append("stage")

    owner = module._DatasetLifetimeOwner(Dataset(), Capability(), Stage())
    with pytest.raises(RuntimeError, match="retry"):
        owner.close()
    assert events == ["dataset", "capability:1"]
    assert owner._fd_capability is not None
    assert owner._staged_artifact is not None
    owner.close()
    assert events == ["dataset", "capability:1", "capability:2", "stage"]
    assert owner._closed


def test_unconfigurable_runtime_reserves_reported_pool_width(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.api_impl import results as module

    requested: list[tuple[int, bool]] = []

    class Lease:
        workers = 3
        parallel = True

        def close(self) -> None:
            pass

    class Runtime:
        @staticmethod
        def thread_pool_size() -> int:
            return 3

    def acquire(desired: int, *, allow_parallel: bool, runtime: object | None = None) -> Lease:
        assert runtime is Runtime
        requested.append((desired, allow_parallel))
        return Lease()

    monkeypatch.setattr(module, "acquire_external_runtime_threads", acquire)
    lease = module._unconfigurable_external_threads(Runtime)
    assert lease.workers == 3
    assert requested == [(3, True)]


def test_dataset_lifetime_retirement_is_single_closer_under_concurrent_close() -> None:
    from schema_sanitizer.adapters.parquet import record_batch_factory as module

    entered = threading.Event()
    continue_close = threading.Event()
    events: list[str] = []

    class Dataset:
        def __del__(self) -> None:
            events.append("dataset")

    class Capability:
        def close(self) -> None:
            events.append("capability")
            entered.set()
            assert continue_close.wait(2)

    class Stage:
        def close(self) -> None:
            events.append("stage")

    owner = module._DatasetLifetimeOwner(Dataset(), Capability(), Stage())
    errors: list[BaseException] = []
    first = threading.Thread(target=lambda: _capture_error(owner.close, errors))
    first.start()
    assert entered.wait(2)

    # A duplicate closer must not race the already-published retirement or
    # release any component a second time.
    owner.close()
    assert events == ["dataset", "capability"]
    continue_close.set()
    first.join(2)
    assert not errors
    assert events == ["dataset", "capability", "stage"]
    assert owner._closed
