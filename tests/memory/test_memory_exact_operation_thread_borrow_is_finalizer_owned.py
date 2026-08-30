"""Combines finalizer-owned thread borrows with pool shrink, shutdown-safe finalizers, lazy
DuckDB and stream handoff, logical or physical claims, file-descriptor receipts, and
generation-wrap guards. Results outlive temporary wrappers safely, while exact receipts
and owner tokens drive cleanup and make interrupted or retried releases idempotent."""

from __future__ import annotations

import gc
import importlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.usefixtures("isolated_external_runtime_coordinator")


def _root() -> Path:
    """Return the repository root used by source-contract checks."""
    return Path(__file__).resolve().parents[2]


def _reset_external(module) -> None:
    """Reset cached external-runtime state between lifecycle checks."""
    module.drain_finalizer_cleanup()
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR.clear()
    module._EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS = 0
    module._EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS = 0


def test_exact_operation_thread_borrow_is_finalizer_owned() -> None:
    """Verify exact operation thread borrow is finalizer owned."""
    from schema_sanitizer.core_impl import process_resources as module

    module.drain_finalizer_cleanup()
    budget = module._OperationThreadBorrowBudget(4)
    owner = budget.try_borrow_up_to_exact(3, minimum=1)
    assert owner is not None
    assert owner.amount == 3
    assert budget.borrowed == 3
    del owner
    gc.collect()
    module.drain_finalizer_cleanup()
    assert budget.borrowed == 0


def test_exact_operation_thread_reservation_serializes_shrinkable_source_pool() -> None:
    """A lazy source cannot consume the suffix reserved for a fixed-width sink."""
    from schema_sanitizer.core_impl import process_resources as module

    budget = module._OperationThreadBorrowBudget(32, exact_reservation=32)
    assert budget.try_borrow_up_to_exact(5, minimum=2) is None

    exact = budget.try_borrow_up_to_exact(32, minimum=32, exact=True)
    assert exact is not None
    assert exact.amount == 32
    exact.release()
    assert budget.borrowed == 0


@pytest.mark.parametrize(
    ("module_name", "owner_name"),
    (
        ("schema_sanitizer.adapters.parquet.record_batch_factory", "_StagedParquetArtifact"),
        (
            "schema_sanitizer.adapters.parquet.record_batch_factory",
            "ParquetRecordBatchStreamFactory",
        ),
        ("schema_sanitizer.api_impl.parquet.replay_stream", "_ReplayReader"),
        ("schema_sanitizer.api_impl.parquet.replay_stream", "ReplayableArrowStream"),
        ("schema_sanitizer.core_impl.generated_bytes", "BufferedGeneratedBytesReader"),
        ("schema_sanitizer.core_impl.process_resources", "GovernedFile"),
        ("schema_sanitizer.remote_impl.sync_backend", "SyncDirectoryDownloadSession"),
    ),
)
def test_resource_finalizers_tolerate_cleared_shutdown_globals(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    owner_name: str,
) -> None:
    """Late interpreter teardown cannot leak an unraisable ``TypeError``."""
    module = importlib.import_module(module_name)
    owner_type = getattr(module, owner_name)
    # CPython 3.11 rejects ``object.__new__`` for ``io.IOBase`` subclasses
    # such as GovernedFile.  Calling the type's allocator still bypasses
    # ``__init__`` (the teardown state this test needs) and is portable across
    # every supported CPython release.
    owner = owner_type.__new__(owner_type)
    monkeypatch.setattr(module, "runtime_is_finalizing", None)
    owner.__del__()


def test_result_safe_point_drops_lazy_value_before_operation_keepalive() -> None:
    """A lazy result cannot retain external workers while its context closes."""
    from schema_sanitizer.api_impl import results as module

    released: list[bool] = []

    class LazyValue:
        def __del__(self) -> None:
            """Run fallback cleanup when the lazy value test double is collected."""
            released.append(True)

    class Keepalive:
        def close(self) -> None:
            """Close the resources owned by the keepalive test double."""
            if not released:
                raise RuntimeError("lazy value still owns the upstream reader")

    state = module._ResultFinalizerState(
        clean_data_cache=LazyValue(),
        keepalive=Keepalive(),
    )
    module._close_result_finalizer_capsule(SimpleNamespace(arg0=state))

    assert released == [True]
    assert state.clean_data_cache is None
    assert state.keepalive is None


def test_duckdb_result_uses_private_connection_without_mutating_default() -> None:
    """Lazy DuckDB ownership must not rewrite a caller's global connection."""
    import schema_sanitizer as ss

    duckdb = pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    default = duckdb.connect(database=":default:")
    previous = int(default.execute("SELECT current_setting('threads')").fetchone()[0])
    expected = max(2, min(4, os.cpu_count() or 2))
    result = None
    relation = None
    payload_owner = None
    try:
        default.execute(f"SET threads={expected}")
        result = ss.to_duckdb(
            [{"a": 1}],
            input_format="python",
            multi_threading=True,
            memory_limit_bytes=64 << 20,
        )
        relation = result.clean_data
        keepalive_items = list(
            getattr(getattr(relation, "_lifetime", None)._keepalive, "_items", ())
        )
        payload_owners = [
            item
            for item in keepalive_items
            if getattr(item, "memory_lease", None) is not None
            and getattr(item, "control_ticket", None) is not None
        ]
        assert len(payload_owners) == 1
        payload_owner = payload_owners[0]
        assert payload_owner.memory_lease.reserved_bytes > 0
        assert len(relation.fetchall()) == 1
        assert payload_owner.memory_lease.reserved_bytes > 0
        assert type(relation).__name__ == "_OwnedDuckDBRelation"
        assert int(default.execute("SELECT current_setting('threads')").fetchone()[0]) == expected
    finally:
        if result is not None:
            result.close()
            assert payload_owner is not None and payload_owner.memory_lease is None
        default.execute(f"SET threads={previous}")


def test_duckdb_relation_and_derivatives_outlive_temporary_result() -> None:
    """One-line relation extraction keeps the private connection and operation alive."""
    import schema_sanitizer as ss
    from schema_sanitizer.core_impl.finalizer_registry import finalizer_domains

    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    relation = ss.to_duckdb(
        [{"a": 1}, {"a": 2}],
        input_format="python",
        multi_threading=True,
        memory_limit_bytes=64 << 20,
    ).clean_data
    lifetime = relation._lifetime
    derived = relation.project("a")
    del relation
    gc.collect()

    successor = ss.to_pyarrow(
        [{"b": 3}],
        input_format="python",
        memory_limit_bytes=64 << 20,
    )
    successor.close()
    for _ in range(4):
        for domain in finalizer_domains():
            domain.drain()

    assert derived.fetchall() == [(1,), (2,)]
    derived.close()
    for _ in range(4):
        for domain in finalizer_domains():
            domain.drain()
    assert lifetime._owner is None
    assert lifetime._keepalive is None


def test_duckdb_base_relation_outlives_temporary_result() -> None:
    """Result finalization drops its reference without closing an extracted relation."""
    import schema_sanitizer as ss
    from schema_sanitizer.core_impl.finalizer_registry import finalizer_domains

    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    relation = ss.to_duckdb(
        [{"a": 1}],
        input_format="python",
        memory_limit_bytes=64 << 20,
    ).clean_data
    gc.collect()
    for _ in range(4):
        for domain in finalizer_domains():
            domain.drain()

    assert relation.fetchall()[0][0] == 1
    relation.close()


def test_duckdb_relation_proxy_unwraps_derived_relation_arguments() -> None:
    """DuckDB methods accepting relations receive native objects from one lifetime."""
    from schema_sanitizer.api_impl.results import convert_arrow_table_output

    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("duckdb")
    relation = convert_arrow_table_output(
        pa.table({"a": [1, 2]}),
        "duckdb",
        feature="owned DuckDB relation argument",
    )
    left = relation.filter("a = 1")
    right = relation.filter("a = 2")
    combined = left.union(right)
    try:
        assert sorted(combined.fetchall()) == [(1,), (2,)]
        assert "a" in combined
        assert combined["a"] is not None
    finally:
        combined.close()
        right.close()
        left.close()
        relation.close()


def test_lazy_duckdb_handoff_survives_async_unwind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An interrupt after owner attachment cannot double-close or leak the context."""
    import schema_sanitizer as ss
    from schema_sanitizer.api_impl import analytical
    from schema_sanitizer.core_impl.cross_process_memory import process_cross_memory_snapshot
    from schema_sanitizer.core_impl.finalizer_registry import finalizer_domains
    from schema_sanitizer.core_impl.process_resources import (
        external_runtime_pool_snapshot,
    )

    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    for _ in range(4):
        for domain in finalizer_domains():
            domain.drain()
    before_external = external_runtime_pool_snapshot()
    before_memory = process_cross_memory_snapshot()
    original = analytical._retain_lazy_analytical_resources
    captured = SimpleNamespace(
        lifetime=None,
        execution_lease=None,
        payload_admission=None,
    )

    def interrupt_after_handoff(*args, **kwargs):
        """Interrupt the operation immediately after ownership handoff."""
        operation_context = kwargs["operation_context"]
        pair_scope = kwargs["pair_scope"]
        captured.execution_lease = operation_context.execution_lease
        captured.payload_admission = pair_scope.payload_admission
        original(*args, **kwargs)
        captured.lifetime = args[0]._clean_data_cache._lifetime
        raise KeyboardInterrupt("fault after lazy owner publication")

    monkeypatch.setattr(
        analytical,
        "_retain_lazy_analytical_resources",
        interrupt_after_handoff,
    )
    with pytest.raises(KeyboardInterrupt, match="lazy owner publication"):
        ss.to_duckdb(
            [{"a": 1}],
            input_format="python",
            multi_threading=True,
            memory_limit_bytes=64 << 20,
        )
    monkeypatch.setattr(analytical, "_retain_lazy_analytical_resources", original)

    for _ in range(12):
        gc.collect()
        for domain in finalizer_domains():
            domain.drain()
    after_external = external_runtime_pool_snapshot()
    after_memory = process_cross_memory_snapshot()
    assert captured.lifetime is not None
    assert captured.lifetime._owner is None
    assert captured.lifetime._keepalive is None
    assert captured.lifetime._references == 0
    assert captured.execution_lease is not None
    assert captured.execution_lease._released is True
    assert captured.payload_admission is not None
    assert captured.payload_admission.memory_lease is None
    assert captured.payload_admission.control_ticket is None
    assert captured.payload_admission.execution_lease is None
    assert after_external["claims"] == before_external["claims"]
    assert after_external["logical_claims"] == before_external["logical_claims"]
    assert after_memory["logical_contributions"] == before_memory["logical_contributions"]


def test_lazy_stream_handoff_survives_async_unwind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An interrupt before the caller STORE cannot duplicate a Stream payload owner."""
    import schema_sanitizer as ss
    from schema_sanitizer.api_impl import analytical
    from schema_sanitizer.core_impl.finalizer_registry import finalizer_domains

    pytest.importorskip("pyarrow")
    captured_resources = []
    original = analytical.lazy_stream_from_opened

    def interrupt_before_return(*args, **kwargs):
        """Interrupt the operation before ownership can be returned."""
        stream = original(*args, **kwargs)
        captured_resources.append(stream._keepalive)
        raise KeyboardInterrupt("fault before lazy stream return")

    monkeypatch.setattr(analytical, "lazy_stream_from_opened", interrupt_before_return)
    with pytest.raises(KeyboardInterrupt, match="lazy stream return"):
        ss.iter_batches(
            [{"a": 1}],
            input_format="python",
            multi_threading=True,
            memory_limit_bytes=64 << 20,
        )
    monkeypatch.setattr(analytical, "lazy_stream_from_opened", original)

    assert len(captured_resources) == 1
    resources = captured_resources[0]
    payload_owner = resources._payload_owner
    assert payload_owner is not None
    for _ in range(12):
        gc.collect()
        for domain in finalizer_domains():
            domain.drain()
    assert resources._opened is None
    assert resources._payload_owner is None
    assert resources._prepared_input is None
    assert resources._operation_context is None
    assert payload_owner.memory_lease is None
    assert payload_owner.control_ticket is None


def test_logical_pool_rollback_clears_partially_published_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify logical pool rollback clears partially published envelope."""
    from schema_sanitizer.core_impl import process_resources as module

    class Injected(RuntimeError):
        pass

    class FailPositiveDict(dict[int, int]):
        def __setitem__(self, key: int, value: int) -> None:
            """Store the requested value in the fail positive dict test double."""
            if int(value) > 0:
                raise Injected("fault after logical envelope publication")
            super().__setitem__(key, value)

    class LogicalLease:
        amount = 2
        _released = False

        def release(self) -> None:
            """Release the resource held by the logical lease test double."""
            self._released = True
            self.amount = 0

        def shrink(self, target: int) -> None:
            """Create a ledger entry whose logical-claim map fails after publication."""
            self.amount = int(target)

    original_entry_type = module._ExternalRuntimePoolCoordinatorEntry

    def make_entry(*, runtime=None, runtime_key=None, **kwargs):
        """Set the logical lease to the requested amount."""
        return original_entry_type(
            runtime=runtime,
            runtime_key=runtime_key,
            logical_claims=FailPositiveDict(),
            **kwargs,
        )

    _reset_external(module)
    monkeypatch.setattr(module, "_ExternalRuntimePoolCoordinatorEntry", make_entry)
    monkeypatch.setattr(module, "acquire_project_threads", lambda *_a, **_k: LogicalLease())

    class Runtime:
        pass

    with pytest.raises(Injected):
        module._acquire_shared_external_logical_thread_lease(Runtime(), 2)

    assert module._EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS == 0
    assert not module._EXTERNAL_RUNTIME_POOL_COORDINATOR


def test_shared_physical_claim_dropped_before_handoff_releases_exact_envelope() -> None:
    """Verify shared physical claim dropped before handoff releases exact envelope."""
    from schema_sanitizer.core_impl import process_resources as module

    class Receipt:
        amount = 2

    class Native:
        supports_exact_permit_lease = True

        def exact_permit_lease_amount(self, lease: Receipt) -> int:
            """Return the exact permit amount tracked by the fake lease."""
            return lease.amount

        def resize_exact_permit_lease(self, lease: Receipt, target: int) -> None:
            """Resize the fake exact-permit lease to the requested amount."""
            if target > lease.amount:
                raise ValueError("grow")
            lease.amount = target

    _reset_external(module)
    key = ("declared", ("exact-operation-thread-borrow-is-finalizer", "physical-handoff"))
    receipt = Receipt()
    entry = module._ExternalRuntimePoolCoordinatorEntry(
        runtime=None,
        runtime_key=key,
        native=Native(),
        native_lease=receipt,
        physical_amount=2,
        physical_claims={1: 2},
    )
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = entry
    module._EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS = 1

    claim = module._SharedExternalRuntimeNativePermit(key, 1)
    del claim
    gc.collect()
    module.drain_finalizer_cleanup()
    assert receipt.amount == 0
    assert module._EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS == 0
    assert key not in module._EXTERNAL_RUNTIME_POOL_COORDINATOR


def test_external_cleanup_uses_exact_owner() -> None:
    """Verify external cleanup uses exact owner."""
    from schema_sanitizer.core_impl import process_resources as module

    class Owner:
        def __init__(self) -> None:
            """Initialize the owner test double."""
            self.amount = 2
            self.targets: list[int] = []

        def resize_physical_thread_permits(self, target: int) -> None:
            """Resize the controlled physical-thread permit allocation."""
            self.targets.append(int(target))
            self.amount = int(target)

    owner = Owner()
    state = module._ExternalRuntimeCleanupState(native=owner)
    module._cleanup_external_runtime_capsule(SimpleNamespace(arg0=state))
    assert owner.targets == [0]
    assert owner.amount == 0
    assert state.native is None


def test_deferred_memory_close_tail_runs_without_second_close_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify deferred memory close tail runs without second close call."""
    from threading import Condition, Lock

    from schema_sanitizer.core_impl import memory_budget as module

    ledger = object.__new__(module.OperationMemoryLedger)
    ledger._pid = os.getpid()
    ledger._close_started = True
    ledger._cross_process_release_deferred = True
    ledger._closing = False
    ledger._python_leases = {}
    ledger._close_condition = Condition(Lock())
    calls: list[int] = []
    monkeypatch.setattr(module.OperationMemoryLedger, "close", lambda self: calls.append(1))
    ledger._maybe_finish_deferred_close()
    assert calls == [1]


def test_fd_exact_receipt_owns_opened_state_and_blocks_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify FD exact receipt owns opened state and blocks release."""
    from schema_sanitizer.core_impl import process_resources as module

    class Receipt:
        def __init__(self, amount: int) -> None:
            """Initialize the receipt test double."""
            self.receipt_id = 1
            self.generation = 1
            self.amount = amount
            self.opened = 0

    class Native:
        def __init__(self) -> None:
            """Initialize the native test double."""
            self.receipt: Receipt | None = None

        def process_file_descriptor_permit_lease_acquire_wait(self, desired, minimum, _timeout):
            """Issue an exact descriptor receipt when the desired count meets the minimum."""
            if desired < minimum:
                return None
            self.receipt = Receipt(int(desired))
            return self.receipt, int(desired)

        def process_file_descriptor_permit_lease_metadata(self, receipt):
            """Return metadata for the exact FD permit lease."""
            return receipt.receipt_id, receipt.generation, receipt.amount, receipt.opened

        def process_file_descriptor_permit_lease_resize(self, receipt, target, generation):
            """Resize a receipt above its opened count and advance its generation."""
            assert generation == receipt.generation
            if target < receipt.opened:
                raise ValueError("below opened")
            receipt.amount = int(target)
            receipt.generation += 1
            return receipt.generation, receipt.amount, receipt.opened

        def process_file_descriptor_permit_lease_mark_opened(self, receipt, amount, generation):
            """Increase opened descriptors within the receipt limit and advance its generation."""
            assert generation == receipt.generation
            if receipt.opened + amount > receipt.amount:
                raise ValueError("open exceeds permits")
            receipt.opened += int(amount)
            receipt.generation += 1
            return receipt.generation, receipt.amount, receipt.opened

        def process_file_descriptor_permit_lease_mark_closed(self, receipt, amount, generation):
            """Decrease opened descriptors without over-closing and advance the generation."""
            assert generation == receipt.generation
            if amount > receipt.opened:
                raise ValueError("over-close")
            receipt.opened -= int(amount)
            receipt.generation += 1
            return receipt.generation, receipt.amount, receipt.opened

    native = Native()
    governor = module._Governor(
        8, "exact-operation-thread-borrow-is-finalizer_fd", teardown_reserve=1
    )
    monkeypatch.setattr(module, "_FD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_native_file_descriptor_api", lambda: native)
    monkeypatch.setattr(module, "_refresh_fd_governor_capacity", lambda: None)

    lease = module._acquire_file_descriptor_lease(1, timeout_seconds=1.0, teardown=False)
    capability = module.FileDescriptorCapability(
        lease, 1, label="exact-operation-thread-borrow-is-finalizer"
    )
    with capability.open_descriptor(lambda: os.open(os.devnull, os.O_RDONLY)):
        assert native.receipt is not None and native.receipt.opened == 1
        with pytest.raises(RuntimeError, match="open descriptors"):
            capability.release()
    assert native.receipt.opened == 0
    capability.release()
    assert native.receipt.amount == 0


def test_fd_interruption_after_exact_open_commit_is_repaired_after_physical_close(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Verify FD interruption after exact open commit is repaired after physical close."""
    from schema_sanitizer.core_impl import process_resources as module

    class Receipt:
        receipt_id = 4
        generation = 1
        amount = 1
        opened = 0

    class Native:
        def __init__(self) -> None:
            """Initialize the native test double."""
            self.receipt = Receipt()
            self.fail = True

        def process_file_descriptor_permit_lease_acquire_wait(self, *_args):
            """Acquire the controlled FD permit lease without blocking."""
            return self.receipt, 1

        def process_file_descriptor_permit_lease_metadata(self, receipt):
            """Return metadata for the exact FD permit lease."""
            return receipt.receipt_id, receipt.generation, receipt.amount, receipt.opened

        def process_file_descriptor_permit_lease_resize(self, receipt, target, generation):
            """Resize a receipt only when its target covers every opened descriptor."""
            assert generation == receipt.generation
            if target < receipt.opened:
                raise ValueError("below opened")
            if target != receipt.amount:
                receipt.amount = target
                receipt.generation += 1
            return receipt.generation, receipt.amount, receipt.opened

        def process_file_descriptor_permit_lease_mark_opened(self, receipt, amount, generation):
            """Commit an opened descriptor, then inject the configured interrupt."""
            assert generation == receipt.generation
            receipt.opened += amount
            receipt.generation += 1
            if self.fail:
                self.fail = False
                raise KeyboardInterrupt("fault after exact opened commit")
            return receipt.generation, receipt.amount, receipt.opened

        def process_file_descriptor_permit_lease_mark_closed(self, receipt, amount, generation):
            """Commit descriptor closure and return the advanced receipt state."""
            assert generation == receipt.generation
            receipt.opened -= amount
            receipt.generation += 1
            return receipt.generation, receipt.amount, receipt.opened

    native = Native()
    governor = module._Governor(
        8, "exact-operation-thread-borrow-is-finalizer_fd_fault", teardown_reserve=1
    )
    monkeypatch.setattr(module, "_FD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_native_file_descriptor_api", lambda: native)
    monkeypatch.setattr(module, "_refresh_fd_governor_capacity", lambda: None)

    lease = module._acquire_file_descriptor_lease(1, timeout_seconds=1.0, teardown=False)
    capability = module.FileDescriptorCapability(
        lease, 1, label="exact-operation-thread-borrow-is-finalizer-fault"
    )
    path = tmp_path / "fd"
    path.write_bytes(b"x")
    with pytest.raises(KeyboardInterrupt):
        with capability.open_descriptor(lambda: os.open(path, os.O_RDONLY)):
            pass
    assert native.receipt.opened == 0
    assert capability.opened == 0
    capability.release()
    assert native.receipt.amount == 0


def test_external_generation_is_passed_as_optimistic_concurrency_token() -> None:
    """Verify external generation is passed as optimistic concurrency token."""
    from schema_sanitizer.core_impl import process_resources as module

    calls: list[tuple[int, int]] = []
    lease = object()

    class Core:
        @staticmethod
        def process_external_runtime_thread_permit_lease_metadata(_lease):
            """Return metadata for the external-runtime thread lease."""
            return 9, 7, 3

        @staticmethod
        def process_external_runtime_thread_permit_lease_resize(_lease, target, generation):
            """Record the external-thread resize and return its next generation."""
            calls.append((target, generation))
            return generation + 1, target

    authority = module._ExternalNativeThreadAuthority(Core())
    authority.resize_exact_permit_lease(lease, 2)
    assert calls == [(2, 7)]


def test_receipt_ids_and_generations_fail_closed_before_wrap() -> None:
    """Verify receipt ids and generations fail closed before wrap."""
    prepare = (_root() / "cpp/src/api/python_abi3/options/prepare.cc").read_text()
    probe = (_root() / "cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc").read_text()

    assert "operation memory reservation id space exhausted" in prepare
    assert "operation memory reservation generation exhausted" in prepare
    assert "external runtime permit receipt id space exhausted" in probe
    assert "external runtime permit receipt generation exhausted" in probe
    assert "file descriptor permit receipt id space exhausted" in probe
    assert "file descriptor permit receipt generation exhausted" in probe
    assert "stale operation memory reservation generation" in prepare
    assert "stale external runtime permit receipt generation" in probe
    assert "stale file descriptor permit receipt generation" in probe


def test_exact_owners_drive_resource_cleanup() -> None:
    """Verify exact owners drive resource cleanup."""
    resources = (_root() / "src/schema_sanitizer/core_impl/process_resources.py").read_text()
    memory = (_root() / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()

    assert "_sync_external_logical_lease_width_locked" in resources
    assert "_sync_external_native_lease_amount_locked" in resources
    assert "native.resize_physical_thread_permits(0)" in resources
    assert "borrow_lease.release()" in resources
    assert "self._maybe_finish_deferred_close()" in memory
    assert "process_file_descriptor_permit_lease_mark_opened" in resources
    assert "process_file_descriptor_permit_lease_mark_closed" in resources


def test_logical_release_retry_recognizes_underlying_target_zero_commit() -> None:
    """Verify logical release retry recognizes underlying target zero commit."""
    from schema_sanitizer.core_impl import process_resources as module

    class LogicalLease:
        def __init__(self) -> None:
            """Initialize the logical lease test double."""
            self.amount = 2
            self._released = False
            self.fail = True

        def release(self) -> None:
            """Release the resource held by the logical lease test double."""
            self._released = True
            self.amount = 0
            if self.fail:
                self.fail = False
                raise KeyboardInterrupt("fault after logical governor release commit")

        def shrink(self, target: int) -> None:
            """Set the logical lease to the requested target amount."""
            self.amount = int(target)

    _reset_external(module)
    key = ("declared", ("exact-operation-thread-borrow-is-finalizer", "logical-release-retry"))
    logical = LogicalLease()
    entry = module._ExternalRuntimePoolCoordinatorEntry(
        runtime=None,
        runtime_key=key,
        logical_lease=logical,  # type: ignore[arg-type]
        logical_width=2,
        logical_claims={1: 2},
    )
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = entry
    module._EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS = 1
    claim = module._SharedExternalRuntimeLogicalLease(key, 1, 2)

    with pytest.raises(KeyboardInterrupt):
        claim.release()
    assert logical._released
    assert entry.logical_claims == {1: 2}

    # Retry must reconcile the already-committed release, not call it twice or
    # declare the stale mirror corrupt.
    claim.release()
    assert module._EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS == 0
    assert key not in module._EXTERNAL_RUNTIME_POOL_COORDINATOR
