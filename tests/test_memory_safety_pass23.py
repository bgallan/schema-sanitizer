"""Regression tests for pass23 transactional ownership and device isolation."""

from __future__ import annotations

import asyncio
import os
import sys
import threading
from pathlib import Path
from typing import Any

import pytest


class _RetryingMemoryOwner:
    """Minimal operation-memory owner with injectable release failures."""

    def __init__(self, failures: int = 0) -> None:
        """Initialize the test double."""
        self.failures = failures
        self.reserved = 0
        self.release_calls: list[int] = []

    def reserve(self, amount: int, *, stage: str) -> None:
        """Reserve the synthetic resource."""
        assert stage in {"pass23", "directory_metadata"}
        self.reserved += amount

    def release(self, amount: int) -> None:
        """Release the synthetic resource."""
        self.release_calls.append(amount)
        if self.failures:
            self.failures -= 1
            raise OSError("native release unavailable")
        self.reserved -= amount


def test_operation_memory_lease_release_remains_retryable() -> None:
    """A failed native release cannot discard the lease's local ownership."""
    from schema_sanitizer.core_impl.memory_budget import OperationMemoryLease

    owner = _RetryingMemoryOwner(failures=1)
    lease = OperationMemoryLease(owner, 17, "pass23")

    with pytest.raises(OSError, match="native release"):
        lease.release()
    assert lease.reserved_bytes == 17
    assert owner.reserved == 17

    lease.release()
    assert lease.reserved_bytes == 0
    assert owner.reserved == 0
    assert owner.release_calls == [17, 17]


def test_operation_memory_lease_shrink_remains_retryable() -> None:
    """A failed shrink retains the original reservation for a later retry."""
    from schema_sanitizer.core_impl.memory_budget import OperationMemoryLease

    owner = _RetryingMemoryOwner(failures=1)
    lease = OperationMemoryLease(owner, 20, "pass23")

    with pytest.raises(OSError, match="native release"):
        lease.resize(7)
    assert lease.reserved_bytes == 20
    assert owner.reserved == 20

    lease.resize(7)
    assert lease.reserved_bytes == 7
    assert owner.reserved == 7
    lease.release()


def _directory_budget_for_test(owner: _RetryingMemoryOwner, used: int) -> Any:
    """Construct a focused directory budget without loading native options."""
    from schema_sanitizer.input_impl.directory_inputs import DirectoryMetadataBudget

    budget = object.__new__(DirectoryMetadataBudget)
    budget.limit_bytes = 1 << 20
    budget._operation_memory_ledger = owner
    budget._used_bytes = used
    budget._lock = threading.Lock()
    budget._close_condition = threading.Condition(budget._lock)
    budget._close_started = False
    budget._closing = False
    budget._closed = False
    owner.reserved = used
    return budget


def test_directory_metadata_close_is_retryable_and_stops_admission() -> None:
    """Close intent survives a native failure without losing retained bytes."""
    owner = _RetryingMemoryOwner(failures=1)
    budget = _directory_budget_for_test(owner, 23)

    with pytest.raises(OSError, match="native release"):
        budget.close()
    assert budget.used_bytes == 23
    assert owner.reserved == 23

    with pytest.raises(RuntimeError, match="closed"):
        budget._charge(5, observed=1)
    assert owner.reserved == 23

    budget.close()
    assert budget.used_bytes == 0
    assert owner.reserved == 0


def test_directory_metadata_rollback_preserves_primary_failure() -> None:
    """A rollback failure is diagnostic and cannot replace the admission error."""
    owner = _RetryingMemoryOwner(failures=1)
    budget = _directory_budget_for_test(owner, 0)
    budget._close_started = True

    with pytest.raises(RuntimeError, match="closed") as captured:
        budget._charge(3, observed=1)
    notes = getattr(captured.value, "__notes__", [])
    assert any("rollback also failed" in note for note in notes)


def test_directory_uri_materialization_is_budget_bounded() -> None:
    """An oversized or infinite source is rejected before full materialization."""
    from schema_sanitizer.input_impl.directory_inputs import DirectoryMetadataBudget

    budget = object.__new__(DirectoryMetadataBudget)
    budget.limit_bytes = 600
    budget._operation_memory_ledger = None
    budget._used_bytes = 0
    budget._lock = threading.Lock()
    budget._close_condition = threading.Condition(budget._lock)
    budget._close_started = False
    budget._closing = False
    budget._closed = False
    yielded = 0

    def uris() -> Any:
        """Yield an intentionally unbounded test sequence."""
        nonlocal yielded
        while True:
            yielded += 1
            if yielded > 4:
                raise AssertionError("URI source was materialized past its budget")
            yield "x" * 10

    with pytest.raises(Exception, match="directory_metadata"):
        budget.charge_uris(uris())
    assert yielded == 3
    assert budget.used_bytes == 0


def test_directory_utf8_measurement_stops_after_limit() -> None:
    """Huge strings are measured incrementally instead of encoded all at once."""
    from schema_sanitizer.input_impl.directory_metadata_budget import (
        _utf8_size_bounded,
    )

    assert _utf8_size_bounded("x" * 1_000_000, 1024) == 1025
    assert _utf8_size_bounded("\ud800" * 1_000_000, 1024) == 1025


def test_directory_budget_reexport_and_module_size() -> None:
    """The extraction preserves imports and keeps the discovery module bounded."""
    from schema_sanitizer.input_impl import directory_inputs
    from schema_sanitizer.input_impl.directory_metadata_budget import (
        DirectoryMetadataBudget,
    )

    assert directory_inputs.DirectoryMetadataBudget is DirectoryMetadataBudget
    assert len(Path(directory_inputs.__file__).read_text().splitlines()) <= 500


@pytest.mark.parametrize("method", ["acquire", "try_acquire_up_to"])
def test_process_resource_owner_is_built_before_capacity_commit(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
) -> None:
    """Owner allocation failure cannot strand logical process capacity."""
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(4, "pass23")

    class BrokenLease:
        """Test double used by pass23 hardening regressions."""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            """Initialize the test double."""
            raise MemoryError("owner allocation failed")

    monkeypatch.setattr(module, "_Lease", BrokenLease)
    with pytest.raises(MemoryError, match="owner allocation"):
        if method == "acquire":
            governor.acquire(2, timeout_seconds=0.01)
        else:
            governor.try_acquire_up_to(2)
    snapshot = governor.snapshot()
    assert snapshot.in_use == 0
    assert snapshot.waiting == 0


def test_remote_submission_owner_is_built_before_accounting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A submission owner allocation failure leaves no pending slot."""
    from schema_sanitizer.remote_impl import io_permits as module

    governor = module.RemoteIoPermitGovernor(2)

    class BrokenReservation:
        """Test double used by pass23 hardening regressions."""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            """Initialize the test double."""
            raise MemoryError("submission owner failed")

    monkeypatch.setattr(module, "RemoteIoSubmissionReservation", BrokenReservation)
    with pytest.raises(MemoryError, match="submission owner"):
        governor.reserve_submission()
    assert governor.snapshot().pending_submissions == 0


def test_remote_capacity_owner_is_built_before_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A registration owner allocation failure leaves no retained ceiling."""
    from schema_sanitizer.remote_impl import io_permits as module

    governor = module.RemoteIoPermitGovernor(2)

    class BrokenRegistration:
        """Test double used by pass23 hardening regressions."""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            """Initialize the test double."""
            raise MemoryError("capacity owner failed")

    monkeypatch.setattr(module, "RemoteIoCapacityRegistration", BrokenRegistration)
    with pytest.raises(MemoryError, match="capacity owner"):
        governor.register_capacity(8)
    snapshot = governor.snapshot()
    assert snapshot.active_capacity_registrations == 0
    assert snapshot.capacity == 2


def test_remote_permit_owner_failure_reclaims_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Permit construction failure wakes the waiter and returns weighted capacity."""
    from schema_sanitizer.remote_impl import io_permits as module

    governor = module.RemoteIoPermitGovernor(2)

    class BrokenPermit:
        """Test double used by pass23 hardening regressions."""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            """Initialize the test double."""
            raise MemoryError("permit owner failed")

    monkeypatch.setattr(module, "RemoteIoPermit", BrokenPermit)

    async def exercise() -> None:
        """Exercise the synthetic regression path."""
        with pytest.raises(MemoryError, match="permit owner"):
            await governor.acquire(2, operation_id="pass23")

    asyncio.run(exercise())
    snapshot = governor.snapshot()
    assert snapshot.in_use == 0
    assert snapshot.waiting == 0
    assert snapshot.delivery_failures == 1


def test_temporary_governor_blocks_only_one_filesystem(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A wedged device journal cannot serialize admission on another device."""
    from schema_sanitizer.core_impl import temporary_storage_governor as module

    governor = module._ProcessTemporaryStorageGovernor()
    entered = threading.Event()
    unblock = threading.Event()
    completed: list[int] = []

    def filesystem(path: str | Path | None) -> tuple[int, Path, int]:
        """Return synthetic filesystem metadata."""
        device = 1 if str(path).endswith("a") else 2
        return device, tmp_path, 1 << 30

    def reserve_cross_process(device: int, *_args: object, **_kwargs: object) -> None:
        """Reserve the synthetic resource."""
        if device == 1:
            entered.set()
            assert unblock.wait(timeout=2.0)

    monkeypatch.setattr(governor, "filesystem", filesystem)
    monkeypatch.setattr(governor, "free_inodes", lambda _path: 1 << 20)
    monkeypatch.setattr(module, "cross_process_storage_enabled", lambda: False)
    monkeypatch.setattr(module, "reserve_cross_process", reserve_cross_process)
    monkeypatch.setattr(module, "release_cross_process", lambda *_a, **_k: None)

    worker = threading.Thread(
        target=lambda: completed.append(governor.reserve(1, path="a", label="blocked-device"))
    )
    worker.start()
    assert entered.wait(timeout=1.0)

    second = governor.reserve(1, path="b", label="independent-device")
    assert second == 2
    unblock.set()
    worker.join(timeout=2.0)
    assert not worker.is_alive()
    assert completed == [1]

    governor.release(1, 1)
    governor.release(2, 1)
    assert governor._states == {}


def test_temporary_state_lifetime_prevents_orphan_race(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An idle state is not retired while another thread has borrowed it."""
    from schema_sanitizer.core_impl import temporary_storage_governor as module

    governor = module._ProcessTemporaryStorageGovernor()
    monkeypatch.setattr(
        governor,
        "filesystem",
        lambda _path: (7, tmp_path, 1 << 30),
    )
    monkeypatch.setattr(governor, "free_inodes", lambda _path: 1 << 20)
    monkeypatch.setattr(module, "cross_process_storage_enabled", lambda: False)
    monkeypatch.setattr(module, "reserve_cross_process", lambda *_a, **_k: None)
    monkeypatch.setattr(module, "release_cross_process", lambda *_a, **_k: None)

    assert governor.reserve(1, path=tmp_path, label="first") == 7
    state = governor._borrow_state(7)
    assert state is not None
    governor.release(7, 1)
    assert governor._states.get(7) is state
    governor._return_state(7, state)
    assert 7 not in governor._states


def test_temporary_existing_state_does_not_reread_dynamic_coordination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An existing device keeps its creation-time coordination configuration."""
    from schema_sanitizer.core_impl import temporary_storage_governor as module

    governor = module._ProcessTemporaryStorageGovernor()
    monkeypatch.setattr(
        governor,
        "filesystem",
        lambda _path: (9, tmp_path, 1 << 30),
    )
    monkeypatch.setattr(governor, "free_inodes", lambda _path: 1 << 20)
    calls = 0

    def enabled() -> bool:
        """Return whether synthetic coordination is enabled."""
        nonlocal calls
        calls += 1
        if calls > 1:
            raise AssertionError("existing state reread dynamic configuration")
        return False

    monkeypatch.setattr(module, "cross_process_storage_enabled", enabled)
    monkeypatch.setattr(module, "reserve_cross_process", lambda *_a, **_k: None)
    monkeypatch.setattr(module, "release_cross_process", lambda *_a, **_k: None)

    governor.reserve(1, path=tmp_path, label="first")
    governor.reserve(1, path=tmp_path, label="second")
    assert calls == 1
    governor.release(9, 2)


def test_pass23_runs_in_owner_process() -> None:
    """Keep PID-sensitive regressions explicit when run under unusual harnesses."""
    assert os.getpid() > 0


class _NativeOutstandingLedger:
    """Small native-ledger double for deferred host ownership tests."""

    def __init__(self, reserved: int) -> None:
        """Initialize the test double."""
        self.reserved = reserved
        self.peak = reserved

    def operation_memory_ledger_release(self, _capsule: object, amount: int) -> None:
        """Release synthetic operation memory."""
        self.reserved = max(0, self.reserved - amount)

    def operation_memory_ledger_snapshot(self, _capsule: object) -> tuple[int, int, int]:
        """Return synthetic memory diagnostics."""
        return 1 << 30, self.reserved, self.peak

    def operation_memory_ledger_diagnostics(self, _capsule: object) -> tuple[int, int]:
        """Return synthetic memory diagnostics."""
        return 0, 0


class _DeferredCrossProcessOwner:
    """Track full host-wide release attempts with injectable failures."""

    def __init__(self, failures: int = 0) -> None:
        """Initialize the test double."""
        self.failures = failures
        self.release_calls = 0
        self.resize_calls: list[int] = []

    def resize(self, amount: int) -> None:
        """Exercise the synthetic regression path."""
        self.resize_calls.append(amount)

    def release(self) -> None:
        """Release the synthetic resource."""
        self.release_calls += 1
        if self.failures:
            self.failures -= 1
            raise OSError("host journal unavailable")


def _outstanding_ledger_for_test(
    native: _NativeOutstandingLedger,
    cross: _DeferredCrossProcessOwner,
) -> Any:
    """Construct one operation ledger without importing the native extension."""
    from schema_sanitizer.core_impl.memory_budget import OperationMemoryLedger

    ledger = object.__new__(OperationMemoryLedger)
    ledger.limit_bytes = 1 << 30
    ledger._pid = os.getpid()
    ledger._native = native
    ledger._capsule = object()
    ledger._cross_process = cross
    ledger._cross_process_reconciliation_failures = 0
    ledger._cross_process_pending_bytes = 0
    ledger._cross_process_release_deferred = False
    ledger._cross_process_release_failures = 0
    ledger._lock = threading.Lock()
    ledger._close_condition = threading.Condition(ledger._lock)
    ledger._close_started = False
    ledger._closing = False
    ledger._closed = False
    ledger._close_outstanding_bytes = 0
    return ledger


def test_memory_close_retains_host_reservation_for_live_results() -> None:
    """Closing admission cannot under-account native bytes retained by results."""
    native = _NativeOutstandingLedger(64)
    cross = _DeferredCrossProcessOwner()
    ledger = _outstanding_ledger_for_test(native, cross)

    ledger.close()
    diagnostics = ledger.diagnostics()
    assert diagnostics.close_outstanding_bytes == 64
    assert diagnostics.cross_process_release_deferred
    assert cross.release_calls == 0

    ledger.release(64)
    assert cross.release_calls == 1
    assert not ledger.diagnostics().cross_process_release_deferred


def test_deferred_host_release_failure_can_be_retried_by_close() -> None:
    """A failed final journal update remains safe and explicitly retryable."""
    native = _NativeOutstandingLedger(11)
    cross = _DeferredCrossProcessOwner(failures=1)
    ledger = _outstanding_ledger_for_test(native, cross)

    ledger.close()
    ledger.release(11)
    diagnostics = ledger.diagnostics()
    assert diagnostics.cross_process_release_deferred
    assert diagnostics.cross_process_release_failures == 1
    assert native.reserved == 0

    ledger.close()
    assert cross.release_calls == 2
    assert not ledger.diagnostics().cross_process_release_deferred


@pytest.mark.parametrize(
    ("module_name", "class_name", "cleanup_name"),
    [
        (
            "schema_sanitizer.core_impl.cross_process_memory",
            "CrossProcessMemoryLease",
            "release",
        ),
        (
            "schema_sanitizer.remote_impl.staging_paths",
            "StagedPath",
            "close",
        ),
        (
            "schema_sanitizer.core_impl.process_resources",
            "_Lease",
            "release",
        ),
        (
            "schema_sanitizer.remote_impl.io_permits",
            "RemoteIoSubmissionReservation",
            "release",
        ),
        (
            "schema_sanitizer.remote_impl.io_permits",
            "RemoteIoCapacityRegistration",
            "release",
        ),
        (
            "schema_sanitizer.remote_impl.io_permits",
            "RemoteIoPermit",
            "release",
        ),
        (
            "schema_sanitizer.remote_impl.provider_throttle",
            "ProviderRequestLease",
            "release",
        ),
    ],
)
def test_blocking_finalizers_skip_interpreter_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    class_name: str,
    cleanup_name: str,
) -> None:
    """Finalizers do not enter journals or filesystem cleanup during teardown."""
    import importlib

    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    instance = object.__new__(cls)
    calls = 0

    def cleanup(_self: object) -> None:
        """Exercise the synthetic regression path."""
        nonlocal calls
        calls += 1

    monkeypatch.setattr(module, "runtime_is_finalizing", lambda: True)
    monkeypatch.setattr(cls, cleanup_name, cleanup)
    instance.__del__()
    assert calls == 0


def test_memory_lease_finalizer_skips_interpreter_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A memory finalizer cannot start cross-process reconciliation at shutdown."""
    from schema_sanitizer.core_impl import memory_budget as module

    owner = _RetryingMemoryOwner()
    lease = module.OperationMemoryLease(owner, 9, "pass23")
    monkeypatch.setattr(module, "runtime_is_finalizing", lambda: True)
    lease.__del__()
    assert lease.reserved_bytes == 9
    monkeypatch.setattr(module, "runtime_is_finalizing", lambda: False)
    lease.release()


def test_temporary_lease_finalizer_skips_interpreter_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A temporary finalizer cannot block on a host-wide journal at shutdown."""
    from schema_sanitizer.core_impl import temporary_storage as module

    class Pool:
        """Test double used by pass23 hardening regressions."""

        def __init__(self) -> None:
            """Initialize the test double."""
            self.calls = 0

        def _release(self, *_args: object, **_kwargs: object) -> None:
            """Release the synthetic resource."""
            self.calls += 1

    pool = Pool()
    lease = module.TemporaryStorageLease(
        pool,
        1,
        label="pass23",
        filesystem_key=1,
        filesystem_path=tmp_path,
        inode_count=1,
    )
    monkeypatch.setattr(module, "runtime_is_finalizing", lambda: True)
    lease.__del__()
    assert pool.calls == 0
    monkeypatch.setattr(module, "runtime_is_finalizing", lambda: False)
    lease.release()
    assert pool.calls == 1


@pytest.fixture
def native_import_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide import-time native metadata for wrapper-only tests."""
    from schema_sanitizer.core_impl import native_runtime

    class Stub:
        """Test double used by pass23 hardening regressions."""

        def options_catalog(self) -> tuple[object, ...]:
            """Return the synthetic native options catalog."""
            return ()

        def __getattr__(self, _name: str) -> Any:
            """Reject unsupported synthetic native attributes."""
            return lambda *_args, **_kwargs: None

    names = (
        "schema_sanitizer.remote_impl.transport",
        "schema_sanitizer.remote_impl.upload_policy",
        "schema_sanitizer.core_impl.execution_policy",
        "schema_sanitizer.core_impl.native_options",
    )
    preexisting_modules = set(sys.modules)
    sentinel = object()
    saved = {name: sys.modules.get(name, sentinel) for name in names}

    def purge(name: str) -> None:
        sys.modules.pop(name, None)
        parent_name, _, attribute = name.rpartition(".")
        parent = sys.modules.get(parent_name)
        if parent is not None and hasattr(parent, attribute):
            delattr(parent, attribute)

    monkeypatch.setattr(native_runtime, "native_core", Stub())
    for name in reversed(names):
        purge(name)
    try:
        yield
    finally:
        created_modules = sorted(
            (
                name
                for name in tuple(sys.modules)
                if name.startswith("schema_sanitizer.") and name not in preexisting_modules
            ),
            key=lambda name: name.count("."),
            reverse=True,
        )
        for name in created_modules:
            purge(name)
        for name in reversed(names):
            purge(name)
        for name, module in saved.items():
            if module is sentinel:
                continue
            sys.modules[name] = module
            parent_name, _, attribute = name.rpartition(".")
            parent = sys.modules.get(parent_name)
            if parent is not None:
                setattr(parent, attribute, module)


class _RetryingClose:
    """Inject one cleanup failure before succeeding."""

    def __init__(self, failures: int = 1) -> None:
        """Initialize the test double."""
        self.failures = failures
        self.calls = 0

    def close(self) -> None:
        """Release the synthetic resource."""
        self.calls += 1
        if self.failures:
            self.failures -= 1
            raise OSError("transient cleanup failure")


@pytest.mark.parametrize(
    ("module_name", "class_name", "value"),
    [
        ("schema_sanitizer.remote_impl.transport", "_BudgetedBytes", b"value"),
        ("schema_sanitizer.remote_impl.transport", "_BudgetedText", "value"),
        ("schema_sanitizer.remote_impl.sync_http", "_BudgetedBytes", b"value"),
        (
            "schema_sanitizer.remote_impl.upload_policy",
            "_BudgetedUploadBytes",
            b"value",
        ),
    ],
)
def test_budgeted_payload_close_retains_failed_lease(
    native_import_stub: None,
    module_name: str,
    class_name: str,
    value: bytes | str,
) -> None:
    """Payload wrappers retain ownership until the underlying release succeeds."""
    import importlib

    cls = getattr(importlib.import_module(module_name), class_name)
    lease = _RetryingClose()
    payload = cls(value, lease)

    with pytest.raises(OSError, match="transient cleanup"):
        payload.close()
    assert getattr(payload, "_operation_memory_lease") is lease

    payload.close()
    assert getattr(payload, "_operation_memory_lease") is None
    assert lease.calls == 2


def test_resource_lifecycle_retains_failed_shared_attributes() -> None:
    """Generic wrappers do not erase the only retry handle after close failure."""
    from schema_sanitizer.core_impl.resource_lifecycle import _close_and_clear_attrs

    resource = _RetryingClose()

    class Owner:
        """Test double used by pass23 hardening regressions."""

        pass

    owner = Owner()
    owner._reader = resource
    owner._raw = resource

    _close_and_clear_attrs(owner, "_reader", "_raw")
    assert owner._reader is resource
    assert owner._raw is resource
    assert resource.calls == 1

    _close_and_clear_attrs(owner, "_reader", "_raw")
    assert owner._reader is None
    assert owner._raw is None
    assert resource.calls == 2


def test_resource_lifecycle_retains_failed_keepalive_and_owner() -> None:
    """Keepalive and resource-owner attributes remain available for retry."""
    from schema_sanitizer.core_impl.resource_lifecycle import (
        _close_keepalive_attr,
        _close_resource_owner_attr,
    )

    class Owner:
        """Test double used by pass23 hardening regressions."""

        pass

    owner = Owner()
    owner._keepalive = _RetryingClose()
    owner._resource_owner = _RetryingClose()

    _close_keepalive_attr(owner)
    _close_resource_owner_attr(owner)
    assert hasattr(owner, "_keepalive")
    assert hasattr(owner, "_resource_owner")

    _close_keepalive_attr(owner)
    _close_resource_owner_attr(owner)
    assert not hasattr(owner, "_keepalive")
    assert not hasattr(owner, "_resource_owner")


def test_chained_keepalive_commits_pop_after_close() -> None:
    """A failed LIFO close keeps the resource at the head for a later retry."""
    from schema_sanitizer.input_impl.prepared import ChainedKeepalive

    resource = _RetryingClose()
    keepalive = ChainedKeepalive(resource)

    with pytest.raises(OSError, match="transient cleanup"):
        keepalive.close()
    assert keepalive._items == [resource]

    keepalive.close()
    assert keepalive._items == []


def test_deferred_memory_close_records_advisory_once() -> None:
    """The last live result completes close telemetry exactly once."""
    native = _NativeOutstandingLedger(31)
    cross = _DeferredCrossProcessOwner()
    ledger = _outstanding_ledger_for_test(native, cross)
    recorded: list[int] = []
    ledger._record_close_advisory = recorded.append

    ledger.close()
    assert recorded == []
    ledger.release(31)
    assert recorded == [31]
    assert ledger.diagnostics().close_outstanding_bytes == 0

    ledger.close()
    assert recorded == [31]


def _small_directory_budget(limit_bytes: int) -> Any:
    """Construct a metadata budget with one exact test ceiling."""
    from schema_sanitizer.input_impl.directory_inputs import DirectoryMetadataBudget

    budget = object.__new__(DirectoryMetadataBudget)
    budget.limit_bytes = limit_bytes
    budget._operation_memory_ledger = None
    budget._used_bytes = 0
    budget._lock = threading.Lock()
    budget._close_condition = threading.Condition(budget._lock)
    budget._close_started = False
    budget._closing = False
    budget._closed = False
    return budget


def test_directory_builder_stops_infinite_duplicate_uri_source() -> None:
    """Matching-key expansion stops once every requested directory is known."""
    from schema_sanitizer.input_impl.directory_inputs import DirectoryDiscoveryBuilder

    builder = DirectoryDiscoveryBuilder[str]({"root": False}, {"root": []})
    yielded = 0

    def uris() -> Any:
        """Yield an intentionally unbounded test sequence."""
        nonlocal yielded
        while True:
            yielded += 1
            if yielded > 2:
                raise AssertionError("duplicate URI source was consumed indefinitely")
            yield "root"

    builder.add(uris(), "file")
    assert yielded == 1
    assert builder.files_by_uri == {"root": ["file"]}


def test_directory_extend_bounds_files_before_retention() -> None:
    """Bulk association rejects an endless file source before list growth."""
    from schema_sanitizer.input_impl.directory_inputs import DirectoryDiscoveryBuilder

    budget = _small_directory_budget(64)
    builder = DirectoryDiscoveryBuilder[str]({"root": False}, {"root": []}, metadata_budget=budget)
    yielded = 0

    def files() -> Any:
        """Yield an intentionally unbounded test sequence."""
        nonlocal yielded
        while True:
            yielded += 1
            if yielded > 5:
                raise AssertionError("file source was materialized past its budget")
            yield f"file-{yielded}"

    with pytest.raises(Exception, match="directory_metadata"):
        builder.extend(["root"], files())
    assert yielded == 5
    assert builder.files_by_uri == {"root": []}
    assert budget.used_bytes == 0


def _temporary_pool_for_pass23(limit: int = 1 << 20) -> Any:
    """Construct a focused temporary pool without loading native options."""
    from schema_sanitizer.core_impl.temporary_storage import TemporaryStoragePermitPool

    pool = object.__new__(TemporaryStoragePermitPool)
    pool.limit_bytes = limit
    pool._lock = threading.Lock()
    pool._condition = threading.Condition(pool._lock)
    pool._reserved_bytes = 0
    pool._pending_reserved_bytes = 0
    pool._pending_active_leases = 0
    pool._peak_reserved_bytes = 0
    pool._active_leases = 0
    pool._closed = False
    pool._close_complete = False
    pool._close_outstanding_bytes = 0
    pool._close_active_leases = 0
    pool._over_release_count = 0
    pool._over_release_bytes = 0
    return pool


def test_temporary_pool_admission_isolated_across_devices(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A blocked device admission does not hold the operation-local pool lock."""
    from schema_sanitizer.core_impl import temporary_storage as module

    entered = threading.Event()
    unblock = threading.Event()

    class Governor:
        """Test double used by pass23 hardening regressions."""

        @staticmethod
        def filesystem(path: object) -> tuple[int, Path, int]:
            """Return synthetic filesystem metadata."""
            device = 1 if str(path).endswith("a") else 2
            return device, Path(str(path)), 1 << 30

        @staticmethod
        def reserve(
            _amount: int,
            *,
            path: object,
            label: str,
            inode_count: int,
        ) -> int:
            """Reserve the synthetic resource."""
            del label, inode_count
            device = 1 if str(path).endswith("a") else 2
            if device == 1:
                entered.set()
                assert unblock.wait(timeout=2.0)
            return device

        @staticmethod
        def release(*_args: object, **_kwargs: object) -> None:
            """Release the synthetic resource."""
            return None

    monkeypatch.setattr(module, "_PROCESS_TEMPORARY_STORAGE", Governor())
    pool = _temporary_pool_for_pass23()
    acquired: list[Any] = []
    worker = threading.Thread(target=lambda: acquired.append(pool.acquire(1, label="a", path="a")))
    worker.start()
    assert entered.wait(timeout=1.0)

    independent = pool.acquire(1, label="b", path="b")
    assert independent.reserved_bytes == 1
    unblock.set()
    worker.join(timeout=2.0)
    assert not worker.is_alive()
    assert len(acquired) == 1

    independent.release()
    acquired[0].release()
    assert pool.snapshot().reserved_bytes == 0


def test_temporary_pool_close_waits_for_started_admission(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Close blocks new work but accounts an admission already in progress."""
    from schema_sanitizer.core_impl import temporary_storage as module

    entered = threading.Event()
    unblock = threading.Event()

    class Governor:
        """Test double used by pass23 hardening regressions."""

        @staticmethod
        def filesystem(_path: object) -> tuple[int, Path, int]:
            """Return synthetic filesystem metadata."""
            return 1, tmp_path, 1 << 30

        @staticmethod
        def reserve(*_args: object, **_kwargs: object) -> int:
            """Reserve the synthetic resource."""
            entered.set()
            assert unblock.wait(timeout=2.0)
            return 1

        @staticmethod
        def release(*_args: object, **_kwargs: object) -> None:
            """Release the synthetic resource."""
            return None

    monkeypatch.setattr(module, "_PROCESS_TEMPORARY_STORAGE", Governor())
    pool = _temporary_pool_for_pass23()
    leases: list[Any] = []
    acquire_thread = threading.Thread(
        target=lambda: leases.append(pool.acquire(7, label="pending", path=tmp_path))
    )
    acquire_thread.start()
    assert entered.wait(timeout=1.0)

    closed = threading.Event()
    close_thread = threading.Thread(target=lambda: (pool.close(), closed.set()))
    close_thread.start()
    assert not closed.wait(timeout=0.05)
    with pytest.raises(RuntimeError, match="closed"):
        pool.acquire(1, label="late", path=tmp_path)

    unblock.set()
    acquire_thread.join(timeout=2.0)
    close_thread.join(timeout=2.0)
    assert closed.is_set()
    assert pool.diagnostics().close_outstanding_bytes == 7
    assert pool.diagnostics().close_active_leases == 1
    leases[0].release()


def test_sync_directory_session_retains_only_compact_provider_plan() -> None:
    """A session must not duplicate or retain the complete discovery packet."""
    import gc
    import weakref

    from schema_sanitizer.remote_impl.sync_backend import SyncDirectoryDownloadSession

    class FileRef:
        """Test double used by pass23 hardening regressions."""

        def __init__(self, uri: str) -> None:
            """Initialize the test double."""
            self.uri = uri

    files = [FileRef(f"s3://bucket/path/{index}.csv") for index in range(512)]
    references = [weakref.ref(file) for file in files]
    session = SyncDirectoryDownloadSession(files, memory_limit_bytes=1 << 20)

    del files
    gc.collect()

    assert all(reference() is None for reference in references)
    assert session._first_uri == "s3://bucket/path/0.csv"
    assert session._provider == "s3"
    assert session._homogeneous_provider is True


def test_source_manifest_avoids_three_full_reference_snapshots() -> None:
    """Manifest construction and date summaries avoid redundant full tuples."""
    import inspect

    from schema_sanitizer.input_impl import source_manifest

    source = inspect.getsource(source_manifest.SourceManifest)
    assert "tuple(sorted(tuple(files)" not in source
    assert "ordered_files = list(files)" in source
    assert "ordered_files.sort(" in source
    assert "updates = tuple(" not in source
