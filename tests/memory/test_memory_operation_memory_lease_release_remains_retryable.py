"""Combines retryable directory-metadata close with remote owners, temporary-device pools,
process checks, shutdown-safe finalizers, keepalive chains, duplicate-URI defense,
compact session plans, and source manifests. Owners are constructed before capacity
commits, failed attributes stay rooted, device admissions remain isolated, and bounded
manifests avoid full reference snapshots."""

from __future__ import annotations

import asyncio
import os
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


class _RetryingMemoryOwner:
    """Minimal exact memory lease with injectable resize/close failures."""

    def __init__(self, failures: int = 0) -> None:
        """Initialize the retrying memory owner test double."""
        self.failures = failures
        self.reserved = 0
        self.resize_calls: list[int] = []

    def resize(self, amount: int) -> None:
        """Resize the resource represented by the retrying memory owner test double."""
        self.resize_calls.append(amount)
        if self.failures:
            self.failures -= 1
            raise OSError("native release unavailable")
        self.reserved = amount

    def close(self) -> None:
        """Close the resources owned by the retrying memory owner test double."""
        self.resize(0)


def _directory_budget_for_test(owner: _RetryingMemoryOwner, used: int) -> Any:
    """Construct a focused directory budget without loading native options."""
    from schema_sanitizer.input_impl.directory_inputs import DirectoryMetadataBudget

    budget = object.__new__(DirectoryMetadataBudget)
    budget.limit_bytes = 1 << 20
    budget._operation_memory_ledger = owner
    budget._memory_lease = owner
    budget._used_bytes = used
    budget._admission_lock = threading.Lock()
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


def test_directory_uri_materialization_is_budget_bounded() -> None:
    """An oversized or infinite source is rejected before full materialization."""
    from schema_sanitizer.input_impl.directory_inputs import DirectoryMetadataBudget

    budget = object.__new__(DirectoryMetadataBudget)
    budget.limit_bytes = 600
    budget._operation_memory_ledger = None
    budget._memory_lease = None
    budget._used_bytes = 0
    budget._admission_lock = threading.Lock()
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


def test_directory_budget_reexport() -> None:
    """The public input package exposes the canonical directory budget."""
    from schema_sanitizer.input_impl import directory_inputs
    from schema_sanitizer.input_impl.directory_metadata_budget import (
        DirectoryMetadataBudget,
    )

    assert directory_inputs.DirectoryMetadataBudget is DirectoryMetadataBudget


@pytest.mark.parametrize("method", ["acquire", "try_acquire_up_to"])
def test_process_resource_owner_is_built_before_capacity_commit(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
) -> None:
    """Owner allocation failure cannot strand logical process capacity."""
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(4, "operation-memory-lease-release-remains-retryable")

    class BrokenLease:
        """Test double used by operation-memory-lease-release-remains-retryable hardening regressions."""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            """Initialize the broken lease test double."""
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
        """Test double used by operation-memory-lease-release-remains-retryable hardening regressions."""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            """Initialize the broken reservation test double."""
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
        """Test double used by operation-memory-lease-release-remains-retryable hardening regressions."""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            """Initialize the broken registration test double."""
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
        """Test double used by operation-memory-lease-release-remains-retryable hardening regressions."""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            """Initialize the broken permit test double."""
            raise MemoryError("permit owner failed")

    monkeypatch.setattr(module, "RemoteIoPermit", BrokenPermit)

    async def exercise() -> None:
        """Exercise the synthetic regression path."""
        with pytest.raises(MemoryError, match="permit owner"):
            await governor.acquire(
                2, operation_id="operation-memory-lease-release-remains-retryable"
            )

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

    original_reconcile = governor._reconcile_state_authority_locked

    def reconcile(device: int, state: object) -> bool:
        """Pause one device after its registry lookup but before admission."""
        if device == 1:
            entered.set()
            assert unblock.wait(timeout=2.0)
        return original_reconcile(device, state)

    monkeypatch.setattr(governor, "filesystem", filesystem)
    monkeypatch.setattr(governor, "free_inodes", lambda _path: 1 << 20)
    monkeypatch.setattr(module, "cross_process_storage_enabled", lambda: False)
    monkeypatch.setattr(governor, "_reconcile_state_authority_locked", reconcile)

    first: list[object] = []
    worker = threading.Thread(
        target=lambda: first.append(
            governor.reserve_capability(1, path="a", label="blocked-device")
        )
    )
    worker.start()
    assert entered.wait(timeout=1.0)

    second = governor.reserve_capability(1, path="b", label="independent-device")
    completed.append(second.device)
    unblock.set()
    worker.join(timeout=2.0)
    assert not worker.is_alive()
    assert completed == [2]
    assert len(first) == 1

    assert governor.release_capability(first[0])
    assert governor.release_capability(second)
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

    capability = governor.reserve_capability(1, path=tmp_path, label="first")
    assert capability.device == 7
    state = governor._borrow_state(7)
    assert state is not None
    assert governor.release_capability(capability)
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

    first = governor.reserve_capability(1, path=tmp_path, label="first")
    second = governor.reserve_capability(1, path=tmp_path, label="second")
    assert calls == 1
    assert governor.release_capability(first)
    assert governor.release_capability(second)


def test_runs_in_owner_process() -> None:
    """Keep PID-sensitive regressions explicit when run under unusual harnesses."""
    assert os.getpid() > 0


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


def test_temporary_lease_finalizer_skips_interpreter_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A temporary finalizer cannot block on a host-wide journal at shutdown."""
    from schema_sanitizer.core_impl import temporary_storage as module

    pool = module.TemporaryStoragePermitPool(1024)
    lease = pool.acquire(
        1,
        label="operation-memory-lease-release-remains-retryable",
        path=tmp_path,
    )
    original_release = pool._release_lease
    calls = 0

    def release(active_lease: object) -> None:
        """Release the resource at the synchronization point under test."""
        nonlocal calls
        calls += 1
        original_release(active_lease)

    monkeypatch.setattr(pool, "_release_lease", release)
    monkeypatch.setattr(module, "runtime_is_finalizing", lambda: True)
    lease.__del__()
    assert calls == 0
    monkeypatch.setattr(module, "runtime_is_finalizing", lambda: False)
    lease.release()
    assert calls == 1


class _RetryingClose:
    """Inject one cleanup failure before succeeding."""

    def __init__(self, failures: int = 1) -> None:
        """Initialize the retrying close test double."""
        self.failures = failures
        self.calls = 0

    def close(self) -> None:
        """Close the resources owned by the retrying close test double."""
        self.calls += 1
        if self.failures:
            self.failures -= 1
            raise OSError("transient cleanup failure")


def test_resource_lifecycle_retains_failed_shared_attributes() -> None:
    """Generic wrappers do not erase the only retry handle after close failure."""
    from schema_sanitizer.core_impl.resource_lifecycle import _close_and_clear_attrs

    resource = _RetryingClose()

    class Owner:
        """Test double used by operation-memory-lease-release-remains-retryable hardening regressions."""

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
        """Test double used by operation-memory-lease-release-remains-retryable hardening regressions."""

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


def _small_directory_budget(limit_bytes: int) -> Any:
    """Construct a metadata budget with one exact test ceiling."""
    from schema_sanitizer.input_impl.directory_inputs import DirectoryMetadataBudget

    budget = object.__new__(DirectoryMetadataBudget)
    budget.limit_bytes = limit_bytes
    budget._operation_memory_ledger = None
    budget._memory_lease = None
    budget._used_bytes = 0
    budget._admission_lock = threading.Lock()
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


def _temporary_pool_for_operation_lease(limit: int = 1 << 20) -> Any:
    """Construct a focused temporary pool without loading native options."""
    from schema_sanitizer.core_impl.temporary_storage import TemporaryStoragePermitPool

    pool = TemporaryStoragePermitPool(limit)
    pool.limit_bytes = limit
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
        """Test double used by operation-memory-lease-release-remains-retryable hardening regressions."""

        @staticmethod
        def filesystem(path: object) -> tuple[int, Path, int]:
            """Map paths ending in 'a' to device one and all others to device two."""
            device = 1 if str(path).endswith("a") else 2
            return device, Path(str(path)), 1 << 30

        def reserve_capability(
            self,
            amount: int,
            *,
            path: object,
            label: str,
            inode_count: int,
        ) -> object:
            """Block device-one admission, then return its storage capability."""
            del label
            device = 1 if str(path).endswith("a") else 2
            if device == 1:
                entered.set()
                assert unblock.wait(timeout=2.0)
            return SimpleNamespace(
                device=device,
                reserved_bytes=amount,
                reserved_inodes=inode_count,
                active=True,
            )

        def release_capability(self, capability: Any) -> bool:
            """Release the cleanup capability and record the call."""
            capability.active = False
            return True

    monkeypatch.setattr(module, "_PROCESS_TEMPORARY_STORAGE", Governor())
    pool = _temporary_pool_for_operation_lease()
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
        """Test double used by operation-memory-lease-release-remains-retryable hardening regressions."""

        @staticmethod
        def filesystem(_path: object) -> tuple[int, Path, int]:
            """Return the controlled filesystem-capacity sample."""
            return 1, tmp_path, 1 << 30

        @staticmethod
        def reserve_capability(amount: int, **kwargs: object) -> object:
            """Signal reservation start, wait for release, and return its capability."""
            entered.set()
            assert unblock.wait(timeout=2.0)
            return SimpleNamespace(
                device=1,
                reserved_bytes=amount,
                reserved_inodes=kwargs.get("inode_count", 0),
                active=True,
            )

        def release_capability(self, capability: Any) -> bool:
            """Release the cleanup capability and record the call."""
            capability.active = False
            return True

    monkeypatch.setattr(module, "_PROCESS_TEMPORARY_STORAGE", Governor())
    pool = _temporary_pool_for_operation_lease()
    close_wait_entered = threading.Event()

    class TrackingCondition(threading.Condition):
        def wait(self, timeout: float | None = None) -> bool:
            """Wait for the tracking condition test double to reach its terminal state."""
            close_wait_entered.set()
            return super().wait(timeout)

    pool._condition = TrackingCondition(pool._lock)
    leases: list[Any] = []
    acquire_thread = threading.Thread(
        target=lambda: leases.append(pool.acquire(7, label="pending", path=tmp_path))
    )
    acquire_thread.start()
    assert entered.wait(timeout=1.0)

    closed = threading.Event()
    close_thread = threading.Thread(target=lambda: (pool.close(), closed.set()))
    close_thread.start()
    assert close_wait_entered.wait(timeout=2.0)
    with pool._condition:
        assert pool._closed
        assert pool._pending_active_leases == 1
        assert not pool._close_complete
    assert not closed.is_set()
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
        """Test double used by operation-memory-lease-release-remains-retryable hardening regressions."""

        def __init__(self, uri: str) -> None:
            """Initialize the file ref test double."""
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

    from schema_sanitizer.sources import models

    source = inspect.getsource(models.SourceManifest)
    assert "tuple(sorted(tuple(files)" not in source
    assert "ordered_files = list(files)" in source
    assert "ordered_files.sort(" in source
    assert "updates = tuple(" not in source
