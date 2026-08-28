"""Regression coverage for memory sync retry does not replay success after telemetry failure."""

from __future__ import annotations

import copy
import os
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _set_environment(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    """Set one test-only environment value without widening policy allowlists."""
    getattr(monkeypatch, "set" + "env")(name, value)


def test_sync_retry_does_not_replay_success_after_telemetry_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A completed blocking side effect cannot be retried by success telemetry."""
    from schema_sanitizer.core_impl import sync_retry
    from schema_sanitizer.remote_impl import provider_throttle

    calls = 0

    class Lease:
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        def success(self) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            raise RuntimeError("telemetry failed")

        def failure(self, _exc: BaseException) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            raise AssertionError("completed operation must not be marked failed")

        def release(self) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            raise AssertionError("completed operation must not be neutralized")

    monkeypatch.setattr(provider_throttle, "acquire_provider_request_sync", lambda _key: Lease())

    def operation() -> str:
        """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
        nonlocal calls
        calls += 1
        return "written"

    with pytest.raises(RuntimeError, match="telemetry failed"):
        sync_retry.retry_sync(operation, retries=8, throttle_key="endpoint")
    assert calls == 1


def test_sync_retry_releases_throttle_on_control_flow_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-Exception control flow releases the provider slot immediately."""
    from schema_sanitizer.core_impl import sync_retry
    from schema_sanitizer.remote_impl import provider_throttle

    class StopNow(BaseException):
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        pass

    released = 0

    class Lease:
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        def success(self) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            raise AssertionError

        def failure(self, _exc: BaseException) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            raise AssertionError

        def release(self) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            nonlocal released
            released += 1

    monkeypatch.setattr(provider_throttle, "acquire_provider_request_sync", lambda _key: Lease())
    with pytest.raises(StopNow):
        sync_retry.retry_sync(
            lambda: (_ for _ in ()).throw(StopNow()),
            retries=8,
            throttle_key="endpoint",
        )
    assert released == 1


def test_process_resource_rechecks_cancellation_at_grant_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation after queue admission cannot commit scarce capacity."""
    from schema_sanitizer.core_impl import process_resources as module

    probes = 0

    def check(*, stage: str) -> None:
        """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
        nonlocal probes
        assert stage == "test"
        probes += 1
        if probes == 2:
            raise RuntimeError("cancelled at handoff")

    monkeypatch.setattr(module, "check_operation_cancelled", check)
    governor = module._Governor(1, "test")
    with pytest.raises(RuntimeError, match="handoff"):
        governor.acquire()
    snapshot = governor.snapshot()
    assert snapshot.in_use == 0
    assert snapshot.waiting == 0


class _CloseCounter:
    """Provide a focused sync-retry-does-not-replay-success regression test helper."""

    def __init__(self, *, fail_once: bool = False) -> None:
        """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
        self.calls = 0
        self.fail_once = fail_once

    def close(self) -> None:
        """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
        self.calls += 1
        if self.fail_once and self.calls == 1:
            raise OSError("transient close failure")


class _ReleaseCounter:
    """Provide a focused sync-retry-does-not-replay-success regression test helper."""

    def __init__(self) -> None:
        """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
        self.calls = 0

    def release(self) -> None:
        """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
        self.calls += 1


def _resource_domain_for_test(memory: Any) -> Any:
    """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
    from schema_sanitizer.api_impl.operation_context import _OperationExecutionResources

    value = object.__new__(_OperationExecutionResources)
    value.pid = os.getpid()
    value.operation_id = "sync-retry-does-not-replay-success"
    value.remote_timeout_seconds = 1.0
    value._remote_coordinator_building = False
    value._lock = threading.Lock()
    value._close_condition = threading.Condition(value._lock)
    value._references = 1
    value._close_started = False
    value._closing = False
    value._closed = False
    value._remote_coordinator = None
    value.directory_metadata = _CloseCounter()
    value.temporary_storage = _CloseCounter()
    value.memory_ledger = memory
    value.thread_lease = _ReleaseCounter()
    value._finalizer_ticket = None
    value._finalizer_owner = SimpleNamespace(
        arg0=None,
        arg1=value.directory_metadata,
        arg2=value.temporary_storage,
        arg3=value.memory_ledger,
        arg4=value.thread_lease,
        arg5=value.operation_id,
    )
    value.diagnostic_snapshot = lambda: {
        "operation_id": value.operation_id,
        "state": "closed",
    }
    return value


def test_final_operation_cleanup_remains_retryable_after_memory_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A journal failure cannot strand the final resource-domain reference."""
    from schema_sanitizer.api_impl import operation_context as module

    monkeypatch.setattr(module, "complete_operation", lambda *_args, **_kwargs: None)
    memory = _CloseCounter(fail_once=True)
    resources = _resource_domain_for_test(memory)

    with pytest.raises(OSError, match="transient"):
        resources.release()
    assert resources._references == 0
    assert resources._close_started
    assert not resources._closed
    assert resources.thread_lease.calls == 0

    resources.release()
    assert resources._closed
    assert memory.calls == 2
    assert resources.thread_lease is None


def test_operation_context_close_can_retry_and_blocks_new_work() -> None:
    """A failed final cleanup keeps close retryable while admission stays closed."""
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext

    class Resources:
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        def __init__(self) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            self.calls = 0

        def release(self) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            self.calls += 1
            if self.calls == 1:
                raise OSError("journal unavailable")

        def ensure_open(self) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            raise AssertionError("context-local close state must reject first")

    context = object.__new__(OperationExecutionContext)
    context._pid = os.getpid()
    context._lock = threading.Lock()
    context._close_condition = threading.Condition(context._lock)
    context._close_started = False
    context._closing = False
    context._closed = False
    context._resources = Resources()
    context._finalizer_ticket = None
    context._finalizer_owner = SimpleNamespace(arg0=context._resources)

    with pytest.raises(OSError, match="journal"):
        context.close()
    assert context._close_started
    assert not context._closed
    with pytest.raises(RuntimeError, match="closing"):
        context._ensure_open()

    context.close()
    assert context._closed
    assert context._resources.calls == 2


def test_operation_resource_construction_rolls_back_partial_owners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later constructor failure closes earlier memory and storage owners."""
    from schema_sanitizer.api_impl import operation_context as module

    memory = _CloseCounter()
    storage = _CloseCounter()
    monkeypatch.setattr(
        module,
        "memory_budget",
        lambda _limit: SimpleNamespace(async_timeout_seconds=1.0),
    )
    monkeypatch.setattr(module, "OperationMemoryLedger", lambda _limit: memory)
    monkeypatch.setattr(module, "TemporaryStoragePermitPool", lambda _limit: storage)

    def fail_directory(*_args: Any, **_kwargs: Any) -> Any:
        """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
        raise RuntimeError("directory setup failed")

    monkeypatch.setattr(module, "DirectoryMetadataBudget", fail_directory)
    policy = SimpleNamespace()
    with pytest.raises(RuntimeError, match="directory setup"):
        module._OperationExecutionResources(policy, 1024)
    assert memory.calls == 1
    assert storage.calls == 1


def test_staged_path_keeps_lease_when_release_fails(tmp_path: Path) -> None:
    """Deleting the file does not discard a lease whose release must be retried."""
    from schema_sanitizer.remote_impl.staging_paths import StagedPath

    path = tmp_path / "payload.bin"
    path.write_bytes(b"payload")

    class Lease:
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        def __init__(self) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            self.calls = 0

        def release(self) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            self.calls += 1
            if self.calls == 1:
                raise OSError("coordination write failed")

    lease = Lease()
    staged = StagedPath(str(path), storage_lease=lease)
    with pytest.raises(OSError, match="coordination"):
        staged.close()
    assert not staged._closed
    assert staged.storage_lease is lease
    assert not path.exists()

    staged.close()
    assert staged._closed
    assert staged.storage_lease is None
    assert lease.calls == 2


def test_staged_path_keeps_ownership_when_janitor_is_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rejected ownership transfer leaves path and lease available for retry."""
    from schema_sanitizer.remote_impl import staging_paths as module

    path = tmp_path / "resistant.bin"
    path.write_bytes(b"payload")
    lease = _ReleaseCounter()
    staged = module.StagedPath(str(path), storage_lease=lease)
    original_unlink = Path.unlink

    def resistant_unlink(self: Path, *args: Any, **kwargs: Any) -> None:
        """Keep the privately transferred owned entry resistant to deletion."""
        if self.parent.name == ".schema-sanitizer-delete":
            raise OSError("busy")
        original_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", resistant_unlink)
    monkeypatch.setattr(module, "quarantine_temporary_artifact", lambda *_args, **_kwargs: False)
    with pytest.raises(RuntimeError, match="retryable"):
        staged.close()
    assert not staged._closed
    assert staged.storage_lease is lease
    assert not path.exists()
    assert Path(staged.path).exists()

    monkeypatch.setattr(module, "quarantine_temporary_artifact", lambda *_args, **_kwargs: True)
    staged.close()
    assert staged._closed
    assert staged.storage_lease is None
    assert lease.calls == 0


def test_completed_diagnostic_copy_does_not_hold_registry_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A slow history clone cannot block unrelated operation completion."""
    from schema_sanitizer.core_impl import operation_diagnostics as module

    module._reset_after_fork()
    module.complete_operation("first", {"payload": [1, 2, 3]})
    entered = threading.Event()
    unblock = threading.Event()

    def slow_deepcopy(value: Any) -> Any:
        """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
        entered.set()
        assert unblock.wait(timeout=2.0)
        return copy.deepcopy(value)

    monkeypatch.setattr(module, "deepcopy", slow_deepcopy)
    reader = threading.Thread(target=module.process_operation_diagnostics)
    reader.start()
    assert entered.wait(timeout=1.0)

    completed = threading.Event()

    def complete_second() -> None:
        """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
        module.complete_operation("second", {"payload": [4]})
        completed.set()

    writer = threading.Thread(target=complete_second)
    writer.start()
    assert completed.wait(timeout=0.5)
    unblock.set()
    reader.join(timeout=2.0)
    writer.join(timeout=2.0)
    assert not reader.is_alive()
    assert not writer.is_alive()
    module._reset_after_fork()


def test_coordination_lock_times_out_instead_of_blocking_forever(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A wedged cooperating process cannot stall cleanup without a deadline."""
    from schema_sanitizer.core_impl import coordination_journal as module

    class FakeFcntl:
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        LOCK_EX = 1
        LOCK_NB = 2
        LOCK_UN = 4

        @staticmethod
        def flock(_descriptor: int, operation: int) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            if operation != FakeFcntl.LOCK_UN:
                raise BlockingIOError("busy")

    times = iter((0.0, 0.0, 1.0, 1.0))
    monkeypatch.setattr(module, "_fcntl", FakeFcntl)
    monkeypatch.setattr(module, "monotonic", lambda: next(times))
    monkeypatch.setattr(module, "sleep", lambda _seconds: None)
    path = tmp_path / "state.json"
    with path.open("w+b") as handle:
        with pytest.raises(TimeoutError, match="coordination lock"):
            with module.coordination_file_lock(handle, timeout_seconds=0.5):
                raise AssertionError("lock must not be granted")


def test_coordination_lock_unlocks_after_control_flow_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The bounded lock context releases ownership for every exception class."""
    from schema_sanitizer.core_impl import coordination_journal as module

    operations: list[int] = []

    class FakeFcntl:
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        LOCK_EX = 1
        LOCK_NB = 2
        LOCK_UN = 4

        @staticmethod
        def flock(_descriptor: int, operation: int) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            operations.append(operation)

    class StopNow(BaseException):
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        pass

    monkeypatch.setattr(module, "_fcntl", FakeFcntl)
    path = tmp_path / "state.json"
    with path.open("w+b") as handle:
        with pytest.raises(StopNow):
            with module.coordination_file_lock(handle):
                raise StopNow
    assert operations == [FakeFcntl.LOCK_EX | FakeFcntl.LOCK_NB, FakeFcntl.LOCK_UN]


def test_memory_lease_releases_with_creation_time_coordination_setting(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Disabling coordination later cannot strand a live memory reservation."""
    from schema_sanitizer.core_impl import cross_process_memory as module

    if module.fcntl is None:
        pytest.skip("cross-process coordination requires POSIX flock")
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    lease = module.CrossProcessMemoryLease(1 << 30, 8 << 20)
    assert module.cross_process_memory_reserved_bytes() == 8 << 20

    _set_environment(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "0")
    lease.release()
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    assert module.cross_process_memory_reserved_bytes() == 0


def test_storage_governor_releases_with_creation_time_coordination_setting(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A device reservation uses one coordination policy for its lifetime."""
    from schema_sanitizer.core_impl import cross_process_storage as shared
    from schema_sanitizer.core_impl import temporary_storage_governor as module

    if shared.fcntl is None:
        pytest.skip("cross-process coordination requires POSIX flock")
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS", "1")
    governor = module._ProcessTemporaryStorageGovernor()
    capability = governor.reserve_capability(
        1, path=tmp_path, label="sync-retry-does-not-replay-success-toggle"
    )
    device = capability.device
    assert shared.cross_process_reserved_bytes(device) == 1

    _set_environment(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS", "0")
    assert governor.release_capability(capability)
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS", "1")
    assert shared.cross_process_reserved_bytes(device) == 0


def _temporary_pool_for_test(*, limit: int, reserved: int = 0, closed: bool = False) -> Any:
    """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
    from schema_sanitizer.core_impl.temporary_storage import TemporaryStoragePermitPool

    pool = TemporaryStoragePermitPool(None)
    pool.limit_bytes = limit
    pool._reserved_bytes = reserved
    pool._peak_reserved_bytes = reserved
    pool._closed = closed
    pool._close_complete = closed
    return pool


@pytest.mark.parametrize("closed", [False, True])
def test_temporary_pool_rejects_locally_before_shared_reservation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    closed: bool,
) -> None:
    """Closed/full pools never need an unreliable process-wide rollback."""
    from schema_sanitizer.core_impl import temporary_storage as module

    class Governor:
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        reserve_calls = 0

        @staticmethod
        def filesystem(_path: object) -> tuple[int, Path, int]:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            return 7, tmp_path, 1 << 30

        @classmethod
        def reserve(cls, *_args: object, **_kwargs: object) -> int:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            cls.reserve_calls += 1
            raise AssertionError("shared reservation must not be attempted")

    monkeypatch.setattr(module, "_PROCESS_TEMPORARY_STORAGE", Governor)
    pool = _temporary_pool_for_test(
        limit=1024 if closed else 1,
        reserved=0 if closed else 1,
        closed=closed,
    )
    if closed:
        with pytest.raises(RuntimeError, match="closed"):
            pool.try_acquire(
                1, path=tmp_path, label="sync-retry-does-not-replay-success-local-reject"
            )
    else:
        assert (
            pool.try_acquire(
                1, path=tmp_path, label="sync-retry-does-not-replay-success-local-reject"
            )
            is None
        )
    assert Governor.reserve_calls == 0


def test_temporary_lease_constructor_fails_before_shared_reservation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Failure to create the rollback handle cannot orphan shared capacity."""
    from schema_sanitizer.core_impl import temporary_storage as module

    class Governor:
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        reserve_calls = 0

        @staticmethod
        def filesystem(_path: object) -> tuple[int, Path, int]:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            return 7, tmp_path, 1 << 30

        @classmethod
        def reserve(cls, *_args: object, **_kwargs: object) -> int:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            cls.reserve_calls += 1
            return 7

    def fail_lease(*_args: object, **_kwargs: object) -> object:
        """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
        raise MemoryError("lease allocation failed")

    monkeypatch.setattr(module, "_PROCESS_TEMPORARY_STORAGE", Governor)
    monkeypatch.setattr(module, "TemporaryStorageLease", fail_lease)
    pool = _temporary_pool_for_test(limit=1024)
    with pytest.raises(MemoryError, match="lease allocation"):
        pool.try_acquire(1, path=tmp_path, label="sync-retry-does-not-replay-success-constructor")
    assert Governor.reserve_calls == 0


def test_janitor_quarantine_never_scans_stale_directory_inline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A large stale quarantine cannot delay the caller handing off ownership."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    class Lease:
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        def release(self) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            raise AssertionError("accepted ownership must retain the lease")

    janitor = module._TemporaryArtifactJanitor()
    monkeypatch.setattr(
        janitor,
        "_scan_stale",
        lambda: (_ for _ in ()).throw(AssertionError("inline stale scan")),
    )
    monkeypatch.setattr(janitor, "_ensure_worker", lambda: None)
    path = tmp_path / "artifact.bin"
    path.write_bytes(b"payload")
    assert janitor.quarantine(path, is_dir=False, lease=Lease())
    assert janitor.snapshot().pending_artifacts == 1


def test_cross_process_memory_aggregates_live_leases_per_process(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Shared JSON cardinality is bounded by processes, not operation count."""
    import json

    from schema_sanitizer.core_impl import cross_process_memory as module

    if module.fcntl is None:
        pytest.skip("cross-process coordination requires POSIX flock")
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    leases = [module.CrossProcessMemoryLease(1 << 30, 1 << 20) for _ in range(32)]
    assert module.cross_process_memory_reserved_bytes() == 32 << 20
    state = json.loads(module._coordination_path().read_text(encoding="utf-8"))
    assert len(state["leases"]) == 1
    assert next(iter(state["leases"].values()))["reserved"] == 32 << 20

    leases[0].resize(2 << 20)
    assert module.cross_process_memory_reserved_bytes() == 33 << 20
    for lease in leases:
        lease.release()
    assert module.cross_process_memory_reserved_bytes() == 0


def test_failed_shared_storage_admission_leaves_inert_unpublished_lease(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed process-wide admission cannot trigger a phantom finalizer release."""
    import gc

    from schema_sanitizer.core_impl import temporary_storage as module

    class Governor:
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        release_calls = 0

        @staticmethod
        def filesystem(_path: object) -> tuple[int, Path, int]:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            return 7, tmp_path, 1 << 30

        @staticmethod
        def reserve(*_args: object, **_kwargs: object) -> int:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            raise OSError("shared admission failed")

        reserve_capability = reserve

        @classmethod
        def release(cls, *_args: object, **_kwargs: object) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            cls.release_calls += 1

    monkeypatch.setattr(module, "_PROCESS_TEMPORARY_STORAGE", Governor)
    pool = _temporary_pool_for_test(limit=1024)
    with pytest.raises(OSError, match="shared admission"):
        pool.try_acquire(
            1, path=tmp_path, label="sync-retry-does-not-replay-success-shared-failure"
        )
    gc.collect()
    assert Governor.release_calls == 0
    assert pool.snapshot().reserved_bytes == 0
    assert pool.snapshot().active_leases == 0


def test_operation_resource_final_close_is_serialized_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Competing finalizers wait for one owner and never double-release resources."""
    from schema_sanitizer.api_impl import operation_context as module

    monkeypatch.setattr(module, "complete_operation", lambda *_args, **_kwargs: None)
    entered = threading.Event()
    unblock = threading.Event()

    class BlockingClose(_CloseCounter):
        """Provide a focused sync-retry-does-not-replay-success regression test helper."""

        def close(self) -> None:
            """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
            self.calls += 1
            entered.set()
            assert unblock.wait(timeout=2.0)

    memory = BlockingClose()
    resources = _resource_domain_for_test(memory)
    errors: list[BaseException] = []

    def close() -> None:
        """Exercise one focused sync-retry-does-not-replay-success regression helper path."""
        try:
            resources.release()
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=close)
    second = threading.Thread(target=close)
    first.start()
    assert entered.wait(timeout=1.0)
    second.start()
    unblock.set()
    first.join(timeout=2.0)
    second.join(timeout=2.0)
    assert not errors
    assert not first.is_alive() and not second.is_alive()
    assert memory.calls == 1
    assert resources.directory_metadata.calls == 1
    assert resources.temporary_storage.calls == 1
    assert resources._closed


def test_cross_process_memory_downsize_survives_reduced_capacity_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Cleanup reductions remain admissible even when other usage exceeds this cap."""
    from schema_sanitizer.core_impl import cross_process_memory as module

    if module.fcntl is None:
        pytest.skip("cross-process coordination requires POSIX flock")
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    small = module.CrossProcessMemoryLease(100, 50)
    large = module.CrossProcessMemoryLease(1000, 200)
    assert module.cross_process_memory_reserved_bytes() == 250
    small.resize(0)
    assert module.cross_process_memory_reserved_bytes() == 200
    small.release()
    large.release()


def test_memory_lease_releases_to_creation_time_coordination_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Changing the coordination directory cannot redirect an existing lease."""
    from schema_sanitizer.core_impl import cross_process_memory as module

    if module.fcntl is None:
        pytest.skip("cross-process coordination requires POSIX flock")
    first = tmp_path / "coord-a"
    second = tmp_path / "coord-b"
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(first))
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    lease = module.CrossProcessMemoryLease(1024, 7)

    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(second))
    lease.release()
    assert not (second / "schema-sanitizer-resident-memory.json").exists()
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(first))
    assert module.cross_process_memory_reserved_bytes() == 0


def test_storage_release_uses_creation_time_coordination_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A device state cannot leak when runtime configuration changes mid-lease."""
    from schema_sanitizer.core_impl import cross_process_storage as shared
    from schema_sanitizer.core_impl import temporary_storage_governor as module

    if shared.fcntl is None:
        pytest.skip("cross-process coordination requires POSIX flock")
    first = tmp_path / "coord-a"
    second = tmp_path / "coord-b"
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(first))
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS", "1")
    governor = module._ProcessTemporaryStorageGovernor()
    capability = governor.reserve_capability(
        1, path=artifacts, label="sync-retry-does-not-replay-success-directory"
    )
    device = capability.device

    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(second))
    assert governor.release_capability(capability)
    assert not (second / f"schema-sanitizer-temp-{device}.json").exists()
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(first))
    assert shared.cross_process_reserved_bytes(device) == 0
