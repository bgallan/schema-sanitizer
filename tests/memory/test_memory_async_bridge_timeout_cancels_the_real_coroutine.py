"""Regression coverage for memory async bridge timeout cancels the real coroutine."""

from __future__ import annotations

import asyncio
import gc
import threading
from concurrent.futures import Future
from types import SimpleNamespace
from typing import Any

import pytest


def test_async_bridge_timeout_cancels_the_real_coroutine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller timeout must stop the helper task and return its thread permit."""
    from schema_sanitizer.remote_impl import async_bridge

    monkeypatch.setattr(async_bridge, "bounded_wait_timeout", lambda _default: 0.01)
    finalized = threading.Event()

    async def exercise() -> None:
        async def slow() -> None:
            try:
                await asyncio.sleep(60)
            finally:
                finalized.set()

        with pytest.raises(TimeoutError, match="bounded wait"):
            async_bridge.run_sync(slow(), threading_mode="multi")

    asyncio.run(exercise())
    assert finalized.wait(1.0)


def test_async_bridge_thread_start_failure_closes_coroutine_and_releases_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Construction failures cannot retain an unstarted coroutine or thread slot."""
    from schema_sanitizer.remote_impl import async_bridge

    class Lease:
        amount = 1

        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    def broken_thread(*args: object, **kwargs: object) -> threading.Thread:
        thread = threading.Thread(*args, **kwargs)

        def fail_start() -> None:
            raise RuntimeError("injected thread start failure")

        thread.start = fail_start  # type: ignore[method-assign]
        return thread

    lease = Lease()
    monkeypatch.setattr(async_bridge, "acquire_project_threads", lambda *_a, **_k: lease)
    monkeypatch.setattr(async_bridge, "Thread", broken_thread)

    async def exercise() -> None:
        coroutine = asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="thread start failure"):
            async_bridge.run_sync(coroutine, threading_mode="multi")
        assert coroutine.cr_frame is None

    asyncio.run(exercise())
    assert lease.releases == 1


def test_provider_pool_closed_race_retains_descriptor_until_client_close_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late client keeps its FD reservation until physical close succeeds."""
    from schema_sanitizer.remote_impl import provider_session_pool as module

    class Lease:
        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    class Client:
        fail_close = True

        async def close(self) -> None:
            if self.fail_close:
                raise RuntimeError("injected client close failure")

    lease = Lease()
    monkeypatch.setattr(module, "acquire_file_descriptors", lambda *_a, **_k: lease)

    async def exercise() -> None:
        pool = module.RemoteProviderSessionPool()
        await pool.__aenter__()
        entered = asyncio.Event()
        resume = asyncio.Event()
        client = Client()

        async def factory() -> Client:
            entered.set()
            await resume.wait()
            return client

        borrower = asyncio.create_task(pool.borrow_client(("key",), factory))
        await entered.wait()
        await pool.__aexit__(None, None, None)
        resume.set()
        with pytest.raises(RuntimeError, match="remote provider session pool is closed"):
            await borrower
        assert lease.releases == 0
        assert len(pool._entries) == 1
        client.fail_close = False
        await pool.__aexit__(None, None, None)
        assert len(pool._entries) == 0

    asyncio.run(exercise())
    assert lease.releases == 1


def test_provider_pool_insertion_failure_closes_client_and_releases_descriptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Memory-pressure insertion failure cannot orphan a newly created client."""
    from schema_sanitizer.remote_impl import provider_session_pool as module

    class Lease:
        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    class Client:
        def __init__(self) -> None:
            self.closes = 0

        async def close(self) -> None:
            self.closes += 1

    class RejectingDict(dict[tuple[Any, ...], Any]):
        def __setitem__(self, key: tuple[Any, ...], value: Any) -> None:
            del key, value
            raise MemoryError("injected pool insertion failure")

    lease = Lease()
    client = Client()
    monkeypatch.setattr(module, "acquire_file_descriptors", lambda *_a, **_k: lease)

    async def exercise() -> None:
        pool = module.RemoteProviderSessionPool()
        await pool.__aenter__()
        pool._entries = RejectingDict()
        with pytest.raises(MemoryError, match="insertion failure"):
            await pool.borrow_client(("key",), lambda: asyncio.sleep(0, result=client))
        await pool.__aexit__(None, None, None)

    asyncio.run(exercise())
    assert client.closes == 1
    assert lease.releases == 1


def test_remote_coordinator_thread_start_failure_releases_capacity_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Coordinator construction is transactional around its host-thread start."""
    from schema_sanitizer.remote_impl import io_coordinator as module

    class Registration:
        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    class Governor:
        def __init__(self) -> None:
            self.registration = Registration()

        def register_capacity(self, _requested: int) -> Registration:
            return self.registration

    class BrokenThread:
        ident = None

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def start(self) -> None:
            raise RuntimeError("injected coordinator start failure")

    governor = Governor()
    monkeypatch.setattr(module.threading, "Thread", BrokenThread)
    with pytest.raises(RuntimeError, match="coordinator start failure"):
        module.RemoteIoCoordinator(permit_governor=governor, permit_capacity=2)
    assert governor.registration.releases == 1


def test_remote_coordinator_submission_failure_closes_unscheduled_coroutine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A closed-loop submission failure must close the invoke coroutine immediately."""
    from schema_sanitizer.remote_impl import io_coordinator as module

    captured: dict[str, Any] = {}

    def reject(coroutine: Any, _loop: Any) -> Future[Any]:
        captured["coroutine"] = coroutine
        raise RuntimeError("injected submission failure")

    class Submission:
        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    submission = Submission()
    governor = SimpleNamespace(
        reserve_submission=lambda: submission,
        acquire=lambda *_a, **_k: None,
    )
    coordinator = object.__new__(module.RemoteIoCoordinator)
    coordinator._lock = threading.Lock()
    coordinator._closed = False
    coordinator._loop = SimpleNamespace()
    coordinator._permit_governor = governor
    coordinator._operation_id = "operation"
    coordinator._context = None
    coordinator._futures = set()
    monkeypatch.setattr(module.asyncio, "run_coroutine_threadsafe", reject)

    async def operation(_context: Any) -> None:
        return None

    with pytest.raises(RuntimeError, match="submission failure"):
        coordinator.submit(operation)
    coroutine = captured["coroutine"]
    assert coroutine.cr_frame is None
    assert submission.releases == 1


def test_dead_operation_diagnostic_is_removed_without_a_diagnostics_poll() -> None:
    """Weak registrations clean their own keys instead of accumulating tombstones."""
    from schema_sanitizer.core_impl import operation_diagnostics as module

    module._reset_after_fork()

    class Source:
        def snapshot(self) -> dict[str, object]:
            return {"operation_id": "dead"}

    source = Source()
    module.register_operation("dead", source.snapshot)
    assert "dead" in module._LIVE
    del source
    gc.collect()
    assert "dead" not in module._LIVE


def test_operation_diagnostic_live_registry_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Observability cannot become an unbounded side channel under operation churn."""
    from schema_sanitizer.core_impl import operation_diagnostics as module

    module._reset_after_fork()
    monkeypatch.setattr(module, "_MAX_LIVE", 2)

    class Source:
        def __init__(self, operation_id: str) -> None:
            self.operation_id = operation_id

        def snapshot(self) -> dict[str, object]:
            return {"operation_id": self.operation_id}

    sources = [Source(str(index)) for index in range(3)]
    for source in sources:
        module.register_operation(source.operation_id, source.snapshot)

    assert len(module._LIVE) == 2
    assert module._LIVE_REGISTRATION_REJECTIONS == 1


def test_temporary_storage_release_remains_retryable_after_process_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """A failed host-wide release cannot commit the operation-local lease first."""
    from schema_sanitizer.core_impl import temporary_storage as module

    monkeypatch.setattr(
        module, "memory_budget", lambda _limit: SimpleNamespace(replay_spool_bytes=1024)
    )
    pool = module.TemporaryStoragePermitPool(None)
    lease = pool.acquire(100, label="retryable-release", path=tmp_path)
    real_release = module._PROCESS_TEMPORARY_STORAGE.release_capability

    def fail_release(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected process release failure")

    monkeypatch.setattr(module._PROCESS_TEMPORARY_STORAGE, "release_capability", fail_release)
    with pytest.raises(OSError, match="process release failure"):
        lease.release()
    assert lease.reserved_bytes == 100
    assert pool.snapshot().reserved_bytes == 100
    assert pool.snapshot().active_leases == 1

    monkeypatch.setattr(module._PROCESS_TEMPORARY_STORAGE, "release_capability", real_release)
    lease.release()
    assert lease.reserved_bytes == 0
    assert pool.snapshot().reserved_bytes == 0
    assert pool.snapshot().active_leases == 0


def test_temporary_storage_shrink_remains_retryable_after_process_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """A failed shrink preserves both lease and pool counters for an exact retry."""
    from schema_sanitizer.core_impl import temporary_storage as module

    monkeypatch.setattr(
        module, "memory_budget", lambda _limit: SimpleNamespace(replay_spool_bytes=1024)
    )
    pool = module.TemporaryStoragePermitPool(None)
    lease = pool.acquire(100, label="retryable-shrink", path=tmp_path)
    real_resize = module._PROCESS_TEMPORARY_STORAGE.resize_capability

    def fail_release(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected process shrink failure")

    monkeypatch.setattr(module._PROCESS_TEMPORARY_STORAGE, "resize_capability", fail_release)
    with pytest.raises(OSError, match="process shrink failure"):
        lease.resize(40)
    assert lease.reserved_bytes == 100
    assert pool.snapshot().reserved_bytes == 100

    monkeypatch.setattr(module._PROCESS_TEMPORARY_STORAGE, "resize_capability", real_resize)
    lease.resize(40)
    assert lease.reserved_bytes == 40
    assert pool.snapshot().reserved_bytes == 40
    lease.release()


def test_process_resource_governor_rejects_unrepresentable_exact_requests() -> None:
    """Exact file/socket accounting must never silently clamp oversized requests."""
    from schema_sanitizer.core_impl.process_resources import _Governor
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    governor = _Governor(2, "exact_descriptors")
    with pytest.raises(SchemaSanitizerResourceError, match="exceeds process"):
        governor.acquire(3, timeout_seconds=0)
    assert governor.snapshot().in_use == 0
    with pytest.raises(TypeError, match="integer"):
        governor.acquire(1.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="> 0"):
        governor.acquire(0)


def test_temporary_storage_move_rolls_back_target_when_old_release_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """A failed cross-filesystem move retains one old reservation, not two."""
    from schema_sanitizer.core_impl import temporary_storage as module

    class Governor:
        def __init__(self) -> None:
            self.reserved = {1: 0, 2: 0}
            self.inodes = {1: 0, 2: 0}
            self.fail_old_release = False

        @staticmethod
        def target(path: Any) -> Any:
            return path

        @staticmethod
        def filesystem(path: Any) -> tuple[int, Any, int]:
            key = 2 if str(path).endswith("target") else 1
            return key, path, 1 << 40

        def reserve(
            self,
            size_bytes: int,
            *,
            path: Any,
            label: str,
            inode_count: int = 0,
        ) -> int:
            del label
            key, _target, _free = self.filesystem(path)
            self.reserved[key] += size_bytes
            self.inodes[key] += inode_count
            return key

        def release(
            self,
            device: int,
            size_bytes: int,
            *,
            inode_count: int = 0,
        ) -> None:
            if device == 1 and self.fail_old_release:
                raise OSError("injected old filesystem release failure")
            self.reserved[device] -= size_bytes
            self.inodes[device] -= inode_count

        def reserve_capability(self, size_bytes: int, **kwargs: Any) -> Any:
            device = self.reserve(size_bytes, **kwargs)
            return SimpleNamespace(
                governor=self,
                device=device,
                reserved_bytes=size_bytes,
                reserved_inodes=kwargs.get("inode_count", 0),
                active=True,
            )

        def release_capability(self, capability: Any) -> bool:
            self.release(
                capability.device,
                capability.reserved_bytes,
                inode_count=capability.reserved_inodes,
            )
            capability.active = False
            return True

        def resize_capability(
            self,
            capability: Any,
            size_bytes: int,
            *,
            path: Any,
            label: str,
            inode_count: int,
        ) -> Any:
            target_device, _target, _free = self.filesystem(path)
            if target_device == capability.device:
                delta = size_bytes - capability.reserved_bytes
                if delta > 0:
                    self.reserve(delta, path=path, label=label)
                elif delta < 0:
                    self.release(capability.device, -delta)
                capability.reserved_bytes = size_bytes
                return capability
            replacement = self.reserve_capability(
                size_bytes,
                path=path,
                label=label,
                inode_count=inode_count,
            )
            try:
                self.release_capability(capability)
            except BaseException:
                self.release_capability(replacement)
                raise
            return replacement

    governor = Governor()
    monkeypatch.setattr(module, "_PROCESS_TEMPORARY_STORAGE", governor)
    monkeypatch.setattr(
        module, "memory_budget", lambda _limit: SimpleNamespace(replay_spool_bytes=1024)
    )
    source = tmp_path / "source"
    target = tmp_path / "target"
    pool = module.TemporaryStoragePermitPool(None)
    lease = pool.acquire(100, label="move", path=source, artifact_count=1)

    governor.fail_old_release = True
    with pytest.raises(OSError, match="old filesystem release failure"):
        lease.resize(60, path=target)
    assert lease.reserved_bytes == 100
    assert pool.snapshot().reserved_bytes == 100
    assert governor.reserved == {1: 100, 2: 0}
    assert governor.inodes == {1: 1, 2: 0}

    governor.fail_old_release = False
    lease.resize(60, path=target)
    assert governor.reserved == {1: 0, 2: 60}
    assert governor.inodes == {1: 0, 2: 1}
    lease.release()


def test_provider_pool_closed_race_retains_descriptor_until_manager_exit_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late manager keeps its FD reservation until __aexit__ succeeds."""
    from schema_sanitizer.remote_impl import provider_session_pool as module

    class Lease:
        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    class Manager:
        fail_exit = True

        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *_exc: object) -> None:
            if self.fail_exit:
                raise RuntimeError("injected manager exit failure")

    lease = Lease()
    monkeypatch.setattr(module, "acquire_file_descriptors", lambda *_a, **_k: lease)

    async def exercise() -> None:
        pool = module.RemoteProviderSessionPool()
        await pool.__aenter__()
        entered = asyncio.Event()
        resume = asyncio.Event()
        manager = Manager()

        async def factory() -> Manager:
            entered.set()
            await resume.wait()
            return manager

        borrower = asyncio.create_task(pool.borrow_manager(("manager",), factory))
        await entered.wait()
        await pool.__aexit__(None, None, None)
        resume.set()
        with pytest.raises(RuntimeError, match="remote provider session pool is closed"):
            await borrower
        assert lease.releases == 0
        assert len(pool._entries) == 1
        manager.fail_exit = False
        await pool.__aexit__(None, None, None)
        assert len(pool._entries) == 0

    asyncio.run(exercise())
    assert lease.releases == 1


def test_remote_coordinator_event_loop_creation_failure_is_reported_promptly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Host-loop setup failure cannot strand construction until the full deadline."""
    from schema_sanitizer.remote_impl import io_coordinator as module

    def fail_loop() -> Any:
        raise RuntimeError("injected event loop creation failure")

    monkeypatch.setattr(module.asyncio, "new_event_loop", fail_loop)
    with pytest.raises(RuntimeError, match="event loop creation failure"):
        module.RemoteIoCoordinator(shutdown_timeout_seconds=1.0)


def test_async_bridge_event_loop_creation_failure_releases_owned_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Helper-loop construction failure closes the coroutine and returns its permit."""
    from schema_sanitizer.remote_impl import async_bridge

    class Lease:
        amount = 1

        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    lease = Lease()
    monkeypatch.setattr(async_bridge, "acquire_project_threads", lambda *_a, **_k: lease)
    monkeypatch.setattr(
        async_bridge.asyncio,
        "new_event_loop",
        lambda: (_ for _ in ()).throw(RuntimeError("injected bridge loop failure")),
    )

    async def exercise() -> None:
        coroutine = asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="bridge loop failure"):
            async_bridge.run_sync(coroutine, threading_mode="multi")
        assert coroutine.cr_frame is None

    asyncio.run(exercise())
    assert lease.releases == 1


def test_temporary_storage_lease_construction_is_transactional(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """A lease constructor failure rolls back host capacity before local commit."""
    from schema_sanitizer.core_impl import temporary_storage as module

    class BrokenLease:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise MemoryError("injected lease construction failure")

    monkeypatch.setattr(
        module, "memory_budget", lambda _limit: SimpleNamespace(replay_spool_bytes=1024)
    )
    pool = module.TemporaryStoragePermitPool(None)
    before = module.process_temporary_storage_snapshot(tmp_path)
    monkeypatch.setattr(module, "TemporaryStorageLease", BrokenLease)

    with pytest.raises(MemoryError, match="lease construction failure"):
        pool.acquire(100, label="constructor", path=tmp_path)

    after = module.process_temporary_storage_snapshot(tmp_path)
    assert pool.snapshot().reserved_bytes == 0
    assert pool.snapshot().active_leases == 0
    assert after.reserved_bytes == before.reserved_bytes
    assert after.reserved_inodes == before.reserved_inodes


def test_process_resource_unscoped_release_is_observable_but_non_mutating() -> None:
    """The compatibility shim records misuse without bypassing the exact ledger."""
    from schema_sanitizer.core_impl.process_resources import _Governor

    governor = _Governor(4, "diagnostic_resource")
    lease = governor.acquire(2, timeout_seconds=0)
    governor.release(3)
    snapshot = governor.snapshot()
    assert snapshot.in_use == 2
    assert snapshot.active_leases == 1
    assert snapshot.over_release_count == 1
    assert snapshot.over_release_amount == 3
    assert snapshot.compatibility_release_attempts == 1
    lease.release()
    snapshot = governor.snapshot()
    assert snapshot.in_use == 0
    assert snapshot.active_leases == 0
    assert snapshot.over_release_count == 1
    assert snapshot.over_release_amount == 3


def test_operation_registry_pressure_is_exposed_in_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bounded live registry reports saturation instead of silently hiding it."""
    from schema_sanitizer.core_impl import operation_diagnostics as module

    module._reset_after_fork()
    monkeypatch.setattr(module, "_MAX_LIVE", 1)

    class Source:
        def snapshot(self) -> dict[str, object]:
            return {"operation_id": "kept"}

    sources = [Source(), Source()]
    module.register_operation("kept", sources[0].snapshot)
    module.register_operation("rejected", sources[1].snapshot)
    snapshot = module.operation_diagnostic_registry_snapshot()
    assert snapshot.live_entries == 1
    assert snapshot.live_capacity == 1
    assert snapshot.registration_rejections == 1


def test_async_bridge_start_preserves_primary_when_cleanup_also_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup anomalies become notes and cannot replace the startup failure."""
    from schema_sanitizer.remote_impl import async_bridge

    def broken_thread(*args: object, **kwargs: object) -> threading.Thread:
        thread = threading.Thread(*args, **kwargs)

        def fail_start() -> None:
            raise RuntimeError("primary thread start failure")

        thread.start = fail_start  # type: ignore[method-assign]
        return thread

    class BrokenCoroutine:
        def close(self) -> None:
            raise OSError("secondary coroutine close failure")

    class BrokenLease:
        def release(self) -> None:
            raise OSError("secondary permit release failure")

    monkeypatch.setattr(async_bridge, "Thread", broken_thread)
    runner = async_bridge._BridgeRunner(
        BrokenCoroutine(), async_bridge.copy_context(), BrokenLease()
    )
    with pytest.raises(RuntimeError, match="primary thread start failure") as captured:
        runner.start()
    notes = getattr(captured.value, "__notes__", ())
    assert any("coroutine cleanup" in note for note in notes)
    assert any("thread permit retained" in note for note in notes)


def test_remote_submission_preserves_primary_when_rollback_also_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Submission cleanup errors are diagnostic notes, not replacement failures."""
    from schema_sanitizer.remote_impl import io_coordinator as module

    class Submission:
        def release(self) -> None:
            raise OSError("secondary submission rollback failure")

    governor = SimpleNamespace(
        reserve_submission=lambda: Submission(),
        acquire=lambda *_args, **_kwargs: None,
    )
    coordinator = object.__new__(module.RemoteIoCoordinator)
    coordinator._lock = threading.Lock()
    coordinator._closed = False
    coordinator._loop = SimpleNamespace()
    coordinator._permit_governor = governor
    coordinator._operation_id = "operation"
    coordinator._context = None
    coordinator._futures = set()
    monkeypatch.setattr(
        module.asyncio,
        "run_coroutine_threadsafe",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("primary submission failure")),
    )

    async def operation(_context: Any) -> None:
        return None

    with pytest.raises(RuntimeError, match="primary submission failure") as captured:
        coordinator.submit(operation)
    notes = getattr(captured.value, "__notes__", ())
    assert any("submission rollback" in note for note in notes)


def test_process_resource_snapshot_keeps_legacy_positional_construction() -> None:
    """New anomaly counters append defaults without breaking older callers."""
    from schema_sanitizer.core_impl.process_resources import ProcessResourceSnapshot

    snapshot = ProcessResourceSnapshot(8, 1, 2, 3, 4, 5)
    assert snapshot.over_release_count == 0
    assert snapshot.over_release_amount == 0
