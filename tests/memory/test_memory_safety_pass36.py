"""Regression tests for pass36 transactional retry and claim ownership."""

from __future__ import annotations

import gc
import os
import stat
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="POSIX descriptor-relative filesystem hardening suite"
)

_NATIVE_STUB_MODULES = (
    "schema_sanitizer.core_impl.native_options",
    "schema_sanitizer.core_impl.execution_policy",
)


def _purge_module(name: str) -> None:
    sys.modules.pop(name, None)
    parent_name, _, attribute = name.rpartition(".")
    parent = sys.modules.get(parent_name)
    if parent is not None and hasattr(parent, attribute):
        delattr(parent, attribute)


@pytest.fixture
def native_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    from schema_sanitizer.core_impl import native_runtime

    class Stub:
        def options_catalog(self) -> tuple[object, ...]:
            return ()

        def __getattr__(self, _name: str) -> Any:
            return lambda *_args, **_kwargs: None

    real_native = native_runtime.native_core
    preexisting_modules = set(sys.modules)
    sentinel = object()
    saved = {name: sys.modules.get(name, sentinel) for name in _NATIVE_STUB_MODULES}
    monkeypatch.setattr(native_runtime, "native_core", Stub())
    for name in reversed(_NATIVE_STUB_MODULES):
        _purge_module(name)
    try:
        yield
    finally:
        native_runtime.native_core = real_native
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
            _purge_module(name)
        for name in reversed(_NATIVE_STUB_MODULES):
            _purge_module(name)
        for name, module in saved.items():
            if module is sentinel:
                continue
            sys.modules[name] = module
            parent_name, _, attribute = name.rpartition(".")
            parent = sys.modules.get(parent_name)
            if parent is not None:
                setattr(parent, attribute, module)


def _move_all_pending_to_ready(scheduler: Any) -> None:
    """Deterministically perform the timer-worker state transition."""
    with scheduler._condition:
        items = sorted(scheduler._current.values(), key=lambda item: item.sequence)
        for item in items:
            scheduler._current.pop(item.key, None)
            scheduler._drop_pending_charge_locked(item)
            scheduler._enqueue_ready_locked(item)
            scheduler._ready_by_key[item.key] = item
            scheduler._ready_bytes += item.retained_bytes


def test_rejected_retry_replacement_keeps_previous_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    monkeypatch.setattr(module, "_MAX_PENDING_RETRIES", 1)
    monkeypatch.setattr(module, "_MAX_PENDING_BYTES", 1024)
    monkeypatch.setattr(module, "_MAX_SUBSYSTEM_RETRIES", 1)
    monkeypatch.setattr(module, "_MAX_SUBSYSTEM_BYTES", 1024)
    monkeypatch.setattr(module, "_MAX_EMERGENCY_RETRIES", 0)

    def old() -> None:
        pass

    key = ("pass36-transaction", 1)
    normalized_key = module._normalize_retry_key(key)
    assert scheduler.schedule(key, old, delay_seconds=3600, retained_bytes=100)
    monkeypatch.setattr(module, "_MAX_PENDING_BYTES", 50)
    monkeypatch.setattr(module, "_MAX_SUBSYSTEM_BYTES", 50)
    assert not scheduler.schedule(key, lambda: None, delay_seconds=0, retained_bytes=100)
    assert scheduler._current[normalized_key].callback is old
    assert scheduler._pending_bytes == 100
    scheduler.cancel(key)


def test_cancel_removes_retry_after_timer_ready_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    calls: list[str] = []
    key = ("pass36-ready-cancel", 1)
    assert scheduler.schedule(key, lambda: calls.append("stale"), delay_seconds=0)
    _move_all_pending_to_ready(scheduler)
    scheduler.cancel(key)
    with scheduler._condition:
        assert key not in scheduler._ready_by_key
        assert scheduler._take_ready_locked() is None
    assert calls == []


def test_retry_payload_finalizer_runs_outside_scheduler_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    lock_states: list[bool] = []

    class Callback:
        def __call__(self) -> None:
            return None

        def __del__(self) -> None:
            owned = getattr(scheduler._condition, "_is_owned", lambda: False)()
            lock_states.append(bool(owned))

    key = ("pass36-finalizer", 1)
    assert scheduler.schedule(key, Callback(), delay_seconds=3600)
    assert scheduler.schedule(key, lambda: None, delay_seconds=3600)
    gc.collect()
    assert lock_states == [False]
    scheduler.cancel(key)


def test_subsystem_quota_covers_ready_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    monkeypatch.setattr(module, "_MAX_SUBSYSTEM_RETRIES", 1)
    monkeypatch.setattr(module, "_MAX_EMERGENCY_RETRIES", 0)
    first = ("same-subsystem", 1)
    second = ("same-subsystem", 2)
    assert scheduler.schedule(first, lambda: None, delay_seconds=0)
    _move_all_pending_to_ready(scheduler)
    assert not scheduler.schedule(second, lambda: None, delay_seconds=0)
    scheduler.cancel(first)


def test_ready_execution_round_robins_subsystems(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    keys = (("a", 1), ("a", 2), ("b", 1))
    for key in keys:
        assert scheduler.schedule(key, lambda: None, delay_seconds=0)
    _move_all_pending_to_ready(scheduler)
    with scheduler._condition:
        selected = [
            scheduler._take_ready_locked(),
            scheduler._take_ready_locked(),
            scheduler._take_ready_locked(),
        ]
    assert [item.key for item in selected if item is not None] == [
        module._normalize_retry_key(("a", 1)),
        module._normalize_retry_key(("b", 1)),
        module._normalize_retry_key(("a", 2)),
    ]
    # Complete the synthetic active-state ownership accounting.
    with scheduler._condition:
        for item in selected:
            if item is not None:
                scheduler._drop_subsystem_charge_locked(item)


def test_release_guardian_uses_bounded_shared_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    monkeypatch.setattr(module, "_IDLE_SECONDS", 0.02)

    class WorkerPermit:
        def release(self) -> None:
            pass

    monkeypatch.setattr(module, "acquire_release_guardian_thread", WorkerPermit)
    guardian = module._ReleaseGuardian()
    released = 0
    release_lock = threading.Lock()

    class Lease:
        def __init__(self) -> None:
            self.calls = 0

        def release(self) -> None:
            nonlocal released
            self.calls += 1
            if self.calls == 1:
                raise OSError("transient")
            with release_lock:
                released += 1

    owners = [Lease() for _ in range(32)]
    assert all(guardian.adopt(owner, retained_bytes=64) for owner in owners)
    deadline = time.monotonic() + 3
    peak_workers = 0
    while time.monotonic() < deadline:
        snapshot = guardian.snapshot()
        peak_workers = max(peak_workers, snapshot.active_workers)
        if snapshot.pending_owners == 0:
            break
        time.sleep(0.005)
    assert released == len(owners)
    assert peak_workers <= module._MAX_RELEASE_GUARDIAN_WORKERS
    assert guardian.snapshot().retained_bytes == 0


def test_default_coordination_roots_isolate_foreign_legacy_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import schema_sanitizer.core_impl.path_identity as module
    import schema_sanitizer.core_impl.temporary_janitor as janitor_module

    getuid = getattr(os, "geteuid", None)
    if getuid is None:
        pytest.skip("effective-UID isolation is POSIX-specific")
    uid = getuid()
    legacy = tmp_path / module._CLAIM_DIRECTORY
    quarantine_legacy = tmp_path / "schema-sanitizer-quarantine"
    legacy.mkdir(mode=0o700)
    quarantine_legacy.mkdir(mode=0o700)
    foreign_roots = {legacy, quarantine_legacy}
    real_lstat = os.lstat

    def foreign_legacy_lstat(path: Any, *args: Any, **kwargs: Any) -> os.stat_result:
        metadata = real_lstat(path, *args, **kwargs)
        try:
            is_legacy = Path(path) in foreign_roots
        except TypeError:
            is_legacy = False
        if not is_legacy:
            return metadata
        fields = list(metadata)
        fields[4] = uid + 1
        return os.stat_result(fields)

    monkeypatch.delenv(module._COORDINATION_ENV, raising=False)
    monkeypatch.setattr(module.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(module.os, "lstat", foreign_legacy_lstat)
    root = module._private_claim_root()
    assert root == tmp_path / f"{module._CLAIM_DIRECTORY}-{uid}"
    assert root.is_dir()
    assert real_lstat(root).st_uid == uid
    quarantine_root = janitor_module._TemporaryArtifactJanitor.root()
    assert quarantine_root == tmp_path / f"schema-sanitizer-quarantine-{uid}"
    assert quarantine_root.is_dir()
    assert real_lstat(quarantine_root).st_uid == uid


def test_claim_sweep_budget_counts_unrelated_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.path_identity as module

    class Lease:
        released = False

        def release(self) -> None:
            self.released = True

    class Iterator:
        def __init__(self) -> None:
            self.index = 0
            self.closed = False

        def __iter__(self) -> "Iterator":
            return self

        def __next__(self) -> Any:
            if self.index >= 10:
                raise StopIteration
            index = self.index
            self.index += 1
            return SimpleNamespace(
                name=f"unrelated-{index}",
                path=f"/virtual/unrelated-{index}",
            )

        def close(self) -> None:
            self.closed = True

    lease = Lease()
    iterator = Iterator()
    monkeypatch.setattr(module, "_CLAIM_SWEEP_CURSOR", None)
    monkeypatch.setattr(module, "_CLAIM_SWEEP_ITERATOR", None)
    monkeypatch.setattr(module, "_CLAIM_SWEEP_OWNER", None)
    monkeypatch.setattr(module, "_CLAIM_SWEEP_ROOT", None)
    monkeypatch.setattr(module, "acquire_file_descriptors", lambda _n: lease)
    monkeypatch.setattr(module.os, "scandir", lambda _root: iterator)
    root = Path("/virtual")
    module._sweep_external_claims(root, limit=3)
    assert iterator.index == 3
    assert not lease.released
    module._sweep_external_claims(root, limit=10)
    assert iterator.index == 10
    assert iterator.closed
    assert lease.released


def test_published_claim_is_rolled_back_if_parent_fsync_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import schema_sanitizer.core_impl.path_identity as module

    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path / "coord"))
    monkeypatch.setattr(module, "_set_new_owner_marker", lambda *_args: False)
    artifact = tmp_path / "artifact"
    artifact.write_text("x")
    real_fsync = module.os.fsync

    def fail_directory_fsync(descriptor: int) -> None:
        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise OSError("directory persistence failed")
        real_fsync(descriptor)

    # Finalizers from earlier tests may leave a deliberately delayed external
    # claim cleanup in the bounded registry. Drain that independent owner before
    # measuring this transaction's admission balance.
    module._drain_abandoned_claim_owners(limit=64)
    before = module._PATH_CLAIM_ADMISSIONS
    monkeypatch.setattr(module.os, "fsync", fail_directory_fsync)
    with pytest.raises(OSError, match="directory persistence"):
        module.claim_path_identity(artifact)
    root = module._private_claim_root()
    assert not tuple(root.glob("claim-*"))
    assert module._PATH_CLAIM_ADMISSIONS == before
    monkeypatch.setattr(module.os, "fsync", real_fsync)
    identity = module.claim_path_identity(artifact)
    assert identity is not None
    module.release_path_identity(identity)


def test_claim_owner_finalizer_only_transfers_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.path_identity as module

    retained: list[Any] = []
    monkeypatch.setattr(
        module,
        "_retain_abandoned_claim_owner",
        lambda owner, **_kwargs: retained.append(owner),
    )
    monkeypatch.setattr(
        module,
        "_release_claim_owner",
        lambda _owner: (_ for _ in ()).throw(AssertionError("blocking I/O")),
    )
    owner = module.PathClaimOwner(b"x", "/never-touch", None)
    owner.__del__()
    assert retained == [owner]
    owner.released = True


def test_sweep_recovers_dead_claim_write_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import schema_sanitizer.core_impl.path_identity as module

    root = tmp_path / "claims"
    root.mkdir()
    record = module._ExternalClaim(99_999_999, "dead-process-token", b"x" * 16, 1)
    temporary = root / ".claim-write-dead"
    temporary.write_bytes(module._serialize_claim(record))
    monkeypatch.setattr(module, "_CLAIM_SWEEP_CURSOR", None)
    monkeypatch.setattr(module, "_CLAIM_SWEEP_ITERATOR", None)
    monkeypatch.setattr(module, "_CLAIM_SWEEP_OWNER", None)
    monkeypatch.setattr(module, "_CLAIM_SWEEP_ROOT", None)
    module._sweep_external_claims(root, limit=32)
    assert not temporary.exists()


def test_inherited_claim_owner_cannot_release_parent_authority(
    tmp_path: Path,
) -> None:
    import schema_sanitizer.core_impl.path_identity as module

    marker = b"p" * 16
    claim = tmp_path / "claim-parent"
    claim.write_bytes(
        module._serialize_claim(module._ExternalClaim(os.getpid(), "token", marker, 1))
    )
    owner = module.PathClaimOwner(
        marker,
        str(claim),
        None,
        owner_pid=os.getpid() + 1,
    )
    owner.release()
    assert claim.exists()
    assert owner.released


def test_path_claim_admission_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.path_identity as module

    monkeypatch.setattr(module, "_MAX_LIVE_PATH_CLAIMS", 1)
    monkeypatch.setattr(module, "_PATH_CLAIM_ADMISSIONS", 0)
    first = module._acquire_path_claim_admission()
    with pytest.raises(OSError, match="capacity exhausted"):
        module._acquire_path_claim_admission()
    first.release()
    assert module._PATH_CLAIM_ADMISSIONS == 0


def test_cleanup_worker_start_is_published_before_start_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.cleanup_dispatcher as module

    dispatcher = module._CleanupDispatcher()
    created = 0

    class Lease:
        def release(self) -> None:
            return None

    class FakeThread:
        def __init__(self, **_kwargs: Any) -> None:
            nonlocal created
            created += 1
            self.started = False

        def is_alive(self) -> bool:
            return self.started

        def start(self) -> None:
            # Re-enter while Thread.start has not committed.  The start
            # reservation plus published worker must suppress a duplicate.
            dispatcher._ensure_workers()
            self.started = True

    monkeypatch.setattr(module, "acquire_project_threads", lambda *_a, **_k: Lease())
    monkeypatch.setattr(module.threading, "Thread", FakeThread)
    assert dispatcher.submit(lambda: None)
    assert created == 1
    assert dispatcher._workers_starting == 0


def test_async_bridge_transfers_failed_lease_to_guardian(
    monkeypatch: pytest.MonkeyPatch,
    native_stub: None,
) -> None:
    from contextvars import copy_context

    import schema_sanitizer.remote_impl.async_bridge as module

    async def operation() -> None:
        return None

    class Lease:
        def release(self) -> None:
            raise OSError("release failed")

    adopted: list[Any] = []
    monkeypatch.setattr(
        module,
        "adopt_failed_release",
        lambda owner, **_kwargs: not adopted.append(owner),
    )
    coro = operation()
    runner = module._BridgeRunner(coro, copy_context(), Lease())
    assert not runner._release_thread_lease()
    assert runner._lease is None
    assert len(adopted) == 1
    coro.close()


def test_remote_host_resources_transfer_after_release_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.remote_impl.io_coordinator as module

    class Owner:
        def release(self) -> None:
            raise OSError("release failed")

    coordinator = module.RemoteIoCoordinator.__new__(module.RemoteIoCoordinator)
    coordinator._lock = threading.Lock()
    coordinator._release_lock = threading.Lock()
    coordinator._permit_registration = Owner()
    coordinator._thread_lease = Owner()
    adopted: list[Any] = []
    monkeypatch.setattr(
        module,
        "adopt_failed_release",
        lambda owner, **_kwargs: not adopted.append(owner),
    )
    assert coordinator._release_permit_registration(transfer_on_failure=True) is False
    assert coordinator._release_thread_lease(transfer_on_failure=True) is False
    assert coordinator._permit_registration is None
    assert coordinator._thread_lease is None
    assert len(adopted) == 2


def test_pass36_source_contracts() -> None:
    root = Path(__file__).resolve().parents[2]
    retry = (root / "src/schema_sanitizer/core_impl/retry_scheduler.py").read_text()
    identity = (root / "src/schema_sanitizer/core_impl/path_identity.py").read_text()
    arena = (root / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    assert "_ready_by_key" in retry
    assert "_ready_queues" in retry
    assert "_ReleaseGuardian" in retry
    assert "adopt_failed_release" in retry
    assert "_drop_subsystem_charge_locked" in retry
    assert "_ScandirCleanupOwner" in identity
    assert "_MAX_LIVE_PATH_CLAIMS" in identity
    assert "owner_pid != os.getpid()" in identity
    assert "published claim rollback was deferred" in identity
    assert "empty task" in arena
    leased = arena.index("OperationTaskArena::SubmitLeased")
    empty_task = arena.index("if (!task)", leased)
    wrapped = arena.index("Task wrapped", leased)
    assert empty_task < wrapped
