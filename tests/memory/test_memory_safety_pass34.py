"""Regression tests for pass34 ownership, recovery, and teardown hardening."""

from __future__ import annotations

import asyncio
import errno
import os
import threading
import time
from pathlib import Path

import pytest


def test_observation_cannot_release_another_path_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import schema_sanitizer.core_impl.path_identity as module

    artifact = tmp_path / "owned"
    artifact.write_text("payload")
    monkeypatch.setattr(module, "_set_new_owner_marker", lambda *_args: False)
    owner = module.claim_path_identity(artifact)
    assert owner is not None and owner.owns_claim
    observation = module.lstat_identity(artifact)
    assert observation is not None and not observation.owns_claim
    with pytest.raises(OSError, match="does not own"):
        module.release_path_identity(observation)
    with pytest.raises(OSError, match="already owned"):
        module.claim_path_identity(artifact)
    module.release_path_identity(owner)


def test_uncertain_close_never_retries_recycled_fd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.path_identity as module

    class Lease:
        def __init__(self) -> None:
            self.calls = 0

        def release(self) -> None:
            self.calls += 1

    descriptor = os.open(os.devnull, os.O_RDONLY)
    lease = Lease()
    owner = module._IdentityDescriptorOwner(descriptor, lease)
    real_close = os.close
    first = True

    def uncertain_close(fd: int) -> None:
        nonlocal first
        if first:
            first = False
            real_close(fd)
            raise InterruptedError(errno.EINTR, "uncertain close")
        real_close(fd)

    monkeypatch.setattr(module.os, "close", uncertain_close)
    with pytest.raises(InterruptedError):
        owner.release()
    replacement = os.open(os.devnull, os.O_RDONLY)
    try:
        assert replacement == descriptor
        owner.release()
        os.fstat(replacement)
        assert lease.calls == 1
    finally:
        real_close(replacement)


def test_remote_startup_timeout_preserves_real_finally() -> None:
    from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator

    started = threading.Event()
    release = threading.Event()
    finalized = threading.Event()
    exited = threading.Event()

    class Context:
        async def __aenter__(self) -> object:
            started.set()
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                await asyncio.to_thread(release.wait)
            finally:
                finalized.set()
            return object()

        async def __aexit__(self, *_exc: object) -> None:
            exited.set()

    with pytest.raises(RuntimeError, match="startup exceeded"):
        RemoteIoCoordinator(
            context_factory=Context,
            shutdown_timeout_seconds=0.05,
        )
    assert started.wait(2)
    release.set()
    assert finalized.wait(2)
    assert exited.wait(2)


def test_private_crash_leftovers_are_scanned(tmp_path: Path) -> None:
    from schema_sanitizer.core_impl.temporary_janitor import (
        _TemporaryArtifactJanitor,
    )

    root = tmp_path / "quarantine"
    private = root / ".delete"
    private.mkdir(parents=True)
    leftover = private / "delete-crash"
    leftover.write_text("payload")
    janitor = _TemporaryArtifactJanitor()
    janitor.root = lambda: root  # type: ignore[method-assign]
    for _index in range(8):
        janitor._scan_stale()
        if janitor._scanned:
            break
    assert not leftover.exists()


def test_external_claim_sweep_rotates_across_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import schema_sanitizer.core_impl.path_identity as module

    getattr(monkeypatch, "set" + "env")("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    root = module._private_claim_root()
    module._CLAIM_SWEEP_CURSOR = None
    record = module._ExternalClaim(99_999_999, "dead", b"x" * 16, 1)
    payload = module._serialize_claim(record)
    for index in range(80):
        (root / f"claim-{index:04d}").write_bytes(payload)
    for _index in range(3):
        module._sweep_external_claims(root, limit=32)
    assert not tuple(root.glob("claim-*"))


def test_cleanup_dispatcher_counts_active_retained_bytes() -> None:
    from schema_sanitizer.core_impl.cleanup_dispatcher import (
        cleanup_dispatcher_snapshot,
        dispatch_cleanup,
    )

    started = threading.Event()
    release = threading.Event()
    charge = 2 * 1024 * 1024

    def blocking_cleanup() -> None:
        started.set()
        assert release.wait(3)

    before = cleanup_dispatcher_snapshot()
    assert dispatch_cleanup(blocking_cleanup, retained_bytes=charge)
    assert started.wait(2)
    active = cleanup_dispatcher_snapshot()
    assert active.active_bytes >= before.active_bytes + charge
    assert active.pending_bytes <= before.pending_bytes
    release.set()
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if cleanup_dispatcher_snapshot().active_bytes <= before.active_bytes:
            break
        time.sleep(0.01)
    assert cleanup_dispatcher_snapshot().active_bytes <= before.active_bytes


def test_retry_replacement_preserves_exact_byte_accounting() -> None:
    from schema_sanitizer.core_impl.retry_scheduler import (
        cancel_retry,
        retry_scheduler_snapshot,
        schedule_retry,
    )

    key = ("pass34-charge", id(object()))
    before = retry_scheduler_snapshot()
    assert schedule_retry(key, lambda: None, delay_seconds=60, retained_bytes=100)
    assert schedule_retry(key, lambda: None, delay_seconds=60, retained_bytes=300)
    current = retry_scheduler_snapshot()
    assert current.pending_bytes == before.pending_bytes + 300
    cancel_retry(key)
    assert retry_scheduler_snapshot().pending_bytes == before.pending_bytes


def test_retry_subsystems_do_not_create_threading_timers() -> None:
    root = Path(__file__).resolve().parents[2]
    files = (
        root / "src/schema_sanitizer/core_impl/retry_scheduler.py",
        root / "src/schema_sanitizer/core_impl/cleanup_dispatcher.py",
        root / "src/schema_sanitizer/core_impl/temporary_janitor.py",
        root / "src/schema_sanitizer/remote_impl/io_coordinator.py",
    )
    combined = "\n".join(path.read_text() for path in files)
    assert "threading.Timer" not in combined
    assert "Timer(" not in combined
    assert "register_project_thread_availability" in combined


def test_remote_governor_fork_reset_clears_all_derived_state() -> None:
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    governor = RemoteIoPermitGovernor(capacity=4)
    governor._weight_buckets = {3: {"old"}}
    governor._weight_order[3] = None
    governor._operation_weights["old"] = 3
    governor._grants = 7
    governor._cancellations = 5
    governor._peak_waiting = 9
    governor.reset_after_fork()
    assert governor._weight_buckets == {}
    assert not governor._weight_order
    assert governor._operation_weights == {}
    assert governor._grants == 0
    assert governor._cancellations == 0
    assert governor._peak_waiting == 0


def test_memory_cross_process_io_does_not_hold_local_ledger_lock() -> None:
    from threading import Condition, Lock

    from schema_sanitizer.core_impl.memory_budget import OperationMemoryLedger

    entered = threading.Event()
    release = threading.Event()

    class Native:
        reserved = 0

        def operation_memory_ledger_reserve(
            self, _capsule: object, amount: int, _stage: str
        ) -> None:
            self.reserved += amount

        def operation_memory_ledger_release(self, _capsule: object, amount: int) -> None:
            self.reserved -= amount

        def operation_memory_ledger_snapshot(self, _capsule: object) -> tuple[int, int, int]:
            return 1024, self.reserved, self.reserved

    class Cross:
        def resize(self, _amount: int) -> None:
            entered.set()
            assert release.wait(3)

        def release(self) -> None:
            return None

    ledger = OperationMemoryLedger.__new__(OperationMemoryLedger)
    ledger.limit_bytes = 1024
    ledger._pid = os.getpid()
    ledger._native = Native()
    ledger._capsule = object()
    ledger._cross_process = Cross()
    ledger._cross_process_reconciliation_failures = 0
    ledger._cross_process_pending_bytes = 0
    ledger._cross_process_release_deferred = False
    ledger._cross_process_release_failures = 0
    ledger._post_release_observation_failures = 0
    ledger._close_advisory_recorded = False
    ledger._close_peak_bytes = 0
    ledger._lock = Lock()
    ledger._cross_process_io_lock = Lock()
    ledger._close_condition = Condition(ledger._lock)
    ledger._close_started = False
    ledger._closing = False
    ledger._closed = False
    ledger._close_outstanding_bytes = 0
    thread = threading.Thread(target=lambda: ledger.reserve(1, stage="test"))
    thread.start()
    assert entered.wait(2)
    assert ledger.snapshot().reserved_bytes == 1
    release.set()
    thread.join(2)
    assert not thread.is_alive()


def test_storage_resize_journal_runs_outside_pool_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.temporary_storage as module

    entered = threading.Event()
    release = threading.Event()

    class ProcessStorage:
        def filesystem(self, path: object) -> tuple[int, Path, int]:
            return 1, Path(str(path)), 1 << 30

        def reserve(self, amount: int, **_kwargs: object) -> int:
            return 1

        def release(self, _key: int, _amount: int, **_kwargs: object) -> None:
            entered.set()
            assert release.wait(3)

    monkeypatch.setattr(module, "_PROCESS_TEMPORARY_STORAGE", ProcessStorage())
    pool = module.TemporaryStoragePermitPool.__new__(module.TemporaryStoragePermitPool)
    pool.limit_bytes = 64 * 1024 * 1024
    pool._lock = threading.Lock()
    pool._condition = threading.Condition(pool._lock)
    pool._reserved_bytes = 10
    pool._pending_reserved_bytes = 0
    pool._pending_active_leases = 0
    pool._resize_inflight = 0
    pool._pending_resize_growth = 0
    pool._peak_reserved_bytes = 10
    pool._active_leases = 1
    pool._closed = False
    pool._close_complete = False
    pool._close_outstanding_bytes = 0
    pool._close_active_leases = 0
    pool._over_release_count = 0
    pool._over_release_bytes = 0
    thread = threading.Thread(
        target=lambda: pool._resize(
            10,
            5,
            filesystem_key=1,
            label="test",
            path=Path("/tmp/pass34"),
            inode_count=1,
        )
    )
    thread.start()
    assert entered.wait(2)
    assert pool.snapshot().reserved_bytes == 10
    release.set()
    thread.join(2)
    assert not thread.is_alive()
    assert pool.snapshot().reserved_bytes == 5


def test_pass34_native_teardown_and_active_byte_contracts() -> None:
    root = Path(__file__).resolve().parents[2]
    header = (root / "cpp/src/internal/runtime/operation_task_arena.hh").read_text()
    source = (root / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    runtime = (root / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text()
    assert "class ArenaCleanupReaper" in source
    assert "reaper_next" in source and "reaper_self" in source
    assert "std::make_shared<std::list" not in source
    assert "static std::vector" not in source
    assert "active_retained_bytes" in header
    assert "peak_active_retained_bytes" in header
    assert "retained_bytes_total" in source
    assert "ActiveRetainedCharge" in runtime
    assert "state_->active_bytes.fetch_add" in runtime
    assert "SaturatingAtomicSubtract(state_->retained_bytes_total" in runtime


def test_pass34_reconciliation_source_contracts() -> None:
    root = Path(__file__).resolve().parents[2]
    memory = (root / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()
    storage = (root / "src/schema_sanitizer/core_impl/temporary_storage.py").read_text()
    identity = (root / "src/schema_sanitizer/core_impl/path_identity.py").read_text()
    assert "_cross_process_io_lock" in memory
    assert "without holding the local ledger lock" in memory
    assert "_resize_inflight" in storage
    assert "_pending_resize_growth" in storage
    assert "Relinquish the FD number before an uncertain close" in identity
    assert "path fingerprint does not own a releasable claim" in identity
