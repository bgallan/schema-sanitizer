"""Consolidate ownership observation, reconciliation, and lock-separation contracts.

The cases cover path claims, recycled descriptors, crash-leftover sweeps, retry accounting,
remote-governor reset, cross-process journals, cleanup queues, and native teardown.
"""

from __future__ import annotations

import asyncio
import errno
import os
import threading
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="POSIX descriptor-relative filesystem hardening suite"
)


def test_observation_cannot_release_another_path_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify observation cannot release another path claim."""
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
    """Verify uncertain close never retries recycled FD."""
    import schema_sanitizer.core_impl.path_identity as module

    class Lease:
        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.calls = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.calls += 1

    descriptor = os.open(os.devnull, os.O_RDONLY)
    lease = Lease()
    owner = module._IdentityDescriptorOwner(descriptor, lease)
    retained_debts: list[object] = []
    monkeypatch.setattr(
        module,
        "retain_uncertain_fd_close",
        lambda capability, **_kwargs: not retained_debts.append(capability),
    )
    real_close = os.close
    first = True

    def uncertain_close(fd: int) -> None:
        """Close the descriptor with an intentionally uncertain outcome."""
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
        assert retained_debts == [lease]
        assert lease.calls == 0
    finally:
        real_close(replacement)


def test_remote_startup_timeout_preserves_real_finally() -> None:
    """Verify remote startup timeout preserves real finally."""
    from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator

    started = threading.Event()
    release = threading.Event()
    finalized = threading.Event()
    exited = threading.Event()

    class Context:
        async def __aenter__(self) -> object:
            """Enter the asynchronous context managed by the context test double."""
            started.set()
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                await asyncio.to_thread(release.wait)
            finally:
                finalized.set()
            return object()

        async def __aexit__(self, *_exc: object) -> None:
            """Exit the asynchronous context managed by the context test double and run cleanup."""
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
    """Verify private crash leftovers are scanned."""
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
    """Verify external claim sweep rotates across namespace."""
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
    """Verify cleanup dispatcher counts active retained bytes."""
    from schema_sanitizer.core_impl.cleanup_dispatcher import (
        cleanup_dispatcher_snapshot,
        dispatch_cleanup,
    )

    started = threading.Event()
    release = threading.Event()
    charge = 2 * 1024 * 1024

    def blocking_cleanup() -> None:
        """Pause at the blocking cleanup synchronization point."""
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


def test_retry_replacement_preserves_exact_byte_accounting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify retry replacement preserves exact byte accounting."""
    from schema_sanitizer.core_impl import retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    key = ("observation-cannot-release-another-path-claim-charge", id(object()))
    normalized_key = module._normalize_retry_key(key)
    assert scheduler.snapshot().pending_bytes == 0
    assert scheduler.schedule(key, lambda: None, delay_seconds=60, retained_bytes=100)
    assert scheduler.snapshot().pending_bytes == 100
    assert scheduler.schedule(key, lambda: None, delay_seconds=60, retained_bytes=300)
    current = scheduler.snapshot()
    assert current.pending_retries == 1
    assert current.pending_bytes == 300
    assert scheduler._current[normalized_key].retained_bytes == 300
    scheduler.cancel(key)
    after = scheduler.snapshot()
    assert after.pending_retries == 0
    assert after.pending_bytes == 0
    assert after.generation_entries == 0
    assert all(
        normalized_key not in owners
        for owners in (
            scheduler._current,
            scheduler._ready_by_key,
            scheduler._active_by_key,
            scheduler._successors,
            scheduler._emergency,
        )
    )


def test_retry_subsystems_do_not_create_threading_timers() -> None:
    """Verify retry subsystems do not create threading timers."""
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
    """Verify remote governor fork reset clears all derived state."""
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
    """Verify memory cross process I/O does not hold local ledger lock."""
    from threading import Condition, Lock

    from schema_sanitizer.core_impl.memory_budget import OperationMemoryLedger

    entered = threading.Event()
    release = threading.Event()

    class Native:
        reserved = 0

        def operation_memory_ledger_reserve_snapshot(
            self, _capsule: object, amount: int, _stage: str
        ) -> tuple[int, int, int]:
            """Increase the native reserved total and return its ledger snapshot."""
            self.reserved += amount
            return 1024, self.reserved, self.reserved

        def operation_memory_ledger_release(self, _capsule: object, amount: int) -> None:
            """Subtract the released bytes from the native reserved total."""
            self.reserved -= amount

        def operation_memory_ledger_snapshot(self, _capsule: object) -> tuple[int, int, int]:
            """Return the current operation-memory ledger snapshot."""
            return 1024, self.reserved, self.reserved

    class Cross:
        def resize(self, _amount: int) -> None:
            """Resize the resource represented by the cross test double."""
            entered.set()
            assert release.wait(3)

        def release(self) -> None:
            """Release the resource held by the cross test double."""
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
    tmp_path: Path,
) -> None:
    """Verify storage resize journal runs outside pool lock."""
    import schema_sanitizer.core_impl.temporary_storage as module

    entered = threading.Event()
    release = threading.Event()
    pool = module.TemporaryStoragePermitPool(64 * 1024 * 1024)
    lease = pool.acquire(10, label="test", path=tmp_path)
    original_resize = module._PROCESS_TEMPORARY_STORAGE.resize_capability

    def blocking_resize(*args: object, **kwargs: object) -> object:
        """Pause at the blocking resize synchronization point."""
        entered.set()
        assert release.wait(3)
        return original_resize(*args, **kwargs)

    monkeypatch.setattr(module._PROCESS_TEMPORARY_STORAGE, "resize_capability", blocking_resize)
    thread = threading.Thread(target=lambda: lease.resize(5))
    thread.start()
    assert entered.wait(2)
    assert pool.snapshot().reserved_bytes == 10
    release.set()
    thread.join(2)
    assert not thread.is_alive()
    assert pool.snapshot().reserved_bytes == 5
    lease.release()


def test_native_teardown_and_active_byte_contracts() -> None:
    """Verify native teardown and active byte contracts."""
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


def test_reconciliation_source_contracts() -> None:
    """Verify reconciliation source contracts."""
    root = Path(__file__).resolve().parents[2]
    memory = (root / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()
    storage = (root / "src/schema_sanitizer/core_impl/temporary_storage.py").read_text()
    identity = (root / "src/schema_sanitizer/core_impl/path_identity.py").read_text()
    assert "_cross_process_io_lock" in memory
    assert "without holding the local ledger lock" in memory
    assert "_resize_inflight" in storage
    assert "_pending_resize_growth" in storage
    assert "Detach before close so EINTR/uncertainty" in identity
    assert "path fingerprint does not own a releasable claim" in identity
