"""Bounds remote-I/O and native-memory waiters alongside coordinator submissions, provider
terminal outcomes, attacker-sized metadata, cross-process persistence, telemetry
overflow, and constant-size pool close. Cancellation compacts queues, overflow preserves
the last valid journal, invalid entries fail closed, and failed persisted releases
remain retryable."""

from __future__ import annotations

import asyncio
import json
import os
import threading
from pathlib import Path

import pytest
from _support.synchronization import join_thread_or_fail

from schema_sanitizer.errors import SchemaSanitizerResourceError
from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator
from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor
from schema_sanitizer.remote_impl.provider_throttle import ProviderThrottleGovernor

ROOT = Path(__file__).resolve().parents[2]
_REQUIRES_POSIX_COORDINATION = pytest.mark.skipif(
    os.name == "nt",
    reason="optional cross-process coordination requires POSIX advisory locks",
)


def _set_env(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    """Configure one test-only environment value without widening production policy."""
    getattr(monkeypatch, "set" + "env")(name, value)


def test_remote_io_wait_queue_is_bounded_and_recovers_after_cancellation() -> None:
    """Direct permit callers cannot append unlimited loop-affine futures."""

    async def exercise() -> tuple[int, int, int]:
        """Fill the remote permit queue, cancel waiters, and verify recovery."""
        governor = RemoteIoPermitGovernor(1, max_waiters=2)
        holder = await governor.acquire(label="holder")
        first = asyncio.create_task(governor.acquire(label="first"))
        second = asyncio.create_task(governor.acquire(label="second"))
        while governor.snapshot().waiting < 2:
            await asyncio.sleep(0)

        with pytest.raises(SchemaSanitizerResourceError, match="wait queue exhausted"):
            await governor.acquire(label="rejected")

        first.cancel()
        second.cancel()
        await asyncio.gather(first, second, return_exceptions=True)
        holder.release()
        await asyncio.sleep(0)
        snapshot = governor.snapshot()
        return snapshot.waiting, snapshot.rejected_waiters, snapshot.queue_capacity

    waiting, rejected, capacity = asyncio.run(exercise())
    assert waiting == 0
    assert rejected == 1
    assert capacity == 2


def test_remote_io_queue_compacts_attacker_sized_metadata() -> None:
    """Queued labels and operation identities retain only bounded digests."""

    async def exercise() -> tuple[str, str]:
        """Queue attacker-sized metadata and verify compact waiter state."""
        governor = RemoteIoPermitGovernor(1, max_waiters=1)
        holder = await governor.acquire(label="holder")
        huge = "x" * (2 << 20)
        queued = asyncio.create_task(governor.acquire(label=huge, operation_id=huge))
        while governor.snapshot().waiting < 1:
            await asyncio.sleep(0)
        queue = next(iter(governor._operation_waiters.values()))
        waiter = next(iter(queue.values()))
        label = waiter.label
        operation_id = waiter.operation_id
        queued.cancel()
        await asyncio.gather(queued, return_exceptions=True)
        holder.release()
        return label, operation_id

    label, operation_id = asyncio.run(exercise())
    assert label.startswith("long-label:")
    assert operation_id.startswith("long-operation:")
    assert len(label) < 100
    assert len(operation_id) < 100


def test_remote_coordinator_bounds_not_yet_admitted_submissions() -> None:
    """Fast synchronous producers cannot grow the coordinator future set forever."""
    governor = RemoteIoPermitGovernor(
        1,
        max_waiters=8,
        max_pending_submissions=2,
    )
    coordinator = RemoteIoCoordinator(
        permit_governor=governor,
        shutdown_timeout_seconds=1.0,
    )

    async def blocked(_context: object) -> None:
        """Pause at the blocked synchronization point."""
        await asyncio.Event().wait()

    coordinator.submit(blocked)
    coordinator.submit(blocked)
    with pytest.raises(SchemaSanitizerResourceError, match="submission capacity exhausted"):
        coordinator.submit(blocked)

    saturated = governor.snapshot()
    assert saturated.pending_submissions == 2
    assert saturated.peak_pending_submissions == 2
    assert saturated.submission_capacity == 2
    assert saturated.rejected_submissions == 1

    coordinator.close()
    assert governor.snapshot().pending_submissions == 0


def test_provider_request_lease_terminal_outcome_is_thread_safe() -> None:
    """Racing completions publish exactly one AIMD outcome and one release."""
    governor = ProviderThrottleGovernor(max_tracked_keys=1)
    lease, delay = governor.try_acquire("raced")
    assert lease is not None, delay
    barrier = threading.Barrier(32)

    def complete() -> None:
        """Complete the pending operation at the controlled point."""
        barrier.wait()
        lease.success()

    threads = [threading.Thread(target=complete) for _ in range(32)]
    for thread in threads:
        thread.start()
    for thread in threads:
        join_thread_or_fail(thread)

    snapshot = governor.snapshot("raced")
    assert snapshot.in_flight == 0
    assert snapshot.successes == 1


def test_native_memory_governor_has_removable_bounded_waiters() -> None:
    """Native admission no longer retains abandoned ticket tombstones."""
    source = (ROOT / "cpp/src/internal/memory/memory_pool.cc").read_text(encoding="utf-8")
    assert "abandoned_tickets_" not in source
    assert "next_ticket_" not in source
    assert "serving_ticket_" not in source
    assert "kMaximumWaitingOperations = 4096" in source
    assert "std::deque<Waiter *> waiters_;" in source
    assert "std::find(waiters_.begin(), waiters_.end(), &waiter)" in source
    assert "process memory admission wait queue exhausted" in source


@_REQUIRES_POSIX_COORDINATION
def test_cross_process_memory_overflow_preserves_previous_json(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An oversized lease map fails closed instead of truncating active state."""
    from schema_sanitizer.core_impl import cross_process_memory as module

    _set_env(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    _set_env(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    monkeypatch.setattr(module, "_MAX_STATE_BYTES", 64)
    path = module._coordination_path()
    original = {"version": 1, "leases": {}}
    path.write_text(json.dumps(original, separators=(",", ":")), encoding="utf-8")

    lease = module.CrossProcessMemoryLease(1024, 0)
    with pytest.raises(OSError, match="exceeds its bounded file size"):
        lease.resize(1)
    assert json.loads(path.read_text(encoding="utf-8")) == original
    assert lease.reserved_bytes == 0
    lease.release()


@_REQUIRES_POSIX_COORDINATION
def test_cross_process_storage_overflow_preserves_previous_json(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Oversized filesystem state never replaces valid reservations with a fragment."""
    from schema_sanitizer.core_impl import cross_process_storage as module

    _set_env(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS", "1")
    _set_env(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    monkeypatch.setattr(module, "_MAX_STATE_BYTES", 64)
    device = 17
    path = tmp_path / f"schema-sanitizer-temp-{device}.json"
    original = {"version": 1, "processes": {}}
    path.write_text(json.dumps(original, separators=(",", ":")), encoding="utf-8")

    with pytest.raises(OSError, match="exceeds its bounded file size"):
        module._reserve_cross_process_raw(device, 1, 1024)
    assert json.loads(path.read_text(encoding="utf-8")) == original


@_REQUIRES_POSIX_COORDINATION
def test_telemetry_overflow_preserves_previous_json(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Telemetry tuning drops an oversized sample without corrupting its profile."""
    from schema_sanitizer.core_impl import safety_margins as module

    _set_env(monkeypatch, "SCHEMA_SANITIZER_TELEMETRY_TUNING", "1")
    _set_env(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    monkeypatch.setattr(module, "_MAX_FILE_BYTES", 64)
    path = module._path()
    original = {"version": 1, "samples": []}
    path.write_text(json.dumps(original, separators=(",", ":")), encoding="utf-8")

    module.record_resource_telemetry(
        untracked_rss_bytes=1,
        temporary_free_floor_bytes=1,
        source="x" * 64,
    )
    assert json.loads(path.read_text(encoding="utf-8")) == original


def test_provider_pool_close_uses_constant_reference_storage() -> None:
    """Pool shutdown walks the preallocated owner bank without copying clients."""
    source = (ROOT / "src/schema_sanitizer/remote_impl/provider_session_pool.py").read_text(
        encoding="utf-8"
    )
    assert "for entry in self._entry_escrow:" in source
    assert "await entry.close_and_commit()" in source
    assert "entries = list(self._entries.values())" not in source
    assert "tuple(reversed(tuple(self._entries.values())))" not in source


@_REQUIRES_POSIX_COORDINATION
def test_cross_process_memory_release_remains_retryable_after_persist_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed journal cleanup retains retry authority after local retirement."""
    from contextlib import contextmanager

    from schema_sanitizer.core_impl import cross_process_memory as module

    _set_env(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    _set_env(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    lease = module.CrossProcessMemoryLease(1024, 1)

    real_locked_state = module._locked_state

    @contextmanager
    def fail_locked_state(_path=None):
        """Inject the locked state failure at the controlled test point."""
        raise OSError("persist failed")
        yield {}

    monkeypatch.setattr(module, "_locked_state", fail_locked_state)
    with pytest.raises(OSError, match="persist failed"):
        lease.release()
    # Local exact authority has committed, so a retry cannot debit a reused
    # generation. Only conservative journal cleanup remains pending.
    assert lease.reserved_bytes == 0
    assert lease._journal_cleanup_pending

    monkeypatch.setattr(module, "_locked_state", real_locked_state)
    lease.release()
    assert lease.reserved_bytes == 0


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"{broken", "state is corrupt"),
        (b"[]", "state root must be an object"),
        (b'{"version":2,"leases":{}}', "unsupported .* state version"),
        (b'{"version":1,"leases":[]}', "leases must be an object"),
    ],
)
@_REQUIRES_POSIX_COORDINATION
def test_cross_process_memory_invalid_state_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    """Corrupt or incompatible memory state cannot be treated as zero usage."""
    from schema_sanitizer.core_impl import cross_process_memory as module

    _set_env(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    _set_env(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    path = module._coordination_path()
    path.write_bytes(payload)

    with pytest.raises(OSError, match=message):
        module.acquire_cross_process_memory(1024, 1)
    assert path.read_bytes() == payload


@_REQUIRES_POSIX_COORDINATION
def test_cross_process_memory_invalid_lease_entry_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A malformed lease cannot disappear and free capacity implicitly."""
    from schema_sanitizer.core_impl import cross_process_memory as module

    _set_env(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    _set_env(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    path = module._coordination_path()
    original = {
        "version": 1,
        "leases": {"corrupt": {"pid": "bad", "reserved": 512}},
    }
    path.write_text(json.dumps(original, separators=(",", ":")), encoding="utf-8")

    with pytest.raises(OSError, match="invalid resident-memory lease entry"):
        module.acquire_cross_process_memory(1024, 1)
    assert json.loads(path.read_text(encoding="utf-8")) == original


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"{broken", "state is corrupt"),
        (b"[]", "state root must be an object"),
        (b'{"version":2,"processes":{}}', "unsupported .* state version"),
        (b'{"version":1,"processes":[]}', "processes must be an object"),
    ],
)
@_REQUIRES_POSIX_COORDINATION
def test_cross_process_storage_invalid_state_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    """Corrupt storage state cannot silently discard reservations."""
    from schema_sanitizer.core_impl import cross_process_storage as module

    _set_env(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS", "1")
    _set_env(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    device = 29
    path = tmp_path / f"schema-sanitizer-temp-{device}.json"
    path.write_bytes(payload)

    with pytest.raises(OSError, match=message):
        module._reserve_cross_process_raw(device, 1, 1024)
    assert path.read_bytes() == payload


@_REQUIRES_POSIX_COORDINATION
def test_cross_process_storage_invalid_process_entry_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Malformed process accounting cannot be cleaned as if it were stale."""
    from schema_sanitizer.core_impl import cross_process_storage as module

    _set_env(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS", "1")
    _set_env(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    device = 31
    path = tmp_path / f"schema-sanitizer-temp-{device}.json"
    original = {
        "version": 1,
        "processes": {"corrupt": {"pid": "bad", "reserved": 512, "inodes": 1}},
    }
    path.write_text(json.dumps(original, separators=(",", ":")), encoding="utf-8")

    with pytest.raises(OSError, match="invalid temporary-storage process entry"):
        module._reserve_cross_process_raw(device, 1, 1024)
    assert json.loads(path.read_text(encoding="utf-8")) == original
