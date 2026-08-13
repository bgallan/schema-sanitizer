"""Regression coverage for memory path identity charges fd and removes claim."""

from __future__ import annotations

import asyncio
import os
import time
from concurrent.futures import Future
from contextvars import copy_context
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from typing import Any

import pytest

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="POSIX descriptor-relative filesystem hardening suite"
)

_NATIVE_STUB_MODULES = (
    "schema_sanitizer.core_impl.native_options",
    "schema_sanitizer.core_impl.execution_policy",
    "schema_sanitizer.api_impl.input.directory_preparation",
    "schema_sanitizer.api_impl.source_plan.attached",
    "schema_sanitizer.api_impl.source_plan.remote",
)


def test_path_identity_charges_fd_and_removes_claim(tmp_path: Path) -> None:
    from schema_sanitizer.core_impl.path_identity import (
        claim_path_identity,
        release_path_identity,
    )
    from schema_sanitizer.core_impl.process_resources import (
        process_file_descriptor_snapshot,
    )

    artifact = tmp_path / "owned"
    artifact.write_bytes(b"payload")
    before = process_file_descriptor_snapshot().in_use
    identity = claim_path_identity(artifact)
    assert identity is not None
    assert process_file_descriptor_snapshot().in_use == before + 1
    release_path_identity(identity)
    assert process_file_descriptor_snapshot().in_use == before

    reclaimed = claim_path_identity(artifact)
    assert reclaimed is not None
    release_path_identity(reclaimed)
    assert process_file_descriptor_snapshot().in_use == before


def test_async_bridge_retains_thread_lease_until_real_finally(
    native_stub: None,
) -> None:
    from schema_sanitizer.remote_impl.async_bridge import _BridgeRunner

    started = Event()
    release = Event()
    finalized = Event()

    class Lease:
        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    async def stubborn() -> None:
        started.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            while not release.is_set():
                await asyncio.sleep(0.005)
        finally:
            finalized.set()

    lease = Lease()
    runner = _BridgeRunner(stubborn(), copy_context(), lease)
    runner.start()
    assert started.wait(1)
    runner.cancel()
    assert runner.result.cancelled()
    time.sleep(0.03)
    assert lease.releases == 0
    assert not finalized.is_set()
    release.set()
    runner._thread.join(1)
    assert finalized.is_set()
    assert lease.releases == 1


def test_coordinator_timeout_keeps_live_host_retryable() -> None:
    from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator

    started = Event()
    release = Event()

    async def stubborn(_context: Any) -> int:
        started.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            while not release.is_set():
                await asyncio.sleep(0.005)
        return 7

    coordinator = RemoteIoCoordinator(
        shutdown_timeout_seconds=0.08,
        permit_capacity=2,
        operation_id="path-identity-charges-fd-and-removes-zombie-regression",
    )
    future = coordinator.submit(stubborn)
    assert started.wait(1)
    with pytest.raises(RuntimeError, match="shutdown exceeded"):
        coordinator.close()
    assert coordinator._thread.is_alive()
    assert coordinator._submissions

    release.set()
    deadline = time.monotonic() + 1
    while coordinator._submissions and time.monotonic() < deadline:
        time.sleep(0.005)
    assert future.done()
    coordinator.close()
    assert not coordinator._thread.is_alive()
    assert not coordinator._submissions


def test_terminal_callback_runs_off_loop_and_is_retried() -> None:
    from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator

    first_failure = Event()
    callback_thread: list[int] = []
    calls = 0

    coordinator = RemoteIoCoordinator(
        shutdown_timeout_seconds=1,
        permit_capacity=2,
        operation_id="path-identity-charges-fd-and-removes-terminal-callback",
    )

    async def immediate(_context: Any) -> int:
        return 1

    future = coordinator.submit(immediate)
    owner = getattr(future, "_schema_sanitizer_remote_submission")

    def cleanup(_future: Future[Any]) -> None:
        nonlocal calls
        calls += 1
        callback_thread.append(__import__("threading").get_ident())
        if calls == 1:
            first_failure.set()
            raise OSError("transient cleanup failure")

    owner.add_terminal_callback(cleanup)
    assert future.result(timeout=1) == 1
    assert first_failure.wait(1)
    second = coordinator.submit(immediate)
    assert second.result(timeout=1) == 1
    coordinator.close()
    assert calls == 2
    assert callback_thread
    assert all(ident != coordinator.thread_ident for ident in callback_thread)


def test_callbackless_storage_waits_for_real_terminal(
    native_stub: None,
) -> None:
    from schema_sanitizer.api_impl.source_plan.remote import (
        RemoteChunkPrefetchIterator,
        _StorageLeaseRollbackOwner,
    )

    class Lease:
        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    iterator = object.__new__(RemoteChunkPrefetchIterator)
    iterator._callbackless_storage_futures = {}
    iterator._failed_storage_leases = __import__("collections").deque()
    future: Future[Any] = Future()
    terminal = Event()
    submission = SimpleNamespace(terminal=terminal, operation_error=RuntimeError("x"))
    setattr(future, "_schema_sanitizer_remote_submission", submission)
    future.cancel()
    lease = Lease()
    rollback = _StorageLeaseRollbackOwner(lease)
    iterator._callbackless_storage_futures[future] = rollback

    iterator._complete_callbackless_storage_futures()
    assert future in iterator._callbackless_storage_futures
    assert lease.releases == 0
    terminal.set()
    iterator._complete_callbackless_storage_futures()
    assert future not in iterator._callbackless_storage_futures
    assert lease.releases == 1


def test_janitor_filesystem_claim_does_not_hold_global_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.core_impl import temporary_janitor as module

    entered = Event()
    allow = Event()

    def blocked_claim(_path: object) -> None:
        entered.set()
        assert allow.wait(1)
        return None

    class Lease:
        def release(self) -> None:
            return None

    janitor = module._TemporaryArtifactJanitor()
    monkeypatch.setattr(module, "claim_path_identity", blocked_claim)
    monkeypatch.setattr(janitor, "_ensure_thread_locked", lambda: None)
    worker = Thread(
        target=lambda: janitor.quarantine(tmp_path / "missing", is_dir=False, lease=Lease())
    )
    worker.start()
    assert entered.wait(1)
    assert janitor._lock.acquire(timeout=0.1)
    janitor._lock.release()
    allow.set()
    worker.join(1)
    assert not worker.is_alive()


def test_permit_operation_queues_have_bounded_removal_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from collections import OrderedDict

    from schema_sanitizer.remote_impl.io_permits import (
        _MAX_LOCAL_BYPASS_SCAN,
        RemoteIoPermitGovernor,
        _Waiter,
    )

    loop = asyncio.new_event_loop()
    try:
        governor = RemoteIoPermitGovernor(capacity=1, max_waiters=5000)
        waiters = [
            _Waiter(loop, loop.create_future(), 1, "x", "one-operation") for _ in range(4096)
        ]
        with governor._lock:
            for waiter in waiters:
                governor._enqueue_waiter_locked(waiter)
            queue = governor._operation_waiters["one-operation"]
            assert isinstance(queue, OrderedDict)

            examined = 0
            effective_weight = governor._effective_weight

            def counted_effective_weight(waiter: _Waiter) -> int:
                nonlocal examined
                examined += 1
                return effective_weight(waiter)

            monkeypatch.setattr(governor, "_effective_weight", counted_effective_weight)
            peak_examined_per_removal = 0
            for waiter in reversed(waiters):
                before = examined
                governor._remove_waiter_locked(waiter)
                peak_examined_per_removal = max(
                    peak_examined_per_removal,
                    examined - before,
                )
        assert peak_examined_per_removal <= _MAX_LOCAL_BYPASS_SCAN + 1
        assert governor._waiting_count == 0
    finally:
        loop.close()


def test_arena_plan_is_opaque_and_queue_is_byte_bounded() -> None:
    root = Path(__file__).resolve().parents[2]
    header = (root / "cpp/src/internal/runtime/operation_task_arena.hh").read_text()
    source = (root / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    ordered = (root / "cpp/src/internal/runtime/ordered_executor.hh").read_text()

    private = header.index("private:", header.index("class TaskArenaSubmissionPlan"))
    width = header.index("std::size_t width_", private)
    assert private < width
    assert "bool ValidPlan" in source or "ValidPlan(" in source
    assert "SubmitCharged" in header
    assert "queue_byte_capacity" in header
    assert "rejected_byte_submissions" in header
    assert "SubmitCharged(Packet packet" in ordered
    assert "TaskMemoryCharge{retained_bytes}" in ordered
    swap = source.index("drain.swap(slot->tasks);")
    unlock_scope = source.index("slot->tasks.clear();", swap)
    detach = source.index("worker->detach();", unlock_scope)
    assert swap < unlock_scope < detach


def test_external_claim_uses_process_instance_identity() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "src/schema_sanitizer/core_impl/path_identity.py").read_text()
    assert '"process_token"' in source
    assert "process_identity_matches" in source
    assert "checksum" in source
    assert "_sweep_external_claims" in source
    assert "os.kill(record.pid, 0)" in source
