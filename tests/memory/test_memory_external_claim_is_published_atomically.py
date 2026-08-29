"""Stress-tests atomic path-claim publication with retry-generation reuse, heap compaction,
guardian startup rollback, callback fairness, partial releases, stale deletion, and
asynchronous bridge drains. A claim becomes visible as one transaction and stays owned
until exact external-marker cleanup, even when callbacks spawn work or deletions fail."""

from __future__ import annotations

import asyncio
import gc
import os
import threading
import time
from contextvars import copy_context
from pathlib import Path
from typing import Any

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS, join_thread_or_fail

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="POSIX descriptor-relative filesystem hardening suite"
)

_NATIVE_STUB_MODULES = (
    "schema_sanitizer.core_impl.native_options",
    "schema_sanitizer.core_impl.execution_policy",
)


def test_external_claim_is_published_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify external claim is published atomically."""
    import schema_sanitizer.core_impl.path_identity as module

    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path / "coord"))
    monkeypatch.setattr(module, "_set_new_owner_marker", lambda *_args: False)
    path = tmp_path / "artifact"
    path.write_text("x")
    entered = threading.Event()
    release = threading.Event()
    original = module._write_claim_payload
    calls = 0
    calls_lock = threading.Lock()

    def blocked_write(descriptor: int, payload: bytes) -> None:
        """Pause at the blocked write synchronization point."""
        nonlocal calls
        with calls_lock:
            calls += 1
            first = calls == 1
        if first:
            entered.set()
            assert release.wait(SCHEDULER_TIMEOUT_SECONDS)
        original(descriptor, payload)

    monkeypatch.setattr(module, "_write_claim_payload", blocked_write)
    outcomes: list[object] = []

    def claim() -> None:
        """Claim the resource at the controlled publication point."""
        try:
            outcomes.append(module.claim_path_identity(path))
        except BaseException as exc:
            outcomes.append(exc)

    first = threading.Thread(target=claim)
    second = threading.Thread(target=claim)
    first.start()
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    second.start()
    join_thread_or_fail(second)
    release.set()
    join_thread_or_fail(first)
    owners = [value for value in outcomes if isinstance(value, module.PathIdentity)]
    errors = [value for value in outcomes if isinstance(value, BaseException)]
    assert len(owners) == 1
    assert len(errors) == 1
    assert "already owned" in str(errors[0])
    module.release_path_identity(owners[0])


def test_retry_cancel_reschedule_never_reuses_old_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify retry cancel reschedule never reuses old generation."""
    import schema_sanitizer.core_impl.retry_scheduler as module

    key = ("external-claim-is-published-atomically-aba", object())
    calls: list[str] = []
    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    assert scheduler.schedule(key, lambda: calls.append("old"), delay_seconds=0)
    old = next(iter(scheduler._current.values()))
    old_token = old.token
    scheduler.cancel(key)
    assert old.token == 0
    assert scheduler.schedule(key, lambda: calls.append("new"), delay_seconds=60)
    new = next(iter(scheduler._current.values()))
    assert new is not old
    assert new.token != old_token
    assert calls == []
    scheduler.cancel(key)
    assert scheduler.close(deadline_seconds=0)


def test_retry_heap_compacts_replaced_payloads() -> None:
    """Verify retry heap compacts replaced payloads."""
    from schema_sanitizer.core_impl.retry_scheduler import (
        cancel_retry,
        retry_scheduler_snapshot,
        schedule_retry,
    )

    key = ("external-claim-is-published-atomically-compact", object())
    for index in range(1000):
        assert schedule_retry(
            key,
            lambda value=index: value,
            delay_seconds=3600,
            retained_bytes=1024,
        )
    snapshot = retry_scheduler_snapshot()
    assert snapshot.heap_entries <= 64
    cancel_retry(key)


def test_retry_worker_start_rollback_has_autonomous_guardian(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify retry worker start rollback has autonomous guardian."""
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    released = threading.Event()

    class Lease:
        calls = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.calls += 1
            if self.calls == 1:
                raise OSError("transient release")
            released.set()

    lease = Lease()
    monkeypatch.setattr(scheduler, "_acquire_worker_lease", lambda: lease)
    real_thread = threading.Thread

    def thread_factory(*args: Any, **kwargs: Any) -> threading.Thread:
        """Construct the controlled worker thread and record publication order."""
        worker = real_thread(*args, **kwargs)
        if kwargs.get("name") == "schema-sanitizer-retry-timer":
            worker.start = lambda: (_ for _ in ()).throw(RuntimeError("start failed"))
        return worker

    monkeypatch.setattr(module.threading, "Thread", thread_factory)
    scheduler._start_timer_worker()
    assert released.wait(SCHEDULER_TIMEOUT_SECONDS)
    deadline = time.monotonic() + SCHEDULER_TIMEOUT_SECONDS
    guardian = module._RELEASE_GUARDIAN
    while time.monotonic() < deadline:
        with guardian._condition:
            if id(lease) not in guardian._items:
                break
        time.sleep(0.01)
    with guardian._condition:
        assert id(lease) not in guardian._items
    assert scheduler.snapshot().failed_worker_leases == 0


def test_retry_timer_is_not_blocked_by_one_callback() -> None:
    """Verify retry timer is not blocked by one callback."""
    from schema_sanitizer.core_impl.retry_scheduler import schedule_retry

    blocked = threading.Event()
    release = threading.Event()
    second = threading.Event()

    def first_callback() -> None:
        """Run the first callback while replacement publication is blocked."""
        blocked.set()
        assert release.wait(SCHEDULER_TIMEOUT_SECONDS)

    assert schedule_retry(
        ("external-claim-is-published-atomically-exec", 1), first_callback, delay_seconds=0
    )
    assert schedule_retry(
        ("external-claim-is-published-atomically-exec", 2), second.set, delay_seconds=0.02
    )
    assert blocked.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert second.wait(SCHEDULER_TIMEOUT_SECONDS)
    release.set()


def test_partial_claim_release_does_not_restore_path_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify partial claim release does not restore path authority."""
    import schema_sanitizer.core_impl.path_identity as module

    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path / "coord"))
    monkeypatch.setattr(module, "_set_new_owner_marker", lambda *_args: False)
    path = tmp_path / "artifact"
    path.write_text("x")
    first = module.claim_path_identity(path)
    assert first is not None and first.claim_owner is not None
    descriptor_owner = first.claim_owner.descriptor_owner
    assert descriptor_owner is not None and descriptor_owner.fd_lease is not None
    real_release = descriptor_owner.fd_lease.release
    failed = False

    def fail_once() -> None:
        """Inject the once failure at the controlled test point."""
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("lease cleanup")
        real_release()

    monkeypatch.setattr(descriptor_owner.fd_lease, "release", fail_once)
    with pytest.raises(OSError, match="lease cleanup"):
        module.release_path_identity(first)
    assert not first.owns_claim
    second = module.claim_path_identity(path)
    assert second is not None
    module.release_path_identity(second)
    module.release_path_identity(first)


def test_failed_stale_delete_retains_claim_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify failed stale delete retains claim owner."""
    from schema_sanitizer.core_impl.temporary_janitor import (
        _TemporaryArtifactJanitor,
    )

    root = tmp_path / "quarantine"
    root.mkdir()
    leftover = root / "artifact-stale"
    leftover.write_text("x")
    janitor = _TemporaryArtifactJanitor()
    janitor.root = lambda: root  # type: ignore[method-assign]
    monkeypatch.setattr(
        janitor,
        "_delete_owned",
        lambda path, _is_dir, identity=None: (False, path, identity, False),
    )
    janitor._scan_stale()
    assert janitor.snapshot().pending_artifacts == 1
    artifact = next(iter(janitor._pending.values()))
    assert artifact.identity is not None and artifact.identity.owns_claim
    from schema_sanitizer.core_impl.path_identity import release_path_identity

    release_path_identity(artifact.identity)
    janitor._pending.clear()


def test_bridge_drain_reaches_tasks_spawned_from_finally(
    native_stub: None,
) -> None:
    """Verify bridge drain reaches tasks spawned from finally."""
    from schema_sanitizer.remote_impl.async_bridge import _BridgeRunner

    started = threading.Event()
    child_started = threading.Event()
    child_finally = threading.Event()

    class Lease:
        def release(self) -> None:
            """Release the resource held by the lease test double."""
            return None

    async def child() -> None:
        """Run the child-side operation in the controlled lifecycle."""
        child_started.set()
        try:
            await asyncio.sleep(3600)
        finally:
            child_finally.set()

    async def operation() -> None:
        """Run the controlled operation under test."""
        started.set()
        try:
            await asyncio.sleep(3600)
        finally:
            asyncio.create_task(child())

    runner = _BridgeRunner(operation(), copy_context(), Lease())
    runner.start()
    assert started.wait(SCHEDULER_TIMEOUT_SECONDS)
    runner.cancel()
    join_thread_or_fail(runner._thread)
    assert child_started.is_set()
    assert child_finally.is_set()


def test_path_claim_owner_finalizer_eventually_removes_external_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify path claim owner finalizer eventually removes external claim."""
    import schema_sanitizer.core_impl.path_identity as module

    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path / "coord"))
    monkeypatch.setattr(module, "_set_new_owner_marker", lambda *_args: False)
    path = tmp_path / "artifact"
    path.write_text("x")
    identity = module.claim_path_identity(path)
    assert identity is not None and identity.external_claim_path is not None
    claim = Path(identity.external_claim_path)
    del identity
    gc.collect()
    deadline = time.monotonic() + SCHEDULER_TIMEOUT_SECONDS
    while claim.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert not claim.exists()
