from __future__ import annotations

import asyncio
import gc
import sys
import threading
import time
from contextvars import copy_context
from pathlib import Path
from typing import Any

import pytest

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


def test_external_claim_is_published_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
        nonlocal calls
        with calls_lock:
            calls += 1
            first = calls == 1
        if first:
            entered.set()
            assert release.wait(3)
        original(descriptor, payload)

    monkeypatch.setattr(module, "_write_claim_payload", blocked_write)
    outcomes: list[object] = []

    def claim() -> None:
        try:
            outcomes.append(module.claim_path_identity(path))
        except BaseException as exc:
            outcomes.append(exc)

    first = threading.Thread(target=claim)
    second = threading.Thread(target=claim)
    first.start()
    assert entered.wait(2)
    second.start()
    second.join(2)
    release.set()
    first.join(2)
    assert not first.is_alive() and not second.is_alive()
    owners = [value for value in outcomes if isinstance(value, module.PathIdentity)]
    errors = [value for value in outcomes if isinstance(value, BaseException)]
    assert len(owners) == 1
    assert len(errors) == 1
    assert "already owned" in str(errors[0])
    module.release_path_identity(owners[0])


def test_retry_cancel_reschedule_never_reuses_old_generation() -> None:
    from schema_sanitizer.core_impl.retry_scheduler import (
        cancel_retry,
        schedule_retry,
    )

    key = ("pass35-aba", object())
    calls: list[str] = []
    assert schedule_retry(key, lambda: calls.append("old"), delay_seconds=0.03)
    cancel_retry(key)
    assert schedule_retry(key, lambda: calls.append("new"), delay_seconds=0.25)
    time.sleep(0.1)
    assert calls == []
    cancel_retry(key)


def test_retry_heap_compacts_replaced_payloads() -> None:
    from schema_sanitizer.core_impl.retry_scheduler import (
        cancel_retry,
        retry_scheduler_snapshot,
        schedule_retry,
    )

    key = ("pass35-compact", object())
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
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    released = threading.Event()

    class Lease:
        calls = 0

        def release(self) -> None:
            self.calls += 1
            if self.calls == 1:
                raise OSError("transient release")
            released.set()

    lease = Lease()
    monkeypatch.setattr(scheduler, "_acquire_worker_lease", lambda: lease)
    real_thread = threading.Thread

    def thread_factory(*args: Any, **kwargs: Any) -> threading.Thread:
        worker = real_thread(*args, **kwargs)
        if kwargs.get("name") == "schema-sanitizer-retry-timer":
            worker.start = lambda: (_ for _ in ()).throw(RuntimeError("start failed"))
        return worker

    monkeypatch.setattr(module.threading, "Thread", thread_factory)
    scheduler._start_timer_worker()
    assert released.wait(2)
    deadline = time.monotonic() + 2
    guardian = module._RELEASE_GUARDIAN
    while time.monotonic() < deadline:
        with guardian._condition:
            if id(lease) not in guardian._owner_index:
                break
        time.sleep(0.01)
    with guardian._condition:
        assert id(lease) not in guardian._owner_index
    assert scheduler.snapshot().failed_worker_leases == 0


def test_retry_timer_is_not_blocked_by_one_callback() -> None:
    from schema_sanitizer.core_impl.retry_scheduler import schedule_retry

    blocked = threading.Event()
    release = threading.Event()
    second = threading.Event()

    def first_callback() -> None:
        blocked.set()
        assert release.wait(3)

    assert schedule_retry(("pass35-exec", 1), first_callback, delay_seconds=0)
    assert schedule_retry(("pass35-exec", 2), second.set, delay_seconds=0.02)
    assert blocked.wait(2)
    assert second.wait(2)
    release.set()


def test_partial_claim_release_does_not_restore_path_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    from schema_sanitizer.core_impl.temporary_janitor import (
        _TemporaryArtifactJanitor,
    )

    root = tmp_path / "quarantine"
    root.mkdir()
    leftover = root / "artifact-stale"
    leftover.write_text("x")
    janitor = _TemporaryArtifactJanitor()
    janitor.root = lambda: root  # type: ignore[method-assign]
    monkeypatch.setattr(janitor, "_delete", lambda *_args, **_kwargs: False)
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
    from schema_sanitizer.remote_impl.async_bridge import _BridgeRunner

    started = threading.Event()
    child_started = threading.Event()
    child_finally = threading.Event()

    class Lease:
        def release(self) -> None:
            return None

    async def child() -> None:
        child_started.set()
        try:
            await asyncio.sleep(3600)
        finally:
            child_finally.set()

    async def operation() -> None:
        started.set()
        try:
            await asyncio.sleep(3600)
        finally:
            asyncio.create_task(child())

    runner = _BridgeRunner(operation(), copy_context(), Lease())
    runner.start()
    assert started.wait(2)
    runner.cancel()
    runner._thread.join(2)
    assert not runner._thread.is_alive()
    assert child_started.is_set()
    assert child_finally.is_set()


def test_path_claim_owner_finalizer_eventually_removes_external_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    deadline = time.monotonic() + 3
    while claim.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert not claim.exists()


def test_pass35_source_contracts() -> None:
    root = Path(__file__).resolve().parents[2]
    retry = (root / "src/schema_sanitizer/core_impl/retry_scheduler.py").read_text()
    path_identity = (root / "src/schema_sanitizer/core_impl/path_identity.py").read_text()
    assert "_next_token_locked" in retry
    assert "discard_payload" in retry
    assert "_compact_heap_locked" in retry
    assert "schema-sanitizer-retry-timer" in retry
    assert "schema-sanitizer-retry-executor" in retry
    assert "schema-sanitizer-retry-lease-guardian" in retry
    assert "_MAX_EMERGENCY_BYTES" in retry
    assert "os.link(" in path_identity
    assert ".claim-write-" in path_identity
    assert "authority_released" in path_identity
    assert "_CLAIM_SWEEP_ITERATOR" in path_identity
    header = (root / "cpp/src/internal/runtime/operation_task_arena.hh").read_text()
    source = (root / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    assert "class TaskMemoryLease" in header
    assert "SubmitLeased" in header and "SubmitLeased" in source
    assert "kLaneCount = 2U" in source
    assert "reaper_queued_bytes" in header
    assert "post_shutdown_retained_bytes" in header
