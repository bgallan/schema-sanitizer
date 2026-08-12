"""Regression coverage for memory coordinator waiters are scoped to the live close generation."""

from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from concurrent.futures import Future
from pathlib import Path
from threading import Event, Lock, RLock, Thread
from types import SimpleNamespace
from typing import Any

import pytest

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="POSIX descriptor-relative filesystem hardening suite"
)

_NATIVE_STUB_MODULES = (
    "schema_sanitizer.core_impl.native_options",
    "schema_sanitizer.core_impl.execution_policy",
    "schema_sanitizer.api_impl.operation_context",
    "schema_sanitizer.api_impl.input.directory_preparation",
    "schema_sanitizer.api_impl.source_plan.attached",
    "schema_sanitizer.api_impl.source_plan.remote",
)


class _DeadThread:
    ident = None

    def is_alive(self) -> bool:
        return False

    def join(self, timeout: float | None = None) -> None:
        return None


class _GenerationRelease:
    def __init__(self) -> None:
        self.calls = 0
        self.recovery_entered = Event()
        self.allow_recovery = Event()

    def release(self) -> None:
        self.calls += 1
        if self.calls == 1:
            raise OSError("first generation failed")
        if self.calls == 2:
            self.recovery_entered.set()
            assert self.allow_recovery.wait(2)


def test_coordinator_waiters_are_scoped_to_the_live_close_generation() -> None:
    from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator

    owner = _GenerationRelease()
    coordinator = object.__new__(RemoteIoCoordinator)
    coordinator._pid = os.getpid()
    coordinator._lock = Lock()
    coordinator._release_lock = Lock()
    coordinator._closed = False
    coordinator._closing = False
    coordinator._close_complete = Event()
    coordinator._close_error = None
    coordinator._shutdown_timeout_seconds = 1.0
    coordinator._loop = None
    coordinator._futures = set()
    coordinator._failed_submissions = deque()
    coordinator._permit_registration = owner
    coordinator._thread_lease = None
    coordinator._thread = _DeadThread()
    coordinator._protocol_violations = 0

    with pytest.raises(OSError, match="first generation"):
        coordinator.close()

    errors: list[BaseException] = []

    def close_once() -> None:
        try:
            coordinator.close()
        except BaseException as exc:
            errors.append(exc)

    recovery = Thread(target=close_once)
    recovery.start()
    assert owner.recovery_entered.wait(1)
    waiter = Thread(target=close_once)
    waiter.start()
    time.sleep(0.03)
    assert waiter.is_alive(), "waiter observed the previous generation's event"
    owner.allow_recovery.set()
    recovery.join(1)
    waiter.join(1)
    assert not errors
    assert coordinator._permit_registration is None


class _BlockingRetryLease:
    def __init__(self) -> None:
        self.calls = 0
        self.first_entered = Event()
        self.allow_first = Event()
        self.released = False

    def release(self) -> None:
        self.calls += 1
        if self.calls == 1:
            self.first_entered.set()
            assert self.allow_first.wait(2)
            raise OSError("callback rollback failed")
        self.released = True


def test_prefetch_close_waits_for_cleanup_callbacks_before_commit(
    native_stub: None,
) -> None:
    from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator

    submitted: Future[Any] = Future()

    class Coordinator:
        shutdown_timeout_seconds = 1.0

        def submit(self, *_args: Any, **_kwargs: Any) -> Future[Any]:
            return submitted

    iterator = object.__new__(RemoteChunkPrefetchIterator)
    iterator._pid = os.getpid()
    iterator._manifest = SimpleNamespace()
    iterator._coordinator = Coordinator()
    iterator._owns_coordinator = False
    iterator._download_session = None
    iterator._session_closer = None
    iterator._close_lock = RLock()
    iterator._close_started = False
    iterator._closed = False
    iterator._failed_storage_leases = deque()
    iterator._futures = deque()
    iterator._remote_timeout_seconds = 1.0

    lease = _BlockingRetryLease()
    future = iterator._submit_stage(0, lease)
    iterator._futures.append(future)
    setter = Thread(target=lambda: future.set_exception(RuntimeError("stage failed")))
    setter.start()
    assert lease.first_entered.wait(1)

    close_errors: list[BaseException] = []

    def close_iterator() -> None:
        try:
            iterator.close()
        except BaseException as exc:
            close_errors.append(exc)

    closer = Thread(target=close_iterator)
    closer.start()
    time.sleep(0.03)
    assert closer.is_alive()
    assert not iterator._closed
    lease.allow_first.set()
    setter.join(1)
    closer.join(1)
    assert not close_errors
    assert lease.released
    assert iterator._closed
    assert not iterator._failed_storage_leases


def test_staged_directory_owner_unlinks_dangling_symlink(tmp_path: Path) -> None:
    from schema_sanitizer.remote_impl.staging_paths import StagedPath

    target = tmp_path / "missing-target"
    link = tmp_path / "staged-directory"
    link.symlink_to(target, target_is_directory=True)

    class Lease:
        released = False

        def release(self) -> None:
            self.released = True

    lease = Lease()
    staged = StagedPath(str(link), is_dir=True, storage_lease=lease)
    staged.close()
    assert not os.path.lexists(link)
    assert lease.released
    assert staged._closed


def test_staged_directory_never_follows_substituted_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.remote_impl import staging_paths as module

    staged_path = tmp_path / "staged-directory"
    staged_path.mkdir()
    external = tmp_path / "external"
    external.mkdir()
    marker = external / "keep.txt"
    marker.write_text("keep", encoding="utf-8")

    class Lease:
        released = False

        def release(self) -> None:
            self.released = True

    lease = Lease()

    def substitute_with_symlink(path: Path) -> None:
        path.rmdir()
        path.symlink_to(external, target_is_directory=True)
        raise OSError("entry substituted")

    monkeypatch.setattr(module.shutil, "rmtree", substitute_with_symlink)

    staged = module.StagedPath(str(staged_path), is_dir=True, storage_lease=lease)
    with pytest.raises(OSError, match="replaced during cleanup"):
        staged.close()
    assert marker.read_text(encoding="utf-8") == "keep"
    assert not os.path.lexists(staged_path)
    retained_path = Path(staged.path)
    assert os.path.lexists(retained_path)
    assert retained_path.is_symlink()
    assert not lease.released
    assert not staged._closed


def test_cancelled_session_bridge_is_not_resubmitted_until_task_terminal() -> None:
    from schema_sanitizer.remote_impl.session_lifecycle import SharedDownloadSessionCloser

    cancelled: Future[Any] = Future()
    cancelled.cancel()

    class Coordinator:
        def __init__(self) -> None:
            self.submissions = 0

        def submit(self, _operation: Any) -> Future[Any]:
            self.submissions += 1
            return cancelled if self.submissions == 1 else Future()

    coordinator = Coordinator()
    closer = SharedDownloadSessionCloser(coordinator, object(), ())
    assert not closer.close(timeout_seconds=0.001)
    assert not closer.close(timeout_seconds=0.001)
    assert coordinator.submissions == 1
    assert closer._attempt is not None
    closer._attempt.terminal.set()
    assert closer.close(timeout_seconds=0.001)
    assert coordinator.submissions == 1
    assert closer.close(timeout_seconds=0.001)


def test_cancelled_session_bridge_retries_after_real_task_cancellation() -> None:
    from schema_sanitizer.remote_impl.session_lifecycle import SharedDownloadSessionCloser

    cancelled: Future[Any] = Future()
    cancelled.cancel()

    class Coordinator:
        def __init__(self) -> None:
            self.submissions = 0

        def submit(self, _operation: Any) -> Future[Any]:
            self.submissions += 1
            return cancelled if self.submissions == 1 else Future()

    coordinator = Coordinator()
    closer = SharedDownloadSessionCloser(coordinator, object(), ())
    assert not closer.close(timeout_seconds=0.001)
    assert closer._attempt is not None
    closer._attempt.error = asyncio.CancelledError()
    closer._attempt.terminal.set()
    assert not closer.close(timeout_seconds=0.001)
    assert coordinator.submissions == 1
    assert not closer.close(timeout_seconds=0.001)
    assert coordinator.submissions == 2


def test_permit_release_never_samples_system_pressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl import io_permits as module

    governor = module.RemoteIoPermitGovernor(4)
    governor._in_use = 1
    monkeypatch.setattr(
        module,
        "system_pressure_snapshot",
        lambda: (_ for _ in ()).throw(AssertionError("release sampled pressure")),
    )
    governor._release(1)
    assert governor._in_use == 0


def test_pressure_refresh_is_single_flight_and_nonblocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import system_pressure as module

    module._prepare_pressure_for_fork()
    module._reset_after_fork()
    entered = Event()
    release = Event()

    def blocked_psi(_path: Path) -> tuple[float | None, float | None]:
        entered.set()
        assert release.wait(2)
        return None, None

    monkeypatch.setattr(module, "_parse_psi", blocked_psi)
    # This test exercises PSI refresh single-flight semantics only. Isolate it
    # from real cgroup pressure on the host so unrelated memory load cannot
    # change the cached scale while the refresher is deliberately blocked.
    monkeypatch.setattr(module, "_cgroup_events", lambda: (0, 0))
    monkeypatch.setattr(module, "_cgroup_usage_ratio", lambda: None)
    refresher = Thread(target=lambda: module.system_pressure_snapshot(refresh=True))
    refresher.start()
    assert entered.wait(1)
    started = time.monotonic()
    snapshot = module.system_pressure_snapshot(refresh=True)
    assert time.monotonic() - started < 0.1
    assert snapshot.scale == 1.0
    release.set()
    refresher.join(1)
    assert not refresher.is_alive()


def test_pressure_refresh_releases_claim_for_control_flow_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import system_pressure as module

    class StopSampling(BaseException):
        pass

    module._prepare_pressure_for_fork()
    module._reset_after_fork()
    monkeypatch.setattr(module, "_parse_psi", lambda _path: (_ for _ in ()).throw(StopSampling()))
    with pytest.raises(StopSampling):
        module.system_pressure_snapshot(refresh=True)
    assert not module._refreshing


def test_output_upload_error_remains_primary_when_cleanup_fails(
    native_stub: None,
) -> None:
    from schema_sanitizer.remote_impl.staging import finalize_output_target

    class Target:
        remote_uri = "memory://output"
        temp = None
        threading_mode = "single"
        operation_context = None
        memory_limit_bytes = None
        local_path = "unused"

        def close(self) -> None:
            raise OSError("cleanup failed")

    def fail_before_upload() -> None:
        raise ValueError("upload preparation failed")

    with pytest.raises(ValueError, match="upload preparation failed") as caught:
        finalize_output_target(Target(), before_remote_upload=fail_before_upload)
    assert any("cleanup failed" in note for note in caught.value.__notes__)


def test_session_close_zero_timeout_is_a_nonblocking_poll() -> None:
    from schema_sanitizer.remote_impl.session_lifecycle import SharedDownloadSessionCloser

    pending: Future[Any] = Future()

    class Coordinator:
        def submit(self, _operation: Any) -> Future[Any]:
            return pending

    closer = SharedDownloadSessionCloser(Coordinator(), object(), ())
    started = time.monotonic()
    assert not closer.close(timeout_seconds=0.0)
    assert time.monotonic() - started < 0.1
    pending.cancel()
