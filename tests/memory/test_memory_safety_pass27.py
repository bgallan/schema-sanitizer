"""Regression tests for pass27 reboot identity and retryable teardown."""

from __future__ import annotations

import asyncio
import os
import stat
import sys
from collections import deque
from concurrent.futures import Future
from pathlib import Path
from threading import Lock
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
    "schema_sanitizer.api_impl.source_plan.remote_cleanup",
    "schema_sanitizer.api_impl.source_plan.remote_runtime.provider",
    "schema_sanitizer.api_impl.source_plan.remote_runtime",
    "schema_sanitizer.api_impl.source_plan.remote",
    "schema_sanitizer.pipeline.partition_lookahead",
)


def _purge_module(name: str) -> None:
    """Remove one module and its cached parent-package attribute."""
    sys.modules.pop(name, None)
    parent_name, _, attribute = name.rpartition(".")
    parent = sys.modules.get(parent_name)
    if parent is not None and hasattr(parent, attribute):
        delattr(parent, attribute)


@pytest.fixture
def native_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide isolated import-time native metadata for Python-only lifecycle tests."""
    from schema_sanitizer.core_impl import native_runtime

    class Stub:
        """Minimal native metadata provider."""

        def options_catalog(self) -> tuple[object, ...]:
            """Return an empty option catalog."""
            return ()

        def __getattr__(self, _name: str) -> Any:
            """Return no-op native entry points."""
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


class _FailingClose:
    """Close owner that commits only after a configured number of failures."""

    def __init__(self, failures: int = 1) -> None:
        """Initialize one bounded sequence of cleanup failures."""
        self.failures = failures
        self.calls = 0
        self.closed = False

    def close(self) -> None:
        """Fail until the configured retry ordinal and then commit."""
        self.calls += 1
        if self.calls <= self.failures:
            raise OSError("transient cleanup failure")
        self.closed = True


class _FailingContext:
    """Operation context double with retryable close."""

    def __init__(self, failures: int = 1) -> None:
        """Initialize one retryable context."""
        self.owner = _FailingClose(failures)

    def close(self) -> None:
        """Delegate close to the retryable owner."""
        self.owner.close()


def test_process_identity_includes_linux_boot_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """New process tokens distinguish identical PID start ticks across reboots."""
    from schema_sanitizer.core_impl import process_identity as module

    suffix = ["S", *("0" for _ in range(18)), "12345", "0"]
    stat_text = f"42 (worker) {' '.join(suffix)}"

    def read_text(path: Path, **_kwargs: object) -> str:
        """Return deterministic procfs identities."""
        if str(path) == "/proc/42/stat":
            return stat_text
        if path == module._BOOT_ID_PATH:
            return "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\n"
        raise OSError("unexpected path")

    monkeypatch.setattr(module.Path, "read_text", read_text)
    assert module.process_start_token(42) == ("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:12345")


def test_process_identity_comparison_supports_legacy_but_rejects_reboot() -> None:
    """Rolling deployments accept start-only tokens while new boot IDs are strict."""
    from schema_sanitizer.core_impl.process_identity import process_identity_matches

    assert process_identity_matches("12345", "boot-a:12345")
    assert process_identity_matches("boot-a:12345", "12345")
    assert process_identity_matches("boot-a:12345", "boot-a:12345")
    assert not process_identity_matches("boot-a:12345", "boot-b:12345")
    assert process_identity_matches("unknown", "boot-b:12345")


def test_cross_process_alive_rejects_same_tick_from_another_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persistent coordination state is not attributed to a rebooted PID twin."""
    from schema_sanitizer.core_impl import cross_process_memory as module

    monkeypatch.setattr(module.os, "kill", lambda *_args: None)
    monkeypatch.setattr(module, "_process_start_token", lambda _pid: "boot-b:77")
    assert not module._process_alive(123, "boot-a:77")
    assert module._process_alive(123, "77")


def test_janitor_retries_lease_release_after_artifact_deletion() -> None:
    """A transient journal failure cannot orphan an already-deleted artifact lease."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    class Lease:
        def __init__(self) -> None:
            self.calls = 0

        def release(self) -> None:
            self.calls += 1
            if self.calls == 1:
                raise OSError("journal busy")

    janitor = module._TemporaryArtifactJanitor()
    lease = Lease()
    key = "gone"
    artifact = module._PendingArtifact(Path(key), False, lease)
    janitor._pending[key] = artifact
    janitor._pending_order.append(key)
    janitor._delete = lambda *_args: True  # type: ignore[method-assign]

    janitor.sweep()
    assert lease.calls == 1
    assert janitor.snapshot().pending_artifacts == 1
    assert list(janitor._pending_order) == [key]

    janitor.sweep()
    assert lease.calls == 2
    assert janitor.snapshot().pending_artifacts == 0
    assert janitor.snapshot().deleted_artifacts == 1


def test_janitor_retains_thread_permit_after_failed_retirement() -> None:
    """A failed permit return remains owned and is retried before replacement."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    class Lease:
        def __init__(self) -> None:
            self.calls = 0

        def release(self) -> None:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("governor unavailable")

    janitor = module._TemporaryArtifactJanitor()
    lease = Lease()
    janitor._thread = __import__("threading").current_thread()
    janitor._thread_lease = lease
    with janitor._lock:
        janitor._retire_current_thread_locked()
    assert janitor._thread is None
    assert janitor._thread_lease is lease

    janitor._closed = True
    with janitor._lock:
        janitor._ensure_thread_locked()
    assert lease.calls == 1
    janitor._closed = False
    with janitor._lock:
        janitor._ensure_thread_locked()
    assert lease.calls == 2
    assert janitor._thread_lease is None or janitor._thread is not None
    janitor.close()


def test_staged_result_ownership_retries_failed_abandon_cleanup() -> None:
    """An abandoned staged result remains reachable until close succeeds."""
    from schema_sanitizer.remote_impl.io_coordinator import StagedResultOwnership

    staged = _FailingClose(1)
    ownership = StagedResultOwnership()
    ownership.publish(staged)
    assert not ownership.abandon()
    assert ownership._staged is staged
    assert ownership.abandon()
    assert ownership._staged is None
    assert staged.closed


def test_staged_result_published_after_abandon_retains_failed_cleanup() -> None:
    """Late future completion cannot drop a staged result whose close failed."""
    from schema_sanitizer.remote_impl.io_coordinator import StagedResultOwnership

    staged = _FailingClose(1)
    ownership = StagedResultOwnership()
    assert ownership.abandon()
    assert ownership.publish(staged) is None
    assert ownership._staged is staged
    assert ownership.abandon()
    assert staged.closed


def _bare_lookahead(module: Any, future: Future[Any], context: Any, executor: Any) -> Any:
    """Construct one lookahead owner without starting project threads."""
    owner = object.__new__(module.PartitionSourceLookahead)
    owner._pid = os.getpid()
    owner._close_lock = Lock()
    owner._close_started = False
    owner._late_close_registered = False
    owner._closed = False
    owner.enabled = True
    owner._armed = None
    owner._future = future
    owner._future_context = context
    owner._executor = executor
    return owner


def test_partition_lookahead_close_retains_cancelled_context_on_failure(
    native_stub: None,
) -> None:
    """Cancelled speculative work keeps its child context until close commits."""
    from schema_sanitizer.pipeline import partition_lookahead as module

    class Executor:
        def __init__(self) -> None:
            self.calls = 0

        def shutdown(self, **_kwargs: object) -> None:
            self.calls += 1

    future: Future[Any] = Future()
    context = _FailingContext(1)
    executor = Executor()
    owner = _bare_lookahead(module, future, context, executor)

    with pytest.raises(OSError, match="transient"):
        owner.close()
    assert owner._future is future
    assert owner._future_context is context
    assert owner._executor is executor
    assert not owner._closed

    owner.close()
    assert owner._future is None
    assert owner._future_context is None
    assert owner._executor is None
    assert owner._closed


def test_partition_lookahead_late_completion_finishes_close(
    native_stub: None,
) -> None:
    """A running future retains the controller until its callback drains ownership."""
    from schema_sanitizer.pipeline import partition_lookahead as module

    class Executor:
        def shutdown(self, **_kwargs: object) -> None:
            return

    prepared = _FailingClose(0)
    future: Future[Any] = Future()
    future.set_running_or_notify_cancel()
    owner = _bare_lookahead(module, future, _FailingContext(0), Executor())
    owner.close()
    assert not owner._closed
    assert owner._future is future

    future.set_result(prepared)
    assert owner._closed
    assert owner._future is None
    assert prepared.closed


def test_partition_take_next_keeps_failed_exception_context_for_retry(
    native_stub: None,
) -> None:
    """A failed future does not orphan its child context when context close fails."""
    from schema_sanitizer.pipeline import partition_lookahead as module

    future: Future[Any] = Future()
    future.set_exception(ValueError("prepare failed"))
    context = _FailingContext(1)
    owner = object.__new__(module.PartitionSourceLookahead)
    owner.enabled = True
    owner._future = future
    owner._future_context = context
    owner._current_options = lambda: object()

    with pytest.raises(ValueError, match="prepare failed") as caught:
        owner.take_next(object())
    assert any("transient cleanup failure" in note for note in caught.value.__notes__)
    assert owner._future is future
    assert owner._future_context is context

    with pytest.raises(ValueError, match="prepare failed"):
        owner.take_next(object())
    assert owner._future is None
    assert owner._future_context is None


def test_shared_session_closer_reuses_one_future_across_timeouts() -> None:
    """Bounded close retries never schedule duplicate async exits while one is live."""
    from schema_sanitizer.remote_impl.session_lifecycle import (
        SharedDownloadSessionCloser,
    )

    close_future: Future[Any] = Future()

    class Coordinator:
        def __init__(self) -> None:
            self.submissions = 0

        def submit(self, _operation: Any) -> Future[Any]:
            self.submissions += 1
            return close_future

    coordinator = Coordinator()
    closer = SharedDownloadSessionCloser(coordinator, object(), ())
    assert not closer.close(timeout_seconds=0.001)
    assert not closer.close(timeout_seconds=0.001)
    assert coordinator.submissions == 1
    close_future.set_result(None)
    assert closer.close(timeout_seconds=0.001)
    assert coordinator.submissions == 1


def test_shared_session_closer_resubmits_after_exit_failure() -> None:
    """A failed async exit remains retryable instead of pinning a failed Future."""
    from schema_sanitizer.remote_impl.session_lifecycle import (
        SharedDownloadSessionCloser,
    )

    class Session:
        def __init__(self) -> None:
            self.calls = 0

        async def __aexit__(self, *_exc: object) -> None:
            self.calls += 1
            if self.calls == 1:
                raise OSError("exit failed")

    class Coordinator:
        def __init__(self) -> None:
            self.submissions = 0

        def submit(self, operation: Any) -> Future[Any]:
            self.submissions += 1
            future: Future[Any] = Future()
            try:
                asyncio.run(operation(None))
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(None)
            return future

    session = Session()
    coordinator = Coordinator()
    closer = SharedDownloadSessionCloser(coordinator, session, ())
    with pytest.raises(OSError, match="exit failed"):
        closer.close(timeout_seconds=1)
    assert closer.close(timeout_seconds=1)
    assert coordinator.submissions == 2
    assert session.calls == 2


def _bare_remote_iterator(module: Any) -> Any:
    """Construct a remote prefetch owner without starting an event loop."""
    iterator = object.__new__(module.RemoteChunkPrefetchIterator)
    iterator._pid = os.getpid()
    iterator._close_lock = Lock()
    iterator._close_started = False
    iterator._session_closer = None
    iterator._closed = False
    iterator._futures = deque()
    iterator._coordinator = None
    iterator._owns_coordinator = False
    iterator._download_session = None
    return iterator


def test_remote_prefetch_close_retains_session_across_timeout(
    native_stub: None,
) -> None:
    """A bounded session timeout leaves all owners reachable for a later retry."""
    from schema_sanitizer.api_impl.source_plan import remote as module

    close_future: Future[Any] = Future()

    class Coordinator:
        shutdown_timeout_seconds = 0.001

        def __init__(self) -> None:
            self.submissions = 0

        def submit(self, _operation: Any) -> Future[Any]:
            self.submissions += 1
            return close_future

    coordinator = Coordinator()
    session = object()
    iterator = _bare_remote_iterator(module)
    iterator._coordinator = coordinator
    iterator._download_session = session

    iterator.close()
    assert not iterator._closed
    assert iterator._coordinator is coordinator
    assert iterator._download_session is session
    assert coordinator.submissions == 1

    close_future.set_result(None)
    iterator.close()
    assert iterator._closed
    assert iterator._coordinator is None
    assert iterator._download_session is None
    assert coordinator.submissions == 1


def test_remote_prefetch_close_retains_failed_staged_result(
    native_stub: None,
) -> None:
    """A failed staged close keeps its future in the iterator for explicit retry."""
    from schema_sanitizer.api_impl.source_plan import remote as module
    from schema_sanitizer.remote_impl.io_coordinator import StagedResultOwnership

    staged = _FailingClose(2)
    ownership = StagedResultOwnership()
    future: Future[Any] = Future()
    future.set_result(ownership.publish(staged))
    setattr(future, "_schema_sanitizer_staged_ownership", ownership)
    iterator = _bare_remote_iterator(module)
    iterator._futures.append(future)

    with pytest.raises(RuntimeError, match="remains retryable"):
        iterator.close()
    assert list(iterator._futures) == [future]
    assert not iterator._closed

    iterator.close()
    assert iterator._closed
    assert not iterator._futures
    assert staged.closed


def test_remote_prefetch_close_retains_owned_coordinator_on_failure(
    native_stub: None,
) -> None:
    """Coordinator ownership is cleared only after its close commits."""
    from schema_sanitizer.api_impl.source_plan import remote as module

    coordinator = _FailingClose(1)
    coordinator.shutdown_timeout_seconds = 0.1
    iterator = _bare_remote_iterator(module)
    iterator._coordinator = coordinator
    iterator._owns_coordinator = True

    with pytest.raises(OSError, match="transient"):
        iterator.close()
    assert iterator._coordinator is coordinator
    assert iterator._owns_coordinator
    assert not iterator._closed

    iterator.close()
    assert iterator._coordinator is None
    assert not iterator._owns_coordinator
    assert iterator._closed


def test_janitor_stale_scan_is_bounded_and_interleavable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Crash-leftover discovery yields after one bounded batch."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    stale = tmp_path / "quarantine"
    stale.mkdir()
    for index in range(module._SWEEP_BATCH_SIZE + 7):
        (stale / f"artifact-stale-{index}").write_text("x")

    janitor = module._TemporaryArtifactJanitor()
    monkeypatch.setattr(janitor, "root", lambda: stale)
    deleted: list[Path] = []

    def delete(path: Path, _is_dir: bool, _identity: object | None = None) -> bool:
        """Record one bounded stale deletion."""
        deleted.append(path)
        path.unlink(missing_ok=True)
        return True

    monkeypatch.setattr(janitor, "_delete", delete)
    janitor._scan_stale()
    assert len(deleted) == module._SWEEP_BATCH_SIZE
    assert not janitor._scanned

    # A live pending owner can be swept between stale-directory batches.
    class Lease:
        released = False

        def release(self) -> None:
            self.released = True

    live_path = tmp_path / "live"
    live_path.write_text("x")
    lease = Lease()
    key = str(live_path)
    janitor._pending[key] = module._PendingArtifact(live_path, False, lease)
    janitor._pending_order.append(key)
    janitor._sweep_cycle()
    assert lease.released
    assert janitor.snapshot().pending_artifacts == 0

    janitor._scan_stale()
    stale_deleted = [path for path in deleted if path.parent == stale]
    assert len(stale_deleted) == module._SWEEP_BATCH_SIZE + 7
    assert janitor._scanned


def test_remote_prefetch_close_start_blocks_new_submissions(
    native_stub: None,
) -> None:
    """A close attempt prevents a concurrent refill from admitting new work."""
    from schema_sanitizer.api_impl.source_plan import remote as module

    class Manifest:
        files = (object(),)
        chunk_size = 1

        def stage_chunk(self, _start: int) -> object:
            raise AssertionError("close-started iterator submitted new work")

    iterator = _bare_remote_iterator(module)
    iterator._manifest = Manifest()
    iterator._policy = type("Policy", (), {"async_concurrency": 1})()
    iterator._prefetch_chunks = 1
    iterator._io_chunk_bytes = 1
    iterator._next_start = 0
    iterator._close_started = True
    iterator._fill_prefetch_window()
    assert not iterator._futures
    assert iterator._next_start == 0


def test_shared_session_closer_drops_coordinator_after_commit() -> None:
    """A completed session close releases the event-loop owner graph."""
    from schema_sanitizer.remote_impl.session_lifecycle import (
        SharedDownloadSessionCloser,
    )

    class Session:
        async def __aexit__(self, *_exc: object) -> None:
            return None

    class Coordinator:
        def submit(self, operation: Any) -> Future[Any]:
            future: Future[Any] = Future()
            asyncio.run(operation(None))
            future.set_result(None)
            return future

    closer = SharedDownloadSessionCloser(Coordinator(), Session(), ())
    assert closer.close(timeout_seconds=1)
    assert closer._coordinator is None
    assert closer._download_session is None
    assert closer._futures == ()


def test_janitor_idle_worker_finishes_all_stale_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An idle worker does not retire after only the first stale batch."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    stale = tmp_path / "quarantine"
    stale.mkdir()
    count = module._SWEEP_BATCH_SIZE * 2 + 3
    for index in range(count):
        (stale / f"artifact-stale-{index}").write_text("x")

    class Lease:
        def release(self) -> None:
            return None

    janitor = module._TemporaryArtifactJanitor()
    janitor._thread = __import__("threading").current_thread()
    janitor._thread_lease = Lease()
    monkeypatch.setattr(janitor, "root", lambda: stale)
    monkeypatch.setattr(module, "_RETRY_SECONDS", 0.0)
    janitor._run()
    assert janitor._scanned
    assert not any(stale.iterdir())
    assert janitor._thread is None
    assert janitor._thread_lease is None


def test_janitor_rejects_symlink_quarantine_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A predictable quarantine symlink cannot redirect recursive deletion."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    target = tmp_path / "target"
    target.mkdir()
    (tmp_path / "schema-sanitizer-quarantine").symlink_to(target, target_is_directory=True)
    monkeypatch.setattr(module.os, "getenv", lambda *_args: str(tmp_path))
    with pytest.raises(OSError, match="real directory"):
        module._TemporaryArtifactJanitor.root()


def test_janitor_unlinks_symlink_artifact_without_following_target(
    tmp_path: Path,
) -> None:
    """A quarantined symlink is removed without deleting its directory target."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    target = tmp_path / "target"
    target.mkdir()
    marker = target / "keep"
    marker.write_text("safe")
    link = tmp_path / "artifact-link"
    link.symlink_to(target, target_is_directory=True)

    assert module._TemporaryArtifactJanitor._delete(link, True)
    assert not link.exists()
    assert marker.read_text() == "safe"


def test_janitor_root_is_private_and_owned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The quarantine root is a private directory owned by this process user."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    monkeypatch.setattr(module.os, "getenv", lambda *_args: str(tmp_path))
    root = module._TemporaryArtifactJanitor.root()
    metadata = root.lstat()
    assert stat.S_IMODE(metadata.st_mode) == 0o700
    getuid = getattr(os, "geteuid", None)
    if getuid is not None:
        assert metadata.st_uid == getuid()


def test_janitor_new_worker_reopens_completed_shared_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Later worker epochs recover artifacts created after an earlier scan."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    class Lease:
        def release(self) -> None:
            return None

    class Thread:
        def __init__(self, **_kwargs: object) -> None:
            self.started = False

        def is_alive(self) -> bool:
            return self.started

        def start(self) -> None:
            self.started = True

    janitor = module._TemporaryArtifactJanitor()
    janitor._scanned = True
    janitor._scan_entries = iter(())
    monkeypatch.setattr(module, "acquire_project_threads", lambda *_a, **_k: Lease())
    monkeypatch.setattr(module.threading, "Thread", Thread)
    with janitor._lock:
        janitor._ensure_thread_locked()
    assert not janitor._scanned
    assert janitor._scan_entries is None
    assert janitor._thread is not None


def test_janitor_scan_does_not_follow_symlink_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stale discovery unlinks a symlink without traversing its target."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    quarantine = tmp_path / "quarantine"
    quarantine.mkdir()
    target = tmp_path / "external"
    target.mkdir()
    marker = target / "keep"
    marker.write_text("safe")
    link = quarantine / "artifact-linked-directory"
    link.symlink_to(target, target_is_directory=True)

    janitor = module._TemporaryArtifactJanitor()
    monkeypatch.setattr(janitor, "root", lambda: quarantine)
    janitor._scan_stale()
    janitor._scan_stale()
    assert not link.exists()
    assert marker.read_text() == "safe"


def test_staged_ownership_rejects_post_fork_use_before_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inherited publish and consume paths fail before acquiring a parent mutex."""
    from schema_sanitizer.remote_impl import staged_ownership as module

    class ForbiddenLock:
        def __enter__(self) -> None:
            raise AssertionError("inherited lock was acquired")

        def __exit__(self, *_exc: object) -> None:
            return None

    ownership = module.StagedResultOwnership()
    ownership._lock = ForbiddenLock()
    monkeypatch.setattr(module.os, "getpid", lambda: ownership._pid + 1)
    with pytest.raises(RuntimeError, match="after fork"):
        ownership.publish(object())
    with pytest.raises(RuntimeError, match="after fork"):
        ownership.consume(object())


def test_shared_session_closer_skips_parent_lock_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child intentionally leaks parent loop ownership without touching its lock."""
    from schema_sanitizer.remote_impl import session_lifecycle as module

    class ForbiddenLock:
        def __enter__(self) -> None:
            raise AssertionError("inherited lock was acquired")

        def __exit__(self, *_exc: object) -> None:
            return None

    closer = module.SharedDownloadSessionCloser(object(), object(), ())
    closer._lock = ForbiddenLock()
    monkeypatch.setattr(module.os, "getpid", lambda: closer._pid + 1)
    assert closer.close(timeout_seconds=0.001)


def test_remote_prefetch_rejects_post_fork_admission_before_lock(
    native_stub: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An inherited iterator cannot submit or consume work through parent state."""
    from schema_sanitizer.api_impl.source_plan import remote as module

    class ForbiddenLock:
        def __enter__(self) -> None:
            raise AssertionError("inherited lock was acquired")

        def __exit__(self, *_exc: object) -> None:
            return None

    iterator = _bare_remote_iterator(module)
    iterator._close_lock = ForbiddenLock()
    monkeypatch.setattr(module.os, "getpid", lambda: iterator._pid + 1)
    with pytest.raises(RuntimeError, match="after fork"):
        iterator._fill_prefetch_window()
    with pytest.raises(RuntimeError, match="after fork"):
        next(iterator)


def test_lookahead_worker_shutdown_retries_failed_exit_permit(
    native_stub: None,
) -> None:
    """Explicit shutdown reclaims a permit whose worker-exit release failed once."""
    from schema_sanitizer.pipeline.partition_lookahead_worker import ThreadPoolExecutor

    class Lease:
        def __init__(self) -> None:
            self.calls = 0

        def release(self) -> None:
            self.calls += 1
            if self.calls == 1:
                raise OSError("governor temporarily unavailable")

    lease = Lease()
    executor = ThreadPoolExecutor(
        max_workers=1,
        thread_name_prefix="pass27-lookahead",
        permit_factory=lambda *_a, **_k: lease,
    )
    executor.shutdown(wait=True, cancel_futures=True)
    assert lease.calls == 2
    assert executor._thread_lease is None


def test_lookahead_worker_rejects_post_fork_use_before_lock(
    native_stub: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inherited submit and shutdown paths do not acquire the parent worker lock."""
    from schema_sanitizer.pipeline import partition_lookahead_worker as module

    class Lease:
        def release(self) -> None:
            return None

    class ForbiddenLock:
        def __enter__(self) -> None:
            raise AssertionError("inherited lock was acquired")

        def __exit__(self, *_exc: object) -> None:
            return None

    executor = module.ThreadPoolExecutor(
        max_workers=1,
        thread_name_prefix="pass27-fork",
        permit_factory=lambda *_a, **_k: Lease(),
    )
    executor.shutdown(wait=True)
    executor._lock = ForbiddenLock()
    monkeypatch.setattr(module.os, "getpid", lambda: executor._pid + 1)
    with pytest.raises(RuntimeError, match="after fork"):
        executor.submit(lambda: None)
    executor.shutdown(wait=True)


def test_lookahead_worker_retries_startup_permit_without_thread(
    native_stub: None,
) -> None:
    """A failed Thread constructor does not make its retained permit unreachable."""
    from schema_sanitizer.pipeline import partition_lookahead_worker as module

    class Lease:
        def __init__(self) -> None:
            self.calls = 0

        def release(self) -> None:
            self.calls += 1

    executor = object.__new__(module.ThreadPoolExecutor)
    executor._pid = os.getpid()
    executor._thread_lease = Lease()
    executor._retry_stopped_thread_lease()
    assert executor._thread_lease is None
