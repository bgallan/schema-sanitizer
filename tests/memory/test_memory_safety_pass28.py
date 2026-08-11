"""Regression tests for pass28 retryable coordinator release ownership."""

from __future__ import annotations

import sys
from collections import deque
from concurrent.futures import Future
from threading import Lock, RLock
from types import SimpleNamespace
from typing import Any

import pytest

_NATIVE_STUB_MODULES = (
    "schema_sanitizer.core_impl.native_options",
    "schema_sanitizer.core_impl.execution_policy",
    "schema_sanitizer.api_impl.operation_context",
    "schema_sanitizer.api_impl.input.directory_preparation",
    "schema_sanitizer.api_impl.source_plan.attached",
    "schema_sanitizer.api_impl.source_plan.remote",
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
    """Provide isolated import-time native metadata for Python-only tests."""
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


class _FailingRelease:
    """Release owner that succeeds after a configured number of failures."""

    def __init__(self, failures: int = 1) -> None:
        """Initialize one retryable owner."""
        self.failures = failures
        self.calls = 0
        self.released = False

    def release(self) -> None:
        """Fail transiently and then commit exactly once."""
        self.calls += 1
        if self.calls <= self.failures:
            raise OSError("transient release failure")
        if self.released:
            raise AssertionError("double release")
        self.released = True


def _bare_coordinator() -> Any:
    """Build the minimum coordinator state needed by release helpers."""
    from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator

    coordinator = object.__new__(RemoteIoCoordinator)
    coordinator._lock = Lock()
    coordinator._release_lock = Lock()
    coordinator._futures = set()
    coordinator._failed_submissions = deque()
    coordinator._permit_registration = None
    coordinator._thread_lease = None
    coordinator._protocol_violations = 0
    return coordinator


def test_done_callback_retains_failed_submission_for_retry(native_stub: None) -> None:
    """A callback release failure must keep the only submission owner."""
    coordinator = _bare_coordinator()
    owner = _FailingRelease()
    future: Future[None] = Future()
    coordinator._futures.add(future)

    coordinator._complete_submission(future, owner)

    assert future not in coordinator._futures
    assert list(coordinator._failed_submissions) == [owner]
    coordinator._retry_failed_submissions()
    assert owner.released
    assert not coordinator._failed_submissions


def test_registration_release_is_commit_after_release(native_stub: None) -> None:
    """A failed registration release must remain reachable for retry."""
    coordinator = _bare_coordinator()
    owner = _FailingRelease()
    coordinator._permit_registration = owner

    with pytest.raises(OSError):
        coordinator._release_permit_registration()
    assert coordinator._permit_registration is owner

    coordinator._release_permit_registration()
    assert coordinator._permit_registration is None
    assert owner.released


def test_thread_release_is_commit_after_release(native_stub: None) -> None:
    """A failed thread-slot release must remain reachable for retry."""
    coordinator = _bare_coordinator()
    owner = _FailingRelease()
    coordinator._thread_lease = owner

    with pytest.raises(OSError):
        coordinator._release_thread_lease()
    assert coordinator._thread_lease is owner

    coordinator._release_thread_lease()
    assert coordinator._thread_lease is None
    assert owner.released


def test_failed_staging_storage_release_is_retained(native_stub: None) -> None:
    """A failed done-callback release must be retried by iterator close."""
    from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator

    submitted: Future[Any] = Future()

    class Coordinator:
        """Minimal submit-only coordinator."""

        def submit(self, *_args: Any, **_kwargs: Any) -> Future[Any]:
            """Return the controlled future."""
            return submitted

    lease = _FailingRelease()
    iterator = object.__new__(RemoteChunkPrefetchIterator)
    iterator._pid = __import__("os").getpid()
    iterator._manifest = SimpleNamespace()
    iterator._coordinator = Coordinator()
    iterator._download_session = None
    iterator._close_lock = RLock()
    iterator._failed_storage_leases = deque()

    future = iterator._submit_stage(0, lease)
    future.set_exception(RuntimeError("staging failed"))

    assert list(iterator._failed_storage_leases) == [lease]

    iterator._futures = deque()
    iterator._coordinator = None
    iterator._owns_coordinator = False
    iterator._download_session = None
    iterator._session_closer = None
    iterator._closed = False
    iterator._close_started = False
    iterator.close()

    assert lease.released
    assert not iterator._failed_storage_leases
    assert iterator._closed


def test_inline_stage_failure_preserves_primary_and_retains_lease(native_stub: None) -> None:
    """Inline staging must not lose its lease when rollback also fails."""
    from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator

    lease = _FailingRelease()
    iterator = object.__new__(RemoteChunkPrefetchIterator)
    iterator._manifest = SimpleNamespace(
        stage_chunk=lambda _start: (_ for _ in ()).throw(ValueError("primary"))
    )
    iterator._coordinator = None
    iterator._close_lock = RLock()
    iterator._failed_storage_leases = deque()

    future = iterator._submit_stage(0, lease)

    with pytest.raises(ValueError, match="primary") as caught:
        future.result()
    assert any("rollback also failed" in note for note in caught.value.__notes__)
    assert list(iterator._failed_storage_leases) == [lease]


def test_submit_failure_preserves_primary_and_retains_lease(native_stub: None) -> None:
    """Scheduler rejection must retain a lease whose rollback fails."""
    from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator

    class RejectingCoordinator:
        """Coordinator double that rejects submission synchronously."""

        def submit(self, *_args: Any, **_kwargs: Any) -> Future[Any]:
            """Reject the operation before publishing a Future."""
            raise RuntimeError("submission rejected")

    lease = _FailingRelease()
    iterator = object.__new__(RemoteChunkPrefetchIterator)
    iterator._manifest = SimpleNamespace()
    iterator._coordinator = RejectingCoordinator()
    iterator._download_session = None
    iterator._close_lock = RLock()
    iterator._failed_storage_leases = deque()

    with pytest.raises(RuntimeError, match="submission rejected") as caught:
        iterator._submit_stage(0, lease)
    assert any("rollback also failed" in note for note in caught.value.__notes__)
    assert list(iterator._failed_storage_leases) == [lease]


def test_close_retries_retained_cleanup_without_reopening_runtime(native_stub: None) -> None:
    """A second close must retry owners retained by the first failed cleanup."""
    from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator

    class DeadThread:
        """Stopped thread double used by cleanup-only close."""

        ident = None

        def is_alive(self) -> bool:
            """Report that event-loop execution has already ended."""
            return False

        def join(self, timeout: float | None = None) -> None:
            """Accept a bounded join without side effects."""

    owner = _FailingRelease()
    coordinator = object.__new__(RemoteIoCoordinator)
    coordinator._pid = __import__("os").getpid()
    coordinator._lock = Lock()
    coordinator._release_lock = Lock()
    coordinator._closed = False
    coordinator._closing = False
    coordinator._close_complete = __import__("threading").Event()
    coordinator._close_error = None
    coordinator._shutdown_timeout_seconds = 0.1
    coordinator._loop = None
    coordinator._futures = set()
    coordinator._failed_submissions = deque()
    coordinator._permit_registration = owner
    coordinator._thread_lease = None
    coordinator._thread = DeadThread()
    coordinator._protocol_violations = 0

    with pytest.raises(OSError, match="transient"):
        coordinator.close()
    assert coordinator._permit_registration is owner

    coordinator._close_complete.clear()
    coordinator.close()
    assert coordinator._permit_registration is None
    assert owner.released
