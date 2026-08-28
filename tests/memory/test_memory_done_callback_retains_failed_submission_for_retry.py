"""Regression coverage for memory done callback retains failed submission for retry."""

from __future__ import annotations

from collections import deque
from concurrent.futures import Future
from threading import Condition, Lock, RLock
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


class _FailingRelease:
    """Release owner that succeeds after a configured number of failures."""

    def __init__(self, failures: int = 1) -> None:
        self.failures = failures
        self.calls = 0
        self.released = False

    def release(self) -> None:
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
    coordinator._close_condition = Condition(coordinator._lock)
    coordinator._release_lock = Lock()
    coordinator._futures = set()
    coordinator._submissions = {}
    coordinator._callbackless_submissions = {}
    coordinator._failed_submissions = deque()
    coordinator._submission_callbacks_inflight = 0
    coordinator._permit_registration = None
    coordinator._thread_lease = None
    coordinator._protocol_violations = 0
    return coordinator


def test_done_callback_retains_failed_submission_for_retry(native_stub: None) -> None:
    """A callback release failure must keep the only submission owner."""
    from schema_sanitizer.remote_impl import io_coordinator as module

    coordinator = _bare_coordinator()
    owner = _FailingRelease()
    future: Future[None] = Future()
    coordinator._futures.add(future)
    submission = module._RemoteIoSubmission(owner)
    submission.future = future
    submission.registration_complete.set()
    coordinator._submissions[future] = submission

    coordinator._finish_submission_real(submission)

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
            return submitted

    lease = _FailingRelease()
    iterator = object.__new__(RemoteChunkPrefetchIterator)
    iterator._pid = __import__("os").getpid()
    iterator._manifest = SimpleNamespace(estimated_chunk_bytes=lambda _start: 1)
    iterator._policy = SimpleNamespace(async_concurrency=1)
    iterator._io_chunk_bytes = 1
    iterator._remote_timeout_seconds = 1.0
    iterator._coordinator = Coordinator()
    iterator._download_session = None
    iterator._close_lock = RLock()
    iterator._close_condition = Condition(iterator._close_lock)
    iterator._failed_storage_leases = deque()
    iterator._callbackless_storage_futures = {}
    iterator._cleanup_callbacks_inflight = 0
    iterator._admissions_inflight = 0
    iterator._consumers_inflight = 0
    iterator._protocol_violations = 0
    iterator._close_in_progress = False
    iterator._starting = False
    iterator._fill_in_progress = False

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
    iterator._finalizer_ticket = None
    iterator._finalizer_capsule = None
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
    iterator._close_condition = Condition(iterator._close_lock)
    iterator._failed_storage_leases = deque()
    iterator._callbackless_storage_futures = {}
    iterator._cleanup_callbacks_inflight = 0
    iterator._protocol_violations = 0

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
            raise RuntimeError("submission rejected")

    lease = _FailingRelease()
    iterator = object.__new__(RemoteChunkPrefetchIterator)
    iterator._manifest = SimpleNamespace(estimated_chunk_bytes=lambda _start: 1)
    iterator._policy = SimpleNamespace(async_concurrency=1)
    iterator._io_chunk_bytes = 1
    iterator._coordinator = RejectingCoordinator()
    iterator._download_session = None
    iterator._close_lock = RLock()
    iterator._close_condition = Condition(iterator._close_lock)
    iterator._failed_storage_leases = deque()
    iterator._callbackless_storage_futures = {}
    iterator._cleanup_callbacks_inflight = 0
    iterator._protocol_violations = 0

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
            return False

        def join(self, timeout: float | None = None) -> None:
            pass

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
    coordinator._submissions = {}
    coordinator._failed_submissions = deque()
    coordinator._failed_permits = deque()
    coordinator._callbackless_submissions = {}
    coordinator._submission_callbacks_inflight = 0
    coordinator._deferred_terminal_callbacks = deque()
    coordinator._terminal_callback_owners = set()
    coordinator._failed_terminal_callbacks = deque()
    coordinator._shutdown_future = None
    coordinator._close_condition = Condition(coordinator._lock)
    coordinator._close_generation = 0
    coordinator._completed_close_generation = 0
    coordinator._close_results = {}
    coordinator._close_waiters = {}
    coordinator._permit_registration = owner
    coordinator._thread_lease = None
    coordinator._runtime_registration = None
    coordinator._thread = DeadThread()
    coordinator._protocol_violations = 0

    with pytest.raises(OSError, match="transient"):
        coordinator.close()
    assert coordinator._permit_registration is owner

    coordinator._close_complete.clear()
    coordinator.close()
    assert coordinator._permit_registration is None
    assert owner.released
