"""Remote source-plan probing and lazy staged-chunk execution."""

from __future__ import annotations

import os
from collections import deque
from collections.abc import Iterator
from concurrent.futures import CancelledError, Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from threading import Condition, Lock, RLock
from typing import Any

from schema_sanitizer.core_impl.safe_errors import add_bounded_note

from ...core_impl.durations import deadline_ns_from_timeout, remaining_seconds
from ...core_impl.execution_policy import execution_policy
from ...core_impl.finalization import runtime_is_finalizing
from ...core_impl.memory_budget import adaptive_concurrency_target, memory_budget
from ...core_impl.resource_lifecycle import (
    _cleanup_with_note,
    _close_suppressing_errors,
)
from ...remote_impl.io_coordinator import RemoteIoCoordinator, StagedResultOwnership
from ...remote_impl.session_lifecycle import (
    SharedDownloadSessionCloser,
    enter_shared_download_session,
)
from ..input.directory_preparation import RemoteNativeDirectorySourceManifest
from .remote_cleanup import take_prefetched_chunks
from .remote_runtime import RemotePathSourceChunkProviderBase

_PREFETCH_COMPAT_INIT_LOCK = Lock()


class _StorageLeaseRollbackOwner:
    """Exactly-once rollback owner for one pre-acquired staging lease."""

    def __init__(self, lease: Any) -> None:
        self.lease = lease
        self._lock = Lock()
        self._claimed = False

    def claim(self) -> bool:
        """Claim the terminal success/failure decision exactly once."""
        with self._lock:
            if self._claimed:
                return False
            self._claimed = True
            return True


class RemoteChunkPrefetchIterator:
    """Iterate staged remote chunks through one operation-owned I/O loop."""

    def __init__(self, manifest: Any, *, start: int = 0) -> None:
        """Create a staging iterator for a remote native manifest."""
        self._pid = os.getpid()
        self._manifest = manifest
        self._policy = execution_policy(
            getattr(manifest, "threading_mode", "single"),
            getattr(manifest, "memory_limit_bytes", None),
        )
        self._prefetch_chunks = self._policy.remote_chunk_prefetch
        budget = memory_budget(getattr(manifest, "memory_limit_bytes", None))
        self._remote_timeout_seconds = budget.async_timeout_seconds
        self._io_chunk_bytes = max(1, budget.io_chunk_bytes)
        self._coordinator: RemoteIoCoordinator | None = None
        self._owns_coordinator = False
        self._download_session: Any | None = None
        self._futures: deque[Future[Any]] = deque()
        self._failed_storage_leases: deque[Any] = deque()
        self._callbackless_storage_futures: dict[Future[Any], _StorageLeaseRollbackOwner] = {}
        self._next_start = max(0, int(start))
        self._close_lock = RLock()
        self._close_condition = Condition(self._close_lock)
        self._close_in_progress = False
        self._cleanup_callbacks_inflight = 0
        self._admissions_inflight = 0
        self._consumers_inflight = 0
        self._starting = False
        self._fill_in_progress = False
        self._close_started = False
        self._session_closer: SharedDownloadSessionCloser | None = None
        self._closed = False
        self._started = False

    def _assert_owner_process(self) -> None:
        """Reject inherited iterator use before touching parent-owned state."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            raise RuntimeError("remote prefetch iterator cannot be reused after fork")

    def __enter__(self) -> RemoteChunkPrefetchIterator:
        """Enter the staging context."""
        self._assert_owner_process()
        self._ensure_started()
        return self

    def __exit__(self, *_exc: object) -> None:
        """Close pending staged chunks."""
        self.close()

    def __iter__(self) -> RemoteChunkPrefetchIterator:
        """Return the iterator."""
        self._assert_owner_process()
        self._ensure_started()
        return self

    def __next__(self) -> Any:
        """Return one staged result through an exclusive lifecycle claim."""
        self._assert_owner_process()
        condition = self._lifecycle_condition()
        try:
            self._ensure_started()
            while True:
                with condition:
                    if self._closed or getattr(self, "_close_started", False):
                        raise StopIteration
                    has_future = bool(self._futures)
                if not has_future:
                    self._fill_prefetch_window()

                with condition:
                    if self._closed or getattr(self, "_close_started", False):
                        raise StopIteration
                    if not self._futures:
                        future = None
                    else:
                        future = self._futures.popleft()
                        self._consumers_inflight = getattr(self, "_consumers_inflight", 0) + 1

                if future is None:
                    self.close()
                    raise StopIteration

                staged: Any | None = None
                try:
                    ownership = getattr(future, "_schema_sanitizer_staged_ownership")
                    try:
                        staged = future.result(timeout=self._remote_timeout_seconds)
                    except FutureTimeoutError:
                        future.cancel()
                        raise TimeoutError(
                            "remote chunk staging exceeded its bounded transport deadline"
                        ) from None
                    self._complete_callbackless_storage_futures()
                    staged = ownership.consume(staged)
                    try:
                        self._fill_prefetch_window()
                    except BaseException:
                        # Ownership has transferred out of the future, but the
                        # value has not yet been returned to the caller.
                        _close_suppressing_errors(staged)
                        raise
                finally:
                    with condition:
                        self._consumers_inflight = max(
                            0, getattr(self, "_consumers_inflight", 1) - 1
                        )
                        condition.notify_all()

                if staged is None:
                    self.close()
                    raise StopIteration
                return staged
        except BaseException as exc:
            try:
                self.close()
            except BaseException as cleanup_error:
                add_bounded_note(
                    exc,
                    "remote prefetch cleanup also failed after iteration error",
                    cleanup_error,
                )
            raise

    def _ensure_started(self) -> None:
        """Start external resources through a claim/work/commit transaction."""
        self._assert_owner_process()
        condition = self._lifecycle_condition()
        with condition:
            while self._starting:
                condition.wait()
            if self._started or getattr(self, "_close_started", False):
                return
            self._starting = True
            self._admissions_inflight += 1

        coordinator: RemoteIoCoordinator | None = None
        owns_coordinator = False
        download_session: Any | None = None
        committed = False
        try:
            if not self._policy.is_single:
                open_session = getattr(self._manifest, "open_staging_session", None)
                stage_async = getattr(self._manifest, "stage_chunk_async", None)
                if not callable(open_session) or not callable(stage_async):
                    raise TypeError("multi remote chunk staging requires an async manifest session")
                operation_context = getattr(self._manifest, "operation_context", None)
                shared = getattr(operation_context, "remote_coordinator", None)
                if shared is not None:
                    coordinator = shared
                    download_session = open_session()
                    try:
                        enter_shared_download_session(
                            coordinator,
                            download_session,
                            timeout_seconds=self._remote_timeout_seconds,
                        )
                    except TimeoutError:
                        # The entry coroutine owns its late self-close handshake.
                        download_session = None
                        raise
                else:
                    coordinator = RemoteIoCoordinator(open_session)
                    owns_coordinator = True

            with condition:
                self._coordinator = coordinator
                self._owns_coordinator = owns_coordinator
                self._download_session = download_session
                self._started = True
                committed = True
        except BaseException as exc:
            if owns_coordinator and coordinator is not None:
                try:
                    coordinator.close()
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc,
                        "remote prefetch coordinator rollback also failed",
                        cleanup_error,
                    )
            raise
        finally:
            with condition:
                self._starting = False
                self._admissions_inflight = max(0, self._admissions_inflight - 1)
                condition.notify_all()

        if committed:
            try:
                self._fill_prefetch_window()
            except BaseException as exc:
                try:
                    self.close()
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc,
                        "remote prefetch cleanup also failed after startup error",
                        cleanup_error,
                    )
                raise

    def _lifecycle_condition(self) -> Condition:
        """Return the lazily initialized close/callback condition."""
        with _PREFETCH_COMPAT_INIT_LOCK:
            close_lock = getattr(self, "_close_lock", None)
            if close_lock is None:
                close_lock = RLock()
                self._close_lock = close_lock
            condition = getattr(self, "_close_condition", None)
            if condition is None:
                condition = Condition(close_lock)
                self._close_condition = condition
            if not hasattr(self, "_close_in_progress"):
                self._close_in_progress = False
            if not hasattr(self, "_cleanup_callbacks_inflight"):
                self._cleanup_callbacks_inflight = 0
            if not hasattr(self, "_callbackless_storage_futures"):
                self._callbackless_storage_futures = {}
            if not hasattr(self, "_admissions_inflight"):
                self._admissions_inflight = 0
            if not hasattr(self, "_consumers_inflight"):
                self._consumers_inflight = 0
            if not hasattr(self, "_starting"):
                self._starting = False
            if not hasattr(self, "_fill_in_progress"):
                self._fill_in_progress = False
            return condition

    def _release_or_retain_storage_lease(
        self, storage_lease: Any, *, primary: BaseException | None = None
    ) -> None:
        """Release one staging lease or retain it for a later close retry."""
        try:
            storage_lease.release()
        except BaseException as cleanup_error:
            condition = self._lifecycle_condition()
            with condition:
                failed = getattr(self, "_failed_storage_leases", None)
                if failed is None:
                    failed = deque()
                    self._failed_storage_leases = failed
                failed.append(storage_lease)
            if primary is not None:
                add_bounded_note(
                    primary,
                    "remote staging storage rollback also failed and remains retryable",
                    cleanup_error,
                )

    def _complete_storage_rollback(
        self, future: Future[Any], owner: _StorageLeaseRollbackOwner
    ) -> None:
        """Resolve one lease rollback owner after real task terminal state."""
        condition = self._lifecycle_condition()
        if not owner.claim():
            with condition:
                self._callbackless_storage_futures.pop(future, None)
                condition.notify_all()
            return
        try:
            remote_submission = getattr(future, "_schema_sanitizer_remote_submission", None)
            if remote_submission is not None:
                task_error = getattr(
                    remote_submission,
                    "task_error",
                    getattr(remote_submission, "operation_error", None),
                )
                failed = task_error is not None
            else:
                try:
                    failed = future.cancelled() or future.exception() is not None
                except CancelledError:
                    failed = True
            if failed:
                self._release_or_retain_storage_lease(owner.lease)
        finally:
            with condition:
                self._callbackless_storage_futures.pop(future, None)
                condition.notify_all()

    def _complete_callbackless_storage_futures(self) -> None:
        """Resolve every terminal Future whose callback registration failed."""
        condition = self._lifecycle_condition()
        with condition:
            terminal_items: list[tuple[Future[Any], _StorageLeaseRollbackOwner]] = []
            for future, owner in self._callbackless_storage_futures.items():
                remote_submission = getattr(future, "_schema_sanitizer_remote_submission", None)
                if remote_submission is not None:
                    ready = remote_submission.terminal.is_set()
                else:
                    ready = future.done()
                if ready:
                    terminal_items.append((future, owner))
            terminal = tuple(terminal_items)
        for future, owner in terminal:
            self._complete_storage_rollback(future, owner)

    def _submit_stage(self, start: int, storage_lease: Any | None) -> Future[Any]:
        """Submit one chunk to the inline or shared-I/O executor."""
        ownership = StagedResultOwnership()
        coordinator = self._coordinator
        if coordinator is None:
            future: Future[Any] = Future()
            try:
                future.set_result(ownership.publish(self._manifest.stage_chunk(start)))
            except BaseException as exc:
                if storage_lease is not None:
                    self._release_or_retain_storage_lease(storage_lease, primary=exc)
                future.set_exception(exc)
            setattr(future, "_schema_sanitizer_staged_ownership", ownership)
            return future

        async def stage(coordinator_context: Any) -> Any:
            """Stage one chunk and self-clean if cancellation abandons its future."""
            download_session = self._download_session or coordinator_context
            if storage_lease is None:
                staged = await self._manifest.stage_chunk_async(start, download_session)
            else:
                staged = await self._manifest.stage_chunk_async(
                    start, download_session, storage_lease=storage_lease
                )
            return ownership.publish(staged)

        try:
            future = coordinator.submit(
                stage,
                permit_weight=self._stage_permit_weight(start),
                permit_label="remote_chunk_staging",
            )
        except BaseException as exc:
            if storage_lease is not None:
                self._release_or_retain_storage_lease(storage_lease, primary=exc)
            raise
        setattr(future, "_schema_sanitizer_staged_ownership", ownership)
        if storage_lease is not None:
            condition = self._lifecycle_condition()
            rollback_owner = _StorageLeaseRollbackOwner(storage_lease)
            with condition:
                self._cleanup_callbacks_inflight += 1

            def release_failed_reservation(done: Future[Any]) -> None:
                """Return or retain a pre-acquired lease after failed staging."""
                try:
                    self._complete_storage_rollback(done, rollback_owner)
                finally:
                    with condition:
                        self._cleanup_callbacks_inflight = max(
                            0, self._cleanup_callbacks_inflight - 1
                        )
                        condition.notify_all()

            try:
                remote_submission = getattr(future, "_schema_sanitizer_remote_submission", None)
                if remote_submission is not None:
                    remote_submission.add_terminal_callback(release_failed_reservation)
                else:
                    future.add_done_callback(release_failed_reservation)
            except BaseException as registration_error:
                # The Future and rollback owner remain reachable even when a
                # non-standard bridge rejects callback registration. Iteration
                # or close resolves the owner after the Future is terminal.
                with condition:
                    self._cleanup_callbacks_inflight = max(0, self._cleanup_callbacks_inflight - 1)
                    self._callbackless_storage_futures[future] = rollback_owner
                    condition.notify_all()
                setattr(
                    future,
                    "_schema_sanitizer_callback_registration_error",
                    registration_error,
                )
                future.cancel()
                self._complete_callbackless_storage_futures()
        return future

    def _stage_permit_weight(self, start: int) -> int:
        """Scale remote admission by the estimated chunk bytes."""
        estimate = getattr(self._manifest, "estimated_chunk_bytes", None)
        if not callable(estimate):
            return 1
        try:
            size_bytes = max(1, int(estimate(start)))
        except (TypeError, ValueError, OverflowError):
            return 1
        desired = 1 + (size_bytes - 1) // self._io_chunk_bytes
        return max(1, min(self._policy.async_concurrency, desired))

    def _fill_prefetch_window(self) -> None:
        """Queue work through per-item claim/work/commit transactions."""
        self._assert_owner_process()
        condition = self._lifecycle_condition()
        with condition:
            while self._fill_in_progress:
                condition.wait()
            if self._closed or getattr(self, "_close_started", False):
                return
            self._fill_in_progress = True

        try:
            while True:
                with condition:
                    if self._closed or getattr(self, "_close_started", False):
                        return
                    target = adaptive_concurrency_target(
                        max(1, self._prefetch_chunks),
                        per_slot_bytes=self._io_chunk_bytes,
                    )
                    if len(self._futures) >= target or self._next_start >= len(
                        self._manifest.files
                    ):
                        return
                    start = self._next_start
                    coordinator = self._coordinator
                    self._admissions_inflight += 1

                storage_lease: Any | None = None
                future: Future[Any] | None = None
                try:
                    acquire = getattr(self._manifest, "try_acquire_storage_lease", None)
                    storage_lease = (
                        acquire(start) if coordinator is not None and callable(acquire) else None
                    )
                    if coordinator is not None and callable(acquire) and storage_lease is None:
                        return
                    next_chunk_start = getattr(self._manifest, "next_chunk_start", None)
                    if callable(next_chunk_start):
                        committed_next_start = max(start + 1, int(next_chunk_start(start)))
                    else:
                        committed_next_start = start + max(1, self._manifest.chunk_size)
                    future = self._submit_stage(start, storage_lease)
                    with condition:
                        # Close waits for this admission, so publishing here is
                        # safe even if it started while external work ran.
                        self._futures.append(future)
                        self._next_start = committed_next_start
                except BaseException as exc:
                    if future is not None:
                        ownership = getattr(future, "_schema_sanitizer_staged_ownership", None)
                        if ownership is not None:
                            ownership.abandon()
                        future.cancel()
                    elif storage_lease is not None:
                        self._release_or_retain_storage_lease(storage_lease, primary=exc)
                    raise
                finally:
                    with condition:
                        self._admissions_inflight = max(0, self._admissions_inflight - 1)
                        condition.notify_all()
        finally:
            with condition:
                self._fill_in_progress = False
                condition.notify_all()

    def close(self) -> None:
        """Close staging transactionally after all cleanup publishers quiesce."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return
        condition = self._lifecycle_condition()
        deadline_ns = deadline_ns_from_timeout(
            getattr(self, "_remote_timeout_seconds", 30.0),
            name="remote prefetch close timeout",
            allow_zero=False,
        )
        with condition:
            while self._close_in_progress:
                remaining = remaining_seconds(deadline_ns)
                if remaining <= 0 or not condition.wait(timeout=remaining):
                    raise RuntimeError("remote prefetch concurrent close exceeded its deadline")
            if self._closed:
                return
            self._close_in_progress = True
            self._close_started = True
            while getattr(self, "_admissions_inflight", 0) or getattr(
                self, "_consumers_inflight", 0
            ):
                remaining = remaining_seconds(deadline_ns)
                if remaining <= 0 or not condition.wait(timeout=remaining):
                    self._close_in_progress = False
                    condition.notify_all()
                    raise RuntimeError(
                        "remote staging admissions or consumers exceeded their close deadline"
                    )
            futures = tuple(self._futures)
            callbackless_futures = tuple(self._callbackless_storage_futures)
            all_futures = tuple(dict.fromkeys((*futures, *callbackless_futures)))
            coordinator = self._coordinator
            shutdown_timeout = (
                coordinator.shutdown_timeout_seconds
                if coordinator is not None
                else getattr(self, "_remote_timeout_seconds", 30.0)
            )
            shutdown_deadline_ns = deadline_ns_from_timeout(
                shutdown_timeout,
                name="remote prefetch coordinator shutdown timeout",
                allow_zero=False,
            )
            deadline_ns = min(deadline_ns, shutdown_deadline_ns)
            download_session = self._download_session
            owns_coordinator = self._owns_coordinator

        try:
            for future in all_futures:
                ownership = getattr(future, "_schema_sanitizer_staged_ownership")
                ownership.abandon()
                if not future.done():
                    future.cancel()

            if coordinator is not None and download_session is not None:
                with condition:
                    closer = getattr(self, "_session_closer", None)
                    if closer is None:
                        closer = SharedDownloadSessionCloser(
                            coordinator, download_session, all_futures
                        )
                        self._session_closer = closer
                if not closer.close(timeout_seconds=remaining_seconds(deadline_ns)):
                    return
                with condition:
                    if self._session_closer is closer:
                        self._download_session = None
                        self._session_closer = None

            if coordinator is not None and owns_coordinator:
                coordinator.close()
                with condition:
                    if self._coordinator is coordinator:
                        self._owns_coordinator = False

            # A Future can wake result waiters before its done callbacks return.
            # Do not snapshot retained leases or commit _closed until every
            # callback capable of publishing a retry owner has terminated.
            with condition:
                while self._cleanup_callbacks_inflight:
                    remaining = remaining_seconds(deadline_ns)
                    if remaining <= 0 or not condition.wait(timeout=remaining):
                        raise RuntimeError(
                            "remote staging cleanup callbacks exceeded their deadline"
                        )

            self._complete_callbackless_storage_futures()
            with condition:
                if self._callbackless_storage_futures:
                    raise RuntimeError(
                        "remote staging callbackless Future remains non-terminal after close drain"
                    )

            failed_storage: deque[Any] = deque()
            with condition:
                retained_storage = getattr(self, "_failed_storage_leases", None)
                if retained_storage is None:
                    retained_storage = deque()
                    self._failed_storage_leases = retained_storage
                storage_snapshot = tuple(retained_storage)
                retained_storage.clear()
            for lease in storage_snapshot:
                try:
                    lease.release()
                except BaseException:
                    failed_storage.append(lease)
            if failed_storage:
                with condition:
                    self._failed_storage_leases.extend(failed_storage)
                raise RuntimeError(
                    "remote staging storage release remains retryable after close failure"
                )

            retained: deque[Future[Any]] = deque()
            for future in futures:
                ownership = getattr(future, "_schema_sanitizer_staged_ownership")
                if not ownership.abandon():
                    retained.append(future)
            if retained:
                with condition:
                    self._futures = retained
                raise RuntimeError(
                    "remote staged-result cleanup remains retryable after close failure"
                )

            with condition:
                # Recheck the callback barrier under the commit lock. No new
                # staging callback can be registered after _close_started.
                if self._cleanup_callbacks_inflight:
                    raise RuntimeError(
                        "remote staging cleanup callback published during close commit"
                    )
                if self._callbackless_storage_futures:
                    raise RuntimeError(
                        "remote staging callbackless owner published during close commit"
                    )
                if getattr(self, "_failed_storage_leases", ()):
                    raise RuntimeError(
                        "remote staging storage release remains retryable after close failure"
                    )
                self._futures = deque()
                self._coordinator = None
                self._closed = True
        finally:
            with condition:
                self._close_in_progress = False
                condition.notify_all()

    def __del__(self) -> None:
        """Close abandoned prefetch work outside interpreter teardown."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            self.close()
        except BaseException:
            pass


def iter_staged_remote_chunks(manifest: Any, *, start: int = 0) -> Iterator[Any]:
    """Return a context-managed iterator over staged native remote chunks."""
    return RemoteChunkPrefetchIterator(manifest, start=start)


def open_staged_remote_chunks(
    manifest: RemoteNativeDirectorySourceManifest, *, start: int = 0
) -> Any:
    """Open the staged-chunk context for one remote manifest."""
    return iter_staged_remote_chunks(manifest, start=start)


class RemotePathSourceChunkProvider(RemotePathSourceChunkProviderBase):
    """Compatibility owner for retryable staged remote path-source chunks."""


def prefetched_remote_chunks(
    manifest: RemoteNativeDirectorySourceManifest,
) -> tuple[list[Any], int]:
    """Take an optional partition-lookahead prefix from one remote manifest."""
    return take_prefetched_chunks(manifest)


def probe_remote_registry(
    raw_context: Any,
    manifest: RemoteNativeDirectorySourceManifest,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any = None,
) -> Any:
    """Infer one registry through the native lazy chunk-provider route."""
    retained_chunks, remaining_start = prefetched_remote_chunks(manifest)
    provider = RemotePathSourceChunkProvider(
        retained_chunks=retained_chunks,
        remaining_manifest=manifest,
        remaining_start=remaining_start,
    )
    try:
        return raw_context.registry_probe_path_source_chunk_provider(
            provider,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            native_registry_state=native_registry_state,
            skip_invalid_json_sources=True,
        )
    except BaseException as exc:
        _cleanup_with_note(
            exc, provider, label="remote registry probe cleanup also failed", method="close_all"
        )
        raise
