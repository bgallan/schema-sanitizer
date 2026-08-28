"""Remote source-plan probing and lazy staged-chunk execution.

It stages remote chunks lazily with bounded prefetch, storage leases, registry probing,
and retryable close or abandonment.
"""

from __future__ import annotations

import os
from collections import deque
from concurrent.futures import CancelledError, Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from threading import Condition, Lock, RLock
from typing import Any, cast

from schema_sanitizer.core_impl.safe_errors import add_bounded_note

from ...core_impl.durations import deadline_ns_from_timeout, remaining_seconds
from ...core_impl.execution_policy import execution_policy
from ...core_impl.finalization import runtime_is_finalizing
from ...core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_finalizer_cleanup,
)
from ...core_impl.memory_budget import (
    adaptive_parallel_slots as _adaptive_parallel_slots,
)
from ...core_impl.memory_budget import (
    memory_budget,
)
from ...core_impl.resource_lifecycle import (
    _cleanup_with_note,
    _close_suppressing_errors,
)
from ...remote_impl.io_coordinator import RemoteIoCoordinator
from ...remote_impl.session_lifecycle import (
    SharedDownloadSessionCloser,
    enter_shared_download_session,
)
from ...remote_impl.staged_ownership import StagedResultOwnership
from ..input.directory_preparation import RemoteNativeDirectorySourceManifest
from .remote_cleanup import take_prefetched_chunks
from .remote_runtime import RemotePathSourceChunkProviderBase


class _StorageLeaseRollbackOwner:
    """Exactly-once rollback owner for one pre-acquired staging lease."""

    def __init__(self, lease: Any) -> None:
        """Bind a pre-acquired storage lease to an exactly-once rollback decision."""
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


def _cleanup_remote_prefetch_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Drain detached remote-prefetch owners without retaining manifest/config graphs."""
    futures = cast("deque[Future[Any]] | None", capsule.arg0)
    callbackless = cast("dict[Future[Any], _StorageLeaseRollbackOwner] | None", capsule.arg1)
    failed_storage = cast("deque[Any] | None", capsule.arg2)
    coordinator = cast("RemoteIoCoordinator | None", capsule.arg3)
    download_session = capsule.arg4
    closer = cast("SharedDownloadSessionCloser | None", capsule.arg5)
    owns_coordinator = bool(capsule.arg6)

    if futures is not None:
        while futures:
            future = futures[-1]
            ownership = getattr(future, "_schema_sanitizer_staged_ownership", None)
            if ownership is not None:
                ownership.abandon()
            if not future.done():
                future.cancel()
                submission = getattr(future, "_schema_sanitizer_remote_submission", None)
                terminal = getattr(submission, "terminal", None)
                if terminal is not None and not terminal.is_set():
                    raise RuntimeError("remote prefetch task remains non-terminal")
                if terminal is None and not future.done():
                    raise RuntimeError("remote prefetch future remains non-terminal")
            futures.pop()
        capsule.arg0 = None

    if callbackless is not None:
        while callbackless:
            future = next(iter(callbackless))
            owner = callbackless[future]
            submission = getattr(future, "_schema_sanitizer_remote_submission", None)
            terminal = getattr(submission, "terminal", None)
            ready = terminal.is_set() if terminal is not None else future.done()
            if not ready:
                future.cancel()
                raise RuntimeError("callbackless staging future remains non-terminal")
            if owner.claim():
                if submission is not None:
                    task_error = getattr(
                        submission, "task_error", getattr(submission, "operation_error", None)
                    )
                    failed = task_error is not None
                else:
                    try:
                        failed = future.cancelled() or future.exception() is not None
                    except CancelledError:
                        failed = True
                if failed:
                    owner.lease.release()
            del callbackless[future]
        capsule.arg1 = None

    if failed_storage is not None:
        while failed_storage:
            lease = failed_storage[-1]
            lease.release()
            failed_storage.pop()
        capsule.arg2 = None

    if download_session is not None:
        if coordinator is None:
            raise RuntimeError("remote download session lost its coordinator")
        if closer is None:
            closer = SharedDownloadSessionCloser(coordinator, download_session, ())
            capsule.arg5 = closer
        if not closer.close(timeout_seconds=0.0):
            raise RuntimeError("remote download session cleanup remains retryable")
        capsule.arg4 = None
        capsule.arg5 = None

    if coordinator is not None and owns_coordinator:
        coordinator.close()
    capsule.arg3 = None
    capsule.arg6 = None


class RemoteChunkPrefetchIterator:
    """Iterate staged remote chunks through one operation-owned I/O loop."""

    def __init__(self, manifest: RemoteNativeDirectorySourceManifest, *, start: int = 0) -> None:
        """Create a staging iterator for a remote native manifest."""
        self._pid = os.getpid()
        self._manifest = manifest
        self._policy = execution_policy(
            manifest.threading_mode,
            manifest.memory_limit_bytes,
        )
        self._prefetch_chunks = self._policy.remote_chunk_prefetch
        budget = memory_budget(manifest.memory_limit_bytes)
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
        self._protocol_violations = 0
        self._starting = False
        self._fill_in_progress = False
        self._close_started = False
        self._session_closer: SharedDownloadSessionCloser | None = None
        self._closed = False
        self._started = False
        finalizer_capsule = reserve_finalizer_cleanup(_cleanup_remote_prefetch_capsule)
        self._finalizer_capsule: PreparedFinalizerCleanup | None = finalizer_capsule
        self._finalizer_ticket: int | None = finalizer_capsule.ticket

    def _assert_owner_process(self) -> None:
        """Reject inherited iterator use before touching parent-owned state."""
        if os.getpid() != self._pid:
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
        """Return this remote chunk prefetch iterator."""
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
                    if any((self._closed, self._close_started)):
                        raise StopIteration
                    has_future = bool(self._futures)
                if not has_future:
                    self._fill_prefetch_window()

                with condition:
                    if any((self._closed, self._close_started)):
                        raise StopIteration
                    if not self._futures:
                        future = None
                    else:
                        future = self._futures.popleft()
                        self._consumers_inflight += 1

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
                        self._finish_lifecycle_counter_locked("_consumers_inflight")
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
        deadline_ns = deadline_ns_from_timeout(
            self._remote_timeout_seconds,
            name="remote prefetch startup wait timeout",
            allow_zero=False,
        )
        with condition:
            while self._starting:
                remaining = remaining_seconds(deadline_ns)
                if remaining <= 0 or not condition.wait(timeout=remaining):
                    raise RuntimeError("remote prefetch startup exceeded its deadline")
            if self._started or self._close_started:
                return
            self._starting = True
            self._admissions_inflight += 1

        coordinator: RemoteIoCoordinator | None = None
        owns_coordinator = False
        download_session: Any | None = None
        committed = False
        try:
            if not self._policy.is_single:
                open_session = self._manifest.open_staging_session
                operation_context = self._manifest.operation_context
                shared = (
                    operation_context.remote_coordinator if operation_context is not None else None
                )
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
                    coordinator = RemoteIoCoordinator(
                        open_session,
                        operation_memory_ledger=(
                            operation_context.memory_ledger
                            if operation_context is not None
                            else None
                        ),
                        stage_bytes_per_permit=self._io_chunk_bytes,
                    )
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
                self._finish_lifecycle_counter_locked("_admissions_inflight")
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
        """Return the close/callback condition created with the iterator."""
        return self._close_condition

    def _finish_lifecycle_counter_locked(self, name: str) -> None:
        """Retire one quiescence latch without hiding protocol underflow."""
        value = int(getattr(self, name))
        if value <= 0:
            self._protocol_violations += 1
            return
        setattr(self, name, value - 1)

    def _release_or_retain_storage_lease(
        self, storage_lease: Any, *, primary: BaseException | None = None
    ) -> None:
        """Release one staging lease or retain it for a later close retry."""
        try:
            storage_lease.release()
        except BaseException as cleanup_error:
            condition = self._lifecycle_condition()
            with condition:
                self._failed_storage_leases.append(storage_lease)
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
                        self._finish_lifecycle_counter_locked("_cleanup_callbacks_inflight")
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
                    self._finish_lifecycle_counter_locked("_cleanup_callbacks_inflight")
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
        try:
            size_bytes = max(1, int(self._manifest.estimated_chunk_bytes(start)))
        except (TypeError, ValueError, OverflowError):
            return 1
        desired = 1 + (size_bytes - 1) // self._io_chunk_bytes
        return max(1, min(self._policy.async_concurrency, desired))

    def _fill_prefetch_window(self) -> None:
        """Queue work through per-item claim/work/commit transactions."""
        self._assert_owner_process()
        condition = self._lifecycle_condition()
        deadline_ns = deadline_ns_from_timeout(
            self._remote_timeout_seconds,
            name="remote prefetch fill wait timeout",
            allow_zero=False,
        )
        with condition:
            while self._fill_in_progress:
                remaining = remaining_seconds(deadline_ns)
                if remaining <= 0 or not condition.wait(timeout=remaining):
                    raise RuntimeError("remote prefetch fill exceeded its deadline")
            if self._closed or self._close_started:
                return
            self._fill_in_progress = True

        try:
            while True:
                with condition:
                    if any((self._closed, self._close_started)):
                        return
                    target = _adaptive_parallel_slots(
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
                    storage_lease = (
                        self._manifest.try_acquire_storage_lease(start)
                        if coordinator is not None
                        else None
                    )
                    if coordinator is not None and storage_lease is None:
                        return
                    committed_next_start = max(
                        start + 1,
                        int(self._manifest.next_chunk_start(start)),
                    )
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
                        self._finish_lifecycle_counter_locked("_admissions_inflight")
                        condition.notify_all()
        finally:
            with condition:
                self._fill_in_progress = False
                condition.notify_all()

    def close(self) -> None:
        """Close staging transactionally after all cleanup publishers quiesce."""
        if os.getpid() != self._pid:
            return
        condition = self._lifecycle_condition()
        deadline_ns = deadline_ns_from_timeout(
            self._remote_timeout_seconds,
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
            while self._admissions_inflight or self._consumers_inflight:
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
                else self._remote_timeout_seconds
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
                    closer = self._session_closer
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
                retained_storage = self._failed_storage_leases
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
                if self._failed_storage_leases:
                    raise RuntimeError(
                        "remote staging storage release remains retryable after close failure"
                    )
                if self._protocol_violations:
                    raise RuntimeError(
                        "remote staging lifecycle protocol violation prevents clean close"
                    )
                self._futures = deque()
                self._coordinator = None
                self._closed = True
                ticket = self._finalizer_ticket
                cleanup = self._finalizer_capsule
                if ticket is not None and cleanup is not None:
                    cancel_prepared_finalizer_cleanup(cleanup)
                    self._finalizer_ticket = None
                    self._finalizer_capsule = None
        finally:
            with condition:
                self._close_in_progress = False
                condition.notify_all()

    def __del__(self) -> None:
        """Detach only bounded staging owners into a preallocated safe-point capsule."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if ticket is None or cleanup is None or getattr(self, "_closed", False):
                return
            cleanup.arg0 = getattr(self, "_futures", None)
            cleanup.arg1 = getattr(self, "_callbackless_storage_futures", None)
            cleanup.arg2 = getattr(self, "_failed_storage_leases", None)
            cleanup.arg3 = getattr(self, "_coordinator", None)
            cleanup.arg4 = getattr(self, "_download_session", None)
            cleanup.arg5 = getattr(self, "_session_closer", None)
            cleanup.arg6 = bool(getattr(self, "_owns_coordinator", False))
            if defer_prepared_finalizer_cleanup(cleanup):
                self._futures = None  # type: ignore[assignment]
                self._callbackless_storage_futures = None  # type: ignore[assignment]
                self._failed_storage_leases = None  # type: ignore[assignment]
                self._coordinator = None
                self._download_session = None
                self._session_closer = None
                cast(Any, self)._manifest = None
                self._finalizer_ticket = None
                self._finalizer_capsule = None
        except BaseException:
            pass


def open_staged_remote_chunks(
    manifest: RemoteNativeDirectorySourceManifest, *, start: int = 0
) -> RemoteChunkPrefetchIterator:
    """Open the staged-chunk context for one remote manifest."""
    return RemoteChunkPrefetchIterator(manifest, start=start)


class RemotePathSourceChunkProvider(RemotePathSourceChunkProviderBase):
    """Own retryable staged remote path-source chunks."""


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
    retained_chunks, remaining_start = take_prefetched_chunks(manifest)
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
