"""Remote source-plan probing and lazy staged-chunk execution."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from concurrent.futures import CancelledError, Future
from typing import Any

from ...core_impl.execution_policy import execution_policy
from ...core_impl.resource_lifecycle import _close_suppressing_errors
from ...input_impl.selection import unsupported_native_directory_ingestion
from ...remote_impl.io_coordinator import RemoteIoCoordinator
from ..input.directory_preparation import RemoteNativeDirectorySourceManifest
from .attached import source_plan_from_native_manifest


class RemoteChunkPrefetchIterator:
    """Iterate staged remote chunks through one operation-owned I/O loop."""

    def __init__(self, manifest: Any, *, start: int = 0) -> None:
        """Create a staging iterator for a remote native manifest."""
        self._manifest = manifest
        self._policy = execution_policy(
            getattr(manifest, "threading_mode", "single"),
            getattr(manifest, "memory_limit_bytes", None),
        )
        self._prefetch_chunks = self._policy.remote_chunk_prefetch
        self._coordinator: RemoteIoCoordinator | None = None
        self._owns_coordinator = False
        self._download_session: Any | None = None
        self._futures: deque[Future[Any]] = deque()
        self._next_start = max(0, int(start))
        self._closed = False
        self._started = False

    def __enter__(self) -> RemoteChunkPrefetchIterator:
        """Enter the staging context."""
        self._ensure_started()
        return self

    def __exit__(self, *_exc: object) -> None:
        """Close pending staged chunks."""
        self.close()

    def __iter__(self) -> RemoteChunkPrefetchIterator:
        """Return the iterator."""
        self._ensure_started()
        return self

    def __next__(self) -> Any:
        """Return the next staged native directory manifest."""
        self._ensure_started()
        while True:
            if self._closed:
                raise StopIteration
            if not self._futures:
                self._fill_prefetch_window()
            if not self._futures:
                self.close()
                raise StopIteration
            future = self._futures.popleft()
            try:
                staged = future.result()
            except Exception:
                self.close()
                raise
            self._fill_prefetch_window()
            if staged is None:
                self.close()
                raise StopIteration
            return staged

    def _ensure_started(self) -> None:
        """Start the operation-owned remote scheduler once."""
        if self._started:
            return
        self._started = True
        if not self._policy.is_single:
            open_session = getattr(self._manifest, "open_staging_session", None)
            stage_async = getattr(self._manifest, "stage_chunk_async", None)
            if not callable(open_session) or not callable(stage_async):
                raise TypeError("multi remote chunk staging requires an async manifest session")
            operation_context = getattr(self._manifest, "operation_context", None)
            shared = getattr(operation_context, "remote_coordinator", None)
            if shared is not None:
                self._coordinator = shared
                download_session = open_session()
                self._download_session = download_session
                self._coordinator.submit(lambda _context: download_session.__aenter__()).result()
            else:
                self._coordinator = RemoteIoCoordinator(open_session)
                self._owns_coordinator = True
        self._fill_prefetch_window()

    def _submit_stage(self, start: int, storage_lease: Any | None) -> Future[Any]:
        """Submit one chunk to the inline or shared-I/O executor."""
        coordinator = self._coordinator
        if coordinator is None:
            future: Future[Any] = Future()
            try:
                future.set_result(self._manifest.stage_chunk(start))
            except Exception as exc:
                if storage_lease is not None:
                    storage_lease.release()
                future.set_exception(exc)
            return future

        async def stage(coordinator_context: Any) -> Any:
            """Stage one chunk through the shared provider session."""
            download_session = self._download_session or coordinator_context
            if storage_lease is None:
                return await self._manifest.stage_chunk_async(start, download_session)
            return await self._manifest.stage_chunk_async(
                start,
                download_session,
                storage_lease=storage_lease,
            )

        try:
            future = coordinator.submit(stage)
        except BaseException:
            if storage_lease is not None:
                storage_lease.release()
            raise
        if storage_lease is not None:

            def release_failed_reservation(done: Future[Any]) -> None:
                """Return a pre-acquired permit if staging never transfers it."""
                try:
                    failed = done.cancelled() or done.exception() is not None
                except CancelledError:
                    failed = True
                if failed:
                    storage_lease.release()

            future.add_done_callback(release_failed_reservation)
        return future

    def _fill_prefetch_window(self) -> None:
        """Queue staging work until the lookahead window is full."""
        if self._closed:
            return
        target = max(1, self._prefetch_chunks)
        while len(self._futures) < target and self._next_start < len(self._manifest.files):
            start = self._next_start
            acquire = getattr(self._manifest, "try_acquire_storage_lease", None)
            storage_lease = (
                acquire(start) if self._coordinator is not None and callable(acquire) else None
            )
            if self._coordinator is not None and callable(acquire) and storage_lease is None:
                break
            next_start = getattr(self._manifest, "next_chunk_start", None)
            if callable(next_start):
                self._next_start = max(start + 1, int(next_start(start)))
            else:
                self._next_start += max(1, self._manifest.chunk_size)
            self._futures.append(self._submit_stage(start, storage_lease))

    def close(self) -> None:
        """Cancel, drain, and clean every unconsumed staged chunk."""
        if self._closed:
            return
        self._closed = True
        futures = tuple(self._futures)
        self._futures.clear()
        for future in futures:
            if not future.done():
                future.cancel()

        coordinator = self._coordinator
        self._coordinator = None
        for future in futures:
            if future.cancelled():
                continue
            try:
                staged = future.result()
            except (CancelledError, Exception):
                continue
            if staged is not None:
                _close_suppressing_errors(staged)

        download_session = self._download_session
        self._download_session = None
        if coordinator is not None and download_session is not None:
            try:
                coordinator.submit(
                    lambda _context: download_session.__aexit__(None, None, None)
                ).result()
            except Exception:
                pass
        if coordinator is not None and self._owns_coordinator:
            coordinator.close()
        self._owns_coordinator = False


def iter_staged_remote_chunks(manifest: Any, *, start: int = 0) -> Iterator[Any]:
    """Return a context-managed iterator over staged native remote chunks."""
    return RemoteChunkPrefetchIterator(manifest, start=start)


def open_staged_remote_chunks(
    manifest: RemoteNativeDirectorySourceManifest,
    *,
    start: int = 0,
) -> Any:
    """Open the staged-chunk context for one remote manifest."""
    return iter_staged_remote_chunks(manifest, start=start)


class RemotePathSourceChunkProvider:
    """Provide staged remote path-source chunks at native stream boundaries."""

    def __init__(
        self,
        *,
        retained_chunks: list[Any],
        remaining_manifest: RemoteNativeDirectorySourceManifest | None,
        retain_consumed_chunks: int = 0,
        retained_chunk_donor: RemotePathSourceChunkProvider | None = None,
        remaining_start: int = 0,
    ) -> None:
        """Store retained chunks and the lazy remaining remote manifest."""
        self._retained_chunks = deque(retained_chunks)
        self._remaining_manifest = remaining_manifest
        self._current_staged: Any | None = None
        self._current_staged_preserved = False
        self._remaining_context: Any | None = None
        self._remaining_iter: Any | None = None
        self._remaining_start = max(0, int(remaining_start))
        self._retain_consumed_chunks = max(0, int(retain_consumed_chunks))
        self._retained_chunk_donor = retained_chunk_donor
        self._donor_checked = False
        self._preserved_chunks: list[Any] = []
        self._preserved_file_count = 0
        self._closed = False

    def next_sources(self) -> Any | None:
        """Return the next staged chunk as a native plan capsule or source tuples."""
        if self._closed:
            return None
        self._close_current()
        staged = self._next_staged_chunk()
        if staged is None:
            self.close()
            return None
        try:
            plan = source_plan_from_native_manifest(staged.manifest)
            if plan is None:
                raise unsupported_native_directory_ingestion()
            self._current_staged = staged
            self._current_staged_preserved = False
            return plan.native_payload if plan.native_payload is not None else plan.payload
        except Exception:
            _close_suppressing_errors(staged)
            raise

    def close(self) -> None:
        """Close current, retained, and not-yet-opened staged resources."""
        if self._closed:
            return
        self._closed = True
        self._close_current()
        while self._retained_chunks:
            _close_suppressing_errors(self._retained_chunks.pop())
        self._close_remaining_context()
        donor = self._retained_chunk_donor
        if donor is not None:
            donor.close_all()
            self._retained_chunk_donor = None

    def __del__(self) -> None:
        """Best-effort cleanup for abandoned providers."""
        try:
            self.close()
        except Exception:
            pass

    @property
    def is_closed(self) -> bool:
        """Return whether this provider has finished its source lifecycle."""
        return self._closed

    @property
    def preserved_file_count(self) -> int:
        """Return the number of files covered by preserved staged chunks."""
        return self._preserved_file_count

    def release_preserved_chunks(self) -> list[Any]:
        """Transfer preserved staged chunks to the caller."""
        preserved = self._preserved_chunks
        self._preserved_chunks = []
        self._preserved_file_count = 0
        return preserved

    def close_all(self) -> None:
        """Close all provider resources, including preserved chunks."""
        self.close()
        while self._preserved_chunks:
            _close_suppressing_errors(self._preserved_chunks.pop())
        self._preserved_file_count = 0

    def _next_staged_chunk(self) -> Any | None:
        """Return the next retained or newly staged remote chunk."""
        self._adopt_preserved_probe_chunks()
        if self._retained_chunks:
            return self._retained_chunks.popleft()
        if self._remaining_manifest is None:
            return None
        if self._remaining_context is None:
            self._remaining_context = open_staged_remote_chunks(
                self._remaining_manifest,
                start=self._remaining_start,
            )
            self._remaining_iter = self._remaining_context.__enter__()
        if self._remaining_iter is None:
            return None
        try:
            return next(self._remaining_iter)
        except StopIteration:
            self._close_remaining_context()
            return None

    def _adopt_preserved_probe_chunks(self) -> None:
        """Reuse a bounded probe prefix when the donor has fully closed."""
        if self._donor_checked:
            return
        self._donor_checked = True
        donor = self._retained_chunk_donor
        if donor is None or not donor.is_closed:
            return
        self._remaining_start = donor.preserved_file_count
        self._retained_chunks.extend(donor.release_preserved_chunks())
        donor.close_all()
        self._retained_chunk_donor = None

    def _close_current(self) -> None:
        """Close or preserve the currently opened staged chunk."""
        if (
            self._current_staged is not None
            and not self._current_staged_preserved
            and len(self._preserved_chunks) < self._retain_consumed_chunks
        ):
            self._preserved_chunks.append(self._current_staged)
            self._preserved_file_count += len(self._current_staged.manifest.source_batch.sources)
            self._current_staged_preserved = True
        elif not self._current_staged_preserved:
            _close_suppressing_errors(self._current_staged)
        self._current_staged = None
        self._current_staged_preserved = False

    def _close_remaining_context(self) -> None:
        """Close the remaining chunk iterator context."""
        context = self._remaining_context
        self._remaining_context = None
        self._remaining_iter = None
        if context is not None:
            try:
                context.__exit__(None, None, None)
            except Exception:
                pass


def prefetched_remote_chunks(
    manifest: RemoteNativeDirectorySourceManifest,
) -> tuple[list[Any], int]:
    """Take an optional partition-lookahead prefix from one remote manifest."""
    take = getattr(manifest, "take_prefetched_chunks", None)
    if not callable(take):
        return [], 0
    chunks, file_count = take()
    return list(chunks), max(0, int(file_count))


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
    except Exception:
        provider.close_all()
        raise
