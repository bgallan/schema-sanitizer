"""Remote source-plan probing and lazy staged-chunk execution."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from ...core_impl.async_scheduler import read_int_env
from ...core_impl.resource_lifecycle import _close_suppressing_errors
from ...input_impl.selection import unsupported_native_directory_ingestion
from ..input.directory_preparation import RemoteNativeDirectorySourceManifest
from .attached import source_plan_from_native_manifest


def remote_chunk_prefetch_count() -> int:
    """Return how many remote directory chunks to stage ahead."""
    return read_int_env("SCHEMA_SANITIZER_REMOTE_CHUNK_PREFETCH_CHUNKS", 1)


class RemoteChunkPrefetchIterator:
    """Iterate staged remote chunks while prefetching bounded lookahead."""

    def __init__(self, manifest: Any, *, prefetch_chunks: int | None = None) -> None:
        """Create a staging iterator for a remote native manifest."""
        self._manifest = manifest
        self._prefetch_chunks = max(
            0,
            remote_chunk_prefetch_count() if prefetch_chunks is None else prefetch_chunks,
        )
        self._executor: ThreadPoolExecutor | None = None
        self._futures: deque[Future[Any]] = deque()
        self._next_start = 0
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
        """Start prefetching once."""
        if self._started:
            return
        self._started = True
        if self._prefetch_chunks > 0:
            self._executor = ThreadPoolExecutor(
                max_workers=self._prefetch_chunks,
                thread_name_prefix="schema-sanitizer-remote-stage",
            )
        self._fill_prefetch_window()

    def _submit_stage(self, start: int) -> Future[Any]:
        """Submit one chunk staging operation."""
        if self._executor is None:
            future: Future[Any] = Future()
            try:
                future.set_result(self._manifest.stage_chunk(start))
            except Exception as exc:
                future.set_exception(exc)
            return future
        return self._executor.submit(self._manifest.stage_chunk, start)

    def _fill_prefetch_window(self) -> None:
        """Queue staging work until the lookahead window is full."""
        if self._closed:
            return
        target = max(1, self._prefetch_chunks)
        while len(self._futures) < target and self._next_start < len(self._manifest.files):
            start = self._next_start
            self._next_start += max(1, self._manifest.chunk_size)
            self._futures.append(self._submit_stage(start))

    def close(self) -> None:
        """Close pending staged chunks and stop the executor."""
        if self._closed:
            return
        self._closed = True
        for future in self._futures:
            if not future.done():
                future.cancel()
                continue
            try:
                staged = future.result()
            except Exception:
                continue
            if staged is not None:
                staged.close()
        self._futures.clear()
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None


def iter_staged_remote_chunks(
    manifest: Any,
    *,
    prefetch_chunks: int | None = None,
) -> Iterator[Any]:
    """Return a context-managed iterator over staged native remote chunks."""
    return RemoteChunkPrefetchIterator(manifest, prefetch_chunks=prefetch_chunks)


def open_staged_remote_chunks(manifest: RemoteNativeDirectorySourceManifest) -> Any:
    """Open the staged-chunk context for one remote manifest."""
    return iter_staged_remote_chunks(manifest)


class RemotePathSourceChunkProvider:
    """Provide staged remote path-source chunks at native stream boundaries."""

    def __init__(
        self,
        *,
        retained_chunks: list[Any],
        remaining_manifest: RemoteNativeDirectorySourceManifest | None,
        retain_consumed_chunks: int = 0,
    ) -> None:
        """Store retained chunks and the lazy remaining remote manifest."""
        self._retained_chunks = deque(retained_chunks)
        self._remaining_manifest = remaining_manifest
        self._current_staged: Any | None = None
        self._current_staged_preserved = False
        self._remaining_context: Any | None = None
        self._remaining_iter: Any | None = None
        self._retain_consumed_chunks = max(0, int(retain_consumed_chunks))
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

    def __del__(self) -> None:
        """Best-effort cleanup for abandoned providers."""
        try:
            self.close()
        except Exception:
            pass

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
        if self._retained_chunks:
            return self._retained_chunks.popleft()
        if self._remaining_manifest is None:
            return None
        if self._remaining_context is None:
            self._remaining_context = open_staged_remote_chunks(self._remaining_manifest)
            self._remaining_iter = self._remaining_context.__enter__()
        if self._remaining_iter is None:
            return None
        try:
            return next(self._remaining_iter)
        except StopIteration:
            self._close_remaining_context()
            return None

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
    provider = RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=manifest,
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
