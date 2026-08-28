"""Retryable lazy provider for staged remote path-source chunks."""

from __future__ import annotations

import os
from collections import deque
from typing import Any, cast

from schema_sanitizer.core_impl.safe_errors import add_bounded_note

from ...core_impl.finalization import runtime_is_finalizing
from ...core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_finalizer_cleanup,
)
from ...input_impl.selection import unsupported_native_directory_ingestion
from ..input.directory_preparation import RemoteNativeDirectorySourceManifest
from .attached import source_plan_from_native_manifest
from .remote_cleanup import (
    close_deque_retryably,
    close_list_retryably,
    close_one_retryably,
    combine_cleanup_error,
    staged_file_count,
)


def _cleanup_remote_path_provider_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Close detached staged-chunk owners without retaining the provider graph."""
    current = capsule.arg0
    if current is not None:
        error = close_one_retryably(current)
        if error is not None:
            raise error
        capsule.arg0 = None
    retained = cast("deque[Any] | None", capsule.arg1)
    if retained is not None:
        while retained:
            item = retained[-1]
            error = close_one_retryably(item)
            if error is not None:
                raise error
            retained.pop()
        capsule.arg1 = None
    context = cast(Any, capsule.arg2)
    if context is not None:
        context.__exit__(None, None, None)
        capsule.arg2 = None
    donor = cast("RemotePathSourceChunkProviderBase | None", capsule.arg3)
    if donor is not None:
        donor.close_all()
        capsule.arg3 = None
    preserved = cast("list[Any] | None", capsule.arg4)
    if preserved is not None:
        while preserved:
            item = preserved[-1]
            error = close_one_retryably(item)
            if error is not None:
                raise error
            preserved.pop()
        capsule.arg4 = None


class RemotePathSourceChunkProviderBase:
    """Provide staged remote path-source chunks at native stream boundaries."""

    def __init__(
        self,
        *,
        retained_chunks: list[Any],
        remaining_manifest: RemoteNativeDirectorySourceManifest | None,
        retain_consumed_chunks: int = 0,
        retained_chunk_donor: RemotePathSourceChunkProviderBase | None = None,
        remaining_start: int = 0,
    ) -> None:
        """Store retained chunks and the lazy remaining remote manifest."""
        self._pid = os.getpid()
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
        finalizer_capsule = reserve_finalizer_cleanup(_cleanup_remote_path_provider_capsule)
        self._finalizer_capsule: PreparedFinalizerCleanup | None = finalizer_capsule
        self._finalizer_ticket: int | None = finalizer_capsule.ticket

    def _ensure_owner_process(self) -> None:
        """Reject inherited iterators before touching parent-owned resources."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            raise RuntimeError("remote chunk provider cannot be reused after fork")

    def next_sources(self) -> Any | None:
        """Return the next staged chunk as a native plan capsule or source tuples."""
        self._ensure_owner_process()
        if self._closed:
            return None
        self._close_current()
        staged = self._next_staged_chunk()
        if staged is None:
            self.close()
            return None
        self._current_staged = staged
        self._current_staged_preserved = False
        try:
            plan = source_plan_from_native_manifest(staged.manifest)
            if plan is None:
                raise unsupported_native_directory_ingestion()
            return plan.native_payload if plan.native_payload is not None else plan.payload
        except BaseException as exc:
            try:
                self._close_current(preserve=False)
            except BaseException as cleanup_error:
                add_bounded_note(
                    exc,
                    "remote staged chunk cleanup also failed after planning",
                    cleanup_error,
                )
            raise

    def close(self) -> None:
        """Close source resources and retain every failed owner for retry."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            return
        self._closed = True
        error: BaseException | None = None
        try:
            self._close_current()
        except BaseException as exc:
            error = combine_cleanup_error(error, exc)
        error = combine_cleanup_error(error, close_deque_retryably(self._retained_chunks))
        try:
            self._close_remaining_context()
        except BaseException as exc:
            error = combine_cleanup_error(error, exc)
        donor = self._retained_chunk_donor
        if donor is not None:
            try:
                donor.close_all()
            except BaseException as exc:
                error = combine_cleanup_error(error, exc)
            else:
                self._retained_chunk_donor = None
        if error is not None:
            raise error
        self._retire_finalizer_if_clean()

    def __del__(self) -> None:
        """Detach only staged cleanup owners into a reserved safe-point capsule."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", None)
            cleanup = getattr(self, "_finalizer_capsule", None)
            if ticket is None or cleanup is None:
                return
            cleanup.arg0 = getattr(self, "_current_staged", None)
            cleanup.arg1 = getattr(self, "_retained_chunks", None)
            cleanup.arg2 = getattr(self, "_remaining_context", None)
            cleanup.arg3 = getattr(self, "_retained_chunk_donor", None)
            cleanup.arg4 = getattr(self, "_preserved_chunks", None)
            if defer_prepared_finalizer_cleanup(cleanup):
                self._current_staged = None
                self._retained_chunks = None  # type: ignore[assignment]
                self._remaining_context = None
                self._remaining_iter = None
                self._remaining_manifest = None
                self._retained_chunk_donor = None
                self._preserved_chunks = None  # type: ignore[assignment]
                self._finalizer_ticket = None
                self._finalizer_capsule = None
        except BaseException:
            pass

    def _retire_finalizer_if_clean(self) -> None:
        if not self.is_closed or self._preserved_chunks:
            return
        ticket = getattr(self, "_finalizer_ticket", None)
        cleanup = getattr(self, "_finalizer_capsule", None)
        if ticket is not None and cleanup is not None:
            cancel_prepared_finalizer_cleanup(cleanup)
            self._finalizer_ticket = None
            self._finalizer_capsule = None

    @property
    def is_closed(self) -> bool:
        """Return whether normal source cleanup has completed."""
        return bool(
            self._closed
            and self._current_staged is None
            and not self._retained_chunks
            and self._remaining_context is None
            and self._retained_chunk_donor is None
        )

    @property
    def preserved_file_count(self) -> int:
        """Return the number of files covered by preserved staged chunks."""
        return self._preserved_file_count

    def release_preserved_chunks(self) -> list[Any]:
        """Transfer preserved staged chunks after normal cleanup completes."""
        self._ensure_owner_process()
        if not self.is_closed:
            raise RuntimeError("remote chunk provider is not fully closed")
        preserved = self._preserved_chunks
        self._preserved_chunks = []
        self._preserved_file_count = 0
        self._retire_finalizer_if_clean()
        return preserved

    def close_all(self) -> None:
        """Close all provider resources, including preserved chunks."""
        error: BaseException | None = None
        try:
            self.close()
        except BaseException as exc:
            error = combine_cleanup_error(error, exc)
        error = combine_cleanup_error(error, close_list_retryably(self._preserved_chunks))
        self._preserved_file_count = sum(staged_file_count(item) for item in self._preserved_chunks)
        if error is not None:
            raise error
        self._retire_finalizer_if_clean()

    def _next_staged_chunk(self) -> Any | None:
        """Return the next retained or newly staged remote chunk."""
        self._adopt_preserved_probe_chunks()
        if self._retained_chunks:
            return self._retained_chunks.popleft()
        if self._remaining_manifest is None:
            return None
        if self._remaining_context is None:
            from .remote import open_staged_remote_chunks

            context = open_staged_remote_chunks(
                self._remaining_manifest,
                start=self._remaining_start,
            )
            iterator = context.__enter__()
            self._remaining_context = context
            self._remaining_iter = iterator
        if self._remaining_iter is None:
            return None
        try:
            return next(self._remaining_iter)
        except StopIteration:
            self._close_remaining_context()
            return None

    def _adopt_preserved_probe_chunks(self) -> None:
        """Reuse a bounded probe prefix once its donor is fully closed."""
        if self._donor_checked:
            return
        donor = self._retained_chunk_donor
        if donor is None:
            self._donor_checked = True
            return
        if not donor.is_closed:
            return
        self._remaining_start = donor.preserved_file_count
        self._retained_chunks.extend(donor.release_preserved_chunks())
        self._retained_chunk_donor = None
        self._donor_checked = True

    def _close_current(self, *, preserve: bool = True) -> None:
        """Close or preserve the currently opened staged chunk transactionally."""
        staged = self._current_staged
        if staged is None:
            return
        if (
            preserve
            and not self._current_staged_preserved
            and len(self._preserved_chunks) < self._retain_consumed_chunks
        ):
            self._preserved_chunks.append(staged)
            self._preserved_file_count += staged_file_count(staged)
            self._current_staged_preserved = True
        elif not self._current_staged_preserved:
            error = close_one_retryably(staged)
            if error is not None:
                raise error
        self._current_staged = None
        self._current_staged_preserved = False

    def _close_remaining_context(self) -> None:
        """Close the remaining chunk iterator context without losing ownership."""
        context = self._remaining_context
        if context is None:
            self._remaining_iter = None
            return
        context.__exit__(None, None, None)
        if self._remaining_context is context:
            self._remaining_context = None
            self._remaining_iter = None


__all__ = ["RemotePathSourceChunkProviderBase"]
