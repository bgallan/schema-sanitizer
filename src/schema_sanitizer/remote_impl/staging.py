"""Owned local staging for remote inputs, outputs, and object transfers."""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..core_impl.execution_policy import normalize_threading_mode
from ..core_impl.temporary_storage import (
    TemporaryStorageLease,
    TemporaryStoragePermitPool,
)
from ..core_impl.uris import (
    local_path_from_file_uri,
    looks_like_file_uri,
    looks_like_remote_uri,
    normalize_extensions,
    suffix_from_uri,
)
from ..input_impl.directory_inputs import RemoteFile
from . import routing, sync_backend
from .directory_downloads import RemoteDirectoryDownloadSession, download_files_to_directory
from .transfer_dispatch import download_single_file, upload_file
from .transport import check_download_size, run_sync

if TYPE_CHECKING:
    from ..api_impl.operation_context import OperationExecutionContext


class StagedPath:
    """Own a local temporary path that mirrors a remote input or output."""

    def __init__(
        self,
        path: str,
        *,
        is_dir: bool = False,
        source_file_by_name: dict[str, str] | None = None,
        storage_lease: TemporaryStorageLease | None = None,
    ) -> None:
        """Store the temporary path and deletion mode."""
        self.path = path
        self.is_dir = is_dir
        self.source_file_by_name = source_file_by_name
        self.storage_lease = storage_lease
        self._closed = False

    def reserve_actual_size(self, pool: TemporaryStoragePermitPool, *, label: str) -> None:
        """Acquire or resize the permit to the exact staged filesystem size."""
        size = self._actual_size()
        if self.storage_lease is None:
            self.storage_lease = pool.acquire(size, label=label, path=self.path)
            return
        self.storage_lease.resize(size, path=self.path)

    def _actual_size(self) -> int:
        """Return the current file or recursive directory byte size."""
        path = Path(self.path)
        if not self.is_dir:
            return path.stat().st_size
        total = 0
        for child in path.rglob("*"):
            if child.is_file():
                total += child.stat().st_size
        return total

    def close(self) -> None:
        """Delete the temporary path."""
        if self._closed:
            return
        self._closed = True
        if self.is_dir:
            shutil.rmtree(self.path, ignore_errors=True)
        else:
            try:
                Path(self.path).unlink(missing_ok=True)
            except OSError:
                pass
        if self.storage_lease is not None:
            self.storage_lease.release()
            self.storage_lease = None


@dataclass(slots=True)
class RemoteOutputTarget:
    """Local output target plus optional remote upload destination."""

    local_path: str
    remote_uri: str | None = None
    temp: StagedPath | None = None
    memory_limit_bytes: int | None = None
    threading_mode: str = "single"
    operation_context: OperationExecutionContext | None = None

    def close(self) -> None:
        """Release the temporary output path."""
        if self.temp is not None:
            self.temp.close()
            self.temp = None


def create_temp_file_path(
    *, suffix: str, storage_lease: TemporaryStorageLease | None = None
) -> StagedPath:
    """Create an owned temporary file path."""
    fd, path = tempfile.mkstemp(
        prefix="schema-sanitizer-",
        suffix=suffix,
    )
    os.close(fd)
    return StagedPath(path, storage_lease=storage_lease)


def create_temp_directory_path(*, storage_lease: TemporaryStorageLease | None = None) -> StagedPath:
    """Create an owned temporary directory path."""
    path = tempfile.mkdtemp(prefix="schema-sanitizer-")
    return StagedPath(path, is_dir=True, storage_lease=storage_lease)


def stage_remote_single_file(
    uri: str,
    *,
    memory_limit_bytes: int | None,
    threading_mode: str = "single",
    operation_context: OperationExecutionContext | None = None,
) -> StagedPath:
    """Download one remote file to a local temporary path."""
    pool = (
        operation_context.temporary_storage
        if operation_context is not None
        else TemporaryStoragePermitPool(memory_limit_bytes)
    )
    from ..core_impl.memory_budget import memory_budget

    single = normalize_threading_mode(threading_mode) == "single"
    if single:

        def metadata_operation_sync():
            """Read object size through the strict blocking provider backend."""
            return sync_backend.remote_file_metadata(
                uri,
                memory_limit_bytes=memory_limit_bytes,
            )

        metadata = (
            metadata_operation_sync()
            if operation_context is None
            else operation_context.run_remote_sync(metadata_operation_sync)
        )
    else:

        def metadata_operation():
            """Read object size on the operation-owned event loop."""
            return routing.remote_file_metadata(
                uri,
                memory_limit_bytes=memory_limit_bytes,
                threading_mode=threading_mode,
            )

        metadata = (
            run_sync(metadata_operation(), threading_mode=threading_mode)
            if operation_context is None
            else operation_context.run_remote(metadata_operation)
        )
    known_size = getattr(metadata, "size", None)
    estimate = (
        known_size
        if isinstance(known_size, int) and known_size >= 0
        else memory_budget(memory_limit_bytes).io_chunk_bytes
    )
    check_download_size(uri, known_size, memory_limit_bytes)
    lease = pool.acquire(estimate, label=f"remote input {uri!r}")
    temp = create_temp_file_path(suffix=suffix_from_uri(uri), storage_lease=lease)
    try:
        if single:

            def operation_sync() -> None:
                """Download the file through the strict blocking provider backend."""
                sync_backend.download_single_file(
                    uri,
                    temp.path,
                    memory_limit_bytes=memory_limit_bytes,
                )

            if operation_context is None:
                operation_sync()
            else:
                operation_context.run_remote_sync(operation_sync)
        else:

            def operation():
                """Download the file on the operation-owned event loop."""
                return download_single_file(
                    uri,
                    temp.path,
                    memory_limit_bytes=memory_limit_bytes,
                    threading_mode=threading_mode,
                )

            if operation_context is None:
                run_sync(operation(), threading_mode=threading_mode)
            else:
                operation_context.run_remote(operation)
        check_download_size(uri, Path(temp.path).stat().st_size, memory_limit_bytes)
        temp.reserve_actual_size(pool, label=f"remote input {uri!r}")
    except BaseException:
        temp.close()
        raise
    return temp


def stage_remote_files_to_directory_sync(
    files: Sequence[RemoteFile],
    *,
    memory_limit_bytes: int | None,
    storage_lease: TemporaryStorageLease | None = None,
) -> StagedPath:
    """Stage selected files through the strict blocking provider backend."""
    selected = list(files)
    if not selected:
        raise ValueError("remote directory input found no matching files")
    temp_dir = (
        create_temp_directory_path()
        if storage_lease is None
        else create_temp_directory_path(storage_lease=storage_lease)
    )
    try:
        sync_backend.download_files_to_directory(
            selected,
            temp_dir.path,
            memory_limit_bytes=memory_limit_bytes,
        )
        if storage_lease is not None:
            storage_lease.resize(temp_dir._actual_size(), path=temp_dir.path)
    except BaseException:
        temp_dir.close()
        raise
    temp_dir.source_file_by_name = {file.name: file.uri for file in selected}
    return temp_dir


async def stage_remote_files_to_directory_async(
    files: Sequence[RemoteFile],
    *,
    memory_limit_bytes: int | None,
    threading_mode: str = "single",
    download_session: RemoteDirectoryDownloadSession | None = None,
    storage_lease: TemporaryStorageLease | None = None,
) -> StagedPath:
    """Download selected remote files into one owned temporary directory."""
    selected = list(files)
    if not selected:
        raise ValueError("remote directory input found no matching files")
    temp_dir = (
        create_temp_directory_path()
        if storage_lease is None
        else create_temp_directory_path(storage_lease=storage_lease)
    )
    try:
        if download_session is None:
            await download_files_to_directory(
                selected,
                temp_dir.path,
                memory_limit_bytes=memory_limit_bytes,
                threading_mode=threading_mode,
            )
        else:
            await download_session.download_files(selected, temp_dir.path)
        if storage_lease is not None:
            storage_lease.resize(temp_dir._actual_size(), path=temp_dir.path)
    except BaseException:
        temp_dir.close()
        raise
    temp_dir.source_file_by_name = {file.name: file.uri for file in selected}
    return temp_dir


def stage_remote_files_to_directory(
    files: Sequence[RemoteFile],
    *,
    memory_limit_bytes: int | None,
    threading_mode: str = "single",
    operation_context: OperationExecutionContext | None = None,
    storage_lease: TemporaryStorageLease | None = None,
) -> StagedPath:
    """Synchronously stage selected remote files into one temporary directory."""
    selected = list(files)
    lease = storage_lease
    if lease is None:
        from ..core_impl.memory_budget import memory_budget

        budget = memory_budget(memory_limit_bytes)
        estimated_bytes = sum(
            file.size if isinstance(file.size, int) and file.size >= 0 else budget.io_chunk_bytes
            for file in selected
        )
        pool = (
            operation_context.temporary_storage
            if operation_context is not None
            else TemporaryStoragePermitPool(memory_limit_bytes)
        )
        lease = pool.acquire(
            estimated_bytes,
            label="remote source directory packet",
        )

    single = normalize_threading_mode(threading_mode) == "single"
    try:
        if single:

            def operation_sync() -> StagedPath:
                """Stage files through the strict blocking provider backend."""
                return stage_remote_files_to_directory_sync(
                    selected,
                    memory_limit_bytes=memory_limit_bytes,
                    storage_lease=lease,
                )

            if operation_context is None:
                return operation_sync()
            return operation_context.run_remote_sync(operation_sync)

        def operation():
            """Stage selected files on the operation-owned event loop."""
            return stage_remote_files_to_directory_async(
                selected,
                memory_limit_bytes=memory_limit_bytes,
                threading_mode=threading_mode,
                storage_lease=lease,
            )

        if operation_context is None:
            return run_sync(operation(), threading_mode=threading_mode)
        return operation_context.run_remote(operation)
    except BaseException:
        lease.release()
        raise


def stage_remote_parquet_directory(
    uri: str,
    *,
    suffixes: Sequence[str],
    memory_limit_bytes: int | None,
    threading_mode: str = "single",
    operation_context: OperationExecutionContext | None = None,
) -> StagedPath:
    """Download a remote Parquet directory to a local temporary directory."""

    single = normalize_threading_mode(threading_mode) == "single"
    if single:

        def list_operation_sync() -> list[RemoteFile]:
            """List Parquet files through the strict blocking provider backend."""
            return sync_backend.list_remote_directory(
                uri,
                suffixes,
                memory_limit_bytes=memory_limit_bytes,
            )

        files = (
            list_operation_sync()
            if operation_context is None
            else operation_context.run_remote_sync(list_operation_sync)
        )
    else:

        def list_operation():
            """List Parquet files on the operation-owned event loop."""
            return routing.list_remote_directory(
                uri,
                suffixes,
                memory_limit_bytes=memory_limit_bytes,
                threading_mode=threading_mode,
            )

        files = (
            run_sync(list_operation(), threading_mode=threading_mode)
            if operation_context is None
            else operation_context.run_remote(list_operation)
        )
    if not files:
        expected = " or ".join(normalize_extensions(suffixes))
        raise ValueError(f"parquet remote directory input found no {expected} files in: {uri}")
    return stage_remote_files_to_directory(
        files,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
        operation_context=operation_context,
    )


def prepare_output_target(
    path: Any,
    *,
    memory_limit_bytes: int | None,
    threading_mode: str = "single",
    operation_context: OperationExecutionContext | None = None,
) -> RemoteOutputTarget:
    """Return a local target, staging remote destinations when needed."""
    raw = os.fspath(path)
    if looks_like_file_uri(raw):
        return RemoteOutputTarget(
            local_path=local_path_from_file_uri(raw),
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
            operation_context=operation_context,
        )
    if not looks_like_remote_uri(raw):
        return RemoteOutputTarget(
            local_path=raw,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
            operation_context=operation_context,
        )
    temp = create_temp_file_path(suffix=suffix_from_uri(raw, default=".tmp"))
    return RemoteOutputTarget(
        local_path=temp.path,
        remote_uri=raw,
        temp=temp,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
        operation_context=operation_context,
    )


def finalize_output_target(
    target: RemoteOutputTarget,
    *,
    before_remote_upload: Callable[[], None] | None = None,
) -> None:
    """Upload a staged output target after an optional safe overlap trigger."""
    try:
        remote_uri = target.remote_uri
        if remote_uri is not None:
            if target.temp is not None:
                pool = (
                    target.operation_context.temporary_storage
                    if target.operation_context is not None
                    else TemporaryStoragePermitPool(target.memory_limit_bytes)
                )
                target.temp.reserve_actual_size(
                    pool,
                    label=f"remote output {remote_uri!r}",
                )
            if before_remote_upload is not None:
                before_remote_upload()

            single = normalize_threading_mode(target.threading_mode) == "single"
            if single:

                def operation_sync() -> None:
                    """Upload through the strict blocking provider backend."""
                    sync_backend.upload_file(
                        target.local_path,
                        remote_uri,
                        memory_limit_bytes=target.memory_limit_bytes,
                    )

                if target.operation_context is None:
                    operation_sync()
                else:
                    target.operation_context.run_remote_sync(operation_sync)
            else:

                def operation():
                    """Upload the completed output on the operation-owned event loop."""
                    return upload_file(
                        target.local_path,
                        remote_uri,
                        memory_limit_bytes=target.memory_limit_bytes,
                        threading_mode=target.threading_mode,
                    )

                if target.operation_context is None:
                    run_sync(operation(), threading_mode=target.threading_mode)
                else:
                    target.operation_context.run_remote(operation)
    finally:
        target.close()


def cleanup_output_target(target: RemoteOutputTarget) -> None:
    """Release a staged output target after a failed write."""
    target.close()
