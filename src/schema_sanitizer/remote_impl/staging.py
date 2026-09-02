"""Owned local staging for remote inputs, outputs, and object transfers.

It owns temporary paths for remote inputs and outputs, coordinates single or multi
transfers, and publishes only after complete success.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from inspect import signature
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from ..core_impl.execution_policy import normalize_threading_mode
from ..core_impl.memory_budget import memory_budget
from ..core_impl.resource_lifecycle import _cleanup_with_note
from ..core_impl.temporary_storage import (
    StreamingStorageReservation,
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
from ..sources.models import RemoteFile
from . import routing, sync_backend
from .async_bridge import run_sync
from .directory_downloads import RemoteDirectoryDownloadSession, download_files_to_directory
from .io_footprint import RemoteIoFootprint
from .transfer_dispatch import download_single_file, upload_file
from .transport import check_download_size

if TYPE_CHECKING:
    from ..api_impl.operation_context import OperationExecutionContext


from .staging_paths import (
    RemoteOutputTarget,
    StagedPath,
    create_temp_directory_path,
    create_temp_file_path,
)


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
            else operation_context.run_remote(
                metadata_operation,
                permit_label="remote_file_metadata",
            )
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
    storage_reservation = StreamingStorageReservation(
        lease,
        initial_credit_bytes=estimate,
        path=temp.path,
        quantum_bytes=memory_budget(memory_limit_bytes).io_chunk_bytes,
    )
    try:
        if single:

            def operation_sync() -> None:
                """Download the file through the strict blocking provider backend."""
                sync_backend.download_single_file(
                    metadata or uri,
                    temp.path,
                    memory_limit_bytes=memory_limit_bytes,
                    storage_reservation=storage_reservation,
                )

            if operation_context is None:
                operation_sync()
            else:
                operation_context.run_remote_sync(
                    operation_sync,
                    permit_label="remote_single_file_download",
                    footprint=RemoteIoFootprint(local_file_fds=1),
                )
        else:

            def operation():
                """Download the file on the operation-owned event loop."""
                return download_single_file(
                    metadata or uri,
                    temp.path,
                    memory_limit_bytes=memory_limit_bytes,
                    threading_mode=threading_mode,
                    storage_reservation=storage_reservation,
                )

            if operation_context is None:
                run_sync(operation(), threading_mode=threading_mode)
            else:
                operation_context.run_remote_transfer(
                    operation,
                    estimated_bytes=estimate,
                    permit_label="remote_single_file_download",
                    network_fds=0,
                    local_file_fds=1,
                )
        check_download_size(uri, Path(temp.path).stat().st_size, memory_limit_bytes)
        temp.reserve_actual_size(pool, label=f"remote input {uri!r}")
    except BaseException as exc:
        _cleanup_with_note(exc, temp, label="remote single-file staging cleanup also failed")
        raise
    return temp


def stage_remote_files_to_directory_sync(
    files: Sequence[RemoteFile],
    *,
    memory_limit_bytes: int | None,
    storage_lease: TemporaryStorageLease | None = None,
) -> StagedPath:
    """Stage selected files through the strict blocking provider backend."""
    selected = files
    if not selected:
        raise ValueError("remote directory input found no matching files")
    temp_dir = (
        create_temp_directory_path()
        if storage_lease is None
        else create_temp_directory_path(storage_lease=storage_lease)
    )
    try:
        downloader = sync_backend.download_files_to_directory
        kwargs: dict[str, object] = {"memory_limit_bytes": memory_limit_bytes}
        if "storage_lease" in signature(downloader).parameters:
            kwargs["storage_lease"] = storage_lease
        cast(Callable[..., None], downloader)(selected, temp_dir.path, **kwargs)
        if storage_lease is not None:
            storage_lease.resize(temp_dir._actual_size(), path=temp_dir.path)
    except BaseException as exc:
        _cleanup_with_note(exc, temp_dir, label="remote directory staging cleanup also failed")
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
    selected = files
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
                storage_lease=storage_lease,
            )
        else:
            if storage_lease is None:
                await download_session.download_files(selected, temp_dir.path)
            else:
                await download_session.download_files(
                    selected, temp_dir.path, storage_lease=storage_lease
                )
        if storage_lease is not None:
            storage_lease.resize(temp_dir._actual_size(), path=temp_dir.path)
    except BaseException as exc:
        _cleanup_with_note(
            exc, temp_dir, label="remote async directory staging cleanup also failed"
        )
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
    selected = files
    budget = memory_budget(memory_limit_bytes)
    estimated_bytes = sum(
        file.size if isinstance(file.size, int) and file.size >= 0 else budget.io_chunk_bytes
        for file in selected
    )
    lease = storage_lease
    if lease is None:
        pool = (
            operation_context.temporary_storage
            if operation_context is not None
            else TemporaryStoragePermitPool(memory_limit_bytes)
        )
        lease = pool.acquire(
            estimated_bytes,
            label="remote source directory packet",
            artifact_count=len(selected) + 1,
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
            return operation_context.run_remote_sync(
                operation_sync,
                permit_label="remote_directory_download",
                footprint=RemoteIoFootprint(local_file_fds=1),
            )

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
        transfer_fds = max(1, min(operation_context.policy.async_concurrency, len(selected)))
        return operation_context.run_remote_transfer(
            operation,
            estimated_bytes=estimated_bytes,
            permit_label="remote_directory_download",
            network_fds=0,
            local_file_fds=transfer_fds,
        )
    except BaseException as exc:
        _cleanup_with_note(
            exc, lease, label="remote directory lease rollback also failed", method="release"
        )
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
            else operation_context.run_remote(
                list_operation,
                permit_label="remote_directory_list",
            )
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
                    target.operation_context.run_remote_sync(
                        operation_sync,
                        permit_label="remote_output_upload",
                        footprint=RemoteIoFootprint(local_file_fds=1),
                    )
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
                    output_size = Path(target.local_path).stat().st_size
                    target.operation_context.run_remote_transfer(
                        operation,
                        estimated_bytes=output_size,
                        permit_label="remote_output_upload",
                        network_fds=0,
                        local_file_fds=1,
                    )
    except BaseException as exc:
        _cleanup_with_note(exc, target, label="remote output cleanup also failed")
        raise
    else:
        target.close()


def cleanup_output_target(target: RemoteOutputTarget) -> None:
    """Release a staged output target after a failed write."""
    target.close()
