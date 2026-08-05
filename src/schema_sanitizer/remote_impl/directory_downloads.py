"""Bounded provider sessions and file transfers for remote directories."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ..core_impl.async_scheduler import drain_ordered_indexed_results, retry_async
from ..core_impl.execution_policy import execution_policy
from ..core_impl.memory_budget import memory_budget
from ..core_impl.temporary_storage import (
    StreamingStorageReservation,
    TemporaryStorageLease,
)
from ..core_impl.uris import RemoteProvider, remote_provider
from ..input_impl.directory_inputs import RemoteFile
from .providers import azure, gcs, s3
from .transport import (
    check_download_size,
    open_aiohttp_session,
    write_response_to_file,
)


@dataclass(frozen=True, slots=True)
class DirectoryDownloadTuning:
    """Runtime controls for concurrent remote directory downloads."""

    concurrency: int
    window: int
    retries: int


def directory_download_tuning(
    memory_limit_bytes: int | None,
    threading_mode: str = "single",
) -> DirectoryDownloadTuning:
    """Derive remote download controls from the shared execution policy."""
    budget = memory_budget(memory_limit_bytes)
    policy = execution_policy(threading_mode, memory_limit_bytes)
    return DirectoryDownloadTuning(
        concurrency=policy.async_concurrency,
        window=policy.async_prefetch_files,
        retries=budget.async_retries,
    )


@dataclass(slots=True)
class DownloadContext:
    """Provider identity plus reusable handles for one staged directory."""

    provider: RemoteProvider
    client: Any = None
    manager: Any = None


class RemoteDirectoryDownloadSession:
    """Reuse one provider client and global transfer budget across staged chunks."""

    def __init__(
        self,
        files: Sequence[RemoteFile],
        *,
        memory_limit_bytes: int | None,
        threading_mode: str,
    ) -> None:
        """Store operation inputs until the async context is entered."""
        self._first_file = files[0] if files else None
        self._memory_limit_bytes = memory_limit_bytes
        self._threading_mode = threading_mode
        self._tuning = directory_download_tuning(memory_limit_bytes, threading_mode)
        policy = execution_policy(threading_mode, memory_limit_bytes)
        self._request_window = max(
            1,
            self._tuning.window // max(1, policy.remote_chunk_prefetch),
        )
        self._context: DownloadContext | None = None
        self._semaphore: asyncio.Semaphore | None = None

    async def __aenter__(self) -> RemoteDirectoryDownloadSession:
        """Open one reusable provider client and global request semaphore."""
        files = () if self._first_file is None else (self._first_file,)
        self._context = await provider_client_for_downloads(
            files,
            memory_limit_bytes=self._memory_limit_bytes,
            threading_mode=self._threading_mode,
        )
        if self._context is None:
            raise RuntimeError("remote download context was not created")
        self._semaphore = asyncio.Semaphore(self._tuning.concurrency)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Close the shared provider client after all transfers are drained."""
        context = self._context
        self._context = None
        self._semaphore = None
        await close_provider_client(context)

    async def download_files(
        self,
        files: Sequence[RemoteFile],
        directory: str,
        *,
        storage_lease: TemporaryStorageLease | None = None,
    ) -> None:
        """Download one chunk under the operation-wide transfer limit."""
        context = self._context
        semaphore = self._semaphore
        if context is None or semaphore is None:
            raise RuntimeError("remote directory download session is not open")
        await _download_files_with_context(
            files,
            directory,
            context=context,
            semaphore=semaphore,
            retries=self._tuning.retries,
            window=self._request_window,
            memory_limit_bytes=self._memory_limit_bytes,
            storage_lease=storage_lease,
        )


async def provider_client_for_downloads(
    files: Sequence[RemoteFile],
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> DownloadContext | None:
    """Open one reusable provider client or session for staged directories."""
    if not files:
        return None
    provider = remote_provider(files[0].uri)
    if provider is None:
        scheme = urlparse(files[0].uri).scheme.lower()
        raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")
    if provider == "gcs":
        client = await open_aiohttp_session(
            gcs.request_headers(),
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
        return DownloadContext(provider, client)
    if provider == "s3":
        manager = await s3.open_client()
        return DownloadContext(provider, await manager.__aenter__(), manager)
    if provider == "http":
        return DownloadContext(
            provider,
            await open_aiohttp_session(
                memory_limit_bytes=memory_limit_bytes,
                threading_mode=threading_mode,
            ),
        )
    if provider == "azure":
        return DownloadContext(provider, await azure.open_service(azure.parse_uri(files[0].uri)))
    raise ValueError(f"Unsupported remote provider: {provider!r}")


async def close_provider_client(context: DownloadContext | None) -> None:
    """Close a reusable provider client or session."""
    if context is None:
        return
    if context.manager is not None:
        await context.manager.__aexit__(None, None, None)
        return
    close = getattr(context.client, "close", None)
    if close is not None:
        result = close()
        if asyncio.iscoroutine(result):
            await result
        return
    exit_fn = getattr(context.client, "__aexit__", None)
    if exit_fn is not None:
        await exit_fn(None, None, None)


async def download_file_to_path(
    context: DownloadContext,
    file: RemoteFile,
    local_path: str,
    *,
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download one file through an already-classified provider context."""
    if context.provider == "gcs":
        await gcs.download_file_with_session(
            context.client, file, local_path, storage_reservation=storage_reservation
        )
        return
    if context.provider == "s3":
        await s3.download_file_with_client(
            context.client, file, local_path, storage_reservation=storage_reservation
        )
        return
    if context.provider == "azure":
        await azure.download_file_with_service(
            context.client, file, local_path, storage_reservation=storage_reservation
        )
        return
    async with context.client.get(file.uri) as response:
        await write_response_to_file(
            response,
            uri=file.uri,
            local_path=local_path,
            storage_reservation=storage_reservation,
        )


async def _download_files_with_context(
    files: Sequence[RemoteFile],
    directory: str,
    *,
    context: DownloadContext,
    semaphore: asyncio.Semaphore,
    retries: int,
    window: int,
    memory_limit_bytes: int | None,
    storage_lease: TemporaryStorageLease | None = None,
) -> None:
    """Download one file packet through a shared provider context."""
    target_root = Path(directory)

    async def fetch(index: int) -> None:
        """Download and validate one indexed remote file."""
        file = files[index]
        target = target_root / file.name
        check_download_size(file.uri, file.size, memory_limit_bytes)
        budget = memory_budget(memory_limit_bytes)
        initial_credit = (
            file.size if isinstance(file.size, int) and file.size >= 0 else budget.io_chunk_bytes
        )
        storage_reservation = StreamingStorageReservation(
            storage_lease,
            initial_credit_bytes=initial_credit,
            path=target,
            quantum_bytes=budget.io_chunk_bytes,
        )

        async def operation() -> None:
            """Download one file while holding the global transfer slot."""
            async with semaphore:
                if storage_lease is None:
                    await download_file_to_path(context, file, str(target))
                else:
                    await download_file_to_path(
                        context,
                        file,
                        str(target),
                        storage_reservation=storage_reservation,
                    )

        try:
            await retry_async(operation, retries=retries)
        except BaseException:
            target.unlink(missing_ok=True)
            raise
        check_download_size(file.uri, target.stat().st_size, memory_limit_bytes)

    await drain_ordered_indexed_results(len(files), fetch, window=max(1, window))


async def download_files_to_directory(
    files: list[RemoteFile],
    directory: str,
    *,
    memory_limit_bytes: int | None,
    threading_mode: str = "single",
    storage_lease: TemporaryStorageLease | None = None,
) -> None:
    """Download files with policy-bounded ordered concurrency."""
    if not files:
        raise ValueError("remote directory input found no matching files")
    async with RemoteDirectoryDownloadSession(
        files,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    ) as session:
        await session.download_files(files, directory, storage_lease=storage_lease)
