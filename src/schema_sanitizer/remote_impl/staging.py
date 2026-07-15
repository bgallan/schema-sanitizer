"""Owned local staging for remote inputs, outputs, and object transfers."""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ..core_impl.async_scheduler import (
    drain_ordered_indexed_results,
    retry_async,
)
from ..core_impl.memory_budget import memory_budget
from ..core_impl.uris import (
    RemoteProvider,
    local_path_from_file_uri,
    looks_like_file_uri,
    looks_like_remote_uri,
    normalize_extensions,
    remote_provider,
    suffix_from_uri,
)
from ..input_impl.directory_inputs import RemoteFile
from . import routing
from .providers import azure, gcs, s3
from .transport import (
    check_download_size,
    download_http_file,
    open_aiohttp_session,
    run_sync,
    upload_http_file,
    write_response_to_file,
)


class StagedPath:
    """Own a local temporary path that mirrors a remote input or output."""

    def __init__(
        self,
        path: str,
        *,
        is_dir: bool = False,
        source_file_by_name: dict[str, str] | None = None,
    ) -> None:
        """Store the temporary path and deletion mode."""
        self.path = path
        self.is_dir = is_dir
        self.source_file_by_name = source_file_by_name
        self._closed = False

    def close(self) -> None:
        """Delete the temporary path."""
        if self._closed:
            return
        self._closed = True
        if self.is_dir:
            shutil.rmtree(self.path, ignore_errors=True)
            return
        try:
            Path(self.path).unlink(missing_ok=True)
        except OSError:
            pass


@dataclass(slots=True)
class RemoteOutputTarget:
    """Local output target plus optional remote upload destination."""

    local_path: str
    remote_uri: str | None = None
    temp: StagedPath | None = None
    memory_limit_bytes: int | None = None

    def close(self) -> None:
        """Release the temporary output path."""
        if self.temp is not None:
            self.temp.close()
            self.temp = None


@dataclass(frozen=True, slots=True)
class _DirectoryDownloadTuning:
    """Runtime controls for concurrent remote directory downloads."""

    concurrency: int
    window: int
    retries: int


def _directory_download_tuning(
    memory_limit_bytes: int | None,
) -> _DirectoryDownloadTuning:
    """Derive remote download controls from the operation memory budget."""
    budget = memory_budget(memory_limit_bytes)
    return _DirectoryDownloadTuning(
        concurrency=budget.async_concurrency,
        window=budget.async_prefetch_files,
        retries=budget.async_retries,
    )


@dataclass(slots=True)
class _DownloadContext:
    """Provider identity plus reusable handles for one staged directory."""

    provider: RemoteProvider
    client: Any = None
    manager: Any = None


def create_temp_file_path(*, suffix: str) -> StagedPath:
    """Create an owned temporary file path."""
    fd, path = tempfile.mkstemp(
        prefix="schema-sanitizer-",
        suffix=suffix,
    )
    os.close(fd)
    return StagedPath(path)


def create_temp_directory_path() -> StagedPath:
    """Create an owned temporary directory path."""
    path = tempfile.mkdtemp(prefix="schema-sanitizer-")
    return StagedPath(path, is_dir=True)


async def provider_client_for_downloads(
    files: Sequence[RemoteFile],
    *,
    memory_limit_bytes: int | None = None,
) -> _DownloadContext | None:
    """Open one reusable provider client or session for a staged directory."""
    if not files:
        return None
    provider = remote_provider(files[0].uri)
    if provider is None:
        scheme = urlparse(files[0].uri).scheme.lower()
        raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")
    if provider == "gcs":
        client = await open_aiohttp_session(
            gcs.request_headers(), memory_limit_bytes=memory_limit_bytes
        )
        return _DownloadContext(provider, client)
    if provider == "s3":
        manager = await s3.open_client()
        return _DownloadContext(provider, await manager.__aenter__(), manager)
    if provider == "http":
        return _DownloadContext(
            provider, await open_aiohttp_session(memory_limit_bytes=memory_limit_bytes)
        )
    if provider == "azure":
        return _DownloadContext(provider, await azure.open_service(azure.parse_uri(files[0].uri)))
    raise ValueError(f"Unsupported remote provider: {provider!r}")


async def close_provider_client(context: _DownloadContext | None) -> None:
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
    context: _DownloadContext,
    file: RemoteFile,
    local_path: str,
) -> None:
    """Download one remote file using an already classified provider context."""
    if context.provider == "gcs":
        await gcs.download_file_with_session(context.client, file, local_path)
        return
    if context.provider == "s3":
        await s3.download_file_with_client(context.client, file, local_path)
        return
    if context.provider == "azure":
        await azure.download_file_with_service(context.client, file, local_path)
        return
    async with context.client.get(file.uri) as response:
        await write_response_to_file(response, uri=file.uri, local_path=local_path)


async def download_single_file(
    uri: str, local_path: str, *, memory_limit_bytes: int | None
) -> None:
    """Download one supported remote URI to a local path."""
    provider = remote_provider(uri)
    if provider == "gcs":
        await gcs.download_file(uri, local_path)
        return
    if provider == "s3":
        await s3.download_file(uri, local_path)
        return
    if provider == "azure":
        await azure.download_file(uri, local_path)
        return
    if provider == "http":
        await download_http_file(
            uri, local_path, memory_limit_bytes=memory_limit_bytes
        )
        return
    scheme = urlparse(uri).scheme.lower()
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


async def upload_file(
    local_path: str, uri: str, *, memory_limit_bytes: int | None
) -> None:
    """Upload a local file to a supported remote URI."""
    provider = remote_provider(uri)
    if provider == "gcs":
        await gcs.upload_file(local_path, uri)
        return
    if provider == "s3":
        await s3.upload_file(local_path, uri)
        return
    if provider == "azure":
        await azure.upload_file(local_path, uri)
        return
    if provider == "http":
        await upload_http_file(
            local_path, uri, memory_limit_bytes=memory_limit_bytes
        )
        return
    scheme = urlparse(uri).scheme.lower()
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


def remote_directory_stage_chunk_size(memory_limit_bytes: int | None) -> int:
    """Derive the maximum staged children from the memory budget."""
    return min(4096, memory_budget(memory_limit_bytes).async_prefetch_files * 4)


async def download_files_to_directory(
    files: list[RemoteFile],
    directory: str,
    *,
    memory_limit_bytes: int | None,
) -> None:
    """Download files concurrently into a local directory."""
    if not files:
        raise ValueError("remote directory input found no matching files")
    tuning = _directory_download_tuning(memory_limit_bytes)
    semaphore = asyncio.Semaphore(tuning.concurrency)
    context = await provider_client_for_downloads(
        files, memory_limit_bytes=memory_limit_bytes
    )
    if context is None:  # pragma: no cover - guarded by the non-empty check
        raise RuntimeError("remote download context was not created")
    target_root = Path(directory)

    async def fetch(index: int) -> None:
        """Download and validate one indexed remote file."""
        file = files[index]
        target = target_root / file.name
        check_download_size(file.uri, file.size, memory_limit_bytes)

        async def operation() -> None:
            """Download one file while holding the concurrency slot."""
            async with semaphore:
                await download_file_to_path(context, file, str(target))

        try:
            await retry_async(operation, retries=tuning.retries)
        except Exception:
            target.unlink(missing_ok=True)
            raise
        check_download_size(file.uri, target.stat().st_size, memory_limit_bytes)

    try:
        await drain_ordered_indexed_results(len(files), fetch, window=tuning.window)
    finally:
        await close_provider_client(context)


def stage_remote_single_file(uri: str, *, memory_limit_bytes: int | None) -> StagedPath:
    """Download one remote file to a local temporary path."""
    temp = create_temp_file_path(suffix=suffix_from_uri(uri))
    try:
        run_sync(
            download_single_file(
                uri, temp.path, memory_limit_bytes=memory_limit_bytes
            )
        )
        check_download_size(uri, Path(temp.path).stat().st_size, memory_limit_bytes)
    except Exception:
        temp.close()
        raise
    return temp


def stage_remote_files_to_directory(
    files: Sequence[RemoteFile],
    *,
    memory_limit_bytes: int | None,
) -> StagedPath:
    """Download selected remote files into one temporary directory."""
    selected = list(files)
    if not selected:
        raise ValueError("remote directory input found no matching files")
    temp_dir = create_temp_directory_path()
    try:
        run_sync(
            download_files_to_directory(
                selected,
                temp_dir.path,
                memory_limit_bytes=memory_limit_bytes,
            )
        )
    except Exception:
        temp_dir.close()
        raise
    temp_dir.source_file_by_name = {file.name: file.uri for file in selected}
    return temp_dir


def stage_remote_parquet_directory(
    uri: str,
    *,
    suffixes: Sequence[str],
    memory_limit_bytes: int | None,
) -> StagedPath:
    """Download a remote Parquet directory to a local temporary directory."""
    files = run_sync(routing.list_remote_directory(uri, suffixes))
    if not files:
        expected = " or ".join(normalize_extensions(suffixes))
        raise ValueError(f"parquet remote directory input found no {expected} files in: {uri}")
    return stage_remote_files_to_directory(files, memory_limit_bytes=memory_limit_bytes)


def prepare_output_target(
    path: Any, *, memory_limit_bytes: int | None
) -> RemoteOutputTarget:
    """Return a local target, staging remote destinations when needed."""
    raw = os.fspath(path)
    if looks_like_file_uri(raw):
        return RemoteOutputTarget(
            local_path=local_path_from_file_uri(raw),
            memory_limit_bytes=memory_limit_bytes,
        )
    if not looks_like_remote_uri(raw):
        return RemoteOutputTarget(
            local_path=raw, memory_limit_bytes=memory_limit_bytes
        )
    temp = create_temp_file_path(suffix=suffix_from_uri(raw, default=".tmp"))
    return RemoteOutputTarget(
        local_path=temp.path,
        remote_uri=raw,
        temp=temp,
        memory_limit_bytes=memory_limit_bytes,
    )


def finalize_output_target(target: RemoteOutputTarget) -> None:
    """Upload a staged output target if it points to a remote URI."""
    try:
        if target.remote_uri is not None:
            run_sync(
                upload_file(
                    target.local_path,
                    target.remote_uri,
                    memory_limit_bytes=target.memory_limit_bytes,
                )
            )
    finally:
        target.close()


def cleanup_output_target(target: RemoteOutputTarget) -> None:
    """Release a staged output target after a failed write."""
    target.close()
