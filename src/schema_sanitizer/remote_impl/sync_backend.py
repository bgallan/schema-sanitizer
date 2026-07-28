"""Strict same-thread remote backend used by threading_mode='single'."""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import urlparse

from ..core_impl.memory_budget import memory_budget
from ..core_impl.sync_retry import retry_sync
from ..core_impl.uris import RemoteProvider, normalize_extensions, remote_provider
from ..input_impl.directory_inputs import DirectoryDiscovery, RemoteFile
from .providers import azure_sync, gcs_sync, s3_sync
from .sync_http import (
    download_http_file,
    http_file_metadata,
    retryable_http_error,
    upload_http_file,
)
from .transport import check_download_size


@dataclass(slots=True)
class SyncDownloadContext:
    """One blocking provider handle reused by a directory packet."""

    provider: RemoteProvider
    client: Any = None
    headers: dict[str, str] | None = None


class SyncDirectoryDownloadSession:
    """Reuse one strictly blocking provider client across sequential chunks."""

    def __init__(
        self,
        files: Sequence[RemoteFile],
        *,
        memory_limit_bytes: int | None,
    ) -> None:
        """Store the homogeneous file set and operation budget."""
        self._files = tuple(files)
        self._memory_limit_bytes = memory_limit_bytes
        self._stack: ExitStack | None = None
        self._context: SyncDownloadContext | None = None

    def __enter__(self) -> SyncDirectoryDownloadSession:
        """Open one provider handle on the caller thread."""
        if not self._files:
            raise ValueError("remote directory input found no matching files")
        provider = remote_provider(self._files[0].uri)
        if provider is None or any(
            remote_provider(file.uri) != provider for file in self._files[1:]
        ):
            raise ValueError("one remote staging packet must use exactly one provider")
        stack = ExitStack()
        self._stack = stack
        if provider == "s3":
            self._context = SyncDownloadContext(
                provider, stack.enter_context(s3_sync.open_client())
            )
        elif provider == "azure":
            ref = azure_sync.parse_uri(self._files[0].uri)
            self._context = SyncDownloadContext(
                provider,
                stack.enter_context(azure_sync.open_service(ref)),
            )
        elif provider == "gcs":
            self._context = SyncDownloadContext(provider, headers=gcs_sync.request_headers())
        else:
            self._context = SyncDownloadContext(provider)
        return self

    def __exit__(self, *_exc: object) -> None:
        """Close provider resources deterministically on the caller thread."""
        stack = self._stack
        self._stack = None
        self._context = None
        if stack is not None:
            stack.close()

    def download_files(self, files: Sequence[RemoteFile], directory: str) -> None:
        """Download a packet serially and preserve canonical file order."""
        context = self._context
        if context is None:
            raise RuntimeError("synchronous remote directory session is not open")
        target_root = Path(directory)
        retries = memory_budget(self._memory_limit_bytes).async_retries
        for file in files:
            target = target_root / file.name
            check_download_size(file.uri, file.size, self._memory_limit_bytes)

            def operation() -> None:
                """Download one file and truncate its destination per retry."""
                target.unlink(missing_ok=True)
                _download_with_context(
                    context,
                    file,
                    str(target),
                    memory_limit_bytes=self._memory_limit_bytes,
                )

            try:
                retry_sync(
                    operation,
                    retries=retries,
                    should_retry=_retryable_download_error,
                )
            except BaseException:
                target.unlink(missing_ok=True)
                raise
            check_download_size(file.uri, target.stat().st_size, self._memory_limit_bytes)


def _retryable_download_error(exc: Exception) -> bool:
    """Return whether one same-thread provider download is transient."""
    if retryable_http_error(exc):
        return True
    if exc.__class__.__module__.split(".", 1)[0] in {"botocore", "azure"}:
        status = getattr(exc, "status_code", None)
        if status == 429 or (isinstance(status, int) and status >= 500):
            return True
        response = getattr(exc, "response", None)
        if isinstance(response, dict):
            metadata = response.get("ResponseMetadata")
            status = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
            return status == 429 or (isinstance(status, int) and status >= 500)
    return False


def _download_with_context(
    context: SyncDownloadContext,
    file: RemoteFile,
    local_path: str,
    *,
    memory_limit_bytes: int | None,
) -> None:
    """Download one file through a classified blocking provider handle."""
    if context.provider == "s3":
        s3_sync.download_file_with_client(context.client, file, local_path)
        return
    if context.provider == "azure":
        azure_sync.download_file_with_service(context.client, file, local_path)
        return
    if context.provider == "gcs":
        gcs_sync.download_file(
            file.uri,
            local_path,
            memory_limit_bytes=memory_limit_bytes,
            headers=context.headers,
        )
        return
    download_http_file(file.uri, local_path, memory_limit_bytes=memory_limit_bytes)


def remote_file_metadata(
    uri: str,
    *,
    memory_limit_bytes: int | None = None,
) -> RemoteFile | None:
    """Return metadata without constructing an event loop or async SDK."""
    provider = remote_provider(uri)
    if provider == "gcs":
        return gcs_sync.file_metadata(uri, memory_limit_bytes=memory_limit_bytes)
    if provider == "s3":
        return s3_sync.file_metadata(uri, memory_limit_bytes=memory_limit_bytes)
    if provider == "azure":
        return azure_sync.file_metadata(uri, memory_limit_bytes=memory_limit_bytes)
    if provider == "http":
        return http_file_metadata(uri, memory_limit_bytes=memory_limit_bytes)
    scheme = urlparse(uri).scheme.lower()
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


def remote_file_exists(uri: str, *, memory_limit_bytes: int | None = None) -> bool:
    """Return whether one blocking remote metadata request succeeds."""
    return remote_file_metadata(uri, memory_limit_bytes=memory_limit_bytes) is not None


def list_remote_directory(
    uri: str,
    suffixes: Sequence[str],
    *,
    memory_limit_bytes: int | None = None,
) -> list[RemoteFile]:
    """List one remote directory serially on the caller thread."""
    accepted = normalize_extensions(suffixes)
    provider = remote_provider(uri)
    if provider == "gcs":
        return gcs_sync.list_directory(uri, accepted, memory_limit_bytes=memory_limit_bytes)
    if provider == "s3":
        return s3_sync.list_files(uri, accepted, memory_limit_bytes=memory_limit_bytes)
    if provider == "azure":
        return azure_sync.list_files(uri, accepted, memory_limit_bytes=memory_limit_bytes)
    if provider == "http":
        raise ValueError("HTTP(S) directory listing is not portable; use single_file mode")
    scheme = urlparse(uri).scheme.lower()
    raise ValueError(f"Unsupported remote directory URI scheme: {scheme!r}")


def directories_containing_files(
    provider: RemoteProvider,
    uris: list[str],
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
) -> DirectoryDiscovery[RemoteFile]:
    """Run grouped remote discovery serially for one provider."""
    if provider == "gcs":
        return gcs_sync.directories_containing_files(
            uris, suffixes, memory_limit_bytes=memory_limit_bytes
        )
    if provider == "s3":
        return s3_sync.directories_containing_files(
            uris, suffixes, memory_limit_bytes=memory_limit_bytes
        )
    if provider == "azure":
        return azure_sync.directories_containing_files(
            uris, suffixes, memory_limit_bytes=memory_limit_bytes
        )
    raise ValueError(f"Unsupported directory discovery provider: {provider!r}")


def download_single_file(
    uri: str,
    local_path: str,
    *,
    memory_limit_bytes: int | None = None,
) -> None:
    """Download one object using only blocking provider APIs."""
    provider = remote_provider(uri)
    if provider == "gcs":
        gcs_sync.download_file(uri, local_path, memory_limit_bytes=memory_limit_bytes)
        return
    if provider == "s3":
        s3_sync.download_file(uri, local_path, memory_limit_bytes=memory_limit_bytes)
        return
    if provider == "azure":
        azure_sync.download_file(uri, local_path, memory_limit_bytes=memory_limit_bytes)
        return
    if provider == "http":
        download_http_file(uri, local_path, memory_limit_bytes=memory_limit_bytes)
        return
    scheme = urlparse(uri).scheme.lower()
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


def upload_file(
    local_path: str,
    uri: str,
    *,
    memory_limit_bytes: int | None = None,
) -> None:
    """Upload one spool using only blocking provider APIs."""
    provider = remote_provider(uri)
    if provider == "gcs":
        gcs_sync.upload_file(local_path, uri, memory_limit_bytes=memory_limit_bytes)
        return
    if provider == "s3":
        s3_sync.upload_file(local_path, uri, memory_limit_bytes=memory_limit_bytes)
        return
    if provider == "azure":
        azure_sync.upload_file(local_path, uri, memory_limit_bytes=memory_limit_bytes)
        return
    if provider == "http":
        upload_http_file(local_path, uri, memory_limit_bytes=memory_limit_bytes)
        return
    scheme = urlparse(uri).scheme.lower()
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


def download_files_to_directory(
    files: Sequence[RemoteFile],
    directory: str,
    *,
    memory_limit_bytes: int | None,
) -> None:
    """Download one homogeneous packet through a reused blocking client."""
    with SyncDirectoryDownloadSession(
        files,
        memory_limit_bytes=memory_limit_bytes,
    ) as session:
        session.download_files(files, directory)


__all__ = [
    "SyncDirectoryDownloadSession",
    "directories_containing_files",
    "download_files_to_directory",
    "download_single_file",
    "list_remote_directory",
    "remote_file_exists",
    "remote_file_metadata",
    "upload_file",
]
