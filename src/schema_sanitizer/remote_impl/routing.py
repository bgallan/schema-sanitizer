"""Provider routing for remote object discovery and existence checks."""

from __future__ import annotations

from collections.abc import Sequence
from urllib.parse import urlparse

from ..core_impl.uris import normalize_extensions, remote_provider
from ..input_impl.directory_inputs import RemoteFile
from .providers import azure, gcs, s3
from .transport import http_file_exists, http_file_metadata


async def list_remote_directory(
    uri: str,
    suffixes: Sequence[str],
    *,
    memory_limit_bytes: int | None = None,
) -> list[RemoteFile]:
    """List one supported remote directory non-recursively."""
    accepted = normalize_extensions(suffixes)
    provider = remote_provider(uri)
    if provider == "gcs":
        return await gcs.list_directory(uri, accepted)
    if provider == "s3":
        return await s3.list_files(uri, accepted)
    if provider == "azure":
        return await azure.list_files(uri, accepted)
    if provider == "http":
        raise ValueError("HTTP(S) directory listing is not portable; use single_file mode")
    scheme = urlparse(uri).scheme.lower()
    raise ValueError(f"Unsupported remote directory URI scheme: {scheme!r}")


async def remote_file_exists(uri: str, *, memory_limit_bytes: int | None = None) -> bool:
    """Return whether one supported remote object exists."""
    provider = remote_provider(uri)
    if provider == "gcs":
        return await gcs.file_exists(uri)
    if provider == "s3":
        return await s3.file_exists(uri)
    if provider == "azure":
        return await azure.file_exists(uri)
    if provider == "http":
        return await http_file_exists(uri, memory_limit_bytes=memory_limit_bytes)
    scheme = urlparse(uri).scheme.lower()
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


async def remote_file_metadata(
    uri: str,
    *,
    memory_limit_bytes: int | None = None,
) -> RemoteFile | None:
    """Return one supported remote object's existence and size metadata."""
    provider = remote_provider(uri)
    if provider == "gcs":
        return await gcs.file_metadata(uri)
    if provider == "s3":
        return await s3.file_metadata(uri)
    if provider == "azure":
        return await azure.file_metadata(uri)
    if provider == "http":
        return await http_file_metadata(uri, memory_limit_bytes=memory_limit_bytes)
    scheme = urlparse(uri).scheme.lower()
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")
