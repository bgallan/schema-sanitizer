"""Asynchronous provider transfer dispatch used by threading_mode='multi'."""

from __future__ import annotations

from urllib.parse import urlparse

from ..core_impl.temporary_storage import StreamingStorageReservation
from ..core_impl.uris import remote_provider
from ..sources.models import RemoteFile
from .providers import azure, gcs, s3
from .transport import download_http_file, upload_http_file


async def download_single_file(
    uri: str | RemoteFile,
    local_path: str,
    *,
    memory_limit_bytes: int | None,
    threading_mode: str = "multi",
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download one supported remote URI through an asynchronous provider."""
    source_uri = uri.uri if isinstance(uri, RemoteFile) else uri
    provider = remote_provider(source_uri)
    if provider == "gcs":
        await gcs.download_file(
            uri,
            local_path,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
            storage_reservation=storage_reservation,
        )
        return
    if provider == "s3":
        await s3.download_file(
            source_uri,
            local_path,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
            storage_reservation=storage_reservation,
        )
        return
    if provider == "azure":
        await azure.download_file(
            source_uri,
            local_path,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
            storage_reservation=storage_reservation,
        )
        return
    if provider == "http":
        await download_http_file(
            source_uri,
            local_path,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
            storage_reservation=storage_reservation,
        )
        return
    scheme = urlparse(source_uri).scheme.lower()
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


async def upload_file(
    local_path: str,
    uri: str,
    *,
    memory_limit_bytes: int | None,
    threading_mode: str = "multi",
) -> None:
    """Upload a local file through an asynchronous provider."""
    provider = remote_provider(uri)
    if provider == "gcs":
        await gcs.upload_file(
            local_path,
            uri,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
        return
    if provider == "s3":
        await s3.upload_file(
            local_path,
            uri,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
        return
    if provider == "azure":
        await azure.upload_file(
            local_path,
            uri,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
        return
    if provider == "http":
        await upload_http_file(
            local_path,
            uri,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
        return
    scheme = urlparse(uri).scheme.lower()
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


__all__ = ["download_single_file", "upload_file"]
