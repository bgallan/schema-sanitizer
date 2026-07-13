"""Azure Blob URI, discovery, and object operations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ...core_impl.async_scheduler import read_int_env
from ...core_impl.uris import name_matches, normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    RemoteFile,
    split_parent_child,
)


@dataclass(frozen=True, slots=True)
class AzureRef:
    """Parsed Azure Blob object reference."""

    account_url: str
    container: str
    blob: str
    original_uri: str


def parse_uri(uri: str) -> AzureRef:
    """Parse common Azure Blob and ADLS URI forms."""
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()
    if scheme in {"http", "https"} and ".blob.core.windows.net" in parsed.netloc:
        parts = parsed.path.lstrip("/").split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Azure Blob URL must include container and blob: {uri!r}")
        return AzureRef(f"{parsed.scheme}://{parsed.netloc}", parts[0], parts[1], uri)
    if scheme in {"abfs", "abfss", "wasb", "wasbs"}:
        container, _, account_host = parsed.netloc.partition("@")
        if not container or not account_host:
            raise ValueError(f"Azure URI must be container@account: {uri!r}")
        account = account_host.split(".", 1)[0]
        return AzureRef(
            f"https://{account}.blob.core.windows.net",
            container,
            parsed.path.lstrip("/"),
            uri,
        )
    if scheme in {"az", "azure"}:
        parts = parsed.path.lstrip("/").split("/", 1)
        if not parsed.netloc or len(parts) != 2:
            raise ValueError(f"Azure URI must be azure://account/container/blob: {uri!r}")
        return AzureRef(
            f"https://{parsed.netloc}.blob.core.windows.net",
            parts[0],
            parts[1],
            uri,
        )
    raise ValueError(f"not an Azure Blob URI: {uri!r}")


async def open_service(ref: AzureRef) -> Any:
    """Open an async Azure Blob service client using default credentials."""
    from azure.identity.aio import DefaultAzureCredential
    from azure.storage.blob.aio import BlobServiceClient

    credential = DefaultAzureCredential()
    return BlobServiceClient(account_url=ref.account_url, credential=credential)


def render_uri(ref: AzureRef, blob: str) -> str:
    """Render an Azure URI in HTTPS Blob form."""
    return f"{ref.account_url}/{ref.container}/{blob}"


async def download_file_with_service(
    service: Any,
    file: RemoteFile,
    local_path: str,
) -> None:
    """Download one Azure Blob using a shared service client."""
    ref = parse_uri(file.uri)
    blob = service.get_blob_client(ref.container, ref.blob)
    stream = await blob.download_blob()
    with Path(local_path).open("wb") as file_handle:
        async for chunk in stream.chunks():
            file_handle.write(chunk)


async def download_file(uri: str, local_path: str) -> None:
    """Download one Azure Blob to a local file."""
    ref = parse_uri(uri)
    service = await open_service(ref)
    try:
        await download_file_with_service(
            service,
            RemoteFile(uri, Path(ref.blob).name),
            local_path,
        )
    finally:
        await service.close()


async def file_exists(uri: str) -> bool:
    """Return whether one Azure Blob object exists."""
    ref = parse_uri(uri)
    service = await open_service(ref)
    try:
        return bool(await service.get_blob_client(ref.container, ref.blob).exists())
    finally:
        await service.close()


async def download_bytes(uri: str) -> bytes:
    """Download one Azure Blob into bytes."""
    ref = parse_uri(uri)
    service = await open_service(ref)
    try:
        stream = await service.get_blob_client(ref.container, ref.blob).download_blob()
        data = bytearray()
        async for chunk in stream.chunks():
            data.extend(chunk)
        return bytes(data)
    finally:
        await service.close()


async def upload_file(local_path: str, uri: str) -> None:
    """Upload a local file to Azure Blob storage."""
    ref = parse_uri(uri)
    service = await open_service(ref)
    try:
        blob = service.get_blob_client(ref.container, ref.blob)
        with Path(local_path).open("rb") as file_handle:
            await blob.upload_blob(file_handle, overwrite=True)
    finally:
        await service.close()


async def list_files(uri: str, suffixes: tuple[str, ...]) -> list[RemoteFile]:
    """List direct Azure Blob child files under a URI prefix."""
    ref = parse_uri(uri)
    prefix = ref.blob.rstrip("/") + "/"
    files: list[RemoteFile] = []
    service = await open_service(ref)
    try:
        container = service.get_container_client(ref.container)
        async for blob in container.walk_blobs(name_starts_with=prefix, delimiter="/"):
            name = getattr(blob, "name", None)
            if not isinstance(name, str):
                continue
            relative = name[len(prefix) :] if name.startswith(prefix) else name
            if not relative or "/" in relative or not name_matches(relative, suffixes):
                continue
            size = getattr(blob, "size", None)
            files.append(
                RemoteFile(render_uri(ref, name), relative, size if isinstance(size, int) else None)
            )
    finally:
        await service.close()
    files.sort(key=lambda file: file.name)
    return files


async def directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
) -> DirectoryDiscovery[RemoteFile]:
    """Return whether Azure directories contain a direct child matching suffixes."""
    accepted = normalize_extensions(suffixes)
    discovery = DirectoryDiscoveryBuilder[RemoteFile].from_uris(uris)
    groups: dict[tuple[str, str, str], dict[str, list[str]]] = {}
    for uri in uris:
        ref = parse_uri(uri)
        parsed = split_parent_child(ref.blob)
        if parsed is None:
            continue
        parent_prefix, child = parsed
        account_url, container = ref.account_url, ref.container
        groups.setdefault((account_url, container, parent_prefix), {}).setdefault(child, []).append(
            uri
        )

    if not groups:
        return discovery.finish()

    concurrency = read_int_env("SCHEMA_SANITIZER_SOURCE_DISCOVERY_AZURE_BULK_CONCURRENCY", 16)
    semaphore = asyncio.Semaphore(concurrency)

    async def scan_group(
        account_url: str,
        container_name: str,
        parent_prefix: str,
        children: dict[str, list[str]],
    ) -> None:
        """Scan one Azure parent prefix and mark matching child directories."""
        prefix = f"{parent_prefix.rstrip('/')}/" if parent_prefix else ""
        ref = AzureRef(account_url, container_name, parent_prefix, "")
        service = await open_service(ref)
        try:
            container = service.get_container_client(container_name)
            async with semaphore:
                async for blob in container.list_blobs(name_starts_with=prefix):
                    name = getattr(blob, "name", None)
                    if not isinstance(name, str) or not name.startswith(prefix):
                        continue
                    relative = name[len(prefix) :]
                    child, separator, filename = relative.partition("/")
                    child_uris = children.get(child) if separator else None
                    if not child_uris or "/" in filename or not name_matches(filename, accepted):
                        continue
                    size = getattr(blob, "size", None)
                    remote_file = RemoteFile(
                        render_uri(ref, name),
                        filename,
                        size if isinstance(size, int) else None,
                    )
                    discovery.add(child_uris, remote_file)
        finally:
            await service.close()

    await asyncio.gather(
        *(
            scan_group(account_url, container, parent, children)
            for (account_url, container, parent), children in groups.items()
        )
    )
    return discovery.finish()
