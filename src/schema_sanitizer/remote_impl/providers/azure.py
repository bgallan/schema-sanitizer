"""Azure Blob URI, discovery, and object operations."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ...core_impl.async_scheduler import drain_ordered_indexed_results
from ...core_impl.execution_policy import execution_policy
from ...core_impl.memory_budget import current_operation_memory_ledger
from ...core_impl.temporary_storage import StreamingStorageReservation
from ...core_impl.uris import name_matches, normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    current_directory_metadata_budget,
    split_parent_child,
)
from ...sources.models import RemoteFile
from ..file_streams import write_async_iterator_to_file
from ..provider_session_pool import current_provider_session_pool
from ..upload_policy import remote_upload_policy


class _AzureServiceOwner:
    """Own one Blob service and the credential created specifically for it."""

    def __init__(self, service: Any, credential: Any) -> None:
        """Store both SDK resources for one idempotent combined close."""
        self._service = service
        self._credential = credential
        self._closed = False

    def __getattr__(self, name: str) -> Any:
        """Forward Azure service methods and properties."""
        return getattr(self._service, name)

    async def close(self) -> None:
        """Close service transport and credential exactly once."""
        if self._closed:
            return
        self._closed = True
        first_error: BaseException | None = None
        for resource in (self._service, self._credential):
            close = getattr(resource, "close", None)
            if close is None:
                continue
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error


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


async def _open_service_unpooled(ref: AzureRef) -> Any:
    """Create one directly owned async Azure Blob service client."""
    blob = import_module("azure.storage.blob.aio")
    identity = import_module("azure.identity.aio")
    credential = identity.DefaultAzureCredential()
    try:
        service = blob.BlobServiceClient(
            account_url=ref.account_url,
            credential=credential,
        )
    except BaseException:
        close = getattr(credential, "close", None)
        if close is not None:
            result = close()
            if inspect.isawaitable(result):
                await result
        raise
    return _AzureServiceOwner(service, credential)


async def open_service(ref: AzureRef) -> Any:
    """Open or borrow one Azure service client for the current operation."""
    pool = current_provider_session_pool()
    if pool is None:
        return await _open_service_unpooled(ref)

    async def create() -> Any:
        """Create the operation-owned Azure service client."""
        return await _open_service_unpooled(ref)

    return await pool.borrow_client(("azure", ref.account_url), create)


def render_uri(ref: AzureRef, blob: str) -> str:
    """Render an Azure URI in HTTPS Blob form."""
    return f"{ref.account_url}/{ref.container}/{blob}"


def _azure_transfer_reservation_bytes() -> int:
    """Return a conservative single-SDK-chunk reservation."""
    ledger = current_operation_memory_ledger()
    return 4 * 1024 * 1024 if ledger is None else min(4 * 1024 * 1024, ledger.limit_bytes)


async def _write_azure_stream(
    stream: Any,
    local_path: str,
    *,
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Write Azure chunks while reserving disk before each local write."""
    await write_async_iterator_to_file(
        stream.chunks().__aiter__(),
        local_path,
        reservation_bytes=_azure_transfer_reservation_bytes(),
        storage_reservation=storage_reservation,
    )


async def download_file_with_service(
    service: Any,
    file: RemoteFile,
    local_path: str,
    *,
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download one Azure Blob using a shared service client."""
    ref = parse_uri(file.uri)
    blob = service.get_blob_client(ref.container, ref.blob)
    stream = await blob.download_blob()
    await _write_azure_stream(stream, local_path, storage_reservation=storage_reservation)


async def download_file(
    uri: str,
    local_path: str,
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download one Azure Blob to a local file."""
    ref = parse_uri(uri)
    service = await open_service(ref)
    try:
        policy = execution_policy(threading_mode, memory_limit_bytes)
        blob = service.get_blob_client(ref.container, ref.blob)
        stream = await blob.download_blob(max_concurrency=policy.async_concurrency)
        await _write_azure_stream(stream, local_path, storage_reservation=storage_reservation)
    finally:
        await service.close()


async def file_exists(
    uri: str, *, memory_limit_bytes: int | None = None, threading_mode: str = "single"
) -> bool:
    """Return whether one Azure Blob object exists."""
    return (
        await file_metadata(
            uri, memory_limit_bytes=memory_limit_bytes, threading_mode=threading_mode
        )
        is not None
    )


async def file_metadata(
    uri: str, *, memory_limit_bytes: int | None = None, threading_mode: str = "single"
) -> RemoteFile | None:
    """Return Azure Blob metadata using the existence request."""
    ref = parse_uri(uri)
    service = await open_service(ref)
    try:
        blob = service.get_blob_client(ref.container, ref.blob)
        try:
            properties = await blob.get_blob_properties()
        except Exception as exc:
            status = getattr(exc, "status_code", None)
            code = getattr(exc, "error_code", None)
            if status == 404 or code in {"BlobNotFound", "ContainerNotFound"}:
                return None
            if status in {401, 403}:
                raise PermissionError(
                    f"Azure returned a permission error while checking source object: {uri!r}"
                ) from exc
            raise
        raw_size = getattr(properties, "size", None)
        size = int(raw_size) if raw_size is not None else None
        return RemoteFile(uri, Path(ref.blob).name, size)
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


async def upload_file(
    local_path: str,
    uri: str,
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> None:
    """Upload a local file to Azure Blob storage."""
    ref = parse_uri(uri)
    tuning = remote_upload_policy(
        "azure",
        local_path,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )
    service = await open_service(ref)
    try:
        blob = service.get_blob_client(ref.container, ref.blob)
        with Path(local_path).open("rb") as file_handle:
            await blob.upload_blob(
                file_handle,
                overwrite=True,
                length=tuning.file_size,
                max_concurrency=tuning.concurrency,
            )
    finally:
        await service.close()


async def list_files(
    uri: str,
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> list[RemoteFile]:
    """List direct Azure Blob child files under a URI prefix."""
    ref = parse_uri(uri)
    prefix = ref.blob.rstrip("/") + "/"
    files: list[RemoteFile] = []
    metadata_budget = current_directory_metadata_budget(memory_limit_bytes)
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
            remote_file = RemoteFile(
                render_uri(ref, name), relative, size if isinstance(size, int) else None
            )
            metadata_budget.charge_file(remote_file)
            files.append(remote_file)
    finally:
        await service.close()
    files.sort(key=lambda file: file.name)
    return files


async def directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> DirectoryDiscovery[RemoteFile]:
    """Return whether Azure directories contain a direct child matching suffixes."""
    accepted = normalize_extensions(suffixes)
    discovery = DirectoryDiscoveryBuilder[RemoteFile].from_uris(
        uris,
        metadata_budget=current_directory_metadata_budget(memory_limit_bytes),
    )
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

    concurrency = execution_policy(threading_mode, memory_limit_bytes).source_discovery_concurrency
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

    grouped = list(groups.items())

    async def scan_index(index: int) -> None:
        """Scan one canonically ordered Azure parent group."""
        (account_url, container, parent), children = grouped[index]
        await scan_group(account_url, container, parent, children)

    await drain_ordered_indexed_results(
        len(grouped),
        scan_index,
        window=concurrency,
    )
    return discovery.finish()
