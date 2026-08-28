"""Azure Blob URI, discovery, and object operations.

It parses Blob URIs, manages asynchronous credentials and services, lists metadata,
transfers objects, and rolls back failed client construction.
"""

from __future__ import annotations

import asyncio
import inspect
import threading
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ...core_impl.async_scheduler import AsyncResultMemoryContract, drain_ordered_iterable_results
from ...core_impl.execution_policy import execution_policy
from ...core_impl.governed_sort import governed_sort
from ...core_impl.memory_budget import current_operation_memory_ledger
from ...core_impl.safe_errors import add_bounded_note
from ...core_impl.temporary_storage import StreamingStorageReservation
from ...core_impl.terminal_ownership import publish_terminal_owner, retire_terminal_owner
from ...core_impl.uris import normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    current_directory_metadata_budget,
)
from ...sources.models import RemoteFile
from ..file_streams import write_async_iterator_to_file
from ..io_footprint import open_remote_local_file
from ..provider_session_pool import current_provider_session_pool
from ..transport import collect_bounded_async_chunks
from ..upload_policy import remote_upload_policy
from . import (
    direct_child_name,
    requested_child,
    requested_directory_groups,
    sdk_error_identity,
)

_MAX_AZURE_ROLLBACK_OWNERS = 128
_AZURE_ROLLBACK_LOCK = threading.Lock()
_AZURE_ROLLBACK_GENERATION = 0


@dataclass(slots=True)
class _AzureRollbackSlot:
    """Preallocated terminal escrow reserved before credential construction."""

    generation: int = 0
    state: str = "free"
    credential: Any | None = None
    task: asyncio.Task[Any] | None = None


_AZURE_ROLLBACK_SLOTS = [_AzureRollbackSlot() for _ in range(_MAX_AZURE_ROLLBACK_OWNERS)]


def _azure_rollback_token(index: int, generation: int) -> int:
    """Encode an Azure rollback slot and generation as a nonzero token."""
    return (generation << 8) | index | 1


def _reserve_azure_rollback_slot() -> tuple[int, int] | None:
    """Reserve terminal ownership before a fallible credential is created."""
    global _AZURE_ROLLBACK_GENERATION
    with _AZURE_ROLLBACK_LOCK:
        for index, slot in enumerate(_AZURE_ROLLBACK_SLOTS):
            if slot.state != "free":
                continue
            _AZURE_ROLLBACK_GENERATION += 1
            slot.generation = _AZURE_ROLLBACK_GENERATION
            slot.state = "reserved"
            slot.credential = None
            slot.task = None
            return index, slot.generation
    return None


def _release_azure_rollback_reservation(reservation: tuple[int, int]) -> None:
    """Release azure rollback reservation."""
    index, generation = reservation
    with _AZURE_ROLLBACK_LOCK:
        slot = _AZURE_ROLLBACK_SLOTS[index]
        if slot.generation != generation or slot.state != "reserved":
            return
        slot.state = "free"
        slot.credential = None
        slot.task = None


async def _retry_azure_credential_rollback(index: int, generation: int) -> None:
    """Keep a failed constructor credential owned until physical close commits."""
    delay = 0.01
    token = _azure_rollback_token(index, generation)
    while True:
        with _AZURE_ROLLBACK_LOCK:
            slot = _AZURE_ROLLBACK_SLOTS[index]
            if slot.generation != generation or slot.state != "published":
                return
            credential = slot.credential
        close = getattr(credential, "close", None)
        try:
            if close is not None:
                result = close()
                if inspect.isawaitable(result):
                    await result
        except asyncio.CancelledError:
            # No false commit: the static slot remains the authoritative owner.
            continue
        except BaseException:
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                continue
            delay = min(0.25, delay * 2.0)
            continue
        with _AZURE_ROLLBACK_LOCK:
            slot = _AZURE_ROLLBACK_SLOTS[index]
            if slot.generation == generation and slot.state == "published":
                slot.state = "free"
                slot.credential = None
                slot.task = None
        retire_terminal_owner("azure_credential_rollback", token)
        return


def _publish_azure_credential_rollback(reservation: tuple[int, int], credential: Any) -> bool:
    """Publish a credential into its already-reserved terminal slot."""
    index, generation = reservation
    token = _azure_rollback_token(index, generation)
    with _AZURE_ROLLBACK_LOCK:
        slot = _AZURE_ROLLBACK_SLOTS[index]
        if slot.generation != generation or slot.state != "reserved":
            return False
        slot.credential = credential
        slot.state = "published"
    publish_terminal_owner("azure_credential_rollback", token, retained_bytes=512)
    try:
        task = asyncio.get_running_loop().create_task(
            _retry_azure_credential_rollback(index, generation)
        )
    except BaseException:
        # The preallocated slot is still authoritative. Provider-pool shutdown
        # and later Azure safe points can drive this generation explicitly.
        return True
    with _AZURE_ROLLBACK_LOCK:
        slot = _AZURE_ROLLBACK_SLOTS[index]
        if slot.generation == generation and slot.state == "published":
            slot.task = task
    return True


async def drain_azure_credential_rollbacks() -> int:
    """Drive published generations that could not create a retry task."""
    pending: list[tuple[int, int]] = []
    with _AZURE_ROLLBACK_LOCK:
        for index, slot in enumerate(_AZURE_ROLLBACK_SLOTS):
            if slot.state == "published" and slot.task is None:
                pending.append((index, slot.generation))
    for index, generation in pending:
        # One direct attempt is enough for a safe point. A failure leaves the
        # static owner published and will be retried at the next safe point.
        with _AZURE_ROLLBACK_LOCK:
            slot = _AZURE_ROLLBACK_SLOTS[index]
            credential = (
                slot.credential
                if slot.generation == generation and slot.state == "published"
                else None
            )
        if credential is None:
            continue
        try:
            close = getattr(credential, "close", None)
            if close is not None:
                result = close()
                if inspect.isawaitable(result):
                    await result
        except BaseException:
            continue
        token = _azure_rollback_token(index, generation)
        with _AZURE_ROLLBACK_LOCK:
            slot = _AZURE_ROLLBACK_SLOTS[index]
            if slot.generation == generation and slot.state == "published":
                slot.state = "free"
                slot.credential = None
                slot.task = None
        retire_terminal_owner("azure_credential_rollback", token)
    with _AZURE_ROLLBACK_LOCK:
        return sum(1 for slot in _AZURE_ROLLBACK_SLOTS if slot.state == "published")


class _AzureServiceOwner:
    """Own one Blob service and credential with retryable per-resource cleanup."""

    def __init__(self, service: Any, credential: Any) -> None:
        """Bind the asynchronous Blob service and credential with independent close state."""
        self._service = service
        self._credential = credential
        self._service_closed = False
        self._credential_closed = False
        self._closed = False

    def __getattr__(self, name: str) -> Any:
        """Delegate unresolved attributes to the wrapped object."""
        return getattr(self._service, name)

    async def _close_one(self, resource: Any, flag_name: str) -> None:
        """Close one Azure SDK resource without masking earlier failures."""
        if getattr(self, flag_name):
            return
        close = getattr(resource, "close", None)
        if close is not None:
            result = close()
            if inspect.isawaitable(result):
                await result
        setattr(self, flag_name, True)

    async def close(self) -> None:
        """Retry only resources whose physical close has not yet committed."""
        if self._closed:
            return
        first_error: BaseException | None = None
        for resource, flag_name in (
            (self._service, "_service_closed"),
            (self._credential, "_credential_closed"),
        ):
            try:
                await self._close_one(resource, flag_name)
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
                if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                    break
        self._closed = self._service_closed and self._credential_closed
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


def _directory_location(uri: str) -> tuple[tuple[str, str], str]:
    """Return the stable grouping location and object name for an Azure URI."""
    ref = parse_uri(uri)
    return (ref.account_url, ref.container), ref.blob


async def _open_service_unpooled(ref: AzureRef) -> Any:
    """Create one directly owned service with construction escrow pre-reserved."""
    reservation = _reserve_azure_rollback_slot()
    if reservation is None:
        raise RuntimeError("Azure credential cleanup escrow exhausted")
    blob = import_module("azure.storage.blob.aio")
    identity = import_module("azure.identity.aio")
    try:
        credential = identity.DefaultAzureCredential()
    except BaseException:
        _release_azure_rollback_reservation(reservation)
        raise
    try:
        service = blob.BlobServiceClient(
            account_url=ref.account_url,
            credential=credential,
        )
    except BaseException as primary:
        close = getattr(credential, "close", None)
        if close is not None:
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except BaseException as cleanup_error:
                retained = _publish_azure_credential_rollback(reservation, credential)
                add_bounded_note(
                    primary,
                    (
                        "Azure credential cleanup also failed after service construction; "
                        + (
                            "retry ownership retained"
                            if retained
                            else "retry ownership publication failed"
                        )
                    ),
                    cleanup_error,
                )
                if retained:
                    reservation = None
        if reservation is not None:
            _release_azure_rollback_reservation(reservation)
        raise
    _release_azure_rollback_reservation(reservation)
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
    stream = await blob.download_blob(max_concurrency=1)
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
        # SDK-internal fanout bypasses the process-wide remote-I/O/task/FD
        # governors. Keep one blob transfer serial and obtain concurrency only
        # from the schema-sanitizer scheduler across governed operations.
        blob = service.get_blob_client(ref.container, ref.blob)
        stream = await blob.download_blob(max_concurrency=1)
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
            status, code = sdk_error_identity(exc)
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


async def download_bytes(uri: str, *, maximum_bytes: int) -> bytes:
    """Download one Azure Blob only under an explicit materialization ceiling."""
    ref = parse_uri(uri)
    service = await open_service(ref)
    try:
        stream = await service.get_blob_client(ref.container, ref.blob).download_blob(
            max_concurrency=1
        )
        return await collect_bounded_async_chunks(
            stream.chunks(), maximum_bytes=maximum_bytes, stage="azure_download_bytes"
        )
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
        with open_remote_local_file(local_path, "rb", label="azure_upload_source") as file_handle:
            await blob.upload_blob(
                file_handle,
                overwrite=True,
                length=tuning.file_size,
                max_concurrency=1,
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
        blobs = container.walk_blobs(name_starts_with=prefix, delimiter="/")
        async for blob in blobs:
            relative = direct_child_name(getattr(blob, "name", None), prefix, suffixes)
            if relative is None:
                continue
            name = blob.name
            size = getattr(blob, "size", None)
            remote_file = RemoteFile(
                render_uri(ref, name), relative, size if isinstance(size, int) else None
            )
            metadata_budget.charge_file(remote_file, associations=4)
            files.append(remote_file)
    finally:
        await service.close()
    governed_sort(files, key=lambda file: file.name, stage="remote_discovery_sort")
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
    metadata_budget = current_directory_metadata_budget(memory_limit_bytes)
    discovery = DirectoryDiscoveryBuilder[RemoteFile].from_uris(
        uris,
        metadata_budget=metadata_budget,
    )
    groups = requested_directory_groups(
        uris,
        discovery,
        _directory_location,
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
                    match = requested_child(getattr(blob, "name", None), prefix, children, accepted)
                    if match is None:
                        continue
                    child_uris, filename = match
                    name = blob.name
                    size = getattr(blob, "size", None)
                    remote_file = RemoteFile(
                        render_uri(ref, name),
                        filename,
                        size if isinstance(size, int) else None,
                    )
                    discovery.add(child_uris, remote_file)
        finally:
            await service.close()

    async def scan_key(key: tuple[str, str, str]) -> None:
        """Scan one Azure parent group without materialising all group keys."""
        account_url, container, parent = key
        await scan_group(account_url, container, parent, groups[key])

    await drain_ordered_iterable_results(
        groups,
        scan_key,
        window=concurrency,
        memory_contract=AsyncResultMemoryContract(preflight_bytes=64),
    )
    return discovery.finish()
