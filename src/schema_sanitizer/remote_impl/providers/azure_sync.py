"""Strictly synchronous Azure Blob operations for threading_mode='single'."""

from __future__ import annotations

from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Any, Iterator

from ...core_impl.memory_budget import current_operation_memory_ledger
from ...core_impl.temporary_storage import StreamingStorageReservation
from ...core_impl.uris import name_matches, normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    RemoteFile,
    current_directory_metadata_budget,
    split_parent_child,
)
from ..file_streams import write_sync_reader_to_file
from .azure import AzureRef, parse_uri, render_uri


class _AzureServiceOwner:
    """Own one synchronous service client and credential."""

    def __init__(self, service: Any, credential: Any) -> None:
        """Store both resources for deterministic same-thread closure."""
        self.service = service
        self.credential = credential

    def close(self) -> None:
        """Close both resources, preserving the first failure."""
        first_error: BaseException | None = None
        for resource in (self.service, self.credential):
            close = getattr(resource, "close", None)
            if close is None:
                continue
            try:
                close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error


@contextmanager
def open_service(ref: AzureRef) -> Iterator[Any]:
    """Open one blocking Azure service and credential on the caller thread."""
    blob = import_module("azure.storage.blob")
    identity = import_module("azure.identity")
    credential = identity.DefaultAzureCredential()
    try:
        service = blob.BlobServiceClient(account_url=ref.account_url, credential=credential)
    except BaseException:
        close = getattr(credential, "close", None)
        if close is not None:
            close()
        raise
    owner = _AzureServiceOwner(service, credential)
    try:
        yield owner.service
    finally:
        owner.close()


def _missing_or_permission(exc: Exception, uri: str) -> bool:
    """Classify Azure metadata failures, raising permissions immediately."""
    status = getattr(exc, "status_code", None)
    code = getattr(exc, "error_code", None)
    if status == 404 or code in {"BlobNotFound", "ContainerNotFound"}:
        return True
    if status in {401, 403}:
        raise PermissionError(
            f"Azure returned a permission error while checking source object: {uri!r}"
        ) from exc
    return False


def file_metadata(uri: str, *, memory_limit_bytes: int | None = None) -> RemoteFile | None:
    """Return Azure object metadata through the synchronous SDK."""
    del memory_limit_bytes
    ref = parse_uri(uri)
    with open_service(ref) as service:
        blob = service.get_blob_client(ref.container, ref.blob)
        try:
            properties = blob.get_blob_properties()
        except Exception as exc:
            if _missing_or_permission(exc, uri):
                return None
            raise
    raw_size = getattr(properties, "size", None)
    size = int(raw_size) if raw_size is not None else None
    return RemoteFile(uri, Path(ref.blob).name, size)


def download_file_with_service(
    service: Any,
    file: RemoteFile,
    local_path: str,
    *,
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download one Azure object while reserving disk before local writes."""
    ref = parse_uri(file.uri)
    stream = service.get_blob_client(ref.container, ref.blob).download_blob(max_concurrency=1)
    ledger = current_operation_memory_ledger()
    reservation = 4 * 1024 * 1024 if ledger is None else min(4 * 1024 * 1024, ledger.limit_bytes)
    iterator = iter(stream.chunks())

    def read(_size: int) -> bytes:
        """Provide a deterministic test or worker helper."""
        try:
            return next(iterator)
        except StopIteration:
            return b""

    write_sync_reader_to_file(
        read,
        local_path,
        chunk_bytes=reservation,
        storage_reservation=storage_reservation,
    )


def download_file(
    uri: str,
    local_path: str,
    *,
    memory_limit_bytes: int | None = None,
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download one Azure Blob without an asynchronous transport."""
    del memory_limit_bytes
    ref = parse_uri(uri)
    with open_service(ref) as service:
        download_file_with_service(
            service,
            RemoteFile(uri, Path(ref.blob).name),
            local_path,
            storage_reservation=storage_reservation,
        )


def upload_file(
    local_path: str,
    uri: str,
    *,
    memory_limit_bytes: int | None = None,
) -> None:
    """Upload one Azure Blob with SDK concurrency fixed to one."""
    del memory_limit_bytes
    ref = parse_uri(uri)
    size = Path(local_path).stat().st_size
    with open_service(ref) as service:
        blob = service.get_blob_client(ref.container, ref.blob)
        with Path(local_path).open("rb") as file_handle:
            blob.upload_blob(
                file_handle,
                overwrite=True,
                length=size,
                max_concurrency=1,
            )


def list_files(
    uri: str,
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
) -> list[RemoteFile]:
    """List direct Azure Blob children serially."""
    ref = parse_uri(uri)
    prefix = ref.blob.rstrip("/") + "/"
    files: list[RemoteFile] = []
    metadata_budget = current_directory_metadata_budget(memory_limit_bytes)
    with open_service(ref) as service:
        container = service.get_container_client(ref.container)
        for blob in container.walk_blobs(name_starts_with=prefix, delimiter="/"):
            name = getattr(blob, "name", None)
            if not isinstance(name, str):
                continue
            relative = name[len(prefix) :] if name.startswith(prefix) else name
            if not relative or "/" in relative or not name_matches(relative, suffixes):
                continue
            size = getattr(blob, "size", None)
            remote_file = RemoteFile(
                render_uri(ref, name),
                relative,
                size if isinstance(size, int) else None,
            )
            metadata_budget.charge_file(remote_file)
            files.append(remote_file)
    files.sort(key=lambda file: file.name)
    return files


def directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
) -> DirectoryDiscovery[RemoteFile]:
    """Discover requested Azure directories serially in canonical order."""
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
        key = (ref.account_url, ref.container, parent_prefix)
        groups.setdefault(key, {}).setdefault(child, []).append(uri)
    by_account: dict[str, list[tuple[str, str, dict[str, list[str]]]]] = {}
    for (account_url, container, parent), children in groups.items():
        by_account.setdefault(account_url, []).append((container, parent, children))
    for account_url, account_groups in by_account.items():
        ref = AzureRef(account_url, account_groups[0][0], "", "")
        with open_service(ref) as service:
            for container_name, parent_prefix, children in account_groups:
                prefix = f"{parent_prefix.rstrip('/')}/" if parent_prefix else ""
                container = service.get_container_client(container_name)
                for blob in container.list_blobs(name_starts_with=prefix):
                    name = getattr(blob, "name", None)
                    if not isinstance(name, str) or not name.startswith(prefix):
                        continue
                    relative = name[len(prefix) :]
                    child, separator, filename = relative.partition("/")
                    child_uris = children.get(child) if separator else None
                    if not child_uris or "/" in filename or not name_matches(filename, accepted):
                        continue
                    size = getattr(blob, "size", None)
                    discovery.add(
                        child_uris,
                        RemoteFile(
                            f"{account_url}/{container_name}/{name}",
                            filename,
                            size if isinstance(size, int) else None,
                        ),
                    )
    return discovery.finish()


__all__ = [
    "directories_containing_files",
    "download_file",
    "download_file_with_service",
    "file_metadata",
    "list_files",
    "open_service",
    "upload_file",
]
