"""Strictly synchronous Azure Blob operations for threading_mode='single'.

It opens Blob services on the caller thread and performs bounded metadata, listing,
download, and upload operations with explicit ownership.
"""

from __future__ import annotations

from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Any, Iterator

from ...core_impl.governed_sort import governed_sort
from ...core_impl.memory_budget import current_operation_memory_ledger
from ...core_impl.temporary_storage import StreamingStorageReservation
from ...core_impl.uris import normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    current_directory_metadata_budget,
)
from ...sources.models import RemoteFile
from ..file_streams import write_sync_reader_to_file
from ..io_footprint import open_remote_local_file
from ..sync_cleanup_escrow import reserve_sync_cleanup
from . import (
    direct_child_items,
    requested_child_items,
    requested_directory_groups,
    sdk_error_identity,
)
from .azure import AzureRef, _directory_location, parse_uri, render_uri


class _AzureServiceOwner:
    """Retryable owner for one synchronous service client and credential."""

    def __init__(self) -> None:
        """Create empty service and credential slots for retryable synchronous cleanup."""
        self.service: Any | None = None
        self.credential: Any | None = None

    def close(self) -> None:
        """Retire each resource only after its own physical close succeeds."""
        first_error: BaseException | None = None
        for attribute in ("service", "credential"):
            resource = getattr(self, attribute)
            if resource is None:
                continue
            close = getattr(resource, "close", None)
            if close is None:
                setattr(self, attribute, None)
                continue
            try:
                close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
            else:
                setattr(self, attribute, None)
        if first_error is not None:
            raise first_error


@contextmanager
def open_service(ref: AzureRef) -> Iterator[Any]:
    """Open Azure under pre-reserved terminal cleanup + network-FD ownership."""
    owner = _AzureServiceOwner()
    reservation = reserve_sync_cleanup(label="azure_sync_service", network_fds=1)
    reservation.bind_owner(owner)
    primary: BaseException | None = None
    try:
        blob = import_module("azure.storage.blob")
        identity = import_module("azure.identity")
        owner.credential = identity.DefaultAzureCredential()
        owner.service = blob.BlobServiceClient(
            account_url=ref.account_url, credential=owner.credential
        )
        yield owner.service
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            reservation.close_and_commit()
        except BaseException as cleanup_error:
            reservation.abandon_to_escrow()
            if primary is not None:
                from ...core_impl.safe_errors import add_bounded_note

                add_bounded_note(
                    primary, "Azure synchronous cleanup retained for retry", cleanup_error
                )
            else:
                raise


def _missing_or_permission(exc: Exception, uri: str) -> bool:
    """Classify Azure metadata failures, raising permissions immediately."""
    status, code = sdk_error_identity(exc)
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
        """Return the next Blob chunk, or empty bytes after iterator exhaustion."""
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
        with open_remote_local_file(
            local_path, "rb", label="azure_sync_upload_source"
        ) as file_handle:
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
        for blob, relative in direct_child_items(
            container.walk_blobs(name_starts_with=prefix, delimiter="/"),
            prefix,
            suffixes,
            "name",
        ):
            name = blob.name
            size = getattr(blob, "size", None)
            remote_file = RemoteFile(
                render_uri(ref, name),
                relative,
                size if isinstance(size, int) else None,
            )
            metadata_budget.charge_file(remote_file, associations=4)
            files.append(remote_file)
    governed_sort(files, key=lambda file: file.name, stage="remote_discovery_sort")
    return files


def directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
) -> DirectoryDiscovery[RemoteFile]:
    """Discover requested Azure directories serially in canonical order."""
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
    by_account: dict[str, list[tuple[str, str, dict[str, list[str]]]]] = {}
    for (account_url, container, parent), children in groups.items():
        discovery.publish_group_association(
            lambda: by_account.setdefault(account_url, []).append((container, parent, children))
        )
    for account_url, account_groups in by_account.items():
        ref = AzureRef(account_url, account_groups[0][0], "", "")
        with open_service(ref) as service:
            for container_name, parent_prefix, children in account_groups:
                prefix = f"{parent_prefix.rstrip('/')}/" if parent_prefix else ""
                container = service.get_container_client(container_name)
                for blob, child_uris, filename in requested_child_items(
                    container.list_blobs(name_starts_with=prefix),
                    prefix,
                    children,
                    accepted,
                    "name",
                ):
                    name = blob.name
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
