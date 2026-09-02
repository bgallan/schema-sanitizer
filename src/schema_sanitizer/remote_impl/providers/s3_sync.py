"""Strictly synchronous Amazon S3 operations for threading_mode='single'.

It opens an owned blocking client and performs S3 metadata, listing, download, and
multipart publication without background transports.
"""

from __future__ import annotations

from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Any, Iterator

from ...core_impl.governed_sort import governed_sort
from ...core_impl.memory_budget import memory_budget
from ...core_impl.safe_errors import add_bounded_note
from ...core_impl.sync_retry import retry_sync
from ...core_impl.temporary_storage import StreamingStorageReservation
from ...core_impl.uris import content_type_for_uri, normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    current_directory_metadata_budget,
)
from ...sources.models import RemoteFile
from ..file_streams import write_sync_reader_to_file
from ..io_footprint import open_remote_local_file
from ..sync_cleanup_escrow import reserve_sync_cleanup
from ..sync_http import TRANSFER_CHUNK_BYTES
from ..upload_policy import (
    acquire_s3_multipart_manifest,
    read_upload_part,
    release_upload_payload,
    remote_upload_policy,
)
from . import (
    direct_child_items,
    next_page_token,
    requested_child_items,
    requested_directory_groups,
    retryable_sdk_error,
    sdk_error_identity,
)
from .s3 import _directory_location, parse_uri


def _client_options() -> dict[str, Any]:
    """Return a one-connection, externally retried Botocore configuration."""
    config_module = import_module("botocore.config")
    return {
        "config": config_module.Config(
            max_pool_connections=1,
            retries={"total_max_attempts": 1, "mode": "standard"},
        )
    }


class _S3ClientOwner:
    """Retryable physical owner for one synchronous Botocore client."""

    def __init__(self) -> None:
        """Create an empty slot for the blocking Botocore client."""
        self.client: Any | None = None

    def close(self) -> None:
        """Physically close the Botocore client before clearing its authoritative slot."""
        client = self.client
        if client is None:
            return
        close = getattr(client, "close", None)
        if close is not None:
            close()
        self.client = None


@contextmanager
def open_client() -> Iterator[Any]:
    """Open S3 under pre-reserved terminal cleanup + network-FD ownership."""
    owner = _S3ClientOwner()
    reservation = reserve_sync_cleanup(label="s3_sync_client", network_fds=1)
    reservation.bind_owner(owner)
    primary: BaseException | None = None
    try:
        session_module = import_module("botocore.session")
        owner.client = session_module.get_session().create_client("s3", **_client_options())
        yield owner.client
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            reservation.close_and_commit()
        except BaseException as cleanup_error:
            reservation.abandon_to_escrow()
            if primary is not None:
                add_bounded_note(
                    primary, "S3 synchronous cleanup retained for retry", cleanup_error
                )
            else:
                raise


def _retryable(exc: Exception) -> bool:
    """Return whether one idempotent S3 request is transient."""
    return retryable_sdk_error(exc)


def file_metadata(uri: str, *, memory_limit_bytes: int | None = None) -> RemoteFile | None:
    """Return S3 object metadata through a blocking HEAD request."""
    ref = parse_uri(uri)
    retries = memory_budget(memory_limit_bytes).async_retries
    with open_client() as client:

        def request() -> dict[str, Any]:
            """Perform one retryable blocking HEAD request."""
            return client.head_object(Bucket=ref.bucket, Key=ref.key)

        try:
            response = retry_sync(
                request, retries=retries, should_retry=_retryable, throttle_key="s3"
            )
        except Exception as exc:
            status, code = sdk_error_identity(exc)
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return None
            if status in {401, 403} or code in {"403", "AccessDenied"}:
                raise PermissionError(
                    f"S3 returned a permission error while checking source object: {uri!r}"
                ) from exc
            raise
    raw_size = response.get("ContentLength")
    size = int(raw_size) if raw_size is not None else None
    return RemoteFile(uri, Path(ref.key).name, size)


def download_file_with_client(
    client: Any,
    file: RemoteFile,
    local_path: str,
    *,
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download one object while reserving local storage before writes."""
    ref = parse_uri(file.uri)
    response = client.get_object(Bucket=ref.bucket, Key=ref.key)
    body = response["Body"]
    try:
        write_sync_reader_to_file(
            body.read,
            local_path,
            chunk_bytes=TRANSFER_CHUNK_BYTES,
            storage_reservation=storage_reservation,
        )
    finally:
        close = getattr(body, "close", None)
        if close is not None:
            close()


def download_file(
    uri: str,
    local_path: str,
    *,
    memory_limit_bytes: int | None = None,
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download one S3 object on the calling thread with bounded replay."""
    file = RemoteFile(uri, Path(parse_uri(uri).key).name)
    retries = memory_budget(memory_limit_bytes).async_retries
    target = Path(local_path)
    with open_client() as client:

        def request() -> None:
            """Truncate the destination before each blocking GET attempt."""
            target.unlink(missing_ok=True)
            download_file_with_client(
                client, file, local_path, storage_reservation=storage_reservation
            )

        try:
            retry_sync(request, retries=retries, should_retry=_retryable, throttle_key="s3")
        except BaseException:
            target.unlink(missing_ok=True)
            raise


def _multipart_upload(
    client: Any,
    local_path: str,
    uri: str,
    *,
    memory_limit_bytes: int | None,
) -> None:
    """Upload S3 parts sequentially and commit them in canonical order."""
    ref = parse_uri(uri)
    tuning = remote_upload_policy(
        "s3",
        local_path,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode="single",
    )
    source = Path(local_path)
    initial_stat = source.stat()
    created = client.create_multipart_upload(
        Bucket=ref.bucket,
        Key=ref.key,
        ContentType=content_type_for_uri(uri),
    )
    upload_id = created.get("UploadId") if isinstance(created, dict) else None
    if not isinstance(upload_id, str) or not upload_id:
        raise RuntimeError("S3 multipart upload did not return an upload id")
    retries = memory_budget(memory_limit_bytes).async_retries
    parts: list[dict[str, Any]] = []
    manifest = None
    try:
        manifest = acquire_s3_multipart_manifest(tuning.part_count)
        for index in range(tuning.part_count):
            payload = read_upload_part(local_path, index, tuning.part_bytes, tuning.file_size)
            part_number = index + 1

            def send_part() -> Any:
                """Send one replayable in-memory multipart body."""
                return client.upload_part(
                    Bucket=ref.bucket,
                    Key=ref.key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=payload,
                )

            try:
                response = retry_sync(
                    send_part, retries=retries, should_retry=_retryable, throttle_key="s3"
                )
                etag = response.get("ETag") if isinstance(response, dict) else None
                if not isinstance(etag, str) or not etag:
                    raise RuntimeError(f"S3 multipart part {part_number} did not return an ETag")
                manifest.append_part(parts, etag, part_number)
            finally:
                release_upload_payload(payload)
        final_stat = source.stat()
        if (final_stat.st_size, final_stat.st_mtime_ns) != (
            initial_stat.st_size,
            initial_stat.st_mtime_ns,
        ):
            raise OSError("remote upload spool changed before S3 multipart commit")
        client.complete_multipart_upload(
            Bucket=ref.bucket,
            Key=ref.key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except BaseException as exc:
        try:
            client.abort_multipart_upload(
                Bucket=ref.bucket,
                Key=ref.key,
                UploadId=upload_id,
            )
        except BaseException as abort_exc:
            add_bounded_note(exc, "S3 multipart abort also failed", abort_exc)
        raise
    finally:
        if manifest is not None:
            manifest.close()


def upload_file(
    local_path: str,
    uri: str,
    *,
    memory_limit_bytes: int | None = None,
) -> None:
    """Upload one local spool without an SDK transfer-manager thread pool."""
    ref = parse_uri(uri)
    tuning = remote_upload_policy(
        "s3",
        local_path,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode="single",
    )
    with open_client() as client:
        if tuning.multipart:
            _multipart_upload(client, local_path, uri, memory_limit_bytes=memory_limit_bytes)
            return
        retries = memory_budget(memory_limit_bytes).async_retries

        def request() -> None:
            """Reopen the complete spool for each direct PUT attempt."""
            with open_remote_local_file(
                local_path, "rb", label="s3_sync_upload_source"
            ) as file_handle:
                client.put_object(Bucket=ref.bucket, Key=ref.key, Body=file_handle)

        retry_sync(request, retries=retries, should_retry=_retryable, throttle_key="s3")


def _list_page(client: Any, bucket: str, prefix: str, token: str | None) -> dict[str, Any]:
    """Fetch one direct-child S3 listing page."""
    kwargs: dict[str, Any] = {
        "Bucket": bucket,
        "Prefix": prefix,
        "Delimiter": "/",
        "MaxKeys": 1000,
    }
    if token:
        kwargs["ContinuationToken"] = token
    return client.list_objects_v2(**kwargs)


def list_files(
    uri: str,
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
) -> list[RemoteFile]:
    """List direct S3 children serially on one blocking client."""
    ref = parse_uri(uri)
    prefix = ref.key.rstrip("/") + "/"
    files: list[RemoteFile] = []
    metadata_budget = current_directory_metadata_budget(memory_limit_bytes)
    retries = memory_budget(memory_limit_bytes).async_retries
    with open_client() as client:
        token: str | None = None
        while True:
            payload = retry_sync(
                lambda: _list_page(client, ref.bucket, prefix, token),
                retries=retries,
                should_retry=_retryable,
                throttle_key="s3",
            )
            for item, relative in direct_child_items(
                payload.get("Contents", ()),
                prefix,
                suffixes,
                "Key",
            ):
                key = item["Key"]
                size = item.get("Size") if isinstance(item.get("Size"), int) else None
                remote_file = RemoteFile(f"s3://{ref.bucket}/{key}", relative, size)
                metadata_budget.charge_file(remote_file, associations=4)
                files.append(remote_file)
            token = next_page_token(
                payload,
                "NextContinuationToken",
                truncated_key="IsTruncated",
                missing_error=f"S3 list for {uri!r} was truncated without a token",
            )
            if token is None:
                break
    governed_sort(files, key=lambda file: file.name, stage="remote_discovery_sort")
    return files


def directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
) -> DirectoryDiscovery[RemoteFile]:
    """Discover requested S3 child directories serially in canonical order."""
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
    retries = memory_budget(memory_limit_bytes).async_retries
    with open_client() as client:
        for (bucket, parent_prefix), children in groups.items():
            prefix = f"{parent_prefix.rstrip('/')}/" if parent_prefix else ""
            token: str | None = None
            while True:
                payload = retry_sync(
                    lambda: _list_page(client, bucket, prefix, token),
                    retries=retries,
                    should_retry=_retryable,
                    throttle_key="s3",
                )
                for item, child_uris, filename in requested_child_items(
                    payload.get("Contents", ()),
                    prefix,
                    children,
                    accepted,
                    "Key",
                ):
                    key = item["Key"]
                    size = item.get("Size") if isinstance(item.get("Size"), int) else None
                    discovery.add(
                        child_uris,
                        RemoteFile(f"s3://{bucket}/{key}", filename, size),
                    )
                token = next_page_token(
                    payload,
                    "NextContinuationToken",
                    truncated_key="IsTruncated",
                    missing_error=(
                        f"S3 bulk source discovery for {parent_prefix!r} was truncated without a token"
                    ),
                )
                if token is None:
                    break
    return discovery.finish()


__all__ = [
    "directories_containing_files",
    "download_file",
    "download_file_with_client",
    "file_metadata",
    "list_files",
    "open_client",
    "upload_file",
]
