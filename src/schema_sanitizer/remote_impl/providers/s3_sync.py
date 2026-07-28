"""Strictly synchronous Amazon S3 operations for threading_mode='single'."""

from __future__ import annotations

from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Any, Iterator

from ...core_impl.memory_budget import memory_budget
from ...core_impl.sync_retry import retry_sync
from ...core_impl.uris import content_type_for_uri, name_matches, normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    RemoteFile,
    split_parent_child,
)
from ..sync_http import TRANSFER_CHUNK_BYTES
from ..upload_policy import read_upload_part, remote_upload_policy
from .s3 import parse_uri


def _client_options() -> dict[str, Any]:
    """Return a one-connection, externally retried Botocore configuration."""
    config_module = import_module("botocore.config")
    return {
        "config": config_module.Config(
            max_pool_connections=1,
            retries={"total_max_attempts": 1, "mode": "standard"},
        )
    }


@contextmanager
def open_client() -> Iterator[Any]:
    """Open one blocking S3 client and close it on the caller thread."""
    session_module = import_module("botocore.session")
    client = session_module.get_session().create_client("s3", **_client_options())
    try:
        yield client
    finally:
        close = getattr(client, "close", None)
        if close is not None:
            close()


def _error_identity(exc: Exception) -> tuple[Any, Any]:
    """Return one Botocore error's status and service code."""
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None, None
    metadata = response.get("ResponseMetadata")
    status = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
    error = response.get("Error")
    code = error.get("Code") if isinstance(error, dict) else None
    return status, code


def _retryable(exc: Exception) -> bool:
    """Return whether one idempotent S3 request is transient."""
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    status, code = _error_identity(exc)
    return (
        status == 429
        or (isinstance(status, int) and status >= 500)
        or code in {"InternalError", "RequestTimeout", "ServiceUnavailable", "SlowDown"}
    )


def file_metadata(uri: str, *, memory_limit_bytes: int | None = None) -> RemoteFile | None:
    """Return S3 object metadata through a blocking HEAD request."""
    ref = parse_uri(uri)
    retries = memory_budget(memory_limit_bytes).async_retries
    with open_client() as client:

        def request() -> dict[str, Any]:
            """Perform one retryable blocking HEAD request."""
            return client.head_object(Bucket=ref.bucket, Key=ref.key)

        try:
            response = retry_sync(request, retries=retries, should_retry=_retryable)
        except Exception as exc:
            status, code = _error_identity(exc)
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


def download_file_with_client(client: Any, file: RemoteFile, local_path: str) -> None:
    """Download one object through an already-open blocking S3 client."""
    ref = parse_uri(file.uri)
    response = client.get_object(Bucket=ref.bucket, Key=ref.key)
    body = response["Body"]
    try:
        with Path(local_path).open("wb") as file_handle:
            while chunk := body.read(TRANSFER_CHUNK_BYTES):
                file_handle.write(chunk)
    finally:
        close = getattr(body, "close", None)
        if close is not None:
            close()


def download_file(
    uri: str,
    local_path: str,
    *,
    memory_limit_bytes: int | None = None,
) -> None:
    """Download one S3 object on the calling thread with bounded replay."""
    file = RemoteFile(uri, Path(parse_uri(uri).key).name)
    retries = memory_budget(memory_limit_bytes).async_retries
    target = Path(local_path)
    with open_client() as client:

        def request() -> None:
            """Truncate the destination before each blocking GET attempt."""
            target.unlink(missing_ok=True)
            download_file_with_client(client, file, local_path)

        try:
            retry_sync(request, retries=retries, should_retry=_retryable)
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
    try:
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

            response = retry_sync(send_part, retries=retries, should_retry=_retryable)
            etag = response.get("ETag") if isinstance(response, dict) else None
            if not isinstance(etag, str) or not etag:
                raise RuntimeError(f"S3 multipart part {part_number} did not return an ETag")
            parts.append({"ETag": etag, "PartNumber": part_number})
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
            exc.add_note(f"S3 multipart abort also failed: {abort_exc!r}")
        raise


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
            with Path(local_path).open("rb") as file_handle:
                client.put_object(Bucket=ref.bucket, Key=ref.key, Body=file_handle)

        retry_sync(request, retries=retries, should_retry=_retryable)


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
    retries = memory_budget(memory_limit_bytes).async_retries
    with open_client() as client:
        token: str | None = None
        while True:
            payload = retry_sync(
                lambda: _list_page(client, ref.bucket, prefix, token),
                retries=retries,
                should_retry=_retryable,
            )
            for item in payload.get("Contents", ()):
                key = item.get("Key")
                if not isinstance(key, str):
                    continue
                relative = key[len(prefix) :] if key.startswith(prefix) else key
                if not relative or "/" in relative or not name_matches(relative, suffixes):
                    continue
                size = item.get("Size") if isinstance(item.get("Size"), int) else None
                files.append(RemoteFile(f"s3://{ref.bucket}/{key}", relative, size))
            if not payload.get("IsTruncated"):
                break
            token = payload.get("NextContinuationToken")
            if not isinstance(token, str) or not token:
                raise RuntimeError(f"S3 list for {uri!r} was truncated without a token")
    files.sort(key=lambda file: file.name)
    return files


def directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
) -> DirectoryDiscovery[RemoteFile]:
    """Discover requested S3 child directories serially in canonical order."""
    accepted = normalize_extensions(suffixes)
    discovery = DirectoryDiscoveryBuilder[RemoteFile].from_uris(uris)
    groups: dict[tuple[str, str], dict[str, list[str]]] = {}
    for uri in uris:
        ref = parse_uri(uri)
        parsed = split_parent_child(ref.key)
        if parsed is None:
            continue
        parent_prefix, child = parsed
        groups.setdefault((ref.bucket, parent_prefix), {}).setdefault(child, []).append(uri)
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
                )
                for item in payload.get("Contents", ()):
                    key = item.get("Key")
                    if not isinstance(key, str) or not key.startswith(prefix):
                        continue
                    relative = key[len(prefix) :]
                    child, separator, filename = relative.partition("/")
                    child_uris = children.get(child) if separator else None
                    if not child_uris or "/" in filename or not name_matches(filename, accepted):
                        continue
                    size = item.get("Size") if isinstance(item.get("Size"), int) else None
                    discovery.add(
                        child_uris,
                        RemoteFile(f"s3://{bucket}/{key}", filename, size),
                    )
                if not payload.get("IsTruncated"):
                    break
                token = payload.get("NextContinuationToken")
                if not isinstance(token, str) or not token:
                    raise RuntimeError(
                        f"S3 bulk source discovery for {parent_prefix!r} was truncated without a token"
                    )
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
