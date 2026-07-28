"""Amazon S3 URI, discovery, and object operations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ...core_impl.async_scheduler import (
    drain_ordered_indexed_results,
    ordered_indexed_results,
    retry_async,
)
from ...core_impl.execution_policy import execution_policy
from ...core_impl.memory_budget import memory_budget
from ...core_impl.uris import content_type_for_uri, name_matches, normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    RemoteFile,
    split_parent_child,
)
from ..provider_session_pool import current_provider_session_pool
from ..transport import TRANSFER_CHUNK_BYTES
from ..upload_policy import read_upload_part, remote_upload_policy


@dataclass(frozen=True, slots=True)
class S3Ref:
    """Parsed S3 object reference."""

    bucket: str
    key: str


def parse_uri(uri: str) -> S3Ref:
    """Parse an ``s3://bucket/key`` URI."""
    parsed = urlparse(uri)
    if parsed.scheme.lower() != "s3" or not parsed.netloc:
        raise ValueError(f"not an S3 URI: {uri!r}")
    return S3Ref(parsed.netloc, parsed.path.lstrip("/"))


def client_options() -> dict[str, Any]:
    """Return explicit SDK options owned by Schema-Sanitizer.

    Credential, region, and endpoint resolution are delegated to the SDK;
    Schema-Sanitizer does not inspect process environment variables.
    """
    return {}


async def _open_client_unpooled() -> Any:
    """Create one directly owned aiobotocore S3 client manager."""
    aiobotocore = import_module("aiobotocore.session")
    return aiobotocore.get_session().create_client("s3", **client_options())


async def open_client() -> Any:
    """Open or borrow an aiobotocore S3 client for the current operation."""
    pool = current_provider_session_pool()
    if pool is None:
        return await _open_client_unpooled()
    return await pool.borrow_manager(("s3",), _open_client_unpooled)


async def download_bytes(client: Any, file: RemoteFile) -> bytes:
    """Download one S3 object into bytes using a shared client."""
    ref = parse_uri(file.uri)
    response = await client.get_object(Bucket=ref.bucket, Key=ref.key)
    body = response["Body"]
    async with body:
        return await body.read()


async def file_exists(
    uri: str, *, memory_limit_bytes: int | None = None, threading_mode: str = "single"
) -> bool:
    """Return whether one S3 object exists."""
    return (
        await file_metadata(
            uri, memory_limit_bytes=memory_limit_bytes, threading_mode=threading_mode
        )
        is not None
    )


async def file_metadata(
    uri: str, *, memory_limit_bytes: int | None = None, threading_mode: str = "single"
) -> RemoteFile | None:
    """Return S3 object metadata using the existence HEAD request."""
    ref = parse_uri(uri)
    async with await open_client() as client:
        try:
            response = await client.head_object(Bucket=ref.bucket, Key=ref.key)
            raw_size = response.get("ContentLength")
            size = int(raw_size) if raw_size is not None else None
            return RemoteFile(uri, Path(ref.key).name, size)
        except Exception as exc:
            response = getattr(exc, "response", None)
            code = None
            status = None
            if isinstance(response, dict):
                error = response.get("Error")
                if isinstance(error, dict):
                    code = error.get("Code")
                metadata = response.get("ResponseMetadata")
                if isinstance(metadata, dict):
                    status = metadata.get("HTTPStatusCode")
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return None
            if status in {401, 403} or code in {"403", "AccessDenied"}:
                raise PermissionError(
                    f"S3 returned a permission error while checking source object: {uri!r}"
                ) from exc
            raise


async def download_file(
    uri: str,
    local_path: str,
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> None:
    """Download one S3 object to a local file."""
    async with await open_client() as client:
        await download_file_with_client(
            client,
            RemoteFile(uri, Path(urlparse(uri).path).name),
            local_path,
        )


async def download_file_with_client(client: Any, file: RemoteFile, local_path: str) -> None:
    """Download one S3 object to a local file using a shared client."""
    ref = parse_uri(file.uri)
    response = await client.get_object(Bucket=ref.bucket, Key=ref.key)
    body = response["Body"]
    async with body:
        with Path(local_path).open("wb") as file_handle:
            while chunk := await body.read(TRANSFER_CHUNK_BYTES):
                file_handle.write(chunk)


def _should_retry_s3_part(exc: Exception) -> bool:
    """Return whether one idempotent UploadPart request may be retried."""
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    if exc.__class__.__module__.split(".", 1)[0] in {"aiohttp", "aiobotocore"}:
        return True
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    metadata = response.get("ResponseMetadata")
    status = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
    error = response.get("Error")
    code = error.get("Code") if isinstance(error, dict) else None
    return (
        status == 429
        or (isinstance(status, int) and status >= 500)
        or code
        in {
            "InternalError",
            "RequestTimeout",
            "ServiceUnavailable",
            "SlowDown",
        }
    )


async def _upload_file_multipart(
    client: Any,
    local_path: str,
    uri: str,
    *,
    memory_limit_bytes: int | None,
    threading_mode: str,
) -> None:
    """Publish one file through bounded S3 multipart upload and ordered commit."""
    ref = parse_uri(uri)
    tuning = remote_upload_policy(
        "s3",
        local_path,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )
    source = Path(local_path)
    initial_stat = source.stat()
    created = await client.create_multipart_upload(
        Bucket=ref.bucket,
        Key=ref.key,
        ContentType=content_type_for_uri(uri),
    )
    upload_id = created.get("UploadId") if isinstance(created, dict) else None
    if not isinstance(upload_id, str) or not upload_id:
        raise RuntimeError("S3 multipart upload did not return an upload id")

    retries = memory_budget(memory_limit_bytes).async_retries
    parts: list[dict[str, Any]] = []

    async def upload_part(index: int) -> dict[str, Any]:
        """Read and upload one immutable local-file part."""
        payload = read_upload_part(local_path, index, tuning.part_bytes, tuning.file_size)
        part_number = index + 1

        async def operation() -> Any:
            """Send one idempotent part request with bounded retry."""
            return await client.upload_part(
                Bucket=ref.bucket,
                Key=ref.key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=payload,
            )

        response = await retry_async(
            operation,
            retries=retries,
            should_retry=_should_retry_s3_part,
        )
        etag = response.get("ETag") if isinstance(response, dict) else None
        if not isinstance(etag, str) or not etag:
            raise RuntimeError(f"S3 multipart part {part_number} did not return an ETag")
        return {"ETag": etag, "PartNumber": part_number}

    try:
        async for _index, completed in ordered_indexed_results(
            tuning.part_count,
            upload_part,
            window=tuning.concurrency,
        ):
            parts.append(completed)
        final_stat = source.stat()
        if (final_stat.st_size, final_stat.st_mtime_ns) != (
            initial_stat.st_size,
            initial_stat.st_mtime_ns,
        ):
            raise OSError("remote upload spool changed before S3 multipart commit")
        await client.complete_multipart_upload(
            Bucket=ref.bucket,
            Key=ref.key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except BaseException as exc:
        try:
            await client.abort_multipart_upload(
                Bucket=ref.bucket,
                Key=ref.key,
                UploadId=upload_id,
            )
        except BaseException as abort_exc:
            exc.add_note(f"S3 multipart abort also failed: {abort_exc!r}")
        raise


async def upload_file(
    local_path: str,
    uri: str,
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> None:
    """Upload a local file to S3 with a bounded multipart fast path."""
    ref = parse_uri(uri)
    tuning = remote_upload_policy(
        "s3",
        local_path,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )
    async with await open_client() as client:
        if tuning.multipart:
            await _upload_file_multipart(
                client,
                local_path,
                uri,
                memory_limit_bytes=memory_limit_bytes,
                threading_mode=threading_mode,
            )
            return
        with Path(local_path).open("rb") as file_handle:
            await client.put_object(Bucket=ref.bucket, Key=ref.key, Body=file_handle)


async def list_files(
    uri: str,
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> list[RemoteFile]:
    """List direct S3 child files under a URI prefix."""
    ref = parse_uri(uri)
    prefix = ref.key.rstrip("/") + "/"
    files: list[RemoteFile] = []
    async with await open_client() as client:
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {
                "Bucket": ref.bucket,
                "Prefix": prefix,
                "Delimiter": "/",
                "MaxKeys": 1000,
            }
            if token:
                kwargs["ContinuationToken"] = token
            payload = await client.list_objects_v2(**kwargs)
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
                raise RuntimeError(
                    f"S3 list for {uri!r} was truncated without a continuation token"
                )
    files.sort(key=lambda file: file.name)
    return files


async def directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> DirectoryDiscovery[RemoteFile]:
    """Return whether S3 directories contain a direct child matching suffixes."""
    accepted = normalize_extensions(suffixes)
    discovery = DirectoryDiscoveryBuilder[RemoteFile].from_uris(uris)
    groups: dict[tuple[str, str], dict[str, list[str]]] = {}
    for uri in uris:
        ref = parse_uri(uri)
        parsed = split_parent_child(ref.key)
        if parsed is None:
            continue
        parent_prefix, child = parsed
        bucket = ref.bucket
        groups.setdefault((bucket, parent_prefix), {}).setdefault(child, []).append(uri)

    if not groups:
        return discovery.finish()

    budget = memory_budget(memory_limit_bytes)
    concurrency = execution_policy(threading_mode, memory_limit_bytes).source_discovery_concurrency
    retries = budget.async_retries
    semaphore = asyncio.Semaphore(concurrency)

    async def scan_group(bucket: str, parent_prefix: str, children: dict[str, list[str]]) -> None:
        """Scan one S3 parent prefix and mark matching child directories."""
        prefix = f"{parent_prefix.rstrip('/')}/" if parent_prefix else ""
        async with await open_client() as client:
            token: str | None = None
            while True:
                kwargs: dict[str, Any] = {
                    "Bucket": bucket,
                    "Prefix": prefix,
                    "MaxKeys": 1000,
                }
                if token:
                    kwargs["ContinuationToken"] = token

                async def request_page() -> dict[str, Any]:
                    """Fetch one S3 list page with retryable transient errors."""
                    async with semaphore:
                        return await client.list_objects_v2(**kwargs)

                payload = await retry_async(request_page, retries=retries)
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
                    remote_file = RemoteFile(f"s3://{bucket}/{key}", filename, size)
                    discovery.add(child_uris, remote_file)
                if not payload.get("IsTruncated"):
                    break
                token = payload.get("NextContinuationToken")
                if not isinstance(token, str) or not token:
                    raise RuntimeError(
                        f"S3 bulk source discovery for {parent_prefix!r} was truncated "
                        "without a continuation token"
                    )

    grouped = list(groups.items())

    async def scan_index(index: int) -> None:
        """Scan one canonically ordered S3 parent group."""
        (bucket, parent), children = grouped[index]
        await scan_group(bucket, parent, children)

    await drain_ordered_indexed_results(
        len(grouped),
        scan_index,
        window=concurrency,
    )
    return discovery.finish()
