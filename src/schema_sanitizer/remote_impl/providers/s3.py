"""Amazon S3 URI, discovery, and object operations."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ...core_impl.async_scheduler import read_int_env, retry_async
from ...core_impl.uris import name_matches, normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    RemoteFile,
    split_parent_child,
)
from ..transport import TRANSFER_CHUNK_BYTES


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


async def open_client() -> Any:
    """Open an aiobotocore S3 client."""
    aiobotocore = import_module("aiobotocore.session")

    session = aiobotocore.get_session()
    kwargs: dict[str, Any] = {}
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if region:
        kwargs["region_name"] = region
    return session.create_client("s3", **kwargs)


async def download_bytes(client: Any, file: RemoteFile) -> bytes:
    """Download one S3 object into bytes using a shared client."""
    ref = parse_uri(file.uri)
    response = await client.get_object(Bucket=ref.bucket, Key=ref.key)
    async with response["Body"] as body:
        return await body.read()


async def file_exists(uri: str) -> bool:
    """Return whether one S3 object exists."""
    ref = parse_uri(uri)
    async with await open_client() as client:
        try:
            await client.head_object(Bucket=ref.bucket, Key=ref.key)
            return True
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
                return False
            if status in {401, 403} or code in {"403", "AccessDenied"}:
                raise PermissionError(
                    f"S3 returned a permission error while checking source object: {uri!r}"
                ) from exc
            raise


async def download_file(uri: str, local_path: str) -> None:
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
    async with response["Body"] as body:
        with Path(local_path).open("wb") as file_handle:
            while chunk := await body.read(TRANSFER_CHUNK_BYTES):
                file_handle.write(chunk)


async def upload_file(local_path: str, uri: str) -> None:
    """Upload a local file to S3."""
    ref = parse_uri(uri)
    async with await open_client() as client:
        with Path(local_path).open("rb") as file_handle:
            await client.put_object(Bucket=ref.bucket, Key=ref.key, Body=file_handle)


async def list_files(uri: str, suffixes: tuple[str, ...]) -> list[RemoteFile]:
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

    concurrency = read_int_env("SCHEMA_SANITIZER_SOURCE_DISCOVERY_S3_BULK_CONCURRENCY", 16)
    retries = read_int_env(
        "SCHEMA_SANITIZER_SOURCE_DISCOVERY_S3_RETRIES",
        read_int_env("SCHEMA_SANITIZER_ASYNC_RETRIES", 4),
    )
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

    await asyncio.gather(
        *(scan_group(bucket, parent, children) for (bucket, parent), children in groups.items())
    )
    return discovery.finish()
