"""Async remote object I/O and local staging for public file APIs."""

from __future__ import annotations

import asyncio
import json
import mimetypes
import os
import shutil
import tempfile
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

from ..core_impl.path_uris import (
    local_path_from_file_uri,
    looks_like_file_uri,
)
from ..errors import SchemaSanitizerResourceError
from .async_remote_scheduler import (
    directory_download_tuning,
    drain_ordered_indexed_results,
    read_float_env,
    read_int_env,
    retry_async,
)

FOLDER_READ_CHUNK_BYTES = 1024 * 1024
_GCS_JSON_API_ENDPOINT = "https://storage.googleapis.com"
_GCS_READ_ONLY_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"
_RETRYABLE_HTTP_STATUSES = {408, 429, 500, 502, 503, 504}
_REMOTE_SCHEMES = {
    "abfs",
    "abfss",
    "adl",
    "az",
    "azure",
    "gcs",
    "gs",
    "http",
    "https",
    "s3",
    "wasb",
    "wasbs",
}


@dataclass(frozen=True, slots=True)
class RemoteFile:
    """One direct remote child object selected for directory staging."""

    uri: str
    name: str
    size: int | None = None


class RemoteDirectoryDiscovery(dict[str, bool]):
    """Directory existence map plus exact child files discovered while listing."""

    def __init__(
        self,
        values: dict[str, bool],
        *,
        files_by_uri: dict[str, list[RemoteFile]] | None = None,
    ):
        """Store bool compatibility values and optional child listings."""
        super().__init__(values)
        self.files_by_uri = files_by_uri or {}


class StagedPath:
    """Own a local temporary path that mirrors a remote input or output."""

    def __init__(
        self,
        path: str,
        *,
        is_dir: bool = False,
        source_file_by_name: dict[str, str] | None = None,
    ):
        """Store the temporary path and deletion mode."""
        self.path = path
        self.is_dir = is_dir
        self.source_file_by_name = source_file_by_name
        self._closed = False

    def close(self) -> None:
        """Delete the temporary path."""
        if self._closed:
            return
        self._closed = True
        if self.is_dir:
            shutil.rmtree(self.path, ignore_errors=True)
            return
        try:
            Path(self.path).unlink(missing_ok=True)
        except OSError:
            pass


@dataclass(slots=True)
class RemoteOutputTarget:
    """Local output target plus optional remote upload destination."""

    local_path: str
    remote_uri: str | None = None
    temp: StagedPath | None = None

    def close(self) -> None:
        """Release the temporary output path."""
        if self.temp is not None:
            self.temp.close()
            self.temp = None


def looks_like_remote_uri(value: Any) -> bool:
    """Return whether a value is a supported non-local remote URI."""
    if not isinstance(value, str):
        return False
    parsed = urlparse(value)
    return bool(parsed.scheme and parsed.scheme.lower() in _REMOTE_SCHEMES and parsed.netloc)


def _run_async(coro: Any) -> Any:
    """Run a coroutine from sync API code, even if another loop is active."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="schema-sanitizer-async") as pool:
        return pool.submit(lambda: asyncio.run(coro)).result()


def _spool_dir() -> str | None:
    """Return the configured local staging directory, if any."""
    return os.getenv("SCHEMA_SANITIZER_SPOOL_DIR") or None


def _temp_file_path(*, suffix: str) -> StagedPath:
    """Create an owned temporary file path."""
    fd, path = tempfile.mkstemp(prefix="schema-sanitizer-", suffix=suffix, dir=_spool_dir())
    os.close(fd)
    return StagedPath(path)


def _temp_dir_path() -> StagedPath:
    """Create an owned temporary directory path."""
    path = tempfile.mkdtemp(prefix="schema-sanitizer-", dir=_spool_dir())
    return StagedPath(path, is_dir=True)


def _suffix_from_uri(uri: str, *, default: str = "") -> str:
    """Return a suffix suitable for staging one URI."""
    suffix = Path(urlparse(uri).path).suffix
    return suffix or default


def _content_type(uri: str) -> str:
    """Return a best-effort content type for uploads."""
    guessed, _ = mimetypes.guess_type(uri)
    return guessed or "application/octet-stream"


def _check_download_size(uri: str, size: int | None, memory_limit_bytes: int | None) -> None:
    """Reject one downloaded object if it crosses the configured limit."""
    if memory_limit_bytes is None or memory_limit_bytes <= 0:
        return
    if size is None or size <= memory_limit_bytes:
        return
    raise SchemaSanitizerResourceError(
        f"memory_limit_bytes limit exceeded during remote_download: "
        f"{size} bytes > {memory_limit_bytes} bytes; file: {uri}",
        detail={
            "stage": "remote_download",
            "limit_name": "memory_limit_bytes",
            "limit_bytes": memory_limit_bytes,
            "actual_bytes": size,
            "file": uri,
        },
    )


def _normalize_extensions(suffixes: Sequence[str]) -> tuple[str, ...]:
    """Normalize suffixes to lowercase values with leading dots."""
    out = []
    for suffix in suffixes:
        value = suffix.lower()
        out.append(value if value.startswith(".") else f".{value}")
    return tuple(out)


def _name_matches(name: str, suffixes: tuple[str, ...]) -> bool:
    """Return whether a child name matches accepted suffixes."""
    return name.lower().endswith(suffixes)


@dataclass(frozen=True, slots=True)
class _GcsRef:
    """Parsed GCS object reference."""

    bucket: str
    object_name: str


def _parse_gcs_uri(uri: str) -> _GcsRef:
    """Parse a gs:// or gcs:// URI."""
    parsed = urlparse(uri)
    if parsed.scheme.lower() not in {"gs", "gcs"} or not parsed.netloc:
        raise ValueError(f"not a GCS URI: {uri!r}")
    return _GcsRef(parsed.netloc, parsed.path.lstrip("/"))


def _gcs_object_uri(bucket: str, object_name: str) -> str:
    """Render a GCS object URI."""
    return f"gs://{bucket}/{object_name}"


def _gcs_token() -> str:
    """Return a Google ADC token for object I/O."""
    import google.auth
    from google.auth.transport.requests import Request

    credentials, _ = google.auth.default(scopes=[_GCS_READ_ONLY_SCOPE])
    if not credentials.valid:
        credentials.refresh(Request())
    if not credentials.token:
        raise RuntimeError("Google ADC did not return an access token")
    return credentials.token


def _gcs_base() -> str:
    """Return the GCS JSON API endpoint."""
    return os.getenv("GCS_JSON_API_ENDPOINT", _GCS_JSON_API_ENDPOINT).rstrip("/")


async def _response_bytes(response: Any, *, uri: str) -> bytes:
    """Read an aiohttp response or raise with context."""
    if response.status in {200, 201}:
        return await response.read()
    body = await response.text()
    raise RuntimeError(f"HTTP {response.status} for {uri}: {body[:1000]!r}")


async def _aiohttp_session(headers: dict[str, str] | None = None) -> Any:
    """Open an aiohttp session with tuned defaults."""
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=read_float_env("SCHEMA_SANITIZER_ASYNC_TIMEOUT", 120.0))
    concurrency = read_int_env("SCHEMA_SANITIZER_ASYNC_CONCURRENCY", 64)
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        ttl_dns_cache=300,
    )
    return aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers)


async def _gcs_list(uri: str, suffixes: tuple[str, ...]) -> list[RemoteFile]:
    """List direct GCS child files under a URI prefix."""
    ref = _parse_gcs_uri(uri)
    token = _gcs_token()
    url = f"{_gcs_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o"
    prefix = ref.object_name.rstrip("/") + "/"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    requester_pays = os.getenv("GCS_REQUESTER_PAYS_PROJECT") or None
    files: list[RemoteFile] = []
    async with await _aiohttp_session(headers) as session:
        page_token: str | None = None
        while True:
            params = {
                "prefix": prefix,
                "delimiter": "/",
                "fields": "nextPageToken,items(name,size)",
                "maxResults": "1000",
            }
            if page_token:
                params["pageToken"] = page_token
            if requester_pays:
                params["userProject"] = requester_pays
            async with session.get(url, params=params) as response:
                body = await response.text()
                if response.status != 200:
                    raise RuntimeError(
                        f"GCS list failed for {uri!r}: {response.status} {body[:1000]!r}"
                    )
                payload = json.loads(body)
            for item in payload.get("items", ()):
                name = item.get("name")
                if not isinstance(name, str):
                    continue
                relative = name[len(prefix) :] if name.startswith(prefix) else name
                if not relative or "/" in relative or not _name_matches(relative, suffixes):
                    continue
                size_raw = item.get("size")
                size = int(size_raw) if isinstance(size_raw, str) and size_raw.isdigit() else None
                files.append(RemoteFile(_gcs_object_uri(ref.bucket, name), relative, size))
            page_token = payload.get("nextPageToken")
            if not isinstance(page_token, str) or not page_token:
                break
    files.sort(key=lambda file: file.name)
    return files


def _gcs_parent_child(uri: str) -> tuple[str, str, str] | None:
    """Return bucket, parent object prefix, and child segment for one GCS directory URI."""
    ref = _parse_gcs_uri(uri)
    object_name = ref.object_name.rstrip("/")
    if not object_name:
        return None
    parent, _sep, child = object_name.rpartition("/")
    if not child:
        return None
    return ref.bucket, parent, child


async def _gcs_directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
) -> dict[str, bool]:
    """Return whether GCS directories contain a direct child matching suffixes.

    Discovery can generate hundreds of adjacent Hive partition directories. Listing each
    directory independently creates many short-lived HTTPS connections and is prone to
    timeout/broken-pipe failures. This groups directories by immediate parent prefix and
    scans each parent once.
    """
    accepted = _normalize_extensions(suffixes)
    out = {uri: False for uri in uris}
    files_by_uri = {uri: [] for uri in uris}
    groups: dict[tuple[str, str], dict[str, list[str]]] = {}
    for uri in uris:
        parsed = _gcs_parent_child(uri)
        if parsed is None:
            continue
        bucket, parent_prefix, child = parsed
        groups.setdefault((bucket, parent_prefix), {}).setdefault(child, []).append(uri)

    if not groups:
        return out

    token = _gcs_token()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    requester_pays = os.getenv("GCS_REQUESTER_PAYS_PROJECT") or None
    concurrency = read_int_env("SCHEMA_SANITIZER_SOURCE_DISCOVERY_GCS_BULK_CONCURRENCY", 16)
    retries = read_int_env(
        "SCHEMA_SANITIZER_SOURCE_DISCOVERY_GCS_RETRIES",
        read_int_env("SCHEMA_SANITIZER_ASYNC_RETRIES", 4),
    )
    semaphore = asyncio.Semaphore(concurrency)

    async with await _aiohttp_session(headers) as session:

        async def scan_group(
            bucket: str, parent_prefix: str, children: dict[str, list[str]]
        ) -> None:
            """Scan one parent prefix and mark matching requested child directories."""
            url = f"{_gcs_base()}/storage/v1/b/{quote(bucket, safe='')}/o"
            prefix = f"{parent_prefix.rstrip('/')}/" if parent_prefix else ""
            requested_children = set(children)
            page_token: str | None = None

            while True:
                params = {
                    "prefix": prefix,
                    "fields": "nextPageToken,items(name,size)",
                    "maxResults": "1000",
                }
                if page_token:
                    params["pageToken"] = page_token
                if requester_pays:
                    params["userProject"] = requester_pays

                async def request_page() -> dict[str, Any]:
                    """Fetch one GCS list page with retryable transient errors."""
                    async with semaphore:
                        async with session.get(url, params=params) as response:
                            body = await response.text()
                            if response.status == 200:
                                return json.loads(body)
                            if response.status in {401, 403}:
                                raise PermissionError(
                                    "GCS returned a permission error while bulk-listing "
                                    f"source directories. status={response.status}, "
                                    f"prefix={prefix!r}, body={body[:1000]!r}"
                                )
                            raise RuntimeError(
                                "GCS bulk source discovery list failed. "
                                f"status={response.status}, prefix={prefix!r}, "
                                f"body={body[:1000]!r}"
                            )

                payload = await retry_async(request_page, retries=retries)
                for item in payload.get("items", ()):
                    name = item.get("name")
                    if not isinstance(name, str) or not name.startswith(prefix):
                        continue
                    relative = name[len(prefix) :]
                    child, sep, filename = relative.partition("/")
                    if not sep or child not in requested_children:
                        continue
                    if "/" in filename or not _name_matches(filename, accepted):
                        continue
                    size_raw = item.get("size")
                    size = (
                        int(size_raw) if isinstance(size_raw, str) and size_raw.isdigit() else None
                    )
                    remote_file = RemoteFile(_gcs_object_uri(bucket, name), filename, size)
                    for uri in children[child]:
                        out[uri] = True
                        files_by_uri[uri].append(remote_file)

                page_token = payload.get("nextPageToken")
                if not isinstance(page_token, str) or not page_token:
                    break

        await asyncio.gather(
            *(scan_group(bucket, parent, children) for (bucket, parent), children in groups.items())
        )

    for files in files_by_uri.values():
        files.sort(key=lambda file: file.name)
    return RemoteDirectoryDiscovery(out, files_by_uri=files_by_uri)


async def _gcs_download_bytes(session: Any, file: RemoteFile) -> bytes:
    """Download one GCS object into bytes."""
    async with session.get(_gcs_media_url(file.uri)) as response:
        return await _response_bytes(response, uri=file.uri)


async def _gcs_file_exists(uri: str) -> bool:
    """Return whether one GCS object exists."""
    ref = _parse_gcs_uri(uri)
    token = _gcs_token()
    url = (
        f"{_gcs_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o/"
        f"{quote(ref.object_name, safe='')}"
    )
    params = {"fields": "name"}
    requester_pays = os.getenv("GCS_REQUESTER_PAYS_PROJECT") or None
    if requester_pays:
        params["userProject"] = requester_pays
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    async with await _aiohttp_session(headers) as session:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                await response.read()
                return True
            if response.status == 404:
                await response.read()
                return False
            body = await response.text()
            if response.status in {401, 403}:
                raise PermissionError(
                    "GCS returned a permission error while checking source object. "
                    f"status={response.status}, uri={uri!r}, body={body[:1000]!r}"
                )
            raise RuntimeError(
                "Unexpected GCS response while checking source object. "
                f"status={response.status}, uri={uri!r}, body={body[:1000]!r}"
            )


def _gcs_media_url(uri: str) -> str:
    """Return the GCS JSON API media-download URL for one object."""
    ref = _parse_gcs_uri(uri)
    url = (
        f"{_gcs_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o/"
        f"{quote(ref.object_name, safe='')}?alt=media"
    )
    requester_pays = os.getenv("GCS_REQUESTER_PAYS_PROJECT") or None
    if requester_pays:
        url += f"&userProject={quote(requester_pays, safe='')}"
    return url


async def _write_aiohttp_response_to_file(response: Any, *, uri: str, local_path: str) -> None:
    """Stream a successful aiohttp response body to a local file."""
    if response.status != 200:
        body = await response.text()
        raise RuntimeError(f"HTTP download failed for {uri!r}: {response.status} {body[:1000]!r}")
    with Path(local_path).open("wb") as f:
        async for chunk in response.content.iter_chunked(FOLDER_READ_CHUNK_BYTES):
            f.write(chunk)


async def _gcs_download_file_with_session(session: Any, file: RemoteFile, local_path: str) -> None:
    """Download one GCS object to a local file using a shared session."""
    async with session.get(_gcs_media_url(file.uri)) as response:
        await _write_aiohttp_response_to_file(response, uri=file.uri, local_path=local_path)


async def _gcs_download_file(uri: str, local_path: str) -> None:
    """Download one GCS object to a local file."""
    token = _gcs_token()
    headers = {"Authorization": f"Bearer {token}"}
    async with await _aiohttp_session(headers) as session:
        await _gcs_download_file_with_session(
            session,
            RemoteFile(uri, Path(urlparse(uri).path).name),
            local_path,
        )


async def _gcs_upload_file(local_path: str, uri: str) -> None:
    """Upload a local file to GCS."""
    ref = _parse_gcs_uri(uri)
    token = _gcs_token()
    url = f"{_gcs_base()}/upload/storage/v1/b/{quote(ref.bucket, safe='')}/o"
    params = {"uploadType": "media", "name": ref.object_name}
    requester_pays = os.getenv("GCS_REQUESTER_PAYS_PROJECT") or None
    if requester_pays:
        params["userProject"] = requester_pays
    headers = {"Authorization": f"Bearer {token}", "Content-Type": _content_type(uri)}
    async with await _aiohttp_session(headers) as session:
        with Path(local_path).open("rb") as f:
            async with session.post(url, params=params, data=f) as response:
                await _response_bytes(response, uri=uri)


@dataclass(frozen=True, slots=True)
class _S3Ref:
    """Parsed S3 object reference."""

    bucket: str
    key: str


def _parse_s3_uri(uri: str) -> _S3Ref:
    """Parse an s3://bucket/key URI."""
    parsed = urlparse(uri)
    if parsed.scheme.lower() != "s3" or not parsed.netloc:
        raise ValueError(f"not an S3 URI: {uri!r}")
    return _S3Ref(parsed.netloc, parsed.path.lstrip("/"))


async def _s3_client() -> Any:
    """Open an aiobotocore S3 client."""
    from aiobotocore.session import get_session

    session = get_session()
    kwargs: dict[str, Any] = {}
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if region:
        kwargs["region_name"] = region
    return session.create_client("s3", **kwargs)


async def _s3_list(uri: str, suffixes: tuple[str, ...]) -> list[RemoteFile]:
    """List direct S3 child files under a URI prefix."""
    ref = _parse_s3_uri(uri)
    prefix = ref.key.rstrip("/") + "/"
    files: list[RemoteFile] = []
    async with await _s3_client() as client:
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
                if not relative or "/" in relative or not _name_matches(relative, suffixes):
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


def _s3_parent_child(uri: str) -> tuple[str, str, str] | None:
    """Return bucket, parent key prefix, and child segment for one S3 directory URI."""
    ref = _parse_s3_uri(uri)
    key = ref.key.rstrip("/")
    if not key:
        return None
    parent, _sep, child = key.rpartition("/")
    if not child:
        return None
    return ref.bucket, parent, child


async def _s3_directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
) -> dict[str, bool]:
    """Return whether S3 directories contain a direct child matching suffixes."""
    accepted = _normalize_extensions(suffixes)
    out = {uri: False for uri in uris}
    files_by_uri = {uri: [] for uri in uris}
    groups: dict[tuple[str, str], dict[str, list[str]]] = {}
    for uri in uris:
        parsed = _s3_parent_child(uri)
        if parsed is None:
            continue
        bucket, parent_prefix, child = parsed
        groups.setdefault((bucket, parent_prefix), {}).setdefault(child, []).append(uri)

    if not groups:
        return out

    concurrency = read_int_env("SCHEMA_SANITIZER_SOURCE_DISCOVERY_S3_BULK_CONCURRENCY", 16)
    retries = read_int_env(
        "SCHEMA_SANITIZER_SOURCE_DISCOVERY_S3_RETRIES",
        read_int_env("SCHEMA_SANITIZER_ASYNC_RETRIES", 4),
    )
    semaphore = asyncio.Semaphore(concurrency)

    async def scan_group(bucket: str, parent_prefix: str, children: dict[str, list[str]]) -> None:
        """Scan one S3 parent prefix and mark matching requested child directories."""
        prefix = f"{parent_prefix.rstrip('/')}/" if parent_prefix else ""
        requested_children = set(children)
        async with await _s3_client() as client:
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
                    child, sep, filename = relative.partition("/")
                    if not sep or child not in requested_children:
                        continue
                    if "/" in filename or not _name_matches(filename, accepted):
                        continue
                    size = item.get("Size") if isinstance(item.get("Size"), int) else None
                    remote_file = RemoteFile(f"s3://{bucket}/{key}", filename, size)
                    for uri in children[child]:
                        out[uri] = True
                        files_by_uri[uri].append(remote_file)
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
    for files in files_by_uri.values():
        files.sort(key=lambda file: file.name)
    return RemoteDirectoryDiscovery(out, files_by_uri=files_by_uri)


async def _s3_download_bytes(client: Any, file: RemoteFile) -> bytes:
    """Download one S3 object into bytes."""
    ref = _parse_s3_uri(file.uri)
    response = await client.get_object(Bucket=ref.bucket, Key=ref.key)
    async with response["Body"] as body:
        return await body.read()


async def _s3_file_exists(uri: str) -> bool:
    """Return whether one S3 object exists."""
    ref = _parse_s3_uri(uri)
    async with await _s3_client() as client:
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


async def _s3_download_file(uri: str, local_path: str) -> None:
    """Download one S3 object to a local file."""
    async with await _s3_client() as client:
        await _s3_download_file_with_client(
            client,
            RemoteFile(uri, Path(urlparse(uri).path).name),
            local_path,
        )


async def _s3_download_file_with_client(client: Any, file: RemoteFile, local_path: str) -> None:
    """Download one S3 object to a local file using a shared client."""
    ref = _parse_s3_uri(file.uri)
    response = await client.get_object(Bucket=ref.bucket, Key=ref.key)
    async with response["Body"] as body:
        with Path(local_path).open("wb") as f:
            while True:
                chunk = await body.read(FOLDER_READ_CHUNK_BYTES)
                if not chunk:
                    break
                f.write(chunk)


async def _s3_upload_file(local_path: str, uri: str) -> None:
    """Upload a local file to S3."""
    ref = _parse_s3_uri(uri)
    async with await _s3_client() as client:
        with Path(local_path).open("rb") as f:
            await client.put_object(Bucket=ref.bucket, Key=ref.key, Body=f)


@dataclass(frozen=True, slots=True)
class _AzureRef:
    """Parsed Azure Blob object reference."""

    account_url: str
    container: str
    blob: str
    original_uri: str


def _parse_azure_uri(uri: str) -> _AzureRef:
    """Parse common Azure Blob/ADLS URI forms."""
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()
    if scheme in {"http", "https"} and ".blob.core.windows.net" in parsed.netloc:
        parts = parsed.path.lstrip("/").split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Azure Blob URL must include container and blob: {uri!r}")
        return _AzureRef(f"{parsed.scheme}://{parsed.netloc}", parts[0], parts[1], uri)
    if scheme in {"abfs", "abfss", "wasb", "wasbs"}:
        container, _, account_host = parsed.netloc.partition("@")
        if not container or not account_host:
            raise ValueError(f"Azure URI must be container@account: {uri!r}")
        account = account_host.split(".", 1)[0]
        blob = parsed.path.lstrip("/")
        return _AzureRef(f"https://{account}.blob.core.windows.net", container, blob, uri)
    if scheme in {"az", "azure"}:
        parts = parsed.path.lstrip("/").split("/", 1)
        if not parsed.netloc or len(parts) != 2:
            raise ValueError(f"Azure URI must be azure://account/container/blob: {uri!r}")
        return _AzureRef(
            f"https://{parsed.netloc}.blob.core.windows.net",
            parts[0],
            parts[1],
            uri,
        )
    raise ValueError(f"not an Azure Blob URI: {uri!r}")


async def _azure_service(ref: _AzureRef) -> Any:
    """Open an async Azure Blob service client using default credentials."""
    from azure.identity.aio import DefaultAzureCredential
    from azure.storage.blob.aio import BlobServiceClient

    credential = DefaultAzureCredential()
    return BlobServiceClient(account_url=ref.account_url, credential=credential)


async def _azure_list(uri: str, suffixes: tuple[str, ...]) -> list[RemoteFile]:
    """List direct Azure Blob child files under a URI prefix."""
    ref = _parse_azure_uri(uri)
    prefix = ref.blob.rstrip("/") + "/"
    files: list[RemoteFile] = []
    service = await _azure_service(ref)
    try:
        container = service.get_container_client(ref.container)
        async for blob in container.walk_blobs(name_starts_with=prefix, delimiter="/"):
            name = getattr(blob, "name", None)
            if not isinstance(name, str):
                continue
            relative = name[len(prefix) :] if name.startswith(prefix) else name
            if not relative or "/" in relative or not _name_matches(relative, suffixes):
                continue
            size = getattr(blob, "size", None)
            files.append(
                RemoteFile(
                    _azure_render_uri(ref, name), relative, size if isinstance(size, int) else None
                )
            )
    finally:
        await service.close()
    files.sort(key=lambda file: file.name)
    return files


def _azure_parent_child(uri: str) -> tuple[str, str, str, str] | None:
    """Return account URL, container, parent blob prefix, and child segment."""
    ref = _parse_azure_uri(uri)
    blob = ref.blob.rstrip("/")
    if not blob:
        return None
    parent, _sep, child = blob.rpartition("/")
    if not child:
        return None
    return ref.account_url, ref.container, parent, child


async def _azure_directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
) -> dict[str, bool]:
    """Return whether Azure directories contain a direct child matching suffixes."""
    accepted = _normalize_extensions(suffixes)
    out = {uri: False for uri in uris}
    files_by_uri = {uri: [] for uri in uris}
    groups: dict[tuple[str, str, str], dict[str, list[str]]] = {}
    for uri in uris:
        parsed = _azure_parent_child(uri)
        if parsed is None:
            continue
        account_url, container, parent_prefix, child = parsed
        groups.setdefault((account_url, container, parent_prefix), {}).setdefault(child, []).append(
            uri
        )

    if not groups:
        return out

    concurrency = read_int_env("SCHEMA_SANITIZER_SOURCE_DISCOVERY_AZURE_BULK_CONCURRENCY", 16)
    semaphore = asyncio.Semaphore(concurrency)

    async def scan_group(
        account_url: str,
        container_name: str,
        parent_prefix: str,
        children: dict[str, list[str]],
    ) -> None:
        """Scan one Azure parent prefix and mark matching requested child directories."""
        prefix = f"{parent_prefix.rstrip('/')}/" if parent_prefix else ""
        requested_children = set(children)
        ref = _AzureRef(account_url, container_name, parent_prefix, "")
        service = await _azure_service(ref)
        try:
            container = service.get_container_client(container_name)
            async with semaphore:
                async for blob in container.list_blobs(name_starts_with=prefix):
                    name = getattr(blob, "name", None)
                    if not isinstance(name, str) or not name.startswith(prefix):
                        continue
                    relative = name[len(prefix) :]
                    child, sep, filename = relative.partition("/")
                    if not sep or child not in requested_children:
                        continue
                    if "/" in filename or not _name_matches(filename, accepted):
                        continue
                    size = getattr(blob, "size", None)
                    remote_file = RemoteFile(
                        _azure_render_uri(ref, name),
                        filename,
                        size if isinstance(size, int) else None,
                    )
                    for uri in children[child]:
                        out[uri] = True
                        files_by_uri[uri].append(remote_file)
        finally:
            await service.close()

    await asyncio.gather(
        *(
            scan_group(account_url, container, parent, children)
            for (account_url, container, parent), children in groups.items()
        )
    )
    for files in files_by_uri.values():
        files.sort(key=lambda file: file.name)
    return RemoteDirectoryDiscovery(out, files_by_uri=files_by_uri)


def _azure_render_uri(ref: _AzureRef, blob: str) -> str:
    """Render an Azure URI in HTTPS Blob form."""
    return f"{ref.account_url}/{ref.container}/{blob}"


async def _azure_download_file(uri: str, local_path: str) -> None:
    """Download one Azure Blob to a local file."""
    ref = _parse_azure_uri(uri)
    service = await _azure_service(ref)
    try:
        blob = service.get_blob_client(ref.container, ref.blob)
        stream = await blob.download_blob()
        with Path(local_path).open("wb") as f:
            async for chunk in stream.chunks():
                f.write(chunk)
    finally:
        await service.close()


async def _azure_file_exists(uri: str) -> bool:
    """Return whether one Azure Blob object exists."""
    ref = _parse_azure_uri(uri)
    service = await _azure_service(ref)
    try:
        blob = service.get_blob_client(ref.container, ref.blob)
        exists = await blob.exists()
        return bool(exists)
    finally:
        await service.close()


async def _azure_download_bytes(uri: str) -> bytes:
    """Download one Azure Blob into bytes."""
    ref = _parse_azure_uri(uri)
    service = await _azure_service(ref)
    try:
        blob = service.get_blob_client(ref.container, ref.blob)
        stream = await blob.download_blob()
        data = bytearray()
        async for chunk in stream.chunks():
            data.extend(chunk)
        return bytes(data)
    finally:
        await service.close()


async def _azure_upload_file(local_path: str, uri: str) -> None:
    """Upload a local file to Azure Blob storage."""
    ref = _parse_azure_uri(uri)
    service = await _azure_service(ref)
    try:
        blob = service.get_blob_client(ref.container, ref.blob)
        with Path(local_path).open("rb") as f:
            await blob.upload_blob(f, overwrite=True)
    finally:
        await service.close()


async def _http_download_file(uri: str, local_path: str) -> None:
    """Download one HTTP(S) object to a local file."""
    async with await _aiohttp_session() as session:
        async with session.get(uri) as response:
            await _write_aiohttp_response_to_file(response, uri=uri, local_path=local_path)


async def _http_file_exists(uri: str) -> bool:
    """Return whether one HTTP(S) object appears to exist."""
    async with await _aiohttp_session() as session:
        async with session.head(uri) as response:
            if response.status in {200, 204}:
                return True
            if response.status == 404:
                return False
            if response.status in {401, 403}:
                raise PermissionError(
                    f"HTTP returned a permission error while checking source object: {uri!r}"
                )
            raise RuntimeError(
                f"Unexpected HTTP response while checking source object: "
                f"status={response.status}, uri={uri!r}"
            )


async def _http_upload_file(local_path: str, uri: str) -> None:
    """Upload a local file to an HTTP(S) endpoint with PUT."""
    async with await _aiohttp_session({"Content-Type": _content_type(uri)}) as session:
        with Path(local_path).open("rb") as f:
            async with session.put(uri, data=f) as response:
                if response.status not in {200, 201, 202, 204}:
                    body = await response.text()
                    raise RuntimeError(
                        f"HTTP upload failed for {uri!r}: {response.status} {body[:1000]!r}"
                    )


async def _list_remote_directory(uri: str, suffixes: Sequence[str]) -> list[RemoteFile]:
    """List one supported remote directory non-recursively."""
    accepted = _normalize_extensions(suffixes)
    scheme = urlparse(uri).scheme.lower()
    if scheme in {"gs", "gcs"}:
        return await _gcs_list(uri, accepted)
    if scheme == "s3":
        return await _s3_list(uri, accepted)
    if scheme in {"abfs", "abfss", "adl", "az", "azure", "wasb", "wasbs"} or (
        scheme in {"http", "https"} and ".blob.core.windows.net" in urlparse(uri).netloc
    ):
        return await _azure_list(uri, accepted)
    if scheme in {"http", "https"}:
        raise ValueError("HTTP(S) directory listing is not portable; use single_file mode")
    raise ValueError(f"Unsupported remote directory URI scheme: {scheme!r}")


async def _remote_file_exists(uri: str) -> bool:
    """Return whether one supported remote object exists."""
    scheme = urlparse(uri).scheme.lower()
    if scheme in {"gs", "gcs"}:
        return await _gcs_file_exists(uri)
    if scheme == "s3":
        return await _s3_file_exists(uri)
    if scheme in {"abfs", "abfss", "adl", "az", "azure", "wasb", "wasbs"} or (
        scheme in {"http", "https"} and ".blob.core.windows.net" in urlparse(uri).netloc
    ):
        return await _azure_file_exists(uri)
    if scheme in {"http", "https"}:
        return await _http_file_exists(uri)
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


def remote_file_exists(uri: str) -> bool:
    """Sync wrapper around async remote object existence checks."""
    return bool(_run_async(_remote_file_exists(uri)))


def list_remote_directory_files(uri: str, suffixes: Sequence[str]) -> list[RemoteFile]:
    """Sync wrapper around async remote directory listing."""
    return list(_run_async(_list_remote_directory(uri, suffixes)))


def remote_directory_stage_chunk_size() -> int:
    """Return the maximum number of remote children staged for one native chunk."""
    return read_int_env(
        "SCHEMA_SANITIZER_REMOTE_STAGE_FILES",
        read_int_env("SCHEMA_SANITIZER_ASYNC_PREFETCH_FILES", 64),
    )


async def _download_one_file_bytes(session_or_client: Any, file: RemoteFile) -> bytes:
    """Download one remote file into bytes using the provider backend."""
    scheme = urlparse(file.uri).scheme.lower()
    if scheme in {"gs", "gcs"}:
        return await _gcs_download_bytes(session_or_client, file)
    if scheme == "s3":
        client = session_or_client[1] if isinstance(session_or_client, tuple) else session_or_client
        return await _s3_download_bytes(client, file)
    if scheme in {"abfs", "abfss", "adl", "az", "azure", "wasb", "wasbs"} or (
        scheme in {"http", "https"} and ".blob.core.windows.net" in urlparse(file.uri).netloc
    ):
        return await _azure_download_bytes(file.uri)
    if scheme in {"http", "https"}:
        async with session_or_client.get(file.uri) as response:
            return await _response_bytes(response, uri=file.uri)
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


async def _download_one_file_to_path(
    session_or_client: Any, file: RemoteFile, local_path: str
) -> None:
    """Download one remote file directly to a local path."""
    scheme = urlparse(file.uri).scheme.lower()
    if scheme in {"gs", "gcs"}:
        await _gcs_download_file_with_session(session_or_client, file, local_path)
        return
    if scheme == "s3":
        client = session_or_client[1] if isinstance(session_or_client, tuple) else session_or_client
        await _s3_download_file_with_client(client, file, local_path)
        return
    if scheme in {"abfs", "abfss", "adl", "az", "azure", "wasb", "wasbs"} or (
        scheme in {"http", "https"} and ".blob.core.windows.net" in urlparse(file.uri).netloc
    ):
        await _azure_download_file(file.uri, local_path)
        return
    if scheme in {"http", "https"}:
        async with session_or_client.get(file.uri) as response:
            await _write_aiohttp_response_to_file(response, uri=file.uri, local_path=local_path)
        return
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


async def _provider_client_for_downloads(files: Sequence[RemoteFile]) -> Any:
    """Open a provider-specific reusable client/session."""
    if not files:
        return None
    scheme = urlparse(files[0].uri).scheme.lower()
    if scheme in {"gs", "gcs"}:
        return await _aiohttp_session({"Authorization": f"Bearer {_gcs_token()}"})
    if scheme == "s3":
        cm = await _s3_client()
        client = await cm.__aenter__()
        return (cm, client)
    if scheme in {"http", "https"}:
        return await _aiohttp_session()
    return None


async def _close_provider_client(client: Any) -> None:
    """Close a reusable provider client/session."""
    if client is None:
        return
    if isinstance(client, tuple) and len(client) == 2:
        cm, _ = client
        await cm.__aexit__(None, None, None)
        return
    close = getattr(client, "close", None)
    if close is not None:
        result = close()
        if asyncio.iscoroutine(result):
            await result
    exit_fn = getattr(client, "__aexit__", None)
    if exit_fn is not None:
        await exit_fn(None, None, None)


async def _download_files_to_directory(
    files: list[RemoteFile],
    directory: str,
    *,
    memory_limit_bytes: int | None,
) -> None:
    """Download files concurrently into a local directory."""
    if not files:
        raise ValueError("remote directory input found no matching files")
    tuning = directory_download_tuning()
    semaphore = asyncio.Semaphore(tuning.concurrency)
    client = await _provider_client_for_downloads(files)

    async def fetch(index: int) -> None:
        """Download one indexed file into the target directory."""
        file = files[index]
        target = str(Path(directory, file.name))
        _check_download_size(file.uri, file.size, memory_limit_bytes)

        async def operation() -> None:
            """Download one file while respecting the concurrency semaphore."""
            async with semaphore:
                await _download_one_file_to_path(client, file, target)

        try:
            await retry_async(operation, retries=tuning.retries)
        except Exception:
            Path(target).unlink(missing_ok=True)
            raise
        size = Path(target).stat().st_size
        _check_download_size(file.uri, size, memory_limit_bytes)

    try:
        await drain_ordered_indexed_results(len(files), fetch, window=tuning.window)
    finally:
        await _close_provider_client(client)


async def _download_single_file(uri: str, local_path: str) -> None:
    """Download one supported remote URI to a local path."""
    scheme = urlparse(uri).scheme.lower()
    if scheme in {"gs", "gcs"}:
        await _gcs_download_file(uri, local_path)
        return
    if scheme == "s3":
        await _s3_download_file(uri, local_path)
        return
    if scheme in {"abfs", "abfss", "adl", "az", "azure", "wasb", "wasbs"} or (
        scheme in {"http", "https"} and ".blob.core.windows.net" in urlparse(uri).netloc
    ):
        await _azure_download_file(uri, local_path)
        return
    if scheme in {"http", "https"}:
        await _http_download_file(uri, local_path)
        return
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


async def _upload_file(local_path: str, uri: str) -> None:
    """Upload one local file to a supported remote URI."""
    scheme = urlparse(uri).scheme.lower()
    if scheme in {"gs", "gcs"}:
        await _gcs_upload_file(local_path, uri)
        return
    if scheme == "s3":
        await _s3_upload_file(local_path, uri)
        return
    if scheme in {"abfs", "abfss", "adl", "az", "azure", "wasb", "wasbs"} or (
        scheme in {"http", "https"} and ".blob.core.windows.net" in urlparse(uri).netloc
    ):
        await _azure_upload_file(local_path, uri)
        return
    if scheme in {"http", "https"}:
        await _http_upload_file(local_path, uri)
        return
    raise ValueError(f"Unsupported remote URI scheme: {scheme!r}")


def stage_remote_single_file(
    uri: str,
    *,
    memory_limit_bytes: int | None,
) -> StagedPath:
    """Download one remote file to a local temporary path."""
    temp = _temp_file_path(suffix=_suffix_from_uri(uri))
    try:
        _run_async(_download_single_file(uri, temp.path))
        size = Path(temp.path).stat().st_size
        if memory_limit_bytes is not None and memory_limit_bytes > 0:
            _check_download_size(uri, size, memory_limit_bytes)
    except Exception:
        temp.close()
        raise
    return temp


def stage_remote_files_to_directory(
    files: Sequence[RemoteFile],
    *,
    memory_limit_bytes: int | None,
) -> StagedPath:
    """Download selected remote files into one temporary directory for native ingestion."""
    selected = list(files)
    if not selected:
        raise ValueError("remote directory input found no matching files")
    temp_dir = _temp_dir_path()
    try:
        _run_async(
            _download_files_to_directory(
                selected,
                temp_dir.path,
                memory_limit_bytes=memory_limit_bytes,
            )
        )
    except Exception:
        temp_dir.close()
        raise
    temp_dir.source_file_by_name = {file.name: file.uri for file in selected}
    return temp_dir


def stage_remote_parquet_directory(
    uri: str,
    *,
    suffixes: Sequence[str],
    memory_limit_bytes: int | None,
) -> StagedPath:
    """Download a remote Parquet directory to a local temporary directory."""
    files = _run_async(_list_remote_directory(uri, suffixes))
    if not files:
        expected = " or ".join(_normalize_extensions(suffixes))
        raise ValueError(f"parquet remote directory input found no {expected} files in: {uri}")
    temp_dir = _temp_dir_path()
    try:
        _run_async(
            _download_files_to_directory(
                files,
                temp_dir.path,
                memory_limit_bytes=memory_limit_bytes,
            )
        )
    except Exception:
        temp_dir.close()
        raise
    temp_dir.source_file_by_name = {file.name: file.uri for file in files}
    return temp_dir


def prepare_output_target(path: Any) -> RemoteOutputTarget:
    """Return a local output target, staging remote destinations when needed."""
    raw = os.fspath(path)
    if looks_like_file_uri(raw):
        return RemoteOutputTarget(local_path=local_path_from_file_uri(raw))
    if not looks_like_remote_uri(raw):
        return RemoteOutputTarget(local_path=raw)
    temp = _temp_file_path(suffix=_suffix_from_uri(raw, default=".tmp"))
    return RemoteOutputTarget(local_path=temp.path, remote_uri=raw, temp=temp)


def finalize_output_target(target: RemoteOutputTarget) -> None:
    """Upload a staged output target if it points to a remote URI."""
    try:
        if target.remote_uri is not None:
            _run_async(_upload_file(target.local_path, target.remote_uri))
    finally:
        target.close()


def cleanup_output_target(target: RemoteOutputTarget) -> None:
    """Release a staged output target after a failed write."""
    target.close()


def remote_io_environment_notes() -> dict[str, str]:
    """Return environment variables used by async remote I/O."""
    return {
        "SCHEMA_SANITIZER_ASYNC_CONCURRENCY": "Maximum concurrent remote downloads/uploads.",
        "SCHEMA_SANITIZER_ASYNC_PREFETCH_FILES": "Maximum scheduled file downloads per directory.",
        "SCHEMA_SANITIZER_REMOTE_CHUNK_PREFETCH_CHUNKS": (
            "Maximum staged remote directory chunks kept ahead of native processing."
        ),
        "SCHEMA_SANITIZER_REMOTE_RETAINED_STAGE_CHUNKS": (
            "Maximum remote chunks retained between schema probing and normal output."
        ),
        "SCHEMA_SANITIZER_ASYNC_TIMEOUT": "Total timeout per async HTTP request.",
        "SCHEMA_SANITIZER_ASYNC_RETRIES": "Retry count for directory child downloads.",
        "SCHEMA_SANITIZER_SPOOL_DIR": "Local directory for replayable temporary staging files.",
    }
