"""Google Cloud Storage URI, discovery, and object operations."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

from ...core_impl.async_scheduler import retry_async
from ...core_impl.memory_budget import memory_budget
from ...core_impl.uris import content_type_for_uri, name_matches, normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    RemoteFile,
    split_parent_child,
)
from ..transport import open_aiohttp_session, read_response_bytes, write_response_to_file

_GCS_JSON_API_ENDPOINT = "https://storage.googleapis.com"
_GCS_READ_WRITE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write"


class _TransientGcsError(RuntimeError):
    """A GCS response that is safe to retry with backoff."""


def _list_page_size() -> int:
    """Return the fixed maximum JSON API page size."""
    return 1000


def _list_retries(memory_limit_bytes: int | None = None) -> int:
    """Derive list retries from the operation memory budget."""
    return memory_budget(memory_limit_bytes).async_retries


def _should_retry_gcs(exc: Exception) -> bool:
    """Return whether a failed GCS JSON API request is transient."""
    return isinstance(exc, _TransientGcsError)


async def _request_list_page(
    session: Any,
    url: str,
    params: dict[str, str],
    *,
    context: str,
) -> dict[str, Any]:
    """Fetch and decode one GCS list page with precise error classification."""
    async with session.get(url, params=params) as response:
        body = await response.text()
        if response.status == 200:
            return json.loads(body)
        message = f"{context}: status={response.status}, body={body[:1000]!r}"
        if response.status in {401, 403}:
            raise PermissionError(message)
        if response.status == 429 or 500 <= response.status <= 599:
            raise _TransientGcsError(message)
        raise RuntimeError(message)


@dataclass(frozen=True, slots=True)
class GcsRef:
    """Parsed GCS object reference."""

    bucket: str
    object_name: str


def parse_uri(uri: str) -> GcsRef:
    """Parse a gs:// or gcs:// URI."""
    parsed = urlparse(uri)
    if parsed.scheme.lower() not in {"gs", "gcs"} or not parsed.netloc:
        raise ValueError(f"not a GCS URI: {uri!r}")
    return GcsRef(parsed.netloc, parsed.path.lstrip("/"))


def object_uri(bucket: str, object_name: str) -> str:
    """Render a GCS object URI."""
    return f"gs://{bucket}/{object_name}"


def access_token() -> str:
    """Return a Google ADC token for object I/O."""
    google_auth = import_module("google.auth")
    google_requests = import_module("google.auth.transport.requests")

    credentials, _ = google_auth.default(scopes=[_GCS_READ_WRITE_SCOPE])
    if not credentials.valid:
        credentials.refresh(google_requests.Request())
    if not credentials.token:
        raise RuntimeError("Google ADC did not return an access token")
    return credentials.token


def api_base() -> str:
    """Return the canonical GCS JSON API endpoint."""
    return _GCS_JSON_API_ENDPOINT


def request_headers(
    *, accept_json: bool = False, content_type: str | None = None
) -> dict[str, str]:
    """Return ADC-authorized request headers."""
    headers: dict[str, str] = {"Authorization": f"Bearer {access_token()}"}
    if accept_json:
        headers["Accept"] = "application/json"
    if content_type is not None:
        headers["Content-Type"] = content_type
    return headers


def media_url(uri: str) -> str:
    """Return the JSON API media-download URL for one object."""
    ref = parse_uri(uri)
    url = (
        f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o/"
        f"{quote(ref.object_name, safe='')}?alt=media"
    )
    return url


async def download_bytes(session: Any, file: RemoteFile) -> bytes:
    """Download one GCS object into bytes."""
    async with session.get(media_url(file.uri)) as response:
        return await read_response_bytes(response, uri=file.uri)


async def file_exists(uri: str) -> bool:
    """Return whether one GCS object exists."""
    return await file_metadata(uri) is not None


async def file_metadata(uri: str) -> RemoteFile | None:
    """Return GCS object metadata using the existence request."""
    ref = parse_uri(uri)
    url = (
        f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o/"
        f"{quote(ref.object_name, safe='')}"
    )
    params = {"fields": "name,size"}
    headers = request_headers(accept_json=True)
    async with await open_aiohttp_session(headers) as session:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                payload = json.loads(await response.text())
                raw_size = payload.get("size")
                size = int(raw_size) if raw_size is not None else None
                return RemoteFile(uri, Path(ref.object_name).name, size)
            if response.status == 404:
                await response.read()
                return None
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


async def download_file_with_session(session: Any, file: RemoteFile, local_path: str) -> None:
    """Download one object to a local file using a shared session."""
    async with session.get(media_url(file.uri)) as response:
        await write_response_to_file(response, uri=file.uri, local_path=local_path)


async def download_file(uri: str, local_path: str) -> None:
    """Download one GCS object to a local file."""
    headers = request_headers()
    async with await open_aiohttp_session(headers) as session:
        await download_file_with_session(
            session,
            RemoteFile(uri, Path(urlparse(uri).path).name),
            local_path,
        )


async def upload_file(local_path: str, uri: str) -> None:
    """Upload a local file to GCS."""
    ref = parse_uri(uri)
    url = f"{api_base()}/upload/storage/v1/b/{quote(ref.bucket, safe='')}/o"
    params = {"uploadType": "media", "name": ref.object_name}
    headers = request_headers(content_type=content_type_for_uri(uri))
    async with await open_aiohttp_session(headers) as session:
        with Path(local_path).open("rb") as file_handle:
            async with session.post(url, params=params, data=file_handle) as response:
                await read_response_bytes(response, uri=uri)


async def delete_file(uri: str) -> None:
    """Delete one GCS object, treating an already-missing object as success."""
    ref = parse_uri(uri)
    url = (
        f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o/"
        f"{quote(ref.object_name, safe='')}"
    )
    params: dict[str, str] = {}
    headers = request_headers()
    async with await open_aiohttp_session(headers) as session:
        async with session.delete(url, params=params) as response:
            if response.status in {200, 204, 404}:
                await response.read()
                return
            body = await response.text()
            if response.status in {401, 403}:
                raise PermissionError(
                    "GCS returned a permission error while deleting an object. "
                    f"status={response.status}, uri={uri!r}, body={body[:1000]!r}"
                )
            raise RuntimeError(
                "Unexpected GCS response while deleting an object. "
                f"status={response.status}, uri={uri!r}, body={body[:1000]!r}"
            )


async def list_directory(uri: str, suffixes: tuple[str, ...]) -> list[RemoteFile]:
    """List direct GCS child files under a URI prefix."""
    ref = parse_uri(uri)
    url = f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o"
    prefix = ref.object_name.rstrip("/") + "/"
    headers = request_headers(accept_json=True)
    files: list[RemoteFile] = []
    retries = _list_retries()
    async with await open_aiohttp_session(headers) as session:
        page_token: str | None = None
        while True:
            params = {
                "prefix": prefix,
                "delimiter": "/",
                "fields": "nextPageToken,items(name,size)",
                "maxResults": str(_list_page_size()),
            }
            if page_token:
                params["pageToken"] = page_token

            async def request_page() -> dict[str, Any]:
                """Fetch one direct-child listing page."""
                return await _request_list_page(
                    session,
                    url,
                    params,
                    context=f"GCS list failed for {uri!r}",
                )

            payload = await retry_async(
                request_page,
                retries=retries,
                should_retry=_should_retry_gcs,
            )
            for item in payload.get("items", ()):
                name = item.get("name")
                if not isinstance(name, str):
                    continue
                relative = name[len(prefix) :] if name.startswith(prefix) else name
                if not relative or "/" in relative or not name_matches(relative, suffixes):
                    continue
                size_raw = item.get("size")
                size = int(size_raw) if isinstance(size_raw, str) and size_raw.isdigit() else None
                files.append(RemoteFile(object_uri(ref.bucket, name), relative, size))
            page_token = payload.get("nextPageToken")
            if not isinstance(page_token, str) or not page_token:
                break
    files.sort(key=lambda file: file.name)
    return files


async def directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
) -> DirectoryDiscovery[RemoteFile]:
    """Return whether GCS directories contain a direct child matching suffixes."""
    accepted = normalize_extensions(suffixes)
    discovery = DirectoryDiscoveryBuilder[RemoteFile].from_uris(uris)
    groups: dict[tuple[str, str], dict[str, list[str]]] = {}
    for uri in uris:
        ref = parse_uri(uri)
        parsed = split_parent_child(ref.object_name)
        if parsed is None:
            continue
        parent_prefix, child = parsed
        bucket = ref.bucket
        groups.setdefault((bucket, parent_prefix), {}).setdefault(child, []).append(uri)

    if not groups:
        return discovery.finish()

    headers = request_headers(accept_json=True)
    budget = memory_budget(memory_limit_bytes)
    concurrency = budget.source_discovery_concurrency
    retries = budget.async_retries
    semaphore = asyncio.Semaphore(concurrency)

    async with await open_aiohttp_session(
        headers, memory_limit_bytes=memory_limit_bytes
    ) as session:

        async def scan_group(
            bucket: str,
            parent_prefix: str,
            children: dict[str, list[str]],
        ) -> None:
            """Scan one parent prefix and mark matching requested child directories."""
            url = f"{api_base()}/storage/v1/b/{quote(bucket, safe='')}/o"
            prefix = f"{parent_prefix.rstrip('/')}/" if parent_prefix else ""
            page_token: str | None = None
            while True:
                params = {
                    "prefix": prefix,
                    "fields": "nextPageToken,items(name,size)",
                    "maxResults": str(_list_page_size()),
                }
                if page_token:
                    params["pageToken"] = page_token

                async def request_page() -> dict[str, Any]:
                    """Fetch one GCS list page with retryable transient errors."""
                    async with semaphore:
                        return await _request_list_page(
                            session,
                            url,
                            params,
                            context=(
                                f"GCS bulk source discovery list failed for prefix={prefix!r}"
                            ),
                        )

                payload = await retry_async(
                    request_page,
                    retries=retries,
                    should_retry=_should_retry_gcs,
                )
                for item in payload.get("items", ()):
                    name = item.get("name")
                    if not isinstance(name, str) or not name.startswith(prefix):
                        continue
                    relative = name[len(prefix) :]
                    child, separator, filename = relative.partition("/")
                    child_uris = children.get(child) if separator else None
                    if not child_uris or "/" in filename or not name_matches(filename, accepted):
                        continue
                    size_raw = item.get("size")
                    size = (
                        int(size_raw) if isinstance(size_raw, str) and size_raw.isdigit() else None
                    )
                    remote_file = RemoteFile(object_uri(bucket, name), filename, size)
                    discovery.add(child_uris, remote_file)

                page_token = payload.get("nextPageToken")
                if not isinstance(page_token, str) or not page_token:
                    break

        await asyncio.gather(
            *(scan_group(bucket, parent, children) for (bucket, parent), children in groups.items())
        )

    return discovery.finish()
