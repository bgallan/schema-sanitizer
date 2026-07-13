"""Google Cloud Storage URI, discovery, and object operations."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

from ...core_impl.async_scheduler import read_int_env, retry_async
from ...core_impl.uris import content_type_for_uri, name_matches, normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    RemoteFile,
    split_parent_child,
)
from ..transport import open_aiohttp_session, read_response_bytes, write_response_to_file

_GCS_JSON_API_ENDPOINT = "https://storage.googleapis.com"
_GCS_READ_ONLY_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"


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

    credentials, _ = google_auth.default(scopes=[_GCS_READ_ONLY_SCOPE])
    if not credentials.valid:
        credentials.refresh(google_requests.Request())
    if not credentials.token:
        raise RuntimeError("Google ADC did not return an access token")
    return credentials.token


def api_base() -> str:
    """Return the GCS JSON API endpoint."""
    return os.getenv("GCS_JSON_API_ENDPOINT", _GCS_JSON_API_ENDPOINT).rstrip("/")


def requester_pays_project() -> str | None:
    """Return the optional requester-pays billing project."""
    return os.getenv("GCS_REQUESTER_PAYS_PROJECT") or None


def media_url(uri: str) -> str:
    """Return the JSON API media-download URL for one object."""
    ref = parse_uri(uri)
    url = (
        f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o/"
        f"{quote(ref.object_name, safe='')}?alt=media"
    )
    project = requester_pays_project()
    if project:
        url += f"&userProject={quote(project, safe='')}"
    return url


async def download_bytes(session: Any, file: RemoteFile) -> bytes:
    """Download one GCS object into bytes."""
    async with session.get(media_url(file.uri)) as response:
        return await read_response_bytes(response, uri=file.uri)


async def file_exists(uri: str) -> bool:
    """Return whether one GCS object exists."""
    ref = parse_uri(uri)
    url = (
        f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o/"
        f"{quote(ref.object_name, safe='')}"
    )
    params = {"fields": "name"}
    project = requester_pays_project()
    if project:
        params["userProject"] = project
    headers = {"Authorization": f"Bearer {access_token()}", "Accept": "application/json"}
    async with await open_aiohttp_session(headers) as session:
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


async def download_file_with_session(session: Any, file: RemoteFile, local_path: str) -> None:
    """Download one object to a local file using a shared session."""
    async with session.get(media_url(file.uri)) as response:
        await write_response_to_file(response, uri=file.uri, local_path=local_path)


async def download_file(uri: str, local_path: str) -> None:
    """Download one GCS object to a local file."""
    headers = {"Authorization": f"Bearer {access_token()}"}
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
    project = requester_pays_project()
    if project:
        params["userProject"] = project
    headers = {
        "Authorization": f"Bearer {access_token()}",
        "Content-Type": content_type_for_uri(uri),
    }
    async with await open_aiohttp_session(headers) as session:
        with Path(local_path).open("rb") as file_handle:
            async with session.post(url, params=params, data=file_handle) as response:
                await read_response_bytes(response, uri=uri)


async def list_directory(uri: str, suffixes: tuple[str, ...]) -> list[RemoteFile]:
    """List direct GCS child files under a URI prefix."""
    ref = parse_uri(uri)
    url = f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o"
    prefix = ref.object_name.rstrip("/") + "/"
    headers = {"Authorization": f"Bearer {access_token()}", "Accept": "application/json"}
    project = requester_pays_project()
    files: list[RemoteFile] = []
    async with await open_aiohttp_session(headers) as session:
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
            if project:
                params["userProject"] = project
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

    headers = {"Authorization": f"Bearer {access_token()}", "Accept": "application/json"}
    project = requester_pays_project()
    concurrency = read_int_env("SCHEMA_SANITIZER_SOURCE_DISCOVERY_GCS_BULK_CONCURRENCY", 16)
    retries = read_int_env(
        "SCHEMA_SANITIZER_SOURCE_DISCOVERY_GCS_RETRIES",
        read_int_env("SCHEMA_SANITIZER_ASYNC_RETRIES", 4),
    )
    semaphore = asyncio.Semaphore(concurrency)

    async with await open_aiohttp_session(headers) as session:

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
                    "maxResults": "1000",
                }
                if page_token:
                    params["pageToken"] = page_token
                if project:
                    params["userProject"] = project

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
