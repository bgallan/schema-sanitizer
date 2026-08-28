"""Google Cloud Storage URI, discovery, and object operations.

It handles authentication, JSON API listing and metadata, bounded download, and
resumable publication for GCS objects.
"""

from __future__ import annotations

import asyncio
import json
from importlib import import_module
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urlparse

from ...core_impl.async_scheduler import (
    AsyncResultMemoryContract,
    drain_ordered_iterable_results,
    retry_async,
)
from ...core_impl.execution_policy import execution_policy
from ...core_impl.governed_sort import governed_sort
from ...core_impl.memory_budget import memory_budget
from ...core_impl.temporary_storage import StreamingStorageReservation
from ...core_impl.uris import content_type_for_uri, normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    current_directory_metadata_budget,
)
from ...sources.models import RemoteFile
from ..gcs_resumable import upload_gcs_resumable_file
from ..io_footprint import open_remote_local_file
from ..transport import (
    MAX_ERROR_RESPONSE_BYTES,
    open_aiohttp_session,
    read_bounded_response_bytes,
    read_bounded_response_text,
    read_response_bytes,
    write_response_to_file,
)
from ..upload_policy import remote_upload_policy
from . import (
    direct_child_items,
    next_page_token,
    requested_child_items,
    requested_directory_groups,
)
from .gcs_objects import (
    GcsRef as GcsRef,
)
from .gcs_objects import (
    directory_prefix,
    parse_uri,
    remote_file_from_metadata,
    remote_file_sort_key,
)

_GCS_JSON_API_ENDPOINT = "https://storage.googleapis.com"
_GCS_READ_WRITE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write"
_GCS_OBJECT_FIELDS = "name,size,updated,timeCreated,generation,metageneration,etag,crc32c"


class _TransientGcsError(RuntimeError):
    """A GCS response that is safe to retry with backoff."""

    def __init__(self, status: int, message: str, *, headers: Any = None) -> None:
        """Initialize a transient GCS error with optional retry guidance."""
        super().__init__(message)
        self.status, self.headers = status, headers


def _should_retry_gcs(exc: Exception) -> bool:
    """Return whether a failed GCS JSON API request is transient."""
    return isinstance(exc, _TransientGcsError)


async def _request_list_page(
    session: Any,
    url: str,
    params: dict[str, str],
    *,
    context: str,
    maximum_bytes: int,
) -> dict[str, Any]:
    """Fetch and decode one bounded GCS list page."""
    async with session.get(url, params=params) as response:
        body = await read_bounded_response_text(
            response,
            maximum_bytes=maximum_bytes,
            stage="remote_control_response",
        )
        if response.status == 200:
            return json.loads(body)
        message = f"{context}: status={response.status}, body={body[:1000]!r}"
        if response.status in {401, 403}:
            raise PermissionError(message)
        if response.status == 429 or 500 <= response.status <= 599:
            raise _TransientGcsError(
                response.status, message, headers=getattr(response, "headers", None)
            )
        raise RuntimeError(message)


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


def media_url(uri: str, *, generation: str | None = None) -> str:
    """Return a generation-pinned JSON API media URL for one object."""
    ref = parse_uri(uri)
    base = (
        f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o/"
        f"{quote(ref.object_name, safe='')}"
    )
    params = {"alt": "media"}
    if generation is not None:
        params["generation"] = generation
        params["ifGenerationMatch"] = generation
    return f"{base}?{urlencode(params)}"


def _directory_location(uri: str) -> tuple[tuple[str], str]:
    """Return the stable grouping location and object name for a GCS URI."""
    ref = parse_uri(uri)
    return (ref.bucket,), ref.object_name


async def download_bytes(session: Any, file: RemoteFile, *, maximum_bytes: int) -> bytes:
    """Download one GCS object only under an explicit materialization ceiling."""
    async with session.get(media_url(file.uri, generation=file.generation)) as response:
        if response.status not in {200, 201}:
            return await read_response_bytes(response, uri=file.uri)
        return await read_bounded_response_bytes(
            response, maximum_bytes=maximum_bytes, stage="gcs_download_bytes"
        )


async def file_exists(
    uri: str, *, memory_limit_bytes: int | None = None, threading_mode: str = "single"
) -> bool:
    """Return whether one GCS object exists."""
    return (
        await file_metadata(
            uri, memory_limit_bytes=memory_limit_bytes, threading_mode=threading_mode
        )
        is not None
    )


async def file_metadata(
    uri: str, *, memory_limit_bytes: int | None = None, threading_mode: str = "single"
) -> RemoteFile | None:
    """Return GCS object metadata using the existence request."""
    ref = parse_uri(uri)
    url = (
        f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o/"
        f"{quote(ref.object_name, safe='')}"
    )
    params = {"fields": _GCS_OBJECT_FIELDS}
    headers = request_headers(accept_json=True)
    async with await open_aiohttp_session(
        headers, memory_limit_bytes=memory_limit_bytes, threading_mode=threading_mode
    ) as session:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                body = await read_bounded_response_text(
                    response,
                    maximum_bytes=memory_budget(memory_limit_bytes).metadata_bytes,
                    stage="remote_control_response",
                )
                payload = json.loads(body)
                return remote_file_from_metadata(
                    ref.bucket,
                    payload,
                    display_name=Path(ref.object_name).name,
                    uri=uri,
                )
            if response.status == 404:
                await read_bounded_response_text(
                    response,
                    maximum_bytes=MAX_ERROR_RESPONSE_BYTES,
                    stage="remote_error_response",
                )
                return None
            body = await read_bounded_response_text(
                response,
                maximum_bytes=MAX_ERROR_RESPONSE_BYTES,
                stage="remote_error_response",
            )
            if response.status in {401, 403}:
                raise PermissionError(
                    "GCS returned a permission error while checking source object. "
                    f"status={response.status}, uri={uri!r}, body={body[:1000]!r}"
                )
            raise RuntimeError(
                "Unexpected GCS response while checking source object. "
                f"status={response.status}, uri={uri!r}, body={body[:1000]!r}"
            )


async def download_file_with_session(
    session: Any,
    file: RemoteFile,
    local_path: str,
    *,
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download one object while reserving local storage before writes."""
    async with session.get(media_url(file.uri, generation=file.generation)) as response:
        await write_response_to_file(
            response,
            uri=file.uri,
            local_path=local_path,
            storage_reservation=storage_reservation,
        )


async def download_file(
    uri: str | RemoteFile,
    local_path: str,
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download one GCS object to a local file."""
    selected = (
        uri if isinstance(uri, RemoteFile) else RemoteFile(uri, Path(urlparse(uri).path).name)
    )
    headers = request_headers()
    async with await open_aiohttp_session(
        headers, memory_limit_bytes=memory_limit_bytes, threading_mode=threading_mode
    ) as session:
        await download_file_with_session(
            session,
            selected,
            local_path,
            storage_reservation=storage_reservation,
        )


async def upload_file(
    local_path: str,
    uri: str,
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> None:
    """Upload a local file through media or resumable GCS publication."""
    ref = parse_uri(uri)
    url = f"{api_base()}/upload/storage/v1/b/{quote(ref.bucket, safe='')}/o"
    tuning = remote_upload_policy(
        "gcs",
        local_path,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )
    headers = request_headers()
    async with await open_aiohttp_session(
        headers, memory_limit_bytes=memory_limit_bytes, threading_mode=threading_mode
    ) as session:
        if tuning.multipart:
            await upload_gcs_resumable_file(
                session,
                local_path,
                uri,
                initiation_url=url,
                object_name=ref.object_name,
                memory_limit_bytes=memory_limit_bytes,
                threading_mode=threading_mode,
            )
            return
        params = {"uploadType": "media", "name": ref.object_name}
        request_headers_for_media = {"Content-Type": content_type_for_uri(uri)}
        with open_remote_local_file(local_path, "rb", label="gcs_upload_source") as file_handle:
            async with session.post(
                url,
                params=params,
                headers=request_headers_for_media,
                data=file_handle,
            ) as response:
                await read_response_bytes(response, uri=uri)


async def delete_file(
    uri: str, *, memory_limit_bytes: int | None = None, threading_mode: str = "single"
) -> None:
    """Delete one GCS object, treating an already-missing object as success."""
    ref = parse_uri(uri)
    url = (
        f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o/"
        f"{quote(ref.object_name, safe='')}"
    )
    params: dict[str, str] = {}
    headers = request_headers()
    async with await open_aiohttp_session(
        headers, memory_limit_bytes=memory_limit_bytes, threading_mode=threading_mode
    ) as session:
        async with session.delete(url, params=params) as response:
            if response.status in {200, 204, 404}:
                await read_bounded_response_text(
                    response,
                    maximum_bytes=MAX_ERROR_RESPONSE_BYTES,
                    stage="remote_control_response",
                )
                return
            body = await read_bounded_response_text(
                response,
                maximum_bytes=MAX_ERROR_RESPONSE_BYTES,
                stage="remote_error_response",
            )
            if response.status in {401, 403}:
                raise PermissionError(
                    "GCS returned a permission error while deleting an object. "
                    f"status={response.status}, uri={uri!r}, body={body[:1000]!r}"
                )
            raise RuntimeError(
                "Unexpected GCS response while deleting an object. "
                f"status={response.status}, uri={uri!r}, body={body[:1000]!r}"
            )


async def list_directory(
    uri: str,
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> list[RemoteFile]:
    """List direct GCS child files under a URI prefix."""
    ref = parse_uri(uri)
    url = f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o"
    prefix = directory_prefix(ref.object_name)
    headers = request_headers(accept_json=True)
    files: list[RemoteFile] = []
    metadata_budget = current_directory_metadata_budget(memory_limit_bytes)
    retries = memory_budget(memory_limit_bytes).async_retries
    async with await open_aiohttp_session(
        headers, memory_limit_bytes=memory_limit_bytes, threading_mode=threading_mode
    ) as session:
        page_token: str | None = None
        while True:
            params = {
                "prefix": prefix,
                "delimiter": "/",
                "fields": f"nextPageToken,items({_GCS_OBJECT_FIELDS})",
                "maxResults": "1000",
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
                    maximum_bytes=metadata_budget.limit_bytes,
                )

            payload = await retry_async(
                request_page,
                retries=retries,
                should_retry=_should_retry_gcs,
                throttle_key="gcs",
            )
            for item, relative in direct_child_items(
                payload.get("items", ()),
                prefix,
                suffixes,
                "name",
            ):
                remote_file = remote_file_from_metadata(
                    ref.bucket,
                    item,
                    display_name=relative,
                )
                metadata_budget.charge_file(remote_file, associations=4)
                files.append(remote_file)
            page_token = next_page_token(payload, "nextPageToken")
            if page_token is None:
                break
    governed_sort(files, key=remote_file_sort_key, stage="remote_discovery_sort")
    return files


async def directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> DirectoryDiscovery[RemoteFile]:
    """Return whether GCS directories contain a direct child matching suffixes."""
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

    headers = request_headers(accept_json=True)
    budget = memory_budget(memory_limit_bytes)
    concurrency = execution_policy(threading_mode, memory_limit_bytes).source_discovery_concurrency
    retries = budget.async_retries
    semaphore = asyncio.Semaphore(concurrency)

    async with await open_aiohttp_session(
        headers,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
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
                    "fields": f"nextPageToken,items({_GCS_OBJECT_FIELDS})",
                    "maxResults": "1000",
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
                            maximum_bytes=budget.metadata_bytes,
                        )

                payload = await retry_async(
                    request_page,
                    retries=retries,
                    should_retry=_should_retry_gcs,
                    throttle_key="gcs",
                )
                for item, child_uris, filename in requested_child_items(
                    payload.get("items", ()),
                    prefix,
                    children,
                    accepted,
                    "name",
                ):
                    remote_file = remote_file_from_metadata(
                        bucket,
                        item,
                        display_name=filename,
                    )
                    discovery.add(child_uris, remote_file)

                page_token = next_page_token(payload, "nextPageToken")
                if page_token is None:
                    break

        async def scan_key(key: tuple[str, str]) -> None:
            """Scan one GCS parent group without materialising all group keys."""
            bucket, parent = key
            await scan_group(bucket, parent, groups[key])

        await drain_ordered_iterable_results(
            groups,
            scan_key,
            window=concurrency,
            memory_contract=AsyncResultMemoryContract(preflight_bytes=64),
        )

    return discovery.finish()
