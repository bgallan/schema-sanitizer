"""Strictly synchronous Google Cloud Storage operations for single mode."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import quote

from ...core_impl.governed_sort import governed_sort
from ...core_impl.memory_budget import memory_budget
from ...core_impl.sync_retry import retry_sync
from ...core_impl.temporary_storage import StreamingStorageReservation
from ...core_impl.uris import content_type_for_uri, name_matches, normalize_extensions
from ...input_impl.directory_inputs import (
    DirectoryDiscovery,
    DirectoryDiscoveryBuilder,
    current_directory_metadata_budget,
    split_parent_child,
)
from ...sources.models import RemoteFile
from ..gcs_sync_resumable import upload_gcs_resumable_file
from ..sync_http import (
    SyncHttpStatusError,
    download_to_file,
    request_bytes,
    request_json_url,
    retryable_http_error,
    upload_file_request,
)
from ..upload_policy import remote_upload_policy
from .gcs import (
    _GCS_OBJECT_FIELDS,
    access_token,
    api_base,
    media_url,
)
from .gcs_objects import (
    directory_prefix,
    parse_uri,
    remote_file_from_metadata,
    remote_file_sort_key,
)


def request_headers(
    *, accept_json: bool = False, content_type: str | None = None
) -> dict[str, str]:
    """Return ADC-authorized headers without an asynchronous transport."""
    headers: dict[str, str] = {"Authorization": f"Bearer {access_token()}"}
    if accept_json:
        headers["Accept"] = "application/json"
    if content_type is not None:
        headers["Content-Type"] = content_type
    return headers


def _json_result(result: Any, *, context: str) -> dict[str, Any]:
    """Decode one successful JSON response or classify its status."""
    if result.status == 200:
        return json.loads(result.body)
    message = f"{context}: status={result.status}, body={result.body[:1000]!r}"
    if result.status in {401, 403}:
        raise PermissionError(message)
    raise SyncHttpStatusError(result.status, message)


def file_metadata(uri: str, *, memory_limit_bytes: int | None = None) -> RemoteFile | None:
    """Return GCS object metadata on the caller thread."""
    ref = parse_uri(uri)
    url = (
        f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o/"
        f"{quote(ref.object_name, safe='')}"
    )
    url = request_json_url(url, {"fields": _GCS_OBJECT_FIELDS})
    headers = request_headers(accept_json=True)
    budget = memory_budget(memory_limit_bytes)

    def request() -> RemoteFile | None:
        """Perform one metadata request."""
        result = request_bytes(
            "GET",
            url,
            headers=headers,
            timeout=budget.async_timeout_seconds,
            max_response_bytes=budget.metadata_bytes,
        )
        if result.status == 404:
            return None
        payload = _json_result(result, context=f"GCS metadata failed for {uri!r}")
        return remote_file_from_metadata(
            ref.bucket,
            payload,
            display_name=Path(ref.object_name).name,
            uri=uri,
        )

    return retry_sync(
        request,
        retries=budget.async_retries,
        should_retry=retryable_http_error,
    )


def download_file(
    uri: str | RemoteFile,
    local_path: str,
    *,
    memory_limit_bytes: int | None = None,
    headers: dict[str, str] | None = None,
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download one GCS object through blocking JSON API media I/O."""
    selected = (
        uri
        if isinstance(uri, RemoteFile)
        else RemoteFile(uri, Path(parse_uri(uri).object_name).name)
    )
    budget = memory_budget(memory_limit_bytes)
    auth_headers = headers or request_headers()
    retry_sync(
        lambda: download_to_file(
            media_url(selected.uri, generation=selected.generation),
            local_path,
            headers=auth_headers,
            timeout=budget.async_timeout_seconds,
            storage_reservation=storage_reservation,
        ),
        retries=budget.async_retries,
        should_retry=retryable_http_error,
    )


def upload_file(
    local_path: str,
    uri: str,
    *,
    memory_limit_bytes: int | None = None,
) -> None:
    """Upload one GCS object via media or sequential resumable publication."""
    ref = parse_uri(uri)
    base_url = f"{api_base()}/upload/storage/v1/b/{quote(ref.bucket, safe='')}/o"
    tuning = remote_upload_policy(
        "gcs",
        local_path,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode="single",
    )
    headers = request_headers()
    if tuning.multipart:
        upload_gcs_resumable_file(
            local_path,
            uri,
            initiation_url=base_url,
            object_name=ref.object_name,
            auth_headers=headers,
            memory_limit_bytes=memory_limit_bytes,
        )
        return
    budget = memory_budget(memory_limit_bytes)
    url = request_json_url(base_url, {"uploadType": "media", "name": ref.object_name})
    media_headers = dict(headers)
    media_headers["Content-Type"] = content_type_for_uri(uri)

    def request() -> None:
        """Replay the completed local spool from byte zero."""
        result = upload_file_request(
            "POST",
            url,
            local_path,
            headers=media_headers,
            timeout=budget.async_timeout_seconds,
        )
        if result.status not in {200, 201}:
            raise SyncHttpStatusError(
                result.status,
                f"GCS media upload failed for {uri!r}: "
                f"status={result.status}, body={result.body[:1000]!r}",
            )

    retry_sync(
        request,
        retries=budget.async_retries,
        should_retry=retryable_http_error,
    )


def _list_page(
    url: str,
    params: dict[str, str],
    *,
    headers: dict[str, str],
    timeout: float,
    context: str,
    maximum_bytes: int,
) -> dict[str, Any]:
    """Fetch and decode one GCS listing page."""
    result = request_bytes(
        "GET",
        request_json_url(url, params),
        headers=headers,
        timeout=timeout,
        max_response_bytes=maximum_bytes,
    )
    return _json_result(result, context=context)


def list_directory(
    uri: str,
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
) -> list[RemoteFile]:
    """List direct GCS child objects serially."""
    ref = parse_uri(uri)
    url = f"{api_base()}/storage/v1/b/{quote(ref.bucket, safe='')}/o"
    prefix = directory_prefix(ref.object_name)
    headers = request_headers(accept_json=True)
    budget = memory_budget(memory_limit_bytes)
    files: list[RemoteFile] = []
    metadata_budget = current_directory_metadata_budget(memory_limit_bytes)
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
        payload = retry_sync(
            lambda: _list_page(
                url,
                params,
                headers=headers,
                timeout=budget.async_timeout_seconds,
                context=f"GCS list failed for {uri!r}",
                maximum_bytes=budget.metadata_bytes,
            ),
            retries=budget.async_retries,
            should_retry=retryable_http_error,
        )
        for item in payload.get("items", ()):
            name = item.get("name")
            if not isinstance(name, str):
                continue
            relative = name[len(prefix) :] if name.startswith(prefix) else name
            if not relative or "/" in relative or not name_matches(relative, suffixes):
                continue
            remote_file = remote_file_from_metadata(
                ref.bucket,
                item,
                display_name=relative,
            )
            metadata_budget.charge_file(remote_file, associations=4)
            files.append(remote_file)
        page_token = payload.get("nextPageToken")
        if not isinstance(page_token, str) or not page_token:
            break
    governed_sort(files, key=remote_file_sort_key, stage="remote_discovery_sort")
    return files


def directories_containing_files(
    uris: list[str],
    suffixes: tuple[str, ...],
    *,
    memory_limit_bytes: int | None = None,
) -> DirectoryDiscovery[RemoteFile]:
    """Discover requested GCS directories serially in stable group order."""
    accepted = normalize_extensions(suffixes)
    metadata_budget = current_directory_metadata_budget(memory_limit_bytes)
    discovery = DirectoryDiscoveryBuilder[RemoteFile].from_uris(
        uris,
        metadata_budget=metadata_budget,
    )
    groups: dict[tuple[str, str], dict[str, list[str]]] = {}
    for uri in uris:
        ref = parse_uri(uri)
        parsed = split_parent_child(ref.object_name)
        if parsed is None:
            continue
        parent_prefix, child = parsed
        # pass63 proof anchor: metadata_budget.charge_group_associations()
        discovery.publish_group_association(
            lambda: (
                groups.setdefault((ref.bucket, parent_prefix), {}).setdefault(child, []).append(uri)
            )
        )
    headers = request_headers(accept_json=True)
    budget = memory_budget(memory_limit_bytes)
    for (bucket, parent_prefix), children in groups.items():
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
            payload = retry_sync(
                lambda: _list_page(
                    url,
                    params,
                    headers=headers,
                    timeout=budget.async_timeout_seconds,
                    context=f"GCS bulk discovery failed for prefix={prefix!r}",
                    maximum_bytes=budget.metadata_bytes,
                ),
                retries=budget.async_retries,
                should_retry=retryable_http_error,
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
                discovery.add(
                    child_uris,
                    remote_file_from_metadata(
                        bucket,
                        item,
                        display_name=filename,
                    ),
                )
            page_token = payload.get("nextPageToken")
            if not isinstance(page_token, str) or not page_token:
                break
    return discovery.finish()


__all__ = [
    "directories_containing_files",
    "download_file",
    "file_metadata",
    "list_directory",
    "request_headers",
    "upload_file",
]
