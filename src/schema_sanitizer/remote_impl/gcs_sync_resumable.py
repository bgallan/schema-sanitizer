"""Blocking GCS resumable upload with committed-offset reconciliation."""

from __future__ import annotations

from pathlib import Path
from typing import Mapping

from ..core_impl.memory_budget import memory_budget
from ..core_impl.sync_retry import retry_sync
from ..core_impl.uris import content_type_for_uri
from .sync_http import (
    SyncHttpResult,
    SyncHttpStatusError,
    request_bytes,
    request_json_url,
    retryable_http_error,
)
from .upload_policy import (
    read_upload_range,
    release_upload_payload,
    remote_upload_policy,
)


class TransientGcsUploadError(RuntimeError):
    """A blocking GCS response that may be retried."""


def _retryable(exc: Exception) -> bool:
    """Return whether one resumable operation is transient."""
    return isinstance(exc, TransientGcsUploadError) or retryable_http_error(exc)


def _committed_end(result: SyncHttpResult) -> int:
    """Return the final committed byte ordinal from a resumable response."""
    raw = result.headers.get("Range") or result.headers.get("range")
    if not isinstance(raw, str) or not raw.startswith("bytes=0-"):
        return -1
    try:
        return int(raw.removeprefix("bytes=0-"))
    except ValueError:
        return -1


def _status(
    upload_url: str,
    *,
    auth_headers: Mapping[str, str],
    total_bytes: int,
    timeout: float,
) -> int:
    """Return the next byte offset durably accepted by GCS."""
    headers = dict(auth_headers)
    headers.update({"Content-Length": "0", "Content-Range": f"bytes */{total_bytes}"})
    result = request_bytes("PUT", upload_url, headers=headers, body=b"", timeout=timeout)
    if result.status == 308:
        return _committed_end(result) + 1
    if result.status in {200, 201}:
        return total_bytes
    if result.status == 429 or 500 <= result.status <= 599:
        raise TransientGcsUploadError(
            f"GCS resumable status failed: status={result.status}, body={result.body[:1000]!r}"
        )
    raise RuntimeError(
        f"GCS resumable status failed: status={result.status}, body={result.body[:1000]!r}"
    )


def _send_range(
    upload_url: str,
    payload: bytes,
    *,
    auth_headers: Mapping[str, str],
    start: int,
    total_bytes: int,
    content_type: str,
    timeout: float,
) -> int:
    """Send one range and return the server-confirmed next byte offset."""
    end = start + len(payload) - 1
    headers = dict(auth_headers)
    headers.update(
        {
            "Content-Type": content_type,
            "Content-Range": f"bytes {start}-{end}/{total_bytes}",
        }
    )
    result = request_bytes("PUT", upload_url, headers=headers, body=payload, timeout=timeout)
    if result.status in {200, 201}:
        if end != total_bytes - 1:
            raise RuntimeError("GCS finalized a resumable upload before the final byte")
        return total_bytes
    if result.status == 308:
        next_offset = _committed_end(result) + 1
        if not start <= next_offset <= end + 1:
            raise RuntimeError(
                "GCS resumable upload returned an invalid committed range: "
                f"start={start}, end={end}, next={next_offset}"
            )
        return next_offset
    if result.status == 429 or 500 <= result.status <= 599:
        raise TransientGcsUploadError(
            f"GCS resumable chunk failed: status={result.status}, body={result.body[:1000]!r}"
        )
    raise RuntimeError(
        f"GCS resumable chunk failed: status={result.status}, body={result.body[:1000]!r}"
    )


def _send_with_reconciliation(
    upload_url: str,
    payload: bytes,
    *,
    auth_headers: Mapping[str, str],
    start: int,
    total_bytes: int,
    content_type: str,
    retries: int,
    timeout: float,
) -> int:
    """Replay one range only after querying GCS's durable offset."""
    for attempt in range(retries + 1):
        try:
            next_offset = _send_range(
                upload_url,
                payload,
                auth_headers=auth_headers,
                start=start,
                total_bytes=total_bytes,
                content_type=content_type,
                timeout=timeout,
            )
            if next_offset > start:
                return next_offset
            if attempt >= retries:
                raise RuntimeError("GCS resumable upload made no forward progress")
        except Exception as exc:
            if attempt >= retries or not _retryable(exc):
                raise
            next_offset = retry_sync(
                lambda: _status(
                    upload_url,
                    auth_headers=auth_headers,
                    total_bytes=total_bytes,
                    timeout=timeout,
                ),
                retries=retries - attempt,
                should_retry=_retryable,
                throttle_key="gcs",
            )
            if next_offset > start:
                return next_offset
    raise RuntimeError("unreachable GCS resumable retry state")


def upload_gcs_resumable_file(
    local_path: str,
    uri: str,
    *,
    initiation_url: str,
    object_name: str,
    auth_headers: Mapping[str, str],
    memory_limit_bytes: int | None,
) -> None:
    """Publish a completed spool through a sequential blocking GCS session."""
    tuning = remote_upload_policy(
        "gcs",
        local_path,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode="single",
    )
    content_type = content_type_for_uri(uri)
    budget = memory_budget(memory_limit_bytes)
    headers = dict(auth_headers)
    headers.update(
        {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Upload-Content-Type": content_type,
            "X-Upload-Content-Length": str(tuning.file_size),
        }
    )
    url = request_json_url(
        initiation_url,
        {"uploadType": "resumable", "name": object_name},
    )

    def initiate() -> str:
        """Create one resumable upload session."""
        result = request_bytes(
            "POST",
            url,
            headers=headers,
            body=b"",
            timeout=budget.async_timeout_seconds,
        )
        if result.status in {200, 201}:
            location = result.headers.get("Location") or result.headers.get("location")
            if location:
                return location
            raise RuntimeError("GCS resumable initiation returned no session location")
        if result.status == 429 or 500 <= result.status <= 599:
            raise TransientGcsUploadError(
                "GCS resumable initiation failed: "
                f"status={result.status}, body={result.body[:1000]!r}"
            )
        raise SyncHttpStatusError(
            result.status,
            f"GCS resumable initiation failed for {uri!r}: "
            f"status={result.status}, body={result.body[:1000]!r}",
        )

    source = Path(local_path)
    initial_stat = source.stat()
    upload_url = retry_sync(
        initiate,
        retries=budget.async_retries,
        should_retry=_retryable,
        throttle_key="gcs",
    )
    try:
        offset = 0
        while offset < tuning.file_size:
            current_stat = source.stat()
            if (current_stat.st_size, current_stat.st_mtime_ns) != (
                initial_stat.st_size,
                initial_stat.st_mtime_ns,
            ):
                raise OSError("remote upload spool changed before GCS resumable commit")
            size = min(tuning.part_bytes, tuning.file_size - offset)
            payload = read_upload_range(local_path, offset, size, tuning.file_size)
            try:
                next_offset = _send_with_reconciliation(
                    upload_url,
                    payload,
                    auth_headers=auth_headers,
                    start=offset,
                    total_bytes=tuning.file_size,
                    content_type=content_type,
                    retries=budget.async_retries,
                    timeout=budget.async_timeout_seconds,
                )
                if next_offset <= offset:
                    raise RuntimeError("GCS resumable upload made no forward progress")
                offset = next_offset
            finally:
                release_upload_payload(payload)
    except BaseException:
        try:
            request_bytes(
                "DELETE",
                upload_url,
                headers=auth_headers,
                timeout=budget.async_timeout_seconds,
            )
        except Exception:
            pass
        raise
    final_stat = source.stat()
    if (final_stat.st_size, final_stat.st_mtime_ns) != (
        initial_stat.st_size,
        initial_stat.st_mtime_ns,
    ):
        raise OSError("remote upload spool changed before GCS publication completed")


__all__ = ["TransientGcsUploadError", "upload_gcs_resumable_file"]
