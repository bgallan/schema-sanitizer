"""Google Cloud Storage resumable-upload protocol with offset reconciliation."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from ..core_impl.async_scheduler import retry_async, retry_delay
from ..core_impl.memory_budget import memory_budget
from ..core_impl.uris import content_type_for_uri
from .upload_policy import read_upload_range, remote_upload_policy


class TransientGcsUploadError(RuntimeError):
    """A GCS upload response or transport failure that may be retried."""


def _committed_end(response: Any) -> int:
    """Return the final committed byte ordinal from a resumable response."""
    raw = response.headers.get("Range") if hasattr(response, "headers") else None
    if not isinstance(raw, str) or not raw.startswith("bytes=0-"):
        return -1
    try:
        return int(raw.removeprefix("bytes=0-"))
    except ValueError:
        return -1


def _retryable(exc: Exception) -> bool:
    """Return whether one protocol or transport error is safe to retry."""
    if isinstance(exc, (TransientGcsUploadError, TimeoutError, ConnectionError, OSError)):
        return True
    return exc.__class__.__module__.split(".", 1)[0] == "aiohttp"


async def _status(session: Any, upload_url: str, *, total_bytes: int) -> int:
    """Return the next byte offset accepted by a resumable session."""
    headers = {"Content-Length": "0", "Content-Range": f"bytes */{total_bytes}"}
    async with session.put(upload_url, headers=headers, data=b"") as response:
        if response.status == 308:
            await response.read()
            return _committed_end(response) + 1
        if response.status in {200, 201}:
            await response.read()
            return total_bytes
        body = await response.text()
        if response.status == 429 or 500 <= response.status <= 599:
            raise TransientGcsUploadError(
                f"GCS resumable status failed: status={response.status}, body={body[:1000]!r}"
            )
        raise RuntimeError(
            f"GCS resumable status failed: status={response.status}, body={body[:1000]!r}"
        )


async def _send_range(
    session: Any,
    upload_url: str,
    payload: bytes,
    *,
    start: int,
    total_bytes: int,
    content_type: str,
) -> int:
    """Send one range and return the server-confirmed next byte offset."""
    end = start + len(payload) - 1
    headers = {
        "Content-Type": content_type,
        "Content-Length": str(len(payload)),
        "Content-Range": f"bytes {start}-{end}/{total_bytes}",
    }
    async with session.put(upload_url, headers=headers, data=payload) as response:
        if response.status in {200, 201}:
            await response.read()
            if end != total_bytes - 1:
                raise RuntimeError("GCS finalized a resumable upload before the final byte")
            return total_bytes
        if response.status == 308:
            await response.read()
            next_offset = _committed_end(response) + 1
            if not start <= next_offset <= end + 1:
                raise RuntimeError(
                    "GCS resumable upload returned an invalid committed range: "
                    f"start={start}, end={end}, next={next_offset}"
                )
            return next_offset
        body = await response.text()
        if response.status == 429 or 500 <= response.status <= 599:
            raise TransientGcsUploadError(
                f"GCS resumable chunk failed: status={response.status}, body={body[:1000]!r}"
            )
        raise RuntimeError(
            f"GCS resumable chunk failed: status={response.status}, body={body[:1000]!r}"
        )


async def _send_with_reconciliation(
    session: Any,
    upload_url: str,
    payload: bytes,
    *,
    start: int,
    total_bytes: int,
    content_type: str,
    retries: int,
) -> int:
    """Retry one range after querying the provider's durable committed offset."""
    for attempt in range(retries + 1):
        try:
            next_offset = await _send_range(
                session,
                upload_url,
                payload,
                start=start,
                total_bytes=total_bytes,
                content_type=content_type,
            )
            if next_offset > start:
                return next_offset
            if attempt >= retries:
                raise RuntimeError("GCS resumable upload made no forward progress")
            await asyncio.sleep(retry_delay(attempt))
            continue
        except Exception as exc:
            if attempt >= retries or not _retryable(exc):
                raise
            try:
                next_offset = await retry_async(
                    lambda: _status(session, upload_url, total_bytes=total_bytes),
                    retries=retries - attempt,
                    should_retry=_retryable,
                )
            except Exception:
                if attempt >= retries:
                    raise
                await asyncio.sleep(retry_delay(attempt))
                continue
            if next_offset > start:
                return next_offset
            await asyncio.sleep(retry_delay(attempt))
    raise RuntimeError("unreachable GCS resumable retry state")


async def _abort(session: Any, upload_url: str) -> None:
    """Best-effort cancel one incomplete GCS resumable session."""
    try:
        async with session.delete(upload_url) as response:
            await response.read()
    except Exception:
        return


async def upload_gcs_resumable_file(
    session: Any,
    local_path: str,
    uri: str,
    *,
    initiation_url: str,
    object_name: str,
    memory_limit_bytes: int | None,
    threading_mode: str,
) -> None:
    """Publish one completed local spool through a resumable GCS session."""
    tuning = remote_upload_policy(
        "gcs",
        local_path,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    )
    content_type = content_type_for_uri(uri)
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Upload-Content-Type": content_type,
        "X-Upload-Content-Length": str(tuning.file_size),
    }
    params = {"uploadType": "resumable", "name": object_name}

    async def initiate() -> str:
        """Create one resumable session and return its opaque location."""
        async with session.post(
            initiation_url,
            params=params,
            headers=headers,
            data=b"",
        ) as response:
            if response.status in {200, 201}:
                await response.read()
                location = response.headers.get("Location")
                if isinstance(location, str) and location:
                    return location
                raise RuntimeError("GCS resumable initiation returned no session location")
            body = await response.text()
            if response.status == 429 or 500 <= response.status <= 599:
                raise TransientGcsUploadError(
                    "GCS resumable initiation failed: "
                    f"status={response.status}, body={body[:1000]!r}"
                )
            raise RuntimeError(
                f"GCS resumable initiation failed for {uri!r}: "
                f"status={response.status}, body={body[:1000]!r}"
            )

    retries = memory_budget(memory_limit_bytes).async_retries
    source = Path(local_path)
    initial_stat = source.stat()
    upload_url = await retry_async(initiate, retries=retries, should_retry=_retryable)
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
            next_offset = await _send_with_reconciliation(
                session,
                upload_url,
                payload,
                start=offset,
                total_bytes=tuning.file_size,
                content_type=content_type,
                retries=retries,
            )
            if next_offset <= offset:
                raise RuntimeError("GCS resumable upload made no forward progress")
            offset = next_offset
    except BaseException:
        await _abort(session, upload_url)
        raise

    if Path(local_path).stat().st_size != tuning.file_size:
        raise OSError("remote upload spool changed before GCS publication completed")


__all__ = ["TransientGcsUploadError", "upload_gcs_resumable_file"]
