"""Shared synchronous and HTTP transport primitives for remote providers."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ..core_impl.async_scheduler import retry_async
from ..core_impl.execution_policy import execution_policy, normalize_threading_mode
from ..core_impl.memory_budget import memory_budget
from ..core_impl.uris import content_type_for_uri
from ..errors import SchemaSanitizerResourceError
from ..input_impl.directory_inputs import RemoteFile
from .provider_session_pool import current_provider_session_pool

TRANSFER_CHUNK_BYTES = 1024 * 1024


class _HttpStatusError(RuntimeError):
    """Carry one HTTP status while preserving the public RuntimeError surface."""

    def __init__(self, status: int, message: str) -> None:
        """Store the response status used by retry classification."""
        super().__init__(message)
        self.status = status


def _retryable_http_error(exc: Exception) -> bool:
    """Return whether a generic HTTP transfer may be safely retried."""
    if isinstance(exc, _HttpStatusError):
        return exc.status in {408, 425, 429} or 500 <= exc.status <= 599
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    if exc.__class__.__module__.split(".", 1)[0] != "aiohttp":
        return False
    aiohttp = import_module("aiohttp")
    retryable_types = (
        aiohttp.ClientConnectionError,
        aiohttp.ClientPayloadError,
        aiohttp.ServerTimeoutError,
    )
    return isinstance(exc, retryable_types)


def _disable_implicit_connection_retries(session: Any) -> None:
    """Prevent aiohttp from replaying a consumed streamed request body."""
    if hasattr(session, "_retry_connection"):
        session._retry_connection = False


def run_sync(coro: Any, *, threading_mode: str = "single") -> Any:
    """Run a coroutine without creating a helper thread in single mode."""
    mode = normalize_threading_mode(threading_mode)
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    if mode == "single":
        close = getattr(coro, "close", None)
        if callable(close):
            close()
        raise RuntimeError(
            "A synchronous schema-sanitizer API cannot run inside an active "
            "asyncio loop with threading_mode='single' because doing so would "
            "require a helper host thread. Call it outside the loop or use "
            "threading_mode='multi'."
        )
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="schema-sanitizer-async") as pool:
        return pool.submit(lambda: asyncio.run(coro)).result()


def check_download_size(uri: str, size: int | None, memory_limit_bytes: int | None) -> None:
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


async def read_response_bytes(response: Any, *, uri: str) -> bytes:
    """Read a successful aiohttp response or raise with request context."""
    if response.status in {200, 201}:
        return await response.read()
    body = await response.text()
    raise RuntimeError(f"HTTP {response.status} for {uri}: {body[:1000]!r}")


async def write_response_to_file(response: Any, *, uri: str, local_path: str) -> None:
    """Stream a successful HTTP response body to a local file."""
    if response.status != 200:
        body = await response.text()
        raise _HttpStatusError(
            response.status,
            f"HTTP download failed for {uri!r}: {response.status} {body[:1000]!r}",
        )
    with Path(local_path).open("wb") as file_handle:
        async for chunk in response.content.iter_chunked(TRANSFER_CHUNK_BYTES):
            file_handle.write(chunk)


async def _open_aiohttp_session_unpooled(
    headers: dict[str, str] | None = None,
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> Any:
    """Create one directly owned aiohttp session."""
    aiohttp = import_module("aiohttp")
    policy = execution_policy(threading_mode, memory_limit_bytes)

    budget = memory_budget(memory_limit_bytes)
    timeout = aiohttp.ClientTimeout(total=budget.async_timeout_seconds)
    concurrency = policy.async_concurrency
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        ttl_dns_cache=300,
    )
    return aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers)


async def open_aiohttp_session(
    headers: dict[str, str] | None = None,
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> Any:
    """Open or borrow an aiohttp session from the operation provider pool."""
    pool = current_provider_session_pool()
    if pool is None:
        return await _open_aiohttp_session_unpooled(
            headers,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )
    key = (
        "aiohttp",
        tuple(sorted((headers or {}).items())),
        memory_limit_bytes,
        normalize_threading_mode(threading_mode),
    )

    async def create() -> Any:
        """Create the operation-owned aiohttp session."""
        return await _open_aiohttp_session_unpooled(
            headers,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
        )

    return await pool.borrow_client(key, create)


async def download_http_file(
    uri: str,
    local_path: str,
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> None:
    """Download one HTTP(S) object with bounded transport retries."""
    retries = memory_budget(memory_limit_bytes).async_retries
    async with await open_aiohttp_session(
        memory_limit_bytes=memory_limit_bytes, threading_mode=threading_mode
    ) as session:

        async def request() -> None:
            """Open a fresh response and truncate the local file per attempt."""
            async with session.get(uri) as response:
                await write_response_to_file(response, uri=uri, local_path=local_path)

        await retry_async(request, retries=retries, should_retry=_retryable_http_error)


async def http_file_exists(
    uri: str, *, memory_limit_bytes: int | None = None, threading_mode: str = "single"
) -> bool:
    """Return whether one HTTP(S) object appears to exist."""
    return (
        await http_file_metadata(
            uri, memory_limit_bytes=memory_limit_bytes, threading_mode=threading_mode
        )
        is not None
    )


async def http_file_metadata(
    uri: str,
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> RemoteFile | None:
    """Return HTTP object metadata with bounded transient retries."""
    retries = memory_budget(memory_limit_bytes).async_retries
    async with await open_aiohttp_session(
        memory_limit_bytes=memory_limit_bytes, threading_mode=threading_mode
    ) as session:

        async def request() -> RemoteFile | None:
            """Perform one HEAD request and classify its status."""
            async with session.head(uri) as response:
                if response.status in {200, 204}:
                    raw_size = response.headers.get("Content-Length")
                    size = int(raw_size) if raw_size is not None else None
                    return RemoteFile(uri, Path(urlparse(uri).path).name, size)
                if response.status == 404:
                    return None
                if response.status in {401, 403}:
                    raise PermissionError(
                        f"HTTP returned a permission error while checking source object: {uri!r}"
                    )
                raise _HttpStatusError(
                    response.status,
                    f"Unexpected HTTP response while checking source object: "
                    f"status={response.status}, uri={uri!r}",
                )

        return await retry_async(request, retries=retries, should_retry=_retryable_http_error)


async def upload_http_file(
    local_path: str,
    uri: str,
    *,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> None:
    """Upload a local file with bounded idempotent PUT retries."""
    headers = {"Content-Type": content_type_for_uri(uri)}
    retries = memory_budget(memory_limit_bytes).async_retries
    async with await open_aiohttp_session(
        headers,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
    ) as session:
        # aiohttp may internally retry idempotent methods after a disconnect,
        # but it cannot rewind an arbitrary file payload. Our bounded outer
        # retry owns replay and reopens the spool from byte zero each time.
        _disable_implicit_connection_retries(session)

        async def request() -> None:
            """Reopen the spool so every retry starts from byte zero."""
            with Path(local_path).open("rb") as file_handle:
                async with session.put(
                    uri,
                    data=file_handle,
                    allow_redirects=False,
                ) as response:
                    if response.status not in {200, 201, 202, 204}:
                        body = await response.text()
                        raise _HttpStatusError(
                            response.status,
                            f"HTTP upload failed for {uri!r}: {response.status} {body[:1000]!r}",
                        )
                    await response.read()

        await retry_async(request, retries=retries, should_retry=_retryable_http_error)
