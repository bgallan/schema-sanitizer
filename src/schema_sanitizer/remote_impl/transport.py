"""Shared synchronous and HTTP transport primitives for remote providers."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from pathlib import Path
from typing import Any

from ..core_impl.async_scheduler import read_float_env, read_int_env
from ..core_impl.uris import content_type_for_uri
from ..errors import SchemaSanitizerResourceError

TRANSFER_CHUNK_BYTES = 1024 * 1024


def run_sync(coro: Any) -> Any:
    """Run a coroutine from synchronous API code, even inside an active loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
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
        raise RuntimeError(f"HTTP download failed for {uri!r}: {response.status} {body[:1000]!r}")
    with Path(local_path).open("wb") as file_handle:
        async for chunk in response.content.iter_chunked(TRANSFER_CHUNK_BYTES):
            file_handle.write(chunk)


async def open_aiohttp_session(headers: dict[str, str] | None = None) -> Any:
    """Open an aiohttp session with the configured timeout and concurrency."""
    aiohttp = import_module("aiohttp")

    timeout = aiohttp.ClientTimeout(total=read_float_env("SCHEMA_SANITIZER_ASYNC_TIMEOUT", 120.0))
    concurrency = read_int_env("SCHEMA_SANITIZER_ASYNC_CONCURRENCY", 64)
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        ttl_dns_cache=300,
    )
    return aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers)


async def download_http_file(uri: str, local_path: str) -> None:
    """Download one HTTP(S) object to a local file."""
    async with await open_aiohttp_session() as session:
        async with session.get(uri) as response:
            await write_response_to_file(response, uri=uri, local_path=local_path)


async def http_file_exists(uri: str) -> bool:
    """Return whether one HTTP(S) object appears to exist."""
    async with await open_aiohttp_session() as session:
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


async def upload_http_file(local_path: str, uri: str) -> None:
    """Upload a local file to an HTTP(S) endpoint with PUT."""
    headers = {"Content-Type": content_type_for_uri(uri)}
    async with await open_aiohttp_session(headers) as session:
        with Path(local_path).open("rb") as file_handle:
            async with session.put(uri, data=file_handle) as response:
                if response.status not in {200, 201, 202, 204}:
                    body = await response.text()
                    raise RuntimeError(
                        f"HTTP upload failed for {uri!r}: {response.status} {body[:1000]!r}"
                    )
