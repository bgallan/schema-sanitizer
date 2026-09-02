"""Shared synchronous and HTTP transport primitives for remote providers.

It centralizes HTTP sessions, retry classification, response cleanup, size checks, and
bounded reader consumption for provider adapters.
"""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ..core_impl.async_scheduler import retry_async
from ..core_impl.execution_policy import execution_policy, normalize_threading_mode
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    acknowledge_prepared_finalizer_cleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_resource_finalizer_cleanup,
)
from ..core_impl.memory_budget import (
    acquire_operation_memory,
    current_operation_memory_ledger,
    memory_budget,
)
from ..core_impl.temporary_storage import StreamingStorageReservation
from ..core_impl.uris import content_type_for_uri
from ..errors import SchemaSanitizerResourceError
from ..sources.models import RemoteFile
from .file_streams import write_async_reader_to_file
from .io_footprint import open_remote_local_file
from .provider_session_pool import current_provider_session_pool

TRANSFER_CHUNK_BYTES = 1024 * 1024
MAX_CONTROL_RESPONSE_BYTES = 1024 * 1024
MAX_ERROR_RESPONSE_BYTES = 64 * 1024


class _BudgetedBytes(bytes):
    """Bytes retaining an operation-memory lease for their Python lifetime."""

    _operation_memory_lease: Any | None
    _finalizer_ticket: int
    _finalizer_capsule: PreparedFinalizerCleanup | None

    def __new__(cls, value: bytes | bytearray | memoryview, lease: Any):
        """Create bytes with a pre-reserved compact lease finalizer."""
        capsule = reserve_resource_finalizer_cleanup(lease)
        ticket = capsule.ticket
        try:
            obj = super().__new__(cls, value)
        except BaseException:
            cancel_prepared_finalizer_cleanup(capsule)
            raise
        obj._operation_memory_lease = lease
        obj._finalizer_ticket = ticket
        obj._finalizer_capsule = capsule
        return obj

    def _cancel_finalizer_slot(self) -> None:
        """Cancel the finalizer escrow slot for a synchronously closed owner."""
        ticket = getattr(self, "_finalizer_ticket", 0)
        capsule = getattr(self, "_finalizer_capsule", None)
        if ticket and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def _acknowledge_finalizer_slot(self) -> None:
        """Acknowledge cleanup and retire the associated finalizer slot."""
        ticket = getattr(self, "_finalizer_ticket", 0)
        capsule = getattr(self, "_finalizer_capsule", None)
        if ticket and capsule is not None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def release_memory(self) -> Any:
        """Detach and return the retained lease for ownership transfer."""
        lease = getattr(self, "_operation_memory_lease", None)
        self._operation_memory_lease = None
        self._cancel_finalizer_slot()
        return lease

    def close(self) -> None:
        """Release the retained charge before committing ownership transfer."""
        lease = getattr(self, "_operation_memory_lease", None)
        if lease is None:
            self._acknowledge_finalizer_slot()
            return
        lease.close()
        if getattr(self, "_operation_memory_lease", None) is lease:
            self._operation_memory_lease = None
            self._acknowledge_finalizer_slot()

    def __del__(self) -> None:
        """Publish only the preallocated memory-lease capsule from GC."""
        try:
            if runtime_is_finalizing():
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None and defer_prepared_finalizer_cleanup(capsule):
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
        except BaseException:
            pass


class _BudgetedText(str):
    """Decoded text retaining the source response's operation-memory lease."""

    _operation_memory_lease: Any | None
    _finalizer_ticket: int
    _finalizer_capsule: PreparedFinalizerCleanup | None

    def __new__(cls, value: str, lease: Any):
        """Create text with a pre-reserved compact lease finalizer."""
        capsule = reserve_resource_finalizer_cleanup(lease)
        ticket = capsule.ticket
        try:
            obj = super().__new__(cls, value)
        except BaseException:
            cancel_prepared_finalizer_cleanup(capsule)
            raise
        obj._operation_memory_lease = lease
        obj._finalizer_ticket = ticket
        obj._finalizer_capsule = capsule
        return obj

    def _acknowledge_finalizer_slot(self) -> None:
        """Acknowledge cleanup and retire the associated finalizer slot."""
        ticket = getattr(self, "_finalizer_ticket", 0)
        capsule = getattr(self, "_finalizer_capsule", None)
        if ticket and capsule is not None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def close(self) -> None:
        """Release the retained charge before clearing local ownership."""
        lease = getattr(self, "_operation_memory_lease", None)
        if lease is None:
            self._acknowledge_finalizer_slot()
            return
        lease.close()
        if getattr(self, "_operation_memory_lease", None) is lease:
            self._operation_memory_lease = None
            self._acknowledge_finalizer_slot()

    def __del__(self) -> None:
        """Publish only the preallocated memory-lease capsule from GC."""
        try:
            if runtime_is_finalizing():
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None and defer_prepared_finalizer_cleanup(capsule):
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
        except BaseException:
            pass


class _HttpStatusError(RuntimeError):
    """Carry one HTTP status while preserving the public RuntimeError surface."""

    def __init__(self, status: int, message: str, *, headers: Any = None) -> None:
        """Store retry-relevant response status and headers."""
        super().__init__(message)
        self.status = status
        self.headers = headers


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


async def read_bounded_async_reader_bytes(
    reader: Any,
    *,
    maximum_bytes: int,
    stage: str,
) -> bytes:
    """Read at most one bounded payload and retain its memory charge."""
    if type(maximum_bytes) is not int or maximum_bytes <= 0:
        raise ValueError("maximum_bytes must be a positive exact integer")
    limit = maximum_bytes
    lease = acquire_operation_memory((limit + 1) * 2 + 256, stage=stage)
    try:
        payload = await reader(limit + 1)
        if len(payload) > limit:
            raise SchemaSanitizerResourceError(
                f"memory_limit_bytes limit exceeded during {stage}: payload exceeds {limit} bytes",
                detail={
                    "stage": stage,
                    "limit_name": "payload_bytes",
                    "limit_bytes": limit,
                    "actual_bytes": len(payload),
                },
            )
        if lease is None:
            return bytes(payload)
        retained = _BudgetedBytes(payload, lease)
        lease.resize(sys.getsizeof(retained))
        lease = None
        return retained
    finally:
        if lease is not None:
            lease.close()


async def collect_bounded_async_chunks(
    chunks: Any,
    *,
    maximum_bytes: int,
    stage: str,
) -> bytes:
    """Collect an async chunk stream under a hard materialization ceiling."""
    if type(maximum_bytes) is not int or maximum_bytes <= 0:
        raise ValueError("maximum_bytes must be a positive exact integer")
    limit = maximum_bytes
    # Reserve source bytearray + immutable result before either can reach limit.
    lease = acquire_operation_memory((limit + 1) * 2 + 512, stage=stage)
    data = bytearray()
    try:
        async for chunk in chunks:
            next_size = len(data) + len(chunk)
            if next_size > limit:
                raise SchemaSanitizerResourceError(
                    f"memory_limit_bytes limit exceeded during {stage}: payload exceeds {limit} bytes",
                    detail={
                        "stage": stage,
                        "limit_name": "payload_bytes",
                        "limit_bytes": limit,
                        "actual_bytes": next_size,
                    },
                )
            data.extend(chunk)
        if lease is None:
            return bytes(data)
        retained = _BudgetedBytes(data, lease)
        del data
        lease.resize(sys.getsizeof(retained))
        lease = None
        return retained
    finally:
        if lease is not None:
            lease.close()


async def read_bounded_response_bytes(
    response: Any,
    *,
    maximum_bytes: int,
    stage: str,
) -> bytes:
    """Read one aiohttp control body without crossing its memory ceiling."""
    limit = max(1, int(maximum_bytes))
    # A bytes subclass is created to retain the lease after returning. Reserve
    # both the provider payload and that immutable retained copy up front.
    lease = acquire_operation_memory((limit + 1) * 2 + 256, stage=stage)
    payload_size = 0
    try:
        content = response.content
        reader = content.read
        # aiohttp StreamReader.read(n) may legally return fewer than n bytes
        # before EOF. Accumulate only into the precharged limit+1 window and
        # consult at_eof() before deciding the bounded body is complete.
        payload_buffer = bytearray()
        while len(payload_buffer) <= limit:
            remaining = limit + 1 - len(payload_buffer)
            chunk = await reader(remaining)
            if chunk:
                # A real aiohttp StreamReader honors the requested ceiling.
                # Reject a non-conforming adapter before copying an oversized
                # chunk into our own retained buffer.
                if len(chunk) > remaining:
                    payload_size = len(payload_buffer) + len(chunk)
                    break
                payload_buffer.extend(chunk)
                if len(payload_buffer) > limit:
                    payload_size = len(payload_buffer)
                    break
            if not chunk:
                break
            if content.at_eof():
                break
        else:
            payload_size = len(payload_buffer)
        if len(payload_buffer) <= limit:
            if lease is None:
                return bytes(payload_buffer)
            # Build the retained immutable value directly from the bytearray so
            # the transient peak remains source + result, not source + bytes +
            # bytes-subclass (three full payload copies).
            retained = _BudgetedBytes(payload_buffer, lease)
            del payload_buffer
            lease.resize(sys.getsizeof(retained))
            lease = None
            return retained
    except BaseException:
        if lease is not None:
            lease.close()
        raise
    if lease is not None:
        lease.close()
    raise SchemaSanitizerResourceError(
        f"memory_limit_bytes limit exceeded during {stage}: response body exceeds {limit} bytes",
        detail={
            "stage": stage,
            "limit_name": "control_response_bytes",
            "limit_bytes": limit,
            "actual_bytes": payload_size,
        },
    )


async def read_bounded_response_text(
    response: Any,
    *,
    maximum_bytes: int,
    stage: str,
) -> str:
    """Read and decode one bounded response body for diagnostics or JSON."""
    payload = await read_bounded_response_bytes(
        response,
        maximum_bytes=maximum_bytes,
        stage=stage,
    )
    charset = getattr(response, "charset", None) or "utf-8"
    if not isinstance(payload, _BudgetedBytes):
        return payload.decode(charset, errors="replace")

    lease = payload.release_memory()
    if lease is None:
        return payload.decode(charset, errors="replace")
    # Decoding and constructing the retained str subclass can temporarily hold
    # two Unicode objects alongside the source bytes. Charge the worst-case
    # four-byte representation before either allocation occurs.
    current_bytes = lease.reserved_bytes
    transient_text_bytes = 2 * (len(payload) * 4 + 256)
    try:
        lease.resize(current_bytes + transient_text_bytes)
        text = payload.decode(charset, errors="replace")
        retained = _BudgetedText(text, lease)
        del text
        del payload
        lease.resize(sys.getsizeof(retained))
        lease = None
        return retained
    finally:
        if lease is not None:
            lease.close()


async def read_response_bytes(response: Any, *, uri: str) -> bytes:
    """Read a successful aiohttp response or raise with request context."""
    if response.status in {200, 201}:
        ledger = current_operation_memory_ledger()
        if ledger is None:
            return await read_bounded_response_bytes(
                response,
                maximum_bytes=MAX_CONTROL_RESPONSE_BYTES,
                stage="remote_response",
            )
        snapshot = ledger.snapshot()
        available = max(1, snapshot.limit_bytes - snapshot.reserved_bytes)
        safe_payload_bytes = max(1, (available - 256) // 2)
        return await read_bounded_response_bytes(
            response,
            maximum_bytes=safe_payload_bytes,
            stage="remote_response",
        )
    body = await read_bounded_response_text(
        response,
        maximum_bytes=MAX_ERROR_RESPONSE_BYTES,
        stage="remote_error_response",
    )
    raise RuntimeError(f"HTTP {response.status} for {uri}: {body[:1000]!r}")


async def write_response_to_file(
    response: Any,
    *,
    uri: str,
    local_path: str,
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Stream a successful HTTP body while reserving disk before each write."""
    if response.status != 200:
        body = await read_bounded_response_text(
            response,
            maximum_bytes=MAX_ERROR_RESPONSE_BYTES,
            stage="remote_error_response",
        )
        raise _HttpStatusError(
            response.status,
            f"HTTP download failed for {uri!r}: {response.status} {body[:1000]!r}",
            headers=getattr(response, "headers", None),
        )
    await write_async_reader_to_file(
        response.content.read,
        local_path,
        chunk_bytes=TRANSFER_CHUNK_BYTES,
        storage_reservation=storage_reservation,
    )


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
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download one HTTP(S) object with bounded transport retries."""
    retries = memory_budget(memory_limit_bytes).async_retries
    async with await open_aiohttp_session(
        memory_limit_bytes=memory_limit_bytes, threading_mode=threading_mode
    ) as session:

        async def request() -> None:
            """Open a fresh response and truncate the local file per attempt."""
            async with session.get(uri) as response:
                await write_response_to_file(
                    response,
                    uri=uri,
                    local_path=local_path,
                    storage_reservation=storage_reservation,
                )

        await retry_async(
            request,
            retries=retries,
            should_retry=_retryable_http_error,
            throttle_key=f"http:{urlparse(uri).netloc}",
        )


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
                    headers=getattr(response, "headers", None),
                )

        return await retry_async(
            request,
            retries=retries,
            should_retry=_retryable_http_error,
            throttle_key=f"http:{urlparse(uri).netloc}",
        )


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
            with open_remote_local_file(
                local_path, "rb", label="http_upload_source"
            ) as file_handle:
                async with session.put(
                    uri,
                    data=file_handle,
                    allow_redirects=False,
                ) as response:
                    if response.status not in {200, 201, 202, 204}:
                        body = await read_bounded_response_text(
                            response,
                            maximum_bytes=MAX_ERROR_RESPONSE_BYTES,
                            stage="remote_error_response",
                        )
                        raise _HttpStatusError(
                            response.status,
                            f"HTTP upload failed for {uri!r}: {response.status} {body[:1000]!r}",
                            headers=getattr(response, "headers", None),
                        )
                    await read_bounded_response_bytes(
                        response,
                        maximum_bytes=MAX_CONTROL_RESPONSE_BYTES,
                        stage="remote_control_response",
                    )

        await retry_async(
            request,
            retries=retries,
            should_retry=_retryable_http_error,
            throttle_key=f"http:{urlparse(uri).netloc}",
        )
