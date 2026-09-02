"""Strictly blocking HTTP transport for single-threaded remote execution.

It performs blocking DNS and HTTP through governed descriptors, bounded retries, strict
status handling, and escrowed connection cleanup.
"""

from __future__ import annotations

import http.client
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit

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
    memory_budget,
)
from ..core_impl.safe_errors import add_bounded_note
from ..core_impl.sync_retry import retry_sync
from ..core_impl.temporary_storage import StreamingStorageReservation
from ..core_impl.uris import content_type_for_uri
from ..errors import SchemaSanitizerResourceError
from ..sources.models import RemoteFile
from .file_streams import write_sync_reader_to_file
from .io_footprint import open_remote_local_file
from .sync_cleanup_escrow import reserve_sync_cleanup

TRANSFER_CHUNK_BYTES = 1024 * 1024
_MAX_CONTROL_RESPONSE_BYTES = 1024 * 1024
_MAX_ERROR_RESPONSE_BYTES = 64 * 1024
_MAX_REDIRECTS = 5


class _BudgetedBytes(bytes):
    """Response bytes retaining their operation-memory reservation."""

    _operation_memory_lease: Any | None
    _finalizer_ticket: int
    _finalizer_capsule: PreparedFinalizerCleanup | None

    def __new__(cls, value: bytes, lease: object):
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

    def close(self) -> None:
        """Close the retained lease before clearing local ownership."""
        lease = getattr(self, "_operation_memory_lease", None)
        close = getattr(lease, "close", None)
        if not callable(close):
            self._operation_memory_lease = None
            return
        close()
        if getattr(self, "_operation_memory_lease", None) is lease:
            self._operation_memory_lease = None
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None:
                acknowledge_prepared_finalizer_cleanup(capsule)
                self._finalizer_ticket = 0
                self._finalizer_capsule = None

    def __del__(self) -> None:
        """Release retained memory unless interpreter teardown has begun."""
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


class SyncHttpStatusError(RuntimeError):
    """Carry an HTTP status for bounded retry classification."""

    def __init__(self, status: int, message: str) -> None:
        """Store the response status."""
        super().__init__(message)
        self.status = status


@dataclass(frozen=True, slots=True)
class SyncHttpResult:
    """Fully consumed HTTP response."""

    status: int
    headers: Mapping[str, str]
    body: bytes


def retryable_http_error(exc: Exception) -> bool:
    """Return whether a blocking HTTP operation may be retried safely."""
    if isinstance(exc, SyncHttpStatusError):
        return exc.status in {408, 425, 429} or 500 <= exc.status <= 599
    return isinstance(
        exc,
        (
            TimeoutError,
            ConnectionError,
            OSError,
            http.client.HTTPException,
            socket.timeout,
        ),
    )


class _HttpConnectionOwner:
    """Retryable owner for one blocking HTTP connection/socket."""

    __slots__ = ("connection",)

    def __init__(self) -> None:
        """Create an empty authoritative slot for one blocking HTTP connection."""
        self.connection: http.client.HTTPConnection | None = None

    def close(self) -> None:
        """Close the socket-backed connection before clearing its authoritative slot."""
        connection = self.connection
        if connection is None:
            return
        connection.close()
        self.connection = None


class _EscrowedHttpConnection:
    """Expose an HTTPConnection while retaining its terminal escrow slot."""

    __slots__ = ("_owner", "_reservation", "_closed")

    def __init__(self, owner: _HttpConnectionOwner, reservation: Any) -> None:
        """Bind the HTTP owner to its pre-reserved terminal cleanup slot."""
        self._owner = owner
        self._reservation = reservation
        self._closed = False

    def __getattr__(self, name: str) -> Any:
        """Delegate unresolved attributes to the wrapped object."""
        connection = self._owner.connection
        if connection is None:
            raise AttributeError(name)
        return getattr(connection, name)

    def close(self) -> None:
        """Close through escrow, retaining the slot for retry if physical cleanup fails."""
        if self._closed:
            return
        try:
            self._reservation.close_and_commit()
        except BaseException:
            self._reservation.abandon_to_escrow()
            raise
        self._closed = True


def _connection(url: str, timeout: float) -> tuple[http.client.HTTPConnection, str]:
    """Create one same-thread HTTP connection and request target."""
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"not an HTTP(S) URI: {url!r}")
    port = parsed.port
    connection_type: type[http.client.HTTPConnection]
    connection_type = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    connection = connection_type(parsed.hostname, port=port, timeout=timeout)
    target = urlunsplit(("", "", parsed.path or "/", parsed.query, parsed.fragment))
    return connection, target


def _request_once(
    method: str,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    body: bytes | BinaryIO | None = None,
    body_length: int | None = None,
    timeout: float,
) -> tuple[_EscrowedHttpConnection, http.client.HTTPResponse]:
    """Open one request under pre-reserved terminal socket ownership."""
    owner = _HttpConnectionOwner()
    reservation = reserve_sync_cleanup(label="sync_http_connection", network_fds=1)
    reservation.bind_owner(owner)
    wrapper = _EscrowedHttpConnection(owner, reservation)
    request_headers = dict(headers or {})
    if body_length is not None:
        request_headers["Content-Length"] = str(body_length)
    try:
        connection, target = _connection(url, timeout)
        owner.connection = connection
        connection.request(method, target, body=body, headers=request_headers)
        return wrapper, connection.getresponse()
    except BaseException as primary:
        try:
            wrapper.close()
        except BaseException as cleanup_error:
            add_bounded_note(primary, "blocking HTTP cleanup retained for retry", cleanup_error)
        raise


def _headers(response: http.client.HTTPResponse) -> dict[str, str]:
    """Return response headers with original and lowercase lookup keys."""
    headers: dict[str, str] = {}
    for key, value in response.getheaders():
        headers[key] = value
        headers.setdefault(key.lower(), value)
    return headers


def _read_bounded_response(
    response: http.client.HTTPResponse,
    *,
    maximum_bytes: int,
    stage: str,
) -> bytes:
    """Read one control response without materializing beyond its ceiling."""
    limit = max(1, int(maximum_bytes))
    lease = acquire_operation_memory(limit + 1, stage=stage)
    try:
        payload = response.read(limit + 1)
        if len(payload) <= limit:
            if lease is None:
                return payload
            retained = _BudgetedBytes(payload, lease)
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
            "actual_bytes": len(payload),
        },
    )


def request_bytes(
    method: str,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    body: bytes | None = None,
    timeout: float,
    allow_redirects: bool = False,
    max_response_bytes: int = _MAX_CONTROL_RESPONSE_BYTES,
) -> SyncHttpResult:
    """Perform one blocking request and fully consume its body."""
    current = url
    for redirect in range(_MAX_REDIRECTS + 1):
        connection, response = _request_once(
            method,
            current,
            headers=headers,
            body=body,
            body_length=len(body) if body is not None else 0 if method in {"POST", "PUT"} else None,
            timeout=timeout,
        )
        try:
            payload = _read_bounded_response(
                response,
                maximum_bytes=max_response_bytes,
                stage="remote_control_response",
            )
            result_headers = _headers(response)
            status = response.status
        finally:
            connection.close()
        if allow_redirects and status in {301, 302, 303, 307, 308}:
            location = result_headers.get("Location") or result_headers.get("location")
            if not location:
                return SyncHttpResult(status, result_headers, payload)
            if redirect >= _MAX_REDIRECTS:
                raise RuntimeError(f"too many HTTP redirects for {url!r}")
            current = urljoin(current, location)
            if status == 303:
                method, body = "GET", None
            continue
        return SyncHttpResult(status, result_headers, payload)
    raise RuntimeError("unreachable redirect state")


def request_json_url(base_url: str, params: Mapping[str, str]) -> str:
    """Append encoded query parameters to one URL."""
    parsed = urlsplit(base_url)
    query = urlencode(params)
    if parsed.query:
        query = f"{parsed.query}&{query}"
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


def download_to_file(
    url: str,
    local_path: str,
    *,
    headers: Mapping[str, str] | None,
    timeout: float,
    expected_status: int = 200,
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Stream one response to disk and reject truncated Content-Length bodies."""
    connection, response = _request_once("GET", url, headers=headers, timeout=timeout)
    try:
        if response.status != expected_status:
            body = _read_bounded_response(
                response,
                maximum_bytes=_MAX_ERROR_RESPONSE_BYTES,
                stage="remote_error_response",
            )
            raise SyncHttpStatusError(
                response.status,
                f"HTTP download failed for {url!r}: {response.status} {body[:1000]!r}",
            )
        raw_length = response.getheader("Content-Length")
        expected_length = int(raw_length) if raw_length and raw_length.isdigit() else None
        written = write_sync_reader_to_file(
            response.read,
            local_path,
            chunk_bytes=TRANSFER_CHUNK_BYTES,
            storage_reservation=storage_reservation,
        )
        if expected_length is not None and written != expected_length:
            raise ConnectionError(
                f"truncated HTTP response for {url!r}: {written} of {expected_length} bytes"
            )
    finally:
        connection.close()


def upload_file_request(
    method: str,
    url: str,
    local_path: str,
    *,
    headers: Mapping[str, str] | None,
    timeout: float,
    max_response_bytes: int = _MAX_CONTROL_RESPONSE_BYTES,
) -> SyncHttpResult:
    """Upload one file from byte zero through a same-thread request."""
    source = Path(local_path)
    size = source.stat().st_size
    with open_remote_local_file(source, "rb", label="http_sync_upload_source") as file_handle:
        connection, response = _request_once(
            method,
            url,
            headers=headers,
            body=file_handle,
            body_length=size,
            timeout=timeout,
        )
        try:
            payload = _read_bounded_response(
                response,
                maximum_bytes=max_response_bytes,
                stage="remote_control_response",
            )
            return SyncHttpResult(response.status, _headers(response), payload)
        finally:
            connection.close()


def http_file_metadata(
    uri: str,
    *,
    memory_limit_bytes: int | None,
) -> RemoteFile | None:
    """Return HTTP metadata through same-thread DNS and transport calls."""
    budget = memory_budget(memory_limit_bytes)

    def request() -> RemoteFile | None:
        """Perform one retryable HEAD request."""
        result = request_bytes(
            "HEAD",
            uri,
            timeout=budget.async_timeout_seconds,
            allow_redirects=True,
        )
        if result.status in {200, 204}:
            raw_size = result.headers.get("Content-Length") or result.headers.get("content-length")
            size = int(raw_size) if raw_size is not None else None
            return RemoteFile(uri, Path(urlsplit(uri).path).name, size)
        if result.status == 404:
            return None
        if result.status in {401, 403}:
            raise PermissionError(
                f"HTTP returned a permission error while checking source object: {uri!r}"
            )
        raise SyncHttpStatusError(
            result.status,
            f"Unexpected HTTP response while checking source object: "
            f"status={result.status}, uri={uri!r}",
        )

    return retry_sync(
        request,
        retries=budget.async_retries,
        should_retry=retryable_http_error,
    )


def download_http_file(
    uri: str,
    local_path: str,
    *,
    memory_limit_bytes: int | None,
    storage_reservation: StreamingStorageReservation | None = None,
) -> None:
    """Download HTTP content with bounded replay from a truncated destination."""
    budget = memory_budget(memory_limit_bytes)
    retry_sync(
        lambda: download_to_file(
            uri,
            local_path,
            headers=None,
            timeout=budget.async_timeout_seconds,
            storage_reservation=storage_reservation,
        ),
        retries=budget.async_retries,
        should_retry=retryable_http_error,
    )


def upload_http_file(
    local_path: str,
    uri: str,
    *,
    memory_limit_bytes: int | None,
) -> None:
    """Upload one HTTP object with bounded full replay from byte zero."""
    budget = memory_budget(memory_limit_bytes)
    headers = {"Content-Type": content_type_for_uri(uri)}

    def request() -> None:
        """Reopen and upload the complete spool for each attempt."""
        result = upload_file_request(
            "PUT",
            uri,
            local_path,
            headers=headers,
            timeout=budget.async_timeout_seconds,
        )
        if result.status not in {200, 201, 202, 204}:
            raise SyncHttpStatusError(
                result.status,
                f"HTTP upload failed for {uri!r}: {result.status} {result.body[:1000]!r}",
            )

    retry_sync(
        request,
        retries=budget.async_retries,
        should_retry=retryable_http_error,
    )


__all__ = [
    "SyncHttpResult",
    "SyncHttpStatusError",
    "download_http_file",
    "download_to_file",
    "http_file_metadata",
    "request_bytes",
    "request_json_url",
    "retryable_http_error",
    "upload_file_request",
    "upload_http_file",
]
