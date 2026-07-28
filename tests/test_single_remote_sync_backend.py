"""Strict single-mode remote backend contracts."""

from __future__ import annotations

import socket
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Iterator

from schema_sanitizer.input_impl.directory_inputs import RemoteFile
from schema_sanitizer.remote_impl import sync_backend
from schema_sanitizer.remote_impl.providers import azure_sync, gcs_sync, s3_sync
from schema_sanitizer.remote_impl.sync_http import SyncHttpResult


class _BlockingObjectHandler(BaseHTTPRequestHandler):
    """Serve one object from an already-running single server thread."""

    payload = b'{"value": 7}\n'
    uploaded = b""

    def do_HEAD(self) -> None:  # noqa: N802
        """Return stable object metadata."""
        self.send_response(200)
        self.send_header("Content-Length", str(len(self.payload)))
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        """Return the complete object body."""
        self.send_response(200)
        self.send_header("Content-Length", str(len(self.payload)))
        self.end_headers()
        self.wfile.write(self.payload)

    def do_PUT(self) -> None:  # noqa: N802
        """Store the complete uploaded object body."""
        size = int(self.headers.get("Content-Length", "0"))
        type(self).uploaded = self.rfile.read(size)
        self.send_response(204)
        self.end_headers()

    def log_message(self, _format: str, *_args: object) -> None:
        """Suppress loopback server logging."""


@contextmanager
def _object_server() -> Iterator[str]:
    """Run one pre-started loopback server used by blocking client tests."""
    _BlockingObjectHandler.uploaded = b""
    server = HTTPServer(("127.0.0.1", 0), _BlockingObjectHandler)
    thread = threading.Thread(target=server.serve_forever, name="sync-http-test-server")
    thread.start()
    try:
        yield f"http://localhost:{server.server_port}/object.jsonl"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_http_single_backend_keeps_dns_and_transfers_on_caller_thread(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """DNS, HEAD, GET, and PUT run inline without starting a client thread."""
    caller_thread = threading.get_ident()
    dns_threads: list[int] = []
    original_getaddrinfo = socket.getaddrinfo

    def tracked_getaddrinfo(*args: Any, **kwargs: Any) -> Any:
        """Record the thread resolving the loopback hostname."""
        dns_threads.append(threading.get_ident())
        return original_getaddrinfo(*args, **kwargs)

    with _object_server() as uri:
        monkeypatch.setattr(socket, "getaddrinfo", tracked_getaddrinfo)

        def forbidden_thread_start(_thread: threading.Thread) -> None:
            """Reject any client-side helper thread creation."""
            raise AssertionError("strict single remote backend started a helper thread")

        monkeypatch.setattr(threading.Thread, "start", forbidden_thread_start)
        metadata = sync_backend.remote_file_metadata(uri, memory_limit_bytes=64 * 1024 * 1024)
        assert metadata == RemoteFile(uri, "object.jsonl", len(_BlockingObjectHandler.payload))

        downloaded = tmp_path / "downloaded.jsonl"
        sync_backend.download_single_file(
            uri,
            str(downloaded),
            memory_limit_bytes=64 * 1024 * 1024,
        )
        assert downloaded.read_bytes() == _BlockingObjectHandler.payload

        upload = tmp_path / "upload.jsonl"
        upload.write_bytes(b'{"uploaded": true}\n')
        sync_backend.upload_file(
            str(upload),
            uri,
            memory_limit_bytes=64 * 1024 * 1024,
        )
        assert _BlockingObjectHandler.uploaded == upload.read_bytes()

    assert dns_threads
    assert set(dns_threads) == {caller_thread}


def test_s3_blocking_download_writes_each_chunk_once(tmp_path: Path) -> None:
    """The direct Botocore stream preserves exact bytes and caller ownership."""
    caller_thread = threading.get_ident()
    calls: list[int] = []

    class Body:
        """Return two chunks followed by EOF."""

        def __init__(self) -> None:
            """Initialize the deterministic chunk iterator."""
            self._chunks = iter((b"abc", b"def", b""))

        def read(self, _size: int) -> bytes:
            """Return the next body chunk and record its thread."""
            calls.append(threading.get_ident())
            return next(self._chunks)

        def close(self) -> None:
            """Record same-thread body closure."""
            calls.append(threading.get_ident())

    class Client:
        """Expose the blocking get_object operation."""

        def get_object(self, **kwargs: object) -> dict[str, object]:
            """Return one blocking body after validating the object key."""
            calls.append(threading.get_ident())
            assert kwargs == {"Bucket": "bucket", "Key": "source.bin"}
            return {"Body": Body()}

    target = tmp_path / "source.bin"
    s3_sync.download_file_with_client(
        Client(),
        RemoteFile("s3://bucket/source.bin", "source.bin", 6),
        str(target),
    )

    assert target.read_bytes() == b"abcdef"
    assert calls
    assert set(calls) == {caller_thread}


def test_s3_blocking_upload_uses_no_transfer_manager(monkeypatch, tmp_path: Path) -> None:
    """Small S3 uploads use one direct blocking put_object call."""
    caller_thread = threading.get_ident()
    calls: list[int] = []
    payload = b"blocking-s3-upload"
    source = tmp_path / "source.bin"
    source.write_bytes(payload)

    class Client:
        """Capture one direct SDK upload."""

        def put_object(self, **kwargs: object) -> None:
            """Consume one direct upload body on the caller thread."""
            calls.append(threading.get_ident())
            assert kwargs["Bucket"] == "bucket"
            assert kwargs["Key"] == "target.bin"
            assert kwargs["Body"].read() == payload  # type: ignore[union-attr]

    @contextmanager
    def fake_open_client() -> Iterator[Client]:
        """Yield one already-open blocking client."""
        calls.append(threading.get_ident())
        yield Client()
        calls.append(threading.get_ident())

    monkeypatch.setattr(s3_sync, "open_client", fake_open_client)
    s3_sync.upload_file(str(source), "s3://bucket/target.bin")

    assert calls
    assert set(calls) == {caller_thread}


def test_azure_blocking_transfers_force_sdk_concurrency_one(tmp_path: Path) -> None:
    """Azure sync download and upload explicitly disable SDK worker fan-out."""
    caller_thread = threading.get_ident()
    calls: list[tuple[str, int, int | None]] = []
    source = tmp_path / "upload.bin"
    source.write_bytes(b"azure-upload")
    target = tmp_path / "download.bin"

    class Stream:
        """Yield blocking download chunks."""

        def chunks(self) -> tuple[bytes, ...]:
            """Return deterministic blocking Azure chunks."""
            calls.append(("chunks", threading.get_ident(), None))
            return (b"azure-", b"download")

    class Blob:
        """Capture Azure Blob SDK options."""

        def download_blob(self, *, max_concurrency: int) -> Stream:
            """Capture the requested Azure download concurrency."""
            calls.append(("download", threading.get_ident(), max_concurrency))
            return Stream()

        def upload_blob(self, body: Any, **kwargs: object) -> None:
            """Capture and validate one Azure upload call."""
            calls.append(("upload", threading.get_ident(), int(kwargs["max_concurrency"])))
            assert kwargs["overwrite"] is True
            assert kwargs["length"] == len(b"azure-upload")
            assert body.read() == b"azure-upload"

    class Service:
        """Return one blocking blob client."""

        def get_blob_client(self, container: str, blob: str) -> Blob:
            """Return the fake client for one container/blob pair."""
            calls.append((f"client:{container}/{blob}", threading.get_ident(), None))
            return Blob()

    service = Service()
    azure_sync.download_file_with_service(
        service,
        RemoteFile("azure://account/container/source.bin", "source.bin"),
        str(target),
    )
    assert target.read_bytes() == b"azure-download"

    ref = azure_sync.parse_uri("azure://account/container/target.bin")

    @contextmanager
    def fake_open_service(_ref: object) -> Iterator[Service]:
        """Reuse the fake blocking service."""
        assert _ref == ref
        yield service

    original = azure_sync.open_service
    azure_sync.open_service = fake_open_service
    try:
        azure_sync.upload_file(str(source), "azure://account/container/target.bin")
    finally:
        azure_sync.open_service = original

    assert {thread for _label, thread, _limit in calls} == {caller_thread}
    assert ("download", caller_thread, 1) in calls
    assert ("upload", caller_thread, 1) in calls


def test_gcs_blocking_credentials_and_metadata_stay_inline(monkeypatch) -> None:
    """ADC token resolution and JSON API metadata run on the caller thread."""
    caller_thread = threading.get_ident()
    calls: list[tuple[str, int]] = []

    def fake_access_token() -> str:
        """Return one token and record credential resolution ownership."""
        calls.append(("token", threading.get_ident()))
        return "token"

    def fake_request_bytes(*_args: object, **_kwargs: object) -> SyncHttpResult:
        """Return deterministic metadata and record request ownership."""
        calls.append(("request", threading.get_ident()))
        return SyncHttpResult(200, {}, b'{"name":"object.jsonl","size":"17"}')

    monkeypatch.setattr(gcs_sync, "access_token", fake_access_token)
    monkeypatch.setattr(gcs_sync, "request_bytes", fake_request_bytes)

    metadata = gcs_sync.file_metadata("gs://bucket/object.jsonl")

    assert metadata == RemoteFile("gs://bucket/object.jsonl", "object.jsonl", 17)
    assert calls == [("token", caller_thread), ("request", caller_thread)]


def test_s3_single_download_retries_from_an_empty_destination(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A transient S3 body failure cannot leave duplicated partial bytes."""
    from schema_sanitizer.core_impl import sync_retry

    attempts = 0

    class Body:
        """Fail one stream after a partial chunk, then return a complete body."""

        def __init__(self, *, fail: bool) -> None:
            """Configure whether this stream fails after its first chunk."""
            self._fail = fail
            self._reads = 0

        def read(self, _size: int) -> bytes:
            """Return one chunk or raise the synthetic transient failure."""
            self._reads += 1
            if self._fail:
                if self._reads == 1:
                    return b"partial-"
                raise ConnectionError("lost S3 body")
            return b"complete" if self._reads == 1 else b""

        def close(self) -> None:
            """Close the fake body."""

    class Client:
        """Return one interrupted and one successful body."""

        def get_object(self, **_kwargs: object) -> dict[str, object]:
            """Return the interrupted body first and successful body second."""
            nonlocal attempts
            attempts += 1
            return {"Body": Body(fail=attempts == 1)}

    @contextmanager
    def fake_open_client() -> Iterator[Client]:
        """Yield one reusable direct client."""
        yield Client()

    monkeypatch.setattr(sync_retry, "sleep", lambda _delay: None)
    monkeypatch.setattr(s3_sync, "open_client", fake_open_client)
    target = tmp_path / "retried.bin"

    s3_sync.download_file(
        "s3://bucket/retried.bin",
        str(target),
        memory_limit_bytes=64 * 1024 * 1024,
    )

    assert attempts == 2
    assert target.read_bytes() == b"complete"


def test_s3_single_upload_reopens_spool_for_retry(monkeypatch, tmp_path: Path) -> None:
    """A transient direct PUT replays the complete spool from byte zero."""
    from schema_sanitizer.core_impl import sync_retry

    payload = b"replay-complete-spool"
    source = tmp_path / "source.bin"
    source.write_bytes(payload)
    attempts: list[bytes] = []

    class Client:
        """Fail after consuming the first direct upload body."""

        def put_object(self, **kwargs: object) -> None:
            """Consume the body and fail only the first response."""
            body = kwargs["Body"]
            attempts.append(body.read())  # type: ignore[union-attr]
            if len(attempts) == 1:
                raise ConnectionError("lost S3 PUT response")

    @contextmanager
    def fake_open_client() -> Iterator[Client]:
        """Yield one reusable direct client."""
        yield Client()

    monkeypatch.setattr(sync_retry, "sleep", lambda _delay: None)
    monkeypatch.setattr(s3_sync, "open_client", fake_open_client)

    s3_sync.upload_file(
        str(source),
        "s3://bucket/target.bin",
        memory_limit_bytes=64 * 1024 * 1024,
    )

    assert attempts == [payload, payload]
