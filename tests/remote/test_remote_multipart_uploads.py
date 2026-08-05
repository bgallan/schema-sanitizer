"""Provider publication contracts for bounded multipart and resumable uploads."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from conftest import require_native


class _AsyncContext:
    """Return one supplied object from an asynchronous context manager."""

    def __init__(self, value: object) -> None:
        """Store the context value."""
        self.value = value

    async def __aenter__(self) -> object:
        """Return the stored value."""
        return self.value

    async def __aexit__(self, *_exc: object) -> None:
        """Leave the fake context."""


class _Response:
    """Minimal aiohttp-like response context used by GCS tests."""

    def __init__(
        self,
        status: int,
        *,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
        enter_error: BaseException | None = None,
    ) -> None:
        """Store status, headers, body, and an optional transport failure."""
        self.status = status
        self.headers = headers or {}
        self._body = body
        self._enter_error = enter_error

    async def __aenter__(self) -> _Response:
        """Return this response or raise the configured transport failure."""
        if self._enter_error is not None:
            raise self._enter_error
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Leave the fake response context."""

    async def read(self) -> bytes:
        """Return the configured body."""
        return self._body

    async def text(self) -> str:
        """Decode the configured body for error messages."""
        return self._body.decode("utf-8", errors="replace")


def _sparse_file(path: Path, size: int) -> None:
    """Create a deterministic sparse file without allocating its full payload."""
    with path.open("wb") as handle:
        handle.truncate(size)


def test_remote_upload_policy_bounds_memory_and_preserves_single_worker(tmp_path: Path) -> None:
    """Provider upload buffers derive only from memory and threading mode."""
    require_native()
    from schema_sanitizer.remote_impl.upload_policy import remote_upload_policy

    source = tmp_path / "large.bin"
    _sparse_file(source, 40 << 20)
    single = remote_upload_policy(
        "s3",
        str(source),
        memory_limit_bytes=256 << 20,
        threading_mode="single",
    )
    multi = remote_upload_policy(
        "s3",
        str(source),
        memory_limit_bytes=256 << 20,
        threading_mode="multi",
    )

    assert single.multipart is True
    assert single.concurrency == 1
    assert multi.multipart is True
    assert 1 < multi.concurrency <= 8
    assert multi.buffered_bytes <= (256 << 20) // 8
    assert single.part_count == multi.part_count


def test_s3_multipart_commits_parts_in_ordinal_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Out-of-order part completion still publishes an ordered S3 manifest."""
    require_native()
    from schema_sanitizer.remote_impl.providers import s3

    source = tmp_path / "s3-large.bin"
    _sparse_file(source, 40 << 20)

    class Client:
        """Record multipart lifecycle and force out-of-order completion."""

        def __init__(self) -> None:
            """Initialize the fake provider state."""
            self.active = 0
            self.peak_active = 0
            self.completed_parts: list[dict[str, Any]] | None = None
            self.aborted = False

        async def create_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
            """Start one fake upload."""
            return {"UploadId": "upload-1"}

        async def upload_part(self, **kwargs: object) -> dict[str, str]:
            """Complete later part numbers first."""
            part = int(kwargs["PartNumber"])
            self.active += 1
            self.peak_active = max(self.peak_active, self.active)
            try:
                await asyncio.sleep((6 - part) * 0.001)
                return {"ETag": f'"etag-{part}"'}
            finally:
                self.active -= 1

        async def complete_multipart_upload(self, **kwargs: object) -> None:
            """Record the ordered completion manifest."""
            self.completed_parts = list(kwargs["MultipartUpload"]["Parts"])

        async def abort_multipart_upload(self, **_kwargs: object) -> None:
            """Record an unexpected abort."""
            self.aborted = True

    client = Client()

    async def open_client() -> _AsyncContext:
        """Return the fake S3 client context."""
        return _AsyncContext(client)

    monkeypatch.setattr(s3, "open_client", open_client)
    asyncio.run(
        s3.upload_file(
            str(source),
            "s3://bucket/result.bin",
            memory_limit_bytes=256 << 20,
            threading_mode="multi",
        )
    )

    assert client.peak_active > 1
    assert client.aborted is False
    assert client.completed_parts is not None
    assert [part["PartNumber"] for part in client.completed_parts] == [1, 2, 3, 4, 5]


def test_s3_multipart_failure_drains_workers_and_aborts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed S3 part cancels later work before aborting remote state."""
    require_native()
    from schema_sanitizer.remote_impl.providers import s3

    source = tmp_path / "s3-failure.bin"
    _sparse_file(source, 32 << 20)

    class Client:
        """Fail part two and expose lifecycle counters."""

        def __init__(self) -> None:
            """Initialize the fake provider state."""
            self.active = 0
            self.aborted = False
            self.completed = False

        async def create_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
            """Start one fake upload."""
            return {"UploadId": "upload-fail"}

        async def upload_part(self, **kwargs: object) -> dict[str, str]:
            """Fail the second canonical part."""
            part = int(kwargs["PartNumber"])
            self.active += 1
            try:
                await asyncio.sleep(0)
                if part == 2:
                    raise ValueError("forced part failure")
                return {"ETag": f'"etag-{part}"'}
            finally:
                self.active -= 1

        async def complete_multipart_upload(self, **_kwargs: object) -> None:
            """Reject any accidental publication."""
            self.completed = True

        async def abort_multipart_upload(self, **_kwargs: object) -> None:
            """Record abort after workers drain."""
            assert self.active == 0
            self.aborted = True

    client = Client()

    async def open_client() -> _AsyncContext:
        """Return the fake S3 client context."""
        return _AsyncContext(client)

    monkeypatch.setattr(s3, "open_client", open_client)
    with pytest.raises(ValueError, match="forced part failure"):
        asyncio.run(
            s3.upload_file(
                str(source),
                "s3://bucket/failure.bin",
                memory_limit_bytes=256 << 20,
                threading_mode="multi",
            )
        )

    assert client.active == 0
    assert client.aborted is True
    assert client.completed is False


def test_s3_large_single_uses_sequential_multipart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Large single-mode publication remains one-task but avoids PutObject limits."""
    require_native()
    from schema_sanitizer.remote_impl.providers import s3

    source = tmp_path / "s3-single-large.bin"
    _sparse_file(source, 24 << 20)

    class Client:
        """Track maximum active parts in single mode."""

        def __init__(self) -> None:
            """Initialize the fake provider state."""
            self.active = 0
            self.peak = 0
            self.parts = 0

        async def create_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
            """Start the upload."""
            return {"UploadId": "single-upload"}

        async def upload_part(self, **kwargs: object) -> dict[str, str]:
            """Upload one sequential part."""
            self.active += 1
            self.peak = max(self.peak, self.active)
            try:
                self.parts += 1
                return {"ETag": f'"etag-{kwargs["PartNumber"]}"'}
            finally:
                self.active -= 1

        async def complete_multipart_upload(self, **_kwargs: object) -> None:
            """Complete the upload."""

        async def abort_multipart_upload(self, **_kwargs: object) -> None:
            """Fail if the successful upload aborts."""
            raise AssertionError("unexpected abort")

    client = Client()

    async def open_client() -> _AsyncContext:
        """Return the fake S3 client context."""
        return _AsyncContext(client)

    monkeypatch.setattr(s3, "open_client", open_client)
    asyncio.run(
        s3.upload_file(
            str(source),
            "s3://bucket/single-large.bin",
            memory_limit_bytes=256 << 20,
            threading_mode="single",
        )
    )

    assert client.parts > 1
    assert client.peak == 1


def test_gcs_resumable_recovers_from_lost_partial_response(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """GCS status reconciliation resumes at the durable provider offset."""
    require_native()
    from schema_sanitizer.remote_impl.providers import gcs

    source = tmp_path / "gcs-large.bin"
    _sparse_file(source, 20 << 20)

    class Session:
        """Simulate one lost response after a partial durable commit."""

        def __init__(self) -> None:
            """Initialize the fake provider state."""
            self.committed_end = -1
            self.range_calls: list[str] = []
            self.failed_once = False
            self.aborted = False

        async def __aenter__(self) -> Session:
            """Return this fake session."""
            return self

        async def __aexit__(self, *_exc: object) -> None:
            """Close this fake session."""

        def post(self, *_args: object, **_kwargs: object) -> _Response:
            """Create a resumable session."""
            return _Response(200, headers={"Location": "https://upload/session"})

        def put(self, _url: str, *, headers: dict[str, str], data: bytes) -> _Response:
            """Handle range uploads and status probes."""
            content_range = headers["Content-Range"]
            if content_range.startswith("bytes */"):
                range_header = (
                    {} if self.committed_end < 0 else {"Range": f"bytes=0-{self.committed_end}"}
                )
                return _Response(308, headers=range_header)

            self.range_calls.append(content_range)
            range_spec = content_range.split(" ", 1)[1].split("/", 1)[0]
            start, end = (int(value) for value in range_spec.split("-", 1))
            if not self.failed_once:
                self.failed_once = True
                self.committed_end = start + (4 << 20) - 1
                return _Response(0, enter_error=ConnectionError("lost response"))
            self.committed_end = end
            total = int(content_range.rsplit("/", 1)[1])
            if end == total - 1:
                return _Response(200)
            return _Response(308, headers={"Range": f"bytes=0-{end}"})

        def delete(self, _url: str) -> _Response:
            """Record an unexpected abort."""
            self.aborted = True
            return _Response(204)

    session = Session()

    async def open_session(*_args: object, **_kwargs: object) -> Session:
        """Return the fake GCS session."""
        return session

    monkeypatch.setattr(gcs, "open_aiohttp_session", open_session)
    monkeypatch.setattr(gcs, "request_headers", lambda **_kwargs: {"Authorization": "Bearer x"})
    asyncio.run(
        gcs.upload_file(
            str(source),
            "gs://bucket/result.bin",
            memory_limit_bytes=256 << 20,
            threading_mode="multi",
        )
    )

    assert session.aborted is False
    assert session.range_calls[0].startswith("bytes 0-")
    assert session.range_calls[1].startswith(f"bytes {4 << 20}-")
    assert session.committed_end == (20 << 20) - 1


def test_gcs_resumable_nonretryable_failure_aborts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A permanent GCS failure deletes the resumable session."""
    require_native()
    from schema_sanitizer.remote_impl.providers import gcs

    source = tmp_path / "gcs-failure.bin"
    _sparse_file(source, 20 << 20)

    class Session:
        """Reject the first chunk with a permanent response."""

        def __init__(self) -> None:
            """Initialize the fake provider state."""
            self.aborted = False

        async def __aenter__(self) -> Session:
            """Return this fake session."""
            return self

        async def __aexit__(self, *_exc: object) -> None:
            """Close this fake session."""

        def post(self, *_args: object, **_kwargs: object) -> _Response:
            """Create a resumable session."""
            return _Response(200, headers={"Location": "https://upload/failure"})

        def put(self, *_args: object, **_kwargs: object) -> _Response:
            """Reject the upload."""
            return _Response(400, body=b"bad range")

        def delete(self, _url: str) -> _Response:
            """Abort the failed session."""
            self.aborted = True
            return _Response(204)

    session = Session()

    async def open_session(*_args: object, **_kwargs: object) -> Session:
        """Return the fake GCS session."""
        return session

    monkeypatch.setattr(gcs, "open_aiohttp_session", open_session)
    monkeypatch.setattr(gcs, "request_headers", lambda **_kwargs: {"Authorization": "Bearer x"})
    with pytest.raises(RuntimeError, match="GCS resumable chunk failed"):
        asyncio.run(
            gcs.upload_file(
                str(source),
                "gs://bucket/failure.bin",
                memory_limit_bytes=256 << 20,
                threading_mode="multi",
            )
        )

    assert session.aborted is True


def test_azure_upload_uses_memory_bounded_sdk_concurrency(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Azure's SDK chunking consumes the shared operation-derived window."""
    require_native()
    from schema_sanitizer.remote_impl.providers import azure

    source = tmp_path / "azure-large.bin"
    _sparse_file(source, 80 << 20)

    class Blob:
        """Record SDK upload options."""

        def __init__(self) -> None:
            """Initialize the fake provider state."""
            self.kwargs: dict[str, Any] | None = None

        async def upload_blob(self, _handle: object, **kwargs: object) -> None:
            """Capture the bounded transfer controls."""
            self.kwargs = dict(kwargs)

    class Service:
        """Return one fake blob and record close."""

        def __init__(self) -> None:
            """Initialize the fake provider state."""
            self.blob = Blob()
            self.closed = False

        def get_blob_client(self, _container: str, _blob: str) -> Blob:
            """Return the fake blob."""
            return self.blob

        async def close(self) -> None:
            """Close the fake service."""
            self.closed = True

    service = Service()

    async def open_service(_ref: object) -> Service:
        """Return the fake Azure service."""
        return service

    monkeypatch.setattr(azure, "open_service", open_service)
    asyncio.run(
        azure.upload_file(
            str(source),
            "az://account/container/result.bin",
            memory_limit_bytes=256 << 20,
            threading_mode="multi",
        )
    )

    assert service.closed is True
    assert service.blob.kwargs is not None
    assert service.blob.kwargs["length"] == 80 << 20
    assert 1 < service.blob.kwargs["max_concurrency"] <= 8


def test_s3_multipart_reports_earliest_failing_part(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A fast later failure cannot overtake a slower earlier part failure."""
    require_native()
    from schema_sanitizer.remote_impl.providers import s3

    source = tmp_path / "s3-ordered-failure.bin"
    _sparse_file(source, 40 << 20)

    class Client:
        """Fail parts two and three in reverse completion order."""

        def __init__(self) -> None:
            """Initialize the fake provider state."""
            self.aborted = False

        async def create_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
            """Start one fake upload."""
            return {"UploadId": "ordered-failure"}

        async def upload_part(self, **kwargs: object) -> dict[str, str]:
            """Fail the later ordinal first in wall-clock time."""
            part = int(kwargs["PartNumber"])
            if part == 2:
                await asyncio.sleep(0.02)
                raise ValueError("canonical part 2 failure")
            if part == 3:
                raise ValueError("later part 3 failure")
            await asyncio.sleep(0)
            return {"ETag": f'"etag-{part}"'}

        async def complete_multipart_upload(self, **_kwargs: object) -> None:
            """Reject accidental publication."""
            raise AssertionError("failed multipart upload must not complete")

        async def abort_multipart_upload(self, **_kwargs: object) -> None:
            """Record cleanup after ordered failure selection."""
            self.aborted = True

    client = Client()

    async def open_client() -> _AsyncContext:
        """Return the fake S3 client context."""
        return _AsyncContext(client)

    monkeypatch.setattr(s3, "open_client", open_client)
    with pytest.raises(ValueError, match="canonical part 2 failure"):
        asyncio.run(
            s3.upload_file(
                str(source),
                "s3://bucket/ordered-failure.bin",
                memory_limit_bytes=256 << 20,
                threading_mode="multi",
            )
        )
    assert client.aborted is True


def test_s3_multipart_cancellation_drains_parts_before_abort(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Cancellation drains active part workers before aborting server state."""
    require_native()
    from schema_sanitizer.remote_impl.providers import s3

    source = tmp_path / "s3-cancel.bin"
    _sparse_file(source, 40 << 20)

    async def scenario() -> None:
        """Cancel one active multipart publication."""
        started = asyncio.Event()

        class Client:
            """Block active parts until the upload task is cancelled."""

            def __init__(self) -> None:
                """Initialize the fake provider state."""
                self.active = 0
                self.aborted = False
                self.completed = False

            async def create_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
                """Start one fake upload."""
                return {"UploadId": "cancel-upload"}

            async def upload_part(self, **_kwargs: object) -> dict[str, str]:
                """Wait indefinitely and expose worker cancellation."""
                self.active += 1
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    self.active -= 1
                raise AssertionError("cancelled part resumed unexpectedly")

            async def complete_multipart_upload(self, **_kwargs: object) -> None:
                """Record accidental publication."""
                self.completed = True

            async def abort_multipart_upload(self, **_kwargs: object) -> None:
                """Require all part tasks to be drained before abort."""
                assert self.active == 0
                self.aborted = True

        client = Client()

        async def open_client() -> _AsyncContext:
            """Return the fake S3 client context."""
            return _AsyncContext(client)

        monkeypatch.setattr(s3, "open_client", open_client)
        task = asyncio.create_task(
            s3.upload_file(
                str(source),
                "s3://bucket/cancel.bin",
                memory_limit_bytes=256 << 20,
                threading_mode="multi",
            )
        )
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.active == 0
        assert client.aborted is True
        assert client.completed is False

    asyncio.run(scenario())


def test_gcs_resumable_cancellation_aborts_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Cancellation during a GCS range request deletes the resumable session."""
    require_native()
    from schema_sanitizer.remote_impl.providers import gcs

    source = tmp_path / "gcs-cancel.bin"
    _sparse_file(source, 20 << 20)

    async def scenario() -> None:
        """Cancel one active GCS resumable upload."""
        started = asyncio.Event()

        class BlockingResponse(_Response):
            """Block response entry until task cancellation."""

            async def __aenter__(self) -> _Response:
                """Expose request start then wait forever."""
                started.set()
                await asyncio.Event().wait()
                return self

        class Session:
            """Create a session then block its first range request."""

            def __init__(self) -> None:
                """Initialize the fake provider state."""
                self.aborted = False

            async def __aenter__(self) -> Session:
                """Return this session."""
                return self

            async def __aexit__(self, *_exc: object) -> None:
                """Close the fake session."""

            def post(self, *_args: object, **_kwargs: object) -> _Response:
                """Create a resumable session."""
                return _Response(200, headers={"Location": "https://upload/cancel"})

            def put(self, *_args: object, **_kwargs: object) -> _Response:
                """Block the active chunk request."""
                return BlockingResponse(308)

            def delete(self, _url: str) -> _Response:
                """Record resumable-session abort."""
                self.aborted = True
                return _Response(204)

        session = Session()

        async def open_session(*_args: object, **_kwargs: object) -> Session:
            """Return the fake GCS session."""
            return session

        monkeypatch.setattr(gcs, "open_aiohttp_session", open_session)
        monkeypatch.setattr(gcs, "request_headers", lambda **_kwargs: {"Authorization": "x"})
        task = asyncio.create_task(
            gcs.upload_file(
                str(source),
                "gs://bucket/cancel.bin",
                memory_limit_bytes=256 << 20,
                threading_mode="multi",
            )
        )
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert session.aborted is True

    asyncio.run(scenario())
