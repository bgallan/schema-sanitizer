"""Provider publication contracts for bounded multipart and resumable uploads."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from _support.remote_harness import (
    AsyncValueContext as _AsyncContext,
)
from _support.remote_harness import (
    BoundedResponse as _Response,
)
from _support.remote_harness import (
    sparse_file as _sparse_file,
)

pytestmark = pytest.mark.usefixtures("require_native")


def test_remote_upload_policy_bounds_memory_and_preserves_single_worker(tmp_path: Path) -> None:
    """Provider upload buffers derive only from memory and threading mode."""
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
    from schema_sanitizer.remote_impl.providers import s3

    source = tmp_path / "s3-large.bin"
    _sparse_file(source, 40 << 20)

    class Client:
        def __init__(self) -> None:
            self.active = 0
            self.peak_active = 0
            self.completed_parts: list[dict[str, Any]] | None = None
            self.aborted = False
            self.part_one_started = asyncio.Event()
            self.part_two_completed = asyncio.Event()
            self.completion_order: list[int] = []

        async def create_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
            return {"UploadId": "upload-1"}

        async def upload_part(self, **kwargs: object) -> dict[str, str]:
            part = int(kwargs["PartNumber"])
            self.active += 1
            self.peak_active = max(self.peak_active, self.active)
            try:
                if part == 1:
                    self.part_one_started.set()
                    await asyncio.wait_for(self.part_two_completed.wait(), timeout=5)
                elif part == 2:
                    await asyncio.wait_for(self.part_one_started.wait(), timeout=5)
                self.completion_order.append(part)
                if part == 2:
                    self.part_two_completed.set()
                return {"ETag": f'"etag-{part}"'}
            finally:
                self.active -= 1

        async def complete_multipart_upload(self, **kwargs: object) -> None:
            self.completed_parts = list(kwargs["MultipartUpload"]["Parts"])

        async def abort_multipart_upload(self, **_kwargs: object) -> None:
            self.aborted = True

    client = Client()

    async def open_client() -> _AsyncContext:
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
    assert client.completion_order.index(2) < client.completion_order.index(1)
    assert client.completed_parts is not None
    assert [part["PartNumber"] for part in client.completed_parts] == [1, 2, 3, 4, 5]


def test_s3_multipart_failure_drains_workers_and_aborts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed S3 part cancels later work before aborting remote state."""
    from schema_sanitizer.remote_impl.providers import s3

    source = tmp_path / "s3-failure.bin"
    _sparse_file(source, 32 << 20)

    class Client:
        def __init__(self) -> None:
            self.active = 0
            self.aborted = False
            self.completed = False

        async def create_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
            return {"UploadId": "upload-fail"}

        async def upload_part(self, **kwargs: object) -> dict[str, str]:
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
            self.completed = True

        async def abort_multipart_upload(self, **_kwargs: object) -> None:
            assert self.active == 0
            self.aborted = True

    client = Client()

    async def open_client() -> _AsyncContext:
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
    from schema_sanitizer.remote_impl.providers import s3

    source = tmp_path / "s3-single-large.bin"
    _sparse_file(source, 24 << 20)

    class Client:
        def __init__(self) -> None:
            self.active = 0
            self.peak = 0
            self.parts = 0

        async def create_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
            return {"UploadId": "single-upload"}

        async def upload_part(self, **kwargs: object) -> dict[str, str]:
            self.active += 1
            self.peak = max(self.peak, self.active)
            try:
                self.parts += 1
                return {"ETag": f'"etag-{kwargs["PartNumber"]}"'}
            finally:
                self.active -= 1

        async def complete_multipart_upload(self, **_kwargs: object) -> None:
            pass

        async def abort_multipart_upload(self, **_kwargs: object) -> None:
            raise AssertionError("unexpected abort")

    client = Client()

    async def open_client() -> _AsyncContext:
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
    from schema_sanitizer.remote_impl.providers import gcs

    source = tmp_path / "gcs-large.bin"
    _sparse_file(source, 20 << 20)

    class Session:
        def __init__(self) -> None:
            self.committed_end = -1
            self.range_calls: list[str] = []
            self.failed_once = False
            self.aborted = False

        async def __aenter__(self) -> Session:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            pass

        def post(self, *_args: object, **_kwargs: object) -> _Response:
            return _Response(200, headers={"Location": "https://upload/session"})

        def put(self, _url: str, *, headers: dict[str, str], data: bytes) -> _Response:
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
            self.aborted = True
            return _Response(204)

    session = Session()

    async def open_session(*_args: object, **_kwargs: object) -> Session:
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
    from schema_sanitizer.remote_impl.providers import gcs

    source = tmp_path / "gcs-failure.bin"
    _sparse_file(source, 20 << 20)

    class Session:
        def __init__(self) -> None:
            self.aborted = False

        async def __aenter__(self) -> Session:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            pass

        def post(self, *_args: object, **_kwargs: object) -> _Response:
            return _Response(200, headers={"Location": "https://upload/failure"})

        def put(self, *_args: object, **_kwargs: object) -> _Response:
            return _Response(400, body=b"bad range")

        def delete(self, _url: str) -> _Response:
            self.aborted = True
            return _Response(204)

    session = Session()

    async def open_session(*_args: object, **_kwargs: object) -> Session:
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


def test_azure_upload_serializes_ungoverned_sdk_concurrency(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Azure SDK fanout stays serial; governed operations own parallelism."""
    from schema_sanitizer.remote_impl.providers import azure

    source = tmp_path / "azure-large.bin"
    _sparse_file(source, 80 << 20)

    class Blob:
        def __init__(self) -> None:
            self.kwargs: dict[str, Any] | None = None

        async def upload_blob(self, _handle: object, **kwargs: object) -> None:
            self.kwargs = dict(kwargs)

    class Service:
        def __init__(self) -> None:
            self.blob = Blob()
            self.closed = False

        def get_blob_client(self, _container: str, _blob: str) -> Blob:
            return self.blob

        async def close(self) -> None:
            self.closed = True

    service = Service()

    async def open_service(_ref: object) -> Service:
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
    assert service.blob.kwargs["max_concurrency"] == 1


def test_s3_multipart_reports_earliest_failing_part(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A fast later failure cannot overtake a slower earlier part failure."""
    from schema_sanitizer.remote_impl.providers import s3

    source = tmp_path / "s3-ordered-failure.bin"
    _sparse_file(source, 40 << 20)

    class Client:
        def __init__(self) -> None:
            self.aborted = False
            self.part_two_started = asyncio.Event()
            self.part_three_failed = asyncio.Event()

        async def create_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
            return {"UploadId": "ordered-failure"}

        async def upload_part(self, **kwargs: object) -> dict[str, str]:
            part = int(kwargs["PartNumber"])
            if part == 2:
                self.part_two_started.set()
                await asyncio.wait_for(self.part_three_failed.wait(), timeout=5)
                raise ValueError("canonical part 2 failure")
            if part == 3:
                await asyncio.wait_for(self.part_two_started.wait(), timeout=5)
                self.part_three_failed.set()
                raise ValueError("later part 3 failure")
            await asyncio.sleep(0)
            return {"ETag": f'"etag-{part}"'}

        async def complete_multipart_upload(self, **_kwargs: object) -> None:
            raise AssertionError("failed multipart upload must not complete")

        async def abort_multipart_upload(self, **_kwargs: object) -> None:
            self.aborted = True

    client = Client()

    async def open_client() -> _AsyncContext:
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
    from schema_sanitizer.remote_impl.providers import s3

    source = tmp_path / "s3-cancel.bin"
    _sparse_file(source, 40 << 20)

    async def scenario() -> None:
        started = asyncio.Event()

        class Client:
            def __init__(self) -> None:
                self.active = 0
                self.aborted = False
                self.completed = False

            async def create_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
                return {"UploadId": "cancel-upload"}

            async def upload_part(self, **_kwargs: object) -> dict[str, str]:
                self.active += 1
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    self.active -= 1
                raise AssertionError("cancelled part resumed unexpectedly")

            async def complete_multipart_upload(self, **_kwargs: object) -> None:
                self.completed = True

            async def abort_multipart_upload(self, **_kwargs: object) -> None:
                assert self.active == 0
                self.aborted = True

        client = Client()

        async def open_client() -> _AsyncContext:
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
    from schema_sanitizer.remote_impl.providers import gcs

    source = tmp_path / "gcs-cancel.bin"
    _sparse_file(source, 20 << 20)

    async def scenario() -> None:
        started = asyncio.Event()

        class BlockingResponse(_Response):
            async def __aenter__(self) -> _Response:
                started.set()
                await asyncio.Event().wait()
                return self

        class Session:
            def __init__(self) -> None:
                self.aborted = False

            async def __aenter__(self) -> Session:
                return self

            async def __aexit__(self, *_exc: object) -> None:
                pass

            def post(self, *_args: object, **_kwargs: object) -> _Response:
                return _Response(200, headers={"Location": "https://upload/cancel"})

            def put(self, *_args: object, **_kwargs: object) -> _Response:
                return BlockingResponse(308)

            def delete(self, _url: str) -> _Response:
                self.aborted = True
                return _Response(204)

        session = Session()

        async def open_session(*_args: object, **_kwargs: object) -> Session:
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
