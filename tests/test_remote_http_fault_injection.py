"""Real-socket fault injection for the generic HTTP remote transport."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
from schema_sanitizer.core_impl.memory_budget import memory_budget
from schema_sanitizer.input_impl.directory_inputs import RemoteFile
from schema_sanitizer.remote_impl import staging
from schema_sanitizer.remote_impl.staging import StagedPath, stage_remote_single_file
from schema_sanitizer.remote_impl.transport import (
    download_http_file,
    http_file_metadata,
    upload_http_file,
)


@asynccontextmanager
async def _http_server(
    routes: list[tuple[str, str, Callable[..., Any]]],
) -> AsyncIterator[str]:
    """Run an aiohttp provider emulator on a real loopback TCP socket."""
    from aiohttp import web

    app = web.Application()
    for method, path, handler in routes:
        app.router.add_route(method, path, handler)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    server = site._server  # noqa: SLF001 - test-only access to the selected ephemeral port.
    assert server is not None and server.sockets
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


def _disable_retry_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep fault-injection retries deterministic and fast."""
    monkeypatch.setattr(
        "schema_sanitizer.core_impl.async_scheduler.retry_delay",
        lambda _attempt: 0.0,
    )


def test_truncated_get_retries_and_replaces_partial_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A broken response body must be retried from an empty destination."""
    _disable_retry_delay(monkeypatch)
    payload = (b"schema-sanitizer-http-download-" * 8192) + b"complete"
    attempts = 0

    async def scenario() -> None:
        """Run the truncated-download provider scenario."""
        from aiohttp import web

        async def download(request: web.Request) -> web.StreamResponse:
            """Truncate the first response and complete the retry."""
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                response = web.StreamResponse(
                    status=200,
                    headers={"Content-Length": str(len(payload))},
                )
                await response.prepare(request)
                await response.write(payload[:4096])
                assert request.transport is not None
                request.transport.abort()
                return response
            return web.Response(body=payload)

        async with _http_server([("GET", "/object", download)]) as base_url:
            target = tmp_path / "download.bin"
            await asyncio.wait_for(
                download_http_file(f"{base_url}/object", str(target)),
                timeout=5,
            )
            assert target.read_bytes() == payload

    asyncio.run(scenario())
    assert attempts == 2


def test_put_disconnect_retries_with_complete_body_from_byte_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every idempotent PUT retry must reopen and resend the complete spool."""
    _disable_retry_delay(monkeypatch)
    payload = (b"schema-sanitizer-http-upload-" * 4096) + b"complete"
    source = tmp_path / "upload.bin"
    source.write_bytes(payload)
    received: list[bytes] = []

    async def scenario() -> None:
        """Run the disconnected-publication provider scenario."""
        from aiohttp import web

        async def upload(request: web.Request) -> web.StreamResponse:
            """Disconnect once after consuming the request body."""
            received.append(await request.read())
            if len(received) == 1:
                assert request.transport is not None
                request.transport.abort()
                return web.Response(status=204)
            return web.Response(status=204)

        async with _http_server([("PUT", "/object", upload)]) as base_url:
            await asyncio.wait_for(
                upload_http_file(str(source), f"{base_url}/object"),
                timeout=5,
            )

    asyncio.run(scenario())
    assert received == [payload, payload]


def test_retryable_status_exhaustion_is_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Persistent provider failures must stop at the derived retry ceiling."""
    _disable_retry_delay(monkeypatch)
    source = tmp_path / "upload.bin"
    source.write_bytes(b"bounded-retry")
    attempts = 0

    async def scenario() -> None:
        """Run a provider that remains transiently unavailable."""
        from aiohttp import web

        async def unavailable(request: web.Request) -> web.Response:
            """Return one retryable service failure."""
            nonlocal attempts
            attempts += 1
            await request.read()
            return web.Response(status=503, text="try later")

        async with _http_server([("PUT", "/object", unavailable)]) as base_url:
            with pytest.raises(RuntimeError, match="503"):
                await asyncio.wait_for(
                    upload_http_file(str(source), f"{base_url}/object"),
                    timeout=5,
                )

    asyncio.run(scenario())
    assert attempts == memory_budget(None).async_retries + 1


def test_nonretryable_upload_status_fails_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Permanent client errors must not consume the transient retry budget."""
    _disable_retry_delay(monkeypatch)
    source = tmp_path / "upload.bin"
    source.write_bytes(b"invalid-request")
    attempts = 0

    async def scenario() -> None:
        """Run a provider that rejects the request permanently."""
        from aiohttp import web

        async def rejected(request: web.Request) -> web.Response:
            """Return one nonretryable client error."""
            nonlocal attempts
            attempts += 1
            await request.read()
            return web.Response(status=400, text="invalid destination")

        async with _http_server([("PUT", "/object", rejected)]) as base_url:
            with pytest.raises(RuntimeError, match="400"):
                await upload_http_file(str(source), f"{base_url}/object")

    asyncio.run(scenario())
    assert attempts == 1


def test_head_transient_failure_retries_before_returning_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Metadata discovery must recover from transient provider status codes."""
    _disable_retry_delay(monkeypatch)
    attempts = 0

    async def scenario() -> None:
        """Run transient metadata discovery against the emulator."""
        from aiohttp import web

        async def metadata(_request: web.Request) -> web.Response:
            """Fail one HEAD request before returning metadata."""
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return web.Response(status=503)
            return web.Response(status=200, headers={"Content-Length": "123"})

        async with _http_server([("HEAD", "/source.parquet", metadata)]) as base_url:
            result = await asyncio.wait_for(
                http_file_metadata(f"{base_url}/source.parquet"),
                timeout=5,
            )
            assert result == RemoteFile(
                f"{base_url}/source.parquet",
                "source.parquet",
                123,
            )

    asyncio.run(scenario())
    assert attempts == 2


def test_cancelled_get_is_not_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Task cancellation must propagate immediately without starting a retry."""
    _disable_retry_delay(monkeypatch)
    attempts = 0

    async def scenario() -> None:
        """Cancel one delayed download after the request starts."""
        from aiohttp import web

        started = asyncio.Event()
        release = asyncio.Event()

        async def delayed(_request: web.Request) -> web.Response:
            """Hold the response until the client has been cancelled."""
            nonlocal attempts
            attempts += 1
            started.set()
            await release.wait()
            return web.Response(body=b"too late")

        async with _http_server([("GET", "/object", delayed)]) as base_url:
            target = tmp_path / "cancelled.bin"
            task = asyncio.create_task(download_http_file(f"{base_url}/object", str(target)))
            await asyncio.wait_for(started.wait(), timeout=2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            await asyncio.sleep(0)
            assert not target.exists()

    asyncio.run(scenario())
    assert attempts == 1


def test_single_file_staging_releases_partial_file_and_permit_on_base_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fatal interruption paths must clean both disk state and permits."""

    class FatalTransfer(BaseException):
        """Synthetic non-Exception interruption used to exercise cleanup."""

    target = tmp_path / "interrupted.parquet"

    def metadata(*_args: Any, **_kwargs: Any) -> RemoteFile:
        """Return deterministic source metadata for staging."""
        return RemoteFile("http://provider/source.parquet", "source.parquet", 4096)

    def interrupted_download(
        _uri: str,
        local_path: str,
        **_kwargs: Any,
    ) -> None:
        """Write a partial file and emulate a fatal interruption."""
        Path(local_path).write_bytes(b"partial")
        raise FatalTransfer

    def create_temp_file(*, suffix: str, storage_lease: Any = None) -> StagedPath:
        """Create the tracked staging path inside the pytest directory."""
        assert suffix == ".parquet"
        target.touch()
        return StagedPath(str(target), storage_lease=storage_lease)

    monkeypatch.setattr(staging.sync_backend, "remote_file_metadata", metadata)
    monkeypatch.setattr(staging.sync_backend, "download_single_file", interrupted_download)
    monkeypatch.setattr(staging, "create_temp_file_path", create_temp_file)

    with OperationExecutionContext(
        threading_mode="single",
        memory_limit_bytes=64 << 20,
    ) as operation:
        with pytest.raises(FatalTransfer):
            stage_remote_single_file(
                "http://provider/source.parquet",
                memory_limit_bytes=64 << 20,
                operation_context=operation,
            )
        snapshot = operation.temporary_storage.snapshot()
        assert snapshot.reserved_bytes == 0
        assert snapshot.active_leases == 0
    assert not target.exists()
