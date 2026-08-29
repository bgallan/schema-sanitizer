"""Real-socket fault injection for the generic HTTP remote transport.

It injects truncated reads, disconnected writes, retryable statuses, cancellation,
metadata failures, and publication cleanup through real sockets.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS

from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
from schema_sanitizer.core_impl.memory_budget import memory_budget
from schema_sanitizer.remote_impl import staging
from schema_sanitizer.remote_impl.staging import StagedPath, stage_remote_single_file
from schema_sanitizer.remote_impl.transport import (
    download_http_file,
    http_file_metadata,
    upload_http_file,
)
from schema_sanitizer.sources import RemoteFile


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
                timeout=SCHEDULER_TIMEOUT_SECONDS,
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
                timeout=SCHEDULER_TIMEOUT_SECONDS,
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
                    timeout=SCHEDULER_TIMEOUT_SECONDS,
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
                timeout=SCHEDULER_TIMEOUT_SECONDS,
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
            await asyncio.wait_for(started.wait(), timeout=SCHEDULER_TIMEOUT_SECONDS)
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


def test_error_translation_preserves_types_and_extracts_resource_details() -> None:
    """Native-style failures map to stable public exceptions with diagnostics."""
    from schema_sanitizer.core_impl.error_translation import translate_core_error
    from schema_sanitizer.errors import (
        SchemaSanitizerCancelledError,
        SchemaSanitizerError,
        SchemaSanitizerInvalidArgumentError,
        SchemaSanitizerOutOfMemoryError,
        SchemaSanitizerResourceError,
    )

    existing = SchemaSanitizerInvalidArgumentError("already translated")
    assert translate_core_error(existing) is existing
    assert isinstance(
        translate_core_error(MemoryError("allocation failed")), SchemaSanitizerOutOfMemoryError
    )
    assert isinstance(
        translate_core_error(RuntimeError("OutOfMemory: ArrowArrayStream::get_next")),
        SchemaSanitizerOutOfMemoryError,
    )
    assert isinstance(
        translate_core_error(RuntimeError("operation CANCELLED")), SchemaSanitizerCancelledError
    )
    assert isinstance(
        translate_core_error(RuntimeError("invalid argument: depth")),
        SchemaSanitizerInvalidArgumentError,
    )
    assert isinstance(
        translate_core_error(RuntimeError("schema_mode='strict' requires canonical_schema")),
        SchemaSanitizerInvalidArgumentError,
    )

    translated = translate_core_error(
        RuntimeError(
            "memory_limit_bytes limit exceeded during remote_download: "
            "8192 bytes > 4096 bytes; file: gs://bucket/a.json"
        )
    )
    assert isinstance(translated, SchemaSanitizerResourceError)
    assert translated.detail == {
        "stage": "remote_download",
        "limit_name": "memory_limit_bytes",
        "actual_bytes": 8192,
        "limit_bytes": 4096,
        "file": "gs://bucket/a.json",
    }
    assert type(translate_core_error(RuntimeError("unexpected"))) is SchemaSanitizerError


def test_call_core_chains_original_failure() -> None:
    """Translated public failures retain the native exception as their cause."""
    from schema_sanitizer.core_impl.error_translation import call_core
    from schema_sanitizer.errors import SchemaSanitizerOutOfMemoryError

    original = RuntimeError("out of memory while allocating nested values")

    def fail() -> None:
        """Raise the original simulated native failure."""
        raise original

    with pytest.raises(SchemaSanitizerOutOfMemoryError) as caught:
        call_core(fail)
    assert caught.value.__cause__ is original


def test_staged_paths_and_remote_targets_cleanup_idempotently(tmp_path: Path) -> None:
    """Temporary files/directories can be closed repeatedly without leakage."""
    from schema_sanitizer.remote_impl.staging import RemoteOutputTarget, StagedPath

    file_path = tmp_path / "staged.tmp"
    file_path.write_bytes(b"data")
    staged_file = StagedPath(str(file_path))
    staged_file.close()
    staged_file.close()
    assert not file_path.exists()

    directory = tmp_path / "staged-dir"
    directory.mkdir()
    (directory / "child").write_text("x", encoding="utf-8")
    staged_dir = StagedPath(str(directory), is_dir=True)
    target = RemoteOutputTarget(
        local_path=str(directory / "output.parquet"),
        remote_uri="gs://bucket/output.parquet",
        temp=staged_dir,
    )
    target.close()
    target.close()
    assert target.temp is None
    assert not directory.exists()


def test_finalize_remote_output_cleans_temp_when_upload_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed remote upload never leaves its staged output behind."""
    from schema_sanitizer.remote_impl import staging

    output_path = tmp_path / "out.parquet"
    output_path.write_bytes(b"parquet")
    staged = staging.StagedPath(str(output_path))
    target = staging.RemoteOutputTarget(
        local_path=staged.path,
        remote_uri="gs://bucket/out.parquet",
        temp=staged,
    )

    def fail_upload(*_args: object, **_kwargs: object) -> None:
        """Simulate a strict blocking publication failure."""
        raise RuntimeError("upload failed")

    monkeypatch.setattr(staging.sync_backend, "upload_file", fail_upload)
    with pytest.raises(RuntimeError, match="upload failed"):
        staging.finalize_output_target(target)
    assert target.temp is None
    assert not Path(staged.path).exists()
