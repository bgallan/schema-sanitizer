"""Regression coverage for bounded directory and remote control metadata."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
from schema_sanitizer.errors import SchemaSanitizerResourceError
from schema_sanitizer.input_impl.directory_inputs import (
    DirectoryMetadataBudget,
    current_directory_metadata_budget,
    folder_files,
)
from schema_sanitizer.remote_impl import sync_http
from schema_sanitizer.remote_impl.transport import read_bounded_response_bytes
from schema_sanitizer.sources import RemoteFile


def _large_remote_file() -> RemoteFile:
    """Return one remote-file record large enough to pressure metadata limits."""
    name = "x" * 40_000 + ".json"
    return RemoteFile(f"s3://bucket/{name}", name, 1)


def test_directory_metadata_budget_is_shared_by_single_operation() -> None:
    """Verify repeated provider calls cannot each consume a fresh allowance."""
    file = _large_remote_file()
    with OperationExecutionContext(
        threading_mode="single",
        memory_limit_bytes=1 << 20,
    ) as operation:

        def charge() -> None:
            """Charge one remote-file record through the active metadata budget."""
            current_directory_metadata_budget(1 << 20).charge_file(file)

        operation.run_remote_sync(charge)
        with pytest.raises(SchemaSanitizerResourceError) as excinfo:
            operation.run_remote_sync(charge)

    assert excinfo.value.detail is not None
    assert excinfo.value.detail["stage"] == "directory_metadata"
    assert excinfo.value.detail["limit_name"] == "directory_metadata_bytes"


def test_folder_listing_rejects_metadata_growth_before_retaining_every_file(
    tmp_path: Path,
) -> None:
    """Verify one huge local listing is bounded even when files are empty."""
    folder = tmp_path / "many"
    folder.mkdir()
    for index in range(600):
        (folder / f"row-{index:04d}.json").touch()

    with pytest.raises(SchemaSanitizerResourceError) as excinfo:
        folder_files(
            folder,
            suffix="json",
            reader_name="metadata budget test",
            memory_limit_bytes=1 << 20,
        )

    assert excinfo.value.detail is not None
    assert excinfo.value.detail["stage"] == "directory_metadata"


def test_small_user_limit_keeps_fixed_directory_runtime_overhead() -> None:
    """Verify one normal path can reach the parser under tiny test budgets."""
    budget = DirectoryMetadataBudget(128)
    budget.charge_file(RemoteFile("s3://bucket/row.json", "row.json", 1))
    assert budget.used_bytes > 0
    assert budget.limit_bytes >= 64 * 1024


class _SyncResponse:
    """Provide a minimal blocking HTTP response test double."""

    status = 200

    def __init__(self, payload: bytes) -> None:
        """Initialize the helper state."""
        self.payload = payload
        self.requested: int | None = None

    def read(self, size: int = -1) -> bytes:
        """Implement the minimal response protocol required by the test."""
        self.requested = size
        return self.payload if size < 0 else self.payload[:size]

    def getheaders(self) -> list[tuple[str, str]]:
        """Implement the minimal response protocol required by the test."""
        return []


class _Connection:
    """Provide a minimal closeable HTTP connection test double."""

    def __init__(self) -> None:
        """Initialize the helper state."""
        self.closed = False

    def close(self) -> None:
        """Close the helper resource exactly once."""
        self.closed = True


def test_sync_control_response_is_bounded_before_full_materialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify blocking HTTP control bodies read at most limit plus one byte."""
    response = _SyncResponse(b"x" * 128)
    connection = _Connection()
    monkeypatch.setattr(
        sync_http,
        "_request_once",
        lambda *_args, **_kwargs: (connection, response),
    )

    with pytest.raises(SchemaSanitizerResourceError) as excinfo:
        sync_http.request_bytes(
            "GET",
            "https://example.invalid/control",
            timeout=1.0,
            max_response_bytes=32,
        )

    assert response.requested == 33
    assert connection.closed
    assert excinfo.value.detail is not None
    assert excinfo.value.detail["stage"] == "remote_control_response"


def test_async_control_response_is_bounded_before_full_materialization() -> None:
    """Verify aiohttp control bodies read at most limit plus one byte."""

    class Content:
        """Provide an asynchronous bounded-body reader test double."""

        requested: int | None = None

        async def read(self, size: int = -1) -> bytes:
            """Implement the minimal response protocol required by the test."""
            self.requested = size
            return b"x" * size

    class Response:
        """Provide the response surface required by the current regression."""

        content = Content()

    async def exercise() -> None:
        response = Response()
        with pytest.raises(SchemaSanitizerResourceError) as excinfo:
            await read_bounded_response_bytes(
                response,
                maximum_bytes=32,
                stage="remote_control_response",
            )

        assert response.content.requested == 33
        assert excinfo.value.detail is not None
        assert excinfo.value.detail["limit_bytes"] == 32

    asyncio.run(exercise())
