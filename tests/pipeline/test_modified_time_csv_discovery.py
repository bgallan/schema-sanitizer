"""GCS metadata and generation-safe discovery contracts for CSV window ingestion."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from schema_sanitizer.input_impl.directory_inputs import (
    DirectoryDiscoveryBuilder,
)
from schema_sanitizer.remote_impl import staging
from schema_sanitizer.remote_impl.providers import gcs, gcs_sync
from schema_sanitizer.remote_impl.providers.gcs_objects import remote_file_from_metadata
from schema_sanitizer.remote_impl.sync_http import SyncHttpResult
from schema_sanitizer.sources import RemoteFile


def _gcs_item(name: str, *, generation: str, updated: str) -> dict[str, str]:
    """Return one complete fake GCS JSON API item."""
    return {
        "name": name,
        "size": "17",
        "updated": updated,
        "timeCreated": "2026-07-31T23:59:58.123456Z",
        "generation": generation,
        "metageneration": "3",
        "etag": f"etag-{generation}",
        "crc32c": f"crc-{generation}",
    }


def test_remote_file_keeps_non_gcs_callers_source_compatible() -> None:
    """Optional metadata must not disturb existing three-argument providers."""
    file = RemoteFile("s3://bucket/a.csv", "a.csv", 7)

    assert file.content_identity == ("s3://bucket/a.csv", None)
    assert file.updated is None
    assert file.generation is None


def test_gcs_metadata_parser_normalizes_rfc3339_offsets_to_utc() -> None:
    """GCS timestamps are aware UTC datetimes and all identity fields survive."""
    file = remote_file_from_metadata(
        "bucket",
        _gcs_item(
            "events/a.csv",
            generation="42",
            updated="2026-08-01T02:30:00.250000+02:00",
        ),
        display_name="a.csv",
    )

    assert file.uri == "gs://bucket/events/a.csv"
    assert file.updated == datetime(2026, 8, 1, 0, 30, 0, 250000, tzinfo=UTC)
    assert file.time_created == datetime(2026, 7, 31, 23, 59, 58, 123456, tzinfo=UTC)
    assert file.content_identity == (file.uri, "42")
    assert file.metageneration == "3"
    assert file.etag == "etag-42"
    assert file.crc32c == "crc-42"


@pytest.mark.parametrize("value", ["2026-08-01T00:00:00", "not-a-time", 123])
def test_gcs_metadata_parser_rejects_invalid_or_naive_timestamps(value: object) -> None:
    """Discovery must never retain ambiguous modification times."""
    item = _gcs_item("events/a.csv", generation="42", updated="2026-08-01T00:00:00Z")
    item["updated"] = value  # type: ignore[assignment]

    with pytest.raises(ValueError, match="updated"):
        remote_file_from_metadata("bucket", item)


def test_async_bucket_root_listing_requests_metadata_and_sorts_by_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bucket-root pagination uses an empty prefix and one canonical ordering."""
    captured: list[dict[str, str]] = []
    pages = [
        {
            "items": [
                _gcs_item("z.csv", generation="9", updated="2026-08-02T00:00:00Z"),
            ],
            "nextPageToken": "next",
        },
        {
            "items": [
                _gcs_item("a.csv", generation="11", updated="2026-08-01T00:00:00Z"),
                _gcs_item("nested/ignored.csv", generation="1", updated="2026-08-01T00:00:00Z"),
            ]
        },
    ]

    class Response:
        """Minimal asynchronous HTTP response double."""

        status = 200

        def __init__(self, payload: dict[str, object]) -> None:
            self._body = json.dumps(payload).encode()
            self._offset = 0
            self.content = self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc: object) -> bool:
            return False

        async def read(self, size: int) -> bytes:
            end = min(len(self._body), self._offset + size)
            chunk = self._body[self._offset : end]
            self._offset = end
            return chunk

        def at_eof(self) -> bool:
            return self._offset == len(self._body)

    class Session:
        """Minimal asynchronous client-session double."""

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc: object) -> bool:
            return False

        def get(self, _url: str, *, params: dict[str, str]):
            captured.append(dict(params))
            return Response(pages.pop(0))

    async def open_session(*_args: object, **_kwargs: object) -> Session:
        """Return the local asynchronous session double."""
        return Session()

    monkeypatch.setattr(gcs, "access_token", lambda: "token")
    monkeypatch.setattr(gcs, "open_aiohttp_session", open_session)

    files = asyncio.run(gcs.list_directory("gs://bucket", (".csv",)))

    assert [(file.uri, file.generation) for file in files] == [
        ("gs://bucket/a.csv", "11"),
        ("gs://bucket/z.csv", "9"),
    ]
    assert captured[0]["prefix"] == ""
    assert captured[0]["delimiter"] == "/"
    assert "updated" in captured[0]["fields"]
    assert "generation" in captured[0]["fields"]
    assert captured[1]["pageToken"] == "next"


def test_sync_metadata_requests_all_gcs_identity_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """Single-thread metadata discovery obtains the same immutable identity."""
    captured_url = ""

    def request_bytes(_method: str, url: str, **_kwargs: object) -> SyncHttpResult:
        """Capture the metadata request and return one object payload."""
        nonlocal captured_url
        captured_url = url
        return SyncHttpResult(
            200,
            {},
            json.dumps(
                _gcs_item(
                    "events/a.csv",
                    generation="99",
                    updated="2026-08-01T10:15:30Z",
                )
            ).encode(),
        )

    monkeypatch.setattr(gcs, "access_token", lambda: "token")
    monkeypatch.setattr(gcs_sync, "request_bytes", request_bytes)

    file = gcs_sync.file_metadata("gcs://bucket/events/a.csv")

    query = parse_qs(urlsplit(captured_url).query)
    assert file is not None
    assert file.uri == "gcs://bucket/events/a.csv"
    assert file.generation == "99"
    assert "updated" in query["fields"][0]
    assert "crc32c" in query["fields"][0]


def test_sync_download_pins_generation_and_uses_precondition(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The exact listed generation is selected and guarded during staging."""
    captured_url = ""

    def download(url: str, _local_path: str, **_kwargs: object) -> None:
        """Capture the generation-pinned download URL."""
        nonlocal captured_url
        captured_url = url

    monkeypatch.setattr(gcs_sync, "download_to_file", download)
    monkeypatch.setattr(gcs, "request_headers", lambda **_kwargs: {})

    selected = RemoteFile(
        "gs://bucket/events/a.csv",
        "a.csv",
        17,
        generation="1700000000000000",
    )
    gcs_sync.download_file(selected, str(tmp_path / "a.csv"))

    query = parse_qs(urlsplit(captured_url).query)
    assert query == {
        "alt": ["media"],
        "generation": ["1700000000000000"],
        "ifGenerationMatch": ["1700000000000000"],
    }


def test_discovery_orders_repeated_object_names_by_generation() -> None:
    """A repeated object URI has a stable version-aware manifest order."""
    root = "gs://bucket"
    builder = DirectoryDiscoveryBuilder[RemoteFile].from_uris((root,))
    builder.add(
        (root,),
        RemoteFile("gs://bucket/a.csv", "a.csv", 1, generation="9"),
    )
    builder.add(
        (root,),
        RemoteFile("gs://bucket/a.csv", "a.csv", 1, generation="2"),
    )

    discovery = builder.finish()

    assert [file.generation for file in discovery.files_by_uri[root]] == ["2", "9"]


def test_single_file_staging_downloads_the_discovered_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Metadata discovery and download share one immutable RemoteFile value."""
    selected = RemoteFile(
        "gs://bucket/events/a.csv",
        "a.csv",
        1,
        generation="123",
    )
    downloaded: list[RemoteFile | str] = []

    monkeypatch.setattr(
        staging.sync_backend,
        "remote_file_metadata",
        lambda *_args, **_kwargs: selected,
    )

    def download(source: RemoteFile | str, local_path: str, **_kwargs: object) -> None:
        """Record the staged source and write a minimal local payload."""
        downloaded.append(source)
        Path(local_path).write_bytes(b"x")

    monkeypatch.setattr(staging.sync_backend, "download_single_file", download)

    staged = staging.stage_remote_single_file(
        selected.uri,
        memory_limit_bytes=None,
        threading_mode="single",
    )
    try:
        assert downloaded == [selected]
    finally:
        staged.close()
