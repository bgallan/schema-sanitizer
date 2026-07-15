"""Remote input staging tests, part two."""

from __future__ import annotations

import json

from input_contract_shared import *  # noqa: F403

# Split from test_input_remote_staging.py: test_remote_gcs_directory_listing_reads_all_pages, test_remote_gcs_bulk_directory_discovery_groups_parent_prefixes, test_remote_s3_bulk_directory_discovery_groups_parent_prefixes, ...


def test_remote_gcs_directory_listing_reads_all_pages(monkeypatch) -> None:
    """Verify GCS remote directory listing follows nextPageToken pages."""
    from schema_sanitizer.input_impl.directory_inputs import RemoteFile
    from schema_sanitizer.remote_impl.providers import gcs as gcs_listing
    from schema_sanitizer.remote_impl.transport import run_sync

    class FakeResponse:
        """Minimal aiohttp-like response for one GCS list page."""

        status = 200

        def __init__(self, payload: dict[str, object]):
            """Store one JSON response payload."""
            self._payload = payload

        async def __aenter__(self):
            """Return this fake response."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Close this fake response."""
            return None

        async def text(self) -> str:
            """Return the JSON response body."""
            return json.dumps(self._payload)

    class FakeSession:
        """Minimal aiohttp-like session with paginated GCS responses."""

        def __init__(self):
            """Seed two pages where only page two has matching files."""
            self.params: list[dict[str, str]] = []
            self.pages = [
                {"items": [{"name": "events/ignore.txt"}], "nextPageToken": "page-2"},
                {"items": [{"name": "events/row.json", "size": "7"}]},
            ]

        async def __aenter__(self):
            """Return this fake session."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Close this fake session."""
            return None

        def get(self, _url, *, params):
            """Return the next fake page and record request parameters."""
            self.params.append(dict(params))
            return FakeResponse(self.pages.pop(0))

    fake_session = FakeSession()

    async def fake_session_factory(headers, *, memory_limit_bytes=None):
        """Return the fake GCS session."""
        assert headers["Authorization"] == "Bearer token"
        return fake_session

    monkeypatch.setattr(gcs_listing, "access_token", lambda: "token")
    monkeypatch.setattr(gcs_listing, "open_aiohttp_session", fake_session_factory)

    files = run_sync(gcs_listing.list_directory("gs://bucket/events/", (".json",)))

    assert files == [RemoteFile("gs://bucket/events/row.json", "row.json", 7)]
    assert fake_session.params[0]["prefix"] == "events/"
    assert "pageToken" not in fake_session.params[0]
    assert fake_session.params[1]["pageToken"] == "page-2"


def test_remote_gcs_bulk_directory_discovery_groups_parent_prefixes(monkeypatch) -> None:
    """Verify GCS source discovery can check sibling partition directories in one listing."""
    from schema_sanitizer.remote_impl.providers import gcs as gcs_bulk_discovery
    from schema_sanitizer.remote_impl.transport import run_sync

    class FakeResponse:
        """Minimal aiohttp-like response for one GCS list page."""

        status = 200

        def __init__(self, payload: dict[str, object]):
            """Store one JSON response payload."""
            self._payload = payload

        async def __aenter__(self):
            """Return this fake response."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Close this fake response."""
            return None

        async def text(self) -> str:
            """Return the JSON response body."""
            return json.dumps(self._payload)

    class FakeSession:
        """Minimal aiohttp-like session with one parent-prefix listing."""

        def __init__(self):
            """Seed one page containing two requested child directories."""
            self.params: list[dict[str, str]] = []

        async def __aenter__(self):
            """Return this fake session."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Close this fake session."""
            return None

        def get(self, _url, *, params):
            """Return one fake page and record request parameters."""
            self.params.append(dict(params))
            return FakeResponse(
                {
                    "items": [
                        {"name": "events/date=2026-01-01/hour=00/a.json"},
                        {"name": "events/date=2026-01-01/hour=00/nested/ignored.json"},
                        {"name": "events/date=2026-01-01/hour=01/b.txt"},
                        {"name": "events/date=2026-01-01/hour=02/c.json"},
                    ]
                }
            )

    fake_session = FakeSession()

    async def fake_session_factory(headers, *, memory_limit_bytes=None):
        """Return the fake GCS session."""
        assert headers["Authorization"] == "Bearer token"
        return fake_session

    monkeypatch.setattr(gcs_bulk_discovery, "access_token", lambda: "token")
    monkeypatch.setattr(gcs_bulk_discovery, "open_aiohttp_session", fake_session_factory)

    result = run_sync(
        gcs_bulk_discovery.directories_containing_files(
            [
                "gs://bucket/events/date=2026-01-01/hour=00",
                "gs://bucket/events/date=2026-01-01/hour=01",
                "gs://bucket/events/date=2026-01-01/hour=02",
            ],
            (".json",),
        )
    )

    assert result.exists_by_uri == {
        "gs://bucket/events/date=2026-01-01/hour=00": True,
        "gs://bucket/events/date=2026-01-01/hour=01": False,
        "gs://bucket/events/date=2026-01-01/hour=02": True,
    }
    assert len(fake_session.params) == 1
    assert fake_session.params[0]["prefix"] == "events/date=2026-01-01/"


def test_remote_s3_bulk_directory_discovery_groups_parent_prefixes(monkeypatch) -> None:
    """Verify S3 source discovery can check sibling partition directories in one listing."""
    from schema_sanitizer.remote_impl.providers import s3 as s3_discovery
    from schema_sanitizer.remote_impl.transport import run_sync

    class FakeS3Client:
        """Minimal async S3 client with one parent-prefix listing."""

        def __init__(self):
            """Initialize captured calls."""
            self.calls: list[dict[str, object]] = []

        async def __aenter__(self):
            """Return this fake client."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Close this fake client."""
            return None

        async def list_objects_v2(self, **kwargs):
            """Return one fake page and record request parameters."""
            self.calls.append(dict(kwargs))
            return {
                "Contents": [
                    {"Key": "events/date=2026-01-01/hour=00/a.json"},
                    {"Key": "events/date=2026-01-01/hour=00/nested/ignored.json"},
                    {"Key": "events/date=2026-01-01/hour=01/b.txt"},
                    {"Key": "events/date=2026-01-01/hour=02/c.json"},
                ],
                "IsTruncated": False,
            }

    fake_client = FakeS3Client()

    async def fake_s3_client():
        """Return the fake S3 client."""
        return fake_client

    monkeypatch.setattr(s3_discovery, "open_client", fake_s3_client)

    result = run_sync(
        s3_discovery.directories_containing_files(
            [
                "s3://bucket/events/date=2026-01-01/hour=00",
                "s3://bucket/events/date=2026-01-01/hour=01",
                "s3://bucket/events/date=2026-01-01/hour=02",
            ],
            (".json",),
        )
    )

    assert result.exists_by_uri == {
        "s3://bucket/events/date=2026-01-01/hour=00": True,
        "s3://bucket/events/date=2026-01-01/hour=01": False,
        "s3://bucket/events/date=2026-01-01/hour=02": True,
    }
    assert fake_client.calls == [
        {
            "Bucket": "bucket",
            "Prefix": "events/date=2026-01-01/",
            "MaxKeys": 1000,
        }
    ]


def test_remote_azure_bulk_directory_discovery_groups_parent_prefixes(monkeypatch) -> None:
    """Verify Azure source discovery can check sibling partition directories in one listing."""
    from types import SimpleNamespace

    from schema_sanitizer.remote_impl.providers import azure as azure_discovery
    from schema_sanitizer.remote_impl.transport import run_sync

    class FakeContainer:
        """Minimal async Azure container client."""

        def __init__(self):
            """Initialize captured prefixes."""
            self.prefixes: list[str] = []

        async def list_blobs(self, *, name_starts_with):
            """Yield fake blobs and record request prefix."""
            self.prefixes.append(name_starts_with)
            for name in [
                "events/date=2026-01-01/hour=00/a.json",
                "events/date=2026-01-01/hour=00/nested/ignored.json",
                "events/date=2026-01-01/hour=01/b.txt",
                "events/date=2026-01-01/hour=02/c.json",
            ]:
                yield SimpleNamespace(name=name)

    class FakeService:
        """Minimal async Azure blob service."""

        def __init__(self):
            """Initialize fake container and close flag."""
            self.container = FakeContainer()
            self.closed = False

        def get_container_client(self, container_name):
            """Return the fake container."""
            assert container_name == "container"
            return self.container

        async def close(self):
            """Mark the fake service closed."""
            self.closed = True

    fake_service = FakeService()

    async def fake_azure_service(ref):
        """Return the fake Azure service."""
        assert ref.account_url == "https://account.blob.core.windows.net"
        return fake_service

    monkeypatch.setattr(azure_discovery, "open_service", fake_azure_service)

    result = run_sync(
        azure_discovery.directories_containing_files(
            [
                "az://account/container/events/date=2026-01-01/hour=00",
                "az://account/container/events/date=2026-01-01/hour=01",
                "az://account/container/events/date=2026-01-01/hour=02",
            ],
            (".json",),
        )
    )

    assert result.exists_by_uri == {
        "az://account/container/events/date=2026-01-01/hour=00": True,
        "az://account/container/events/date=2026-01-01/hour=01": False,
        "az://account/container/events/date=2026-01-01/hour=02": True,
    }
    assert fake_service.container.prefixes == ["events/date=2026-01-01/"]
    assert fake_service.closed is True


def test_remote_s3_directory_listing_reads_all_pages(monkeypatch) -> None:
    """Verify S3 remote directory listing follows continuation tokens."""
    from schema_sanitizer.input_impl.directory_inputs import RemoteFile
    from schema_sanitizer.remote_impl.providers import s3 as s3_discovery
    from schema_sanitizer.remote_impl.transport import run_sync

    class FakeS3Client:
        """Minimal async S3 client with paginated list_objects_v2 responses."""

        def __init__(self):
            """Seed two pages where only page two has matching files."""
            self.calls: list[dict[str, object]] = []

        async def __aenter__(self):
            """Return this fake client."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Close this fake client."""
            return None

        async def list_objects_v2(self, **kwargs):
            """Return one fake S3 list page."""
            self.calls.append(kwargs)
            if "ContinuationToken" not in kwargs:
                return {
                    "IsTruncated": True,
                    "NextContinuationToken": "page-2",
                    "Contents": [{"Key": "events/ignore.txt", "Size": 4}],
                }
            assert kwargs["ContinuationToken"] == "page-2"
            return {
                "IsTruncated": False,
                "Contents": [{"Key": "events/row.json", "Size": 7}],
            }

    fake_client = FakeS3Client()

    async def fake_s3_client():
        """Return the fake S3 client context manager."""
        return fake_client

    monkeypatch.setattr(s3_discovery, "open_client", fake_s3_client)

    files = run_sync(s3_discovery.list_files("s3://bucket/events/", (".json",)))

    assert files == [RemoteFile("s3://bucket/events/row.json", "row.json", 7)]
    assert fake_client.calls[0]["Prefix"] == "events/"
    assert "ContinuationToken" not in fake_client.calls[0]
    assert fake_client.calls[1]["ContinuationToken"] == "page-2"
