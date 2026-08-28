"""Provider configuration tests without process-environment coupling."""

from __future__ import annotations

import asyncio
import tomllib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from _support.remote_harness import BoundedResponse as _Response

from schema_sanitizer.core_impl.uris import (
    location_kind,
    looks_like_supported_uri,
    remote_provider,
)
from schema_sanitizer.input_impl.directory_inputs import (
    DirectoryDiscoveryBuilder,
    split_parent_child,
)
from schema_sanitizer.input_impl.selection import looks_like_uri_string
from schema_sanitizer.sources import RemoteFile

ROOT = Path(__file__).resolve().parents[2]


def test_s3_chunked_download_reads_through_streaming_body(tmp_path) -> None:
    """Sized reads must target aiobotocore's wrapper, not its context value."""
    from schema_sanitizer.remote_impl.providers import s3
    from schema_sanitizer.remote_impl.transport import TRANSFER_CHUNK_BYTES
    from schema_sanitizer.sources import RemoteFile

    class RawResponse:
        """Model aiohttp's response, whose read method accepts no size."""

        async def read(self) -> bytes:
            raise AssertionError("chunk reads must use the streaming-body wrapper")

    class StreamingBody:
        """Model aiobotocore's wrapper around an aiohttp response."""

        def __init__(self) -> None:
            self.chunks = [b"first", b"second", b""]
            self.read_sizes: list[int] = []
            self.exited = False

        async def __aenter__(self) -> RawResponse:
            return RawResponse()

        async def __aexit__(self, _exc_type, _exc, _tb) -> None:
            self.exited = True

        async def read(self, size: int) -> bytes:
            self.read_sizes.append(size)
            return self.chunks.pop(0)

    body = StreamingBody()

    class Client:
        """Return the streaming response for one S3 object."""

        async def get_object(self, **kwargs) -> dict[str, object]:
            assert kwargs == {"Bucket": "bucket", "Key": "input/data.json"}
            return {"Body": body}

    output = tmp_path / "data.json"
    asyncio.run(
        s3.download_file_with_client(
            Client(),
            RemoteFile("s3://bucket/input/data.json", "data.json"),
            str(output),
        )
    )

    assert output.read_bytes() == b"firstsecond"
    assert body.read_sizes == [TRANSFER_CHUNK_BYTES] * 3
    assert body.exited is True


def test_s3_delegates_sdk_configuration() -> None:
    """Schema-Sanitizer does not synthesize endpoint, region, or credential options."""
    from schema_sanitizer.remote_impl.providers import s3

    assert s3.client_options() == {}


def test_azure_uses_default_sdk_credential_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    """Azure owns and closes both its service transport and SDK credential."""
    from schema_sanitizer.remote_impl.providers import azure

    captured: dict[str, object] = {}

    class FakeCredential:
        """Record explicit credential cleanup."""

        def __init__(self) -> None:
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    class FakeService:
        """Record service construction and transport cleanup."""

        def __init__(self, *, account_url: str, credential: object) -> None:
            captured.update(account_url=account_url, credential=credential)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    def fake_import(name: str) -> object:
        """Provide Azure SDK test doubles."""
        if name == "azure.identity.aio":
            return SimpleNamespace(DefaultAzureCredential=FakeCredential)
        if name == "azure.storage.blob.aio":
            return SimpleNamespace(BlobServiceClient=FakeService)
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(azure, "import_module", fake_import)
    ref = azure.parse_uri("az://account/container/input/a.json")
    owner = asyncio.run(azure.open_service(ref))
    service = owner._service
    credential = owner._credential

    assert captured == {
        "account_url": "https://account.blob.core.windows.net",
        "credential": credential,
    }
    asyncio.run(owner.close())
    asyncio.run(owner.close())
    assert service.close_calls == 1
    assert credential.close_calls == 1


def test_gcs_uses_canonical_endpoint_and_adc_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """GCS requests use the fixed JSON API endpoint and explicit ADC headers."""
    from schema_sanitizer.remote_impl.providers import gcs

    monkeypatch.setattr(gcs, "access_token", lambda: "token")

    assert gcs.api_base() == "https://storage.googleapis.com"
    assert gcs.request_headers(accept_json=True) == {
        "Authorization": "Bearer token",
        "Accept": "application/json",
    }


def test_gcs_list_directory_retries_and_paginates(monkeypatch: pytest.MonkeyPatch) -> None:
    """GCS listing derives retry policy from the operation budget and paginates."""
    from schema_sanitizer.core_impl import async_scheduler
    from schema_sanitizer.remote_impl.providers import gcs

    requests: list[dict[str, str]] = []
    responses = [
        (503, '{"error":"temporary"}'),
        (200, '{"items":[{"name":"root/a.json","size":"4"}],"nextPageToken":"next"}'),
        (200, '{"items":[{"name":"root/b.json","size":"5"}]}'),
    ]

    class FakeSession:
        """Provide a lightweight test double."""

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc, _tb):
            return False

        def get(self, _url: str, *, params: dict[str, str]):
            requests.append(dict(params))
            status, body = responses.pop(0)
            return _Response(status, body=body)

    async def fake_open_session(_headers, **_kwargs):
        """Provide a test helper implementation."""
        return FakeSession()

    async def no_sleep(_delay: float) -> None:
        """Provide a test helper implementation."""
        return None

    monkeypatch.setattr(gcs, "access_token", lambda: "token")
    monkeypatch.setattr(gcs, "open_aiohttp_session", fake_open_session)
    monkeypatch.setattr(async_scheduler.asyncio, "sleep", no_sleep)

    files = asyncio.run(gcs.list_directory("gs://bucket/root", (".json",)))

    assert [(item.name, item.size) for item in files] == [("a.json", 4), ("b.json", 5)]
    assert all(request["maxResults"] == "1000" for request in requests)
    assert "pageToken" not in requests[1]
    assert requests[2]["pageToken"] == "next"
    assert responses == []


def test_gcs_permission_errors_do_not_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Permanent GCS errors bypass the derived retry loop."""
    from schema_sanitizer.core_impl import async_scheduler
    from schema_sanitizer.remote_impl.providers import gcs

    attempts = 0

    class FakeSession:
        """Provide a lightweight test double."""

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc, _tb):
            return False

        def get(self, _url: str, *, params: dict[str, str]):
            del params
            nonlocal attempts
            attempts += 1
            return _Response(403, body="forbidden")

    async def fake_open_session(_headers, **_kwargs):
        """Provide a test helper implementation."""
        return FakeSession()

    async def fail_sleep(_delay: float) -> None:
        """Provide a test helper implementation."""
        raise AssertionError("permission errors must not back off")

    monkeypatch.setattr(gcs, "access_token", lambda: "token")
    monkeypatch.setattr(gcs, "open_aiohttp_session", fake_open_session)
    monkeypatch.setattr(async_scheduler.asyncio, "sleep", fail_sleep)

    with pytest.raises(PermissionError, match="status=403"):
        asyncio.run(gcs.list_directory("gs://bucket/root", (".json",)))
    assert attempts == 1


def test_gcs_adc_scope_supports_pipeline_uploads(monkeypatch: pytest.MonkeyPatch) -> None:
    """ADC tokens request the object read/write scope required by the pipeline."""
    from schema_sanitizer.remote_impl.providers import gcs

    captured: dict[str, object] = {}

    class Credentials:
        """Provide a lightweight test double."""

        valid = True
        token = "token"

    def default(*, scopes: list[str]):
        """Provide a test helper implementation."""
        captured["scopes"] = scopes
        return Credentials(), "project"

    def fake_import(name: str):
        """Provide a test helper implementation."""
        if name == "google.auth":
            return SimpleNamespace(default=default)
        if name == "google.auth.transport.requests":
            return SimpleNamespace(Request=object)
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(gcs, "import_module", fake_import)

    assert gcs.access_token() == "token"
    assert captured["scopes"] == ["https://www.googleapis.com/auth/devstorage.read_write"]


def test_azure_directory_downloads_reuse_one_service(monkeypatch, tmp_path: Path) -> None:
    """A staged Azure directory does not create one SDK service per child."""
    from schema_sanitizer.remote_impl import directory_downloads
    from schema_sanitizer.remote_impl.providers import azure

    opened: list[str] = []
    downloaded: list[tuple[str, str]] = []
    requested_concurrency: list[int] = []

    class FakeStream:
        async def chunks(self):
            yield b"payload"

    class FakeBlob:
        def __init__(self, container: str, blob: str) -> None:
            self.container = container
            self.blob = blob

        async def download_blob(self, *, max_concurrency: int = 1) -> FakeStream:
            requested_concurrency.append(max_concurrency)
            downloaded.append((self.container, self.blob))
            return FakeStream()

    class FakeService:
        closed = False

        def get_blob_client(self, container: str, blob: str) -> FakeBlob:
            return FakeBlob(container, blob)

        async def close(self) -> None:
            self.closed = True

    service = FakeService()

    async def fake_open_service(ref: Any) -> FakeService:
        opened.append(ref.account_url)
        return service

    monkeypatch.setattr(azure, "open_service", fake_open_service)
    files = [
        RemoteFile("https://acct.blob.core.windows.net/container/a.parquet", "a.parquet"),
        RemoteFile("https://acct.blob.core.windows.net/container/b.parquet", "b.parquet"),
    ]

    async def exercise() -> None:
        context = await directory_downloads.provider_client_for_downloads(files)
        assert context is not None
        for file in files:
            await directory_downloads.download_file_to_path(
                context, file, str(tmp_path / file.name)
            )
        await directory_downloads.close_provider_client(context)

    asyncio.run(exercise())
    assert opened == ["https://acct.blob.core.windows.net"]
    assert downloaded == [("container", "a.parquet"), ("container", "b.parquet")]
    assert requested_concurrency == [1, 1]
    assert service.closed is True
    assert (tmp_path / "a.parquet").read_bytes() == b"payload"
    assert (tmp_path / "b.parquet").read_bytes() == b"payload"


def test_bulk_discovery_reuses_unique_uri_classifications(monkeypatch) -> None:
    """Directory grouping consumes preclassified locations without reparsing."""
    import schema_sanitizer.pipeline.source_discovery as source_discovery

    source_locations = {
        "https://example.test/a.json": "http",
        "https://example.test/b.json": "http",
        "https://example.test/c.json": "http",
    }

    def fail_classification(_uri: str):
        raise AssertionError("location classification must be reused")

    monkeypatch.setattr(source_discovery, "location_kind", fail_classification)
    checked = asyncio.run(
        source_discovery._discover_directories(
            source_locations,
            extensions=("json",),
            input_format="json",
            exists_by_uri={},
            discovered_by_uri={},
        )
    )
    assert checked == set()


def test_cloud_extra_declares_direct_blocking_s3_dependency() -> None:
    """The blocking S3 backend declares Botocore instead of relying on transitivity."""
    extras = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"][
        "optional-dependencies"
    ]
    assert "botocore>=1.34" in extras["cloud"]
    assert "botocore>=1.34" in extras["all"]


def test_remote_directory_discovery_is_deterministic() -> None:
    """The shared accumulator preserves keys and sorts each completed group."""
    builder = DirectoryDiscoveryBuilder[RemoteFile].from_uris(("gs://bucket/b", "gs://bucket/a"))
    builder.add(("gs://bucket/b",), RemoteFile("gs://bucket/b/z.parquet", "z.parquet", 2))
    builder.add(("gs://bucket/b",), RemoteFile("gs://bucket/b/a.parquet", "a.parquet", 1))

    result = builder.finish()

    assert result.exists_by_uri == {"gs://bucket/b": True, "gs://bucket/a": False}
    assert [file.name for file in result.files_by_uri["gs://bucket/b"]] == [
        "a.parquet",
        "z.parquet",
    ]
    assert split_parent_child("year=2026/month=07/") == ("year=2026", "month=07")


def test_remote_uri_classification_is_canonical() -> None:
    """Public classifiers agree across paths, file URIs, and remote providers."""
    assert location_kind("C:\\data\\rows.json") == "path"
    assert location_kind("file:///tmp/rows.json") == "file"
    assert location_kind("gs://bucket/rows.json") == "gcs"
    assert remote_provider("gcs://bucket/path") == "gcs"
    assert remote_provider("s3://bucket/path") == "s3"
    assert remote_provider("abfss://container@account.dfs.core.windows.net/path") == "azure"
    assert remote_provider("https://account.blob.core.windows.net/container/path") == "azure"
    assert remote_provider("https://example.test/data.json") == "http"
    assert remote_provider("hdfs://cluster/path") is None
    assert looks_like_uri_string("file:///tmp/data.json")
    assert looks_like_supported_uri("wasbs://container@account.blob.core.windows.net/path")
    assert not looks_like_supported_uri("hdfs://cluster/path")
