"""Provider configuration tests without process-environment coupling."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest


def test_s3_chunked_download_reads_through_streaming_body(tmp_path) -> None:
    """Sized reads must target aiobotocore's wrapper, not its context value."""
    from schema_sanitizer.input_impl.directory_inputs import RemoteFile
    from schema_sanitizer.remote_impl.providers import s3
    from schema_sanitizer.remote_impl.transport import TRANSFER_CHUNK_BYTES

    class RawResponse:
        """Model aiohttp's response, whose read method accepts no size."""

        async def read(self) -> bytes:
            """Fail if the context-manager return value is used for streaming."""
            raise AssertionError("chunk reads must use the streaming-body wrapper")

    class StreamingBody:
        """Model aiobotocore's wrapper around an aiohttp response."""

        def __init__(self) -> None:
            """Create deterministic chunks and record requested sizes."""
            self.chunks = [b"first", b"second", b""]
            self.read_sizes: list[int] = []
            self.exited = False

        async def __aenter__(self) -> RawResponse:
            """Return the wrapped response, as aiobotocore does."""
            return RawResponse()

        async def __aexit__(self, _exc_type, _exc, _tb) -> None:
            """Record release of the response body."""
            self.exited = True

        async def read(self, size: int) -> bytes:
            """Return one chunk through the sized wrapper API."""
            self.read_sizes.append(size)
            return self.chunks.pop(0)

    body = StreamingBody()

    class Client:
        """Return the streaming response for one S3 object."""

        async def get_object(self, **kwargs) -> dict[str, object]:
            """Validate the parsed S3 reference."""
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
            """Initialize close accounting."""
            self.close_calls = 0

        async def close(self) -> None:
            """Record one credential close."""
            self.close_calls += 1

    class FakeService:
        """Record service construction and transport cleanup."""

        def __init__(self, *, account_url: str, credential: object) -> None:
            """Capture the SDK constructor arguments."""
            captured.update(account_url=account_url, credential=credential)
            self.close_calls = 0

        async def close(self) -> None:
            """Record one service close."""
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

    class FakeResponse:
        """Provide a lightweight test double."""

        def __init__(self, status: int, body: str) -> None:
            """Implement the test-double protocol method."""
            self.status = status
            self._body = body

        async def __aenter__(self):
            """Implement the test-double protocol method."""
            return self

        async def __aexit__(self, _exc_type, _exc, _tb):
            """Implement the test-double protocol method."""
            return False

        async def text(self) -> str:
            """Provide a test helper implementation."""
            return self._body

    class FakeSession:
        """Provide a lightweight test double."""

        async def __aenter__(self):
            """Implement the test-double protocol method."""
            return self

        async def __aexit__(self, _exc_type, _exc, _tb):
            """Implement the test-double protocol method."""
            return False

        def get(self, _url: str, *, params: dict[str, str]):
            """Provide a test helper implementation."""
            requests.append(dict(params))
            status, body = responses.pop(0)
            return FakeResponse(status, body)

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

    class FakeResponse:
        """Provide a lightweight test double."""

        status = 403

        async def __aenter__(self):
            """Implement the test-double protocol method."""
            return self

        async def __aexit__(self, _exc_type, _exc, _tb):
            """Implement the test-double protocol method."""
            return False

        async def text(self) -> str:
            """Provide a test helper implementation."""
            return "forbidden"

    class FakeSession:
        """Provide a lightweight test double."""

        async def __aenter__(self):
            """Implement the test-double protocol method."""
            return self

        async def __aexit__(self, _exc_type, _exc, _tb):
            """Implement the test-double protocol method."""
            return False

        def get(self, _url: str, *, params: dict[str, str]):
            """Provide a test helper implementation."""
            del params
            nonlocal attempts
            attempts += 1
            return FakeResponse()

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
