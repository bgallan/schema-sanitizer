"""Provider configuration tests without process-environment coupling."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest


def test_s3_delegates_sdk_configuration() -> None:
    """Schema-Sanitizer does not synthesize endpoint, region, or credential options."""
    from schema_sanitizer.remote_impl.providers import s3

    assert s3.client_options() == {}


def test_azure_uses_default_sdk_credential_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    """Azure clients are built from the URI plus the SDK credential chain."""
    from schema_sanitizer.remote_impl.providers import azure

    credential = object()
    service = object()
    captured: dict[str, object] = {}

    class FakeCredential:
        """Provide a lightweight test double."""
        def __new__(cls) -> object:
            """Implement the test-double protocol method."""
            return credential

    class FakeService:
        """Provide a lightweight test double."""
        def __new__(cls, *, account_url: str, credential: object) -> object:
            """Implement the test-double protocol method."""
            captured.update(account_url=account_url, credential=credential)
            return service

    def fake_import(name: str) -> object:
        """Provide a test helper implementation."""
        if name == "azure.identity.aio":
            return SimpleNamespace(DefaultAzureCredential=FakeCredential)
        if name == "azure.storage.blob.aio":
            return SimpleNamespace(BlobServiceClient=FakeService)
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(azure, "import_module", fake_import)
    ref = azure.parse_uri("az://account/container/input/a.json")

    assert asyncio.run(azure.open_service(ref)) is service
    assert captured == {
        "account_url": "https://account.blob.core.windows.net",
        "credential": credential,
    }


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
    assert captured["scopes"] == [
        "https://www.googleapis.com/auth/devstorage.read_write"
    ]
