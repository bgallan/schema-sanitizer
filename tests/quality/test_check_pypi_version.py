"""Tests for the fail-closed PyPI release preflight."""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path
from types import ModuleType
from urllib.error import HTTPError, URLError

import pytest


def _checker() -> ModuleType:
    """Load the standalone preflight helper."""
    path = Path(__file__).parents[2] / "meta/ci/release/check_pypi_version.py"
    spec = importlib.util.spec_from_file_location("check_pypi_version", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(path.parent))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


class _Response(io.BytesIO):
    """Minimal JSON HTTP response context manager."""

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def test_pypi_lookup_distinguishes_existing_and_available_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exact JSON resource blocks release, while a 404 permits validation."""
    checker = _checker()
    captured: list[str] = []

    def existing(request: object, *, timeout: int) -> _Response:
        captured.append(request.full_url)
        assert timeout == 20
        return _Response(json.dumps({"info": {"version": "0.4.2"}}).encode())

    monkeypatch.setattr(checker, "urlopen", existing)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.2") is True
    assert captured == ["https://pypi.org/pypi/schema-sanitizer/0.4.2/json"]

    def missing(request: object, *, timeout: int) -> _Response:
        raise HTTPError(request.full_url, 404, "not found", {}, None)

    monkeypatch.setattr(checker, "urlopen", missing)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.3") is False


def test_pypi_lookup_fails_closed_on_service_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """An inconclusive index response cannot authorize publication."""
    checker = _checker()

    def unavailable(request: object, *, timeout: int) -> _Response:
        raise HTTPError(request.full_url, 503, "unavailable", {}, None)

    calls = 0

    def counted_unavailable(request: object, *, timeout: int) -> _Response:
        nonlocal calls
        calls += 1
        return unavailable(request, timeout=timeout)

    monkeypatch.setattr(checker, "urlopen", counted_unavailable)
    with pytest.raises(RuntimeError, match="HTTP 503"):
        checker.pypi_release_exists("schema-sanitizer", "0.4.3", sleeper=lambda _delay: None)
    assert calls == 3


def test_pypi_lookup_retries_transport_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bounded retry absorbs transient transport failure without weakening policy."""
    checker = _checker()
    responses: list[object] = [URLError("reset"), {"info": {"version": "0.4.3"}}]
    delays: list[float] = []

    def flaky(_request: object, *, timeout: int) -> _Response:
        assert timeout == 20
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return _Response(json.dumps(response).encode())

    monkeypatch.setattr(checker, "urlopen", flaky)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.3", sleeper=delays.append) is True
    assert delays == [1.0]


def test_pypi_lookup_does_not_retry_semantic_client_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only 429 is a retryable 4xx; authentication and policy failures are immediate."""
    checker = _checker()
    calls = 0
    delays: list[float] = []

    def forbidden(request: object, *, timeout: int) -> _Response:
        nonlocal calls
        calls += 1
        raise HTTPError(request.full_url, 403, "forbidden", {}, None)

    monkeypatch.setattr(checker, "urlopen", forbidden)
    with pytest.raises(RuntimeError, match="HTTP 403"):
        checker.pypi_release_exists("schema-sanitizer", "0.4.3", sleeper=delays.append)
    assert calls == 1
    assert delays == []


def test_pypi_lookup_retries_rate_limit_before_exact_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rate-limited lookup may retry, while the eventual 404 still proves availability."""
    checker = _checker()
    statuses = iter((429, 404))
    delays: list[float] = []

    def response(request: object, *, timeout: int) -> _Response:
        status = next(statuses)
        raise HTTPError(request.full_url, status, "response", {}, None)

    monkeypatch.setattr(checker, "urlopen", response)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.3", sleeper=delays.append) is False
    assert delays == [1.0]


@pytest.mark.parametrize(
    "index_url",
    [
        "http://pypi.org/pypi",
        "file:///tmp/pypi",
        "https://example.invalid/pypi",
    ],
)
def test_pypi_lookup_rejects_untrusted_index_urls(index_url: str) -> None:
    """Release preflight never dereferences a local or attacker-controlled URL."""
    with pytest.raises(RuntimeError, match="non-PyPI HTTPS"):
        _checker().pypi_release_exists("schema-sanitizer", "0.4.3", index_url=index_url)


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {},
        {"info": []},
        {"info": {"version": "0.4.2"}},
    ],
)
def test_pypi_lookup_never_treats_an_unexpected_200_as_available(
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
) -> None:
    """Only a 404 proves availability; malformed or inconsistent 200s fail closed."""
    checker = _checker()

    def unexpected(_request: object, *, timeout: int) -> _Response:
        assert timeout == 20
        return _Response(json.dumps(payload).encode())

    monkeypatch.setattr(checker, "urlopen", unexpected)
    with pytest.raises(RuntimeError, match="malformed JSON|exact resource"):
        checker.pypi_release_exists("schema-sanitizer", "0.4.3")
