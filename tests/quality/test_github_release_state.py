"""Tests for the immutable GitHub release-candidate check."""

from __future__ import annotations

import importlib.util
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from urllib.error import HTTPError, URLError

import pytest


def _checker() -> ModuleType:
    path = Path(__file__).parents[2] / "meta/ci/release/check_github_release_state.py"
    spec = importlib.util.spec_from_file_location("check_github_release_state", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Response(io.BytesIO):
    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def test_main_sha_uses_the_retrying_api_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Release identity is checked against the authenticated remote main ref."""
    checker = _checker()
    expected = "a" * 40
    captured: list[str] = []

    def reference(url: str, _token: str) -> dict[str, object]:
        captured.append(url)
        return {"object": {"sha": expected}}

    monkeypatch.setattr(checker, "_github_json", reference)
    checker.validate_main_sha("bgallan/project", expected, token="token")
    assert captured == ["https://api.github.com/repos/bgallan/project/git/ref/heads/main"]


def test_main_sha_fails_closed_if_the_branch_moved(monkeypatch: pytest.MonkeyPatch) -> None:
    """A release cannot proceed from a stale dispatch commit."""
    checker = _checker()
    monkeypatch.setattr(
        checker, "_github_json", lambda *_args, **_kwargs: {"object": {"sha": "b" * 40}}
    )

    with pytest.raises(RuntimeError, match="main moved"):
        checker.validate_main_sha("bgallan/project", "a" * 40)


def test_github_lookup_retries_transport_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """A temporary API transport failure consumes one deterministic retry."""
    checker = _checker()
    payload = {"object": {"sha": "a" * 40}}
    responses: list[object] = [URLError("reset"), payload]
    delays: list[float] = []

    def flaky(_request: object, *, timeout: int) -> _Response:
        assert timeout == 20
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return _Response(json.dumps(response).encode())

    monkeypatch.setattr(checker, "urlopen", flaky)
    assert (
        checker._github_json(
            "https://api.github.com/repos/bgallan/project/git/ref/heads/main",
            sleeper=delays.append,
        )
        == payload
    )
    assert delays == [1.0]


@pytest.mark.parametrize("status", [429, 500, 503])
def test_github_lookup_retries_transient_http_statuses(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    """Rate limits and server failures retry without weakening the final result."""
    checker = _checker()
    calls = 0
    delays: list[float] = []
    payload = {"object": {"sha": "a" * 40}}

    def transient(request: object, *, timeout: int) -> _Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise HTTPError(request.full_url, status, "transient", {}, None)
        return _Response(json.dumps(payload).encode())

    monkeypatch.setattr(checker, "urlopen", transient)
    assert (
        checker._github_json(
            "https://api.github.com/repos/bgallan/project/git/ref/heads/main",
            sleeper=delays.append,
        )
        == payload
    )
    assert calls == 2
    assert delays == [1.0]


@pytest.mark.parametrize(
    ("headers", "now", "expected_delay"),
    [
        ({"Retry-After": "12"}, 0.0, 12.0),
        (
            {"Retry-After": "Thu, 13 Aug 2026 15:00:00 GMT"},
            datetime(2026, 8, 13, 14, 59, 50, tzinfo=timezone.utc).timestamp(),
            10.0,
        ),
        ({"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "115"}, 100.0, 15.0),
        ({"Retry-After": "300"}, 0.0, 30.0),
    ],
)
def test_github_lookup_retries_documented_rate_limited_403(
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
    now: float,
    expected_delay: float,
) -> None:
    """A 403 retries only when GitHub supplies a documented rate-limit signal."""
    checker = _checker()
    calls = 0
    delays: list[float] = []
    payload = {"object": {"sha": "a" * 40}}

    def rate_limited(request: object, *, timeout: int) -> _Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise HTTPError(request.full_url, 403, "rate limited", headers, None)
        return _Response(json.dumps(payload).encode())

    monkeypatch.setattr(checker, "urlopen", rate_limited)
    assert (
        checker._github_json(
            "https://api.github.com/repos/bgallan/project/git/ref/heads/main",
            sleeper=delays.append,
            clock=lambda: now,
        )
        == payload
    )
    assert calls == 2
    assert delays == [expected_delay]


@pytest.mark.parametrize("headers", [{}, {"Retry-After": "invalid"}])
def test_github_lookup_rejects_unqualified_403_without_retry(
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
) -> None:
    """Permission failures cannot be disguised as transient rate limits."""
    checker = _checker()
    calls = 0
    delays: list[float] = []

    def forbidden(request: object, *, timeout: int) -> _Response:
        nonlocal calls
        calls += 1
        raise HTTPError(request.full_url, 403, "forbidden", headers, None)

    monkeypatch.setattr(checker, "urlopen", forbidden)
    with pytest.raises(RuntimeError, match="HTTP 403"):
        checker._github_json(
            "https://api.github.com/repos/bgallan/project/git/ref/heads/main",
            sleeper=delays.append,
        )
    assert calls == 1
    assert delays == []


@pytest.mark.parametrize("payload", [[], "main", None])
def test_github_lookup_rejects_non_object_json(
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
) -> None:
    """A syntactically valid but structurally invalid API response fails closed."""

    def invalid(_request: object, *, timeout: int) -> _Response:
        assert timeout == 20
        return _Response(json.dumps(payload).encode())

    checker = _checker()
    monkeypatch.setattr(checker, "urlopen", invalid)
    with pytest.raises(RuntimeError, match="non-object"):
        checker._github_json("https://api.github.com/repos/bgallan/project/git/ref/heads/main")


def test_github_lookup_rejects_malformed_json_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed data is semantic corruption, not a retryable transport failure."""
    calls = 0
    delays: list[float] = []

    def malformed(_request: object, *, timeout: int) -> _Response:
        nonlocal calls
        calls += 1
        return _Response(b"{")

    checker = _checker()
    monkeypatch.setattr(checker, "urlopen", malformed)
    with pytest.raises(RuntimeError, match="verify GitHub release state"):
        checker._github_json(
            "https://api.github.com/repos/bgallan/project/git/ref/heads/main",
            sleeper=delays.append,
        )
    assert calls == 1
    assert delays == []


@pytest.mark.parametrize("repository", ["", "owner", "../owner/repository", "owner/../repo"])
def test_main_sha_rejects_invalid_repository_identifiers(repository: str) -> None:
    """Repository input cannot alter the intended GitHub API path."""
    with pytest.raises(RuntimeError, match="invalid GitHub repository identifier"):
        _checker().validate_main_sha(repository, "a" * 40)


@pytest.mark.parametrize(
    "api_url",
    ["http://api.github.com", "file:///tmp/github", "https://example.invalid"],
)
def test_main_sha_rejects_untrusted_api_urls(api_url: str) -> None:
    """Release preflight never dereferences a local or attacker-controlled URL."""
    with pytest.raises(RuntimeError, match="non-GitHub HTTPS"):
        _checker().validate_main_sha("bgallan/project", "a" * 40, api_url=api_url)
