"""Tests for the externally configured PyPI deployment boundary."""

from __future__ import annotations

import importlib.util
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Callable
from urllib.error import HTTPError, URLError

import pytest


def _checker() -> ModuleType:
    path = Path(__file__).parents[2] / "meta/ci/release/check_github_release_environment.py"
    spec = importlib.util.spec_from_file_location("check_github_release_environment", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Response(io.BytesIO):
    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def _settings() -> dict[str, object]:
    return {
        "can_admins_bypass": False,
        "deployment_branch_policy": {
            "protected_branches": False,
            "custom_branch_policies": True,
        },
        "protection_rules": [
            {
                "type": "required_reviewers",
                "prevent_self_review": True,
                "reviewers": [{"type": "User", "reviewer": {"login": "auditor"}}],
            }
        ],
    }


def test_release_environment_requires_exact_protected_main_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checker = _checker()
    responses = iter(
        [
            _settings(),
            {
                "total_count": 1,
                "branch_policies": [{"type": "branch", "name": "main"}],
            },
        ]
    )

    def fake_urlopen(request: object, *, timeout: int) -> _Response:
        assert timeout == 20
        assert request.full_url.startswith("https://api.github.com/repos/bgallan/project/")
        assert request.get_header("X-github-api-version") == "2026-03-10"
        return _Response(json.dumps(next(responses)).encode())

    monkeypatch.setattr(checker, "urlopen", fake_urlopen)
    checker.validate_release_environment("bgallan/project", "pypi")


def test_github_lookup_retries_transport_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """A temporary API transport failure consumes one deterministic retry."""
    checker = _checker()
    responses: list[object] = [URLError("reset"), _settings()]
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
            "https://api.github.com/repos/bgallan/project/environments/pypi",
            sleeper=delays.append,
        )
        == _settings()
    )
    assert delays == [1.0]


def test_github_lookup_retries_429_but_not_other_4xx(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rate limits are transient while invalid authority fails immediately."""
    checker = _checker()
    statuses = iter((429, 403))
    calls = 0
    delays: list[float] = []

    def rejected(request: object, *, timeout: int) -> _Response:
        nonlocal calls
        calls += 1
        status = next(statuses)
        raise HTTPError(request.full_url, status, "rejected", {}, None)

    monkeypatch.setattr(checker, "urlopen", rejected)
    with pytest.raises(RuntimeError, match="HTTP 403"):
        checker._github_json(
            "https://api.github.com/repos/bgallan/project/environments/pypi",
            sleeper=delays.append,
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
        ({"Retry-After": "300"}, 0.0, 30.0),
        ({"X-RateLimit-Remaining": "0"}, 100.0, 1.0),
        (
            {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "115"},
            100.0,
            15.0,
        ),
        (
            {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1000"},
            100.0,
            30.0,
        ),
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

    def rate_limited(request: object, *, timeout: int) -> _Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise HTTPError(request.full_url, 403, "rate limited", headers, None)
        return _Response(json.dumps(_settings()).encode())

    monkeypatch.setattr(checker, "urlopen", rate_limited)
    assert (
        checker._github_json(
            "https://api.github.com/repos/bgallan/project/environments/pypi",
            sleeper=delays.append,
            clock=lambda: now,
        )
        == _settings()
    )
    assert calls == 2
    assert delays == [expected_delay]


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Retry-After": "unbounded"},
        {"Retry-After": "-1"},
        {"X-RateLimit-Remaining": "1"},
    ],
)
def test_github_lookup_rejects_unqualified_403_without_retry(
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
) -> None:
    """Permissions and malformed headers cannot be disguised as rate limits."""
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
            "https://api.github.com/repos/bgallan/project/environments/pypi",
            sleeper=delays.append,
        )
    assert calls == 1
    assert delays == []


def test_main_sha_uses_the_retrying_api_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Release identity is checked through the same hardened GitHub transport."""
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


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.update(can_admins_bypass=True), "administrator bypass"),
        (
            lambda value: value["protection_rules"][0].update(prevent_self_review=False),
            "prevent self-review",
        ),
        (lambda value: value.update(protection_rules=[]), "independent reviewer"),
        (
            lambda value: value["protection_rules"][0].update(reviewers="auditor"),
            "independent reviewer",
        ),
    ],
)
def test_release_environment_fails_closed_on_weak_controls(
    monkeypatch: pytest.MonkeyPatch,
    mutation: Callable[[dict[str, Any]], None],
    message: str,
) -> None:
    checker = _checker()
    settings = _settings()
    mutation(settings)
    monkeypatch.setattr(checker, "_github_json", lambda *_args, **_kwargs: settings)

    with pytest.raises(RuntimeError, match=message):
        checker.validate_release_environment("bgallan/project", "pypi")


def test_release_environment_rejects_paginated_or_extra_branch_policies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A first page containing main cannot conceal additional deployment policies."""
    checker = _checker()
    responses = iter(
        [
            _settings(),
            {
                "total_count": 2,
                "branch_policies": [{"name": "main"}],
            },
        ]
    )
    monkeypatch.setattr(checker, "_github_json", lambda *_args, **_kwargs: next(responses))

    with pytest.raises(RuntimeError, match="exactly one main branch policy"):
        checker.validate_release_environment("bgallan/project", "pypi")


def test_release_environment_rejects_a_tag_policy_named_main(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tag pattern cannot impersonate the required deployment branch policy."""
    checker = _checker()
    responses = iter(
        [
            _settings(),
            {
                "total_count": 1,
                "branch_policies": [{"type": "tag", "name": "main"}],
            },
        ]
    )
    monkeypatch.setattr(checker, "_github_json", lambda *_args, **_kwargs: next(responses))

    with pytest.raises(RuntimeError, match="allow only branch 'main'"):
        checker.validate_release_environment("bgallan/project", "pypi")


@pytest.mark.parametrize("repository", ["", "owner", "../owner/repository", "owner/../repo"])
def test_release_environment_rejects_invalid_repository_identifiers(repository: str) -> None:
    """Repository input cannot alter the intended GitHub API resource path."""
    with pytest.raises(RuntimeError, match="invalid GitHub repository identifier"):
        _checker().validate_release_environment(repository, "pypi")


@pytest.mark.parametrize(
    "api_url",
    ["http://api.github.com", "file:///tmp/github", "https://example.invalid"],
)
def test_release_environment_rejects_untrusted_api_urls(api_url: str) -> None:
    """The release preflight never dereferences a local or attacker-controlled URL."""
    with pytest.raises(RuntimeError, match="non-GitHub HTTPS"):
        _checker().validate_release_environment("bgallan/project", "pypi", api_url=api_url)
