"""Fail-closed contracts for the protected publication-environment preflight.

The suite exercises exact reviewer and branch policy, URL confinement, redirect
rejection, and bounded recovery from only documented transient API failures.
"""

from __future__ import annotations

import io
import json
import urllib.error
from email.message import Message
from typing import Any

import pytest

from meta.ci.release import check_publish_environment as publish_environment


def _environment_configuration() -> dict[str, Any]:
    """Return one exact protected-environment API response."""
    return {
        "name": "pypi",
        "can_admins_bypass": False,
        "protection_rules": [
            {
                "type": "required_reviewers",
                "prevent_self_review": True,
                "reviewers": [{"type": "User", "reviewer": {"id": 1}}],
            }
        ],
        "deployment_branch_policy": {
            "protected_branches": False,
            "custom_branch_policies": True,
        },
    }


def _branch_policies() -> dict[str, Any]:
    """Return one exact literal-main branch policy response."""
    return {
        "total_count": 1,
        "branch_policies": [{"name": "main", "type": "branch"}],
    }


def _policy_fetcher(
    configuration: dict[str, Any], policies: dict[str, Any]
) -> publish_environment.JsonFetcher:
    """Return a deterministic two-endpoint policy fixture."""

    def fetch(path: str) -> dict[str, Any]:
        """Select the matching fixture by repository-relative endpoint."""
        return policies if "deployment-branch-policies" in path else configuration

    return fetch


def test_exact_publish_environment_policy_is_accepted() -> None:
    """A non-bypassable reviewer gate restricted to main passes."""
    publish_environment.validate_publish_environment(
        _policy_fetcher(_environment_configuration(), _branch_policies())
    )


@pytest.mark.parametrize(
    "case",
    (
        "admin-bypass",
        "self-review",
        "no-reviewers",
        "missing-rules",
        "wrong-branch-name",
        "wrong-branch-type",
        "multiple-branches",
        "wrong-policy-mode",
    ),
)
def test_publish_environment_rejects_weakened_or_malformed_policy(case: str) -> None:
    """Every relaxation of reviewer or literal-main protection fails closed."""
    configuration = _environment_configuration()
    policies = _branch_policies()
    if case == "admin-bypass":
        configuration["can_admins_bypass"] = True
    elif case == "self-review":
        configuration["protection_rules"][0]["prevent_self_review"] = False
    elif case == "no-reviewers":
        configuration["protection_rules"][0]["reviewers"] = []
    elif case == "missing-rules":
        configuration["protection_rules"] = None
    elif case == "wrong-branch-name":
        policies["branch_policies"][0]["name"] = "release/*"
    elif case == "wrong-branch-type":
        policies["branch_policies"][0]["type"] = "tag"
    elif case == "multiple-branches":
        policies["total_count"] = 2
        policies["branch_policies"].append({"name": "other", "type": "branch"})
    elif case == "wrong-policy-mode":
        configuration["deployment_branch_policy"] = {
            "protected_branches": True,
            "custom_branch_policies": False,
        }

    with pytest.raises(RuntimeError):
        publish_environment.validate_publish_environment(_policy_fetcher(configuration, policies))


@pytest.mark.parametrize(
    "api_url",
    (
        "http://api.github.com",
        "https://token@api.github.com",
        "https://api.github.com?redirect=1",
        "//api.github.com",
    ),
)
def test_publish_environment_rejects_unsafe_api_origins(api_url: str) -> None:
    """Only a credential-free HTTPS API origin/path can receive the token."""
    with pytest.raises(ValueError, match="credential-free HTTPS"):
        publish_environment._github_fetcher(api_url, "owner/repository", "token")


class _Response:
    """Provide a bounded context-managed JSON API response."""

    def __init__(self, url: str, payload: object) -> None:
        """Store the effective URL and canonical JSON body."""
        self._url = url
        self._payload = json.dumps(payload).encode()

    def __enter__(self) -> _Response:
        """Return this fixture as the opened response."""
        return self

    def __exit__(self, *_args: object) -> None:
        """Close the fixture without suppressing errors."""

    def geturl(self) -> str:
        """Return the effective response URL used for redirect confinement."""
        return self._url

    def read(self, amount: int) -> bytes:
        """Return no more than the requested response bytes."""
        return self._payload[:amount]


class _SequenceOpener:
    """Replay exact responses or exceptions from a finite request sequence."""

    def __init__(self, outcomes: list[object]) -> None:
        """Store the request outcomes and initialize a call counter."""
        self.outcomes = outcomes
        self.calls = 0

    def open(self, request: object, *, timeout: int) -> object:
        """Return or raise the next configured outcome."""
        assert request is not None and timeout == 15
        outcome = self.outcomes[self.calls]
        self.calls += 1
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _http_error(url: str, status: int, **headers: str) -> urllib.error.HTTPError:
    """Construct one HTTPError with normalized test headers."""
    message = Message()
    for name, value in headers.items():
        message[name.replace("_", "-")] = value
    return urllib.error.HTTPError(url, status, "failure", message, io.BytesIO())


def test_publish_environment_retries_transient_http_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient server error honors bounded Retry-After before succeeding."""
    url = "https://api.github.com/repos/owner/repository/environments/pypi"
    opener = _SequenceOpener(
        [_http_error(url, 503, retry_after="7"), _Response(url, {"name": "pypi"})]
    )
    monkeypatch.setattr(publish_environment.urllib.request, "build_opener", lambda *_: opener)
    sleeps: list[float] = []
    fetch = publish_environment._github_fetcher(
        "https://api.github.com",
        "owner/repository",
        "token",
        sleeper=sleeps.append,
        clock=lambda: 0.0,
    )

    assert fetch("environments/pypi") == {"name": "pypi"}
    assert opener.calls == 2
    assert sleeps == [7.0]


def test_publish_environment_rejects_redirect_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An alternate effective URL cannot receive another request or token."""
    opener = _SequenceOpener([_Response("https://example.invalid/stolen", {"name": "pypi"})])
    monkeypatch.setattr(publish_environment.urllib.request, "build_opener", lambda *_: opener)
    fetch = publish_environment._github_fetcher(
        "https://api.github.com", "owner/repository", "token", sleeper=lambda _: None
    )

    with pytest.raises(RuntimeError, match="redirected"):
        fetch("environments/pypi")
    assert opener.calls == 1


def test_publish_environment_rejects_missing_environment_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A semantic 404 proves missing protection and is never retried."""
    url = "https://api.github.com/repos/owner/repository/environments/pypi"
    opener = _SequenceOpener([_http_error(url, 404), _Response(url, {})])
    monkeypatch.setattr(publish_environment.urllib.request, "build_opener", lambda *_: opener)
    sleeps: list[float] = []
    fetch = publish_environment._github_fetcher(
        "https://api.github.com", "owner/repository", "token", sleeper=sleeps.append
    )

    with pytest.raises(RuntimeError, match="not configured"):
        fetch("environments/pypi")
    assert opener.calls == 1
    assert sleeps == []


def test_publish_environment_rejects_nonobject_api_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Syntactically valid but structurally malformed JSON fails closed."""
    url = "https://api.github.com/repos/owner/repository/environments/pypi"
    opener = _SequenceOpener([_Response(url, [])])
    monkeypatch.setattr(publish_environment.urllib.request, "build_opener", lambda *_: opener)
    fetch = publish_environment._github_fetcher(
        "https://api.github.com", "owner/repository", "token", sleeper=lambda _: None
    )

    with pytest.raises(RuntimeError, match="not an object"):
        fetch("environments/pypi")
