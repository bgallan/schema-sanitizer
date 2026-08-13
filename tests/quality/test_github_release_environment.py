"""Tests for the externally configured PyPI deployment boundary."""

from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

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
