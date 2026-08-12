#!/usr/bin/env python3
"""Verify the external GitHub Environment required by the PyPI release gate."""

from __future__ import annotations

import argparse
import json
import os
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

_REPOSITORY_PATTERN = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")


def _github_json(url: str, token: str = "") -> dict[str, Any]:
    """Read one GitHub REST resource or fail closed with a concise error."""
    parsed_url = urlsplit(url)
    if parsed_url.scheme != "https" or parsed_url.hostname != "api.github.com":
        raise RuntimeError(f"refusing non-GitHub HTTPS API URL: {url!r}")
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "schema-sanitizer-release-preflight",
        "X-GitHub-Api-Version": "2026-03-10",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        with urlopen(Request(url, headers=headers), timeout=20) as response:  # nosec B310
            payload = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"GitHub returned HTTP {exc.code} for {url}") from exc
    except (OSError, URLError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"could not verify GitHub release environment: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"GitHub returned a non-object response for {url}")
    return payload


def validate_release_environment(
    repository: str,
    environment: str,
    *,
    token: str = "",
    api_url: str = "https://api.github.com",
) -> None:
    """Require independent review, no admin bypass, and an exact main policy."""
    repository_parts = repository.split("/")
    if _REPOSITORY_PATTERN.fullmatch(repository) is None or any(
        part in {".", ".."} for part in repository_parts
    ):
        raise RuntimeError(f"invalid GitHub repository identifier: {repository!r}")
    parsed_api = urlsplit(api_url)
    if parsed_api.scheme != "https" or parsed_api.hostname != "api.github.com":
        raise RuntimeError(f"refusing non-GitHub HTTPS API URL: {api_url!r}")
    base = (
        f"{api_url.rstrip('/')}/repos/{quote(repository, safe='/')}/environments/"
        f"{quote(environment, safe='')}"
    )
    settings = _github_json(base, token)
    protection_rules = settings.get("protection_rules")
    if not isinstance(protection_rules, list):
        raise RuntimeError(f"GitHub environment {environment!r} has invalid protection rules")
    reviewer_rules = [
        rule
        for rule in protection_rules
        if isinstance(rule, dict) and rule.get("type") == "required_reviewers"
    ]
    if len(reviewer_rules) != 1:
        raise RuntimeError(f"GitHub environment {environment!r} requires an independent reviewer")
    reviewer_rule = reviewer_rules[0]
    reviewers = reviewer_rule.get("reviewers")
    if (
        not isinstance(reviewers, list)
        or not reviewers
        or not all(isinstance(reviewer, dict) for reviewer in reviewers)
    ):
        raise RuntimeError(f"GitHub environment {environment!r} requires an independent reviewer")
    if reviewer_rule.get("prevent_self_review") is not True:
        raise RuntimeError(f"GitHub environment {environment!r} must prevent self-review")
    if settings.get("can_admins_bypass") is not False:
        raise RuntimeError(f"GitHub environment {environment!r} must disable administrator bypass")

    deployment = settings.get("deployment_branch_policy")
    if (
        not isinstance(deployment, dict)
        or deployment.get("protected_branches") is not False
        or deployment.get("custom_branch_policies") is not True
    ):
        raise RuntimeError(f"GitHub environment {environment!r} must use a custom main policy")
    policy_response = _github_json(f"{base}/deployment-branch-policies", token)
    policies = policy_response.get("branch_policies")
    if (
        policy_response.get("total_count") != 1
        or not isinstance(policies, list)
        or len(policies) != 1
        or not isinstance(policies[0], dict)
    ):
        raise RuntimeError(
            f"GitHub environment {environment!r} must have exactly one main branch policy"
        )
    policy = policies[0]
    if policy.get("type") != "branch" or policy.get("name") != "main":
        raise RuntimeError(
            f"GitHub environment {environment!r} must allow only branch 'main', "
            f"got type={policy.get('type')!r}, name={policy.get('name')!r}"
        )


def main() -> None:
    """Validate the configured production publication boundary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--environment", default="pypi")
    parser.add_argument(
        "--api-url", default=os.environ.get("GITHUB_API_URL", "https://api.github.com")
    )
    args = parser.parse_args()
    if not args.repository:
        raise SystemExit("--repository or GITHUB_REPOSITORY is required")
    try:
        validate_release_environment(
            args.repository,
            args.environment,
            token=os.environ.get("GITHUB_TOKEN", ""),
            api_url=args.api_url,
        )
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    print(f"GitHub release environment passed: {args.environment}")


if __name__ == "__main__":
    main()
