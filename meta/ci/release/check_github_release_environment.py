#!/usr/bin/env python3
"""Verify the external GitHub Environment required by the PyPI release gate."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections.abc import Callable
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

_REPOSITORY_PATTERN = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_GIT_SHA_PATTERN = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
_RETRY_DELAYS_SECONDS = (1.0, 2.0)
_MAX_SERVER_RETRY_DELAY_SECONDS = 30.0
_TRANSIENT_HTTP_STATUSES = {429, *range(500, 600)}


def _retry_after_delay(value: str, now: float) -> float | None:
    """Parse an RFC Retry-After value into a nonnegative delay."""
    value = value.strip()
    if re.fullmatch(r"[0-9]+", value):
        return float(value)
    try:
        deadline = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if deadline.tzinfo is None:
        return None
    return max(0.0, deadline.timestamp() - now)


def _is_rate_limited_403(error: HTTPError) -> bool:
    """Recognize only GitHub's documented rate-limit signals on HTTP 403."""
    if error.code != 403 or error.headers is None:
        return False
    remaining = error.headers.get("X-RateLimit-Remaining")
    if isinstance(remaining, str) and remaining.strip() == "0":
        return True
    retry_after_header = error.headers.get("Retry-After")
    if not isinstance(retry_after_header, str) or not retry_after_header.strip():
        return False
    return _retry_after_delay(retry_after_header, 0.0) is not None


def _is_transient_http_error(error: HTTPError) -> bool:
    """Return whether a bounded retry can safely distinguish a transient error."""
    return error.code in _TRANSIENT_HTTP_STATUSES or _is_rate_limited_403(error)


def _http_retry_delay(error: HTTPError, fallback: float, now: float) -> float:
    """Prefer GitHub's official retry timing while retaining a strict upper bound."""
    headers = error.headers
    if headers is not None:
        retry_after = headers.get("Retry-After")
        if isinstance(retry_after, str):
            delay = _retry_after_delay(retry_after, now)
            if delay is not None:
                return min(delay, _MAX_SERVER_RETRY_DELAY_SECONDS)
        remaining = headers.get("X-RateLimit-Remaining")
        reset = headers.get("X-RateLimit-Reset")
        if isinstance(remaining, str) and remaining.strip() == "0" and isinstance(reset, str):
            reset = reset.strip()
            if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", reset):
                return min(max(0.0, float(reset) - now), _MAX_SERVER_RETRY_DELAY_SECONDS)
    return fallback


def _github_json(
    url: str,
    token: str = "",
    *,
    sleeper: Callable[[float], object] = time.sleep,
    clock: Callable[[], float] = time.time,
) -> dict[str, Any]:
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
    request = Request(url, headers=headers)
    attempts = len(_RETRY_DELAYS_SECONDS) + 1
    for attempt in range(attempts):
        try:
            with urlopen(request, timeout=20) as response:  # nosec B310
                payload = json.load(response)
            break
        except HTTPError as exc:
            if not _is_transient_http_error(exc) or attempt == attempts - 1:
                raise RuntimeError(f"GitHub returned HTTP {exc.code} for {url}") from exc
            retry_delay = _http_retry_delay(
                exc,
                _RETRY_DELAYS_SECONDS[attempt],
                clock(),
            )
        except (OSError, URLError) as exc:
            if attempt == attempts - 1:
                raise RuntimeError(f"could not verify GitHub release environment: {exc}") from exc
            retry_delay = _RETRY_DELAYS_SECONDS[attempt]
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"could not verify GitHub release environment: {exc}") from exc
        sleeper(retry_delay)
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


def validate_main_sha(
    repository: str,
    expected_sha: str,
    *,
    token: str = "",
    api_url: str = "https://api.github.com",
) -> None:
    """Require the remote main ref to remain at the immutable dispatch SHA."""
    repository_parts = repository.split("/")
    if _REPOSITORY_PATTERN.fullmatch(repository) is None or any(
        part in {".", ".."} for part in repository_parts
    ):
        raise RuntimeError(f"invalid GitHub repository identifier: {repository!r}")
    if _GIT_SHA_PATTERN.fullmatch(expected_sha) is None:
        raise RuntimeError(f"invalid expected main SHA: {expected_sha!r}")
    parsed_api = urlsplit(api_url)
    if parsed_api.scheme != "https" or parsed_api.hostname != "api.github.com":
        raise RuntimeError(f"refusing non-GitHub HTTPS API URL: {api_url!r}")
    resource = f"{api_url.rstrip('/')}/repos/{quote(repository, safe='/')}/git/ref/heads/main"
    payload = _github_json(resource, token)
    reference = payload.get("object")
    actual_sha = reference.get("sha") if isinstance(reference, dict) else None
    if actual_sha != expected_sha:
        raise RuntimeError(
            f"main moved to {actual_sha!r}; start a new release from commit {actual_sha!r}"
        )


def main() -> None:
    """Validate the configured production publication boundary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--environment", default="pypi")
    parser.add_argument(
        "--api-url", default=os.environ.get("GITHUB_API_URL", "https://api.github.com")
    )
    parser.add_argument("--expected-main-sha")
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
        if args.expected_main_sha:
            validate_main_sha(
                args.repository,
                args.expected_main_sha,
                token=os.environ.get("GITHUB_TOKEN", ""),
                api_url=args.api_url,
            )
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    print(f"GitHub release environment passed: {args.environment}")


if __name__ == "__main__":
    main()
