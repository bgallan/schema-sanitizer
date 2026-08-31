#!/usr/bin/env python3
"""Require the protected GitHub environment used by production publication.

The release preflight fails before canonical validation unless the ``pypi`` environment
already exists, requires a non-self reviewer, and permits deployments only from the
literal ``main`` branch.  This prevents GitHub from silently auto-creating an
unprotected environment on the first publication attempt.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from email.utils import parsedate_to_datetime
from typing import Any, Sequence

JsonFetcher = Callable[[str], dict[str, Any]]
_RETRY_DELAYS_SECONDS = (1.0, 2.0)
_MAX_SERVER_RETRY_DELAY_SECONDS = 30.0
_TRANSIENT_HTTP_STATUSES = {429, *range(500, 600)}


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    """Prevent authorization-bearing GitHub API requests from following redirects."""

    def redirect_request(
        self,
        _request: urllib.request.Request,
        _file_pointer: object,
        _code: int,
        _message: str,
        _headers: object,
        _new_url: str,
    ) -> None:
        """Refuse every redirect so urllib returns the original 3xx as an error."""
        return None


def _retry_after_delay(value: str, now: float) -> float | None:
    """Parse a GitHub Retry-After value into one bounded nonnegative delay."""
    value = value.strip()
    if value.isdecimal():
        return min(float(value), _MAX_SERVER_RETRY_DELAY_SECONDS)
    try:
        deadline = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if deadline.tzinfo is None:
        return None
    return min(max(0.0, deadline.timestamp() - now), _MAX_SERVER_RETRY_DELAY_SECONDS)


def _retry_delay(headers: dict[str, str], fallback: float, now: float) -> float:
    """Honor bounded GitHub retry guidance and otherwise use the fixed fallback."""
    retry_after = headers.get("retry-after")
    if retry_after is not None:
        delay = _retry_after_delay(retry_after, now)
        if delay is not None:
            return delay
    if headers.get("x-ratelimit-remaining", "").strip() == "0":
        reset = headers.get("x-ratelimit-reset", "").strip()
        try:
            return min(max(0.0, float(reset) - now), _MAX_SERVER_RETRY_DELAY_SECONDS)
        except ValueError:
            pass
    return fallback


def _github_fetcher(
    api_url: str,
    repository: str,
    token: str,
    *,
    sleeper: Callable[[float], object] = time.sleep,
    clock: Callable[[], float] = time.time,
) -> JsonFetcher:
    """Return a bounded authenticated JSON reader for this repository's API path."""
    parsed_api = urllib.parse.urlsplit(api_url)
    if (
        parsed_api.scheme != "https"
        or not parsed_api.netloc
        or parsed_api.username is not None
        or parsed_api.password is not None
        or parsed_api.query
        or parsed_api.fragment
    ):
        raise ValueError("GitHub API URL must be one credential-free HTTPS origin and path")
    if repository.count("/") != 1 or any(
        not part or part in {".", ".."} or "/" in part for part in repository.split("/")
    ):
        raise ValueError("repository must identify one owner/repository")
    encoded_repository = "/".join(
        urllib.parse.quote(part, safe="-._~") for part in repository.split("/")
    )
    prefix = f"{api_url.rstrip('/')}/repos/{encoded_repository}/"
    opener = urllib.request.build_opener(_RejectRedirects())

    def fetch(relative_path: str) -> dict[str, Any]:
        """Fetch one repository-relative endpoint without following an alternate host."""
        url = prefix + relative_path.lstrip("/")
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        payload = b""
        for attempt in range(len(_RETRY_DELAYS_SECONDS) + 1):
            try:
                with opener.open(request, timeout=15) as response:  # nosec B310
                    if response.geturl() != url:
                        raise RuntimeError("GitHub environment API redirected unexpectedly")
                    payload = response.read(1_048_577)
                break
            except urllib.error.HTTPError as error:
                headers = {
                    name.lower(): value
                    for name, value in (error.headers.items() if error.headers else ())
                }
                if error.code == 404:
                    raise RuntimeError(
                        "the protected pypi environment is not configured"
                    ) from error
                rate_limited = error.code == 403 and (
                    headers.get("x-ratelimit-remaining", "").strip() == "0"
                    or "retry-after" in headers
                )
                transient = error.code in _TRANSIENT_HTTP_STATUSES or rate_limited
                if not transient or attempt == len(_RETRY_DELAYS_SECONDS):
                    raise RuntimeError(
                        f"GitHub environment API returned HTTP {error.code}"
                    ) from error
                delay = _retry_delay(headers, _RETRY_DELAYS_SECONDS[attempt], clock())
            except (OSError, urllib.error.URLError) as error:
                if attempt == len(_RETRY_DELAYS_SECONDS):
                    raise RuntimeError("GitHub environment API transport failed") from error
                delay = _RETRY_DELAYS_SECONDS[attempt]
            sleeper(delay)
        if len(payload) > 1_048_576:
            raise RuntimeError("GitHub environment API response exceeded 1 MiB")
        parsed = json.loads(payload)
        if not isinstance(parsed, dict):
            raise RuntimeError("GitHub environment API response is not an object")
        return parsed

    return fetch


def validate_publish_environment(fetch: JsonFetcher, environment: str = "pypi") -> None:
    """Require reviewer protection and an exact single-main deployment policy."""
    encoded_environment = urllib.parse.quote(environment, safe="")
    configuration = fetch(f"environments/{encoded_environment}")
    if configuration.get("name") != environment:
        raise RuntimeError(f"GitHub returned the wrong environment: {configuration.get('name')}")
    if configuration.get("can_admins_bypass") is not False:
        raise RuntimeError("pypi environment must prohibit administrator bypass")
    rules = configuration.get("protection_rules")
    if not isinstance(rules, list):
        raise RuntimeError("pypi environment protection rules are missing")
    reviewer_rules = [rule for rule in rules if rule.get("type") == "required_reviewers"]
    if len(reviewer_rules) != 1:
        raise RuntimeError("pypi environment must have exactly one required-reviewer rule")
    reviewer_rule = reviewer_rules[0]
    reviewers = reviewer_rule.get("reviewers")
    if not isinstance(reviewers, list) or not reviewers:
        raise RuntimeError("pypi environment must require at least one reviewer")
    if reviewer_rule.get("prevent_self_review") is not True:
        raise RuntimeError("pypi environment must prevent self-review")
    branch_policy = configuration.get("deployment_branch_policy")
    if branch_policy != {"protected_branches": False, "custom_branch_policies": True}:
        raise RuntimeError("pypi environment must use an exact custom branch policy")
    policies = fetch(f"environments/{encoded_environment}/deployment-branch-policies?per_page=100")
    branches = policies.get("branch_policies")
    if policies.get("total_count") != 1 or not isinstance(branches, list) or len(branches) != 1:
        raise RuntimeError("pypi environment must have exactly one deployment branch policy")
    if {"name": branches[0].get("name"), "type": branches[0].get("type")} != {
        "name": "main",
        "type": "branch",
    }:
        raise RuntimeError("pypi environment policy must be the literal main branch type")


def main(argv: Sequence[str] | None = None) -> int:
    """Read the token from standard input and enforce the requested remote policy."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", default="pypi")
    parser.add_argument("--repository", required=True)
    parser.add_argument("--api-url", default="https://api.github.com")
    args = parser.parse_args(argv)
    token = sys.stdin.read(16_385)
    if args.repository.count("/") != 1:
        parser.error("--repository must identify one owner/repository")
    if len(token) > 16_384:
        parser.error("GitHub token input exceeded 16 KiB")
    if not token:
        parser.error("GitHub token is required on standard input")
    try:
        validate_publish_environment(
            _github_fetcher(args.api_url, args.repository, token), args.environment
        )
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
