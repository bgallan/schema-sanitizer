#!/usr/bin/env python3
"""Verify that a manual release still targets the current main commit.

It queries GitHub with bounded retry handling and confirms that main still points at the
intended release commit.
"""

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
    """Parse a Retry-After value into a nonnegative delay."""
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
    retry_after = error.headers.get("Retry-After")
    return isinstance(retry_after, str) and _retry_after_delay(retry_after, 0.0) is not None


def _http_retry_delay(error: HTTPError, fallback: float, now: float) -> float:
    """Prefer GitHub's retry timing while retaining a strict upper bound."""
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
    """Read one GitHub REST resource with bounded transient retries."""
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
    payload: object = None
    for attempt in range(attempts):
        try:
            with urlopen(request, timeout=20) as response:  # nosec B310
                payload = json.load(response)
            break
        except HTTPError as exc:
            transient = exc.code in _TRANSIENT_HTTP_STATUSES or _is_rate_limited_403(exc)
            if not transient or attempt == attempts - 1:
                raise RuntimeError(f"GitHub returned HTTP {exc.code} for {url}") from exc
            retry_delay = _http_retry_delay(exc, _RETRY_DELAYS_SECONDS[attempt], clock())
        except (OSError, URLError) as exc:
            if attempt == attempts - 1:
                raise RuntimeError(f"could not verify GitHub release state: {exc}") from exc
            retry_delay = _RETRY_DELAYS_SECONDS[attempt]
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"could not verify GitHub release state: {exc}") from exc
        sleeper(retry_delay)
    if not isinstance(payload, dict):
        raise RuntimeError(f"GitHub returned a non-object response for {url}")
    return payload


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
    """Validate that the release request still owns the current main commit."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument(
        "--api-url", default=os.environ.get("GITHUB_API_URL", "https://api.github.com")
    )
    parser.add_argument("--expected-main-sha", required=True)
    args = parser.parse_args()
    if not args.repository:
        raise SystemExit("--repository or GITHUB_REPOSITORY is required")
    try:
        validate_main_sha(
            args.repository,
            args.expected_main_sha,
            token=os.environ.get("GITHUB_TOKEN", ""),
            api_url=args.api_url,
        )
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    print(f"GitHub release state passed: main={args.expected_main_sha}")


if __name__ == "__main__":
    main()
