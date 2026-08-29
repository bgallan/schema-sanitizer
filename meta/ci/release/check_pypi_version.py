#!/usr/bin/env python3
"""Fail the release gate when the selected version already exists on PyPI.

It queries the PyPI JSON API with bounded failure handling and rejects versions that
have already been published.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable, Mapping
from http.client import HTTPException, HTTPSConnection
from pathlib import Path
from urllib.parse import quote, urlsplit

_RETRY_DELAYS_SECONDS = (1.0, 2.0)
_TRANSIENT_HTTP_STATUSES = {429, *range(500, 600)}

if __package__:
    from .validate_release_version import validate_release_version
else:
    from validate_release_version import validate_release_version


def _pypi_https_json(hostname: str, target: str, headers: Mapping[str, str]) -> tuple[int, object]:
    """Issue one bounded request to an explicitly allowed PyPI HTTPS origin."""
    if hostname == "pypi.org":
        connection = HTTPSConnection("pypi.org", timeout=20)
    elif hostname == "test.pypi.org":
        connection = HTTPSConnection("test.pypi.org", timeout=20)
    else:
        raise RuntimeError(f"refusing non-PyPI HTTPS host: {hostname!r}")
    try:
        connection.request("GET", target, headers=dict(headers))
        response = connection.getresponse()
        status = response.status
        body = response.read()
    finally:
        connection.close()
    payload: object = json.loads(body) if status == 200 else None
    return status, payload


def pypi_release_exists(
    project: str,
    version: str,
    *,
    index_url: str = "https://pypi.org/pypi",
    sleeper: Callable[[float], object] = time.sleep,
) -> bool:
    """Return whether PyPI exposes an exact project/version JSON resource."""
    try:
        parsed_index = urlsplit(index_url)
        port = parsed_index.port
    except ValueError as exc:
        raise RuntimeError(f"refusing non-PyPI HTTPS index URL: {index_url!r}") from exc
    if (
        parsed_index.scheme != "https"
        or parsed_index.hostname not in {"pypi.org", "test.pypi.org"}
        or parsed_index.username is not None
        or parsed_index.password is not None
        or port not in {None, 443}
        or parsed_index.path.rstrip("/") != "/pypi"
        or parsed_index.query
        or parsed_index.fragment
    ):
        raise RuntimeError(f"refusing non-PyPI HTTPS index URL: {index_url!r}")
    hostname = parsed_index.hostname
    target = (
        f"{parsed_index.path.rstrip('/')}/{quote(project, safe='')}/{quote(version, safe='')}/json"
    )
    if not target.startswith("/"):
        target = f"/{target}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "schema-sanitizer-release-preflight",
    }
    attempts = len(_RETRY_DELAYS_SECONDS) + 1
    payload: object = None
    for attempt in range(attempts):
        try:
            status, payload = _pypi_https_json(hostname, target, headers)
            if status == 200:
                break
            if status == 404:
                return False
            if status not in _TRANSIENT_HTTP_STATUSES or attempt == attempts - 1:
                raise RuntimeError(f"PyPI returned HTTP {status} for {project} {version}")
        except (OSError, HTTPException) as exc:
            if attempt == attempts - 1:
                raise RuntimeError(f"could not verify {project} {version} on PyPI: {exc}") from exc
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"could not verify {project} {version} on PyPI: {exc}") from exc
        sleeper(_RETRY_DELAYS_SECONDS[attempt])
    if not isinstance(payload, dict) or not isinstance(payload.get("info"), dict):
        raise RuntimeError(f"PyPI returned malformed JSON for {project} {version}")
    reported_version = payload["info"].get("version")
    if reported_version != version:
        raise RuntimeError(
            f"PyPI returned version {reported_version!r} for exact resource {project} {version}"
        )
    return True


def main() -> None:
    """Validate the local version and reject an existing immutable release."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version-file", type=Path, default=Path("meta/VERSION"))
    parser.add_argument("--project", default="schema-sanitizer")
    parser.add_argument("--index-url", default="https://pypi.org/pypi")
    args = parser.parse_args()
    try:
        version = validate_release_version(args.version_file)
        exists = pypi_release_exists(args.project, version, index_url=args.index_url)
    except (OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    if exists:
        raise SystemExit(
            f"Refusing release: {args.project} {version} already exists on PyPI; "
            "published files are immutable. Increment meta/VERSION."
        )
    print(f"PyPI version is available: {args.project} {version}")


if __name__ == "__main__":
    main()
