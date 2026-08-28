#!/usr/bin/env python3
"""Fail the release gate when the selected version already exists on PyPI.

It queries the PyPI JSON API with bounded failure handling and rejects versions that
have already been published.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

_RETRY_DELAYS_SECONDS = (1.0, 2.0)
_TRANSIENT_HTTP_STATUSES = {429, *range(500, 600)}

if __package__:
    from .validate_release_version import validate_release_version
else:
    from validate_release_version import validate_release_version


def pypi_release_exists(
    project: str,
    version: str,
    *,
    index_url: str = "https://pypi.org/pypi",
    sleeper: Callable[[float], object] = time.sleep,
) -> bool:
    """Return whether PyPI exposes an exact project/version JSON resource."""
    parsed_index = urlsplit(index_url)
    if parsed_index.scheme != "https" or parsed_index.hostname not in {
        "pypi.org",
        "test.pypi.org",
    }:
        raise RuntimeError(f"refusing non-PyPI HTTPS index URL: {index_url!r}")
    resource = f"{index_url.rstrip('/')}/{quote(project, safe='')}/{quote(version, safe='')}/json"
    request = Request(
        resource,
        headers={"Accept": "application/json", "User-Agent": "schema-sanitizer-release-preflight"},
    )
    attempts = len(_RETRY_DELAYS_SECONDS) + 1
    for attempt in range(attempts):
        try:
            # The parsed index is restricted to two HTTPS PyPI hosts above.
            with urlopen(request, timeout=20) as response:  # nosec B310
                payload = json.load(response)
            break
        except HTTPError as exc:
            if exc.code == 404:
                return False
            if exc.code not in _TRANSIENT_HTTP_STATUSES or attempt == attempts - 1:
                raise RuntimeError(
                    f"PyPI returned HTTP {exc.code} for {project} {version}"
                ) from exc
        except (OSError, URLError) as exc:
            if attempt == attempts - 1:
                raise RuntimeError(f"could not verify {project} {version} on PyPI: {exc}") from exc
        except json.JSONDecodeError as exc:
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
