#!/usr/bin/env python3
"""Validate the package version and manual PyPI release controls.

It validates package and requested versions, reads manual-workflow inputs, and requires
explicit publication confirmation.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

VERSION_PATTERN = re.compile(r"\d+\.\d+(?:\.\d+)?(?:\.post\d+)?")
PUBLISH_CONFIRMATION = "publish schema-sanitizer"


def validate_release_version(version_file: Path, release_version: str = "") -> str:
    """Return the validated version or raise ``ValueError``."""
    version = version_file.read_text(encoding="utf-8").strip()
    if VERSION_PATTERN.fullmatch(version) is None:
        raise ValueError(f"Invalid {version_file}: {version}")

    if release_version and release_version != version:
        raise ValueError(
            f"release version ({release_version}) does not match {version_file} ({version})"
        )
    return version


def workflow_inputs_from_event(event_file: Path) -> dict[str, object]:
    """Read manual workflow inputs from a GitHub event payload."""
    payload = json.loads(event_file.read_text(encoding="utf-8"))
    inputs = payload.get("inputs", {})
    return inputs if isinstance(inputs, dict) else {}


def release_version_from_event(event_file: Path) -> str:
    """Read the optional manual release version from a GitHub event payload."""
    inputs = workflow_inputs_from_event(event_file)
    release_version = inputs.get("release_version", "")
    return release_version if isinstance(release_version, str) else ""


def require_release_version(release_version: str) -> None:
    """Reject a release request without an explicit package version."""
    if not release_version:
        raise ValueError("Refusing release: release_version is required.")


def require_publish_confirmation(event_file: Path) -> None:
    """Require the exact manual confirmation phrase from a workflow event."""
    confirmation = workflow_inputs_from_event(event_file).get("confirm_publish")
    if confirmation != PUBLISH_CONFIRMATION:
        raise ValueError(
            f"Refusing upload: confirm_publish must be exactly {PUBLISH_CONFIRMATION!r}."
        )


def main() -> None:
    """Run the release-version validation CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version-file", type=Path, default=Path("meta/VERSION"))
    parser.add_argument("--release-version", default="")
    parser.add_argument("--github-event", type=Path)
    parser.add_argument("--require-release-version", action="store_true")
    parser.add_argument("--require-publish-confirmation", action="store_true")
    args = parser.parse_args()
    try:
        release_version = (
            release_version_from_event(args.github_event)
            if args.github_event
            else args.release_version
        )
        if args.require_release_version:
            require_release_version(release_version)
        version = validate_release_version(args.version_file, release_version)
        if args.require_publish_confirmation:
            if args.github_event is None:
                raise ValueError("--require-publish-confirmation requires --github-event")
            require_publish_confirmation(args.github_event)
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    print(f"package-version={version}")


if __name__ == "__main__":
    main()
