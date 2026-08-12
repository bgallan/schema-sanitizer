#!/usr/bin/env python3
"""Validate the package version and manual PyPI release controls."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

VERSION_PATTERN = re.compile(r"\d+\.\d+(?:\.\d+)?(?:\.post\d+)?")
PUBLISH_CONFIRMATION = "publish schema-sanitizer"


def validate_release_version(version_file: Path, release_tag: str = "") -> str:
    """Return the validated version or raise ``ValueError``."""
    version = version_file.read_text(encoding="utf-8").strip()
    if VERSION_PATTERN.fullmatch(version) is None:
        raise ValueError(f"Invalid {version_file}: {version}")

    expected_tag = f"v{version}"
    if release_tag and release_tag != expected_tag:
        raise ValueError(
            f"release tag ({release_tag}) does not match {version_file} ({expected_tag})"
        )
    return version


def workflow_inputs_from_event(event_file: Path) -> dict[str, object]:
    """Read manual workflow inputs from a GitHub event payload."""
    payload = json.loads(event_file.read_text(encoding="utf-8"))
    inputs = payload.get("inputs", {})
    return inputs if isinstance(inputs, dict) else {}


def release_tag_from_event(event_file: Path) -> str:
    """Read the optional manual release tag from a GitHub event payload."""
    inputs = workflow_inputs_from_event(event_file)
    release_tag = inputs.get("release_tag", "")
    return release_tag if isinstance(release_tag, str) else ""


def require_release_tag(release_tag: str) -> None:
    """Reject a release request without an explicit version tag."""
    if not release_tag:
        raise ValueError("Refusing release: release_tag is required.")


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
    parser.add_argument("--release-tag", default="")
    parser.add_argument("--github-event", type=Path)
    parser.add_argument("--require-release-tag", action="store_true")
    parser.add_argument("--require-publish-confirmation", action="store_true")
    args = parser.parse_args()
    try:
        release_tag = (
            release_tag_from_event(args.github_event) if args.github_event else args.release_tag
        )
        if args.require_release_tag:
            require_release_tag(release_tag)
        version = validate_release_version(args.version_file, release_tag)
        if args.require_publish_confirmation:
            if args.github_event is None:
                raise ValueError("--require-publish-confirmation requires --github-event")
            require_publish_confirmation(args.github_event)
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    print(f"package-version={version}")


if __name__ == "__main__":
    main()
