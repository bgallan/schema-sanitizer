#!/usr/bin/env python3
"""Create or verify the deterministic CI manifest for one PyPI release set."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Iterable

if __package__:
    from .check_distribution_contents import RELEASE_PROJECT, validate_release_set
else:
    from check_distribution_contents import RELEASE_PROJECT, validate_release_set

_GIT_SHA_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_MANIFEST_FORMAT = f"{RELEASE_PROJECT}-release-manifest-v1"


def _positive_integer(value: str) -> int:
    """Parse one positive GitHub run identifier."""
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _sha256(path: Path) -> str:
    """Return the lowercase SHA-256 digest of one regular file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _release_artifacts(paths: Iterable[Path]) -> list[Path]:
    """Return five unique, regular release files in canonical name order."""
    artifacts = sorted(paths, key=lambda path: path.name)
    if len(artifacts) != 5:
        raise AssertionError(f"expected exactly 5 release files, found {len(artifacts)}")
    names = [path.name for path in artifacts]
    if len(set(names)) != len(names):
        raise AssertionError(f"release filenames must be unique: {names}")
    for artifact in artifacts:
        if artifact.is_symlink() or not artifact.is_file():
            raise AssertionError(f"release artifact must be a regular file: {artifact}")
    return artifacts


def build_release_manifest(
    paths: Iterable[Path],
    *,
    version_file: Path,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> dict[str, object]:
    """Validate a release set and return its deterministic audit manifest."""
    if _GIT_SHA_PATTERN.fullmatch(github_sha) is None:
        raise ValueError("github_sha must be a lowercase 40- or 64-character Git object ID")
    if github_run_id < 1 or github_run_attempt < 1:
        raise ValueError("GitHub run ID and attempt must be positive integers")

    version = version_file.read_text(encoding="utf-8").strip()
    artifacts = _release_artifacts(paths)
    release_version = validate_release_set(artifacts, expected_version=version)
    if release_version != version:
        raise AssertionError(
            f"normalized release version {release_version!r} != {version_file} value {version!r}"
        )

    return {
        "artifacts": [
            {
                "filename": artifact.name,
                "sha256": _sha256(artifact),
                "size": artifact.stat().st_size,
            }
            for artifact in artifacts
        ],
        "format": _MANIFEST_FORMAT,
        "project": RELEASE_PROJECT,
        "provenance": {
            "git_sha": github_sha,
            "github_run_attempt": github_run_attempt,
            "github_run_id": github_run_id,
        },
        "version": version,
    }


def canonical_json(manifest: dict[str, object]) -> str:
    """Serialize a manifest with stable key order and whitespace."""
    return json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _verify_serialized_manifest(
    manifest_file: Path,
    expected: dict[str, object],
) -> None:
    """Verify both manifest values and their canonical JSON representation."""
    serialized = manifest_file.read_text(encoding="utf-8")
    actual = json.loads(serialized)
    if actual != expected:
        raise AssertionError(f"{manifest_file}: metadata or artifact digest mismatch")
    if serialized != canonical_json(expected):
        raise AssertionError(f"{manifest_file}: manifest JSON is not canonical")


def write_release_manifest(
    manifest_file: Path,
    paths: Iterable[Path],
    *,
    version_file: Path,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> None:
    """Create a canonical manifest and read it back before returning."""
    expected = build_release_manifest(
        paths,
        version_file=version_file,
        github_sha=github_sha,
        github_run_id=github_run_id,
        github_run_attempt=github_run_attempt,
    )
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file.write_text(canonical_json(expected), encoding="utf-8")
    _verify_serialized_manifest(manifest_file, expected)


def verify_release_manifest(
    manifest_file: Path,
    paths: Iterable[Path],
    *,
    version_file: Path,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> None:
    """Rebuild a manifest from source artifacts and compare it byte-for-byte."""
    expected = build_release_manifest(
        paths,
        version_file=version_file,
        github_sha=github_sha,
        github_run_id=github_run_id,
        github_run_attempt=github_run_attempt,
    )
    _verify_serialized_manifest(manifest_file, expected)


def main() -> None:
    """Run the release-manifest CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("create", "verify"))
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--version-file", type=Path, default=Path("meta/VERSION"))
    parser.add_argument("--github-sha", required=True)
    parser.add_argument("--github-run-id", required=True, type=_positive_integer)
    parser.add_argument("--github-run-attempt", required=True, type=_positive_integer)
    parser.add_argument("artifacts", nargs=5, type=Path)
    args = parser.parse_args()

    options = {
        "version_file": args.version_file,
        "github_sha": args.github_sha,
        "github_run_id": args.github_run_id,
        "github_run_attempt": args.github_run_attempt,
    }
    try:
        if args.operation == "create":
            write_release_manifest(args.manifest, args.artifacts, **options)
            print(f"created and verified {args.manifest}")
        else:
            verify_release_manifest(args.manifest, args.artifacts, **options)
            print(f"verified {args.manifest}")
    except (AssertionError, json.JSONDecodeError, OSError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
