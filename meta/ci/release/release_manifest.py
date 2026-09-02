#!/usr/bin/env python3
"""Create or verify the deterministic CI manifest for one PyPI release set.

It inventories release artifacts, hashes their bytes, serializes canonical JSON, and
verifies a saved manifest without ambiguity.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import tempfile
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


def _artifact_metadata(path: Path) -> dict[str, object]:
    """Hash one stable regular artifact and return its manifest metadata."""
    path_identity = path.stat(follow_symlinks=False)
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        opened_identity = os.fstat(handle.fileno())
        if not stat.S_ISREG(opened_identity.st_mode) or (
            opened_identity.st_dev,
            opened_identity.st_ino,
        ) != (path_identity.st_dev, path_identity.st_ino):
            raise AssertionError(f"release artifact changed before hashing: {path}")
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
        final_identity = os.fstat(handle.fileno())
    current_identity = path.stat(follow_symlinks=False)
    identity_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
    if any(
        getattr(opened_identity, field) != getattr(final_identity, field)
        or getattr(final_identity, field) != getattr(current_identity, field)
        for field in identity_fields
    ):
        raise AssertionError(f"release artifact changed while hashing: {path}")
    return {
        "filename": path.name,
        "sha256": digest.hexdigest(),
        "size": final_identity.st_size,
    }


def _write_text_atomically(destination: Path, content: str) -> None:
    """Replace one regular text output atomically and skip unchanged bytes."""
    payload = content.encode("utf-8")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise AssertionError(f"manifest output must not be a symlink: {destination}")
    if destination.exists() and not destination.is_file():
        raise AssertionError(f"manifest output must be a regular file: {destination}")
    if destination.is_file() and destination.read_bytes() == payload:
        return
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


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
    if (
        isinstance(github_run_id, bool)
        or not isinstance(github_run_id, int)
        or isinstance(github_run_attempt, bool)
        or not isinstance(github_run_attempt, int)
        or github_run_id < 1
        or github_run_attempt < 1
    ):
        raise ValueError("GitHub run ID and attempt must be positive integers")

    if version_file.is_symlink() or not version_file.is_file():
        raise AssertionError(f"version source must be a regular file: {version_file}")
    version = version_file.read_text(encoding="utf-8").strip()
    artifacts = _release_artifacts(paths)
    release_version = validate_release_set(artifacts, expected_version=version)
    if release_version != version:
        raise AssertionError(
            f"normalized release version {release_version!r} != {version_file} value {version!r}"
        )

    return {
        "artifacts": [_artifact_metadata(artifact) for artifact in artifacts],
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
    if manifest_file.is_symlink() or not manifest_file.is_file():
        raise AssertionError(f"manifest must be a regular file: {manifest_file}")
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
    _write_text_atomically(manifest_file, canonical_json(expected))
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
