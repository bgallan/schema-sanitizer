#!/usr/bin/env python3
"""Install an exact owner-lock selection through pip's hash-checking mode.

The command renders a temporary hashed constraint for the complete owner environment,
then asks pip to install either the complete flat lock or a named dependency closure.
Artifact bytes outside the checked-in SHA-256 allowlist are therefore never accepted.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path
from typing import Sequence

if __package__:
    from .locked_requirements import (
        read_artifact_lock,
        read_owner_lock,
        render_hashed_requirements,
        select_requirements,
    )
    from .run_bounded_command import run_bounded
else:
    from locked_requirements import (  # type: ignore[no-redef]
        read_artifact_lock,
        read_owner_lock,
        render_hashed_requirements,
        select_requirements,
    )
    from run_bounded_command import run_bounded  # type: ignore[no-redef]

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ARTIFACT_LOCK = ROOT / "meta/ci/requirements/python-artifact-sha256.lock"


def install_locked_requirements(
    lock: Path,
    artifact_lock: Path,
    package_names: Sequence[str],
    *,
    install_all: bool,
    download: Path | None = None,
    allow_sdist: Sequence[str] = (),
) -> None:
    """Install or download one owner selection through pip's hash-checking mode."""
    requirements = read_owner_lock(lock)
    hashes = read_artifact_lock(artifact_lock)
    selected = requirements if install_all else select_requirements(requirements, package_names)
    source_requirements = select_requirements(requirements, allow_sdist) if allow_sdist else ()
    selected_names = {requirement.name for requirement in selected}
    source_names = {requirement.name for requirement in source_requirements}
    if not source_names <= selected_names:
        missing = ", ".join(sorted(source_names - selected_names))
        raise ValueError(f"sdist exceptions must be part of the selected install: {missing}")
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-pip-") as temporary_name:
        temporary = Path(temporary_name)
        hashed_lock = temporary / "hashed-constraints.txt"
        selected_requirements = temporary / "selected-requirements.txt"
        hashed_lock.write_text(render_hashed_requirements(requirements, hashes), encoding="utf-8")
        selected_requirements.write_text(
            render_hashed_requirements(selected, hashes),
            encoding="utf-8",
        )
        command = [
            sys.executable,
            "-m",
            "pip",
            "download" if download is not None else "install",
            "--require-hashes",
            "--only-binary=:all:",
        ]
        for name in sorted(source_names):
            command.extend(("--no-binary", name))
        if source_names:
            command.extend(("--no-build-isolation", "--no-cache-dir"))
        if install_all:
            command.append("--no-deps")
        command.extend(
            [
                "--constraint",
                os.fspath(hashed_lock),
                "--requirement",
                os.fspath(selected_requirements),
            ]
        )
        if download is not None:
            download.mkdir(parents=True, exist_ok=True)
            command.extend(("--dest", os.fspath(download)))
        run_bounded(command, timeout_seconds=900, label="hash-locked-pip")


def main(argv: Sequence[str] | None = None) -> int:
    """Parse the owner selection, perform the hashed install, and return success."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--artifact-lock", type=Path, default=DEFAULT_ARTIFACT_LOCK)
    parser.add_argument(
        "--download",
        type=Path,
        help="Download the authenticated closure instead of installing it.",
    )
    parser.add_argument(
        "--allow-sdist",
        action="append",
        default=[],
        help="Allow one explicitly named, hash-locked source distribution.",
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--all", action="store_true", dest="install_all")
    selection.add_argument("--packages", nargs="+")
    args = parser.parse_args(argv)
    install_locked_requirements(
        args.lock,
        args.artifact_lock,
        args.packages or (),
        install_all=args.install_all,
        download=args.download,
        allow_sdist=args.allow_sdist,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
