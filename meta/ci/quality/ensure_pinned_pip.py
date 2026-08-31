"""Idempotently enforce the exact pip version used by repository automation.

CI runners frequently already provide the required pip wheel.  This helper avoids a
redundant installer invocation while retaining an exact, fail-closed postcondition
when a runner image carries any other version.
"""

from __future__ import annotations

import importlib.metadata
import os
import sys
import tempfile
from pathlib import Path

if __package__:
    from .run_bounded_command import run_bounded
else:
    from run_bounded_command import run_bounded  # type: ignore[no-redef]

PIP_VERSION = "26.2.1"
PIP_SHA256 = "71138adf1f4ca900cdb7d289c21b7494329f2332b6d85f0e1c42108c0384ed3e"


def installed_pip_version() -> str | None:
    """Return the installed pip distribution version, or ``None`` when absent."""
    try:
        return importlib.metadata.version("pip")
    except importlib.metadata.PackageNotFoundError:
        return None


def ensure_pinned_pip() -> bool:
    """Install the pinned pip wheel only when needed and verify the final version."""
    current = installed_pip_version()
    if current == PIP_VERSION:
        print(f"pip {PIP_VERSION} already satisfies the repository pin")
        return False

    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-pip-bootstrap-") as directory:
        requirement = Path(directory) / "pip-requirement.txt"
        requirement.write_text(f"pip=={PIP_VERSION} --hash=sha256:{PIP_SHA256}\n", encoding="utf-8")
        command = [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--only-binary=:all:",
            "--require-hashes",
            "--requirement",
            os.fspath(requirement),
        ]
        run_bounded(command, timeout_seconds=300, label="pinned-pip-bootstrap")
    installed = installed_pip_version()
    if installed != PIP_VERSION:
        raise RuntimeError(
            f"pip bootstrap postcondition failed: expected {PIP_VERSION}, got {installed}"
        )
    return True


def main() -> int:
    """Enforce the repository pip pin and return a command-line status code."""
    ensure_pinned_pip()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
