"""Idempotently enforce the exact pip version used by repository automation.

CI runners frequently already provide the required pip wheel.  This helper avoids a
redundant installer invocation while retaining an exact, fail-closed postcondition
when a runner image carries any other version.
"""

from __future__ import annotations

import importlib.metadata
import os
import sys

PIP_VERSION = "26.2.1"


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

    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--no-deps",
        "--only-binary=:all:",
        f"pip=={PIP_VERSION}",
    ]
    status = os.spawnv(os.P_WAIT, sys.executable, command)
    if status != 0:
        raise RuntimeError(f"pip bootstrap failed with status {status}")
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
