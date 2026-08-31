"""Route ordinary CI child processes through the process-tree-safe watchdog.

This adapter deliberately does not own subprocess creation.  It launches the sole
reviewed watchdog entry point with fixed sanitizer-neutral state, converts its bounded
status codes into explicit exceptions, and keeps timeout/process-tree policy centralized.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[3]
WATCHDOG = ROOT / "meta/ci/sanitizers/run_with_watchdog.py"
_SANITIZER_ENVIRONMENT = ("ASAN_OPTIONS", "LSAN_OPTIONS", "TSAN_OPTIONS", "UBSAN_OPTIONS")


class BoundedCommandError(RuntimeError):
    """Report a bounded child command's exact nonzero exit status."""

    def __init__(self, status: int, label: str) -> None:
        """Preserve the status so callers can distinguish documented outcomes."""
        self.status = status
        super().__init__(f"bounded command failed with status {status}: {label}")


def run_bounded(command: Sequence[str], *, timeout_seconds: int, label: str) -> None:
    """Run an argv through the watchdog and reject timeout, leaks, or nonzero exit."""
    if timeout_seconds < 1:
        raise ValueError("bounded command timeout must be positive")
    if not command or not command[0]:
        raise ValueError("bounded command cannot be empty")
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-watchdog-") as directory:
        certificate = Path(directory) / "certificate.json"
        watchdog_command = [
            sys.executable,
            os.fspath(WATCHDOG),
            "--certificate",
            os.fspath(certificate),
            "--label",
            label,
            "--timeout",
            str(timeout_seconds),
        ]
        for name in _SANITIZER_ENVIRONMENT:
            watchdog_command.extend(("--environment", f"{name}="))
        watchdog_command.extend(("--", *command))
        status = os.spawnv(os.P_WAIT, sys.executable, watchdog_command)  # nosec B606
        if status == 124:
            raise TimeoutError(f"bounded command timed out after {timeout_seconds}s: {label}")
        if status == 125:
            raise RuntimeError(f"bounded command leaked descendants: {label}")
        if status != 0:
            raise BoundedCommandError(status, label)
        if not certificate.is_file() or certificate.is_symlink():
            raise RuntimeError(f"bounded command produced no safe certificate: {label}")
