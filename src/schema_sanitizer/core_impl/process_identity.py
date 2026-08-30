"""Build process identities that survive PID reuse and host reboots safely.

Linux process start tokens and boot identifiers are combined with the PID so coordination files
can distinguish a live owner from stale state.
"""

from __future__ import annotations

import os
from pathlib import Path

# This is a public process-identity sentinel, never authentication material.
_UNKNOWN_START_TOKEN = "unknown"  # nosec B105
_BOOT_ID_PATH = Path("/proc/sys/kernel/random/boot_id")


def parse_linux_proc_start_token(raw_stat: str) -> str:
    """Return Linux ``/proc/<pid>/stat`` field 22 without splitting ``comm``."""
    closing = raw_stat.rfind(")")
    if closing < 0:
        return _UNKNOWN_START_TOKEN
    fields = raw_stat[closing + 1 :].strip().split()
    if len(fields) <= 19:
        return _UNKNOWN_START_TOKEN
    token = fields[19]
    return token if token.isdecimal() else _UNKNOWN_START_TOKEN


def linux_boot_id() -> str:
    """Return the current Linux boot identifier in one bounded canonical form."""
    try:
        value = _BOOT_ID_PATH.read_text(encoding="ascii").strip().lower()
    except OSError:
        return _UNKNOWN_START_TOKEN
    if not value or len(value) > 128:
        return _UNKNOWN_START_TOKEN
    if any(character not in "0123456789abcdef-" for character in value):
        return _UNKNOWN_START_TOKEN
    return value


def process_start_token(pid: int) -> str:
    """Return a PID-instance token that also distinguishes Linux reboots."""
    try:
        raw = Path(f"/proc/{int(pid)}/stat").read_text(encoding="ascii")
    except (OSError, ValueError, OverflowError):
        return _UNKNOWN_START_TOKEN
    start = parse_linux_proc_start_token(raw)
    if start == _UNKNOWN_START_TOKEN:
        return start
    boot = linux_boot_id()
    return start if boot == _UNKNOWN_START_TOKEN else f"{boot}:{start}"


def process_identity_matches(recorded: str, current: str) -> bool:
    """Compare canonical composite tokens, conservatively handling unknowns."""
    recorded = str(recorded)
    current = str(current)
    if recorded == _UNKNOWN_START_TOKEN or current == _UNKNOWN_START_TOKEN:
        return True
    return recorded == current


def process_is_alive(pid: int, start_token: str) -> bool:
    """Return whether the recorded process still owns its PID instance."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return process_identity_matches(start_token, process_start_token(pid))


__all__ = [
    "linux_boot_id",
    "parse_linux_proc_start_token",
    "process_identity_matches",
    "process_is_alive",
    "process_start_token",
]
