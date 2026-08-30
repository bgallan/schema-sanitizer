#!/usr/bin/env python3
"""Run one sanitizer process behind a deterministic process-tree watchdog.

The wrapper streams child output directly to CI, bounds startup and teardown,
terminates the complete POSIX process group (or Windows process tree) on timeout,
and records canonical machine-readable evidence only after clean completion.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess  # nosec B404
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

CERTIFICATE_FORMAT = "schema-sanitizer-sanitizer-watchdog-v1"
TERMINATION_GRACE_SECONDS = 10
SANITIZER_ENVIRONMENT_NAMES = (
    "ASAN_OPTIONS",
    "LSAN_OPTIONS",
    "TSAN_OPTIONS",
    "UBSAN_OPTIONS",
)


def _canonical_json(payload: Mapping[str, Any]) -> str:
    """Serialize watchdog evidence with stable ordering and whitespace."""
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _write_atomically(destination: Path, content: str) -> None:
    """Write one evidence certificate atomically without following symlinks."""
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise ValueError(f"watchdog certificate output is unsafe: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
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


def _posix_group_is_live(process_group: int) -> bool:
    """Return whether a POSIX group contains a non-zombie process."""
    try:
        process_table = subprocess.run(  # nosec B603
            ["/bin/ps", "-e", "-o", "pgid=,stat="],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        # Inspection failure is unknown state and therefore fails closed.
        return True
    for line in process_table.splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[0] == str(process_group) and not fields[1].startswith("Z"):
            return True
    return False


def _wait_until(predicate: Any, timeout_seconds: int) -> bool:
    """Poll one cleanup predicate against a monotonic bounded deadline."""
    deadline = time.monotonic() + timeout_seconds
    while predicate():
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.05)
    return True


def _terminate_posix_group(process: subprocess.Popen[bytes]) -> None:
    """Terminate and then kill a POSIX child process group within fixed bounds."""
    process_group = process.pid
    if _posix_group_is_live(process_group):
        try:
            os.killpg(process_group, signal.SIGTERM)
        except ProcessLookupError:
            pass
    if not _wait_until(lambda: _posix_group_is_live(process_group), TERMINATION_GRACE_SECONDS):
        try:
            os.killpg(process_group, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if not _wait_until(lambda: _posix_group_is_live(process_group), TERMINATION_GRACE_SECONDS):
            raise RuntimeError(f"sanitizer process group resisted termination: {process_group}")
    try:
        process.wait(timeout=TERMINATION_GRACE_SECONDS)
    except subprocess.TimeoutExpired as error:
        raise RuntimeError("sanitizer process leader resisted termination") from error


def _terminate_windows_tree(process: subprocess.Popen[bytes]) -> None:
    """Terminate a Windows process and all descendants with the system tree tool."""
    system_root = Path(os.environ.get("SystemRoot", r"C:\Windows")).resolve()
    taskkill = (system_root / "System32" / "taskkill.exe").resolve(strict=True)
    if taskkill.is_symlink() or not taskkill.is_file() or system_root not in taskkill.parents:
        raise RuntimeError(f"Windows taskkill executable is unsafe: {taskkill}")
    result = subprocess.run(  # nosec B603
        [os.fspath(taskkill), "/PID", str(process.pid), "/T", "/F"],
        check=False,
        timeout=TERMINATION_GRACE_SECONDS,
    )
    if result.returncode not in {0, 128}:
        raise RuntimeError(f"taskkill failed for sanitizer process tree: {result.returncode}")
    try:
        process.wait(timeout=TERMINATION_GRACE_SECONDS)
    except subprocess.TimeoutExpired as error:
        raise RuntimeError("Windows sanitizer process tree resisted termination") from error


def _terminate_process_tree(process: subprocess.Popen[bytes]) -> None:
    """Terminate the complete child process tree for the active operating system."""
    if os.name == "nt":
        _terminate_windows_tree(process)
    else:
        _terminate_posix_group(process)


def _portable_command(command: Sequence[str]) -> list[str]:
    """Remove workspace-specific prefixes from command evidence."""
    workspace = Path.cwd().resolve()
    portable: list[str] = []
    for argument in command:
        try:
            path = Path(argument)
            resolved = path.resolve()
            portable.append(resolved.relative_to(workspace).as_posix())
        except (OSError, ValueError):
            portable.append(argument)
    return portable


def _environment_assignment(raw: str) -> tuple[str, str]:
    """Parse one explicitly assigned sanitizer environment variable."""
    name, separator, value = raw.partition("=")
    if not separator or name not in SANITIZER_ENVIRONMENT_NAMES:
        expected = ", ".join(SANITIZER_ENVIRONMENT_NAMES)
        raise argparse.ArgumentTypeError(f"sanitizer environment must assign one of {expected}")
    if "\n" in value or "\r" in value:
        raise argparse.ArgumentTypeError("sanitizer environment values must be one line")
    return name, value


def _sanitizer_environment(
    assignments: Sequence[tuple[str, str]],
) -> dict[str, str]:
    """Require exactly one explicit value for every sanitizer runtime variable."""
    environment: dict[str, str] = {}
    for name, value in assignments:
        if name in environment:
            raise ValueError(f"duplicate sanitizer environment assignment: {name}")
        environment[name] = value
    observed = set(environment)
    expected = set(SANITIZER_ENVIRONMENT_NAMES)
    if observed != expected:
        raise ValueError(
            "sanitizer environment inventory mismatch: "
            f"missing={sorted(expected - observed)}, extra={sorted(observed - expected)}"
        )
    return environment


def run_guarded(
    command: Sequence[str],
    *,
    timeout_seconds: int,
    label: str,
    certificate: Path,
    sanitizer_environment: Mapping[str, str],
) -> int:
    """Run a child with exact sanitizer options and record bounded success."""
    if not command or not command[0]:
        raise ValueError("watchdog command must not be empty")
    environment = _sanitizer_environment(tuple(sanitizer_environment.items()))
    child_environment = os.environ.copy()
    child_environment.update(environment)
    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    process = subprocess.Popen(  # nosec B603
        list(command),
        creationflags=creationflags,
        env=child_environment,
        start_new_session=os.name != "nt",
    )
    try:
        return_code = process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        print(f"sanitizer watchdog timed out after {timeout_seconds}s: {label}", file=sys.stderr)
        _terminate_process_tree(process)
        return 124
    if os.name != "nt" and _posix_group_is_live(process.pid):
        print(f"sanitizer command leaked descendants after exit: {label}", file=sys.stderr)
        _terminate_posix_group(process)
        return 125
    if return_code != 0:
        return return_code
    evidence = {
        "command": _portable_command(command),
        "format": CERTIFICATE_FORMAT,
        "label": label,
        "platform": sys.platform,
        "sanitizer_environment": environment,
        "status": "passed",
        "timeout_seconds": timeout_seconds,
    }
    _write_atomically(certificate, _canonical_json(evidence))
    print(f"sanitizer watchdog passed: {label}")
    return 0


def _positive_integer(raw: str) -> int:
    """Parse one strictly positive command-line integer."""
    value = int(raw)
    if value < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return value


def main() -> None:
    """Parse and execute one guarded sanitizer command."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--certificate", required=True, type=Path)
    parser.add_argument(
        "--environment",
        action="append",
        default=[],
        type=_environment_assignment,
    )
    parser.add_argument("--label", required=True)
    parser.add_argument("--timeout", required=True, type=_positive_integer)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    try:
        sanitizer_environment = _sanitizer_environment(args.environment)
        raise SystemExit(
            run_guarded(
                command,
                timeout_seconds=args.timeout,
                label=args.label,
                certificate=args.certificate,
                sanitizer_environment=sanitizer_environment,
            )
        )
    except (OSError, RuntimeError, subprocess.SubprocessError, ValueError) as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
