"""Validated command execution for local benchmark isolation.

Local benchmarks need fresh processes on the same host so native modules, CPU affinity,
and performance counters remain isolated. This module provides a validated argv-only
boundary without importing Python's :mod:`subprocess` module.
"""

from __future__ import annotations

import asyncio
import locale
import os
import shutil
import tempfile
from collections.abc import Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Generic, TypeAlias, TypeVar

PathArgument: TypeAlias = str | os.PathLike[str]
Output = TypeVar("Output", str, bytes)


class CommandError(Exception):
    """Base error raised while starting or waiting for a local command."""


class CommandFailed(CommandError):
    """A checked command returned a non-zero status."""

    def __init__(self, returncode: int, argv: tuple[str, ...]) -> None:
        """Record the failed status and validated argument vector."""
        self.returncode = returncode
        self.argv = argv
        super().__init__(f"command {argv[0]!r} returned non-zero exit status {returncode}")


class StreamMode(Enum):
    """Supported child stream routing."""

    CAPTURE = auto()
    DISCARD = auto()
    MERGE_WITH_STDOUT = auto()


CAPTURE = StreamMode.CAPTURE
DISCARD = StreamMode.DISCARD
MERGE_WITH_STDOUT = StreamMode.MERGE_WITH_STDOUT


@dataclass(frozen=True, slots=True)
class CompletedCommand(Generic[Output]):
    """Result returned after one local command exits."""

    args: tuple[str, ...]
    returncode: int
    stdout: Output | None
    stderr: Output | None


def _working_directory(cwd: PathArgument | None) -> Path | None:
    """Resolve and validate an optional child working directory."""
    if cwd is None:
        return None
    directory = Path(cwd).expanduser().resolve(strict=True)
    if not directory.is_dir():
        raise NotADirectoryError(f"child working directory is not a directory: {directory}")
    return directory


def _validated_argv(command: Sequence[PathArgument], *, cwd: Path | None) -> tuple[str, ...]:
    """Return a shell-free argument vector with a validated executable."""
    if isinstance(command, (str, bytes)):
        raise TypeError("child command must be an argument sequence, not a shell string")
    argv = tuple(os.fspath(argument) for argument in command)
    if not argv:
        raise ValueError("child command must not be empty")
    if any(not argument or "\0" in argument for argument in argv):
        raise ValueError("child command arguments must be non-empty and contain no NUL bytes")

    executable = argv[0]
    if os.path.dirname(executable):
        candidate = Path(executable).expanduser()
        if not candidate.is_absolute():
            candidate = (cwd if cwd is not None else Path.cwd()) / candidate
        invoked = Path(os.path.abspath(candidate))
    else:
        discovered = shutil.which(executable)
        if discovered is None:
            raise FileNotFoundError(f"child executable was not found: {executable!r}")
        invoked = Path(os.path.abspath(discovered))
    resolved = invoked.resolve(strict=True)
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise PermissionError(f"child executable is not a regular executable file: {resolved}")
    # Preserve virtual-environment launcher paths: their location selects the
    # adjacent pyvenv.cfg even when the launcher itself is a symlink.
    return (os.fspath(invoked), *argv[1:])


def _decode(data: bytes | None, *, text: bool) -> bytes | str | None:
    """Decode captured bytes when the caller requested text output."""
    if data is None or not text:
        return data
    return data.decode(locale.getpreferredencoding(False))


async def _execute(
    argv: tuple[str, ...],
    *,
    cwd: Path | None,
    stdout: StreamMode | None,
    stderr: StreamMode | None,
    timeout: float | None,
) -> tuple[int, bytes | None, bytes | None]:
    """Execute one validated command with bounded stream-routing choices."""
    if stdout is MERGE_WITH_STDOUT:
        raise ValueError("MERGE_WITH_STDOUT is valid only for stderr")

    with tempfile.TemporaryFile() if stdout is CAPTURE else nullcontext() as stdout_file:
        with tempfile.TemporaryFile() if stderr is CAPTURE else nullcontext() as stderr_file:
            with open(os.devnull, "wb") if stdout is DISCARD else nullcontext() as stdout_null:
                with open(os.devnull, "wb") if stderr is DISCARD else nullcontext() as stderr_null:
                    stdout_target = stdout_file if stdout is CAPTURE else stdout_null
                    if stderr is MERGE_WITH_STDOUT:
                        if stdout_target is None:
                            raise ValueError("stderr merging requires captured or discarded stdout")
                        stderr_target = stdout_target
                    else:
                        stderr_target = stderr_file if stderr is CAPTURE else stderr_null

                    process = await asyncio.create_subprocess_exec(
                        *argv,
                        cwd=os.fspath(cwd) if cwd is not None else None,
                        stdout=stdout_target,
                        stderr=stderr_target,
                    )
                    try:
                        await asyncio.wait_for(process.wait(), timeout=timeout)
                    except TimeoutError:
                        process.kill()
                        await process.wait()
                        raise

                    captured_stdout = None
                    if stdout_file is not None:
                        stdout_file.seek(0)
                        captured_stdout = stdout_file.read()
                    captured_stderr = None
                    if stderr_file is not None:
                        stderr_file.seek(0)
                        captured_stderr = stderr_file.read()
                    return process.returncode, captured_stdout, captured_stderr


def run_command(
    command: Sequence[PathArgument],
    *,
    check: bool = False,
    cwd: PathArgument | None = None,
    stdout: StreamMode | None = None,
    stderr: StreamMode | None = None,
    text: bool = False,
    timeout: float | None = None,
) -> CompletedCommand[str] | CompletedCommand[bytes]:
    """Run one validated argv without a shell or environment overrides."""
    directory = _working_directory(cwd)
    argv = _validated_argv(command, cwd=directory)
    returncode, captured_stdout, captured_stderr = asyncio.run(
        _execute(
            argv,
            cwd=directory,
            stdout=stdout,
            stderr=stderr,
            timeout=timeout,
        )
    )
    if check and returncode != 0:
        raise CommandFailed(returncode, argv)
    return CompletedCommand(
        args=argv,
        returncode=returncode,
        stdout=_decode(captured_stdout, text=text),
        stderr=_decode(captured_stderr, text=text),
    )
