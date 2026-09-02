"""Validate command execution for local benchmark isolation.

Local benchmarks need fresh processes on the same host, so this module provides an
argv-only boundary that isolates native modules, CPU affinity, and performance counters.
Every invocation has a deadline; timeout cleanup kills the POSIX process domain or
Windows process tree and reaps the direct child under a second fixed deadline.
"""

from __future__ import annotations

import asyncio
import ctypes
import math
import os
import shutil
import signal
import tempfile
from collections.abc import Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import IO, Any, Generic, TypeAlias, TypeVar, cast

PathArgument: TypeAlias = str | os.PathLike[str]
Output = TypeVar("Output", str, bytes)
_POST_KILL_REAP_TIMEOUT_SECONDS = 5.0
_WINDOWS_SYSTEM_DIRECTORY_BUFFER_CHARS = 32_768
_PLATFORM_FAMILY = os.name
_PROCESS_DOMAIN_KILL_SIGNAL = getattr(signal, "SIGKILL", signal.SIGTERM)


class CommandError(Exception):
    """Base error raised while starting or waiting for a local command."""


class CommandFailed(CommandError):
    """A checked command returned a non-zero status."""

    def __init__(self, returncode: int, argv: tuple[str, ...]) -> None:
        """Record the failed status and validated argument vector."""
        self.returncode = returncode
        self.argv = argv
        super().__init__(f"command {argv[0]!r} returned non-zero exit status {returncode}")


class CommandCleanupFailed(CommandError):
    """A timed-out command's process tree could not be killed and reaped."""

    def __init__(self, argv: tuple[str, ...]) -> None:
        """Record the command whose bounded post-kill cleanup failed."""
        self.argv = argv
        super().__init__(f"timed-out command {argv[0]!r} could not be reaped after kill")


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
    return data.decode("utf-8")


def _windows_taskkill_path() -> Path | None:
    """Resolve the system-owned Windows tree-kill executable without consulting PATH."""
    try:
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        get_system_directory = kernel32.GetSystemDirectoryW
        get_system_directory.argtypes = [wintypes.LPWSTR, wintypes.UINT]
        get_system_directory.restype = wintypes.UINT
        buffer = ctypes.create_unicode_buffer(_WINDOWS_SYSTEM_DIRECTORY_BUFFER_CHARS)
        length = int(get_system_directory(buffer, len(buffer)))
    except (AttributeError, OSError, ValueError):
        return None
    if length == 0 or length >= len(buffer):
        return None
    candidate = Path(buffer.value) / "taskkill.exe"
    return candidate if candidate.is_file() else None


async def _kill_process_domain_and_reap(
    process: asyncio.subprocess.Process,
    *,
    argv: tuple[str, ...],
) -> None:
    """Kill one isolated process domain and bound direct-child reaping."""
    cleanup_failed = False
    direct_child_only_kill = False
    if _PLATFORM_FAMILY == "posix":
        try:
            os.killpg(process.pid, _PROCESS_DOMAIN_KILL_SIGNAL)
        except ProcessLookupError:
            pass
    elif _PLATFORM_FAMILY == "nt":
        taskkill = _windows_taskkill_path()
        if taskkill is None or not taskkill.is_file():
            cleanup_failed = True
        else:
            with open(os.devnull, "wb") as sink:
                try:
                    tree_killer = await asyncio.create_subprocess_exec(
                        os.fspath(taskkill),
                        "/PID",
                        str(process.pid),
                        "/T",
                        "/F",
                        stdout=sink,
                        stderr=sink,
                    )
                except OSError:
                    cleanup_failed = True
                else:
                    try:
                        try:
                            await asyncio.wait_for(
                                tree_killer.wait(),
                                timeout=_POST_KILL_REAP_TIMEOUT_SECONDS,
                            )
                        except TimeoutError:
                            cleanup_failed = True
                            try:
                                tree_killer.kill()
                            except ProcessLookupError:
                                pass
                            try:
                                await asyncio.wait_for(
                                    tree_killer.wait(),
                                    timeout=_POST_KILL_REAP_TIMEOUT_SECONDS,
                                )
                            except TimeoutError:
                                pass
                        if tree_killer.returncode != 0:
                            cleanup_failed = True
                    finally:
                        if tree_killer.returncode is None:
                            try:
                                tree_killer.kill()
                            except ProcessLookupError:
                                pass
        if cleanup_failed:
            if process.returncode is None:
                try:
                    process.kill()
                except OSError:
                    pass
                else:
                    direct_child_only_kill = process.returncode is None
    else:
        process.kill()
    try:
        await asyncio.wait_for(
            process.wait(),
            timeout=_POST_KILL_REAP_TIMEOUT_SECONDS,
        )
    except TimeoutError as error:
        raise CommandCleanupFailed(argv) from error
    if cleanup_failed and direct_child_only_kill:
        raise CommandCleanupFailed(argv)


async def _execute(
    argv: tuple[str, ...],
    *,
    cwd: Path | None,
    stdout: StreamMode | None,
    stderr: StreamMode | None,
    timeout: float,
) -> tuple[int, bytes | None, bytes | None]:
    """Execute one validated command with bounded stream-routing choices."""
    if stdout is MERGE_WITH_STDOUT:
        raise ValueError("MERGE_WITH_STDOUT is valid only for stderr")

    with tempfile.TemporaryFile() if stdout is CAPTURE else nullcontext() as stdout_file:
        with tempfile.TemporaryFile() if stderr is CAPTURE else nullcontext() as stderr_file:
            with open(os.devnull, "wb") if stdout is DISCARD else nullcontext() as stdout_null:
                with open(os.devnull, "wb") if stderr is DISCARD else nullcontext() as stderr_null:
                    stdout_target: int | IO[Any] | None = (
                        stdout_file if stdout is CAPTURE else stdout_null
                    )
                    if stderr is MERGE_WITH_STDOUT:
                        if stdout_target is None:
                            raise ValueError("stderr merging requires captured or discarded stdout")
                        stderr_target: int | IO[Any] | None = stdout_target
                    else:
                        stderr_target = stderr_file if stderr is CAPTURE else stderr_null

                    process = await asyncio.create_subprocess_exec(
                        *argv,
                        cwd=os.fspath(cwd) if cwd is not None else None,
                        stdout=stdout_target,
                        stderr=stderr_target,
                        start_new_session=_PLATFORM_FAMILY == "posix",
                    )
                    try:
                        await asyncio.wait_for(process.wait(), timeout=timeout)
                    except TimeoutError:
                        await _kill_process_domain_and_reap(process, argv=argv)
                        raise

                    captured_stdout = None
                    if stdout_file is not None:
                        stdout_file.seek(0)
                        captured_stdout = stdout_file.read()
                    captured_stderr = None
                    if stderr_file is not None:
                        stderr_file.seek(0)
                        captured_stderr = stderr_file.read()
                    if process.returncode is None:
                        raise CommandError(f"command {argv[0]!r} exited without a status")
                    return process.returncode, captured_stdout, captured_stderr


def run_command(
    command: Sequence[PathArgument],
    *,
    check: bool = False,
    cwd: PathArgument | None = None,
    stdout: StreamMode | None = None,
    stderr: StreamMode | None = None,
    text: bool = False,
    timeout: float,
) -> CompletedCommand[str] | CompletedCommand[bytes]:
    """Run one validated argv without a shell or environment overrides."""
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("child command timeout must be finite and positive")
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
    if text:
        return CompletedCommand[str](
            args=argv,
            returncode=returncode,
            stdout=cast(str | None, _decode(captured_stdout, text=True)),
            stderr=cast(str | None, _decode(captured_stderr, text=True)),
        )
    return CompletedCommand[bytes](
        args=argv,
        returncode=returncode,
        stdout=cast(bytes | None, _decode(captured_stdout, text=False)),
        stderr=cast(bytes | None, _decode(captured_stderr, text=False)),
    )
