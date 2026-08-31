#!/usr/bin/env python3
"""Dispatch every local pre-commit hook through one authenticated tool environment.

An already exact CI environment takes a zero-install fast path, while an ordinary
developer environment receives a content-addressed virtual environment below ``.work``.
The active environment is never mutated, and a cross-process lock prevents partial or
concurrent bootstraps from becoming observable.
"""

from __future__ import annotations

import contextlib
import errno
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import shutil
import struct
import sys
import time
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import BinaryIO

if __package__:
    from .run_bounded_command import run_bounded
else:
    sys.path.insert(0, os.fspath(Path(__file__).resolve().parent))
    from run_bounded_command import run_bounded  # type: ignore[no-redef]

ROOT = Path(__file__).resolve().parents[3]
HOOK_LOCK = ROOT / "meta/ci/requirements/pre-commit-hooks.txt"
ARTIFACT_LOCK = ROOT / "meta/ci/requirements/python-artifact-sha256.lock"
READY_SCHEMA = 1
LOCK_TIMEOUT_SECONDS = 3_600
CERTIFIED_CURRENT_ENVIRONMENT = "SCHEMA_SANITIZER_PRE_COMMIT_CERTIFIED_CURRENT"
_READY_NAME = "pre-commit-tools-ready.json"
_BOOTSTRAP_PACKAGES = ("setuptools", "wheel", "requests", "semver")
_INSTALL_ENVIRONMENT_REMOVALS = (
    "PIP_BUILD_CONSTRAINT",
    "PIP_CONSTRAINT",
    "PIP_PREFIX",
    "PIP_REQUIRE_VIRTUALENV",
    "PIP_TARGET",
    "PIP_USER",
    "PYTHONHOME",
    "PYTHONPATH",
)
_TOOL_COMMANDS: dict[str, tuple[str, ...]] = {
    "action-sha-pins": (":python", "meta/ci/quality/check_action_pins.py"),
    "actionlint": ("actionlint",),
    "check-json": ("check-json",),
    "check-toml": ("check-toml",),
    "check-yaml": ("check-yaml",),
    "clang-format": ("clang-format",),
    "clang-format-check": ("clang-format",),
    "cmake-format": ("cmake-format",),
    "cmake-format-check": ("cmake-format",),
    "end-of-file-fixer": ("end-of-file-fixer",),
    "fuzz-corpus-integrity": (":python", "meta/ci/fuzz/check_fuzz_corpus.py"),
    "mdformat": ("mdformat",),
    "mdformat-check": ("mdformat",),
    "mypy": ("mypy",),
    "pretty-format-json": ("pretty-format-json",),
    "primary-cleanup-safety": (":python", "meta/ci/quality/check_primary_cleanup.py"),
    "ruff-check": ("ruff", "check"),
    "ruff-format": ("ruff", "format"),
    "shellcheck": ("shellcheck",),
    "shfmt": ("shfmt",),
    "toml-sort": ("toml-sort",),
    "trailing-whitespace": ("trailing-whitespace-fixer",),
    "yamlfix": ("yamlfix",),
    "zizmor": ("zizmor",),
}
_REQUIRED_EXECUTABLES = tuple(
    sorted({command[0] for command in _TOOL_COMMANDS.values() if command[0] != ":python"})
)
_FINGERPRINT_INPUTS = (
    Path(__file__).resolve(),
    ROOT / "meta/ci/quality/ensure_pinned_pip.py",
    ROOT / "meta/ci/quality/install_locked_requirements.py",
    ROOT / "meta/ci/quality/locked_requirements.py",
    ROOT / "meta/ci/quality/run_bounded_command.py",
    ROOT / "meta/ci/sanitizers/run_with_watchdog.py",
    HOOK_LOCK,
    ARTIFACT_LOCK,
)


def _interpreter_identity() -> str:
    """Return the exact public interpreter and host identity for cache ownership."""
    fields = (
        sys.implementation.name,
        sys.implementation.cache_tag or "no-cache-tag",
        platform.python_version(),
        str(8 * struct.calcsize("P")),
        platform.system(),
        platform.machine(),
    )
    return "-".join(fields)


def _environment_fingerprint() -> str:
    """Digest every input that can affect the isolated hook environment."""
    digest = hashlib.sha256()
    for value in (
        f"schema={READY_SCHEMA}",
        f"root={ROOT.resolve()}",
        f"executable={Path(sys.executable).resolve()}",
        f"base-prefix={Path(sys.base_prefix).resolve()}",
        f"identity={_interpreter_identity()}",
    ):
        digest.update(value.encode())
        digest.update(b"\0")
    for path in _FINGERPRINT_INPUTS:
        digest.update(path.relative_to(ROOT).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _tools_root() -> Path:
    """Create and return the non-symlinked repository-owned tool-cache root."""
    work = ROOT / ".work"
    tools = work / "pre-commit-tools"
    for path in (work, tools):
        if path.is_symlink():
            raise RuntimeError(f"pre-commit tool-cache path cannot be a symlink: {path}")
    tools.mkdir(parents=True, exist_ok=True)
    if tools.resolve().parent != work.resolve() or work.resolve().parent != ROOT.resolve():
        raise RuntimeError(f"pre-commit tool cache escaped the repository: {tools}")
    return tools


def _environment_path(fingerprint: str) -> Path:
    """Return the content-addressed virtual-environment path for one fingerprint."""
    identity = re.sub(r"[^a-z0-9]+", "-", _interpreter_identity().lower()).strip("-")
    return _tools_root() / f"{identity}-{fingerprint[:20]}"


def _environment_python(environment: Path, *, windows: bool | None = None) -> Path:
    """Return the platform-native Python executable inside one virtual environment."""
    is_windows = os.name == "nt" if windows is None else windows
    relative = Path("Scripts/python.exe") if is_windows else Path("bin/python")
    return environment / relative


def _executable_path(python: Path, name: str) -> Path | None:
    """Resolve one console script only from the selected interpreter's script directory."""
    resolved = shutil.which(name, path=os.fspath(python.parent))
    return Path(resolved).resolve() if resolved is not None else None


def _applicable_locked_versions() -> dict[str, str] | None:
    """Parse exact applicable hook pins, or return ``None`` without Packaging."""
    try:
        from packaging.requirements import Requirement
    except ImportError:
        return None
    versions: dict[str, str] = {}
    for raw_line in HOOK_LOCK.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        requirement = Requirement(line)
        if requirement.marker is not None and not requirement.marker.evaluate():
            continue
        specifiers = tuple(requirement.specifier)
        if len(specifiers) != 1 or specifiers[0].operator != "==":
            return None
        versions[requirement.name] = specifiers[0].version
    return versions


def _current_environment_is_exact() -> bool:
    """Report whether the running interpreter owns every exact hook pin and command."""
    versions = _applicable_locked_versions()
    if versions is None:
        return False
    try:
        if any(importlib.metadata.version(name) != version for name, version in versions.items()):
            return False
    except importlib.metadata.PackageNotFoundError:
        return False
    python = Path(sys.executable).absolute()
    return all(_executable_path(python, name) is not None for name in _REQUIRED_EXECUTABLES)


def _ready_payload(fingerprint: str) -> dict[str, object]:
    """Return the canonical readiness certificate for one tool environment."""
    return {
        "fingerprint": fingerprint,
        "interpreter": _interpreter_identity(),
        "schema": READY_SCHEMA,
    }


def _write_ready(environment: Path, fingerprint: str) -> None:
    """Publish the readiness certificate atomically as the final bootstrap action."""
    destination = environment / _READY_NAME
    temporary = environment / f".{_READY_NAME}.{os.getpid()}.tmp"
    content = json.dumps(_ready_payload(fingerprint), indent=2, sort_keys=True) + "\n"
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _has_ready_certificate(environment: Path, fingerprint: str) -> bool:
    """Report whether an environment has the exact final readiness certificate."""
    certificate = environment / _READY_NAME
    if certificate.is_symlink() or not certificate.is_file():
        return False
    try:
        payload = json.loads(certificate.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return payload == _ready_payload(fingerprint)


def _spawn(python: Path, arguments: Sequence[str], environment: dict[str, str]) -> int:
    """Run one explicit interpreter command with no shell or ambient executable lookup."""
    argv = [os.fspath(python), *arguments]
    return os.spawnve(  # nosec B606
        os.P_WAIT,
        os.fspath(python),
        argv,
        environment,
    )


def _runtime_environment(python: Path) -> dict[str, str]:
    """Return an environment that selects only the chosen virtual environment first."""
    environment = os.environ.copy()
    environment.pop("PYTHONHOME", None)
    environment.pop("PYTHONPATH", None)
    environment["PATH"] = os.pathsep.join(
        (os.fspath(python.parent), environment.get("PATH", ""))
    ).rstrip(os.pathsep)
    environment["VIRTUAL_ENV"] = os.fspath(python.parent.parent)
    return environment


def _external_environment_is_exact(environment: Path, fingerprint: str) -> bool:
    """Validate a certified cached environment through its own interpreter."""
    if not _has_ready_certificate(environment, fingerprint):
        return False
    python = _environment_python(environment)
    if not python.exists():
        return False
    status = _spawn(
        python,
        (os.fspath(Path(__file__).resolve()), "--check-current"),
        _runtime_environment(python),
    )
    return status == 0


def _try_platform_lock(handle: BinaryIO) -> None:
    """Attempt one nonblocking platform-native lock operation."""
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _acquire_lock(handle: BinaryIO) -> None:
    """Retry genuine lock contention within a deadline and reject other errors."""
    deadline = time.monotonic() + LOCK_TIMEOUT_SECONDS
    retryable = {errno.EACCES, errno.EAGAIN, errno.EINTR}
    if hasattr(errno, "EDEADLK"):
        retryable.add(errno.EDEADLK)
    if hasattr(errno, "EWOULDBLOCK"):
        retryable.add(errno.EWOULDBLOCK)
    while True:
        try:
            _try_platform_lock(handle)
            return
        except OSError as error:
            if error.errno not in retryable:
                raise
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "timed out waiting for the pre-commit tool-cache lock"
                ) from error
            time.sleep(0.1)


def _release_lock(handle: BinaryIO) -> None:
    """Release the platform-native byte lock held by this process."""
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextlib.contextmanager
def _environment_lock(fingerprint: str) -> Iterator[None]:
    """Serialize bootstrap, validation, and execution for one cached environment."""
    lock_directory = _tools_root() / ".locks"
    if lock_directory.is_symlink():
        raise RuntimeError(f"pre-commit lock directory cannot be a symlink: {lock_directory}")
    lock_directory.mkdir(exist_ok=True)
    lock_path = lock_directory / f"{fingerprint}.lock"
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(lock_path, flags, 0o600)
    with os.fdopen(descriptor, "r+b", closefd=True) as handle:
        if os.fstat(handle.fileno()).st_size == 0:
            handle.write(b"\0")
            handle.flush()
        _acquire_lock(handle)
        try:
            yield
        finally:
            _release_lock(handle)


@contextlib.contextmanager
def _isolated_install_environment(environment: Path) -> Iterator[None]:
    """Remove ambient install targets while bootstrapping the owned environment."""
    original = os.environ.copy()
    python = _environment_python(environment)
    try:
        for name in _INSTALL_ENVIRONMENT_REMOVALS:
            os.environ.pop(name, None)
        os.environ["PATH"] = os.pathsep.join(
            (os.fspath(python.parent), original.get("PATH", ""))
        ).rstrip(os.pathsep)
        os.environ["VIRTUAL_ENV"] = os.fspath(environment)
        yield
    finally:
        os.environ.clear()
        os.environ.update(original)


def _reset_environment(environment: Path) -> None:
    """Remove only the exact owned cache child after validating its boundary."""
    tools = _tools_root()
    if environment.parent != tools or environment == tools:
        raise RuntimeError(f"refusing to reset an unowned tool environment: {environment}")
    if environment.is_symlink():
        environment.unlink()
    elif environment.exists():
        if not environment.is_dir():
            environment.unlink()
        else:
            shutil.rmtree(environment)


def _run_helper(python: Path, script: Path, arguments: Sequence[str]) -> None:
    """Run one repository helper and preserve its exact nonzero status."""
    status = _spawn(
        python,
        (os.fspath(script), *arguments),
        _runtime_environment(python),
    )
    if status != 0:
        raise RuntimeError(
            f"pre-commit bootstrap helper failed with status {status}: {script.name}"
        )


def _bootstrap_environment(environment: Path, fingerprint: str) -> None:
    """Build, verify, and certify one exact hook environment at its final path."""
    _reset_environment(environment)
    print(f"Bootstrapping hash-locked pre-commit tools in {environment.relative_to(ROOT)}")
    with _isolated_install_environment(environment):
        run_bounded(
            (sys.executable, "-I", "-m", "venv", os.fspath(environment)),
            timeout_seconds=300,
            label="pre-commit-tool-venv",
        )
        python = _environment_python(environment)
        _run_helper(python, ROOT / "meta/ci/quality/ensure_pinned_pip.py", ())
        installer = ROOT / "meta/ci/quality/install_locked_requirements.py"
        _run_helper(
            python,
            installer,
            ("--lock", os.fspath(HOOK_LOCK), "--packages", *_BOOTSTRAP_PACKAGES),
        )
        for attempt in range(3):
            try:
                _run_helper(
                    python,
                    installer,
                    (
                        "--lock",
                        os.fspath(HOOK_LOCK),
                        "--all",
                        "--allow-sdist",
                        "actionlint-py",
                    ),
                )
                break
            except RuntimeError:
                if attempt == 2:
                    raise
                time.sleep(attempt + 1)
        run_bounded(
            (os.fspath(python), "-m", "pip", "check"),
            timeout_seconds=120,
            label="pre-commit-tool-pip-check",
        )
    _write_ready(environment, fingerprint)
    if not _external_environment_is_exact(environment, fingerprint):
        raise RuntimeError("bootstrapped pre-commit tool environment failed certification")


def _execute_hook(python: Path, hook_id: str, arguments: Sequence[str]) -> int:
    """Execute one allowlisted hook with its original filenames and exit status."""
    command = _TOOL_COMMANDS[hook_id]
    if command[0] == ":python":
        executable = python
        argv = (os.fspath(ROOT / command[1]), *arguments)
    else:
        executable = _executable_path(python, command[0])
        if executable is None:
            raise RuntimeError(f"certified hook executable is missing: {command[0]}")
        argv = (*command[1:], *arguments)
    return os.spawnve(  # nosec B606
        os.P_WAIT,
        os.fspath(executable),
        [os.fspath(executable), *argv],
        _runtime_environment(python),
    )


def run_hook(hook_id: str, arguments: Sequence[str]) -> int:
    """Select an exact environment and run one allowlisted local hook."""
    if hook_id not in _TOOL_COMMANDS:
        expected = ", ".join(sorted(_TOOL_COMMANDS))
        raise ValueError(f"unknown pre-commit hook {hook_id!r}; expected one of: {expected}")
    current_python = Path(sys.executable).absolute()
    if os.environ.get(CERTIFIED_CURRENT_ENVIRONMENT) == "1" and _current_environment_is_exact():
        return _execute_hook(current_python, hook_id, arguments)

    fingerprint = _environment_fingerprint()
    environment = _environment_path(fingerprint)
    if _external_environment_is_exact(environment, fingerprint):
        return _execute_hook(_environment_python(environment), hook_id, arguments)
    with _environment_lock(fingerprint):
        if not _external_environment_is_exact(environment, fingerprint):
            _bootstrap_environment(environment, fingerprint)
    return _execute_hook(_environment_python(environment), hook_id, arguments)


def main(argv: Sequence[str] | None = None) -> int:
    """Handle internal certification mode or dispatch one configured hook."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments == ["--check-current"]:
        return 0 if _current_environment_is_exact() else 1
    if not arguments:
        raise SystemExit("usage: run_pre_commit_tool.py HOOK_ID [HOOK_ARGUMENT ...]")
    return run_hook(arguments[0], arguments[1:])


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, TimeoutError, ValueError) as error:
        raise SystemExit(f"pre-commit tool dispatch failed: {error}") from error
