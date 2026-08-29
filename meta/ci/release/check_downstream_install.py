#!/usr/bin/env python3
"""Prepare isolated downstream checks for a composite-action shell step.

The planner validates its wheel and support files, creates one environment per
published extra, and emits a safely quoted, fail-fast Bash execution plan.
"""

from __future__ import annotations

import argparse
import os
import shlex
import sys
import tempfile
from pathlib import Path

EXTRA_IMPORTS = {
    "core": (),
    "pyarrow": ("pyarrow",),
    "pandas": ("pandas",),
    "polars": ("polars",),
    "duckdb": ("duckdb",),
    "gcs": ("aiohttp", "google.auth"),
    "s3": ("aiobotocore", "botocore"),
    "azure": ("aiohttp", "azure.identity", "azure.storage.blob"),
    "bigquery": ("adbc_driver_bigquery.dbapi", "pyarrow"),
    "cloud": (
        "aiohttp",
        "aiobotocore",
        "azure.identity",
        "azure.storage.blob",
        "botocore",
        "google.auth",
    ),
    "all": (
        "adbc_driver_bigquery.dbapi",
        "aiohttp",
        "aiobotocore",
        "azure.identity",
        "azure.storage.blob",
        "botocore",
        "duckdb",
        "google.auth",
        "pandas",
        "polars",
        "pyarrow",
    ),
}
_ISOLATED_DIRECTORY = "downstream"


def environment_python(environment: Path) -> Path:
    """Return the interpreter path created by ``python -m venv``."""
    directory = "Scripts" if os.name == "nt" else "bin"
    executable = "python.exe" if os.name == "nt" else "python"
    return environment / directory / executable


def _shell_path(path: Path) -> str:
    """Return a path spelling accepted by Bash on every supported runner."""
    return path.as_posix()


def _command(arguments: list[str | Path]) -> str:
    """Render one argument vector with POSIX shell quoting for every value."""
    return shlex.join(
        _shell_path(argument) if isinstance(argument, Path) else argument for argument in arguments
    )


def _write_text_atomically(destination: Path, content: str) -> None:
    """Replace one regular text output atomically and skip unchanged bytes."""
    payload = content.encode("utf-8")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise ValueError(f"refusing symlinked command output: {destination}")
    if destination.exists() and not destination.is_file():
        raise ValueError(f"command output must be a regular file: {destination}")
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


def _resolved_regular_file(path: Path, description: str) -> Path:
    """Resolve one required regular non-symlink input file."""
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{description} does not exist as a regular file: {path}")
    return path.resolve()


def _paths_overlap(left: Path, right: Path) -> bool:
    """Return whether two resolved locations contain or equal one another."""
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def _validate_plan_paths(
    *,
    isolated_root: Path,
    wheel: Path,
    scripts: Path,
    constraints: Path,
    command_output: Path | None,
) -> None:
    """Reject cleanup-owned, input, script, and command locations that overlap."""
    resolved_isolated = isolated_root.resolve()
    inputs = {
        "wheel": wheel.resolve(),
        "scripts root": scripts.resolve(),
        "constraints": constraints.resolve(),
    }
    for input_name, input_path in inputs.items():
        if _paths_overlap(resolved_isolated, input_path):
            raise ValueError(
                f"downstream owned downstream root and {input_name} must be disjoint: "
                f"{resolved_isolated} and {input_path}"
            )
    if command_output is None:
        return
    resolved_output = command_output.resolve()
    if _paths_overlap(resolved_isolated, resolved_output):
        raise ValueError(
            "downstream owned downstream root and command output must be disjoint: "
            f"{resolved_isolated} and {resolved_output}"
        )
    for input_name, input_path in inputs.items():
        if _paths_overlap(resolved_output, input_path):
            raise ValueError(
                f"downstream command output and {input_name} must be disjoint: "
                f"{resolved_output} and {input_path}"
            )


def shell_plan(
    wheel: Path,
    work_root: Path,
    scripts: Path,
    constraints: Path,
    *,
    command_output: Path | None = None,
) -> str:
    """Return one fail-fast Bash plan preserving per-extra isolation."""
    if work_root.is_symlink():
        raise ValueError(f"work root must be a regular directory: {work_root}")
    isolated_root = work_root.resolve() / _ISOLATED_DIRECTORY
    _validate_plan_paths(
        isolated_root=isolated_root,
        wheel=wheel,
        scripts=scripts,
        constraints=constraints,
        command_output=command_output,
    )
    work_root.mkdir(parents=True, exist_ok=True)
    work_root = work_root.resolve()
    if not work_root.is_dir():
        raise ValueError(f"work root must be a regular directory: {work_root}")
    isolated_root = work_root / _ISOLATED_DIRECTORY
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "umask 077",
        f"isolated_root={shlex.quote(_shell_path(isolated_root))}",
        "cleanup_downstream() {",
        "  local status=$?",
        "  local cleanup_status=0",
        "  trap - EXIT",
        '  rm -rf -- "${isolated_root}" || cleanup_status=$?',
        "  if (( status != 0 )); then",
        '    exit "${status}"',
        "  fi",
        '  exit "${cleanup_status}"',
        "}",
        "trap cleanup_downstream EXIT",
        'rm -rf -- "${isolated_root}"',
        'mkdir -p -- "${isolated_root}"',
        'cd "${isolated_root}"',
    ]

    def create_environment(name: str) -> Path:
        """Append commands that create and bootstrap one isolated environment."""
        environment = isolated_root / name
        python = environment_python(environment)
        lines.append(_command([Path(sys.executable), "-m", "venv", environment]))
        lines.append(_command([python, "-m", "pip", "install", "-c", constraints, "pip==26.2.1"]))
        return python

    consumer = create_environment("consumer")
    lines.extend(
        (
            _command(
                [
                    consumer,
                    "-m",
                    "pip",
                    "install",
                    "-c",
                    constraints,
                    "mypy",
                    "pyarrow",
                    wheel,
                ]
            ),
            _command([consumer, "-I", scripts / "downstream_smoke.py"]),
            _command(
                [
                    consumer,
                    "-I",
                    "-m",
                    "mypy",
                    "--strict",
                    scripts / "downstream_typecheck.py",
                ]
            ),
        )
    )

    for extra, imports in EXTRA_IMPORTS.items():
        lines.append("printf '%s\\n' " + shlex.quote(f"[downstream-extra] {extra}"))
        python = create_environment(f"extra-{extra}")
        requirement = _shell_path(wheel) if extra == "core" else f"{_shell_path(wheel)}[{extra}]"
        lines.append(_command([python, "-m", "pip", "install", "-c", constraints, requirement]))
        statements = ["import schema_sanitizer", *(f"import {name}" for name in imports)]
        lines.append(_command([python, "-I", "-c", "; ".join(statements)]))
    lines.append("printf '%s\\n' 'downstream wheel and isolated extras passed'")
    return "\n".join(lines) + "\n"


def main() -> None:
    """Validate paths and write the downstream command plan."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--command-output", type=Path, required=True)
    parser.add_argument("--scripts", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument(
        "--constraints",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "requirements/downstream.txt",
    )
    args = parser.parse_args()

    try:
        wheel = _resolved_regular_file(args.wheel, "wheel")
        constraints = _resolved_regular_file(args.constraints, "constraints")
        if args.scripts.is_symlink() or not args.scripts.is_dir():
            raise ValueError(f"scripts root is not a regular directory: {args.scripts}")
        scripts = args.scripts.resolve()
        for script in (scripts / "downstream_smoke.py", scripts / "downstream_typecheck.py"):
            _resolved_regular_file(script, "downstream script")
        plan = shell_plan(
            wheel,
            args.work_root,
            scripts,
            constraints,
            command_output=args.command_output,
        )
        _write_text_atomically(args.command_output, plan)
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
