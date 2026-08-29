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


def shell_plan(
    wheel: Path,
    work_root: Path,
    scripts: Path,
    constraints: Path,
) -> str:
    """Return one fail-fast Bash plan preserving per-extra isolation."""
    work_root.mkdir(parents=True, exist_ok=True)
    isolated_root = Path(
        tempfile.mkdtemp(prefix="schema-sanitizer-downstream-", dir=work_root)
    ).resolve()
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"isolated_root={shlex.quote(_shell_path(isolated_root))}",
        'cleanup_downstream() { rm -rf -- "${isolated_root}"; }',
        "trap cleanup_downstream EXIT",
        'cd "${isolated_root}"',
    ]

    def create_environment(name: str) -> Path:
        """Append commands that create and bootstrap one isolated environment."""
        environment = isolated_root / name
        python = environment_python(environment)
        lines.append(_command([Path(sys.executable), "-m", "venv", environment]))
        lines.append(_command([python, "-m", "pip", "install", "pip==26.1.2"]))
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

    wheel = args.wheel.resolve()
    scripts = args.scripts.resolve()
    constraints = args.constraints.resolve()
    if not wheel.is_file():
        parser.error(f"wheel does not exist: {wheel}")
    if not constraints.is_file():
        parser.error(f"constraints do not exist: {constraints}")
    for script in (scripts / "downstream_smoke.py", scripts / "downstream_typecheck.py"):
        if not script.is_file():
            parser.error(f"downstream script does not exist: {script}")

    args.command_output.parent.mkdir(parents=True, exist_ok=True)
    args.command_output.write_text(
        shell_plan(wheel, args.work_root.resolve(), scripts, constraints),
        encoding="utf-8",
        newline="\n",
    )


if __name__ == "__main__":
    main()
