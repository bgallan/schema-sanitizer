#!/usr/bin/env python3
"""Prepare isolated downstream checks for a composite-action shell step.

The planner validates its wheel and support files, creates one pinned offline
environment per published extra, and emits a safely quoted fail-fast Bash plan.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shlex
import struct
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
_PIP_VERSION = "26.2.1"
_PIP_WHEEL_NAME = f"pip-{_PIP_VERSION}-py3-none-any.whl"
_PIP_WHEEL_SHA256 = (
    "71138adf1f4ca900cdb7d289c21b7494329f2332b6d85f0e1c42108c0384ed3e"  # pragma: allowlist secret
)


def environment_python(environment: Path) -> Path:
    """Return the interpreter path created by the pinned virtualenv tool."""
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


def _validated_seed_directory(path: Path) -> Path:
    """Return a directory containing only the exact trusted pip seed wheel."""
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"virtualenv seed root is not a regular directory: {path}")
    entries = sorted(path.iterdir())
    if len(entries) != 1 or entries[0].name != _PIP_WHEEL_NAME:
        raise ValueError(
            "virtualenv seed root must contain exactly "
            f"{_PIP_WHEEL_NAME}: {[entry.name for entry in entries]}"
        )
    wheel = _resolved_regular_file(entries[0], "virtualenv pip seed wheel")
    actual = hashlib.sha256(wheel.read_bytes()).hexdigest()
    if actual != _PIP_WHEEL_SHA256:
        raise ValueError(
            f"virtualenv pip seed SHA-256 mismatch: expected {_PIP_WHEEL_SHA256}, got {actual}"
        )
    return path.resolve()


def _paths_overlap(left: Path, right: Path) -> bool:
    """Return whether two resolved locations contain or equal one another."""
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def _validate_plan_paths(
    *,
    isolated_root: Path,
    app_data_root: Path,
    wheel: Path,
    scripts: Path,
    constraints: Path,
    seed_wheels: Path,
    command_output: Path | None,
) -> None:
    """Reject cleanup-owned, input, script, and command locations that overlap."""
    owned_roots = {
        "downstream root": isolated_root.resolve(),
        "virtualenv app-data root": app_data_root.resolve(),
    }
    inputs = {
        "wheel": wheel.resolve(),
        "scripts root": scripts.resolve(),
        "constraints": constraints.resolve(),
        "virtualenv seed root": seed_wheels.resolve(),
    }
    for owned_name, owned_path in owned_roots.items():
        for input_name, input_path in inputs.items():
            if _paths_overlap(owned_path, input_path):
                raise ValueError(
                    f"downstream owned {owned_name} and {input_name} must be disjoint: "
                    f"{owned_path} and {input_path}"
                )
    if command_output is None:
        return
    resolved_output = command_output.resolve()
    for owned_name, owned_path in owned_roots.items():
        if _paths_overlap(owned_path, resolved_output):
            raise ValueError(
                f"downstream owned {owned_name} and command output must be disjoint: "
                f"{owned_path} and {resolved_output}"
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
    seed_wheels: Path,
    *,
    command_output: Path | None = None,
) -> str:
    """Return one fail-fast Bash plan preserving per-extra isolation."""
    if work_root.is_symlink():
        raise ValueError(f"work root must be a regular directory: {work_root}")
    isolated_root = work_root.resolve() / _ISOLATED_DIRECTORY
    app_data_root = work_root.resolve() / "virtualenv-app-data"
    _validate_plan_paths(
        isolated_root=isolated_root,
        app_data_root=app_data_root,
        wheel=wheel,
        scripts=scripts,
        constraints=constraints,
        seed_wheels=seed_wheels,
        command_output=command_output,
    )
    work_root.mkdir(parents=True, exist_ok=True)
    work_root = work_root.resolve()
    if not work_root.is_dir():
        raise ValueError(f"work root must be a regular directory: {work_root}")
    isolated_root = work_root / _ISOLATED_DIRECTORY
    app_data_root = work_root / "virtualenv-app-data"
    expected_python = tuple(sys.version_info[:3])
    expected_pointer_bits = struct.calcsize("P") * 8
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "umask 077",
        f"isolated_root={shlex.quote(_shell_path(isolated_root))}",
        f"app_data_root={shlex.quote(_shell_path(app_data_root))}",
        "cleanup_downstream() {",
        "  local status=$?",
        "  local cleanup_status=0",
        "  trap - EXIT",
        '  rm -rf -- "${isolated_root}" "${app_data_root}" || cleanup_status=$?',
        "  if (( status != 0 )); then",
        '    exit "${status}"',
        "  fi",
        '  exit "${cleanup_status}"',
        "}",
        "trap cleanup_downstream EXIT",
        'rm -rf -- "${isolated_root}" "${app_data_root}"',
        'mkdir -p -- "${isolated_root}" "${app_data_root}"',
        'cd "${isolated_root}"',
    ]

    def create_environment(name: str) -> Path:
        """Append an offline app-data-seeded environment and its identity check."""
        environment = isolated_root / name
        python = environment_python(environment)
        lines.append(
            _command(
                [
                    Path(sys.executable),
                    "-m",
                    "virtualenv",
                    environment,
                    "--creator",
                    "builtin",
                    "--seeder",
                    "app-data",
                    "--app-data",
                    app_data_root,
                    "--no-download",
                    "--no-periodic-update",
                    "--copies",
                    "--pip",
                    _PIP_VERSION,
                    "--no-setuptools",
                    "--extra-search-dir",
                    seed_wheels,
                ]
            )
        )
        identity_check = (
            "import pip, struct, sys; "
            f"assert tuple(sys.version_info[:3]) == {expected_python!r}; "
            f"assert struct.calcsize('P') * 8 == {expected_pointer_bits}; "
            f"assert pip.__version__ == {_PIP_VERSION!r}"
        )
        lines.append(_command([python, "-I", "-c", identity_check]))
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
        "--seed-wheel-dir",
        type=Path,
        default=Path(__file__).resolve().parents[3] / ".work/virtualenv-seed",
    )
    parser.add_argument(
        "--constraints",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "requirements/downstream.txt",
    )
    args = parser.parse_args()

    try:
        wheel = _resolved_regular_file(args.wheel, "wheel")
        constraints = _resolved_regular_file(args.constraints, "constraints")
        seed_wheels = _validated_seed_directory(args.seed_wheel_dir)
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
            seed_wheels,
            command_output=args.command_output,
        )
        _write_text_atomically(args.command_output, plan)
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
