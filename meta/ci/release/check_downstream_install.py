#!/usr/bin/env python3
"""Validate a built wheel from isolated downstream environments.

It creates isolated environments and validates base, typed, and optional-extra consumer
profiles against the built wheel.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import venv
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
    """Return the interpreter created in a virtual environment."""
    directory = "Scripts" if os.name == "nt" else "bin"
    executable = "python.exe" if os.name == "nt" else "python"
    return environment / directory / executable


def create_environment(root: Path, name: str) -> Path:
    """Create a clean downstream virtual environment and return its Python."""
    environment = root / name
    if environment.exists():
        shutil.rmtree(environment)
    venv.EnvBuilder(with_pip=True).create(environment)
    python = environment_python(environment)
    subprocess.run(
        [os.fspath(python), "-m", "pip", "install", "pip==26.1.2"],
        check=True,
    )
    return python


def validate_consumer(wheel: Path, root: Path, scripts: Path, constraints: Path) -> None:
    """Exercise runtime and typing behavior outside the repository."""
    python = create_environment(root, "consumer")
    subprocess.run(
        [
            os.fspath(python),
            "-m",
            "pip",
            "install",
            "-c",
            os.fspath(constraints),
            "mypy",
            "pyarrow",
            os.fspath(wheel),
        ],
        check=True,
    )
    subprocess.run(
        [os.fspath(python), "-I", os.fspath(scripts / "downstream_smoke.py")],
        check=True,
        cwd=root,
    )
    subprocess.run(
        [
            os.fspath(python),
            "-I",
            "-m",
            "mypy",
            "--strict",
            os.fspath(scripts / "downstream_typecheck.py"),
        ],
        check=True,
        cwd=root,
    )


def validate_extras(wheel: Path, root: Path, constraints: Path) -> None:
    """Install every optional extra alone and verify its advertised imports."""
    for extra, imports in EXTRA_IMPORTS.items():
        print(f"[downstream-extra] {extra}", flush=True)
        python = create_environment(root, f"extra-{extra}")
        requirement = os.fspath(wheel) if extra == "core" else f"{wheel}[{extra}]"
        subprocess.run(
            [
                os.fspath(python),
                "-m",
                "pip",
                "install",
                "-c",
                os.fspath(constraints),
                requirement,
            ],
            check=True,
        )
        statements = ["import schema_sanitizer", *(f"import {name}" for name in imports)]
        subprocess.run(
            [os.fspath(python), "-I", "-c", "; ".join(statements)],
            check=True,
            cwd=root,
        )


def main() -> None:
    """Parse paths and run all downstream installation checks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--work-root", type=Path, required=True)
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
    args.work_root.mkdir(parents=True, exist_ok=True)
    if not wheel.is_file():
        raise SystemExit(f"wheel does not exist: {wheel}")
    if not constraints.is_file():
        raise SystemExit(f"constraints do not exist: {constraints}")
    validate_consumer(wheel, args.work_root, scripts, constraints)
    validate_extras(wheel, args.work_root, constraints)
    print("downstream wheel and isolated extras passed")


if __name__ == "__main__":
    main()
