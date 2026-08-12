"""Smoke contracts for every executable example shipped with the project."""

from __future__ import annotations

import os
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples"
ENTRYPOINTS = (
    EXAMPLES / "example_07" / "07_gcs_jsonl_to_silver_parquet_range_prefix.py",
    EXAMPLES / "example_08" / "08_gcs_csv_modified_window_to_polars_parquet.py",
    EXAMPLES / "example_08" / "08_local_csv_directory_to_polars.py",
)


@pytest.mark.parametrize("entrypoint", ENTRYPOINTS, ids=lambda path: path.stem)
def test_example_entrypoint_help_runs_without_external_services(entrypoint: Path) -> None:
    """Every public script must remain importable before credentials are configured."""
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    completed = subprocess.run(
        [sys.executable, str(entrypoint), "--help"],
        cwd=ROOT,
        env=environment,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )

    assert completed.stderr == ""
    assert completed.stdout.startswith("usage:")


def test_example_readme_uses_current_optional_dependency_groups() -> None:
    """Cloud setup instructions use the narrow extras defined by this checkout."""
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    extras = project["optional-dependencies"]
    readme = (EXAMPLES / "README.md").read_text(encoding="utf-8")

    assert {"gcs", "bigquery", "polars"} <= extras.keys()
    assert 'pip install "schema-sanitizer[gcs,bigquery]"' in readme
    assert 'pip install "schema-sanitizer[polars,gcs,bigquery]"' in readme
    assert "schema-sanitizer[pyarrow,cloud]" not in readme
    assert "schema-sanitizer[polars,pyarrow,cloud]" not in readme


def test_example_readme_links_every_executable_entrypoint() -> None:
    """The example index exposes every supported script rather than orphaning it."""
    readme = (EXAMPLES / "README.md").read_text(encoding="utf-8")
    discovered = tuple(
        sorted(
            path
            for path in EXAMPLES.rglob("*.py")
            if 'if __name__ == "__main__":' in path.read_text(encoding="utf-8")
        )
    )

    assert discovered == tuple(sorted(ENTRYPOINTS))
    for entrypoint in ENTRYPOINTS:
        assert entrypoint.relative_to(EXAMPLES).as_posix() in readme
