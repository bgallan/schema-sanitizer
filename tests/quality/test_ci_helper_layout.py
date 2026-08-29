"""Behavioral architecture contracts for the CI helper tree.

The check keeps fuzz, native, Parquet, quality, and release helpers under explicit owner
directories and rejects obsolete flat-script locations.
"""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CI_ROOT = ROOT / "meta" / "ci"

OWNER_DIRECTORIES = {
    "fuzz",
    "native",
    "parquet",
    "quality",
    "release",
    "requirements",
    "sanitizers",
}


def test_ci_helpers_are_grouped_by_owner() -> None:
    """Runnable helpers belong to a thematic owner, not a filename inventory."""
    helpers = [
        path.relative_to(CI_ROOT)
        for path in CI_ROOT.rglob("*")
        if path.is_file() and path.suffix in {".cc", ".py", ".sh"}
    ]

    assert helpers
    assert all(len(path.parts) >= 2 for path in helpers)
    assert {path.parts[0] for path in helpers} <= OWNER_DIRECTORIES
    assert all(path.suffix == ".md" for path in CI_ROOT.iterdir() if path.is_file())


def test_ci_shell_entry_points_remain_executable() -> None:
    """Moved shell gates retain the executable bit expected by workflows."""
    scripts = tuple(CI_ROOT.rglob("*.sh"))

    assert scripts
    assert all(os.access(script, os.X_OK) for script in scripts)


def test_repository_orchestration_does_not_import_the_subprocess_module() -> None:
    """CI and benchmark tools keep process creation outside the flagged module."""
    production_roots = (CI_ROOT, ROOT / "benchmarks")
    offenders = {
        path.relative_to(ROOT).as_posix()
        for root in production_roots
        for path in root.rglob("*.py")
        if "import subprocess" in path.read_text(encoding="utf-8")
        or "from subprocess import" in path.read_text(encoding="utf-8")
    }

    assert offenders == set()
    assert not (CI_ROOT / "quality" / "run_process.py").exists()


def test_retired_source_zip_pipeline_stays_absent() -> None:
    """The obsolete ZIP chain must not return beside the canonical sdist flow."""
    retired = {
        "check_cmake_sources_exist.sh",
        "check_zip_contains_cmake_sources.sh",
        "create_source_zip.sh",
    }

    assert not any(path.name in retired for path in CI_ROOT.rglob("*"))
