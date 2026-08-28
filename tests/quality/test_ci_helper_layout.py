"""Behavioral architecture contracts for the CI helper tree."""

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
