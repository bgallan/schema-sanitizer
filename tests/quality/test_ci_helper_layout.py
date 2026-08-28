"""Behavioral architecture contracts for the CI helper tree.

The check keeps fuzz, native, Parquet, quality, and release helpers under explicit owner
directories and rejects obsolete flat-script locations.
"""

from __future__ import annotations

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
