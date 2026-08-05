"""Structural contract for the domain-oriented test suite."""

from __future__ import annotations

from pathlib import Path

TEST_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_DOMAINS = frozenset(
    {
        "concurrency",
        "examples",
        "io",
        "memory",
        "parquet",
        "pipeline",
        "quality",
        "remote",
        "schema",
        "sinks",
    }
)


def test_test_modules_are_partitioned_into_documented_domains() -> None:
    """Test modules must not return to one unauditable flat directory."""
    assert not tuple(TEST_ROOT.glob("test_*.py"))
    assert (TEST_ROOT / "README.md").is_file()
    observed = {
        directory.name
        for directory in TEST_ROOT.iterdir()
        if directory.is_dir() and tuple(directory.glob("test_*.py"))
    }
    assert observed == EXPECTED_DOMAINS


def test_test_module_names_remain_unique_across_domains() -> None:
    """Non-package test directories require globally unique module names."""
    modules = [path.name for path in TEST_ROOT.glob("*/test_*.py")]
    assert len(modules) == len(set(modules))
    assert len(modules) >= 379
