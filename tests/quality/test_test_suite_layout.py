"""Structural contract for the domain-oriented test suite."""

from __future__ import annotations

import ast
import re
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
HISTORICAL_MODULE_NAME = re.compile(
    r"(?:_pass\d+(?:_|$)|_phase\d+(?:_|$)|_version\d+(?:_|$)|_v\d+$|"
    r"_part\d+$|^test_maintenance_layout_\d+$)"
)
HISTORICAL_TEST_NAME = re.compile(r"^test_(?:pass|phase|version|v)\d+_")
SPLIT_HISTORY_MARKER = re.compile(r"^#\s+Split from\b", re.MULTILINE)


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


def test_test_modules_use_stable_contract_names_not_migration_sequences() -> None:
    """Git history owns chronology; module and case names describe behavior."""
    modules = tuple(TEST_ROOT.glob("*/test_*.py"))
    stale_modules = [
        path.relative_to(TEST_ROOT).as_posix()
        for path in modules
        if HISTORICAL_MODULE_NAME.search(path.stem)
    ]
    stale_tests: list[str] = []
    split_markers: list[str] = []
    for path in modules:
        source = path.read_text(encoding="utf-8")
        relative = path.relative_to(TEST_ROOT).as_posix()
        if SPLIT_HISTORY_MARKER.search(source):
            split_markers.append(relative)
        tree = ast.parse(source, filename=str(path))
        stale_tests.extend(
            f"{relative}::{node.name}"
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and HISTORICAL_TEST_NAME.match(node.name)
        )

    assert stale_modules == []
    assert stale_tests == []
    assert split_markers == []
