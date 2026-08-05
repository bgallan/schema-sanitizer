"""Protect ownership boundaries introduced by maintenance layout 51."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_execution_probe_methods_have_one_cohesive_owner() -> None:
    """Probe dispatch should not retain pass-through modules around ABI3 calls."""
    owner = ROOT / "src/schema_sanitizer/core_impl/probes.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    for name in (
        "_ExecutionSchemaProbeMethods",
        "_ExecutionRegistryInputProbeMethods",
        "_ExecutionRegistryPathSourceProbeMethods",
    ):
        assert f"class {name}" in text
    assert "probe_dependencies" not in text
    assert len(text.splitlines()) <= 500


def test_projection_subset_audit_has_one_bounded_owner() -> None:
    """Subset audit phases stay cohesive without a five-file micro-package."""
    assert (
        importlib.util.find_spec("schema_sanitizer.adapters.parquet.projection.audits.subset")
        is not None
    )
    owner = ROOT / "src/schema_sanitizer/adapters/parquet/projection/audits/subset.py"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not owner.with_suffix("").exists()


def test_temporal_primitives_are_split_by_value_domain() -> None:
    """Date, time, and timestamp parsers must compile as separate units."""
    core = ROOT / "cpp/src/core"
    package = core / "temporal"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "date.cpp",
        "parse_internal.hh",
        "time.cpp",
        "timestamp.cpp",
    }
    assert not (core / "primitives_temporal.cpp").exists()


def test_c_sink_bridge_is_split_by_operation() -> None:
    """Shared sink helpers, input calls, and diagnostics remain independent."""
    api = ROOT / "cpp/src/api/c"
    for name in (
        "schema_sanitizer_c_sink_common.cc",
        "schema_sanitizer_c_sink_input.cc",
        "schema_sanitizer_c_sink_diagnostics.cc",
    ):
        assert (api / name).is_file()
    assert not (api / "schema_sanitizer_c_sink.cc").exists()
