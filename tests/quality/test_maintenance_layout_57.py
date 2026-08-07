"""Protect ownership boundaries introduced by maintenance layout 57."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_recursive_parquet_fields_have_one_direct_owner() -> None:
    """Accumulation, leaf contracts, and fingerprints stay in one bounded module."""
    for module in (
        "schema_sanitizer.adapters.parquet.layout.leaf_contracts",
        "schema_sanitizer.adapters.parquet.layout.reducer_fields",
    ):
        assert importlib.util.find_spec(module) is None

    layout = ROOT / "src/schema_sanitizer/adapters/parquet/layout"
    owner = layout / "fields.py"
    assert owner.is_file()
    assert not (layout / "fields").exists()
    assert not (layout / "leaf_contracts.py").exists()
    assert not (layout / "reducer_fields.py").exists()
    text = owner.read_text(encoding="utf-8")
    for symbol in (
        "def accumulate_recursive_field",
        "def leaf_contracts_from_field",
    ):
        assert symbol in text
    assert len(text.splitlines()) <= 500


def test_arrow_provider_chunk_flow_has_one_visible_owner() -> None:
    """Small provider phases stay visible in the real compilation unit."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    provider = registry / "arrow_source_provider.cc"
    runtime = registry / "arrow_source_sinks.cc"
    assert provider.is_file() and runtime.is_file()
    assert not (registry / "arrow_source_sinks/provider_chunks").exists()
    provider_text = provider.read_text(encoding="utf-8")
    runtime_text = runtime.read_text(encoding="utf-8")
    assert "merge_arrow_source_provider_schemas" in provider_text
    for symbol in (
        "finish_opened_source_metadata",
        "try_open_passthrough_arrow_source",
        "ingest_arrow_source_with_registry_plan",
    ):
        assert symbol in runtime_text
    assert len(provider_text.splitlines()) <= 500
    assert len(runtime_text.splitlines()) <= 500


def test_json_ondemand_iteration_is_split_by_container() -> None:
    """Object, array, and child-value iteration remain separate units."""
    package = ROOT / "cpp/src/internal/parsing/json/ondemand"
    assert not (package / "iteration.cc").exists()
    for name in ("array_iteration.cc", "object_iteration.cc", "value_iteration.cc"):
        assert (package / name).is_file()
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "cpp/src/internal/parsing/json/ondemand/iteration.cc" not in manifest
    for name in ("array_iteration.cc", "object_iteration.cc", "value_iteration.cc"):
        assert f"cpp/src/internal/parsing/json/ondemand/{name}" in manifest


def test_statistics_scan_separates_recursive_values_and_rows() -> None:
    """Inference statistics stay cohesive without mixing row and nested scans."""
    inference = ROOT / "cpp/src/internal/inference"
    package = inference / "statistics"
    for retired in (
        "state.hh",
        "statistics_state.cc",
        "statistics_scan_internal.hh",
        "statistics_scan_nested.cc",
        "statistics_scan_row.cc",
        "statistics_scan.cc",
    ):
        assert not (inference / retired).exists()
    assert {path.name for path in package.iterdir()} == {
        "scan_internal.hh",
        "scan_nested.cc",
        "scan_row.cc",
        "state.cc",
        "state.hh",
    }
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "cpp/src/internal/inference/statistics_scan" not in manifest
    assert "cpp/src/internal/inference/statistics/scan_nested.cc" in manifest
    assert "cpp/src/internal/inference/statistics/scan_row.cc" in manifest
    assert "cpp/src/internal/inference/statistics/state.cc" in manifest
