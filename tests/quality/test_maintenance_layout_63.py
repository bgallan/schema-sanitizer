"""Protect maintenance layout revision 63."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_source_plan_registry_has_one_cohesive_owner() -> None:
    """Opening, materialization, and output share one explicit registry owner."""
    owner = ROOT / "src/schema_sanitizer/api_impl/source_plan/registry.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "class OpenedSourcePlanRegistryStream" in text
    assert "def open_source_plan_registry_stream" in text
    assert "def materialize_opened_registry_stream" in text
    assert "def write_source_plan_registry_to_file" in text
    assert len(text.splitlines()) <= 500


def test_registry_path_sink_methods_have_one_direct_owner() -> None:
    """Path collections and lazy providers must not regain pass-through packages."""
    owner = ROOT / "src/schema_sanitizer/core_impl/registry_sinks.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    source = owner.read_text(encoding="utf-8")
    assert "class _RegistryPathProviderSinkMethods" in source
    assert "class _RegistryPathSourceSinkMethods" in source
    assert "call_native_registry_sink" not in source
    assert len(source.splitlines()) <= 500


def test_jsonl_output_adapters_have_one_bounded_lifecycle_owner() -> None:
    """Closely coupled JSONL destinations must not return to micro-units."""
    package = ROOT / "cpp/src/api/python_abi3/json/output_adapters"
    owner = package / "output_adapters.cc"
    assert {path.name for path in package.iterdir()} == {"api.hh", "output_adapters.cc"}
    source = owner.read_text(encoding="utf-8")
    assert "class FileJsonlOutput" in source
    assert "class PythonJsonlOutput" in source
    assert "class StringJsonlOutput" in source
    assert len(source.splitlines()) <= 500


def test_registry_state_endpoints_are_grouped_in_the_path_methods_unit() -> None:
    """Explicit registry-state path methods share one focused compilation unit."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    methods = registry / "path_source_registry_methods.cc"
    source = methods.read_text(encoding="utf-8")
    assert "py_context_to_registry_sink_from_path_sources_registry_state" in source
    assert "py_context_to_registry_sink_from_path_source_chunk_provider_registry_state" in source
    assert not (registry / "path_source_methods.cc").exists()
    assert len(source.splitlines()) <= 500
