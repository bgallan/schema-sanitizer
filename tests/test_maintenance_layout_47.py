"""Protect ownership boundaries introduced by maintenance layout 47."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_retired_input_and_conversion_modules_stay_absent() -> None:
    """Removed mixed modules must not return as compatibility facades."""
    for module_name in (
        "schema_sanitizer.api_impl.input.prepare",
        "schema_sanitizer.api_impl.input.types",
    ):
        assert importlib.util.find_spec(module_name) is None


def test_prepared_input_value_objects_have_neutral_owner() -> None:
    """Prepared-input contracts remain available without importing API implementation."""
    from schema_sanitizer.input_impl import prepared

    assert hasattr(prepared, "PreparedPublicInput")
    assert not hasattr(prepared, "prepare_public_input")


def test_public_input_preparation_has_one_direct_owner() -> None:
    """Discovery reuse, target resolution, and orchestration share one small owner."""
    owner = ROOT / "src/schema_sanitizer/api_impl/input/preparation.py"
    retired = ROOT / "src/schema_sanitizer/api_impl/input/preparation"
    assert owner.is_file()
    assert not retired.exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500

    from schema_sanitizer.api_impl.input import preparation

    assert hasattr(preparation, "prepare_public_input")


def test_pipeline_runtime_dependencies_are_not_owned_by_file_conversion() -> None:
    """Registry state, probe options, and low-level context pooling stay neutral."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    assert (core / "probes.py").is_file()
    assert (core / "execution.py").is_file()
    assert not (core / "execution").exists()
    assert (ROOT / "src/schema_sanitizer/core_impl/schema_registry.py").is_file()

    execution = ROOT / "src/schema_sanitizer/api_impl/file_conversion/converters.py"
    combined = execution.read_text(encoding="utf-8")
    assert "ContextVar" not in combined
    assert "def options_for_schema_probe" not in combined
    assert "def schema_registry_native_state_context" not in combined


def test_json_stream_scanner_is_split_by_responsibility() -> None:
    """Scanner lifecycle, traversal, and value parsing remain independent units."""
    package = ROOT / "cpp/src/internal/parsing/streaming/json"
    assert not (package / "scanner.cc").exists()
    assert {path.name for path in package.glob("scanner_*.cc")} == {
        "scanner_flow.cc",
        "scanner_line.cc",
        "scanner_state.cc",
        "scanner_value.cc",
    }


def test_logical_field_name_traversal_shares_the_field_name_owner() -> None:
    """Recursive naming and collision policy form one bounded native domain."""
    planning = ROOT / "cpp/src/planning"
    owner = planning / "field_name_sanitizer.cc"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    assert "sanitize_logical_schema_field_names" in source
    assert "BorrowedStringLookupMap<std::size_t> base_counts" in source
    assert not (planning / "logical_field_name_sanitizer.cc").exists()
