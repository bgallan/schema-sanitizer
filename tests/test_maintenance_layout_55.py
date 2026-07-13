"""Protect ownership boundaries introduced by maintenance layout 55."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_native_runtime_loader_is_separate_from_cohesive_option_contract() -> None:
    """ABI3 loading stays neutral while the option domain has one owner."""
    assert importlib.util.find_spec("schema_sanitizer.core_impl.native") is None
    core = ROOT / "src/schema_sanitizer/core_impl"
    runtime = core / "native_runtime.py"
    options = core / "native_options.py"
    assert runtime.is_file() and options.is_file()
    assert not (core / "native_runtime").exists()
    assert not (core / "native_options").exists()
    assert "SchemaEvolutionMode" not in runtime.read_text(encoding="utf-8")
    option_text = options.read_text(encoding="utf-8")
    assert "class SchemaEvolutionMode" in option_text
    assert "class OptionSpec" in option_text


def test_native_file_outputs_have_one_direct_owner() -> None:
    """Small native writer routes stay cohesive instead of regaining format facades."""
    owner = ROOT / "src/schema_sanitizer/api_impl/file_conversion/direct_writers.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    source = owner.read_text(encoding="utf-8")
    for name in (
        "try_write_csv_direct_native",
        "try_write_jsonl_direct_native",
        "try_write_parquet_direct_native",
    ):
        assert f"def {name}" in source
    assert len(source.splitlines()) <= 500


def test_compiled_plan_layout_has_an_independent_builder() -> None:
    """Struct lookup construction must not reconverge with plan recursion."""
    planning = ROOT / "cpp/src/planning"
    internal = ROOT / "cpp/src/internal/planning"
    assert (planning / "struct_layout.cpp").is_file()
    assert (internal / "struct_layout.hh").is_file()
    plan = (planning / "plan.cpp").read_text(encoding="utf-8")
    assert "build_dispatch_table" not in plan
    assert "make_struct_layout" in plan
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "cpp/src/planning/struct_layout.cpp" in manifest


def test_compact_reader_has_one_bounded_implementation_owner() -> None:
    """Primitive reads and bounded skipping share one cohesive implementation."""
    package = ROOT / "cpp/src/internal/parquet/footer_reader/thrift"
    declaration = package / "compact_reader.hh.inc"
    owner = package / "compact_reader.cc.inc"
    assert declaration.is_file() and owner.is_file()
    assert not (package / "compact_reader_values.cc.inc").exists()
    assert not (package / "compact_reader_skip.cc.inc").exists()
    source = owner.read_text(encoding="utf-8")
    assert "CompactReader::read_varint" in source
    assert "CompactReader::skip_struct" in source
    assert len(source.splitlines()) <= 500
