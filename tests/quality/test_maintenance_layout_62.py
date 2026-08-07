"""Protect ownership boundaries introduced by maintenance layout 62."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_low_level_execution_is_one_cohesive_module() -> None:
    """Small ABI3 sink routes share one context owner instead of mixin shells."""
    owner = ROOT / "src/schema_sanitizer/core_impl/execution.py"
    assert owner.is_file()
    assert not owner.with_suffix("").is_dir()
    text = owner.read_text(encoding="utf-8")
    assert "class ExecutionContext" in text
    assert "def to_sink_from_source" in text
    assert "def to_sink_path_sources" in text
    assert "def to_sink_arrow_stream" in text
    assert len(text.splitlines()) <= 500


def test_file_conversion_execution_has_one_small_orchestration_owner() -> None:
    """Target lifecycle, source-plan routing, and public entry points share one owner."""
    assert importlib.util.find_spec("schema_sanitizer.api_impl.file_conversion.core") is None
    owner = ROOT / "src/schema_sanitizer/api_impl/file_conversion/converters.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "def try_convert_source_plan_with_options" in text
    assert "def convert_file_with_options" in text
    assert len(text.splitlines()) <= 500


def test_object_struct_conversion_has_a_focused_subdomain() -> None:
    """Field lookup and object materialization must remain separate units."""
    conversion = ROOT / "cpp/src/internal/materialization/conversion"
    assert not (conversion / "struct_object.cc").exists()
    assert not (conversion / "struct_object.hh").exists()
    package = conversion / "object_struct"
    assert {path.name for path in package.iterdir()} == {
        "api.hh",
        "conversion.cc",
        "fields.cc",
        "fields.hh",
    }
    assert (
        "find_strict_extra_field"
        not in (package / "conversion.cc")
        .read_text(encoding="utf-8")
        .split("Status convert_object_struct", maxsplit=1)[0]
    )


def test_parquet_collection_and_thrift_parsing_have_bounded_phase_owners() -> None:
    """Batch collection and compact-Thrift parsing use bounded phase owners."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    collection = writer / "stream_writer_collection.cc.inc"
    assert collection.is_file()
    assert len(collection.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (writer / "collection").exists()
    thrift = ROOT / "cpp/src/internal/parquet/footer_reader/thrift"
    assert not (thrift / "thrift_schema_pages.cc.inc").exists()
    assert (thrift / "schema_elements.cc.inc").is_file()
    column_reader = thrift / "footer_metadata_column_reader.cc.inc"
    source = column_reader.read_text(encoding="utf-8")
    assert "read_i32_list" in source
    assert "read_i64_list" in source
    assert not (thrift / "primitive_lists.cc.inc").exists()
    assert len(source.splitlines()) <= 500
