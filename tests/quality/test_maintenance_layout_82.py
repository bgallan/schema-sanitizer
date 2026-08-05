"""Protect ownership and native-read improvements introduced by layout 82."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_input_preparation_and_discovery_have_direct_owners() -> None:
    """Input preparation and discovery must not return to micro-packages."""
    preparation = ROOT / "src/schema_sanitizer/api_impl/input/preparation.py"
    discovery = ROOT / "src/schema_sanitizer/input_impl/directory_inputs.py"
    assert preparation.is_file()
    assert discovery.is_file()
    assert not preparation.with_suffix("").is_dir()
    assert not discovery.with_suffix("").is_dir()
    assert len(preparation.read_text(encoding="utf-8").splitlines()) <= 500
    assert len(discovery.read_text(encoding="utf-8").splitlines()) <= 500


def test_native_parquet_value_kind_has_one_enum_representation() -> None:
    """Native-read dispatch must not store and compare heap strings per page."""
    pages = ROOT / "cpp/src/internal/parquet/footer_reader/model/pages.hh"
    column = ROOT / "cpp/src/internal/parquet/footer_reader/model/column.hh"
    model = pages.read_text(encoding="utf-8") + column.read_text(encoding="utf-8")
    assert "enum class NativeValueBufferKind" in model
    assert "std::string value_buffer_kind" not in model
    assert "std::string native_read_value_buffer_kind" not in model

    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    production = "\n".join(
        path.read_text(encoding="utf-8")
        for path in reader.rglob("*")
        if path.is_file() and path.suffix in {".cc", ".hh", ".inc"}
    )
    assert 'native_read_value_buffer_kind == "' not in production
    assert 'native_read_value_buffer_kind != "' not in production


def test_page_index_validation_uses_filtered_view_without_pointer_vector() -> None:
    """Page-index checks should not allocate a pointer vector for every column."""
    owner = ROOT / (
        "cpp/src/internal/parquet/footer_reader/pages/footer_reader_page_indexes.cc.inc"
    )
    text = owner.read_text(encoding="utf-8")
    assert "std::views::filter" in text
    assert "std::vector<const PageHeaderInfo *>" not in text
    assert not (owner.parent / "footer_reader_page_index_parse.cc.inc").exists()
    assert not (owner.parent / "footer_reader_page_index_validation.cc.inc").exists()


def test_native_page_layout_blocks_remain_cohesive() -> None:
    """Value classification shares the repeated-layout phase that consumes it."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    pages = reader / "pages"
    schema = reader / "native_stream/schema"
    repeated = schema / "native_stream_repeated_level_layouts.cc.inc"
    plans = pages / "footer_reader_native_page_plans.cc.inc"
    assert repeated.is_file()
    assert plans.is_file()
    assert "value_buffer_kind_for_page" in repeated.read_text(encoding="utf-8")
    assert len(repeated.read_text(encoding="utf-8").splitlines()) <= 500
    assert len(plans.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (pages / "footer_reader_value_layout.cc.inc").exists()
    assert not (pages / "footer_reader_page_buffer_layout.cc.inc").exists()
    assert not (pages / "footer_reader_native_page_spans.cc.inc").exists()
