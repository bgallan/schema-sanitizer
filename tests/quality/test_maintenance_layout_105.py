"""Protect ownership and hot-path changes introduced by maintenance layout 105."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_call_options_have_one_canonical_python_owner() -> None:
    """Wrapper filtering and normalization must not return to parallel modules."""
    package = ROOT / "src/schema_sanitizer"
    owner = package / "options_impl/call_options.py"
    source = owner.read_text(encoding="utf-8")

    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    assert "FILE_CONVERSION_HELPER_KEYS" in source
    assert "ANALYTICAL_HELPER_KEYS" in source
    assert "def call_options_from_locals(" in source
    assert "def normalize_call_options(" in source
    assert not (package / "core_impl/call_options.py").exists()

    production = "\n".join(path.read_text(encoding="utf-8") for path in package.rglob("*.py"))
    assert "core_impl.call_options" not in production


def test_source_plan_is_owned_by_the_input_domain() -> None:
    """Prepared-input models must not depend back on API orchestration."""
    package = ROOT / "src/schema_sanitizer"
    owner = package / "input_impl/source_plan.py"

    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (package / "api_impl/source_plan/plan.py").exists()
    assert not (package / "input_impl/source_plan").exists()

    input_sources = "\n".join(
        path.read_text(encoding="utf-8") for path in (package / "input_impl").rglob("*.py")
    )
    assert "api_impl" not in input_sources


def test_parquet_output_layout_indexes_top_level_fields_once() -> None:
    """Wide nested schemas must not linearly rescan prior output fields per leaf."""
    footer = ROOT / "cpp/src/internal/parquet/footer_reader"
    layout = (footer / "native_stream/schema/native_stream_output_layout.cc.inc").read_text(
        encoding="utf-8"
    )
    validation = layout
    entry = (footer / "footer_reader.cc").read_text(encoding="utf-8")

    assert '"internal/string_lookup.hh"' in entry
    assert "StringLookupMap<std::size_t> *field_index_by_name" in layout
    assert "field_index_by_name->find(top_level_name)" in layout
    assert "field_index_by_name->try_emplace" in layout
    assert "std::find_if(fields->begin(), fields->end()" not in layout
    assert validation.count("field_index_by_name.reserve(") == 2
    assert validation.count("&field_index_by_name") == 2
    assert "std::ranges::sort(recursive_leaf_columns)" not in layout
