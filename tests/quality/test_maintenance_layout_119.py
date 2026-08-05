"""Protect analytical result ownership and cached recursive layout columns."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_analytical_output_conversion_belongs_to_results() -> None:
    """Table conversion and analytical result wrappers share one bounded owner."""
    api_impl = ROOT / "src/schema_sanitizer/api_impl"
    owner = api_impl / "results.py"
    source = owner.read_text(encoding="utf-8")
    assert "TABLE_ADAPTER_FORMATS" in source
    assert "def normalize_table_output_format" in source
    assert "def convert_arrow_table_output" in source
    assert "class Result" in source
    assert "class SinkResult" in source
    assert not (api_impl / "analytical_output.py").exists()
    assert len(source.splitlines()) <= 500


def test_recursive_materialization_caches_container_columns() -> None:
    """Container materialization must not rebuild paths or rescan subtree leaves."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    model = (reader / "native_stream/schema/native_stream_recursive_model.cc.inc").read_text(
        encoding="utf-8"
    )
    output = (reader / "native_stream/schema/native_stream_output_layout.cc.inc").read_text(
        encoding="utf-8"
    )
    children = (
        reader / "native_stream/materialization/native_stream_recursive_children.cc.inc"
    ).read_text(encoding="utf-8")
    containers = (
        reader / "native_stream/materialization/native_stream_recursive_containers.cc.inc"
    ).read_text(encoding="utf-8")
    page_layout = (
        reader / "native_stream/materialization/native_stream_page_layout.cc.inc"
    ).read_text(encoding="utf-8")

    assert "std::optional<std::size_t> layout_column_index" in model
    assert "std::optional<std::int16_t> definition_level" in model
    assert "std::vector<std::size_t> repeated_node_indices" in model
    assert "plan_native_recursive_layout_columns" in output
    assert "select_native_recursive_layout_column_index" in output
    assert "native_recursive_layout_column_for_node" in page_layout
    assert "tree.repeated_node_indices" in page_layout
    combined = "\n".join((children, containers, page_layout))
    for retired in (
        "native_recursive_node_path",
        "definition_level_for_path_prefix",
        "column_path_has_prefix",
    ):
        assert retired not in combined
    assert not (
        reader / "native_stream/schema/native_stream_repeated_layout_validation.cc.inc"
    ).exists()


def test_cmake_manifest_sources_are_present() -> None:
    """A clean source archive must contain every compilation unit in its manifest."""
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    sources = set(re.findall(r"cpp/src/[A-Za-z0-9_./-]+\.(?:cc|cpp|c)", manifest))
    assert sources
    missing = sorted(source for source in sources if not (ROOT / source).is_file())
    assert missing == []
    builders = ROOT / "cpp/src/internal/materialization/builders"
    assert {path.name for path in builders.iterdir() if path.is_file()} == {
        "detail.hh",
        "factory.cc",
        "nested.cc",
        "scalar.cc",
    }
