"""Protect ownership and stream-plan reuse introduced by maintenance layout 116."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_source_selected_registry_routes_live_with_execution_context() -> None:
    """A one-consumer registry mixin must not split the execution ABI owner."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    owner = core / "execution.py"
    source = owner.read_text(encoding="utf-8")
    assert "class ExecutionContext" in source
    assert "def _call_native_registry_sink_from_source" in source
    assert "def to_registry_sink_from_source" in source
    assert "context_to_registry_sink_from_source" in source
    assert "_RegistrySourceSinkMethods" not in source
    assert not (core / "registry_sources.py").exists()
    assert len(source.splitlines()) <= 500


def test_native_parquet_stream_reuses_one_output_layout_plan() -> None:
    """Schema and every row group reuse the output tree and allocation counts."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    state = (reader / "native_stream/schema/native_stream_arrow_state.cc.inc").read_text(
        encoding="utf-8"
    )
    schema = (reader / "native_stream/schema/native_stream_arrow_schema_root.cc.inc").read_text(
        encoding="utf-8"
    )
    rows = (
        reader / "native_stream/materialization/row_group/native_stream_row_group.cc.inc"
    ).read_text(encoding="utf-8")
    public = (reader / "reporting/footer_reader_public.cc.inc").read_text(encoding="utf-8")

    assert "std::vector<NativeParquetOutputField> output_layout" in state
    assert "bool output_layout_initialized" in state
    assert "recursive_struct_array_count" in state
    assert "recursive_list_array_count" in state
    assert "def initialize_native_stream_output_layout" not in schema
    assert "initialize_native_stream_output_layout(" in schema
    assert "stream->output_layout_initialized = true" in schema
    assert "for (const auto &field : stream->output_layout)" in rows
    assert "build_native_output_layout(row_group.columns" not in rows
    assert "validate_native_recursive_row_group_output_layout(row_group)" not in public


def test_footer_root_parser_shares_the_metadata_owner() -> None:
    """The tiny root parser stays with the Thrift row-group/footer reader."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    owner = reader / "thrift/footer_metadata_row_group_reader.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "sanitize::Result<FooterInfo>" in source
    assert "parse_footer(std::string_view footer" in source
    assert "read_row_groups" in source
    assert len(source.splitlines()) <= 500
    assert not (reader / "runtime/footer_reader_footer_parse.cc.inc").exists()
