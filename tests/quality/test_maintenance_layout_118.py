"""Protect consolidated conversion ownership and footer-wide layout planning."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_file_conversion_orchestration_has_one_bounded_owner() -> None:
    """Public converters and target lifecycle must not split into one-consumer modules."""
    package = ROOT / "src/schema_sanitizer/api_impl/file_conversion"
    owner = package / "converters.py"
    source = owner.read_text(encoding="utf-8")
    assert "def try_convert_source_plan_with_options" in source
    assert "def convert_file_with_options" in source
    assert "def _convert_public_file" in source
    assert not (package / "execution.py").exists()
    assert len(source.splitlines()) <= 600


def test_parquet_stream_plans_layout_once_and_loads_row_groups_lazily() -> None:
    """Stream opening plans metadata once and decodes only the active row group."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    public = (reader / "reporting/footer_reader_public.cc.inc").read_text(encoding="utf-8")
    row_group = (
        reader / "native_stream/materialization/row_group/native_stream_row_group.cc.inc"
    ).read_text(encoding="utf-8")
    compact_row_group = " ".join(row_group.split())
    schema = (reader / "native_stream/schema/native_stream_arrow_schema_root.cc.inc").read_text(
        encoding="utf-8"
    )

    assert "read_footer_metadata_impl(path, projected_columns)" in public
    assert "initialize_native_stream_output_layout(state.get())" in public
    assert "native_reader_readiness(info, &planned_output_layout)" not in public
    assert "prepare_native_row_group" in row_group
    assert "auto status = read_page_headers(stream->file, &current," in compact_row_group
    assert "release_native_row_group_runtime_state(&row_group)" in row_group
    assert "validate_native_recursive_row_group_output_layout(" in row_group
    assert "finalize_native_stream_output_layout_plan" in schema


def test_repeated_leaf_read_planning_shares_level_decoder_owner() -> None:
    """Repeated page observations and level layout assembly form one bounded phase."""
    schema = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/schema"
    owner = schema / "native_stream_repeated_level_layouts.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "struct RepeatedLeafReadPlanState" in source
    assert "observe_repeated_leaf_data_page" in source
    assert "assign_repeated_leaf_native_read_plan" in source
    assert "assign_simple_list_level_layout" in source
    assert not (schema / "native_stream_repeated_leaf_read_plan.cc.inc").exists()
    assert len(source.splitlines()) <= 500
