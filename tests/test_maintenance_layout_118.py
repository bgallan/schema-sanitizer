"""Protect consolidated conversion ownership and footer-wide layout planning."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_file_conversion_orchestration_has_one_bounded_owner() -> None:
    """Public converters and target lifecycle must not split into one-consumer modules."""
    package = ROOT / "src/schema_sanitizer/api_impl/file_conversion"
    owner = package / "converters.py"
    source = owner.read_text(encoding="utf-8")
    assert "def try_convert_source_plan_with_options" in source
    assert "def convert_file_with_options" in source
    assert "def _convert_public_file" in source
    assert not (package / "execution.py").exists()
    assert len(source.splitlines()) <= 500


def test_parquet_readiness_reuses_one_footer_layout_plan() -> None:
    """Readiness and stream opening share one layout instead of rebuilding per row group."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    readiness = (reader / "runtime/native_stream_readiness.cc.inc").read_text(encoding="utf-8")
    validation = (
        reader / "native_stream/materialization/native_stream_page_layout.cc.inc"
    ).read_text(encoding="utf-8")
    public = (reader / "reporting/footer_reader_public.cc.inc").read_text(encoding="utf-8")
    schema = (reader / "native_stream/schema/native_stream_arrow_schema_root.cc.inc").read_text(
        encoding="utf-8"
    )

    assert "std::vector<NativeParquetOutputField> *planned_output_layout" in readiness
    assert readiness.count("build_native_output_layout(") == 1
    assert "info.row_groups.front().columns" in readiness
    assert "validate_native_recursive_row_group_output_layout(row_group," in readiness
    assert "build_native_output_layout(row_group.columns" not in validation
    assert "native_reader_readiness(info, &planned_output_layout)" in public
    assert "state->output_layout = std::move(planned_output_layout)" in public
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
