"""Maintenance contracts for layout revision 111."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src/internal/parquet/footer_reader"


def test_python_rows_have_one_bounded_core_owner() -> None:
    """Python-row streaming remains centralized after native iterator batching."""
    execution = (SRC / "core_impl/execution.py").read_text(encoding="utf-8")
    owner = SRC / "core_impl/python_rows.py"
    source = owner.read_text(encoding="utf-8")
    assert "from .python_rows import" in execution
    assert "class PythonRowsJsonlByteReader" in source
    assert "def last_python_rows_route" in source
    assert "PYTHON_ITER_ROWS_JSONL_BYTES" in source
    assert len(execution.splitlines()) <= 500
    assert len(source.splitlines()) <= 500


def test_source_plan_sink_opening_is_owned_by_execution_context() -> None:
    """High-level sink routing owns its only source-plan opening helper."""
    owner = SRC / "api_impl/execution_context.py"
    source = owner.read_text(encoding="utf-8")
    assert "def _open_source_plan_sink_stream_or_none" in source
    assert not (SRC / "api_impl/source_plan/sink_stream.py").exists()
    assert len(source.splitlines()) <= 500


def test_repeated_layout_validation_uses_cached_page_layout_owner() -> None:
    """Repeated validation reuses cached node plans beside decoded page layouts."""
    owner = CPP / "native_stream/materialization/native_stream_page_layout.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "native_recursive_layout_column_for_node" in source
    assert "tree.repeated_node_indices" in source
    assert "native_recursive_node_path" not in source
    assert "column_path_has_prefix" not in source
    assert "generic_list_defined_levels_from_path(candidate)" not in source
    assert len(source.splitlines()) <= 500
    assert not (
        CPP / "native_stream/schema/native_stream_repeated_layout_validation.cc.inc"
    ).exists()
    for retired in (
        CPP / "native_stream/materialization/layout/native_stream_recursive_paths.cc.inc",
        CPP / "native_stream/materialization/layout/native_stream_repeated_column_selection.cc.inc",
        CPP / "native_stream/schema/native_stream_row_group_validation.cc.inc",
    ):
        assert not retired.exists()


def test_recursive_metadata_validation_is_iterative() -> None:
    """Metadata validation does not consume one C++ stack frame per node."""
    owner = CPP / "native_stream/schema/native_stream_output_layout.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "struct NativeRecursiveMetadataValidationState" in source
    assert "while (!pending.empty())" in source
    assert "validate_native_recursive_materialization_node_metadata" not in source
    assert len(source.splitlines()) <= 500
