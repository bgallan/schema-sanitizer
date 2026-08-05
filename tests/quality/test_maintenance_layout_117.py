"""Protect registry ownership and cached recursive navigation from layout 117."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_registry_sink_methods_share_one_bounded_owner() -> None:
    """Arrow and path registry routes must not return to one-consumer modules."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    owner = core / "registry_sinks.py"
    source = owner.read_text(encoding="utf-8")
    for owner_type in (
        "_RegistryArrowSinkMethods",
        "_RegistryPathProviderSinkMethods",
        "_RegistryPathSourceSinkMethods",
    ):
        assert f"class {owner_type}" in source
    execution = (core / "execution.py").read_text(encoding="utf-8")
    assert "from .registry_sinks import (" in execution
    assert not (core / "registry_arrow.py").exists()
    assert not (core / "registry_paths.py").exists()
    assert len(source.splitlines()) <= 500


def test_recursive_tree_caches_parent_and_leaf_ranges() -> None:
    """Subtree leaf lookups and node paths reuse one finalized traversal plan."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    model = (reader / "native_stream/schema/native_stream_recursive_model.cc.inc").read_text(
        encoding="utf-8"
    )
    validation = (
        reader / "native_stream/materialization/native_stream_page_layout.cc.inc"
    ).read_text(encoding="utf-8")
    output = (reader / "native_stream/schema/native_stream_output_layout.cc.inc").read_text(
        encoding="utf-8"
    )

    assert "std::optional<std::size_t> parent_index" in model
    assert "std::size_t subtree_leaf_offset" in model
    assert "std::size_t subtree_leaf_count" in model
    assert "std::vector<std::size_t> leaf_column_indices" in model
    assert "plan_native_recursive_materialization_tree" in model
    assert "std::span<const std::size_t>" in model
    assert "collect_native_recursive_materialization_leaf_columns" not in model
    assert "plan_native_recursive_materialization_tree(" in output
    assert "native_recursive_layout_column_for_node" in validation
    assert "native_recursive_node_path" not in validation
    assert "collect_native_recursive_materialization_leaf_columns" not in validation


def test_stream_callbacks_live_with_row_group_runtime() -> None:
    """The tiny Arrow stream callbacks stay with their only runtime consumer."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    owner = reader / "native_stream/materialization/row_group/native_stream_row_group.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "native_parquet_stream_get_schema" in source
    assert "native_parquet_stream_get_next" in source
    assert "native_parquet_stream_release" in source
    assert len(source.splitlines()) <= 500
    assert not (reader / "runtime/footer_reader_stream_callbacks.cc.inc").exists()
