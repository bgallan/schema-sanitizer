"""Protect ownership boundaries introduced by maintenance layout 59."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_bigquery_namespace_workflows_have_one_direct_owner() -> None:
    """Namespace-derived client and table workflows stay in one bounded module."""
    bigquery = ROOT / "src/schema_sanitizer/integrations/bigquery"
    owner = bigquery / "namespace_ops.py"
    registry = bigquery / "registry.py"
    assert owner.is_file()
    assert not (bigquery / "namespaces").exists()
    source = owner.read_text(encoding="utf-8")
    assert "def import_bigquery_adbc" in source
    assert "def create_or_replace_external_bigquery_table_from_namespace" in source
    assert len(source.splitlines()) <= 500
    assert registry.is_file()
    assert not registry.with_suffix("").exists()
    registry_source = registry.read_text(encoding="utf-8")
    assert "def fetch_latest_schema_registry" in registry_source
    assert "def prepare_existing_schema_registry_from_namespace" in registry_source
    assert len(registry_source.splitlines()) <= 500


def test_native_reader_limits_are_owned_by_runtime_and_repeated_schema() -> None:
    """Buffer limits and repeated-path support keep direct bounded owners."""
    footer = ROOT / "cpp/src/internal/parquet/footer_reader"
    assert not (footer / "runtime/footer_reader_native_limits.cc.inc").exists()
    assert (footer / "runtime/native_buffer_limits.cc.inc").is_file()
    repeated = footer / "native_stream/schema/native_stream_repeated_path_support.cc.inc"
    assert repeated.is_file()
    assert not (
        footer / "native_stream/schema/native_stream_generic_repeated_limits.cc.inc"
    ).exists()
    assert not (footer / "native_stream/schema/native_stream_path_support.cc.inc").exists()
    entry = (footer / "footer_reader.cc").read_text(encoding="utf-8")
    assert "footer_reader_native_limits.cc.inc" not in entry
    assert "runtime/native_buffer_limits.cc.inc" in entry
    assert "native_stream/schema/native_stream_repeated_path_support.cc.inc" in entry


def test_recursive_tree_mutation_and_model_analysis_stay_separate() -> None:
    """Tree construction and mutation stay separate from traversal analysis."""
    schema = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/schema"
    assert not (schema / "native_stream_schema_recursive_merge.cc.inc").exists()
    assert not (schema / "native_stream_recursive_resource_counts.cc.inc").exists()
    tree = schema / "native_stream_recursive_tree.cc.inc"
    model = schema / "native_stream_recursive_model.cc.inc"
    assert tree.is_file()
    assert model.is_file()
    assert "count_native_recursive_materialization" not in tree.read_text(encoding="utf-8")
    assert "merge_native_recursive_materialization" not in model.read_text(encoding="utf-8")
    assert not (schema / "native_stream_recursive_tree_merge.cc.inc").exists()
    assert not (schema / "native_stream_schema_recursive_build.cc.inc").exists()
