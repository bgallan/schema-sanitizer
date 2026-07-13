"""Maintenance contracts for layout revision 112."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src/schema_sanitizer"
FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader"
SCHEMA = FOOTER / "native_stream/schema"


def test_http_transport_has_one_owner() -> None:
    """HTTP session primitives and object operations share one transport owner."""
    owner = SRC / "remote_impl/transport.py"
    source = owner.read_text(encoding="utf-8")
    for symbol in ("download_http_file", "http_file_exists", "upload_http_file"):
        assert f"def {symbol}" in source
    assert not (SRC / "remote_impl/providers/http.py").exists()
    assert len(source.splitlines()) <= 500


def test_parquet_pipeline_status_is_owned_by_telemetry() -> None:
    """Runtime pipeline verdicts remain next to the diagnostics they inspect."""
    owner = SRC / "adapters/parquet/telemetry.py"
    source = owner.read_text(encoding="utf-8")
    assert "def _parquet_pipeline_contract_status_from_diagnostics" in source
    assert "copy.deepcopy" not in source
    assert not (SRC / "adapters/parquet/contract_gates/pipeline.py").exists()
    assert len(source.splitlines()) <= 500


def test_recursive_output_layout_builds_and_counts_once() -> None:
    """Each leaf tree is built once and subtree counts are reused by Arrow setup."""
    owner = SCHEMA / "native_stream_output_layout.cc.inc"
    source = owner.read_text(encoding="utf-8")
    model = (SCHEMA / "native_stream_recursive_model.cc.inc").read_text(encoding="utf-8")
    schema_root = (SCHEMA / "native_stream_arrow_schema_root.cc.inc").read_text(encoding="utf-8")
    assert source.count("build_native_recursive_materialization_tree(") == 1
    assert "plan_native_recursive_path(path" not in source
    assert "recursive_subtree_counts" in source
    assert "recursive_subtree_counts" in model
    assert "const auto &subtree_counts = field.recursive_subtree_counts" in schema_root
    assert "count_native_recursive_materialization_subtree_resources(" not in schema_root
    assert not (SCHEMA / "native_stream_output_field_layout.cc.inc").exists()
    assert not (SCHEMA / "native_stream_metadata_validation.cc.inc").exists()
    assert len(source.splitlines()) <= 500


def test_thrift_primitive_lists_share_column_metadata_owner() -> None:
    """Tiny primitive-list parsing does not remain a detached fragment."""
    owner = FOOTER / "thrift/footer_metadata_column_reader.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "read_i32_list" in source
    assert "read_i64_list" in source
    assert "read_column_metadata" in source
    assert not (FOOTER / "thrift/primitive_lists.cc.inc").exists()
    assert len(source.splitlines()) <= 500
