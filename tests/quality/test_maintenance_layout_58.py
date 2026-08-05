"""Protect ownership boundaries introduced by maintenance layout 58."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_bigquery_external_table_helpers_have_one_bounded_owner() -> None:
    """External-table model, URI, partition, and DDL helpers stay cohesive."""
    bigquery = ROOT / "src/schema_sanitizer/integrations/bigquery"
    owner = bigquery / "external_table.py"
    assert owner.is_file()
    assert not (bigquery / "external_tables").exists()
    source = owner.read_text(encoding="utf-8")
    assert "class ExternalTableSpec" in source
    assert "def external_table_ddl" in source
    assert "def external_table_spec_from_namespace" in source
    assert len(source.splitlines()) <= 500


def test_csv_nested_stream_has_one_bounded_lifecycle_owner() -> None:
    """CSV nested rewriting keeps its Arrow callbacks and wrapper cohesive."""
    csv = ROOT / "cpp/src/api/python_abi3/csv"
    package = csv / "nested_stream"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "nested_stream.cc",
        "state.hh",
    }
    owner = package / "nested_stream.cc"
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "int get_schema" in source
    assert "int get_next" in source
    assert "PyObject *py_csv_nested_stream_wrap" in source
    assert not list(csv.glob("_core_abi3_csv_nested_stream*"))
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "cpp/src/api/python_abi3/csv/nested_stream/nested_stream.cc" in manifest
    for retired in ("callbacks.cc", "columns.cc", "release.cc", "schema.cc", "wrapper.cc"):
        assert f"cpp/src/api/python_abi3/csv/nested_stream/{retired}" not in manifest


def test_metadata_stream_is_grouped_by_lifecycle_and_build_phase() -> None:
    """Metadata wrappers keep array construction separate from stream lifecycle."""
    metadata = ROOT / "cpp/src/api/python_abi3/metadata"
    package = metadata / "stream"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "array_builder.cc",
        "stream.cc",
        "stream.hh",
    }
    assert not list(metadata.glob("_core_abi3_metadata_stream*"))
    assert not (metadata / "utf8").exists()
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "metadata/stream/array_builder.cc" in manifest
    assert "metadata/stream/stream.cc" in manifest
    assert "metadata/utf8/column.cc" not in manifest
