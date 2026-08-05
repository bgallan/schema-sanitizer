"""Protect ownership boundaries introduced by maintenance layout 54."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_hive_uri_helpers_have_one_neutral_bounded_owner() -> None:
    """BigQuery and pipeline share one neutral Hive URI implementation."""
    assert not (ROOT / "src/schema_sanitizer/pipeline/hive").exists()
    owner = ROOT / "src/schema_sanitizer/core_impl/hive_uris.py"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not owner.with_suffix("").exists()
    bigquery_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (ROOT / "src/schema_sanitizer/integrations/bigquery").rglob("*.py")
    )
    assert "pipeline.hive.uris" not in bigquery_text


def test_native_schema_contract_codec_has_explicit_owners() -> None:
    """Logical payload conversion and option-wire encoding stay separate."""
    assert not (ROOT / "src/schema_sanitizer/core_impl/native_options").exists()
    core = ROOT / "src/schema_sanitizer/core_impl"
    logical_schema = (core / "logical_schema.py").read_text(encoding="utf-8")
    option_wire = (core / "native_options.py").read_text(encoding="utf-8")
    assert "class LogicalSchemaPayload" in logical_schema
    assert "def encode_arrow_schema_payload" in logical_schema
    assert "def pyarrow_schema_from_payload" in logical_schema
    assert "def _append_schema" in option_wire
    assert "def _append_schema" not in logical_schema


def test_python_reader_adapter_has_one_bounded_owner() -> None:
    """One small adapter should not be split by method implementation."""
    sources = ROOT / "cpp/src/api/python_abi3/sources"
    owner = sources / "python_reader.cc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (sources / "python_reader").exists()
    assert not (sources / "_core_abi3_python_reader.cc").exists()


def test_csv_projection_is_one_bounded_cohesive_unit() -> None:
    """Small projection lifecycle, header mapping, and row filtering share one owner."""
    csv = ROOT / "cpp/src/frontends/csv"
    owner = csv / "column_projection.cc"
    assert owner.is_file()
    assert not (csv / "column_projection").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
