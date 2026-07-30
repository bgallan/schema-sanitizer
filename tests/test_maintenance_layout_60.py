"""Protect ownership boundaries introduced by maintenance layout 60."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_direct_parquet_routes_have_one_cohesive_owner() -> None:
    """Direct Parquet state, retry, stream, and sink routing share one owner."""
    assert importlib.util.find_spec("schema_sanitizer.api_impl.parquet.direct") is None
    owner = ROOT / "src/schema_sanitizer/api_impl/parquet/direct_routes.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    for symbol in (
        "def last_parquet_direct_route",
        "def should_retry_native_parquet_reader_failure",
        "def parquet_direct_stream_factory_or_none",
        "def parquet_direct_sink_raw_or_none",
        "def parquet_direct_registry_sink_raw_or_none",
    ):
        assert symbol in text
    assert len(text.splitlines()) <= 500


def test_source_plan_probing_has_one_direct_owner() -> None:
    """Probe dispatch and sequence accumulation stay in one cohesive module."""
    owner = ROOT / "src/schema_sanitizer/api_impl/source_plan/probing.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "def probe_source_plan_registry" in text
    assert "def _probe_sequence_registry" in text
    assert "def probe_prepared_source_plan_registry" in text
    assert "probe_child" not in text
    assert len(text.splitlines()) <= 500


def test_ingest_preparation_is_split_by_phase() -> None:
    """Inference, schema resolution, and PreparedIngest assembly stay independent."""
    ingest = ROOT / "cpp/src/ingest"
    assert not (ingest / "prepare.cc").exists()
    package = ingest / "prepare"
    assert {path.name for path in package.iterdir()} == {
        "inference.cc",
        "prepare.cc",
        "prepare_internal.hh",
        "schema.cc",
    }
    assert "scan_shapes_row" not in (package / "prepare.cc").read_text(encoding="utf-8")
    assert "compile_plan" not in (package / "inference.cc").read_text(encoding="utf-8")


def test_metadata_column_parsing_has_one_bounded_owner() -> None:
    """Closely coupled metadata parsers stay in one ABI3 translation unit."""
    metadata = ROOT / "cpp/src/api/python_abi3/metadata"
    assert not (metadata / "_core_abi3_metadata_columns.cc").exists()
    assert not (metadata / "_core_abi3_metadata_columns.hh").exists()
    columns = metadata / "columns"
    assert {path.name for path in columns.iterdir()} == {"api.hh", "columns.cc"}
    owner = (columns / "columns.cc").read_text(encoding="utf-8")
    assert "append_row_span_columns_from_dict" in owner
    assert "append_timestamp_columns" in owner
    assert "std::in_range<std::int64_t>" in owner
    assert len(owner.splitlines()) <= 500
