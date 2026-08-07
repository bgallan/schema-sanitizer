"""Protect ownership boundaries introduced by maintenance layout 50."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_bigquery_sidecar_has_one_bounded_owner() -> None:
    """BigQuery sidecar SQL, lookup, and mutation stay in one cohesive module."""
    assert (
        importlib.util.find_spec("schema_sanitizer.integrations.bigquery.registry_sidecar") is None
    )
    owner = ROOT / "src/schema_sanitizer/integrations/bigquery/sidecar.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    source = owner.read_text(encoding="utf-8")
    assert "def sidecar_table_ddl" in source
    assert "def fetch_sidecar_last_ingested_partition" in source
    assert "def update_registry_sidecar_table" in source
    assert len(source.splitlines()) <= 500


def test_arrow_direct_schema_parser_has_explicit_subsystem() -> None:
    """Arrow schema dispatch, nested parsing, and payload encoding stay separate."""
    package = ROOT / "cpp/src/api/python_abi3/arrow_direct/schema"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "logical.cc",
        "logical.hh",
        "nested.cc",
        "parser_internal.hh",
        "payload.cc",
        "payload.hh",
        "type.cc",
    }
    parent = package.parent
    for retired in (
        "_core_abi3_arrow_direct_schema.cc",
        "_core_abi3_arrow_direct_schema.hh",
        "_core_abi3_arrow_direct_schema_payload.cc",
    ):
        assert not (parent / retired).exists()


def test_parquet_readiness_has_one_bounded_runtime_owner() -> None:
    """Closely related readiness checks stay cohesive without micro-fragments."""
    runtime = ROOT / "cpp/src/internal/parquet/footer_reader/runtime"
    owner = runtime / "native_stream_readiness.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (runtime / "readiness").exists()
    assert not (runtime / "footer_reader_readiness_gate.cc.inc").exists()
    assert not (runtime / "footer_reader_readiness_gate.cc.inc").exists()


def test_xml_document_parser_is_split_by_phase() -> None:
    """XML document lifecycle, tokens, and recursive elements stay separate."""
    parsing = ROOT / "cpp/src/internal/parsing"
    package = parsing / "xml"
    for name in ("document.cc", "document.hh", "element.cc", "tokens.cc"):
        assert (package / name).is_file()
    assert not (parsing / "xml_document.cc").exists()
    assert not (parsing / "xml_document.hh").exists()


def test_dictionary_and_provider_protocols_are_not_monolithic() -> None:
    """Dictionary pages and Python providers remain split by operation."""
    pages = ROOT / "cpp/src/internal/parquet/footer_reader/pages"
    assert (pages / "footer_reader_dictionary_indices.cc.inc").is_file()
    assert (pages / "footer_reader_dictionary_page.cc.inc").is_file()
    assert not (pages / "footer_reader_dictionary_index_pages.cc.inc").exists()

    registry = ROOT / "cpp/src/api/python_abi3/registry"
    providers = registry / "arrow_source_provider.cc"
    source = providers.read_text(encoding="utf-8")
    for symbol in (
        "close_arrow_chunk_provider",
        "parse_arrow_sources",
        "load_next_arrow_provider_chunk",
    ):
        assert symbol in source
    assert len(source.splitlines()) <= 500
    assert not (registry / "arrow_source_sinks").exists()
