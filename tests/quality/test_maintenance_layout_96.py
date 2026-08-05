"""Protect maintenance layout revision 96."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_bigquery_registry_and_sidecar_have_one_owner_each() -> None:
    """Embedded-registry and sidecar operations stay cohesive and bounded."""
    package = ROOT / "src/schema_sanitizer/integrations/bigquery"
    registry = package / "registry.py"
    sidecar = package / "sidecar.py"
    assert registry.is_file() and not registry.with_suffix("").exists()
    assert sidecar.is_file() and not sidecar.with_suffix("").exists()
    registry_text = registry.read_text(encoding="utf-8")
    sidecar_text = sidecar.read_text(encoding="utf-8")
    for symbol in (
        "fetch_latest_schema_registry",
        "prepare_existing_schema_registry_from_namespace",
        "latest_schema_registry_query",
    ):
        assert f"def {symbol}" in registry_text
    for symbol in (
        "fetch_sidecar_last_ingested_partition",
        "update_registry_sidecar_table",
        "sidecar_upsert_query",
    ):
        assert f"def {symbol}" in sidecar_text
    assert len(registry_text.splitlines()) <= 500
    assert len(sidecar_text.splitlines()) <= 500


def test_registry_sidecar_partition_is_parsed_once_per_query() -> None:
    """The fetch path delegates validation to the query builder exactly once."""
    owner = ROOT / "src/schema_sanitizer/integrations/bigquery/registry.py"
    text = owner.read_text(encoding="utf-8")
    fetch_body = text.split("def fetch_latest_schema_registry(", 1)[1].split(
        "def fetch_latest_schema_registry_from_namespace(", 1
    )[0]
    assert "partition_filter_sql(partition_key, partition_columns)" not in fetch_body
    assert "except ValueError:" in fetch_body


def test_xml_frontend_and_field_hashing_have_single_native_owners() -> None:
    """XML lifecycle stays together and field hashes are cached in the node model."""
    frontend_dir = ROOT / "cpp/src/frontends/xml"
    assert {path.name for path in frontend_dir.iterdir()} == {
        "frontend.cc",
        "frontend_internal.hh",
    }
    frontend = (frontend_dir / "frontend.cc").read_text(encoding="utf-8")
    document = (ROOT / "cpp/src/internal/parsing/xml/document.hh").read_text(encoding="utf-8")
    model = (ROOT / "cpp/src/internal/parsing/xml_value_model.cc").read_text(encoding="utf-8")
    assert len(frontend.splitlines()) <= 500
    assert "default_key_hash_" in frontend
    assert "std::uint64_t key_hash" in document
    assert "field.key_hash" in model
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "frontends/xml/frontend.cc" in manifest
    assert "frontends/xml/frontend_batch.cc" not in manifest
    assert "frontends/xml/frontend_lifecycle.cc" not in manifest
