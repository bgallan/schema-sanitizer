"""Protect maintenance layout revision 95."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from schema_sanitizer.integrations.bigquery import external_table as external_table_owner

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"


def test_bigquery_external_table_and_namespace_owners_are_flat() -> None:
    """BigQuery no longer has parallel singular and package-shaped owners."""
    bigquery = SRC / "integrations/bigquery"
    assert (bigquery / "external_table.py").is_file()
    assert (bigquery / "namespace_ops.py").is_file()
    assert not (bigquery / "external_tables").exists()
    assert not (bigquery / "namespaces").exists()
    assert len((bigquery / "external_table.py").read_text(encoding="utf-8").splitlines()) <= 500
    assert len((bigquery / "namespace_ops.py").read_text(encoding="utf-8").splitlines()) <= 500


def test_external_table_spec_resolves_partition_location_once(monkeypatch) -> None:
    """Spec construction must not recompute partition columns and URI prefixes."""
    calls = 0
    original = external_table_owner.external_table_hive_uri_prefix

    def counted_prefix(**kwargs: object) -> str:
        """Count Hive-prefix resolutions while preserving behavior."""
        nonlocal calls
        calls += 1
        return original(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        external_table_owner,
        "external_table_hive_uri_prefix",
        counted_prefix,
    )
    spec = external_table_owner.external_table_spec_from_namespace(
        SimpleNamespace(
            silver_parquet_prefix="gs://bucket/table",
            partition_granularity="hourly",
            external_table_source_uri=None,
        )
    )
    assert calls == 1
    assert spec.hive_uri_prefix == "gs://bucket/table"
    assert spec.source_uris == ["gs://bucket/table/*"]
    assert spec.partition_columns[-1] == ("hour", "INT64")


def test_abi3_method_declarations_have_one_catalogue() -> None:
    """ABI3 declarations stay in one catalogue instead of six include fragments."""
    abi = CPP / "internal/abi/python_abi3"
    owner = abi / "methods.hh"
    assert owner.is_file()
    assert not (abi / "methods").exists()
    source = owner.read_text(encoding="utf-8")
    assert "py_context_new" in source
    assert "py_options_catalog" in source
    assert "py_schema_registry_merge" in source
    assert "py_context_to_registry_sink_arrow_sources" in source
    assert len(source.splitlines()) <= 500

    production = "\n".join(
        path.read_text(encoding="utf-8")
        for path in CPP.rglob("*")
        if path.is_file() and path.suffix in {".cc", ".cpp", ".hh", ".hpp", ".inc"}
    )
    assert "internal/abi/python_abi3/methods/" not in production


def test_value_view_owns_empty_container_detection_without_message_building() -> None:
    """Empty-container checks stay beside ValueView and use allocation-free cancellation."""
    core = CPP / "core"
    assert not (core / "value_view_empty.cc").exists()
    assert not (core / "value_view_empty.hh").exists()
    source = (core / "value_view.cpp").read_text(encoding="utf-8")
    header = (CPP / "sanitize/core/value_view.hh").read_text(encoding="utf-8")
    assert "Status ValueView::container_is_empty" in source
    assert "Status(StatusCode::kCancelled, {})" in source
    assert "Status::Cancelled" not in source
    assert "Status container_is_empty(bool *out) const" in header


def test_product_files_remain_bounded() -> None:
    """All Python and native product owners remain within 500 lines."""
    candidates = [
        *SRC.rglob("*.py"),
        *CPP.rglob("*.cc"),
        *CPP.rglob("*.cpp"),
        *CPP.rglob("*.hh"),
        *CPP.rglob("*.hpp"),
        *CPP.rglob("*.inc"),
    ]
    oversized = {
        str(path.relative_to(ROOT)): len(path.read_text(encoding="utf-8").splitlines())
        for path in candidates
        if len(path.read_text(encoding="utf-8").splitlines()) > 500
    }
    assert oversized == {}
