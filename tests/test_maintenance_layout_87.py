"""Protect ownership, URI routing, and hot-path changes from maintenance layout 87."""

from __future__ import annotations

import asyncio
from pathlib import Path

from schema_sanitizer.core_impl.uris import remote_provider
from schema_sanitizer.input_impl.selection import looks_like_uri_string

ROOT = Path(__file__).resolve().parents[1]


def test_root_private_facades_moved_to_domain_owners() -> None:
    """Root-level implementation modules must not return as compatibility facades."""
    package = ROOT / "src/schema_sanitizer"
    owners = (
        package / "core_impl/schema_registry.py",
        package / "input_impl/source_plan.py",
        package / "api_impl/streams.py",
    )
    retired = (
        package / "schema_registry_impl.py",
        package / "source_plan_impl.py",
        package / "stream_impl.py",
        package / "api_impl/schema_registry.py",
        package / "api_impl/source_plan/plan.py",
    )
    assert all(path.is_file() for path in owners)
    assert all(len(path.read_text(encoding="utf-8").splitlines()) <= 500 for path in owners)
    assert all(not path.exists() for path in retired)


def test_dead_environment_facades_stay_removed() -> None:
    """Environment notes without production callers must not become modules again."""
    package = ROOT / "src/schema_sanitizer"
    assert not (package / "pipeline/source_discovery_environment.py").exists()
    assert not (package / "remote_impl/environment.py").exists()


def test_remote_uri_provider_has_one_canonical_owner() -> None:
    """Supported schemes and Azure endpoint recognition share one classifier."""
    assert remote_provider("gs://bucket/path") == "gcs"
    assert remote_provider("gcs://bucket/path") == "gcs"
    assert remote_provider("s3://bucket/path") == "s3"
    assert remote_provider("abfss://container@account.dfs.core.windows.net/path") == "azure"
    assert remote_provider("https://account.blob.core.windows.net/container/path") == "azure"
    assert remote_provider("https://example.test/data.json") == "http"
    assert remote_provider("hdfs://cluster/path") is None
    assert looks_like_uri_string("file:///tmp/data.json")
    assert looks_like_uri_string("wasbs://container@account.blob.core.windows.net/path")
    assert not looks_like_uri_string("hdfs://cluster/path")

    paths = (ROOT / "src/schema_sanitizer/pipeline/source_discovery.py").read_text(encoding="utf-8")
    selection = (ROOT / "src/schema_sanitizer/input_impl/selection.py").read_text(encoding="utf-8")
    routing = (ROOT / "src/schema_sanitizer/remote_impl/routing.py").read_text(encoding="utf-8")
    uris = (ROOT / "src/schema_sanitizer/core_impl/uris.py").read_text(encoding="utf-8")
    assert "REMOTE_SCHEMES" not in uris
    assert "_GCS_SCHEMES" not in paths
    assert "_AZURE_SCHEMES" not in paths
    assert "def is_gcs_uri" not in paths
    assert "_URI_SCHEMES" not in selection
    assert "remote_provider(uri)" in routing
    assert "def is_azure_blob_uri" not in routing


def test_bulk_discovery_reuses_unique_uri_classifications(monkeypatch) -> None:
    """Directory grouping consumes preclassified locations without reparsing."""
    import schema_sanitizer.pipeline.source_discovery as source_discovery

    source_locations = {
        "https://example.test/a.json": "http",
        "https://example.test/b.json": "http",
        "https://example.test/c.json": "http",
    }

    def fail_classification(_uri: str):
        """Fail if grouped discovery reparses a location."""
        raise AssertionError("location classification must be reused")

    monkeypatch.setattr(source_discovery, "location_kind", fail_classification)
    checked = asyncio.run(
        source_discovery._discover_directories(
            source_locations,
            extensions=("json",),
            input_format="json",
            exists_by_uri={},
            discovered_by_uri={},
        )
    )
    assert checked == set()


def test_registry_numeric_normalization_is_a_direct_bounded_unit() -> None:
    """The final oversized C++ registry owner stays split at a real phase boundary."""
    package = ROOT / "cpp/src/schema_registry"
    recursive = (package / "schema_registry.cc").read_text(encoding="utf-8")
    numeric = (package / "schema_registry_numeric.cc").read_text(encoding="utf-8")
    assert len(recursive.splitlines()) <= 500
    assert len(numeric.splitlines()) <= 500
    assert "void normalize_integer_float_schema" not in recursive
    assert "void normalize_integer_float_schema" in numeric
    assert "BorrowedStringLookupMap<std::size_t> emitted_by_name" in numeric
    assert "std::vector<NumericFamilyPlan> plans" in numeric
    assert "BorrowedStringLookupMap<std::size_t> plan_index_by_family" in numeric
    assert "std::vector<std::size_t> plan_by_field" in numeric
    assert "positions_by_family" not in numeric


def test_heterogeneous_string_lookup_has_one_cpp_owner() -> None:
    """CSV, JSON, and schema registry hot paths share one transparent hash contract."""
    shared = ROOT / "cpp/src/internal/string_lookup.hh"
    assert shared.is_file()
    consumers = (
        ROOT / "cpp/src/frontends/csv/column_projection.hh",
        ROOT / "cpp/src/frontends/json/root_field_filter.hh",
        ROOT / "cpp/src/schema_registry/schema_registry.cc",
        ROOT / "cpp/src/schema_registry/schema_registry_numeric.cc",
    )
    assert all(
        '"internal/string_lookup.hh"' in path.read_text(encoding="utf-8") for path in consumers
    )
    text = "\n".join(path.read_text(encoding="utf-8") for path in consumers)
    assert "CsvProjectionStringHash" not in text
    assert "StringViewHash" not in text


def test_parquet_statistics_borrow_binary_values_until_final_result() -> None:
    """Min/max collection must allocate only the two persisted statistics values."""
    owner = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_statistics.cc.inc"
    ).read_text(encoding="utf-8")
    assert "std::optional<std::string_view> min_value" in owner
    assert "const std::string_view current" in owner
    assert "std::string current" not in owner
    assert "if (has_true && has_false)" in owner


def test_native_probe_results_share_lazy_schema_payload_state() -> None:
    """Schema and registry probes must not duplicate lazy payload decoding."""
    owner = (ROOT / "src/schema_sanitizer/core_impl/native_results.py").read_text(encoding="utf-8")
    assert "class _LazySchemaPayloadResult" in owner
    assert "class SchemaProbeResult(_LazySchemaPayloadResult)" in owner
    assert "class RegistryProbeResult(_LazySchemaPayloadResult)" in owner
    assert owner.count("def field_names") == 1
    assert owner.count("def schema_payload") == 1


def test_cpp23_contains_replaces_manual_npos_checks() -> None:
    """C++ string membership checks use the direct C++23 API."""
    owners = (
        ROOT / "cpp/src/api/python_abi3/path_sources/path_sources.cc",
        ROOT / "cpp/src/api/python_abi3/registry/path_source_provider.cc",
        ROOT / "cpp/src/internal/json_output/jsonl_value_writer_floating.cc",
    )
    source = "\n".join(path.read_text(encoding="utf-8") for path in owners)
    assert source.count(".contains(") >= 5
    assert "!= std::string::npos" not in source
    assert "== std::string_view::npos" not in source
