"""Protect the ownership, de-fragmentation, and hot paths from maintenance layout 88."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from schema_sanitizer.core_impl.uris import (
    location_kind,
    looks_like_supported_uri,
    remote_provider,
)
from schema_sanitizer.pipeline.source_discovery import _unique_source_locations
from schema_sanitizer.pipeline.types import PartitionRunPlan

ROOT = Path(__file__).resolve().parents[1]


def test_uri_classification_has_one_core_owner() -> None:
    """Local paths, file URIs, and remote providers share one parser and owner."""
    package = ROOT / "src/schema_sanitizer"
    owner = package / "core_impl/uris.py"
    retired = (
        package / "core_impl/path_uris.py",
        package / "remote_impl/uris.py",
    )

    assert owner.is_file()
    assert all(not path.exists() for path in retired)
    assert location_kind(r"C:\data\rows.json") == "path"
    assert location_kind("file:///tmp/rows.json") == "file"
    assert location_kind("gs://bucket/rows.json") == "gcs"
    assert remote_provider("https://account.blob.core.windows.net/container/rows") == "azure"
    assert remote_provider("https://example.test/rows") == "http"
    assert not looks_like_supported_uri("hdfs://cluster/rows")

    production = "\n".join(
        path.read_text(encoding="utf-8") for path in package.rglob("*.py") if path != owner
    )
    assert "core_impl.path_uris" not in production
    assert "remote_impl.uris" not in production


def test_source_plan_deduplication_keeps_only_first_seen_uris() -> None:
    """Discovery must not allocate a list of plan positions for every URI."""
    plans = [
        PartitionRunPlan(date(2026, 1, 1), "gs://bucket/a", "out-a"),
        PartitionRunPlan(date(2026, 1, 2), "gs://bucket/b", "out-b"),
        PartitionRunPlan(date(2026, 1, 3), "gs://bucket/a", "out-c"),
    ]

    assert _unique_source_locations(plans) == {
        "gs://bucket/a": "gcs",
        "gs://bucket/b": "gcs",
    }
    with pytest.raises(ValueError, match="Unsupported source URI scheme: 'hdfs'"):
        _unique_source_locations([PartitionRunPlan(date(2026, 1, 1), "hdfs://cluster/a", "out")])

    owner = (ROOT / "src/schema_sanitizer/pipeline/source_discovery.py").read_text(encoding="utf-8")
    unique_owner = owner[
        owner.index("def _unique_source_locations") : owner.index("\ndef _partition_plans")
    ]
    assert "def _source_indices" not in unique_owner
    assert "defaultdict" not in unique_owner
    assert ".append(index)" not in unique_owner
    assert "location_kind(source_uri)" in unique_owner
    assert "source_uri in locations" in unique_owner


def test_bulk_remote_discovery_avoids_requested_child_set_copies() -> None:
    """Provider scans perform one child lookup instead of building a set per group."""
    owners = (
        ROOT / "src/schema_sanitizer/remote_impl/providers/gcs.py",
        ROOT / "src/schema_sanitizer/remote_impl/providers/s3.py",
        ROOT / "src/schema_sanitizer/remote_impl/providers/azure.py",
    )
    for owner in owners:
        source = owner.read_text(encoding="utf-8")
        assert "requested_children = set(children)" not in source
        assert "children.get(child)" in source


def test_passthrough_python_modules_stay_folded_into_real_owners() -> None:
    """Single-call facades must not return after their behavior gained a domain owner."""
    package = ROOT / "src/schema_sanitizer"
    retired = (
        package / "adapters/pyarrow/csv_native.py",
        package / "adapters/pyarrow/csv_values.py",
        package / "api_impl/analytical/invocation.py",
        package / "api_impl/file_conversion/writer_lifecycle.py",
        package / "api_impl/parquet/folder_reader.py",
        package / "integrations/bigquery/partition_defaults.py",
    )
    owners = (
        package / "adapters/pyarrow/csv_sink.py",
        package / "api_impl/analytical.py",
        package / "api_impl/stream_output.py",
        package / "api_impl/input/directory_preparation.py",
        package / "integrations/bigquery/external_table.py",
    )

    assert all(not path.exists() for path in retired)
    assert all(path.is_file() for path in owners)
    assert all(len(path.read_text(encoding="utf-8").splitlines()) <= 500 for path in owners)

    csv_sink = owners[0].read_text(encoding="utf-8")
    parquet_directory = owners[3].read_text(encoding="utf-8")
    assert "def native_csv_nested_reader" in csv_sink
    assert "return any(" in csv_sink
    assert "nested_column_indices" not in csv_sink
    assert "folder_files(" in parquet_directory
    assert "prepare_parquet_directory_from_files(" in parquet_directory


def test_schema_registry_uses_one_version_name_parser_and_borrowed_indexes() -> None:
    """Registry merge loops borrow stable names and use the planning parser directly."""
    package = ROOT / "cpp/src/schema_registry"
    header = (package / "schema_registry_internal.hh").read_text(encoding="utf-8")
    merge = (package / "schema_registry.cc").read_text(encoding="utf-8")
    numeric = (package / "schema_registry_numeric.cc").read_text(encoding="utf-8")
    types = (package / "schema_registry_types.cc").read_text(encoding="utf-8")

    assert "variant_base_name" not in header
    assert "variant_version" not in header
    assert "parse_versioned_field_name" in merge
    assert "variant_family_base" in merge
    assert merge.count("BorrowedStringLookupMap") >= 3
    assert "BorrowedStringLookupMap<std::size_t> emitted_by_name" in numeric
    assert "std::string_view\nsource_segment_for_output" in types


def test_parquet_dictionary_encoding_borrows_values_without_per_row_indices() -> None:
    """Dictionary indexing retains unique keys and emits row indices directly."""
    writer = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_value_encodings.cc.inc"
    ).read_text(encoding="utf-8")
    entry = (ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer.cc").read_text(
        encoding="utf-8"
    )

    assert "BorrowedStringLookupMap<std::uint32_t> index_by_value" in writer
    assert "dictionary.reserve(initial_unique_capacity)" in writer
    assert "std::vector<std::uint32_t> indices" not in writer
    assert "append_rle_run_u32(out.encoded_indices" in writer
    assert '"internal/string_lookup.hh"' in entry


def test_all_productive_source_units_remain_bounded() -> None:
    """Python and C++ units, including included implementation fragments, stay bounded."""
    source_roots = (ROOT / "src", ROOT / "cpp")
    suffixes = {".py", ".cc", ".cpp", ".hh", ".hpp", ".inc"}
    oversized = {
        path.relative_to(ROOT): len(path.read_text(encoding="utf-8").splitlines())
        for source_root in source_roots
        for path in source_root.rglob("*")
        if path.is_file() and path.suffix in suffixes
        if len(path.read_text(encoding="utf-8").splitlines()) > 500
    }
    assert oversized == {}


def test_cpp23_contains_is_used_in_path_probe() -> None:
    """The path probe uses the direct C++23 membership operation."""
    source = (ROOT / "cpp/src/api/python_abi3/path_sources/path_source_probe.cc").read_text(
        encoding="utf-8"
    )
    assert ".contains(" in source
    assert "!= std::string_view::npos" not in source
