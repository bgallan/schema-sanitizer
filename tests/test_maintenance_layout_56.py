"""Protect ownership boundaries introduced by maintenance layout 56."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_cloud_providers_have_one_module_per_backend() -> None:
    """Each cloud backend owns URI, discovery, and transfer behavior in one module."""
    providers = ROOT / "src/schema_sanitizer/remote_impl/providers"
    for name in ("gcs", "s3", "azure"):
        owner = providers / f"{name}.py"
        assert owner.is_file()
        assert not (providers / name).exists()
        source = owner.read_text(encoding="utf-8")
        assert "def parse_uri" in source
        assert "async def directories_containing_files" in source
        assert "async def file_exists" in source
        assert len(source.splitlines()) <= 500


def test_partition_execution_has_one_bounded_owner() -> None:
    """The small partition loop, result, and registry bootstrap share one owner."""
    assert importlib.util.find_spec("schema_sanitizer.pipeline.execution") is None
    owner = ROOT / "src/schema_sanitizer/pipeline/partition_execution.py"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    assert "def _compile_native_registry_state" in source
    assert "class PartitionPipelineResult" in source
    assert "def run_partitioned_to_parquet_registry_json" in source


def test_native_stream_decoders_are_grouped_by_data_model() -> None:
    """Native decoders stay cohesive without returning to per-codec fragments."""
    package = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/decode"
    assert {path.name for path in package.glob("*.cc.inc")} == {
        "native_stream_binary_columns.cc.inc",
        "native_stream_dictionary_binary_columns.cc.inc",
        "native_stream_dictionary_fixed_columns.cc.inc",
        "native_stream_list_columns.cc.inc",
        "native_stream_scalar_columns.cc.inc",
    }
    assert all(
        path.read_text(encoding="utf-8").count("sanitize::Status") > 1
        for path in package.glob("*.cc.inc")
    )


def test_option_deserialization_separates_envelope_and_fields() -> None:
    """SZOPT envelope validation and field decoding remain independent."""
    planning = ROOT / "cpp/src/planning"
    internal = ROOT / "cpp/src/internal/planning"
    assert not (planning / "options_io.cc").exists()
    assert (planning / "options_deserialization.cc").is_file()
    assert (planning / "options_field_deserialization.cc").is_file()
    assert (internal / "options_deserialization.hh").is_file()
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "cpp/src/planning/options_deserialization.cc" in manifest
    assert "cpp/src/planning/options_field_deserialization.cc" in manifest
    assert "cpp/src/planning/options_io.cc" not in manifest
