"""Protect ownership boundaries introduced by maintenance layout 45."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_retired_python_service_modules_stay_absent() -> None:
    """Mixed discovery and multi-source sink modules must not return as facades."""
    discovery = importlib.util.find_spec("schema_sanitizer.pipeline.source_discovery")
    assert discovery is not None and discovery.submodule_search_locations is None
    assert not (ROOT / "src/schema_sanitizer/api_impl/parquet/multisource").exists()


def test_parquet_multisource_is_one_cohesive_module() -> None:
    """The small Parquet directory domain must not be split into shell packages."""
    owner = ROOT / "src/schema_sanitizer/api_impl/parquet/multisource.py"
    assert owner.is_file()
    assert not owner.with_suffix("").is_dir()
    text = owner.read_text(encoding="utf-8")
    assert "class ParquetDirectorySourceManifest" in text
    assert "def parquet_multisource_registry_sink_raw_or_none" in text
    assert "def infer_parquet_multisource_registry" in text
    assert len(text.splitlines()) <= 500


def test_source_discovery_has_one_bounded_pipeline_owner() -> None:
    """Closely coupled discovery phases must not return to a micro-package."""
    owner = ROOT / "src/schema_sanitizer/pipeline/source_discovery.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "def _discover_directories" in text
    assert "def _discover_source" in text
    assert "def discover_existing_source_plans_async" in text
    assert len(text.splitlines()) <= 600


def test_path_source_sink_endpoints_are_grouped_as_public_units() -> None:
    """Path-source ABI3 endpoints use small compilation units by operation."""
    registry = ROOT / "cpp/src/api/python_abi3/registry"
    owners = (
        registry / "path_source_input_methods.cc",
        registry / "path_source_registry_methods.cc",
        registry / "path_source_auto_methods.cc",
    )
    source = "\n".join(owner.read_text(encoding="utf-8") for owner in owners)
    assert "py_context_to_sink_from_path_sources" in source
    assert "py_context_to_sink_from_path_source_chunk_provider" in source
    assert "py_context_to_registry_sink_from_path_sources" in source
    assert not (registry / "path_source_methods.cc").exists()
    assert not (registry / "path_source_sinks").exists()
    assert all(len(owner.read_text(encoding="utf-8").splitlines()) <= 500 for owner in owners)


def test_metadata_stream_has_two_bounded_implementation_owners() -> None:
    """Metadata schema/arrays and stream lifecycle stay visible without micro-units."""
    metadata = ROOT / "cpp/src/api/python_abi3/metadata"
    stream = metadata / "stream"
    assert {path.name for path in stream.iterdir() if path.is_file()} == {
        "array_builder.cc",
        "stream.cc",
        "stream.hh",
    }
    assert not (metadata / "utf8").exists()
    assert all(
        len(path.read_text(encoding="utf-8").splitlines()) <= 500
        for path in stream.iterdir()
        if path.is_file()
    )
    sources = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "metadata/stream/array_builder.cc" in sources
    assert "metadata/stream/stream.cc" in sources
    assert "metadata/utf8/column.cc" not in sources


def test_scalar_materialization_has_one_cohesive_scalar_owner() -> None:
    """Scalar conversion stays visible in one real translation unit."""
    conversion = ROOT / "cpp/src/internal/materialization/conversion"
    owner = conversion / "scalar.cc"
    assert owner.is_file()
    assert not (conversion / "scalar").exists()
    text = owner.read_text(encoding="utf-8")
    assert "convert_bool_scalar" in text
    assert "convert_timestamp_scalar" in text
    assert ".cc.inc" not in text
    assert len(text.splitlines()) <= 500
