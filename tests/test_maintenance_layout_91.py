"""Protect maintenance layout revision 91."""

from __future__ import annotations

from pathlib import Path

from schema_sanitizer.pipeline import (
    SchemaDriftDiff,
    discover_existing_source_plans,
    read_parquet_schema,
)

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"


def test_directory_input_and_pipeline_discovery_have_distinct_single_owners() -> None:
    """Reusable folder input state and pipeline orchestration must not share a vague name."""
    directory_inputs = SRC / "input_impl/directory_inputs.py"
    source_discovery = SRC / "pipeline/source_discovery.py"
    assert directory_inputs.is_file()
    assert source_discovery.is_file()
    assert not (SRC / "input_impl/discovery.py").exists()
    assert not (SRC / "pipeline/source_discovery").exists()
    assert "def folder_files" in directory_inputs.read_text(encoding="utf-8")
    discovery_text = source_discovery.read_text(encoding="utf-8")
    directory_text = directory_inputs.read_text(encoding="utf-8")
    assert "def discover_existing_source_plans_async" in discovery_text
    assert "DirectoryDiscoveryBuilder" in discovery_text
    assert "dict.fromkeys" in directory_text
    assert discover_existing_source_plans.__module__ == "schema_sanitizer.pipeline.source_discovery"


def test_pipeline_schema_operations_have_one_owner_without_facades() -> None:
    """Parquet schema loading and drift comparison stay in one bounded module."""
    owner = SRC / "pipeline/schemas.py"
    assert owner.is_file()
    assert not (SRC / "pipeline/parquet.py").exists()
    assert not (SRC / "pipeline/schema_drift.py").exists()
    text = owner.read_text(encoding="utf-8")
    assert "def read_parquet_schema" in text
    assert "def diff_arrow_schemas" in text
    assert read_parquet_schema.__module__ == "schema_sanitizer.pipeline.schemas"
    assert SchemaDriftDiff.__module__ == "schema_sanitizer.pipeline.schemas"
    assert len(text.splitlines()) <= 500


def test_jsonl_output_adapters_have_one_translation_unit() -> None:
    """File, Python, and string JSONL destinations share one lifecycle owner."""
    package = CPP / "api/python_abi3/json/output_adapters"
    owner = package / "output_adapters.cc"
    assert {path.name for path in package.iterdir()} == {"api.hh", "output_adapters.cc"}
    source = owner.read_text(encoding="utf-8")
    assert source.count("class FileJsonlOutput") == 1
    assert source.count("class PythonJsonlOutput") == 1
    assert source.count("class StringJsonlOutput") == 1
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert manifest.count("json/output_adapters/output_adapters.cc") == 1
    for retired in ("file.cc", "python.cc", "python_stream.cc", "string.cc"):
        assert f"json/output_adapters/{retired}" not in manifest


def test_product_owners_stay_below_500_lines_including_inc_fragments() -> None:
    """No Python or native implementation may hide an oversized owner."""
    candidates = [
        *SRC.rglob("*.py"),
        *CPP.rglob("*.cc"),
        *CPP.rglob("*.cpp"),
        *CPP.rglob("*.hh"),
        *CPP.rglob("*.inc"),
    ]
    oversized = {
        str(path.relative_to(ROOT)): len(path.read_text(encoding="utf-8").splitlines())
        for path in candidates
        if len(path.read_text(encoding="utf-8").splitlines()) > 500
    }
    assert oversized == {}
