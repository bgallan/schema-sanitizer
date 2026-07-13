"""Protect maintenance layout revision 64."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_file_output_metadata_has_one_cohesive_owner() -> None:
    """Metadata planning, lifecycle, and route state share one small owner."""
    assert importlib.util.find_spec("schema_sanitizer.adapters.pyarrow.output_metadata") is None
    owner = ROOT / "src/schema_sanitizer/adapters/pyarrow/file_metadata.py"
    assert owner.is_file()
    assert not (ROOT / "src/schema_sanitizer/adapters/pyarrow/file_metadata").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500


def test_arrow_stream_wrappers_have_one_runtime_owner() -> None:
    """Stream protocols, lifecycle, and diagnostics share one cohesive owner."""
    owner = ROOT / "src/schema_sanitizer/api_impl/streams.py"
    assert owner.is_file()
    assert not (ROOT / "src/schema_sanitizer/stream_impl.py").exists()
    assert not (ROOT / "src/schema_sanitizer/api_impl/streams").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500


def test_numeric_primitives_are_split_by_number_family() -> None:
    """Integer parsing and locale-aware floating parsing must compile separately."""
    core = ROOT / "cpp/src/core"
    assert not (core / "primitives_numeric.cpp").exists()
    assert (core / "numeric/integer.cpp").is_file()
    assert (core / "numeric/floating.cpp").is_file()
    sources = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "cpp/src/core/primitives_numeric.cpp" not in sources
    assert "cpp/src/core/numeric/integer.cpp" in sources
    assert "cpp/src/core/numeric/floating.cpp" in sources


def test_jsonl_numeric_writers_are_split_by_number_family() -> None:
    """Integer and floating JSON formatting must remain independent units."""
    output = ROOT / "cpp/src/internal/json_output"
    assert not (output / "jsonl_value_writer_numeric.cc").exists()
    assert (output / "jsonl_value_writer_integer.cc").is_file()
    assert (output / "jsonl_value_writer_floating.cc").is_file()
