"""Protect ownership and hot-path changes introduced by maintenance layout 98."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_directory_preparation_package_stays_retired() -> None:
    """Directory preparation must not regain storage-role facade modules."""
    assert importlib.util.find_spec("schema_sanitizer.api_impl.input.directories") is None
    owner = ROOT / "src/schema_sanitizer/api_impl/input/directory_preparation.py"
    source = owner.read_text(encoding="utf-8")
    assert source.count("native_text_encoding_supported(") == 1
    assert source.count('len(csv_delimiter.encode("utf-8"))') == 1
    assert "suffix=FORMAT_SUFFIXES[input_format]" in source
    assert "def _attach_native_directory_manifest" not in source
    assert "def _attach_remote_native_directory_manifest" not in source


def test_metadata_stream_allocates_state_by_column_kind() -> None:
    """Metadata batches must not construct UTF-8 and timestamp state per column."""
    owner = ROOT / "cpp/src/api/python_abi3/metadata/stream/array_builder.cc"
    source = owner.read_text(encoding="utf-8")
    assert "struct MetadataColumnData" not in source
    assert "std::vector<Utf8ColumnData> utf8_columns" in source
    assert "std::vector<TimestampMicrosColumnData> timestamp_columns" in source
    assert "std::ranges::count_if" in source
    layout = (owner.parent / "stream.cc").read_text(encoding="utf-8")
    assert "BorrowedStringLookupSet names" in layout


def test_retired_metadata_stream_micro_units_stay_absent() -> None:
    """Old stream phases and UTF-8 subpackage must not return as compatibility files."""
    stream = ROOT / "cpp/src/api/python_abi3/metadata/stream"
    retired = {
        "api.hh",
        "array.cc",
        "builder_parts.hh",
        "builders.hh",
        "callbacks.cc",
        "callbacks.hh",
        "column.cc",
        "release.cc",
        "schema.cc",
        "state.hh",
        "wrapper.cc",
    }
    assert all(not (stream / name).exists() for name in retired)
    assert not (ROOT / "cpp/src/api/python_abi3/metadata/utf8").exists()
