"""Protect ownership boundaries introduced by maintenance layout 43."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_retired_python_facades_stay_absent() -> None:
    """Retired mixed-responsibility modules must not return as facades."""
    for module_name in (
        "schema_sanitizer.api_impl.file_conversion.api",
        "schema_sanitizer.adapters.parquet.observability",
        "schema_sanitizer.adapters.parquet.stream_factory",
    ):
        assert importlib.util.find_spec(module_name) is None


def test_new_python_owner_packages_do_not_flatten_symbols() -> None:
    """Owner packages must not recreate the removed modules through reexports."""
    from schema_sanitizer.adapters.parquet import record_batch_factory, telemetry

    assert importlib.util.find_spec("schema_sanitizer.api_impl.file_conversion.public") is None
    assert not hasattr(record_batch_factory, "iter_parquet_record_batches")
    assert not hasattr(telemetry, "record_native_reader_result")


def test_xml_streaming_scanner_is_grouped_by_phase() -> None:
    """The incremental XML scanner must remain split under its owned package."""
    streaming = ROOT / "cpp/src/internal/parsing/streaming"
    scanner = streaming / "xml"

    assert {path.name for path in scanner.iterdir() if path.is_file()} == {
        "row_scanner.cc",
        "row_scanner.hh",
        "row_scanner_buffer.cc",
        "row_scanner_markup.cc",
    }
    assert not (streaming / "xml_row_tag_scanner.cc").exists()
    assert not (streaming / "xml_row_tag_scanner.hh").exists()
    assert not (streaming / "xml_row_tag_scanner_buffer.cc").exists()
