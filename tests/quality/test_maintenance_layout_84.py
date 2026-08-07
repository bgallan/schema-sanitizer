"""Protect ownership and allocation improvements introduced by layout 84."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_parquet_status_and_telemetry_are_direct_owners() -> None:
    """Status and telemetry must not regress to forwarding subpackages."""
    parquet = ROOT / "src/schema_sanitizer/adapters/parquet"
    assert (parquet / "status.py").is_file()
    assert (parquet / "telemetry.py").is_file()
    assert not (parquet / "status").exists()
    assert not (parquet / "telemetry").exists()
    for module in (
        "schema_sanitizer.adapters.parquet.status",
        "schema_sanitizer.adapters.parquet.telemetry",
    ):
        spec = importlib.util.find_spec(module)
        assert spec is not None
        assert spec.submodule_search_locations is None


def test_native_page_scratch_reuses_decode_buffers() -> None:
    """Per-page dictionary and delta buffers stay owned by stream scratch."""
    footer_reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    state = (footer_reader / "native_stream/schema/native_stream_arrow_state.cc.inc").read_text(
        encoding="utf-8"
    )
    decode = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (footer_reader / "native_stream/decode").glob("*.cc.inc")
    )
    assert "dictionary_indices" in state
    assert "delta_i64_values" in state
    assert "delta_i32_lengths" in state
    assert "scratch->dictionary_indices" in decode
    assert "scratch->delta_i64_values" in decode
    assert "scratch->delta_i32_lengths" in decode
    assert "std::vector<std::uint32_t> indices;" not in decode


def test_delta_binary_widths_are_a_borrowed_view() -> None:
    """DELTA_BINARY_PACKED must not allocate a bit-width vector per block."""
    stream = (
        ROOT / "cpp/src/internal/parquet/footer_reader/pages/footer_reader_delta_binary.cc.inc"
    ).read_text(encoding="utf-8")
    assert "const auto bit_widths =" in stream
    assert "values.substr(offset" in stream
    assert "std::vector<std::uint8_t> bit_widths" not in stream
