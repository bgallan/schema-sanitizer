"""Protect ownership and hot-path improvements from maintenance layout 114."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_parquet_memory_policy_owns_native_batch_contract() -> None:
    """Batch sizing and footer row-group limits have one memory-policy owner."""
    parquet = ROOT / "src/schema_sanitizer/adapters/parquet"
    owner = parquet / "memory.py"
    text = owner.read_text(encoding="utf-8")
    assert "def _native_parquet_batch_size_contract_issue" in text
    assert "def _native_parquet_max_row_group_rows" in text
    assert not (parquet / "contract_gates/reader_limits.py").exists()
    assert len(text.splitlines()) <= 500


def test_recursive_row_group_fingerprints_sort_once() -> None:
    """Every fingerprint family reuses one canonical field order per row group."""
    reducer = ROOT / "src/schema_sanitizer/adapters/parquet/layout/reducer.py"
    text = reducer.read_text(encoding="utf-8")
    assert "canonical_bundles = sorted(named_bundles" in text
    assert text.count("canonical_bundles = sorted(") == 1
    assert "canonical_named_fingerprint" not in text


def test_parquet_page_buffer_primitives_have_one_owner() -> None:
    """Arrow buffer sizes live with low-level page and bitmap primitives."""
    pages = ROOT / "cpp/src/internal/parquet/footer_reader/pages"
    owner = pages / "footer_reader_format_primitives.cc.inc"
    text = owner.read_text(encoding="utf-8")
    assert "arrow_i32_offset_buffer_bytes" in text
    assert "bool validity_bit_is_set" in text
    assert not (pages / "footer_reader_arrow_buffer_sizes.cc.inc").exists()
    assert len(text.splitlines()) <= 500


def test_native_projection_moves_unique_column_chunks() -> None:
    """Normal projections avoid deep-copying decoded page and column state."""
    owner = ROOT / "cpp/src/internal/parquet/footer_reader/reporting/footer_reader_public.cc.inc"
    text = owner.read_text(encoding="utf-8")
    assert "duplicate_selection" in text
    assert "selected.push_back(std::move(row_group.columns[index]))" in text
    assert "selected.push_back(row_group.columns[index])" in text
