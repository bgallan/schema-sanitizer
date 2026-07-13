"""Protect cohesive layout owners and constant-memory delta encoding from layout 85."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_recursive_layout_helpers_are_direct_bounded_owners() -> None:
    """Field reduction and finalization must not return to micro-packages."""
    layout = ROOT / "src/schema_sanitizer/adapters/parquet/layout"
    for name, symbols in (
        (
            "fields.py",
            (
                "def accumulate_recursive_field",
                "def leaf_contracts_from_field",
            ),
        ),
        (
            "finalization.py",
            (
                "def collect_layout_path_collisions",
                "def build_layout_contract_maps",
                "def finalize_recursive_layout_summary",
            ),
        ),
    ):
        owner = layout / name
        text = owner.read_text(encoding="utf-8")
        assert len(text.splitlines()) <= 500
        for symbol in symbols:
            assert symbol in text
    assert not (layout / "fields").exists()
    assert not (layout / "finalization").exists()


def test_delta_binary_writer_uses_fixed_block_storage() -> None:
    """DELTA_BINARY_PACKED encoding must not allocate vectors per block."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    text = (writer / "stream_writer_value_encodings.cc.inc").read_text(encoding="utf-8")
    assert "std::array<std::int64_t, kBlockSize> block_deltas" in text
    assert "std::array<std::array<std::uint64_t, kMiniBlockSize>" in text
    assert "std::span<const std::uint64_t> values" in text
    assert "std::vector<std::int64_t> deltas" not in text
    assert "std::array<std::vector<std::uint64_t>" not in text


def test_delta_binary_reader_has_one_visible_owner() -> None:
    """Decode phases stay together without a three-file include package."""
    pages = ROOT / "cpp/src/internal/parquet/footer_reader/pages"
    owner = pages / "footer_reader_delta_binary.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (pages / "delta_binary").exists()
