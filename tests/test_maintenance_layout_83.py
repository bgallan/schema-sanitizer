"""Protect ownership and footer projection improvements introduced by layout 83."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_parquet_reader_python_owners_are_direct_and_cohesive() -> None:
    """Native routing and reusable stream creation must not return to micro-packages."""
    parquet = ROOT / "src/schema_sanitizer/adapters/parquet"
    owners = (parquet / "native_reader.py", parquet / "record_batch_factory.py")
    assert all(owner.is_file() for owner in owners)
    assert all(len(owner.read_text(encoding="utf-8").splitlines()) <= 500 for owner in owners)
    assert not (parquet / "native_reader").exists()
    assert not (parquet / "record_batch_factory").exists()
    assert not (parquet / "direct_fallback.py").exists()


def test_footer_reporting_has_three_cohesive_blocks() -> None:
    """Footer reporting should expose its actual JSON, diagnostics, and public owners."""
    reporting = ROOT / "cpp/src/internal/parquet/footer_reader/reporting"
    assert {path.name for path in reporting.iterdir() if path.is_file()} == {
        "footer_reader_diagnostics_json.cc.inc",
        "footer_reader_json.cc.inc",
        "footer_reader_public.cc.inc",
    }
    assert all(
        len(path.read_text(encoding="utf-8").splitlines()) <= 500
        for path in reporting.iterdir()
        if path.is_file()
    )


def test_footer_projection_uses_an_ordered_top_level_index() -> None:
    """Projection must not scan every row-group column for every requested name."""
    owner = ROOT / ("cpp/src/internal/parquet/footer_reader/reporting/footer_reader_public.cc.inc")
    text = owner.read_text(encoding="utf-8")
    projection = text.split("sanitize::Status project_footer_row_group_columns(", 1)[1].split(
        "sanitize::Result<ArrowArrayStream *>", 1
    )[0]
    assert "projected_top_level_leaf_indices(" in projection
    assert "std::vector<std::size_t> selected_indices" in projection
    assert "std::ranges::equal_range" not in projection
    assert "std::ranges::sort" not in projection
    assert "for (const auto &column : row_group.columns)" not in projection
