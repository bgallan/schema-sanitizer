"""Protect ownership and performance changes introduced by maintenance layout 81."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_ingest_and_remote_source_plan_are_direct_modules() -> None:
    """Small orchestration domains must not regress into pass-through packages."""
    owners = (
        ROOT / "src/schema_sanitizer/api_impl/ingest.py",
        ROOT / "src/schema_sanitizer/api_impl/source_plan/remote.py",
    )
    for owner in owners:
        assert owner.is_file()
        assert not owner.with_suffix("").exists()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 750


def test_projection_duplicate_detection_is_linear() -> None:
    """Projection audits use one Counter pass instead of repeated list.count scans."""
    audits = ROOT / "src/schema_sanitizer/adapters/parquet/projection/audits"
    for name in ("partitions.py", "coverage.py"):
        text = (audits / name).read_text(encoding="utf-8")
        assert "Counter(" in text
        assert ".count(name)" not in text


def test_footer_reader_schema_has_one_bounded_owner() -> None:
    """Leaf formats, levels, and lookup indexing share one cohesive fragment."""
    reader = ROOT / "cpp/src/internal/parquet/footer_reader"
    owner = reader / "footer_reader_schema.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (reader / "schema").exists()
    text = owner.read_text(encoding="utf-8")
    assert "struct SchemaTraversalFrame" in text
    assert "pending.reserve(schema.size())" in text
    assert "row_group.columns[index]" in text
    assert "leaf_path_hash" not in text
    assert "std::ranges::equal_range" not in text


def test_retired_schema_and_python_fragments_stay_absent() -> None:
    """Removed internal paths must not return as compatibility facades."""
    removed = (
        "src/schema_sanitizer/api_impl/ingest/binary.py",
        "src/schema_sanitizer/api_impl/ingest/plan.py",
        "src/schema_sanitizer/api_impl/source_plan/remote/chunk_provider.py",
        "src/schema_sanitizer/api_impl/source_plan/remote/native_probe.py",
        "cpp/src/internal/parquet/footer_reader/schema/footer_reader_arrow_formats.cc.inc",
        "cpp/src/internal/parquet/footer_reader/schema/footer_reader_leaf_schema.cc.inc",
        "cpp/src/internal/parquet/footer_reader/schema/footer_reader_schema_levels.cc.inc",
    )
    assert not [relative for relative in removed if (ROOT / relative).exists()]
