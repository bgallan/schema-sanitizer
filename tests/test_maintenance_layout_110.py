"""Maintenance contracts for layout 110 ownership and projection cleanup."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src/schema_sanitizer"
FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader"


def test_remote_chunk_prefetch_is_owned_by_the_remote_source_plan() -> None:
    """Remote plan lifecycle must not be split across the generic input package."""
    owner = SRC / "api_impl/source_plan/remote.py"
    source = owner.read_text(encoding="utf-8")
    assert "class RemoteChunkPrefetchIterator" in source
    assert "class RemotePathSourceChunkProvider" in source
    assert "deque[Future[Any]]" in source
    assert "RemoteIoCoordinator" in source
    assert "ThreadPoolExecutor" not in source
    assert ".popleft()" in source
    assert len(source.splitlines()) <= 500
    assert not (SRC / "api_impl/input/remote_chunks.py").exists()


def test_direct_native_writers_live_with_file_conversion() -> None:
    """Native direct output must stay beside its only orchestration consumer."""
    owner = SRC / "api_impl/file_conversion/direct_writers.py"
    consumer = SRC / "api_impl/file_conversion/writers.py"
    assert owner.is_file()
    assert "try_write_parquet_direct_native" in owner.read_text(encoding="utf-8")
    assert "from . import direct_writers as _native_output" in consumer.read_text(encoding="utf-8")
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (SRC / "api_impl/native_output.py").exists()


def test_parquet_schema_leaf_analysis_is_iterative_and_reused() -> None:
    """Deep schemas and projections must avoid recursion and repeated sorting."""
    schema = (FOOTER / "footer_reader_schema.cc.inc").read_text(encoding="utf-8")
    public = (FOOTER / "reporting/footer_reader_public.cc.inc").read_text(encoding="utf-8")
    assert "struct SchemaTraversalFrame" in schema
    assert "pending.reserve(schema.size())" in schema
    assert "collect_leaf_levels(" not in schema
    assert "BorrowedStringLookupMap<std::vector<std::size_t>>" in schema
    assert "projected_top_level_leaf_indices" in schema
    assert "leaf_path_hash" not in schema
    assert "std::ranges::sort" not in schema
    assert "SAN_ASSIGN_OR_RAISE(leaves, assign_column_levels(&info))" in public
    assert "projected_top_level_leaf_indices(" in public
    assert "std::ranges::sort" not in public
    assert public.count("projected_top_level_leaf_indices(") == 1


def test_level_decoder_fragments_are_consolidated_but_bounded() -> None:
    """Bitmap, primitive, and stream decoding belong to one cohesive owner."""
    owner = FOOTER / "pages/footer_reader_levels.cc.inc"
    source = owner.read_text(encoding="utf-8")
    assert "initialize_validity_bitmap" in source
    assert "read_varint_from" in source
    assert "decode_level_stream" in source
    assert len(source.splitlines()) <= 500
    for retired in (
        "footer_reader_level_bitmap.cc.inc",
        "footer_reader_level_primitives.cc.inc",
        "footer_reader_level_stream.cc.inc",
    ):
        assert not (FOOTER / "pages" / retired).exists()
