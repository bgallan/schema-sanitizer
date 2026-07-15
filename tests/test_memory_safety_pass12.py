"""Regression coverage for bounded input sources under the unified budget."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native

ROOT = Path(__file__).resolve().parents[1]


def test_chunk_sources_share_a_finite_request_ceiling() -> None:
    """Every built-in source rejects a single oversized chunk request."""
    limits = (ROOT / "cpp/src/ingest/chunk_source_detail.hh").read_text(encoding="utf-8")
    budget = (ROOT / "cpp/src/internal/memory/memory_budget.hh").read_text(encoding="utf-8")
    compact_budget = " ".join(budget.split())
    sources = [
        ROOT / "cpp/src/ingest/chunk_source_file.cc",
        ROOT / "cpp/src/ingest/chunk_source_memory.cc",
        ROOT / "cpp/src/ingest/chunk_source_multi_path.cc",
        ROOT / "cpp/src/ingest/transcoding/chunk_source.cc",
    ]
    assert "kMaxChunkRequestBytes" in limits
    assert "std::int64_t{256} * 1024 * 1024" in limits
    assert "out.io_chunk_bytes = bounded_fraction" in compact_budget
    for source in sources:
        assert "validate_chunk_request" in source.read_text(encoding="utf-8")


def test_transcoding_decoder_avoids_a_second_utf16_input_copy() -> None:
    """UTF-16 decoding processes carried bytes and raw input without joining them."""
    decoder = (ROOT / "cpp/src/ingest/transcoding/decoder.cc").read_text(encoding="utf-8")
    utf16 = decoder.split("TranscodingDecoder::transcode_utf16", 1)[1].split(
        "TranscodingDecoder::append_utf16_pair", 1
    )[0]
    latin1 = decoder.split("TranscodingDecoder::transcode_latin1", 1)[1].split(
        "TranscodingDecoder::transcode_utf16", 1
    )[0]
    assert "std::string bytes" not in utf16
    assert "append_utf16_pair" in utf16
    assert "out.reserve(reserve_size)" in utf16
    assert "std::string out(output_size, '\\0')" in latin1
    assert "out.push_back" not in latin1


def test_full_materialization_uses_the_operation_budget() -> None:
    """mmap, transcoding and multi-path views receive one explicit limit."""
    detail = (ROOT / "cpp/src/ingest/chunk_source_detail.hh").read_text(encoding="utf-8")
    file_source = (ROOT / "cpp/src/ingest/chunk_source_file.cc").read_text(encoding="utf-8")
    transcoding = (ROOT / "cpp/src/ingest/transcoding/chunk_source.cc").read_text(encoding="utf-8")
    multi_path = (ROOT / "cpp/src/ingest/chunk_source_multi_path.cc").read_text(encoding="utf-8")
    assert "validate_materialized_input_growth" in detail
    for source in (file_source, transcoding, multi_path):
        assert "materialized_limit_" in source
        assert "memory_budget_from_limit(memory_limit_bytes)" in source
    assert "source->View()" not in multi_path
    assert "source->NextChunk(kMaterializationReadBytes)" in multi_path


def test_secure_cleanup_wipes_transcoding_input_scratch() -> None:
    """Raw encoded input is cleared before temporary storage is released."""
    source = (ROOT / "cpp/src/ingest/transcoding/chunk_source.cc").read_text(encoding="utf-8")
    assert "class SensitiveStringGuard" in source
    assert "secure_memory_cleanup_enabled" in source
    assert "secure_zero_memory(value_->data(), value_->size())" in source
    assert "SensitiveStringGuard raw_guard(&raw)" in source


def test_removed_chunk_parameter_is_not_accepted(tmp_path: Path) -> None:
    """Chunk sizing is derived internally and is no longer a public knob."""
    require_native()
    import schema_sanitizer as ss

    source = tmp_path / "row.jsonl"
    source.write_text('{"value":1}\n', encoding="utf-8")
    with pytest.raises(TypeError, match="read_chunk_bytes"):
        ss.to_jsonl(
            source,
            tmp_path / "out.jsonl",
            input_format="jsonl",
            read_chunk_bytes=1,
        )


def test_utf16_round_trip_under_explicit_budget(tmp_path: Path) -> None:
    """UTF-16 transcoding remains correct with only memory_limit_bytes exposed."""
    require_native()
    import schema_sanitizer as ss

    source = tmp_path / "utf16.jsonl"
    output = tmp_path / "out.jsonl"
    expected = {"name": "café 😀"}
    source.write_bytes((json.dumps(expected, ensure_ascii=False) + "\n").encode("utf-16"))
    ss.to_jsonl(
        source,
        output,
        input_format="jsonl",
        input_text_encoding="utf-16",
        memory_limit_bytes=1 << 20,
    )
    assert json.loads(output.read_text(encoding="utf-8"))["name"] == expected["name"]


def test_transcoded_output_is_sliced_without_copying_oversized_scalars() -> None:
    """Decoded UTF-8 is retained once and exposed through bounded views."""
    source = (ROOT / "cpp/src/ingest/transcoding/chunk_source.cc").read_text(encoding="utf-8")
    assert "pending_utf8_" in source
    assert "take_pending_chunk(max_bytes)" in source
    assert "std::string_view(*owner).substr(pending_utf8_pos_, take)" in source


def test_multi_path_directory_preserves_separator_order(tmp_path: Path) -> None:
    """Directory children remain ordered under the derived chunk budget."""
    require_native()
    import schema_sanitizer as ss

    source = tmp_path / "inputs"
    source.mkdir()
    output = tmp_path / "out.jsonl"
    (source / "a.jsonl").write_text('{"value":1}\n', encoding="utf-8")
    (source / "b.jsonl").write_text('{"value":2}\n', encoding="utf-8")
    ss.to_jsonl(
        source,
        output,
        input_format="jsonl",
        input_mode="directory",
        memory_limit_bytes=1 << 20,
    )
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert [row["value"] for row in rows] == [1, 2]
