"""Regression coverage for concurrency sources use one shared batch and disjoint row spans."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from _support.threading_goldens import assert_logical_files_equivalent, semantic_stats
from conftest import require_native

import schema_sanitizer as ss

pytestmark = pytest.mark.usefixtures("fixed_operation_clock")

_MEMORY_LIMIT = 64 * 1024 * 1024
_COLUMNS = tuple(f"packet_{index:02d}" for index in range(32))


def _write_rows(path: Path, rows: int) -> None:
    """Write deterministic scalar JSONL with enough rows for many packets."""
    with path.open("w", encoding="utf-8", newline="") as output:
        for row_index in range(rows):
            output.write(
                json.dumps(
                    {name: row_index + column_index for column_index, name in enumerate(_COLUMNS)},
                    separators=(",", ":"),
                )
            )
            output.write("\n")


def test_sources_use_one_shared_batch_and_disjoint_row_spans() -> None:
    """Parallel packets no longer allocate and copy a RowRef vector each."""
    root = Path(__file__).resolve().parents[2]
    header = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_packets.hh"
    ).read_text(encoding="utf-8")
    packets = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_packets.cc"
    ).read_text(encoding="utf-8")
    dispatch = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_dispatch.cc"
    ).read_text(encoding="utf-8")
    inference = (root / "cpp/src/ingest/prepare/inference.cc").read_text(encoding="utf-8")

    assert "struct OwnedRowBatch" in header
    assert "std::vector<RowRef> rows;" in header
    assert "std::span<RowRef> rows;" in header
    assert "std::shared_ptr<const void> owner;" in header
    assert "std::shared_ptr<std::vector<RowRef>> row_owner" not in header
    assert "make_owned_row_batch" in header
    assert "packet.owner = batch_owner;" in packets
    assert "std::span<RowRef>(rows.data() + start, row_count)" in packets
    builder = packets.split("build_owned_row_packet", 1)[1]
    assert "packet.rows.reserve" not in builder
    assert "packet.rows.push_back" not in builder
    assert "make_owned_row_batch(" in dispatch
    assert "std::move(current.rows)" in dispatch
    assert "std::move(current.owner)" in dispatch
    assert "make_owned_row_batch(" in inference
    assert "std::move(batch.rows)" in inference
    assert "std::move(batch.owner)" in inference


def test_zero_copy_packets_preserve_single_oracle(tmp_path: Path) -> None:
    """Shared packet views preserve every row and byte of logical output."""
    require_native()
    source = tmp_path / "source.jsonl"
    _write_rows(source, 12_000)

    single_output = tmp_path / "single.jsonl"
    single = ss.to_jsonl(
        source,
        single_output,
        input_format="jsonl",
        parse_integers=True,
        field_name_policy="preserve",
        multi_threading=False,
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    multi_output = tmp_path / "multi.jsonl"
    multi = ss.to_jsonl(
        source,
        multi_output,
        input_format="jsonl",
        parse_integers=True,
        field_name_policy="preserve",
        multi_threading=True,
        memory_limit_bytes=_MEMORY_LIMIT,
    )

    assert semantic_stats(multi.stats) == semantic_stats(single.stats)
    assert multi.schema_registry_json == single.schema_registry_json
    assert single_output.read_bytes() == multi_output.read_bytes()
    assert_logical_files_equivalent(single_output, multi_output)
