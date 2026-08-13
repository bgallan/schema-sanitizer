"""Regression coverage for concurrency sources define direct rows and lazy segment state."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from _support.threading_goldens import assert_logical_files_equivalent, semantic_stats
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.api_impl.file_conversion.writers import write_jsonl_native_first_stream
from schema_sanitizer.api_impl.stream_output import write_raw_stream_to_file
from schema_sanitizer.core_impl.schema_registry import schema_contract_from_registry_json
from schema_sanitizer.options_impl.call_options import normalize_call_options

pytestmark = pytest.mark.usefixtures("fixed_operation_clock")

_MEMORY_LIMIT = 64 * 1024 * 1024
_COLUMNS = tuple(
    f"direct{chr(ord('a') + index // 26)}{chr(ord('a') + index % 26)}" for index in range(128)
)


def _write_rows(path: Path, rows: int, *, final_newline: bool = True) -> None:
    """Write deterministic wide rows with optional final newline."""
    lines = [
        json.dumps(
            {name: row_index + column for column, name in enumerate(_COLUMNS)},
            separators=(",", ":"),
        )
        for row_index in range(rows)
    ]
    path.write_text(
        "\r\n".join(lines) + ("\r\n" if final_newline else ""),
        encoding="utf-8",
    )


def _contract(source: Path, output: Path):
    """Build one frozen scalar contract through the single-thread oracle."""
    result = ss.to_jsonl(
        source,
        output,
        input_format="jsonl",
        parse_integers=True,
        field_name_policy="preserve",
        multi_threading=False,
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    contract = schema_contract_from_registry_json(result.schema_registry_json)
    assert contract is not None
    return contract


def _consume(source: Path, output: Path, *, mode: str, contract: object):
    """Consume a strict contract through the native streaming surface."""
    options = normalize_call_options(
        schema_contract=contract,
        schema_mode="strict",
        on_error="stop",
        parse_integers=True,
        field_name_policy="preserve",
        multi_threading=mode == "multi",
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    context = ExecutionContext()
    sink = context.to_sink(source, sink="stream", options=options, format="jsonl", source="path")
    result = write_raw_stream_to_file(
        sink.raw,
        output,
        writer=write_jsonl_native_first_stream,
        feature="sources-define-direct-rows-and-lazy-segment deferred JSONL row assembly",
        first_row_columns=None,
        memory_limit_bytes=_MEMORY_LIMIT,
        threading_mode=mode,
    )
    return result, context


def test_sources_define_direct_rows_and_lazy_segment_state() -> None:
    """Common JSONL rows avoid FlatRowBatch export and slow-path ownership."""
    root = Path(__file__).resolve().parents[2]
    frontend_path = root / "cpp/src/frontends/json/text_frontend.cc"
    frontend = frontend_path.read_text()
    storage = (root / "cpp/src/frontends/json/text_batch_storage.hh").read_text()
    scanner = (root / "cpp/src/internal/parsing/streaming/json/scanner_line.cc").read_text()

    assert "emits_deferred_raw_rows" in frontend
    assert "storage->append_deferred_raw(slice, rows_.requires_object_rows()," in frontend
    assert "finish_output_rows" in frontend
    assert "if (!direct_raw)" in storage
    assert "append_deferred_raw" in storage
    assert "std::to_underlying(RowFlags::kRawOnly)" in storage
    assert "RowFlags::kJsonObjectRequired" in storage
    assert scanner.index("std::memchr") < scanner.index("std::pmr::vector<LineSegment>")
    assert "until a record actually crosses an input chunk boundary" in scanner
    assert len(frontend.splitlines()) <= 600


def test_direct_raw_rows_preserve_crlf_and_missing_final_newline(
    tmp_path: Path,
) -> None:
    """The one-pass RowRef path retains exact output at the final record."""
    require_native()
    contract_source = tmp_path / "contract.jsonl"
    _write_rows(contract_source, 64)
    contract = _contract(contract_source, tmp_path / "contract-output.jsonl")
    source = tmp_path / "rows.jsonl"
    _write_rows(source, 2_048, final_newline=False)

    single_output = tmp_path / "single.jsonl"
    single_result, _ = _consume(source, single_output, mode="single", contract=contract)
    multi_output = tmp_path / "multi.jsonl"
    multi_result, context = _consume(source, multi_output, mode="multi", contract=contract)
    counters = context.performance_stats()["counters"]

    assert semantic_stats(multi_result.stats) == semantic_stats(single_result.stats)
    assert multi_result.schema_registry_json == single_result.schema_registry_json
    assert_logical_files_equivalent(single_output, multi_output)
    assert counters["jsonl_validation_packets_submitted"] >= 2
    assert counters["jsonl_token_rows_indexed"] == 2_048


def test_chunk_crossing_jsonl_record_keeps_exact_owner_and_offsets(
    tmp_path: Path,
) -> None:
    """The lazy segment vector still handles records larger than one chunk."""
    require_native()
    payload = "x" * (2 * 1024 * 1024 + 257)
    source = tmp_path / "large.jsonl"
    rows = [
        {"ordinal": 0, "payload": payload},
        {"ordinal": 1, "payload": payload[::-1]},
    ]
    source.write_text(
        "\n".join(json.dumps(row, separators=(",", ":")) for row in rows),
        encoding="utf-8",
    )

    single = ss.to_jsonl(
        source,
        tmp_path / "single.jsonl",
        input_format="jsonl",
        parse_integers=True,
        multi_threading=False,
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    multi = ss.to_jsonl(
        source,
        tmp_path / "multi.jsonl",
        input_format="jsonl",
        parse_integers=True,
        multi_threading=True,
        memory_limit_bytes=_MEMORY_LIMIT,
    )

    assert semantic_stats(multi.stats) == semantic_stats(single.stats)
    assert multi.schema_registry_json == single.schema_registry_json
    assert_logical_files_equivalent(tmp_path / "single.jsonl", tmp_path / "multi.jsonl")
