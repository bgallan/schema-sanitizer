"""Regression coverage for v38 stable column materializer slots."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native
from threading_golden import assert_logical_files_equivalent, semantic_stats

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.api_impl.file_conversion.writers import write_jsonl_native_first_stream
from schema_sanitizer.api_impl.stream_output import write_raw_stream_to_file
from schema_sanitizer.core_impl.schema_registry import schema_contract_from_registry_json
from schema_sanitizer.options_impl.call_options import normalize_call_options

_MEMORY_LIMIT = 64 * 1024 * 1024
_FIXED_TIME_NS = 1_700_000_000_123_456_000
_COLUMNS = tuple(
    f"slotfield{chr(ord('a') + index // 26)}{chr(ord('a') + index % 26)}" for index in range(128)
)


@pytest.fixture(autouse=True)
def fixed_operation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep generated registry metadata identical across execution modes."""
    from schema_sanitizer.api_impl import operation_context

    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_TIME_NS)


def _write_wide_rows(path: Path, rows: int) -> None:
    """Write deterministic fixed-width rows that require multiple packets."""
    with path.open("w", encoding="utf-8") as handle:
        for row_index in range(rows):
            row = {name: row_index + column for column, name in enumerate(_COLUMNS)}
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")


def _strict_contract(source: Path, output: Path):
    """Build one exact scalar contract through the single-thread oracle."""
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


def _consume_strict(source: Path, output: Path, *, mode: str, contract: object):
    """Consume one strict contract through the native streaming surface."""
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
        feature="v38 stable column materializer slots",
        first_row_columns=None,
        memory_limit_bytes=_MEMORY_LIMIT,
        threading_mode=mode,
    )
    return result, context


def test_v38_sources_bound_column_builders_by_packet_slots() -> None:
    """One stable builder exists per packet-slot/group pair, not per worker/group."""
    root = Path(__file__).resolve().parents[2]
    task_header = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_preparer.hh"
    ).read_text(encoding="utf-8")
    state_header = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_preparer_internal.hh"
    ).read_text(encoding="utf-8")
    columns = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_preparer_columns.cc"
    ).read_text(encoding="utf-8")
    dispatch = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_dispatch.cc"
    ).read_text(encoding="utf-8")
    assembly = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_columns.cc"
    ).read_text(encoding="utf-8")
    telemetry = (root / "cpp/src/internal/runtime/performance_telemetry.cc").read_text(
        encoding="utf-8"
    )

    assert "column_state_index" in task_header
    assert "std::vector<BatchAppenderPtr> column_appenders" not in state_header
    assert "struct ParallelRowPreparer::ColumnMaterializerState" in state_header
    assert state_header.count("BatchAppenderPtr appender") == 2
    assert "slot_count = groups * packet_window" in columns
    assert "state->group_index = slot_index % groups" in columns
    assert "state.group_index != group_index" in columns
    assert "packet_slot * groups + group" in dispatch
    assert "release_column_partition_slot(completed_slot)" in assembly
    assert "if (!input->plan_ordered)" in columns
    assert '"column_slots_initialized"' in telemetry
    assert '"column_slot_reuses"' in telemetry


def test_column_slots_are_reused_with_exact_single_multi_output(tmp_path: Path) -> None:
    """Repeated packets reuse bounded slots while preserving exact output."""
    require_native()
    source = tmp_path / "stable-slots.jsonl"
    _write_wide_rows(source, 2_048)
    contract = _strict_contract(source, tmp_path / "contract-output.jsonl")

    single_output = tmp_path / "single.jsonl"
    single_result, _ = _consume_strict(source, single_output, mode="single", contract=contract)
    multi_output = tmp_path / "multi.jsonl"
    multi_result, context = _consume_strict(source, multi_output, mode="multi", contract=contract)
    counters = context.performance_stats()["counters"]

    assert semantic_stats(multi_result.stats) == semantic_stats(single_result.stats)
    assert multi_result.schema_registry_json == single_result.schema_registry_json
    assert_logical_files_equivalent(single_output, multi_output)
    if counters["column_groups_submitted"] > 0:
        assert counters["column_slots_initialized"] <= 16
        assert (
            counters["column_slots_initialized"] + counters["column_slot_reuses"]
            == counters["column_groups_submitted"]
        )
    else:
        # v39 supersedes long-lived wide packets with row-parallel JSONL.
        assert counters["jsonl_row_packets_submitted"] > 0
        assert counters["column_slots_initialized"] == 0
        assert counters["column_slot_reuses"] == 0
