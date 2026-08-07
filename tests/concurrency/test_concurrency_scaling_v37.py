"""Regression coverage for v37 critical-path-balanced column fan-out."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native
from threading_golden import (
    assert_exceptions_equivalent,
    assert_logical_files_equivalent,
    semantic_stats,
)

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.api_impl.file_conversion.writers import write_jsonl_native_first_stream
from schema_sanitizer.api_impl.stream_output import write_raw_stream_to_file
from schema_sanitizer.core_impl.schema_registry import schema_contract_from_registry_json
from schema_sanitizer.input_impl.selection import (
    native_input_format,
    normalize_format_selector,
)
from schema_sanitizer.options_impl.call_options import normalize_call_options

_MEMORY_LIMIT = 64 * 1024 * 1024
_FIXED_TIME_NS = 1_700_000_000_123_456_000


def _alpha_column_names(prefix: str, count: int) -> tuple[str, ...]:
    """Return ordered names that cannot be mistaken for version siblings."""
    return tuple(
        f"{prefix}{chr(ord('a') + index // 26)}{chr(ord('a') + index % 26)}"
        for index in range(count)
    )


_INTEGER_COLUMNS = _alpha_column_names("integer", 96)
_TEMPORAL_COLUMNS = _alpha_column_names("temporal", 32)
_REORDER_INTEGER_COLUMNS = _alpha_column_names("reorderinteger", 96)
_REORDER_TEMPORAL_COLUMNS = _alpha_column_names("reordertemporal", 31)
_REORDER_TEXT_COLUMN = "reordertexttail"


@pytest.fixture(autouse=True)
def fixed_operation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep generated registry metadata identical across execution modes."""
    from schema_sanitizer.api_impl import operation_context

    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_TIME_NS)


def _write_clustered_mixed_jsonl(path: Path, rows: int) -> None:
    """Cluster higher-cost temporal fields after cheaper integer fields."""
    with path.open("w", encoding="utf-8") as handle:
        for row_index in range(rows):
            row = {name: row_index + column for column, name in enumerate(_INTEGER_COLUMNS)}
            row.update(
                {
                    name: f"2026-07-{1 + (row_index + column) % 28:02d}T"
                    f"{(row_index + column) % 24:02d}:00:00Z"
                    for column, name in enumerate(_TEMPORAL_COLUMNS)
                }
            )
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")


def _critical_path_contract(tmp_path: Path):
    """Create a strict mixed contract whose final group is submitted first."""
    source = tmp_path / "critical-path-contract.jsonl"
    row = {name: index for index, name in enumerate(_REORDER_INTEGER_COLUMNS)}
    row.update(
        {
            name: f"2026-07-{1 + index % 28:02d}T{index % 24:02d}:00:00Z"
            for index, name in enumerate(_REORDER_TEMPORAL_COLUMNS)
        }
    )
    row[_REORDER_TEXT_COLUMN] = "tail"
    source.write_text(json.dumps(row, separators=(",", ":")) + "\n", encoding="utf-8")
    result = ss.to_jsonl(
        source,
        tmp_path / "critical-path-contract-output.jsonl",
        input_format="jsonl",
        parse_integers=True,
        parse_iso_timestamps=True,
        field_name_policy="preserve",
        multi_threading=False,
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    contract = schema_contract_from_registry_json(result.schema_registry_json)
    assert contract is not None
    return contract


def _consume_critical_path_strict(source: Path, output: Path, *, mode: str, contract: object):
    """Consume the reordered mixed fixture without requiring PyArrow."""
    options = normalize_call_options(
        schema_contract=contract,
        schema_mode="strict",
        on_error="stop",
        parse_integers=True,
        parse_iso_timestamps=True,
        field_name_policy="preserve",
        multi_threading=mode == "multi",
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    context = ExecutionContext()
    sink = context.to_sink(
        source,
        sink="stream",
        options=options,
        format="jsonl",
        source="path",
    )
    return write_raw_stream_to_file(
        sink.raw,
        output,
        writer=write_jsonl_native_first_stream,
        feature="v37 critical-path error-order regression",
        first_row_columns=None,
        memory_limit_bytes=_MEMORY_LIMIT,
        threading_mode=mode,
    )


def test_v37_sources_balance_and_submit_the_critical_path_first() -> None:
    """Keep weighted contiguous ranges and order-independent merge explicit."""
    root = Path(__file__).resolve().parents[2]
    partition_header = (
        root / "cpp/src/internal/materialization/ingest_stream/column_partition.hh"
    ).read_text(encoding="utf-8")
    partition = (
        root / "cpp/src/internal/materialization/ingest_stream/column_partition.cc"
    ).read_text(encoding="utf-8")
    preparer = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_preparer.cc"
    ).read_text(encoding="utf-8")
    coordinator = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_dispatch.cc"
    ).read_text(encoding="utf-8") + (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_columns.cc"
    ).read_text(encoding="utf-8")
    state = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_impl.hh"
    ).read_text(encoding="utf-8")
    diagnostics = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_diagnostics.cc"
    ).read_text(encoding="utf-8")
    json_frontend = (root / "cpp/src/frontends/json/text_row_pipeline.cc").read_text(
        encoding="utf-8"
    )

    assert "estimated_cost" in partition_header
    assert "column_conversion_cost" in partition
    assert "remaining_cost / groups_remaining" in partition
    assert "kMaximumColumnGroups = 8" in partition
    assert "std::stable_sort" in preparer
    assert "column_group_submission_order" in coordinator
    assert "std::vector<std::uint8_t> received" in state
    assert "assembly.received[packet.column_group_index] = 1" in coordinator
    assert "packet.column_group_index != assembly.received_groups" not in coordinator
    assert "direct_max_rows_ = max_rows" in diagnostics
    assert "line_delimited && stop_on_error" in json_frontend


def test_jsonl_aliases_reach_the_dedicated_native_frontend() -> None:
    """Do not collapse line-delimited JSON back into the generic JSON scanner."""
    assert normalize_format_selector("jsonl") == "jsonl"
    assert normalize_format_selector("ndjson") == "jsonl"
    assert native_input_format("jsonl") == "jsonl"
    assert native_input_format("ndjson") == "jsonl"


def test_clustered_mixed_wide_rows_preserve_exact_output(tmp_path: Path) -> None:
    """Weighted fan-out keeps exact Arrow ownership and single-mode semantics."""
    require_native()
    source = tmp_path / "clustered-mixed.jsonl"
    _write_clustered_mixed_jsonl(source, 2_000)

    single = tmp_path / "single.jsonl"
    multi = tmp_path / "multi.jsonl"
    common = {
        "input_format": "jsonl",
        "parse_integers": True,
        "parse_iso_timestamps": True,
        "field_name_policy": "preserve",
        "on_error": "stop",
        "memory_limit_bytes": _MEMORY_LIMIT,
    }
    single_result = ss.to_jsonl(
        source,
        single,
        multi_threading=False,
        **common,
    )
    multi_result = ss.to_jsonl(
        source,
        multi,
        multi_threading=True,
        **common,
    )

    assert semantic_stats(multi_result.stats) == semantic_stats(single_result.stats)
    assert multi_result.schema_registry_json == single_result.schema_registry_json
    assert_logical_files_equivalent(single, multi)


def test_mixed_fixture_activates_parallel_materialization_telemetry(
    tmp_path: Path,
) -> None:
    """Exercise the production hybrid path instead of a serial fallback."""
    require_native()
    contract = _critical_path_contract(tmp_path)
    source = tmp_path / "telemetry-mixed.jsonl"
    with source.open("w", encoding="utf-8") as handle:
        for row_index in range(512):
            row = {name: row_index + index for index, name in enumerate(_REORDER_INTEGER_COLUMNS)}
            row.update(
                {
                    name: f"2026-07-{1 + (row_index + index) % 28:02d}T"
                    f"{(row_index + index) % 24:02d}:00:00Z"
                    for index, name in enumerate(_REORDER_TEMPORAL_COLUMNS)
                }
            )
            row[_REORDER_TEXT_COLUMN] = f"tail-{row_index}"
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")

    options = normalize_call_options(
        schema_contract=contract,
        schema_mode="strict",
        on_error="stop",
        parse_integers=True,
        parse_iso_timestamps=True,
        field_name_policy="preserve",
        multi_threading=True,
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    context = ExecutionContext()
    sink = context.to_sink(source, sink="stream", options=options, format="jsonl", source="path")
    result = write_raw_stream_to_file(
        sink.raw,
        tmp_path / "telemetry-mixed-output.jsonl",
        writer=write_jsonl_native_first_stream,
        feature="v37 column fan-out telemetry regression",
        first_row_columns=None,
        memory_limit_bytes=_MEMORY_LIMIT,
        threading_mode="multi",
    )
    report = context.performance_stats()
    counters = report["counters"]
    assert counters["jsonl_row_packets_submitted"] > 0
    assert counters["column_groups_submitted"] == 0
    assert counters["column_groups_merged"] == 0
    assert report["tasks"]["materialization"]["submitted"] > 0
    assert result.stats["batches"] == 1
    assert counters["output_batches"] >= result.stats["batches"]


def test_critical_path_first_submission_preserves_column_error_order(
    tmp_path: Path,
) -> None:
    """A heavy tail submitted first cannot overtake a lower column failure."""
    require_native()
    contract = _critical_path_contract(tmp_path)
    row = {name: index for index, name in enumerate(_REORDER_INTEGER_COLUMNS)}
    row.update(
        {
            name: f"2026-07-{1 + index % 28:02d}T{index % 24:02d}:00:00Z"
            for index, name in enumerate(_REORDER_TEMPORAL_COLUMNS)
        }
    )
    row[_REORDER_TEXT_COLUMN] = "tail"
    row[_REORDER_INTEGER_COLUMNS[0]] = "lower-column-failure"
    row[_REORDER_TEMPORAL_COLUMNS[-1]] = "heavy-tail-failure"
    source = tmp_path / "critical-path-errors.jsonl"
    source.write_text(json.dumps(row, separators=(",", ":")) + "\n", encoding="utf-8")

    def run(mode: str):
        """Run the critical-path error fixture in one execution mode."""
        return _consume_critical_path_strict(
            source,
            tmp_path / f"critical-path-errors-{mode}.jsonl",
            mode=mode,
            contract=contract,
        )

    assert_exceptions_equivalent(lambda: run("single"), lambda: run("multi"))
    with pytest.raises(RuntimeError, match=_REORDER_INTEGER_COLUMNS[0]):
        run("multi")
