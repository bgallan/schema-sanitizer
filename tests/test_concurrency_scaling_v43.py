"""Regression coverage for v43 adaptive wide fixed-width JSONL output."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native
from threading_golden import assert_logical_files_equivalent

import schema_sanitizer as ss
from schema_sanitizer.api_impl import operation_context
from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.api_impl.file_conversion.writers import (
    write_jsonl_native_first_stream,
)
from schema_sanitizer.api_impl.stream_output import write_raw_stream_to_file
from schema_sanitizer.core_impl.native_runtime import native_core
from schema_sanitizer.core_impl.schema_registry import (
    schema_contract_from_registry_json,
)
from schema_sanitizer.options_impl.call_options import normalize_call_options

_MEMORY_LIMIT = 64 * 1024 * 1024
_FIXED_TIME_NS = 1_700_000_000_123_456_000
_COLUMNS = tuple(f"fixed_output_{index:03d}" for index in range(128))


@pytest.fixture(autouse=True)
def fixed_operation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep generated registry metadata identical across execution modes."""
    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_TIME_NS)


def _write_rows(path: Path, rows: int) -> None:
    """Write deterministic fixed-width scalar JSONL rows."""
    with path.open("w", encoding="utf-8", newline="") as output:
        for row_index in range(rows):
            output.write(
                json.dumps(
                    {name: row_index + column_index for column_index, name in enumerate(_COLUMNS)},
                    separators=(",", ":"),
                )
            )
            output.write("\n")


def _contract(source: Path, output: Path):
    """Build a frozen contract through the single-thread oracle."""
    result = ss.to_jsonl(
        source,
        output,
        input_format="jsonl",
        parse_integers=True,
        field_name_policy="preserve",
        threading_mode="single",
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    contract = schema_contract_from_registry_json(result.schema_registry_json)
    assert contract is not None
    return contract


def _consume(source: Path, output: Path, *, mode: str, contract: object):
    """Consume one strict contract and return result plus telemetry context."""
    options = normalize_call_options(
        schema_contract=contract,
        schema_mode="strict",
        on_error="stop",
        parse_integers=True,
        field_name_policy="preserve",
        threading_mode=mode,
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    context = ExecutionContext()
    sink = context.to_sink(source, sink="stream", options=options, format="jsonl", source="path")
    result = write_raw_stream_to_file(
        sink.raw,
        output,
        writer=write_jsonl_native_first_stream,
        feature="v43 adaptive fixed-width output",
        first_row_columns=None,
        memory_limit_bytes=_MEMORY_LIMIT,
        threading_mode=mode,
    )
    sink.close()
    return result, context


def test_v43_sources_scale_only_fixed_wide_output_after_eight_cpus() -> None:
    """The 16-CPU frontier doubles fixed-wide output without global priority."""
    root = Path(__file__).resolve().parents[1]
    writer = (root / "cpp/src/internal/json_output/jsonl_stream_writer.cc").read_text()
    arena = (root / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    assert "wide_flat_worker_ceiling_for(8) == 4" in writer
    assert "wide_flat_worker_ceiling_for(16) == 8" in writer
    assert "is_wide_fixed_flat_schema" in writer
    assert "should_scale_wide_fixed_output(true, 8)" in writer
    assert "should_scale_wide_fixed_output(true, 16)" in writer
    assert "should_scale_wide_fixed_output(false, 16)" in writer
    assert "scale_wide_fixed_output || has_nested_output(root)" in writer
    assert "TaskArenaLane::kOutput" in writer
    assert "first_non_output" not in arena
    assert "kOutputPrioritySubmissions" not in arena


def test_synthetic_sixteen_worker_arena_separates_eight_plus_eight_lanes() -> None:
    """Eight upstream and eight output workers share exactly sixteen threads."""
    require_native()
    workers, peak, total_threads, overlap, upstream, output, submitted = (
        native_core.operation_task_arena_probe(16, 8, 8, 32)
    )

    assert workers == 16
    assert peak == 16
    assert total_threads == 16
    assert overlap == 0
    assert upstream == 8
    assert output == 8
    assert submitted == 64


def test_fixed_wide_output_preserves_v42_admission_below_nine_cpus(
    tmp_path: Path,
) -> None:
    """The 16-CPU policy is dormant on this four-CPU host."""
    require_native()
    contract_source = tmp_path / "contract.jsonl"
    _write_rows(contract_source, 64)
    contract = _contract(contract_source, tmp_path / "contract-output.jsonl")
    source = tmp_path / "rows.jsonl"
    _write_rows(source, 4_096)

    single_output = tmp_path / "single.jsonl"
    single_result, single_context = _consume(
        source, single_output, mode="single", contract=contract
    )
    multi_output = tmp_path / "multi.jsonl"
    multi_result, multi_context = _consume(source, multi_output, mode="multi", contract=contract)

    single_stats = single_context.performance_stats()
    multi_stats = multi_context.performance_stats()
    assert multi_result.stats == single_result.stats
    assert multi_result.schema_registry_json == single_result.schema_registry_json
    assert_logical_files_equivalent(single_output, multi_output)
    assert single_stats["counters"]["started_workers"] == 0
    assert multi_stats["effective_workers"] == 4
    assert multi_stats["tasks"]["output"]["submitted"] > 4
