"""Pin source-side high-core scaling to eligible fixed-wide output.

A synthetic sixteen-worker arena must form distinct input and output lanes, while machines below
nine CPUs keep deferred admission instead of paying the high-width scheduling cost.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from _support.threading_goldens import assert_logical_files_equivalent, semantic_stats

import schema_sanitizer as ss
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

pytestmark = pytest.mark.usefixtures("fixed_operation_clock")

_MEMORY_LIMIT = (256 if os.name == "nt" else 64) * 1024 * 1024
_COLUMNS = tuple(f"fixed_output_{index:03d}" for index in range(128))


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
        multi_threading=False,
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
        multi_threading=mode == "multi",
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    context = ExecutionContext()
    sink = context.to_sink(source, sink="stream", options=options, format="jsonl", source="path")
    result = write_raw_stream_to_file(
        sink.raw,
        output,
        writer=write_jsonl_native_first_stream,
        feature="sources-scale-only-fixed-wide-output-after adaptive fixed-width output",
        first_row_columns=None,
        memory_limit_bytes=_MEMORY_LIMIT,
        threading_mode=mode,
    )
    sink.close()
    return result, context


def test_sources_scale_only_fixed_wide_output_after_eight_cpus() -> None:
    """Eligible fixed-wide output scales through the high half of 32 CPUs."""
    root = Path(__file__).resolve().parents[2]
    writer = (root / "cpp/src/internal/json_output/jsonl_stream_writer.cc").read_text()
    arena = (root / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    assert "wide_flat_worker_ceiling_for(8) == 4" in writer
    assert "wide_flat_worker_ceiling_for(16) == 8" in writer
    assert "wide_flat_worker_ceiling_for(32) == 16" in writer
    assert "wide_fixed_worker_ceiling_for(32, 128) == 8" in writer
    assert "is_wide_fixed_flat_schema" in writer
    assert "should_scale_wide_fixed_output(true, 8)" in writer
    assert "should_scale_wide_fixed_output(true, 16)" in writer
    assert "should_scale_wide_fixed_output(false, 16)" in writer
    assert "scale_wide_fixed_output || has_nested_output(root)" in writer
    assert "TaskArenaLane::kOutput" in writer
    assert "first_non_output" not in arena
    assert "kOutputPrioritySubmissions" not in arena


def test_synthetic_sixteen_worker_arena_separates_eight_plus_eight_lanes(
    require_native: None,
) -> None:
    """Eight upstream and eight output workers share exactly sixteen threads."""
    workers, peak, total_threads, overlap, upstream, output, submitted = (
        native_core.operation_task_arena_probe(16, 8, 8, 32)
    )

    assert workers == 16
    # Physical arena width remains separate from runnable CPU admission. The
    # arena still owns sixteen physical workers/lane identities, while the
    # dynamic ProcessCpuGovernor may cap simultaneous runnable tasks below 16.
    assert 1 <= peak <= workers
    assert total_threads == 16
    assert overlap == 0
    assert upstream == 8
    assert output == 8
    assert submitted == 64


def test_fixed_wide_output_preserves_deferred_admission_below_nine_cpus(
    tmp_path: Path,
    require_native: None,
) -> None:
    """The high-core policy is dormant whenever memory/CPU policy stays below nine."""
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
    assert semantic_stats(multi_result.stats) == semantic_stats(single_result.stats)
    assert multi_result.schema_registry_json == single_result.schema_registry_json
    assert_logical_files_equivalent(single_output, multi_output)
    assert single_stats["counters"]["started_workers"] == 0
    assert 1 < multi_stats["effective_workers"] <= 8
    assert multi_stats["tasks"]["output"]["submitted"] > 4
