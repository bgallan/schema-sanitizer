"""Regression coverage for operation-local concurrency telemetry."""

from __future__ import annotations

import json
from pathlib import Path

from conftest import require_native

from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.api_impl.file_conversion.writers import write_jsonl_native_first_stream
from schema_sanitizer.api_impl.stream_output import write_raw_stream_to_file
from schema_sanitizer.options_impl.call_options import normalize_call_options

_MEMORY_LIMIT = 128 * 1024 * 1024
_COLUMNS = tuple(f"column_{index:03d}" for index in range(128))


def _write_wide_jsonl(path: Path, rows: int) -> None:
    """Write a deterministic fixed-width-dominant fixture."""
    with path.open("w", encoding="utf-8") as handle:
        for row_index in range(rows):
            row = {name: row_index + column for column, name in enumerate(_COLUMNS)}
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")


def _run_native_jsonl(context: ExecutionContext, source: Path, output: Path) -> dict:
    """Consume one native stream and return telemetry after ownership closes."""
    options = normalize_call_options(
        multi_threading=True,
        memory_limit_bytes=_MEMORY_LIMIT,
        on_error="stop",
    )
    sink = context.to_sink(
        source,
        sink="stream",
        options=options,
        format="jsonl",
        source="path",
    )
    write_raw_stream_to_file(
        sink.raw,
        output,
        writer=write_jsonl_native_first_stream,
        feature="v33 concurrency telemetry regression",
        first_row_columns=None,
        memory_limit_bytes=_MEMORY_LIMIT,
        threading_mode="multi",
    )
    return context.performance_stats()


def test_fresh_context_has_no_operation_telemetry() -> None:
    """No synthetic report is exposed before the first operation."""
    require_native()
    assert ExecutionContext().performance_stats() == {}


def test_completed_operation_reports_phases_tasks_and_bounded_memory(
    tmp_path: Path,
) -> None:
    """The public report covers phases, workers, queues, and operation memory."""
    require_native()
    source = tmp_path / "wide.jsonl"
    output = tmp_path / "wide-output.jsonl"
    _write_wide_jsonl(source, 2_000)

    report = _run_native_jsonl(ExecutionContext(), source, output)

    assert report["schema_version"] == 1
    assert report["operation_id"] == 1
    assert report["finished"] is True
    assert report["threading_mode"] == "multi"
    assert report["effective_workers"] >= 1
    assert report["elapsed_ns"] > 0

    memory = report["memory"]
    assert 0 <= memory["current_bytes"] <= memory["peak_bytes"]
    assert memory["peak_bytes"] <= memory["limit_bytes"] == _MEMORY_LIMIT
    assert 0.0 <= memory["peak_to_limit_ratio"] <= 1.0
    assert memory["bandwidth_proven"] is False

    phases = report["phases"]
    for phase in ("prepare", "inference", "stream_get_next", "frontend_read", "output"):
        assert phases[phase]["calls"] > 0
        assert phases[phase]["elapsed_ns"] > 0

    counters = report["counters"]
    assert counters["source_rows"] == 2_000
    assert counters["frontend_batches"] > 0
    assert counters["output_batches"] > 0
    assert counters["packets_submitted"] == counters["packets_completed"]
    assert counters["peak_queue_depth"] >= 0
    assert counters["peak_active_tasks"] >= 0

    materialization = report["tasks"]["materialization"]
    assert materialization["submitted"] == materialization["started"]
    assert materialization["started"] == materialization["finished"]
    assert materialization["run_ns"] > 0
    assert materialization["average_run_ns"] > 0

    diagnosis = report["diagnosis"]
    assert diagnosis["memory_bandwidth_proven"] is False
    assert diagnosis["memory_bandwidth_status"].startswith("requires_hardware")
    assert diagnosis["memory_capacity_status"] in {
        "no_operation_budget_pressure_observed",
        "near_operation_budget_not_proof_of_capacity_bottleneck",
    }


def test_prepare_failure_finishes_the_latest_report(tmp_path: Path) -> None:
    """An operation rejected during preparation does not remain in progress."""
    require_native()
    source = tmp_path / "invalid-json.jsonl"
    source.write_text('{"value":\n', encoding="utf-8")
    context = ExecutionContext()
    options = normalize_call_options(
        on_error="stop",
        multi_threading=True,
        memory_limit_bytes=_MEMORY_LIMIT,
    )

    try:
        context.to_sink(
            source,
            sink="stream",
            options=options,
            format="jsonl",
            source="path",
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("malformed JSON must fail during preparation")

    report = context.performance_stats()
    assert report["finished"] is True
    assert report["phases"]["prepare"]["calls"] == 1
    assert report["counters"]["packets_submitted"] == 0


def test_context_replaces_report_and_increments_operation_id(tmp_path: Path) -> None:
    """A reused context exposes only its latest operation with a stable sequence id."""
    require_native()
    context = ExecutionContext()
    source = tmp_path / "input.jsonl"
    _write_wide_jsonl(source, 64)

    first = _run_native_jsonl(context, source, tmp_path / "first.jsonl")
    second = _run_native_jsonl(context, source, tmp_path / "second.jsonl")

    assert first["operation_id"] == 1
    assert second["operation_id"] == 2
    assert second["finished"] is True
    assert context.performance_stats() == second
