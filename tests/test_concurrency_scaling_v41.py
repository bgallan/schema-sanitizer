"""Regression coverage for v41 parallel JSONL validation and tokenization."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native
from threading_golden import assert_logical_files_equivalent

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.api_impl.file_conversion.writers import write_jsonl_native_first_stream
from schema_sanitizer.api_impl.stream_output import write_raw_stream_to_file
from schema_sanitizer.core_impl.schema_registry import schema_contract_from_registry_json
from schema_sanitizer.options_impl.call_options import normalize_call_options

_MEMORY_LIMIT = 64 * 1024 * 1024
_FIXED_TIME_NS = 1_700_000_000_123_456_000
_COLUMNS = tuple(
    f"validate{chr(ord('a') + index // 26)}{chr(ord('a') + index % 26)}" for index in range(128)
)


@pytest.fixture(autouse=True)
def fixed_operation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep generated registry metadata identical across execution modes."""
    from schema_sanitizer.api_impl import operation_context

    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_TIME_NS)


def _write_rows(path: Path, rows: int) -> None:
    """Write deterministic wide integer JSONL rows."""
    with path.open("w", encoding="utf-8") as handle:
        for row_index in range(rows):
            row = {name: row_index + column for column, name in enumerate(_COLUMNS)}
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")


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
        feature="v41 parallel JSON validation",
        first_row_columns=None,
        memory_limit_bytes=_MEMORY_LIMIT,
        threading_mode=mode,
    )
    return result, context


def test_v41_sources_define_one_arena_ordered_validation_barrier() -> None:
    """Validation uses the operation arena and completes before publication."""
    root = Path(__file__).resolve().parents[1]
    row_stream = (root / "cpp/src/sanitize/core/row_stream.hh").read_text()
    source = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source.cc"
    ).read_text()
    validation = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_json_validation.cc"
    ).read_text()
    worker = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_json_validation.cc"
    ).read_text()
    frontend = (root / "cpp/src/frontends/json/text_frontend.cc").read_text()
    telemetry = (root / "cpp/src/internal/runtime/performance_telemetry.cc").read_text()

    assert "kDeferredValidationRaw" in row_stream
    assert "FrontendMaterializationMode::kDeferredValidationRaw" in source
    assert "task_arena, TaskArenaLane::kUpstream" in source
    assert "TaskTelemetryKind::kJsonValidation" in source
    assert "json_validation_executor_->TakeNext()" in validation
    assert "validated_jsonl_packets_.push_back" in validation
    assert "proportional_token_share" in validation
    assert "ParallelJsonRowValidator::Validate" in worker
    assert "validate_json_text_row" in worker
    assert "return validate_raw_ ? token_index_max_fields_" in frontend
    assert '"json_validation"' in telemetry
    assert '"jsonl_validation_packets_submitted"' in telemetry
    assert '"jsonl_validation_packets_completed"' in telemetry


def test_parallel_validation_and_materialization_share_exact_output(
    tmp_path: Path,
) -> None:
    """Both stages use several tasks while preserving the single oracle."""
    require_native()
    source = tmp_path / "wide.jsonl"
    _write_rows(source, 2_048)
    contract = _contract(source, tmp_path / "contract.jsonl")

    single_output = tmp_path / "single.jsonl"
    single_result, single_context = _consume(
        source, single_output, mode="single", contract=contract
    )
    multi_output = tmp_path / "multi.jsonl"
    multi_result, context = _consume(source, multi_output, mode="multi", contract=contract)
    stats = context.performance_stats()
    counters = stats["counters"]
    validation_tasks = stats["tasks"]["json_validation"]
    materialization_tasks = stats["tasks"]["materialization"]
    diagnosis = stats["diagnosis"]
    single_stats = single_context.performance_stats()

    assert multi_result.stats == single_result.stats
    assert multi_result.schema_registry_json == single_result.schema_registry_json
    assert_logical_files_equivalent(single_output, multi_output)
    assert counters["jsonl_validation_packets_submitted"] >= 2
    assert (
        counters["jsonl_validation_packets_submitted"]
        == counters["jsonl_validation_packets_completed"]
    )
    assert validation_tasks["submitted"] == counters["jsonl_validation_packets_submitted"]
    assert validation_tasks["finished"] == validation_tasks["submitted"]
    assert materialization_tasks["submitted"] == counters["jsonl_row_packets_submitted"]
    assert counters["jsonl_token_rows_indexed"] == 2_048
    assert counters["jsonl_token_fields_indexed"] == 2_048 * len(_COLUMNS)
    assert counters["jsonl_token_rows_fallback"] == 0
    assert counters["peak_active_tasks"] >= 2
    assert counters["started_workers"] == stats["effective_workers"]
    assert single_stats["counters"]["started_workers"] == 0
    assert single_stats["tasks"]["json_validation"]["submitted"] == 0
    assert diagnosis["json_validation_worker_parallelism"] > 0
    assert (
        diagnosis["combined_worker_parallelism"] >= diagnosis["materialization_worker_parallelism"]
    )


def test_validation_barrier_preserves_later_scanner_error_precedence(
    tmp_path: Path,
) -> None:
    """A later scanner failure still beats an earlier worker conversion."""
    require_native()
    clean = tmp_path / "clean.jsonl"
    _write_rows(clean, 512)
    contract = _contract(clean, tmp_path / "contract.jsonl")

    source = tmp_path / "errors.jsonl"
    rows: list[str] = []
    for row_index in range(512):
        row = {name: row_index + column for column, name in enumerate(_COLUMNS)}
        if row_index == 7:
            row[_COLUMNS[0]] = "not-an-integer"
        rows.append(json.dumps(row, separators=(",", ":")))
    rows[400] = '{"broken":'
    source.write_text("\n".join(rows) + "\n", encoding="utf-8")

    errors: dict[str, str] = {}
    for mode in ("single", "multi"):
        with pytest.raises(Exception) as exc_info:
            _consume(
                source,
                tmp_path / f"{mode}.jsonl",
                mode=mode,
                contract=contract,
            )
        errors[mode] = str(exc_info.value)

    assert errors["multi"] == errors["single"]
    assert "JSON parse error" in errors["multi"]
