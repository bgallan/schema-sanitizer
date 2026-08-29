"""Specify hybrid row handling, validation, and column slots for wide JSONL.

Row packets and the shared arena barrier must preserve output and parse-error precedence, while
column builders remain bounded by packet slots and reuse those slots exactly across worker modes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from _support.threading_goldens import assert_logical_files_equivalent, semantic_stats
from _support.wide_jsonl import (
    consume_strict,
    strict_contract,
    wide_column_names,
    write_wide_integer_rows,
)

pytestmark = pytest.mark.usefixtures("fixed_operation_clock")

_MEMORY_LIMIT = 64 * 1024 * 1024
_HYBRID_COLUMNS = wide_column_names("hybrid")
_HYBRID_FEATURE = "sources-define-bounded-hybrid-row-mode adaptive JSONL row parallelism"


def test_sources_define_bounded_hybrid_row_mode() -> None:
    """The source exposes explicit deferred-row and micro-column policies."""
    root = Path(__file__).resolve().parents[2]
    row_stream = (root / "cpp/src/sanitize/core/row_stream.hh").read_text()
    pipeline = (root / "cpp/src/frontends/json/text_row_pipeline.cc").read_text()
    partition = (
        root / "cpp/src/internal/materialization/ingest_stream/column_partition.cc"
    ).read_text()
    packets = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_packets.cc"
    ).read_text()
    source = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source.cc"
    ).read_text()
    dispatch = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_dispatch.cc"
    ).read_text()
    validation = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_json_validation.cc"
    ).read_text()
    telemetry = (root / "cpp/src/internal/runtime/performance_telemetry.cc").read_text()

    assert "kValidatedRaw" in row_stream
    assert "kDeferredValidationRaw" in row_stream
    assert "set_materialization_mode" in row_stream
    assert "validate_json_text_row" in pipeline
    assert "ForEachObjectFieldC" in pipeline
    assert "micro_rows" in partition
    assert "workers * 8" in partition
    assert "kEstimatedWideJsonRowBytes" in partition
    assert "desired_packets" in packets
    assert "workers * 2" in packets
    assert "scaled_worker_baseline" in packets
    assert "sustained_wide_flat_worker_ceiling(64, 64) == 32" in packets
    assert "FrontendMaterializationMode::kDeferredValidationRaw" in source
    assert "submit_validated_jsonl_packets" in dispatch
    assert "balanced_rows" in validation
    assert "policy_.effective_workers * 2" in validation
    assert '"jsonl_row_packets_submitted"' in telemetry
    assert '"column_logical_packets_submitted"' in telemetry


def test_wide_jsonl_uses_row_packets_with_exact_output(
    tmp_path: Path, require_native: None
) -> None:
    """A sustained wide stream parses and materializes across row packets."""
    source = tmp_path / "wide.jsonl"
    write_wide_integer_rows(source, _HYBRID_COLUMNS, 2_048)
    contract = strict_contract(source, tmp_path / "contract.jsonl")

    single_output = tmp_path / "single.jsonl"
    single_result, _ = consume_strict(
        source, single_output, mode="single", contract=contract, feature=_HYBRID_FEATURE
    )
    multi_output = tmp_path / "multi.jsonl"
    multi_result, context = consume_strict(
        source, multi_output, mode="multi", contract=contract, feature=_HYBRID_FEATURE
    )
    stats = context.performance_stats()
    counters = stats["counters"]

    assert semantic_stats(multi_result.stats) == semantic_stats(single_result.stats)
    assert multi_result.schema_registry_json == single_result.schema_registry_json
    assert_logical_files_equivalent(single_output, multi_output)
    assert counters["jsonl_row_packets_submitted"] >= 2
    assert counters["column_groups_submitted"] == 0
    assert counters["column_logical_packets_submitted"] == 0
    materialization_tasks = stats["tasks"]["materialization"]
    assert (
        materialization_tasks["submitted"]
        == materialization_tasks["started"]
        == materialization_tasks["finished"]
        == counters["jsonl_row_packets_submitted"]
    )


def test_validated_raw_mode_preserves_later_parse_error_stage(
    tmp_path: Path,
    require_native: None,
) -> None:
    """A later scanner error still precedes an earlier conversion failure."""
    source = tmp_path / "errors.jsonl"
    rows = []
    for row_index in range(256):
        row = {name: row_index + column for column, name in enumerate(_HYBRID_COLUMNS)}
        if row_index == 7:
            row[_HYBRID_COLUMNS[0]] = "not-an-integer"
        rows.append(json.dumps(row, separators=(",", ":")))
    rows[200] = '{"broken":'
    source.write_text("\n".join(rows) + "\n", encoding="utf-8")

    clean = tmp_path / "clean.jsonl"
    write_wide_integer_rows(clean, _HYBRID_COLUMNS, 256)
    contract = strict_contract(clean, tmp_path / "contract.jsonl")

    errors: dict[str, str] = {}
    for mode in ("single", "multi"):
        with pytest.raises(Exception) as exc_info:
            consume_strict(
                source,
                tmp_path / f"{mode}.jsonl",
                mode=mode,
                contract=contract,
                feature=_HYBRID_FEATURE,
            )
        errors[mode] = str(exc_info.value)

    assert errors["multi"] == errors["single"]
    assert "JSON parse error" in errors["multi"]


_VALIDATION_COLUMNS = wide_column_names("validate")
_VALIDATION_FEATURE = "sources-define-one-arena-ordered-validation-barrier parallel JSON validation"


def test_sources_define_one_arena_ordered_validation_barrier() -> None:
    """Validation uses the operation arena and completes before publication."""
    root = Path(__file__).resolve().parents[2]
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
    require_native: None,
) -> None:
    """Validation and materialization use several tasks with exact output."""
    source = tmp_path / "wide.jsonl"
    write_wide_integer_rows(source, _VALIDATION_COLUMNS, 2_048)
    contract = strict_contract(source, tmp_path / "contract.jsonl")
    single_output = tmp_path / "single.jsonl"
    single_result, single_context = consume_strict(
        source,
        single_output,
        mode="single",
        contract=contract,
        feature=_VALIDATION_FEATURE,
    )
    multi_output = tmp_path / "multi.jsonl"
    multi_result, context = consume_strict(
        source,
        multi_output,
        mode="multi",
        contract=contract,
        feature=_VALIDATION_FEATURE,
    )
    stats = context.performance_stats()
    counters = stats["counters"]
    validation_tasks = stats["tasks"]["json_validation"]
    materialization_tasks = stats["tasks"]["materialization"]
    diagnosis = stats["diagnosis"]
    single_stats = single_context.performance_stats()
    assert semantic_stats(multi_result.stats) == semantic_stats(single_result.stats)
    assert multi_result.schema_registry_json == single_result.schema_registry_json
    assert_logical_files_equivalent(single_output, multi_output)
    assert counters["jsonl_validation_packets_submitted"] >= 2
    assert (
        counters["jsonl_validation_packets_submitted"]
        == counters["jsonl_validation_packets_completed"]
    )
    assert validation_tasks["submitted"] == counters["jsonl_validation_packets_submitted"]
    assert (
        validation_tasks["submitted"] == validation_tasks["started"] == validation_tasks["finished"]
    )
    assert (
        materialization_tasks["submitted"]
        == materialization_tasks["started"]
        == materialization_tasks["finished"]
        == counters["jsonl_row_packets_submitted"]
    )
    assert counters["jsonl_token_rows_indexed"] == 2_048
    assert counters["jsonl_token_fields_indexed"] == 2_048 * len(_VALIDATION_COLUMNS)
    assert counters["jsonl_token_rows_fallback"] == 0
    started_workers = counters["started_workers"]
    assert 1 <= started_workers <= stats["effective_workers"]
    assert single_stats["counters"]["started_workers"] == 0
    assert single_stats["tasks"]["json_validation"]["submitted"] == 0
    assert diagnosis["json_validation_worker_parallelism"] > 0
    assert (
        diagnosis["combined_worker_parallelism"] >= diagnosis["materialization_worker_parallelism"]
    )


def test_validation_barrier_preserves_later_scanner_error_precedence(
    tmp_path: Path,
    require_native: None,
) -> None:
    """A later scanner failure still beats an earlier worker conversion."""
    clean = tmp_path / "clean.jsonl"
    write_wide_integer_rows(clean, _VALIDATION_COLUMNS, 512)
    contract = strict_contract(clean, tmp_path / "contract.jsonl")
    source = tmp_path / "errors.jsonl"
    rows: list[str] = []
    for row_index in range(512):
        row = {name: row_index + column for column, name in enumerate(_VALIDATION_COLUMNS)}
        if row_index == 7:
            row[_VALIDATION_COLUMNS[0]] = "not-an-integer"
        rows.append(json.dumps(row, separators=(",", ":")))
    rows[400] = '{"broken":'
    source.write_text("\n".join(rows) + "\n", encoding="utf-8")
    errors: dict[str, str] = {}
    for mode in ("single", "multi"):
        with pytest.raises(Exception) as exc_info:
            consume_strict(
                source,
                tmp_path / f"{mode}.jsonl",
                mode=mode,
                contract=contract,
                feature=_VALIDATION_FEATURE,
            )
        errors[mode] = str(exc_info.value)
    assert errors["multi"] == errors["single"]
    assert "JSON parse error" in errors["multi"]


_SLOT_COLUMNS = wide_column_names("slotfield")
_SLOT_FEATURE = "sources-bound-column-builders-by-packet-slots stable column materializer slots"


def test_sources_bound_column_builders_by_packet_slots() -> None:
    """One stable builder exists per packet-slot/group pair."""
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


def test_column_slots_are_reused_with_exact_single_multi_output(
    tmp_path: Path, require_native: None
) -> None:
    """Repeated packets reuse bounded slots while preserving exact output."""
    source = tmp_path / "stable-slots.jsonl"
    write_wide_integer_rows(source, _SLOT_COLUMNS, 2_048)
    contract = strict_contract(source, tmp_path / "contract-output.jsonl")
    single_output = tmp_path / "single.jsonl"
    single_result, _ = consume_strict(
        source,
        single_output,
        mode="single",
        contract=contract,
        feature=_SLOT_FEATURE,
    )
    multi_output = tmp_path / "multi.jsonl"
    multi_result, context = consume_strict(
        source,
        multi_output,
        mode="multi",
        contract=contract,
        feature=_SLOT_FEATURE,
    )
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
        assert counters["jsonl_row_packets_submitted"] > 0
        assert counters["column_slots_initialized"] == 0
        assert counters["column_slot_reuses"] == 0
