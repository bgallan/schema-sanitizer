"""Regression coverage for concurrency sources define bounded hybrid row mode."""

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
    f"hybrid{chr(ord('a') + index // 26)}{chr(ord('a') + index % 26)}" for index in range(128)
)


def _write_rows(path: Path, rows: int) -> None:
    """Write deterministic wide scalar JSONL rows."""
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
        feature="sources-define-bounded-hybrid-row-mode adaptive JSONL row parallelism",
        first_row_columns=None,
        memory_limit_bytes=_MEMORY_LIMIT,
        threading_mode=mode,
    )
    return result, context


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


def test_wide_jsonl_uses_row_packets_with_exact_output(tmp_path: Path) -> None:
    """A sustained wide stream parses and materializes across row packets."""
    require_native()
    source = tmp_path / "wide.jsonl"
    _write_rows(source, 2_048)
    contract = _contract(source, tmp_path / "contract.jsonl")

    single_output = tmp_path / "single.jsonl"
    single_result, _ = _consume(source, single_output, mode="single", contract=contract)
    multi_output = tmp_path / "multi.jsonl"
    multi_result, context = _consume(source, multi_output, mode="multi", contract=contract)
    stats = context.performance_stats()
    counters = stats["counters"]

    assert semantic_stats(multi_result.stats) == semantic_stats(single_result.stats)
    assert multi_result.schema_registry_json == single_result.schema_registry_json
    assert_logical_files_equivalent(single_output, multi_output)
    assert counters["jsonl_row_packets_submitted"] >= 2
    assert counters["column_groups_submitted"] == 0
    assert counters["column_logical_packets_submitted"] == 0
    assert counters["peak_active_tasks"] >= 2
    assert stats["tasks"]["materialization"]["submitted"] == counters["jsonl_row_packets_submitted"]


def test_validated_raw_mode_preserves_later_parse_error_stage(
    tmp_path: Path,
) -> None:
    """A later scanner error still precedes an earlier conversion failure."""
    require_native()
    source = tmp_path / "errors.jsonl"
    rows = []
    for row_index in range(256):
        row = {name: row_index + column for column, name in enumerate(_COLUMNS)}
        if row_index == 7:
            row[_COLUMNS[0]] = "not-an-integer"
        rows.append(json.dumps(row, separators=(",", ":")))
    rows[200] = '{"broken":'
    source.write_text("\n".join(rows) + "\n", encoding="utf-8")

    clean = tmp_path / "clean.jsonl"
    _write_rows(clean, 256)
    contract = _contract(clean, tmp_path / "contract.jsonl")

    errors: dict[str, str] = {}
    for mode in ("single", "multi"):
        with pytest.raises(Exception) as exc_info:
            _consume(source, tmp_path / f"{mode}.jsonl", mode=mode, contract=contract)
        errors[mode] = str(exc_info.value)

    assert errors["multi"] == errors["single"]
    assert "JSON parse error" in errors["multi"]
