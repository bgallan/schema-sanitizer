"""Regression coverage for v40 validated JSON token handoff."""

from __future__ import annotations

import json
import os
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

_FIXED_TIME_NS = 1_700_000_000_123_456_000
_COLUMNS = tuple(
    f"token{chr(ord('a') + index // 26)}{chr(ord('a') + index % 26)}" for index in range(128)
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


def _contract(source: Path, output: Path, memory_limit: int):
    """Build one frozen scalar contract through the single-thread oracle."""
    result = ss.to_jsonl(
        source,
        output,
        input_format="jsonl",
        parse_integers=True,
        field_name_policy="preserve",
        multi_threading=False,
        memory_limit_bytes=memory_limit,
    )
    contract = schema_contract_from_registry_json(result.schema_registry_json)
    assert contract is not None
    return contract


def _consume(
    source: Path,
    output: Path,
    *,
    mode: str,
    contract: object,
    memory_limit: int,
):
    """Consume a strict contract through the native streaming surface."""
    options = normalize_call_options(
        schema_contract=contract,
        schema_mode="strict",
        on_error="stop",
        parse_integers=True,
        field_name_policy="preserve",
        multi_threading=mode == "multi",
        memory_limit_bytes=memory_limit,
    )
    context = ExecutionContext()
    sink = context.to_sink(source, sink="stream", options=options, format="jsonl", source="path")
    result = write_raw_stream_to_file(
        sink.raw,
        output,
        writer=write_jsonl_native_first_stream,
        feature="v40 validated JSON token handoff",
        first_row_columns=None,
        memory_limit_bytes=memory_limit,
        threading_mode=mode,
    )
    return result, context


def test_v40_sources_define_bounded_immutable_token_handoff() -> None:
    """The implementation indexes root fields without sharing parser state."""
    root = Path(__file__).resolve().parents[2]
    row_stream = (root / "cpp/src/sanitize/core/row_stream.hh").read_text()
    tokens = (root / "cpp/src/internal/parsing/json/validated_row.hh").read_text()
    pipeline = (root / "cpp/src/frontends/json/text_row_pipeline.cc").read_text()
    storage = (root / "cpp/src/frontends/json/text_batch_storage.hh").read_text()
    direct = (root / "cpp/src/internal/materialization/direct_rows.cc").read_text()
    scanner = (root / "cpp/src/internal/parsing/json/ondemand/scan.hh").read_text()
    diagnostics = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_diagnostics.cc"
    ).read_text()
    dispatch = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_source_dispatch.cc"
    ).read_text()
    materializer = (
        root / "cpp/src/internal/materialization/row_appender_json_tokens.cc"
    ).read_text()

    assert "kJsonValidatedTokens" in row_stream
    assert "sizeof(JsonValidatedFieldToken) == 8" in tokens
    assert "key_offset" in tokens and "value_offset" in tokens
    assert "scan_object_tokens" in pipeline
    assert "tokens->resize(token_begin)" in pipeline
    assert "budget.total_bytes, 8" in pipeline
    assert "max_validated_tokens" in storage
    assert "finalize_validated_rows" in storage
    assert "json_validated_row_tokens" in direct
    assert "value_text" in materializer
    assert "doc->ParseValue" in materializer
    assert "ForEachObjectFieldC" not in materializer
    assert "saw_escape" in scanner
    assert "projected_capacity_bytes" in diagnostics
    assert "executor_->Cancel()" in dispatch


def test_wide_jsonl_reuses_validated_tokens_with_exact_output(
    tmp_path: Path,
) -> None:
    """Workers consume the immutable token index instead of the root parser."""
    require_native()
    memory_limit = 64 * 1024 * 1024
    source = tmp_path / "wide.jsonl"
    _write_rows(source, 2_048)
    contract = _contract(source, tmp_path / "contract.jsonl", memory_limit)

    single_output = tmp_path / "single.jsonl"
    single_result, _ = _consume(
        source,
        single_output,
        mode="single",
        contract=contract,
        memory_limit=memory_limit,
    )
    multi_output = tmp_path / "multi.jsonl"
    multi_result, context = _consume(
        source,
        multi_output,
        mode="multi",
        contract=contract,
        memory_limit=memory_limit,
    )
    counters = context.performance_stats()["counters"]

    assert semantic_stats(multi_result.stats) == semantic_stats(single_result.stats)
    assert multi_result.schema_registry_json == single_result.schema_registry_json
    assert_logical_files_equivalent(single_output, multi_output)
    assert counters["jsonl_token_rows_indexed"] == 2_048
    assert counters["jsonl_token_fields_indexed"] == 2_048 * len(_COLUMNS)
    assert counters["jsonl_token_rows_fallback"] == 0


def test_token_budget_falls_back_per_row_without_changing_results(
    tmp_path: Path,
) -> None:
    """A full token budget degrades to raw rows, never a partial index."""
    require_native()
    # MSVC's allocator/STL overhead is part of the strict global budget.
    oracle_memory_limit = (256 if os.name == "nt" else 128) * 1024 * 1024
    constrained_memory_limit = (63 if os.name == "nt" else 32) * 1024 * 1024
    source = tmp_path / "budget.jsonl"
    _write_rows(source, 8_192)
    contract = _contract(source, tmp_path / "contract.jsonl", 256 * 1024 * 1024)

    single_output = tmp_path / "single.jsonl"
    single_result, _ = _consume(
        source,
        single_output,
        mode="single",
        contract=contract,
        memory_limit=oracle_memory_limit,
    )
    multi_output = tmp_path / "multi.jsonl"
    multi_result, context = _consume(
        source,
        multi_output,
        mode="multi",
        contract=contract,
        memory_limit=constrained_memory_limit,
    )
    stats = context.performance_stats()
    counters = stats["counters"]

    assert stats["effective_workers"] > 1
    single_semantics = semantic_stats(single_result.stats)
    multi_semantics = semantic_stats(multi_result.stats)
    for execution_detail in ("batches", "operation_memory_limit_bytes"):
        single_semantics.pop(execution_detail, None)
        multi_semantics.pop(execution_detail, None)
    assert multi_semantics == single_semantics
    assert multi_result.schema_registry_json == single_result.schema_registry_json
    assert_logical_files_equivalent(single_output, multi_output)
    assert 0 < counters["jsonl_token_rows_indexed"] < 8_192
    assert counters["jsonl_token_rows_fallback"] > 0
    assert counters["jsonl_token_rows_indexed"] + counters["jsonl_token_rows_fallback"] == 8_192
    assert counters["jsonl_token_fields_indexed"] == (
        counters["jsonl_token_rows_indexed"] * len(_COLUMNS)
    )


def test_escaped_and_duplicate_keys_keep_single_thread_semantics(
    tmp_path: Path,
) -> None:
    """Escaped names and duplicate fields retain canonical lookup behavior."""
    require_native()
    memory_limit = 64 * 1024 * 1024
    clean = tmp_path / "clean.jsonl"
    _write_rows(clean, 256)
    contract = _contract(clean, tmp_path / "contract.jsonl", memory_limit)

    source = tmp_path / "special.jsonl"
    lines: list[str] = []
    for row_index in range(256):
        fields = [
            f"{json.dumps(name)}:{row_index + column}" for column, name in enumerate(_COLUMNS)
        ]
        if row_index == 7:
            fields[0] = f'"token\\u0061a":{row_index}'
        if row_index == 11:
            fields.insert(1, f"{json.dumps(_COLUMNS[0])}:999999")
        lines.append("{" + ",".join(fields) + "}")
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")

    single_output = tmp_path / "single.jsonl"
    single_result, _ = _consume(
        source,
        single_output,
        mode="single",
        contract=contract,
        memory_limit=memory_limit,
    )
    multi_output = tmp_path / "multi.jsonl"
    multi_result, context = _consume(
        source,
        multi_output,
        mode="multi",
        contract=contract,
        memory_limit=memory_limit,
    )

    assert semantic_stats(multi_result.stats) == semantic_stats(single_result.stats)
    assert multi_result.schema_registry_json == single_result.schema_registry_json
    assert_logical_files_equivalent(single_output, multi_output)
    assert context.performance_stats()["counters"]["jsonl_token_rows_indexed"] == 256


@pytest.mark.parametrize(
    ("bad_row", "message_fragment"),
    [
        (r'{"broken":"\q"}', "invalid escape"),
        ('{"broken":1} trailing', "failed to coerce string to int64"),
    ],
)
def test_syntax_error_still_precedes_worker_conversion_failure(
    tmp_path: Path, bad_row: str, message_fragment: str
) -> None:
    """Frontend syntax errors still outrank earlier worker conversion errors."""
    require_native()
    memory_limit = 64 * 1024 * 1024
    clean = tmp_path / "clean.jsonl"
    _write_rows(clean, 256)
    contract = _contract(clean, tmp_path / "contract.jsonl", memory_limit)

    rows = []
    for row_index in range(256):
        row = {name: row_index + column for column, name in enumerate(_COLUMNS)}
        if row_index == 7:
            row[_COLUMNS[0]] = "not-an-integer"
        rows.append(json.dumps(row, separators=(",", ":")))
    rows[200] = bad_row
    source = tmp_path / "errors.jsonl"
    source.write_text("\n".join(rows) + "\n", encoding="utf-8")

    errors: dict[str, str] = {}
    for mode in ("single", "multi"):
        with pytest.raises(Exception) as exc_info:
            _consume(
                source,
                tmp_path / f"{mode}.jsonl",
                mode=mode,
                contract=contract,
                memory_limit=memory_limit,
            )
        errors[mode] = str(exc_info.value)

    assert errors["multi"] == errors["single"]
    assert message_fragment in errors["multi"]
