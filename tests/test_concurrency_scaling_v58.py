"""Regression coverage for v58 validation-certified JSON token order."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss
from benchmarks.concurrency_telemetry_support import consume_arrow_c_stream
from schema_sanitizer.api_impl import operation_context
from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options

_FIXED_TIME_NS = 1_700_000_000_123_456_000
_MEMORY_LIMIT = 128 * 1024 * 1024


@pytest.fixture(autouse=True)
def fixed_operation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep generated metadata identical across execution modes."""
    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_TIME_NS)


def _columns(count: int = 128) -> list[str]:
    """Return normalization-stable alphabetic root field names."""
    names: list[str] = []
    for index in range(count):
        value = index
        suffix = []
        for _ in range(3):
            suffix.append(chr(ord("a") + value % 26))
            value //= 26
        names.append("field" + "".join(reversed(suffix)))
    return names


def _write_rows(path: Path, rows: list[dict[str, int]]) -> None:
    """Write deterministic compact JSONL rows."""
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


def _consume_with_telemetry(source: Path) -> dict[str, object]:
    """Consume one native Arrow stream and return completed telemetry."""
    options = normalize_call_options(
        multi_threading=True,
        memory_limit_bytes=_MEMORY_LIMIT,
        on_error="stop",
        field_name_policy="preserve",
        parse_integers=True,
    )
    context = ExecutionContext()
    sink = context.to_sink(
        source,
        sink="stream",
        options=options,
        format="jsonl",
        source="path",
    )
    try:
        consume_arrow_c_stream(sink.raw)
    finally:
        sink.close()
    return context.performance_stats()


def test_v58_ordered_tokens_activate_and_preserve_output(tmp_path: Path) -> None:
    """Exact plan order bypasses key hashing without changing output bytes."""
    require_native()
    columns = _columns()
    rows = [{name: row + index for index, name in enumerate(columns)} for row in range(4_096)]
    source = tmp_path / "ordered.jsonl"
    _write_rows(source, rows)

    common = dict(
        input_format="jsonl",
        parse_integers=True,
        field_name_policy="preserve",
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    single_path = tmp_path / "single.jsonl"
    multi_path = tmp_path / "multi.jsonl"
    ss.to_jsonl(source, single_path, multi_threading=False, **common)
    ss.to_jsonl(source, multi_path, multi_threading=True, **common)

    report = _consume_with_telemetry(source)
    counters = report["counters"]
    assert multi_path.read_bytes() == single_path.read_bytes()
    assert counters["jsonl_plan_ordered_rows"] == len(rows)
    assert counters["jsonl_token_rows_indexed"] == len(rows)


def test_v58_reordered_missing_and_escaped_keys_use_fallback(tmp_path: Path) -> None:
    """Any positional mismatch returns to canonical lookup with exact parity."""
    require_native()
    columns = _columns()
    lines: list[str] = []
    ordered_rows = 1_024
    total_rows = 2_048
    for row in range(total_rows):
        ordered = columns if row < ordered_rows else columns[1:] + columns[:1]
        record = {name: row + index for index, name in enumerate(ordered)}
        if row >= ordered_rows and row % 29 == 0:
            record.pop(ordered[-1])
        line = json.dumps(record, separators=(",", ":"))
        if row == ordered_rows:
            line = line.replace('"fieldaab"', '"\\u0066ieldaab"', 1)
        lines.append(line)
    source = tmp_path / "fallback.jsonl"
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")

    common = dict(
        input_format="jsonl",
        parse_integers=True,
        field_name_policy="preserve",
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    single_path = tmp_path / "fallback-single.jsonl"
    multi_path = tmp_path / "fallback-multi.jsonl"
    ss.to_jsonl(source, single_path, multi_threading=False, **common)
    ss.to_jsonl(source, multi_path, multi_threading=True, **common)

    report = _consume_with_telemetry(source)
    ordered = report["counters"]["jsonl_plan_ordered_rows"]
    assert multi_path.read_bytes() == single_path.read_bytes()
    assert 0 < ordered < total_rows


def test_v58_plan_order_is_certified_once_during_validation() -> None:
    """The optimized path cannot re-scan or hash JSON object keys."""
    root = Path(__file__).resolve().parents[1]
    validator = (root / "cpp/src/frontends/json/text_row_pipeline.cc").read_text(encoding="utf-8")
    materializer = (
        root / "cpp/src/internal/materialization/row_appender_json_tokens.cc"
    ).read_text(encoding="utf-8")
    flags = (root / "cpp/src/sanitize/core/row_stream.hh").read_text(encoding="utf-8")

    assert "kJsonPlanOrderedTokens = 8" in flags
    assert "plan->columns[fields].name" in validator
    fast_path = materializer.split("try_append_plan_ordered_json_tokens", 1)[1].split(
        "materialize_validated_json_fields", 1
    )[0]
    assert "validated_key_token" not in fast_path
    assert "hash_key64" not in fast_path
    assert "validated_value_token" in fast_path
