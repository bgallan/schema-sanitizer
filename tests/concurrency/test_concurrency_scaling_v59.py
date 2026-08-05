"""Regression coverage for v59 lexical scalar token materialization."""

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
    """Keep generated registry metadata identical across execution modes."""
    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_TIME_NS)


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    """Write compact deterministic JSONL rows in insertion order."""
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _telemetry(source: Path, **extra: object) -> dict[str, object]:
    """Consume the native stream and return completed operation telemetry."""
    options = normalize_call_options(
        multi_threading=True,
        memory_limit_bytes=_MEMORY_LIMIT,
        on_error="stop",
        field_name_policy="preserve",
        **extra,
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


def test_v59_lexical_scalars_preserve_single_multi_output(tmp_path: Path) -> None:
    """Direct bool/int/float/null/string tokens remain byte-identical."""
    require_native()
    rows: list[dict[str, object]] = []
    float_values = (1.25, -0.0, 1e-200, 1.7976931348623157e308, 42.0)
    for index in range(8_192):
        row: dict[str, object] = {
            "boolv": index % 2 == 0,
            "intv": (-(2**63) if index == 1 else 2**63 - 1 if index == 2 else index - 4_096),
            "floatv": float_values[index % len(float_values)],
            "textv": (f'escaped\\line\n"{index}' if index % 97 == 0 else f"plain-{index}"),
        }
        for filler in range(124):
            row[f"filler{filler:02d}"] = index + filler
        rows.append({key: row[key] for key in sorted(row)})
    source = tmp_path / "scalars.jsonl"
    _write_rows(source, rows)

    common = dict(
        input_format="jsonl",
        field_name_policy="preserve",
        memory_limit_bytes=_MEMORY_LIMIT,
        parse_integers=True,
    )
    single = tmp_path / "single.jsonl"
    multi = tmp_path / "multi.jsonl"
    ss.to_jsonl(source, single, multi_threading=False, **common)
    ss.to_jsonl(source, multi, multi_threading=True, **common)

    report = _telemetry(source, parse_integers=True)
    assert multi.read_bytes() == single.read_bytes()
    assert report["counters"]["jsonl_plan_ordered_rows"] == len(rows)


def test_v59_string_coercions_and_escapes_use_canonical_fallback(
    tmp_path: Path,
) -> None:
    """Quoted coercions and escaped UTF-8 retain the existing parser path."""
    require_native()
    rows: list[dict[str, object]] = []
    for index in range(2_048):
        row: dict[str, object] = {
            "integer_text": str(index - 1_024),
            "float_text": f"{index / 7:.6f}",
            "date_text": f"2024-01-{index % 28 + 1:02d}",
            "textv": f"value-{index}" if index % 13 else f'line\\{index}\n"',
        }
        for filler in range(124):
            row[f"filler{filler:03d}"] = index + filler
        rows.append({key: row[key] for key in sorted(row)})
    source = tmp_path / "coercions.jsonl"
    _write_rows(source, rows)

    common = dict(
        input_format="jsonl",
        field_name_policy="preserve",
        memory_limit_bytes=_MEMORY_LIMIT,
        parse_integers=True,
        parse_floats=True,
        parse_iso_dates=True,
    )
    single = tmp_path / "coercions-single.jsonl"
    multi = tmp_path / "coercions-multi.jsonl"
    ss.to_jsonl(source, single, multi_threading=False, **common)
    ss.to_jsonl(source, multi, multi_threading=True, **common)

    report = _telemetry(
        source,
        parse_integers=True,
        parse_floats=True,
        parse_iso_dates=True,
    )
    assert multi.read_bytes() == single.read_bytes()
    assert report["counters"]["jsonl_plan_ordered_rows"] == len(rows)


def test_v59_direct_token_path_precedes_generic_value_parsing() -> None:
    """The ordered fast path must attempt lexical conversion before ParseValue."""
    root = Path(__file__).resolve().parents[2]
    owner = (root / "cpp/src/internal/materialization/row_appender_json_tokens.cc").read_text(
        encoding="utf-8"
    )
    direct = owner.split("try_append_plan_ordered_json_tokens", 1)[1].split(
        "materialize_validated_json_fields", 1
    )[0]

    assert "try_convert_plan_ordered_scalar_token" in direct
    assert direct.index("try_convert_plan_ordered_scalar_token") < direct.index("doc->ParseValue")
    helper = owner.split("try_convert_plan_ordered_scalar_token", 1)[1].split(
        "try_append_plan_ordered_json_tokens", 1
    )[0]
    assert "std::from_chars" not in helper  # Parsing stays in the shared helper.
    assert "parse_float64_token" in helper
    assert "token.find('\\\\')" in helper
