"""Regression coverage for v57 single-pass flat JSONL inference parsing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.api_impl import operation_context
from schema_sanitizer.core_impl.execution import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options

_FIXED_TIME_NS = 1_700_000_000_123_456_000
_MEMORY_LIMIT = 128 * 1024 * 1024


@pytest.fixture(autouse=True)
def fixed_operation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep generated metadata identical across execution modes."""
    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_TIME_NS)


def _probe(payload: str, mode: str):
    """Run one raw JSONL schema probe with explicit options."""
    options = normalize_call_options(
        threading_mode=mode,
        memory_limit_bytes=_MEMORY_LIMIT,
        field_name_policy="preserve",
        parse_integers=True,
        parse_floats=True,
    ).raw
    return ExecutionContext().schema_probe_from_source("jsonl", "text", payload, options)


def test_v57_numeric_boundaries_match_single() -> None:
    """Lexical int64 classification preserves every numeric boundary."""
    tokens = [
        "0",
        "-0",
        "1",
        "-1",
        "9223372036854775807",
        "-9223372036854775808",
        "9223372036854775808",
        "-9223372036854775809",
        "18446744073709551615",
        "1.0",
        "-0.25",
        "6.022e23",
        "1e-300",
        "99999999999999999999999999999999999999",
    ]
    fields = [f'"field_{index:03d}":{tokens[index % len(tokens)]}' for index in range(256)]
    payload = "{" + ",".join(fields) + "}\n"

    single = _probe(payload, "single")
    multi = _probe(payload, "multi")

    assert multi.schema_payload == single.schema_payload
    assert multi.field_names == single.field_names
    assert multi.diagnostics.to_json() == single.diagnostics.to_json()
    assert len(multi.field_names) == 256


def test_v57_escaped_strings_literals_and_empty_containers_match_single() -> None:
    """Single-pass literals retain decoding and empty-container semantics."""
    rows = [
        '{"empty_object":{},"empty_array":[],"id":1,'
        '"text":"123","escaped":"line\\nvalue","flag":true,"none":null}',
        '{"id":2,"text":"2.5","escaped":"\\u20ac","flag":false,"none":null}',
    ]
    payload = "\n".join(rows) + "\n"

    single = _probe(payload, "single")
    multi = _probe(payload, "multi")

    assert multi.schema_payload == single.schema_payload
    assert multi.field_names == single.field_names
    assert multi.diagnostics.to_json() == single.diagnostics.to_json()
    assert "empty_object" not in multi.field_names
    assert "empty_array" not in multi.field_names


def test_v57_nested_fallback_and_invalid_float_preserve_contract() -> None:
    """Nested rows fall back and invalid floating tokens still fail."""
    nested = (
        '{"id":1,"object":{"value":2},"array":[1,2]}\n{"id":2,"object":{"value":3},"array":[3]}\n'
    )
    single = _probe(nested, "single")
    multi = _probe(nested, "multi")
    assert multi.schema_payload == single.schema_payload
    assert multi.field_names == single.field_names
    assert multi.diagnostics.to_json() == single.diagnostics.to_json()

    with pytest.raises(RuntimeError, match="invalid float at byte"):
        _probe('{"value":1e9999}\n', "multi")


def test_v57_wide_output_is_byte_identical(tmp_path: Path) -> None:
    """The specialized inference visitor cannot change materialized bytes."""
    require_native()
    source = tmp_path / "wide.jsonl"
    rows = [{f"field_{column:03d}": row + column for column in range(128)} for row in range(4_096)]
    source.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )
    single_path = tmp_path / "single.jsonl"
    multi_path = tmp_path / "multi.jsonl"
    common = dict(
        input_format="jsonl",
        parse_integers=True,
        field_name_policy="preserve",
        memory_limit_bytes=_MEMORY_LIMIT,
    )

    single = ss.to_jsonl(source, single_path, threading_mode="single", **common)
    multi = ss.to_jsonl(source, multi_path, threading_mode="multi", **common)

    assert multi_path.read_bytes() == single_path.read_bytes()
    assert single.stats["materialized_rows"] == len(rows)
    assert multi.stats["materialized_rows"] == len(rows)


def test_v57_flat_parser_avoids_hash_and_duplicate_primitive_scan() -> None:
    """The production JSONL fast path must retain its single-pass contract."""
    root = Path(__file__).resolve().parents[1]
    parser = (root / "cpp/src/internal/parsing/json/ondemand/flat_object_iteration.cc").read_text()
    generic = (root / "cpp/src/internal/parsing/json/ondemand/object_iteration.cc").read_text()
    inference = (root / "cpp/src/internal/inference/parallel_evidence.cc").read_text()

    assert "ForEachFlatObjectFieldC" in parser
    assert "integer_token_fits_int64" in parser
    assert "std::from_chars" not in parser
    assert "hash_key64" not in parser
    assert "hash_key64" in generic
    assert "append_flat_json_inference_row" in inference
