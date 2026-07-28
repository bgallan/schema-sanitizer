"""Regression coverage for v55 wide flat JSONL inference evidence."""

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


def _schema_probe(payload: str, threading_mode: str, memory_limit_bytes: int = _MEMORY_LIMIT):
    """Run the raw JSONL inference path with one explicit execution mode."""
    options = normalize_call_options(
        threading_mode=threading_mode,
        memory_limit_bytes=memory_limit_bytes,
        field_name_policy="preserve",
        parse_integers=True,
    ).raw
    return ExecutionContext().schema_probe_from_source("jsonl", "text", payload, options)


def test_v55_wide_reordered_flat_inference_matches_single() -> None:
    """Tracked overflow and ordered matching preserve wide schema semantics."""
    require_native()
    columns = [f"field_{index:03d}" for index in range(128)]
    lines: list[str] = []
    for row in range(2_048):
        rotation = row % len(columns)
        ordered = columns[rotation:] + columns[:rotation]
        record = {
            key: (None if (row + index) % 97 == 0 else row + index)
            for index, key in enumerate(ordered)
            if not (row % 211 == 0 and index % 31 == 0)
        }
        lines.append(json.dumps(record, separators=(",", ":")))
    payload = "\n".join(lines) + "\n"

    single = _schema_probe(payload, "single")
    multi = _schema_probe(payload, "multi")

    assert multi.schema_payload == single.schema_payload
    assert multi.field_names == single.field_names
    assert multi.diagnostics.to_json() == single.diagnostics.to_json()
    assert len(multi.field_names) == len(columns)


def test_v55_field_ceiling_falls_back_to_generic_without_semantic_drift() -> None:
    """More than the bounded flat capacity retains the generic reference path."""
    require_native()
    columns = [f"f{index:03d}" for index in range(520)]
    payload = "\n".join(
        json.dumps({key: row + index for index, key in enumerate(columns)}, separators=(",", ":"))
        for row in range(8)
    )

    single = _schema_probe(payload, "single")
    multi = _schema_probe(payload, "multi")

    assert multi.schema_payload == single.schema_payload
    assert multi.field_names == single.field_names
    assert multi.diagnostics.to_json() == single.diagnostics.to_json()
    assert len(multi.field_names) == len(columns)


def test_v55_wide_low_memory_output_is_byte_identical(tmp_path: Path) -> None:
    """Overflow evidence stays inside the operation budget and preserves output."""
    require_native()
    source = tmp_path / "wide.jsonl"
    rows = [{f"field_{column:03d}": row + column for column in range(128)} for row in range(4_096)]
    source.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )
    single = tmp_path / "single.jsonl"
    multi = tmp_path / "multi.jsonl"
    common = dict(
        input_format="jsonl",
        parse_integers=True,
        field_name_policy="preserve",
        memory_limit_bytes=64 * 1024 * 1024,
    )

    single_result = ss.to_jsonl(source, single, threading_mode="single", **common)
    multi_result = ss.to_jsonl(source, multi, threading_mode="multi", **common)

    assert multi.read_bytes() == single.read_bytes()
    assert single_result.stats["materialized_rows"] == len(rows)
    assert multi_result.stats["materialized_rows"] == len(rows)


def test_v55_flat_overflow_is_bounded_and_memory_accounted() -> None:
    """Wide flat storage must use a tracked packet-local overflow, not a global cache."""
    root = Path(__file__).resolve().parents[1]
    header = (root / "cpp/src/internal/inference/parallel_evidence.hh").read_text()
    source = (root / "cpp/src/internal/inference/parallel_flat_evidence.cc").read_text()

    assert "kInlineFlatInferenceFields = 16" in header
    assert "kMaxFlatInferenceFields = 512" in header
    assert "std::pmr::vector<FlatInferenceField> fields" in header
    assert "schema_sanitizer::FlatInferenceEvidencePacket" in header
    assert "thread_safe_registry=*/false" in header
    assert "expected < storage.field_count" in source
    assert "storage.field(expected)" in source
