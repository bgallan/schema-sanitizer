"""Compare wide mixed-scalar inference with the single-worker schema oracle.

Nested and empty containers must take the same fallback, serialized output must be byte-identical,
and scalar dispatch must stay a single tag switch rather than duplicating type logic.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from _support.diagnostics import assert_diagnostics_semantically_equal

import schema_sanitizer as ss
from schema_sanitizer.core_impl.execution import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options

pytestmark = pytest.mark.usefixtures("fixed_operation_clock")

_MEMORY_LIMIT = 128 * 1024 * 1024


def _schema_probe(payload: str, threading_mode: str):
    """Run raw JSONL inference with one explicit execution mode."""
    options = normalize_call_options(
        multi_threading=threading_mode == "multi",
        memory_limit_bytes=_MEMORY_LIMIT,
        field_name_policy="preserve",
        parse_integers=True,
        parse_floats=True,
    ).raw
    return ExecutionContext().schema_probe_from_source("jsonl", "text", payload, options)


def _mixed_scalar_row(row: int, columns: int = 128) -> dict[str, object]:
    """Build one wide row exercising every scalar category."""
    values: dict[str, object] = {}
    for column in range(columns):
        key = f"field_{column:03d}"
        category = column % 6
        if category == 0:
            value: object = row + column
        elif category == 1:
            value = row + column + 0.25
        elif category == 2:
            value = (row + column) % 2 == 0
        elif category == 3:
            value = f"text-{row}-{column}"
        elif category == 4:
            value = str(row + column)
        else:
            value = None if row % 17 == 0 else row - column
        values[key] = value
    return values


def test_wide_mixed_scalar_inference_matches_single(require_native: None) -> None:
    """One tag dispatch preserves wide scalar schema and diagnostics."""
    lines: list[str] = []
    for row in range(2_048):
        record = _mixed_scalar_row(row)
        keys = list(record)
        rotation = row % len(keys)
        ordered = keys[rotation:] + keys[:rotation]
        lines.append(json.dumps({key: record[key] for key in ordered}, separators=(",", ":")))
    payload = "\n".join(lines) + "\n"

    single = _schema_probe(payload, "single")
    multi = _schema_probe(payload, "multi")

    assert multi.schema_payload == single.schema_payload
    assert multi.field_names == single.field_names
    assert_diagnostics_semantically_equal(multi.diagnostics, single.diagnostics)
    assert len(multi.field_names) == 128


def test_nested_and_empty_container_fallback_matches_single(require_native: None) -> None:
    """Container tags retain empty semantics and generic nested fallback."""
    rows = []
    for row in range(512):
        rows.append(
            {
                "id": row,
                "empty_object": {},
                "empty_array": [],
                "object": {"value": row, "flag": row % 2 == 0},
                "array": [row, row + 1],
                "text": str(row),
            }
        )
    payload = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows)

    single = _schema_probe(payload, "single")
    multi = _schema_probe(payload, "multi")

    assert multi.schema_payload == single.schema_payload
    assert multi.field_names == single.field_names
    assert_diagnostics_semantically_equal(multi.diagnostics, single.diagnostics)


def test_wide_mixed_output_is_byte_identical(tmp_path: Path, require_native: None) -> None:
    """Scalar-category dispatch does not alter materialized JSONL bytes."""
    source = tmp_path / "mixed.jsonl"
    rows = [_mixed_scalar_row(row) for row in range(4_096)]
    source.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )
    single = tmp_path / "single.jsonl"
    multi = tmp_path / "multi.jsonl"
    common = dict(
        input_format="jsonl",
        parse_integers=True,
        parse_floats=True,
        field_name_policy="preserve",
        memory_limit_bytes=_MEMORY_LIMIT,
    )

    single_result = ss.to_jsonl(source, single, multi_threading=False, **common)
    multi_result = ss.to_jsonl(source, multi, multi_threading=True, **common)

    assert multi.read_bytes() == single.read_bytes()
    assert single_result.stats["materialized_rows"] == len(rows)
    assert multi_result.stats["materialized_rows"] == len(rows)


def test_scalar_dispatch_is_single_tag_switch() -> None:
    """The hot path must not regress to repeated category predicates."""
    root = Path(__file__).resolve().parents[2]
    value_view = (root / "cpp/src/sanitize/core/value_view.hh").read_text()
    parallel = (root / "cpp/src/internal/inference/parallel_flat_evidence.cc").read_text()
    observation = (root / "cpp/src/internal/inference/value_observation.cc").read_text()

    assert "[[nodiscard]] Tag tag() const noexcept" in value_view
    assert "switch (value.tag())" in parallel
    assert "infer_scalar_mask_from_string(value.as_string_view(), opts)" in parallel
    assert "case ValueView::Tag::kObject:" in parallel
    assert "value.container_is_empty(&empty)" in parallel
    assert "switch (value.tag())" in observation
