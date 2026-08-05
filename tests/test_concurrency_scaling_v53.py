"""Regression coverage for v53 constant-cost wide JSONL planning."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native
from threading_golden import assert_logical_files_equivalent, semantic_stats

import schema_sanitizer as ss
from schema_sanitizer.api_impl import operation_context

_MEMORY_LIMIT = 64 * 1024 * 1024
_FIXED_TIME_NS = 1_700_000_000_123_456_000
_COLUMNS = tuple(f"wide_{index:03d}" for index in range(64))


@pytest.fixture(autouse=True)
def fixed_operation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep generated metadata byte-identical across execution modes."""
    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_TIME_NS)


def _write_rows(path: Path, rows: int, *, nullable: bool) -> None:
    """Write deterministic wide scalar JSONL, optionally including nulls."""
    with path.open("w", encoding="utf-8", newline="") as output:
        for row_index in range(rows):
            values = {
                name: (
                    None
                    if nullable and (row_index + column_index) % 97 == 0
                    else row_index + column_index
                )
                for column_index, name in enumerate(_COLUMNS)
            }
            output.write(json.dumps(values, separators=(",", ":")))
            output.write("\n")


def _convert(source: Path, output: Path, mode: str):
    """Convert one source with the selected deterministic threading mode."""
    return ss.to_jsonl(
        source,
        output,
        input_format="jsonl",
        parse_integers=True,
        field_name_policy="preserve",
        multi_threading=mode == "multi",
        memory_limit_bytes=_MEMORY_LIMIT,
    )


def test_v53_prepares_the_row_estimator_once_per_batch() -> None:
    """Wide fixed planning must not rely on reusable ArrowArray addresses."""
    root = Path(__file__).resolve().parents[1]
    writer = (root / "cpp/src/internal/json_output/jsonl_stream_writer.cc").read_text(
        encoding="utf-8"
    )
    ordered = (root / "cpp/src/internal/output/ordered_text_output.hh").read_text(encoding="utf-8")
    estimator = (root / "cpp/src/internal/output/text_output_estimator.hh").read_text(
        encoding="utf-8"
    )

    assert "class JsonlRowEstimator final" in writer
    assert "void prepare(const ArrowArray &array) noexcept" in writer
    assert "JsonlRowEstimator(root)" in writer
    assert "cached_array" not in writer
    assert "prepare_row_estimator_for_batch" in ordered
    assert "estimate_row.prepare(array)" in ordered
    assert ordered.index("prepare_row_estimator_for_batch") < ordered.index(
        "ensure_executor_for(batch->value())", ordered.index("while (true)")
    )
    assert "estimate_wide_fixed_jsonl_row_upper_bound" in estimator
    assert "wide_fixed_jsonl_batch_has_no_nulls" in estimator
    assert "estimate_jsonl_row_bytes(*root_, array, row" in writer


def test_v53_fixed_wide_planning_preserves_the_single_oracle(
    tmp_path: Path,
) -> None:
    """The O(1) estimate preserves packetization and exact output bytes."""
    require_native()
    source = tmp_path / "fixed.jsonl"
    _write_rows(source, 6_000, nullable=False)

    single_output = tmp_path / "single-fixed.jsonl"
    single = _convert(source, single_output, "single")
    multi_output = tmp_path / "multi-fixed.jsonl"
    multi = _convert(source, multi_output, "multi")

    assert semantic_stats(multi.stats) == semantic_stats(single.stats)
    assert multi.schema_registry_json == single.schema_registry_json
    assert multi_output.read_bytes() == single_output.read_bytes()
    assert_logical_files_equivalent(single_output, multi_output)


def test_v53_nullable_wide_batches_keep_the_row_aware_fallback(
    tmp_path: Path,
) -> None:
    """Null-bearing batches must retain exact row-aware size estimation."""
    require_native()
    source = tmp_path / "nullable.jsonl"
    _write_rows(source, 3_000, nullable=True)

    single_output = tmp_path / "single-nullable.jsonl"
    single = _convert(source, single_output, "single")
    multi_output = tmp_path / "multi-nullable.jsonl"
    multi = _convert(source, multi_output, "multi")

    assert semantic_stats(multi.stats) == semantic_stats(single.stats)
    assert multi.schema_registry_json == single.schema_registry_json
    assert multi_output.read_bytes() == single_output.read_bytes()
    assert b"null" in multi_output.read_bytes()
    assert_logical_files_equivalent(single_output, multi_output)
