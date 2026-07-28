"""Regression coverage for v36 high-width worker-state reductions."""

from __future__ import annotations

import gc
import json
from pathlib import Path

import pytest
from conftest import require_native
from threading_golden import assert_logical_files_equivalent

import schema_sanitizer as ss

_MEMORY_LIMIT = 64 * 1024 * 1024
_FIXED_TIME_NS = 1_700_000_000_123_456_000
_COLUMNS = tuple(
    f"field{chr(ord('a') + index // 26)}{chr(ord('a') + index % 26)}" for index in range(128)
)


@pytest.fixture(autouse=True)
def fixed_operation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep generated registry metadata identical across execution modes."""
    from schema_sanitizer.api_impl import operation_context

    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_TIME_NS)


def _write_wide_jsonl(path: Path, rows: int) -> None:
    """Write deterministic scalar rows eligible for plan-ordered emission."""
    with path.open("w", encoding="utf-8") as handle:
        for row_index in range(rows):
            row = {
                name: str(row_index + column)
                for column, name in enumerate(_COLUMNS)
                if not (row_index % 29 == 0 and column == 73)
            }
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")


def test_lazy_worker_state_is_encoded_in_sources() -> None:
    """General and column builders remain first-use initialized."""
    root = Path(__file__).resolve().parents[1]
    implementation = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_preparer.cc"
    ).read_text(encoding="utf-8")
    columns = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_preparer_columns.cc"
    ).read_text(encoding="utf-8")
    state = (
        root / "cpp/src/internal/materialization/ingest_stream/parallel_preparer_internal.hh"
    ).read_text(encoding="utf-8")

    make_body = implementation.split("auto parent =", 1)[1].split(
        "ParallelRowPreparer::~ParallelRowPreparer", 1
    )[0]
    assert "make_direct_materializer" not in make_body
    assert "make_batch_appender" not in make_body
    assert "BatchAppenderPtr appender" in state
    assert "if (!state.appender)" in columns
    assert "make_batch_appender(*column_plans_[group_index]" in columns
    assert "std::optional<std::pmr::vector<FieldRef>> projected_fields" in state
    assert "if (!input->plan_ordered)" in columns
    assert "state.projected_fields->clear()" in columns
    assert implementation.count("if (!state.direct)") >= 3


def test_plan_ordered_fields_are_emitted_in_place() -> None:
    """The JSON frontend no longer stages and recopies all wide-row fields."""
    root = Path(__file__).resolve().parents[1]
    scratch = (root / "cpp/src/frontends/json/text_row_materializer.hh").read_text(encoding="utf-8")
    implementation = (root / "cpp/src/frontends/json/text_row_materializer.cc").read_text(
        encoding="utf-8"
    )
    batch = (root / "cpp/src/internal/parsing/flat_row_batch.hh").read_text(encoding="utf-8")

    assert "planned_fields" not in scratch
    assert "extra_fields" not in scratch
    assert "set_current_row_field" in batch
    assert "batch->set_current_row_field" in implementation
    assert "for (const auto &column : plan.columns)" in implementation
    assert "scratch->planned_fields" not in implementation
    assert "scratch->extra_fields" not in implementation


def test_wide_rows_repeat_under_low_budget_without_output_drift(tmp_path: Path) -> None:
    """Lazy state and in-place fields preserve ownership and exact output."""
    require_native()
    source = tmp_path / "wide.jsonl"
    _write_wide_jsonl(source, 3_000)

    single = tmp_path / "single.jsonl"
    single_result = ss.to_jsonl(
        source,
        single,
        input_format="jsonl",
        parse_integers=True,
        on_error="stop",
        multi_threading=False,
        memory_limit_bytes=_MEMORY_LIMIT,
    )

    for repetition in range(2):
        multi = tmp_path / f"multi-{repetition}.jsonl"
        multi_result = ss.to_jsonl(
            source,
            multi,
            input_format="jsonl",
            parse_integers=True,
            on_error="stop",
            multi_threading=True,
            memory_limit_bytes=_MEMORY_LIMIT,
        )
        gc.collect()
        assert multi_result.stats == single_result.stats
        assert multi_result.schema_registry_json == single_result.schema_registry_json
        assert_logical_files_equivalent(single, multi)
