"""Regression coverage for v54 bounded low-core output progress."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.api_impl import operation_context
from schema_sanitizer.core_impl.native_runtime import native_core

_MEMORY_LIMIT = 128 * 1024 * 1024
_FIXED_TIME_NS = 1_700_000_000_123_456_000


@pytest.fixture(autouse=True)
def fixed_operation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep generated registry metadata identical across execution modes."""
    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_TIME_NS)


def test_v54_shallow_local_output_progress_at_four_workers() -> None:
    """One shallow output wave drains without exceeding its task count."""
    require_native()
    promoted, outputs, broad, started, queued, _elapsed_us = (
        native_core.operation_task_arena_output_preference_probe(4)
    )
    # Cross-lane steals may drain broad work before the owning high worker gets
    # CPU time, so the observed promotion count is scheduling dependent.
    assert 0 <= promoted <= outputs
    assert outputs == 2
    assert broad == 4
    assert started == 4
    assert queued == 0


def test_v54_second_output_wave_restores_fifo_fairness() -> None:
    """Two output waves and the broad wave drain within the worker budget."""
    require_native()
    for workers in (4, 5, 8, 16):
        promoted, outputs, broad, started, queued, _elapsed_us = (
            native_core.operation_task_arena_output_preference_probe(workers, 2)
        )
        # Promotion is a local observation: a compatible steal may finish the
        # broad packet before its owning high lane is scheduled.
        assert 0 <= promoted <= outputs
        assert outputs == (workers // 2) * 2
        assert broad == workers
        assert 1 <= started <= workers
        assert queued == 0


@pytest.mark.parametrize("workers", [4, 5])
def test_v54_shallow_remote_output_steal_preserves_thread_budget(
    workers: int,
) -> None:
    """Idle low-core helpers can recover front output without deep scanning."""
    require_native()
    promoted, outputs, broad, stolen, started, queued, submitted = (
        native_core.operation_task_arena_output_steal_probe(workers)
    )
    expected_outputs = workers // 2 - 1
    assert promoted == expected_outputs
    assert outputs == expected_outputs
    assert broad == workers - 1
    assert stolen > 0
    assert started == workers
    assert queued == 0
    assert submitted == workers + expected_outputs + workers - 1


def test_v54_single_and_multi_jsonl_outputs_remain_byte_identical(
    tmp_path: Path,
) -> None:
    """Bounded output scheduling does not alter exact output."""
    require_native()
    source = tmp_path / "source.jsonl"
    rows = [{f"field_{column}": row + column for column in range(8)} for row in range(12_000)]
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
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    single_result = ss.to_jsonl(source, single, multi_threading=False, **common)
    multi_result = ss.to_jsonl(source, multi, multi_threading=True, **common)

    assert single.read_bytes() == multi.read_bytes()
    assert single_result.stats["materialized_rows"] == len(rows)
    assert multi_result.stats["materialized_rows"] == len(rows)
