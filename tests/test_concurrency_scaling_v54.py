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
    """One output wave bypasses one broad packet on shallow high queues."""
    require_native()
    promoted, outputs, broad, started, queued, elapsed_us = (
        native_core.operation_task_arena_output_preference_probe(4)
    )
    assert promoted == 2
    assert outputs == 2
    assert broad == 4
    assert started == 4
    assert queued == 0
    assert elapsed_us < 5_000


def test_v54_second_output_wave_restores_fifo_fairness() -> None:
    """Only the first wave bypasses; broad work runs before wave two."""
    require_native()
    for workers in (4, 5, 8, 16):
        promoted, outputs, broad, started, queued, elapsed_us = (
            native_core.operation_task_arena_output_preference_probe(workers, 2)
        )
        if workers == 4:
            assert promoted == 2
        elif workers == 5:
            # Fairness is per physical worker. With an odd upper lane, the
            # second wave may land on a worker that did not consume wave one.
            assert 2 <= promoted <= 4
        elif workers == 8:
            assert promoted == 0
        else:
            assert promoted == 8
        assert outputs == (workers // 2) * 2
        assert broad == workers
        assert started == workers
        assert queued == 0
        assert 1_000 < elapsed_us < 5_000


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


def test_v54_scheduler_is_queue_bounded_and_keeps_v49_telemetry_gate() -> None:
    """Low-core preference adds no scan to queues lacking dedicated output."""
    root = Path(__file__).resolve().parents[1]
    arena = (root / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    runtime = (root / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text()
    assert "dedicated_output_queued" in arena
    assert "bounded_low_core_output(slot.tasks.back(), state_)" in arena
    assert "bounded_low_core_output" in runtime
    assert "output_preference_queue_is_bounded" in runtime
    assert "queue_size <= 4U" in runtime
    assert "state->worker_count > 5U" in runtime
    assert "slot.shallow_output_preference" in runtime
    assert "TaskTelemetryBatch telemetry_batch(" in runtime
    assert "if constexpr (PreferDedicatedOutput)" in runtime
    assert "telemetry_batch.Flush()" in runtime


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
    single_result = ss.to_jsonl(source, single, threading_mode="single", **common)
    multi_result = ss.to_jsonl(source, multi, threading_mode="multi", **common)

    assert single.read_bytes() == multi.read_bytes()
    assert single_result.stats["materialized_rows"] == len(rows)
    assert multi_result.stats["materialized_rows"] == len(rows)
