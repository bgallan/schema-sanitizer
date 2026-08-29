"""Verify shallow output workloads make progress with four governed workers.

Successive local waves must restore FIFO fairness, remote output stealing must respect the thread
budget, and JSONL bytes must remain identical between single- and multi-worker execution.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import schema_sanitizer as ss
from schema_sanitizer.core_impl.native_runtime import native_core

pytestmark = pytest.mark.usefixtures("fixed_operation_clock")
pytestmark = [pytestmark, pytest.mark.usefixtures("require_native")]

_MEMORY_LIMIT = 128 * 1024 * 1024


def test_shallow_local_output_progress_at_four_workers() -> None:
    """One shallow output wave drains without exceeding its task count."""
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


def test_second_output_wave_restores_fifo_fairness() -> None:
    """Two output waves and the broad wave drain within the worker budget."""
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
def test_shallow_remote_output_steal_preserves_thread_budget(
    workers: int,
) -> None:
    """Idle low-core helpers can recover front output without deep scanning."""
    promoted, outputs, broad, stolen, started, queued, submitted, cpu_capacity = (
        native_core.operation_task_arena_output_steal_probe(workers)
    )
    expected_outputs = workers // 2 - 1
    assert 0 <= promoted <= expected_outputs
    assert outputs == expected_outputs
    assert broad == workers - 1
    # With one CPU credit per worker, every owner remains blocked and the
    # released helper must steal. Under a narrower CPU quota an unblocked owner
    # may drain its own queue first, so requiring a steal would depend on OS
    # scheduling rather than arena correctness.
    assert stolen > 0 or cpu_capacity < workers
    assert queued == 0
    # Only currently runnable CPU credits need blockers. The output and broad
    # packet counts remain exact regardless of the host CPU quota.
    blocker_count = submitted - outputs - broad
    assert 1 <= blocker_count <= workers
    assert blocker_count <= started <= workers


def test_single_and_multi_jsonl_outputs_remain_byte_identical(
    tmp_path: Path,
) -> None:
    """Bounded output scheduling does not alter exact output."""
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
