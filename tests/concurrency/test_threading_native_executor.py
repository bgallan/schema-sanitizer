"""Test native ordinal execution in strict inline and bounded pooled modes.

The pool must hide completion reordering and report the earliest failure, while the shared arena
honors probe limits, reuses exact budgets across stages, scales beyond 32 workers, starts only
useful lanes, caps its peak, and steals only compatible work.
"""

from __future__ import annotations

import pytest

from schema_sanitizer.core_impl.native_runtime import native_core

pytestmark = pytest.mark.usefixtures("require_native")


def test_native_inline_executor_preserves_order_without_worker_threads() -> None:
    """The inline executor runs every packet on one calling host thread."""
    ordinals, values, thread_count, failure, inline, workers, status = (
        native_core.ordered_executor_probe(0, 8, 24, -1)
    )

    assert ordinals == tuple(range(24))
    assert values == tuple(index * 10 for index in range(24))
    assert thread_count == 1
    assert failure == -1
    assert inline is True
    assert workers == 1
    assert status == "OK"


def test_native_pool_hides_forced_out_of_order_completion() -> None:
    """Pool completion timing never changes coordinator-visible ordinal order."""
    ordinals, values, thread_count, failure, inline, workers, status = (
        native_core.ordered_executor_probe(1, 4, 48, -1)
    )

    assert ordinals == tuple(range(48))
    assert values == tuple(index * 10 for index in range(48))
    assert 2 <= thread_count <= 4
    assert failure == -1
    assert inline is False
    assert workers == 4
    assert status == "OK"


def test_native_pool_reports_earliest_source_order_failure() -> None:
    """A later fast failure cannot overtake the lowest failing ordinal."""
    ordinals, values, _threads, failure, inline, workers, status = (
        native_core.ordered_executor_probe(1, 4, 32, 3)
    )

    assert ordinals == (0, 1, 2)
    assert values == (0, 10, 20)
    assert failure == 3
    assert inline is False
    assert workers == 4
    assert "probe failure at ordinal 3" in status


def test_native_executor_probe_validates_limits() -> None:
    """The internal probe rejects invalid modes, worker counts, and ordinals."""
    with pytest.raises(ValueError, match="mode"):
        native_core.ordered_executor_probe(2, 1, 1, -1)
    with pytest.raises(ValueError, match="workers"):
        native_core.ordered_executor_probe(1, 0, 1, -1)
    with pytest.raises(ValueError, match="task_count"):
        native_core.ordered_executor_probe(1, 1, -1, -1)
    with pytest.raises(ValueError, match="fail_ordinal"):
        native_core.ordered_executor_probe(1, 1, 1, 1)


def test_operation_task_arena_reuses_exact_worker_budget_across_stages() -> None:
    """Complementary stages share N physical workers without oversubscription."""
    workers, peak, total_threads, overlap, upstream, output, submitted = (
        native_core.operation_task_arena_probe(8, 4, 4, 32)
    )

    assert workers == 8
    assert 1 <= peak <= workers
    assert total_threads == 8
    assert overlap == 0
    assert upstream == 4
    assert output == 4
    assert submitted == 64


def test_operation_task_arena_executes_beyond_32_workers() -> None:
    """A 64-worker arena retains its lanes while sharing process CPU capacity."""
    workers, peak, total_threads, overlap, upstream, output, submitted = (
        native_core.operation_task_arena_probe(64, 32, 32, 128)
    )

    assert workers == 64
    assert 1 <= peak <= workers
    assert total_threads == 64
    assert overlap == 0
    assert upstream == 32
    assert output == 32
    assert submitted == 256


def test_operation_task_arena_single_mode_is_strictly_inline() -> None:
    """An arena with one worker does not create a native helper thread."""
    workers, peak, total_threads, overlap, upstream, output, submitted = (
        native_core.operation_task_arena_probe(1, 1, 1, 4)
    )

    assert workers == 1
    assert peak <= 1
    assert total_threads == 1
    assert overlap == 1
    assert upstream == 1
    assert output == 1
    assert submitted == 0


def test_operation_task_arena_starts_only_workers_used_by_stage_lanes() -> None:
    """N remains available while narrow stages avoid starting idle helpers."""
    workers, peak, total_threads, overlap, upstream, output, submitted = (
        native_core.operation_task_arena_probe(8, 2, 2, 16)
    )

    assert workers == 8
    assert 1 <= peak <= 4
    assert total_threads == 4
    assert overlap == 0
    assert upstream == 2
    assert output == 2
    assert submitted == 32


def test_operation_task_arena_peak_respects_available_stage_tasks() -> None:
    """Peak validation counts runnable packets, not merely configured lane widths."""
    workers, peak, total_threads, overlap, upstream, output, submitted = (
        native_core.operation_task_arena_probe(8, 8, 1, 4)
    )

    assert workers == 8
    assert 1 <= peak <= 5
    assert total_threads == 5
    assert overlap == 0
    assert upstream == 4
    assert output == 1
    assert submitted == 8


def test_operation_task_arena_steals_lane_compatible_backlog() -> None:
    """An idle compatible worker drains work queued behind a slow packet."""
    stolen, displaced_worker, completed, queued, peak = (
        native_core.operation_task_arena_stealing_probe()
    )

    effective_workers = completed // 2
    assert 2 <= effective_workers <= 4
    assert stolen >= 1
    assert displaced_worker in range(1, effective_workers)
    assert completed == effective_workers * 2
    assert queued == 0
    assert peak == effective_workers
