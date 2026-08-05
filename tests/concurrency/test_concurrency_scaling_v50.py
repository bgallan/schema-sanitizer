"""Regression coverage for v50 high-core startup and wake fast paths."""

from __future__ import annotations

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_v50_high_core_mixed_lanes_drain_without_extra_workers() -> None:
    """Signal coalescing cannot strand compatible work or oversubscribe."""
    require_native()
    elapsed_ns, stolen, started, peak, finished, queued, submitted = (
        native_core.operation_task_arena_mixed_lane_probe(16, 4_000)
    )

    assert elapsed_ns > 0
    assert stolen > 0
    assert 1 <= started <= 16
    assert 1 <= peak <= 16
    assert finished == 12_000
    assert queued == 0
    assert submitted == 12_008


def test_v50_output_priority_survives_wake_coalescing() -> None:
    """A running target still allows a real idle high-lane helper to wake."""
    require_native()
    promoted, outputs, broad, stolen, started, queued, submitted = (
        native_core.operation_task_arena_output_steal_probe(16)
    )

    assert promoted == outputs == 7
    assert broad == 15
    assert stolen >= 7
    assert started == 16
    assert queued == 0
    assert submitted == 38


def test_v50_low_and_high_core_ordered_results_remain_identical() -> None:
    """Crossing the fast-path gate cannot change the value or ordinal oracle."""
    require_native()
    low = native_core.ordered_executor_arena_completion_probe(8, 16_000, 32)
    high = native_core.ordered_executor_arena_completion_probe(16, 16_000, 32)

    assert low[1] == high[1] == 16_000
    assert low[2] == high[2]
    assert low[5] == high[5] == 0
    assert low[6] == high[6] == 16_000
