"""Regression coverage for v50 high-core startup and wake fast paths."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_v50_startup_fast_path_is_preserved_and_broadened_by_v96() -> None:
    """The original high-core gate now safely covers sustained 4+ worker lanes."""
    root = Path(__file__).resolve().parents[1]
    arena = (root / "cpp/src/internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    runtime = (root / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text(
        encoding="utf-8"
    )

    assert "worker_already_started_fast_path" in runtime
    assert "state->worker_count >= 4U" in runtime
    assert "started_mask.load(std::memory_order_acquire)" in runtime
    assert "worker_already_started_fast_path(state_, physical)" in arena
    assert "const auto wake_target = !target_running" in arena
    assert "const auto wake_helper" in arena
    assert "slot.wake_epoch.fetch_add(1" in arena
    assert "helper_slot.wake_epoch.fetch_add(1" in arena
    assert "state_->work_epoch.fetch_add(1" not in arena
    assert "if (candidates == 0U)" in runtime
    assert "return end;" in runtime


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
