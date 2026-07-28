"""Regression coverage for v46 high-core output-aware compatible stealing."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_v46_output_steal_preference_is_dormant_through_eight_workers() -> None:
    """The eight-worker stealing path remains the legacy reverse scan."""
    require_native()
    promoted, outputs, broad, stolen, started, queued, submitted = (
        native_core.operation_task_arena_output_steal_probe(8)
    )

    assert promoted == 0
    assert outputs == 3
    assert broad == 7
    assert stolen > 0
    assert started == 8
    assert queued == 0
    assert submitted == 18


def test_v46_idle_high_worker_steals_front_output_before_later_broad_work() -> None:
    """An idle high worker no longer hides front output behind back broad work."""
    require_native()
    promoted, outputs, broad, stolen, started, queued, submitted = (
        native_core.operation_task_arena_output_steal_probe(16)
    )

    assert promoted == 7
    assert outputs == 7
    assert broad == 15
    assert stolen >= 7
    assert started == 16
    assert queued == 0
    assert submitted == 38


def test_v46_steal_preference_is_constant_time_and_high_core_only() -> None:
    """The extension adds no queue scan, global index, or low-core branch."""
    root = Path(__file__).resolve().parents[1]
    runtime = (root / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text()
    probe = (root / "cpp/src/api/python_abi3/runtime/arena_scheduler_probe.cc").read_text()

    assert "template <bool PreferDedicatedOutput>\nbool steal_compatible" in runtime
    assert "dedicated_high_output(candidate.tasks.front()" in runtime
    assert "selected = candidate.tasks.begin()" in runtime
    assert "worker_loop<true, false>(state, index, stop)" in runtime
    assert "worker_loop<false, false>(state, index, stop)" in runtime
    assert "worker_loop<false, true>(state, index, stop)" in runtime
    assert "global lane index, queue scan" in runtime
    assert "operation_task_arena_output_steal_probe" in probe


def test_v46_preserves_v45_local_fairness_and_complete_drain() -> None:
    """Remote preference does not weaken the one-bypass local FIFO contract."""
    require_native()
    promoted, outputs, broad, started, queued, elapsed_us = (
        native_core.operation_task_arena_output_preference_probe(16, 2)
    )

    assert promoted == 8
    assert outputs == 16
    assert broad == 16
    assert started == 16
    assert queued == 0
    assert elapsed_us < 5_000
