"""Regression coverage for concurrency output preference is dormant through eight workers."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_output_preference_is_dormant_through_eight_workers() -> None:
    """The low-core arena keeps strict local FIFO and the exact thread budget."""
    require_native()
    promoted, outputs, broad, started, queued, _elapsed_us = (
        native_core.operation_task_arena_output_preference_probe(8)
    )

    assert promoted == 0
    assert outputs == 4
    assert broad == 8
    assert started == 8
    assert queued == 0


def test_high_core_output_lane_bypasses_local_broad_backlog() -> None:
    """Dedicated output tasks run before broad upstream tasks on high workers."""
    require_native()
    promoted, outputs, broad, started, queued, _elapsed_us = (
        native_core.operation_task_arena_output_preference_probe(16)
    )

    assert 0 <= promoted <= 8
    assert outputs == 8
    assert broad == 16
    # Lazy startup is exact but demand-driven; a short mixed-lane run may
    # complete before every physical worker is needed.
    assert 1 <= started <= 16
    assert queued == 0


def test_scheduler_uses_compile_time_low_core_specialization() -> None:
    """One-through-eight workers retain the legacy front/pop-front hot path."""
    root = Path(__file__).resolve().parents[2]
    runtime = (root / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text()
    probe = (root / "cpp/src/api/python_abi3/runtime/arena_scheduler_probe.cc").read_text()

    assert "template <bool PreferDedicatedOutput, bool CheckGlobalStopping>" in runtime
    assert "worker_loop<true, false>(state, index, stop)" in runtime
    assert "worker_loop<false, false>(state, index, stop)" in runtime
    assert "worker_loop<false, true>(state, index, stop)" in runtime
    assert "take_local<false>(state, index, false, nullptr, &queued, true)" in runtime
    assert "queued.lane_end == state->worker_count" in runtime
    assert "queued.lane_begin >= high_begin" in runtime
    assert "allow_output_preference = !preference_used" in runtime
    assert "A steal must obey the same one-bypass fairness contract" in runtime
    assert "Cross-worker stealing never recreates the output-priority" in runtime
    assert "selected = candidate.tasks.begin()" in runtime
    assert "slot.tasks.pop_front()" in runtime
    assert "operation_task_arena_output_preference_probe" in probe


def test_mixed_lanes_still_drain_and_steal_without_extra_workers() -> None:
    """The preference keeps mixed lanes live and within the arena budget."""
    require_native()
    (
        _elapsed_ns,
        stolen,
        started,
        peak,
        work_finished,
        queued,
        submitted,
    ) = native_core.operation_task_arena_mixed_lane_probe(16, 1_000)

    assert stolen > 0
    assert 1 <= started <= 16
    assert 1 <= peak <= 16
    assert work_finished == 3_000
    assert queued == 0
    assert submitted == 3_008


def test_output_preference_forces_fifo_after_one_bypass() -> None:
    """A second output wave cannot repeatedly starve the broad front task."""
    require_native()
    promoted, outputs, broad, started, queued, _elapsed_us = (
        native_core.operation_task_arena_output_preference_probe(16, 2)
    )

    # At most one local bypass per high output queue; steals must preserve
    # the compatible broad front, so the second wave cannot double promotion.
    assert 0 <= promoted <= 8
    assert outputs == 16
    assert broad == 16
    assert 1 <= started <= 16
    assert queued == 0
