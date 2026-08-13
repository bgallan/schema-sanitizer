"""Regression coverage for concurrency worker initialization is one shot and park sampled."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"


def test_worker_initialization_is_one_shot_and_park_sampled() -> None:
    """Steady-state local packets avoid initialization mask and flag traffic."""
    runtime = RUNTIME.read_text(encoding="utf-8")
    signature = runtime.index("bool take_local(")
    first_store = runtime.index("slot.first_task_pending.store", signature)
    init_guard = runtime.rfind("if (initialize_worker)", signature, first_store)
    loop = runtime.index("while (!stop.stop_requested())")
    no_work = runtime.index("if (!found)", loop)
    task = runtime.index("queued.task(index - static_cast<std::size_t>(queued.lane_begin), stop);")

    assert signature < init_guard < first_store < loop < no_work < task
    assert "bool first_task_pending =" in runtime[:loop]
    assert "slot.first_task_pending.load" not in runtime[loop:no_work]
    assert runtime.count("slot.first_task_pending.load") == 3
    assert "park/wake boundaries" in runtime


def test_native_mixed_lanes_preserve_exact_drain_and_worker_budget() -> None:
    """Transition-only masks do not strand upstream, output, or broad work."""
    require_native()
    rounds = 20_000
    (
        _elapsed_ns,
        stolen,
        started,
        peak,
        finished,
        queued,
        submitted,
    ) = native_core.operation_task_arena_mixed_lane_probe(4, rounds)

    assert submitted == 2 + rounds * 3
    assert finished == rounds * 3
    assert queued == 0
    assert started == 4
    assert peak == 4
    assert stolen >= 0


def test_native_park_wake_cycles_keep_transition_masks_live() -> None:
    """Repeated empty/nonempty transitions remain visible after worker parks."""
    require_native()
    workers = 4
    rounds = 10_000
    waves = 96
    (
        _elapsed_ns,
        submitted,
        finished,
        queued,
        wake_before_preload,
        wake_after_preload,
        wake_final,
        started,
        peak,
    ) = native_core.operation_task_arena_wake_coalescing_probe(workers, rounds, waves)

    wave_tasks = waves * workers * 2
    runnable_blockers = submitted - rounds - wave_tasks
    assert 1 <= runnable_blockers <= workers
    assert submitted == runnable_blockers + rounds + wave_tasks
    assert finished == rounds + wave_tasks
    assert queued == 0
    assert wake_before_preload == wake_after_preload == runnable_blockers
    assert runnable_blockers < wake_final < submitted
    assert started == workers
    assert runnable_blockers <= peak <= workers
