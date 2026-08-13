"""Regression coverage for concurrency wake generation is targeted and cache line aligned."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
HEADER = ROOT / "cpp/src/internal/runtime/operation_task_arena.hh"


def test_wake_generation_is_targeted_and_cache_line_aligned() -> None:
    """The operation-global wake cache line is absent from the hot path."""
    arena = ARENA.read_text(encoding="utf-8")
    runtime = RUNTIME.read_text(encoding="utf-8")

    assert "alignas(64) std::atomic<std::uint64_t> wake_epoch" in arena
    assert "std::atomic<std::uint64_t> work_epoch" not in arena
    assert "state_->work_epoch.fetch_add" not in arena
    assert "slot.wake_epoch.fetch_add" in arena
    assert "helper_slot.wake_epoch.fetch_add" in arena
    assert "slot.wake_epoch.load" in runtime
    assert "state->work_epoch.load" not in runtime


def test_running_target_coalesces_without_weakening_helper_wakes() -> None:
    """Only parked targets and real idle helpers receive a generation."""
    arena = ARENA.read_text(encoding="utf-8")

    helper = arena.index("auto helper = lane_end;")
    target = arena.index("const auto wake_target = !target_running;", helper)
    target_publish = arena.index("slot.wake_epoch.fetch_add", target)
    helper_publish = arena.index("helper_slot.wake_epoch.fetch_add", target_publish)

    assert helper < target < target_publish < helper_publish
    assert "if (wake_target)" in arena[target:target_publish]
    assert "if (wake_helper)" in arena[target_publish:helper_publish]
    assert "if (state_->worker_count <= 8U)" not in arena
    assert "worker must actually leave its park state" in arena


def test_worker_wait_predicate_uses_its_own_generation() -> None:
    """The pre-park queue recheck remains ahead of targeted waiting."""
    runtime = RUNTIME.read_text(encoding="utf-8")
    recheck = runtime.index("if (!slot.tasks.empty())")
    stop = runtime.index("activity.Stop();", recheck)
    wait = runtime.index("WaitWithStop(slot.ready", stop)

    assert recheck < stop < wait
    assert "slot.wake_epoch.load(std::memory_order_acquire)" in runtime[recheck:]
    comment = runtime.rfind("// A producer may have appended", 0, recheck)
    assert comment >= 0
    assert "targeted wake coalescing cannot strand" in runtime[comment:stop]


def test_native_probe_coalesces_busy_submissions_and_survives_park_waves() -> None:
    """Busy preloads publish no wake, while repeated mixed-lane parks drain."""
    require_native()
    rounds = 20_000
    waves = 64
    workers = 4
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
    assert runnable_blockers <= started <= workers
    assert runnable_blockers <= peak <= workers
