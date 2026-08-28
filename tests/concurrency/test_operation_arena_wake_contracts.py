"""Exercise wake, park, and one-shot worker initialization in the operation arena.

Epoch sampling stays at park boundaries, targeted generations remain cache-aligned, running-target
coalescing preserves helper wakes, and native mixed lanes drain exactly through repeated waves.
"""

from __future__ import annotations

import pytest
from _support.source_contracts import source_text

from schema_sanitizer.core_impl.native_runtime import native_core

ARENA = "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"


def test_wake_epoch_is_sampled_only_at_park_wake_boundaries() -> None:
    """Completed packets avoid wake-generation acquire loads."""
    runtime = source_text(RUNTIME)
    task_run = runtime.index(
        "queued.task(index - static_cast<std::size_t>(queued.lane_begin), stop);"
    )
    loop_end = runtime.index("\n  }\n}\n[[nodiscard]] bool worker_already_started", task_run)
    assert "slot.wake_epoch.load" not in runtime[task_run:loop_end]
    park = runtime.index("// Refresh the generation exactly once at the park boundary")
    stop = runtime.index("activity.Stop();", park)
    wait = runtime.index("WaitWithStop(slot.ready", stop)
    assert park < stop < wait
    park_comment = " ".join(runtime[park:stop].replace("//", " ").split())
    assert "sampling after every completed packet is redundant" in park_comment


def test_worker_initialization_is_one_shot_and_park_sampled() -> None:
    """Steady-state packets avoid initialization-mask and flag traffic."""
    runtime = source_text(RUNTIME)
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


def test_wake_generation_is_targeted_and_cache_line_aligned() -> None:
    """The operation-global wake cache line is absent from the hot path."""
    arena = source_text(ARENA)
    runtime = source_text(RUNTIME)
    assert "alignas(64) std::atomic<std::uint64_t> wake_epoch" in arena
    assert "std::atomic<std::uint64_t> work_epoch" not in arena
    assert "state_->work_epoch.fetch_add" not in arena
    assert "slot.wake_epoch.fetch_add" in arena
    assert "helper_slot.wake_epoch.fetch_add" in arena
    assert "slot.wake_epoch.load" in runtime
    assert "state->work_epoch.load" not in runtime


def test_running_target_coalesces_without_weakening_helper_wakes() -> None:
    """Only parked targets and real idle helpers receive a generation."""
    arena = source_text(ARENA)
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
    runtime = source_text(RUNTIME)
    recheck = runtime.index("if (!slot.tasks.empty())")
    stop = runtime.index("activity.Stop();", recheck)
    wait = runtime.index("WaitWithStop(slot.ready", stop)
    assert recheck < stop < wait
    assert "slot.wake_epoch.load(std::memory_order_acquire)" in runtime[recheck:]
    comment = runtime.rfind("// A producer may have appended", 0, recheck)
    assert comment >= 0
    assert "targeted wake coalescing cannot strand" in runtime[comment:stop]


@pytest.mark.parametrize(
    "rounds",
    (
        pytest.param(10_000, id="park-boundary-sampling"),
        pytest.param(20_000, id="one-shot-worker-initialization"),
    ),
)
def test_native_mixed_lanes_preserve_exact_drain(rounds: int, require_native: None) -> None:
    """Transition-only state preserves every mixed-lane task."""
    (_elapsed, stolen, started, peak, finished, queued, submitted) = (
        native_core.operation_task_arena_mixed_lane_probe(4, rounds)
    )
    assert submitted == 2 + rounds * 3
    assert finished == rounds * 3
    assert queued == 0
    assert 1 <= started <= 4
    assert 1 <= peak <= started
    assert stolen >= 0


@pytest.mark.parametrize(
    ("rounds", "waves"),
    (
        pytest.param(40_000, 128, id="park-boundary-epochs"),
        pytest.param(10_000, 96, id="transition-mask-liveness"),
        pytest.param(20_000, 64, id="busy-submission-coalescing"),
    ),
)
def test_native_wake_coalescing_preserves_every_park_wave(
    rounds: int,
    waves: int,
    require_native: None,
) -> None:
    """Busy preloads coalesce while repeated park waves drain exactly."""
    workers = 4
    (
        _elapsed,
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
