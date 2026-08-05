"""Regression coverage for v83 park-boundary scheduler sampling."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"


def test_v83_wake_epoch_is_sampled_only_at_park_wake_boundaries() -> None:
    """Completed packets no longer issue a wake-generation acquire load."""
    runtime = RUNTIME.read_text(encoding="utf-8")
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


def test_v83_all_56_pairs_inherit_park_boundary_sampling() -> None:
    """Every supported source-to-sink route crosses the optimized worker loop."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for output_name, guarantee in outputs.items():
            shared = guarantee["shared_parallel_stages"]
            assert "park_boundary_wake_epoch_sampling" in shared, (
                input_name,
                output_name,
            )
            assert "telemetry_aware_clock_elision" in shared, (
                input_name,
                output_name,
            )
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v83_native_mixed_lane_drain_preserves_exact_counts() -> None:
    """Cached control state still drains upstream, output, and broad work."""
    require_native()
    rounds = 10_000
    (
        elapsed_ns,
        stolen,
        started,
        peak,
        finished,
        queued,
        submitted,
    ) = native_core.operation_task_arena_mixed_lane_probe(4, rounds)

    assert elapsed_ns > 0
    assert submitted == 2 + rounds * 3
    assert finished == rounds * 3
    assert queued == 0
    assert started == 4
    assert peak == 4
    assert stolen >= 0


def test_v83_native_park_wake_cycles_preserve_targeted_epochs() -> None:
    """Repeated parks refresh private epochs without losing mixed-lane work."""
    require_native()
    rounds = 40_000
    waves = 128
    workers = 4
    (
        elapsed_ns,
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
    assert elapsed_ns > 0
    assert submitted == workers + rounds + wave_tasks
    assert finished == rounds + wave_tasks
    assert queued == 0
    assert wake_before_preload == wake_after_preload == workers
    assert workers < wake_final < submitted
    assert started == workers
    assert peak == workers
