"""Regression coverage for v112 monotonic initialized-worker park snapshots."""

from __future__ import annotations

import json
from pathlib import Path

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
EVIDENCE = ROOT / "benchmarks/v112_monotonic_park_snapshot_ab.json"
STAGE = "monotonic_initialized_worker_park_snapshot_elision"


def test_v112_startup_flag_is_reloaded_only_while_it_can_change() -> None:
    """A cached false one-shot startup flag suppresses all later loads."""
    source = RUNTIME.read_text(encoding="utf-8")
    park = source.split("if (!found) {", 1)[1].split("activity.Start();", 1)[0]

    assert "first_task_pending is monotonic" in park
    assert park.count("if (first_task_pending) {") == 2
    assert park.count("slot.first_task_pending.load") == 2
    for fragment in park.split("slot.first_task_pending.load")[:-1]:
        assert fragment.rfind("if (first_task_pending) {") > fragment.rfind("}")


def test_v112_local_recheck_and_wait_capture_remove_epoch_reloads() -> None:
    """Only a real park samples the epoch; the predicate retains its wake."""
    source = RUNTIME.read_text(encoding="utf-8")
    park = source.split("if (!found) {", 1)[1].split("activity.Start();", 1)[0]
    local_recheck = park.split("if (!slot.tasks.empty()) {", 1)[1].split("}", 1)[0]
    wait = park.split("WaitWithStop(slot.ready", 1)[1]

    assert "wake_epoch.load" not in local_recheck
    assert "const auto current_epoch" in wait
    assert "observed_epoch = current_epoch;" in wait
    after_wait = wait.split("});", 1)[1]
    assert "observed_epoch = slot.wake_epoch.load" not in after_wait


def test_v112_real_arena_probe_exercises_repeated_park_wake_waves() -> None:
    """The standalone probe requires exact activity and zero-drain waves."""
    source = (ROOT / "benchmarks/v112_monotonic_park_snapshot_tsan.cc").read_text(encoding="utf-8")

    assert "OperationTaskArena::Make(workers)" in source
    assert "for (const auto workers : {2U, 4U, 8U, 16U})" in source
    assert "constexpr std::size_t kWaves = 128U" in source
    assert "arena->active_tasks() == workers" in source
    assert "arena->active_tasks() == 0U" in source
    assert "arena->queued_tasks() == 0U" in source
    assert "arena->wake_epoch_publishes() >= workers" in source


def test_v112_all_56_pairs_inherit_monotonic_park_snapshots() -> None:
    """Every supported source/sink pair crosses the common worker loop."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for output_name, guarantee in outputs.items():
            assert STAGE in guarantee["shared_parallel_stages"], (
                input_name,
                output_name,
            )
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v112_evidence_is_positive_and_scoped_to_snapshot_bookkeeping() -> None:
    """Evidence avoids claims about condition variables or full pipelines."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    assert evidence["pair_count"] == 15
    assert "park/wake atomic snapshot bookkeeping" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {2, 4, 8, 16}
    for item in scenarios.values():
        assert item["candidate_wins"] >= 13
        assert item["paired_median_reduction_percent"] > 25.0
