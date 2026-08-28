"""Regression coverage for concurrency startup flag is reloaded only while it can change."""

from __future__ import annotations

from pathlib import Path

from benchmarks.concurrency.assets import load_evidence, load_probe

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"


def test_startup_flag_is_reloaded_only_while_it_can_change() -> None:
    """A cached false one-shot startup flag suppresses all later loads."""
    source = RUNTIME.read_text(encoding="utf-8")
    park = source.split("if (!found) {", 1)[1].split("activity.Start();", 1)[0]

    assert "first_task_pending is monotonic" in park
    assert park.count("if (first_task_pending) {") == 2
    assert park.count("slot.first_task_pending.load") == 2
    for fragment in park.split("slot.first_task_pending.load")[:-1]:
        assert fragment.rfind("if (first_task_pending) {") > fragment.rfind("}")


def test_local_recheck_and_wait_capture_remove_epoch_reloads() -> None:
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


def test_real_arena_probe_exercises_repeated_park_wake_waves() -> None:
    """The standalone probe requires exact activity and zero-drain waves."""
    source = load_probe("scheduler/initialized-worker-park-snapshot-tsan.cc")

    assert "OperationTaskArena::Make(workers)" in source
    assert "for (const auto workers : {2U, 4U, 8U, 16U})" in source
    assert "constexpr std::size_t kWaves = 128U" in source
    assert "arena->active_tasks() == workers" in source
    assert "arena->active_tasks() == 0U" in source
    assert "arena->queued_tasks() == 0U" in source
    assert "arena->wake_epoch_publishes() >= workers" in source


def test_evidence_is_positive_and_scoped_to_snapshot_bookkeeping() -> None:
    """Evidence avoids claims about condition variables or full pipelines."""
    evidence = load_evidence("initialized-worker-park-snapshot")

    assert evidence["pair_count"] == 15
    assert "park/wake atomic snapshot bookkeeping" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {2, 4, 8, 16}
    for item in scenarios.values():
        assert item["candidate_wins"] >= 13
        assert item["paired_median_reduction_percent"] > 25.0
