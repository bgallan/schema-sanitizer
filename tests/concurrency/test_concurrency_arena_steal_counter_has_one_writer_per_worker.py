"""Regression coverage for concurrency arena steal counter has one writer per worker."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
TELEMETRY_HEADER = ROOT / "cpp/src/internal/runtime/performance_telemetry.hh"
TELEMETRY_IMPL = ROOT / "cpp/src/internal/runtime/performance_telemetry.cc"
TELEMETRY_JSON = ROOT / "cpp/src/internal/runtime/performance_telemetry_json.cc.inc"


def test_arena_steal_counter_has_one_writer_per_worker() -> None:
    """Each worker publishes steals through its own counter."""
    arena = ARENA.read_text(encoding="utf-8")
    runtime = RUNTIME.read_text(encoding="utf-8")

    worker_slot = arena[arena.index("struct WorkerSlot") : arena.index("explicit State")]
    state_fields = arena[arena.index("const std::size_t worker_count") : arena.index("namespace {")]
    steal_path = runtime[runtime.index("bool steal_compatible") : runtime.index("bool take_task")]

    assert "std::atomic<std::size_t> stolen{0};" in worker_slot
    assert "std::atomic<std::size_t> stolen{0};" not in state_fields
    assert "auto &thief = *state->slots[index];" in steal_path
    assert "std::size_t stolen_local = 0;" in worker_slot
    assert "++thief.stolen_local;" in steal_path
    assert "thief.stolen.store(thief.stolen_local" in steal_path
    assert "state->stolen.fetch_add" not in steal_path


def test_performance_telemetry_uses_the_same_worker_ownership() -> None:
    """Enabled telemetry does not reintroduce a second global steal RMW."""
    header = TELEMETRY_HEADER.read_text(encoding="utf-8")
    implementation = TELEMETRY_IMPL.read_text(encoding="utf-8")
    json_source = TELEMETRY_JSON.read_text(encoding="utf-8")

    assert "RecordWorkerTaskStolen(std::size_t worker_index)" in header
    assert "std::atomic<std::int64_t> stolen{0};" in header
    worker_method = implementation[
        implementation.index("RecordWorkerTaskStolen") : implementation.index("RecordWorkerStarted")
    ]
    assert "++shard.stolen_local;" in worker_method
    assert "shard.stolen.store(shard.stolen_local" in worker_method
    assert "fetch_add" not in worker_method
    assert "stolen_task_count" in json_source
    assert "shard.stolen.load(std::memory_order_relaxed)" in json_source


def test_native_mixed_lanes_accumulate_multiple_worker_shards() -> None:
    """Mixed input/output lanes still drain while compatible workers steal."""
    require_native()
    _elapsed, stolen, started, peak, finished, queued, submitted = (
        native_core.operation_task_arena_mixed_lane_probe(4, 8_000)
    )

    assert stolen > 0
    assert 1 <= peak <= 4
    assert finished == 24_000
    assert queued == 0
    assert submitted == 24_002
    blocker_count = submitted - finished
    assert blocker_count <= started <= 4
