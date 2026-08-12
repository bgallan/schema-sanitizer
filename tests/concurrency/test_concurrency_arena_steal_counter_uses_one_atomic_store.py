"""Regression coverage for concurrency arena steal counter uses one atomic store."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
TELEMETRY_H = ROOT / "cpp/src/internal/runtime/performance_telemetry.hh"
TELEMETRY_CC = ROOT / "cpp/src/internal/runtime/performance_telemetry.cc"


def test_arena_steal_counter_uses_one_atomic_store() -> None:
    """Verify the named concurrency regression contract."""
    arena = ARENA.read_text(encoding="utf-8")
    runtime = RUNTIME.read_text(encoding="utf-8")
    slot = arena[arena.index("struct WorkerSlot") : arena.index("explicit State")]
    steal = runtime[runtime.index("bool steal_compatible") : runtime.index("bool take_task")]
    assert "std::size_t stolen_local = 0;" in slot
    assert "++thief.stolen_local;" in steal
    assert "thief.stolen.store(thief.stolen_local" in steal
    assert "thief.stolen.load" not in steal


def test_telemetry_steal_counter_uses_same_single_store_rule() -> None:
    """Verify the named concurrency regression contract."""
    header = TELEMETRY_H.read_text(encoding="utf-8")
    source = TELEMETRY_CC.read_text(encoding="utf-8")
    method = source[source.index("RecordWorkerTaskStolen") : source.index("RecordWorkerStarted")]
    assert "std::int64_t stolen_local = 0;" in header
    assert "++shard.stolen_local;" in method
    assert "shard.stolen.store(shard.stolen_local" in method
    assert ".stolen.load" not in method


def test_native_stealing_and_mixed_lanes_remain_exact() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    stolen, displaced, completed, queued, peak = native_core.operation_task_arena_stealing_probe()
    assert stolen >= 1
    assert displaced in {1, 2, 3}
    assert completed == 8
    assert queued == 0
    assert 2 <= peak <= 4
    for workers in (4, 8, 16):
        elapsed, stolen, started, peak, finished, queued, submitted = (
            native_core.operation_task_arena_mixed_lane_probe(workers, 2_000)
        )
        assert elapsed > 0
        assert stolen > 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert finished == 6_000
        assert queued == 0
        assert submitted > finished
