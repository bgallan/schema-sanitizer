"""Regression coverage for concurrency idle selection uses initialized snapshot without started reload."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
BENCH = (
    ROOT / "benchmarks/evidence/concurrency/scheduler/initialized-worker-admission-snapshot.json"
)


def test_idle_selection_uses_initialized_snapshot_without_started_reload() -> None:
    """Verify the named concurrency regression contract."""
    source = RUNTIME.read_text(encoding="utf-8")
    helper = source[source.index("idle_started_worker(") : source.index("void mark_nonempty")]
    assert "std::uint64_t initialized_snapshot" in helper
    assert "initialized_snapshot & allowed" in helper
    assert "started_mask.load" not in helper
    assert "initialized implies started" in helper


def test_documentation_and_benchmark_cover_matrix_and_host_limit() -> None:
    """Verify the named concurrency regression contract."""
    benchmark = BENCH.read_text(encoding="utf-8")
    for workers in ('"2"', '"4"', '"5"', '"8"', '"16"'):
        assert workers in benchmark
