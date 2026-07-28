"""Regression coverage for v120 compact queued-task lane metadata."""

from __future__ import annotations

import json
from pathlib import Path

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[1]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
EVIDENCE = ROOT / "benchmarks/v120_compact_queued_task_ab.json"
PROBE = ROOT / "benchmarks/v120_compact_queued_task_tsan.cc"
STAGE = "compact_queued_task_lane_metadata"


def test_v120_queue_packet_uses_unbounded_lane_bounds() -> None:
    """Queued lane metadata remains lossless beyond the historical 32-worker path."""
    arena = ARENA.read_text(encoding="utf-8")
    packet = arena.split("struct QueuedTask final", 1)[1].split("};", 1)[0]

    assert "std::size_t lane_begin = 0;" in packet
    assert "std::size_t lane_end = 1;" in packet
    assert "std::uint8_t lane_begin" not in packet
    assert "std::uint8_t lane_end" not in packet
    assert ".lane_begin = lane_begin" in arena
    assert ".lane_end = lane_end" in arena
    assert "scalable_scan(count > 32U)" in arena
    assert "worker count exceeds 32" not in arena


def test_v120_workers_use_native_size_bounds_for_arithmetic() -> None:
    """Compatibility and relative worker arithmetic remain size_t exact."""
    source = RUNTIME.read_text(encoding="utf-8")

    assert "index >= queued.lane_begin && index < queued.lane_end" in source
    assert "index - static_cast<std::size_t>(queued.lane_begin)" in source
    assert "static_cast<std::size_t>(queued.lane_end)" not in source
    assert "dedicated_high_output" in source
    assert "compatible(" in source


def test_v120_probe_covers_every_lane_kind_and_boundary_width() -> None:
    """The real arena probe validates compact bounds through local and stolen work."""
    source = PROBE.read_text(encoding="utf-8")
    compact = source.replace(" ", "").replace("\n", "")

    assert "{2U,3U,5U,8U,16U,32U}" in compact
    assert "TaskArenaLane::kUpstream" in source
    assert "TaskArenaLane::kOutputCompact" in source
    assert "TaskArenaLane::kOutput" in source
    assert "TaskArenaLane::kAll" in source
    assert "relative>=widths[producer]" in compact
    assert "arena->stolen_tasks()>0U" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_v120_all_56_pairs_inherit_compact_queue_packets() -> None:
    """Every supported source/sink pair schedules the common compact packet."""
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


def test_v120_evidence_is_positive_and_narrowly_scoped() -> None:
    """Fixed-affinity evidence records density wins without throughput claims."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    assert evidence["pair_count"] == 15
    assert evidence["baseline_packet_bytes"] == 72
    assert evidence["candidate_packet_bytes"] == 56
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {item["queue_packets"]: item for item in evidence["scenarios"]}
    assert set(scenarios) == {64, 256, 1024}
    for item in scenarios.values():
        assert item["candidate_wins"] >= 13
        assert item["paired_median_reduction_percent"] > 5.0


def test_v120_version_is_0373() -> None:
    """The source package exposes the v120 project version."""
    assert (ROOT / "meta/VERSION").read_text(encoding="utf-8").strip() == "0.3.73"
