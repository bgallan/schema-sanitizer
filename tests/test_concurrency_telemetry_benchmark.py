"""Unit coverage for the evidence-driven telemetry benchmark."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from benchmarks.bench_concurrency_telemetry import _dram_high_width_coverage
from benchmarks.concurrency_telemetry_analysis import (
    classify_run,
    parse_perf_stat,
    recommend_frontier,
)
from benchmarks.concurrency_telemetry_support import format_cpu_list, parse_cpu_list


def _native(primary: str = "mixed_or_unresolved") -> dict:
    """Return the minimal native diagnosis payload used by classifier tests."""
    return {
        "effective_workers": 4,
        "diagnosis": {
            "primary": primary,
            "confidence": "low",
            "memory_capacity_pressure": False,
        },
    }


def _run(
    seconds: float,
    *,
    primary: str = "mixed_or_unresolved",
    frontend_share: float = 0.0,
    wait_share: float = 0.0,
    bandwidth: bool = False,
) -> dict:
    """Return one synthetic matrix run."""
    return {
        "median_wall_seconds": seconds,
        "representative_native": {
            "diagnosis": {
                "primary": primary,
                "frontend_share": frontend_share,
                "coordinator_wait_share": wait_share,
            }
        },
        "combined_diagnosis": {
            "primary": primary,
            "memory_bandwidth_proven": bandwidth,
        },
    }


def test_generic_counters_do_not_claim_dram_bandwidth() -> None:
    """Cycles and cache misses alone are not a DRAM bandwidth measurement."""
    perf = {
        "counters": {
            "cycles": 1_000.0,
            "instructions": 500.0,
            "cache-references": 100.0,
            "cache-misses": 40.0,
        }
    }
    diagnosis = classify_run(_native(), perf, None)
    assert diagnosis["primary"] == "cache_or_memory_latency"
    assert diagnosis["memory_bandwidth_proven"] is False
    assert diagnosis["dram_bandwidth_ratio"] is None


def test_supplied_platform_bandwidth_can_prove_saturation() -> None:
    """An explicit measured/sustainable baseline is required for saturation."""
    diagnosis = classify_run(
        _native("worker_compute_or_memory_hierarchy"),
        {"counters": {}},
        {"measured_gib_s": 91.0, "sustainable_gib_s": 100.0},
    )
    assert diagnosis["primary"] == "dram_bandwidth_saturation"
    assert diagnosis["confidence"] == "high"
    assert diagnosis["memory_bandwidth_proven"] is True
    assert diagnosis["dram_bandwidth_ratio"] == 0.91


def test_native_specific_diagnosis_precedes_generic_perf_heuristics() -> None:
    """A measured coordinator bottleneck is not relabelled from generic IPC."""
    perf = {"counters": {"cycles": 100.0, "instructions": 250.0}}
    diagnosis = classify_run(_native("insufficient_parallel_granularity"), perf, None)
    assert diagnosis["primary"] == "insufficient_parallel_granularity"
    assert diagnosis["memory_bandwidth_proven"] is False


def test_perf_parser_aggregates_repeated_uncore_event_rows(tmp_path: Path) -> None:
    """Per-channel uncore rows are summed rather than overwritten."""
    raw = tmp_path / "perf.txt"
    raw.write_text(
        "100;;uncore_imc_0/cas_count_read/;1;100.00\n"
        "120;;uncore_imc_0/cas_count_read/;1;100.00\n"
        "300;;cycles:u;1;100.00\n",
        encoding="utf-8",
    )
    parsed = parse_perf_stat(raw)
    assert parsed["counters"]["uncore_imc_0/cas_count_read/"] == 220.0
    assert parsed["counters"]["cycles"] == 300.0


def test_cpu_list_round_trip_is_canonical() -> None:
    """Explicit host affinities retain exact nested CPU identities."""
    assert parse_cpu_list("0-2,4,6-7") == (0, 1, 2, 4, 6, 7)
    assert format_cpu_list((0, 1, 2, 4, 6, 7)) == "0-2,4,6-7"


def test_paired_workloads_select_output_partitioning() -> None:
    """Arrow scaling plus a flat full pipeline identifies output as the frontier."""
    workloads = {
        "arrow_stream": {"runs": {"8": _run(1.0), "16": _run(0.9)}},
        "jsonl_to_jsonl": {"runs": {"8": _run(2.0), "16": _run(1.98)}},
    }
    recommendation = recommend_frontier(workloads, (1, 2, 4, 8, 16))
    assert recommendation["primary"] == "output_path_plateau"
    assert recommendation["recommended_action"] == ("prototype_partitioned_or_pipelined_output")


def test_frontier_uses_the_highest_adjacent_pair_and_reports_every_gain() -> None:
    """A 32-worker matrix is diagnosed from 16→32 rather than 8→16."""
    workloads = {
        "arrow_stream": {
            "runs": {
                "8": _run(1.0),
                "16": _run(0.5),
                "32": _run(0.49),
            }
        },
        "jsonl_to_jsonl": {
            "runs": {
                "8": _run(2.0),
                "16": _run(1.0),
                "32": _run(0.98),
            }
        },
    }
    recommendation = recommend_frontier(workloads, (8, 16, 32))
    assert recommendation["comparison"] == {
        "low_workers": 16,
        "high_workers": 32,
    }
    assert [
        (gain["low_workers"], gain["high_workers"]) for gain in recommendation["adjacent_gains"]
    ] == [(8, 16), (16, 32)]
    assert recommendation["primary"] != "scaling_still_useful"


def test_dram_coverage_uses_the_highest_requested_width() -> None:
    """A 32-worker run cannot be certified by a 16-worker DRAM sample."""
    coverage = _dram_high_width_coverage(
        {
            "arrow_stream": {"16": {"measured_gib_s": 1.0}},
        },
        ("arrow_stream",),
        (8, 16, 32),
    )
    assert coverage == {
        "high_workers": 32,
        "complete": False,
        "missing_workloads": ["arrow_stream"],
    }


def test_proven_dram_plateau_precedes_cache_guess() -> None:
    """A flat paired curve with platform bandwidth evidence selects memory traffic."""
    workloads = {
        "arrow_stream": {
            "runs": {
                "8": _run(1.0),
                "16": _run(0.99, primary="cache_or_memory_latency", bandwidth=True),
            }
        },
        "jsonl_to_jsonl": {
            "runs": {
                "8": _run(1.4),
                "16": _run(1.39, primary="cache_or_memory_latency", bandwidth=True),
            }
        },
    }
    recommendation = recommend_frontier(workloads, (8, 16))
    assert recommendation["primary"] == "proven_dram_bandwidth_plateau"
    assert recommendation["confidence"] == "high"


def test_plan_only_emits_exact_cpu_sets_without_native_runtime() -> None:
    """The host can validate its affinity plan before building or running native code."""
    root = Path(__file__).parents[1]
    completed = subprocess.run(
        [
            sys.executable,
            str(root / "benchmarks" / "bench_concurrency_telemetry.py"),
            "--plan-only",
            "--workers",
            "1,2",
        ],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    payload = json.loads(completed.stdout)
    assert payload["schema_version"] == 3
    assert payload["plan_only"] is True
    assert len(parse_cpu_list(payload["cpu_sets"]["2"])) == 2
    readiness = payload["host_readiness"]
    assert readiness["ready_for_timing_8_16"] is False
    assert readiness["ready_for_full_evidence"] is False
    assert "generic_hardware_counters_disabled" in readiness["evidence_warnings"]


def test_plan_report_can_be_reused_as_exact_affinity_input(tmp_path: Path) -> None:
    """A reviewed plan report is accepted directly for an exact rerun."""
    root = Path(__file__).parents[1]
    script = root / "benchmarks" / "bench_concurrency_telemetry.py"
    plan = tmp_path / "plan.json"
    subprocess.run(
        [
            sys.executable,
            str(script),
            "--plan-only",
            "--workers",
            "1,2",
            "--output",
            str(plan),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--plan-only",
            "--workers",
            "1,2",
            "--cpu-affinity-json",
            str(plan),
        ],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    rerun = json.loads(completed.stdout)
    original = json.loads(plan.read_text(encoding="utf-8"))
    assert rerun["cpu_sets"] == original["cpu_sets"]


def test_devnull_mode_bypasses_atomic_publication(tmp_path: Path) -> None:
    """Explicit /dev/null measurements use the direct native sink successfully."""
    root = Path(__file__).parents[1]
    report_path = tmp_path / "devnull.json"
    subprocess.run(
        [
            sys.executable,
            str(root / "benchmarks" / "bench_concurrency_telemetry.py"),
            "--workers",
            "1,2",
            "--workloads",
            "jsonl_to_jsonl",
            "--rows",
            "20",
            "--columns",
            "2",
            "--memory-mib",
            "64",
            "--warmups",
            "0",
            "--repeats",
            "1",
            "--output-mode",
            "devnull",
            "--output",
            str(report_path),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["workload"]["output_mode"] == "devnull"
    assert report["workloads"]["jsonl_to_jsonl"]["runs"]["2"]["representative_output"] == {
        "batches": 1,
        "rows": 20,
    }


def test_high_core_wide_fixture_activates_32_workers_and_breaks_16(
    tmp_path: Path,
) -> None:
    """Eligible fixed-wide work starts 32 workers and exceeds 16-way activity."""
    if not hasattr(os, "sched_getaffinity") or len(os.sched_getaffinity(0)) < 32:
        pytest.skip("requires 32 visible CPUs")
    root = Path(__file__).parents[1]
    report_path = tmp_path / "high-core.json"
    subprocess.run(
        [
            sys.executable,
            str(root / "benchmarks" / "bench_concurrency_telemetry.py"),
            "--workers",
            "16,32",
            "--workloads",
            "arrow_stream",
            "--rows",
            "8192",
            "--columns",
            "64",
            "--memory-mib",
            "512",
            "--warmups",
            "0",
            "--repeats",
            "1",
            "--output",
            str(report_path),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    run = report_path.read_text(encoding="utf-8")
    payload = json.loads(run)["workloads"]["arrow_stream"]["runs"]["32"]
    native = payload["representative_native"]
    assert native["effective_workers"] == 32
    assert native["counters"]["started_workers"] == 32
    assert 16 < native["counters"]["peak_active_tasks"] <= 32


def test_v35_benchmark_owners_remain_cohesive_and_below_500_lines() -> None:
    """The host protocol stays split by responsibility without large owners."""
    root = Path(__file__).parents[1] / "benchmarks"
    owners = [
        root / "bench_concurrency_telemetry.py",
        root / "concurrency_telemetry_analysis.py",
        root / "concurrency_telemetry_runner.py",
        root / "concurrency_telemetry_support.py",
        root / "concurrency_high_core_suite.py",
        root / "bench_high_core_evidence.py",
    ]
    assert all(len(path.read_text(encoding="utf-8").splitlines()) <= 500 for path in owners)
