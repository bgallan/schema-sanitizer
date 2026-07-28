"""Regression coverage for paired 8/16 evidence and two-profile suites."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from benchmarks.bench_high_core_evidence import (
    _resume_profile,
    _write_profile_meta,
)
from benchmarks.concurrency_high_core_suite import recommend_suite_frontier
from benchmarks.concurrency_telemetry_analysis import (
    paired_gain_evidence,
    recommend_frontier,
)


def _run(seconds: list[float], *, frontend_share: float = 0.0) -> dict:
    """Build one synthetic isolated-run report with robust timing metadata."""
    median = sorted(seconds)[len(seconds) // 2]
    deviations = sorted(abs(value - median) for value in seconds)
    mad = deviations[len(deviations) // 2]
    return {
        "median_wall_seconds": median,
        "sample_records": [
            {"round_index": index, "wall_seconds": value} for index, value in enumerate(seconds)
        ],
        "timing_stability": {
            "samples": len(seconds),
            "median_seconds": median,
            "mad_seconds": mad,
            "relative_mad": mad / median,
        },
        "representative_native": {
            "diagnosis": {
                "primary": "worker_compute_or_memory_hierarchy",
                "frontend_share": frontend_share,
                "coordinator_wait_share": 0.0,
            }
        },
        "combined_diagnosis": {
            "primary": "worker_compute_or_memory_hierarchy",
            "memory_bandwidth_proven": False,
        },
    }


def _report(primary: str, action: str, gain: float) -> dict:
    """Build one minimal completed profile report for suite selection."""
    return {
        "next_frontier": {
            "primary": primary,
            "recommended_action": action,
            "evidence": {"full_pipeline_gain_low_to_high": gain},
        }
    }


def test_paired_gain_requires_repeatable_direction() -> None:
    """A stable repeated improvement is accepted as paired evidence."""
    runs = {
        "8": _run([1.00, 1.01, 0.99, 1.00, 1.02, 0.98, 1.00]),
        "16": _run([0.90, 0.91, 0.89, 0.90, 0.91, 0.89, 0.90]),
    }
    evidence = paired_gain_evidence(runs, 8, 16)
    assert evidence["status"] == "paired"
    assert evidence["stable"] is True
    assert evidence["consistent_improvement"] is True
    assert evidence["positive_fraction"] == 1.0


def test_unstable_paired_matrix_blocks_production_recommendation() -> None:
    """High dispersion blocks every production-frontier recommendation."""
    workloads = {
        "arrow_stream": {
            "runs": {
                "8": _run([1.0, 1.3, 0.8, 1.2, 0.9, 1.25, 0.85]),
                "16": _run([0.9, 1.4, 0.7, 1.3, 0.8, 1.35, 0.75]),
            }
        },
        "jsonl_to_jsonl": {
            "runs": {
                "8": _run([2.0, 2.6, 1.6, 2.4, 1.8, 2.5, 1.7]),
                "16": _run([1.9, 2.7, 1.5, 2.5, 1.7, 2.6, 1.6]),
            }
        },
    }
    recommendation = recommend_frontier(workloads, (8, 16))
    assert recommendation["primary"] == "measurement_unstable"
    assert recommendation["recommended_action"] == (
        "repeat_interleaved_isolated_matrix_before_production_changes"
    )


def test_suite_prefers_sustained_profile_when_short_scales_but_long_plateaus() -> None:
    """A stable sustained plateau takes precedence over a short-run gain."""
    short = _report(
        "scaling_still_useful",
        "retain_current_architecture_and_measure_larger_sustained_inputs",
        0.08,
    )
    sustained = _report(
        "proven_dram_bandwidth_plateau",
        "reduce_memory_traffic_then_compare_fixed_numa_placement",
        0.01,
    )
    recommendation = recommend_suite_frontier(short, sustained)
    assert recommendation["primary"] == "sustained_only_plateau"
    assert recommendation["recommended_action"] == (
        "reduce_memory_traffic_then_compare_fixed_numa_placement"
    )


def test_stable_high_width_regression_is_not_called_a_plateau() -> None:
    """A repeatable slowdown selects topology and contention investigation."""
    workloads = {
        "arrow_stream": {
            "runs": {
                "8": _run([1.00, 1.01, 0.99, 1.00, 1.02, 0.98, 1.00]),
                "16": _run([1.10, 1.11, 1.09, 1.10, 1.12, 1.08, 1.10]),
            }
        },
        "jsonl_to_jsonl": {
            "runs": {
                "8": _run([1.40, 1.41, 1.39, 1.40, 1.42, 1.38, 1.40]),
                "16": _run([1.54, 1.55, 1.53, 1.54, 1.56, 1.52, 1.54]),
            }
        },
    }
    recommendation = recommend_frontier(workloads, (8, 16))
    assert recommendation["primary"] == "high_width_regression"
    assert recommendation["recommended_action"] == (
        "inspect_smt_numa_frequency_and_contention_before_code_changes"
    )


def test_suite_preserves_stable_sustained_regression() -> None:
    """The cross-profile decision does not average away a long-run slowdown."""
    short = _report(
        "scaling_still_useful",
        "retain_current_architecture_and_measure_larger_sustained_inputs",
        0.08,
    )
    sustained = _report(
        "high_width_regression",
        "inspect_smt_numa_frequency_and_contention_before_code_changes",
        -0.10,
    )
    recommendation = recommend_suite_frontier(short, sustained)
    assert recommendation["primary"] == "sustained_high_width_regression"
    assert recommendation["recommended_action"] == (
        "inspect_smt_numa_frequency_and_contention_before_code_changes"
    )


def test_resume_requires_identical_fingerprint_and_command(tmp_path: Path) -> None:
    """Profile reuse is rejected across revisions or command changes."""
    output = tmp_path / "short.json"
    output.write_text('{"next_frontier": {}}\n', encoding="utf-8")
    command = ["python", "benchmark.py", "--rows", "20000"]
    _write_profile_meta(output, fingerprint="revision-a", command=command)

    assert _resume_profile(output, fingerprint="revision-a", command=command) == {
        "next_frontier": {}
    }
    assert _resume_profile(output, fingerprint="revision-b", command=command) is None
    assert (
        _resume_profile(output, fingerprint="revision-a", command=[*command, "--hardware-counters"])
        is None
    )


def test_high_core_suite_plan_locks_one_affinity_for_both_profiles(
    tmp_path: Path,
) -> None:
    """Both profile commands reuse one host-validated exact affinity plan."""
    root = Path(__file__).parents[1]
    completed = subprocess.run(
        [
            sys.executable,
            str(root / "benchmarks" / "bench_high_core_evidence.py"),
            "--workers",
            "1,2",
            "--short-rows",
            "100",
            "--sustained-rows",
            "200",
            "--repeats",
            "5",
            "--plan-only",
            "--allow-low-core",
            "--allow-unbound",
            "--no-hardware-counters",
            "--output-dir",
            str(tmp_path),
        ],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    report = json.loads(completed.stdout)
    short_command = report["profile_commands"]["short"]["command"]
    sustained_command = report["profile_commands"]["sustained"]["command"]
    short_plan = short_command[short_command.index("--cpu-affinity-json") + 1]
    sustained_plan = sustained_command[sustained_command.index("--cpu-affinity-json") + 1]

    assert report["plan_only"] is True
    assert short_plan == sustained_plan
    assert Path(short_plan).exists()
    assert (tmp_path / "suite.json").exists()
    assert (tmp_path / "suite.md").exists()
