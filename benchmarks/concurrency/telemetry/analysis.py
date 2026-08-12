"""Evidence combination and reporting for concurrency telemetry matrices."""

from __future__ import annotations

import json
import math
import statistics
from pathlib import Path
from typing import Any


def parse_perf_stat(path: Path) -> dict[str, Any]:
    """Parse ``perf stat -x ';'`` output and aggregate repeated event rows."""
    counters: dict[str, float] = {}
    unavailable: dict[str, str] = {}
    if not path.exists():
        return {"status": "not_collected", "counters": counters}
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split(";")
        if len(fields) < 3:
            continue
        raw_value, unit, raw_event = (
            fields[0].strip(),
            fields[1].strip(),
            fields[2].strip(),
        )
        event = raw_event.removesuffix(":u").removesuffix(":k")
        if raw_value.startswith("<"):
            unavailable[event] = raw_value
            continue
        try:
            value = float(raw_value.replace(" ", "").replace(",", ""))
        except ValueError:
            unavailable[event] = raw_value
            continue
        if unit in {"msec", "ms"}:
            value /= 1_000.0
        counters[event] = counters.get(event, 0.0) + value
    return {
        "status": "collected" if counters else "unavailable",
        "counters": counters,
        "unavailable": unavailable,
    }


def load_dram_payload(path: Path | None) -> dict[str, Any]:
    """Load optional platform DRAM measurements in legacy or workload form."""
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("dram-bandwidth-json must contain an object")
    for workload, mapping in _workload_mappings(payload).items():
        if not isinstance(mapping, dict):
            raise ValueError(f"DRAM mapping for {workload!r} must be an object")
        for workers, entry in mapping.items():
            if not isinstance(workers, str) or not workers.isdigit() or int(workers) <= 0:
                raise ValueError("DRAM bandwidth keys must be positive worker counts")
            if not isinstance(entry, dict):
                raise ValueError("each DRAM bandwidth entry must be an object")
            for field in ("measured_gib_s", "sustainable_gib_s"):
                value = entry.get(field)
                if not isinstance(value, (int, float)) or value <= 0:
                    raise ValueError(f"{field} must be positive")
    return payload


def _workload_mappings(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    if all(str(key).isdigit() for key in payload):
        return {"*": payload}
    return payload


def dram_entry(payload: dict[str, Any], workload: str, workers: int) -> dict[str, Any] | None:
    """Return one DRAM entry, accepting the legacy worker-only shape."""
    mappings = _workload_mappings(payload)
    mapping = mappings.get(workload, mappings.get("*", {}))
    entry = mapping.get(str(workers)) if isinstance(mapping, dict) else None
    return entry if isinstance(entry, dict) else None


def derived_hardware(perf: dict[str, Any], workers: int) -> dict[str, Any]:
    """Derive portable ratios from generic perf counters."""
    counters = perf.get("counters", {})
    cycles = float(counters.get("cycles", 0.0))
    instructions = float(counters.get("instructions", 0.0))
    cache_references = float(counters.get("cache-references", 0.0))
    cache_misses = float(counters.get("cache-misses", 0.0))
    branches = float(counters.get("branches", 0.0))
    branch_misses = float(counters.get("branch-misses", 0.0))
    task_clock_seconds = float(counters.get("task-clock", 0.0))
    return {
        "ipc": instructions / cycles if cycles > 0 else None,
        "cache_miss_ratio": (cache_misses / cache_references if cache_references > 0 else None),
        "branch_miss_ratio": branch_misses / branches if branches > 0 else None,
        "task_clock_seconds": task_clock_seconds or None,
        "task_clock_seconds_per_worker": (
            task_clock_seconds / workers if task_clock_seconds > 0 else None
        ),
        "context_switches": counters.get("context-switches"),
        "cpu_migrations": counters.get("cpu-migrations"),
        "page_faults": counters.get("page-faults"),
    }


def classify_run(
    native: dict[str, Any], perf: dict[str, Any], dram: dict[str, Any] | None
) -> dict[str, Any]:
    """Combine native and hardware evidence without overclaiming DRAM saturation."""
    native_diagnosis = native.get("diagnosis", {})
    hardware = derived_hardware(perf, int(native.get("effective_workers", 1)))
    measured = float((dram or {}).get("measured_gib_s", 0.0))
    sustainable = float((dram or {}).get("sustainable_gib_s", 0.0))
    dram_ratio = measured / sustainable if measured > 0 and sustainable > 0 else None
    bandwidth_proven = dram_ratio is not None and dram_ratio >= 0.85

    primary = str(native_diagnosis.get("primary", "mixed_or_unresolved"))
    confidence = str(native_diagnosis.get("confidence", "low"))
    ipc = hardware["ipc"]
    cache_miss_ratio = hardware["cache_miss_ratio"]
    if bandwidth_proven:
        primary = "dram_bandwidth_saturation"
        confidence = "high"
    elif primary == "mixed_or_unresolved" and ipc is not None:
        if ipc < 1.0 and cache_miss_ratio is not None and cache_miss_ratio >= 0.15:
            primary = "cache_or_memory_latency"
            confidence = "medium"
        elif ipc >= 1.5:
            primary = "worker_compute"
            confidence = "medium"

    return {
        "primary": primary,
        "confidence": confidence,
        "memory_capacity_pressure": bool(native_diagnosis.get("memory_capacity_pressure", False)),
        "memory_bandwidth_proven": bandwidth_proven,
        "dram_bandwidth_ratio": dram_ratio,
        "dram_status": (
            "saturated_against_supplied_baseline"
            if bandwidth_proven
            else "unresolved_without_platform_dram_measurement_and_baseline"
        ),
        "hardware_ratios": hardware,
    }


def _sample_map(run: dict[str, Any]) -> dict[int, float]:
    """Return isolated sample wall times keyed by interleaving round."""
    records = run.get("sample_records", [])
    if not isinstance(records, list):
        return {}
    result: dict[int, float] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        if record.get("order_direction") == "grouped":
            continue
        round_index = record.get("round_index")
        wall_seconds = record.get("wall_seconds")
        if isinstance(round_index, int) and isinstance(wall_seconds, (int, float)):
            result[round_index] = float(wall_seconds)
    return result


def paired_gain_evidence(runs: dict[str, Any], low: int, high: int) -> dict[str, Any]:
    """Describe low/high scaling with paired isolated samples when available."""
    low_run = runs.get(str(low), {})
    high_run = runs.get(str(high), {})
    low_samples = _sample_map(low_run)
    high_samples = _sample_map(high_run)
    common = sorted(set(low_samples).intersection(high_samples))
    summary_gain = _gain(runs, low, high)
    if not common:
        return {
            "status": "summary_only",
            "pairs": 0,
            "median_gain": summary_gain,
            "positive_fraction": None,
            "near_zero_fraction": None,
            "stable": None,
            "consistent_improvement": None,
            "consistent_plateau": None,
            "consistent_regression": None,
        }

    gains = [low_samples[index] / high_samples[index] - 1.0 for index in common]
    median_gain = statistics.median(gains)
    positive_fraction = sum(gain > 0 for gain in gains) / len(gains)
    near_zero_fraction = sum(abs(gain) <= 0.05 for gain in gains) / len(gains)
    low_relative_mad = low_run.get("timing_stability", {}).get("relative_mad")
    high_relative_mad = high_run.get("timing_stability", {}).get("relative_mad")
    dispersion_known = isinstance(low_relative_mad, (int, float)) and isinstance(
        high_relative_mad, (int, float)
    )
    stable = bool(
        len(gains) >= 5
        and dispersion_known
        and float(low_relative_mad) <= 0.05
        and float(high_relative_mad) <= 0.05
    )
    return {
        "status": "paired",
        "pairs": len(gains),
        "median_gain": median_gain,
        "minimum_gain": min(gains),
        "maximum_gain": max(gains),
        "positive_fraction": positive_fraction,
        "near_zero_fraction": near_zero_fraction,
        "low_relative_mad": low_relative_mad,
        "high_relative_mad": high_relative_mad,
        "stable": stable,
        "consistent_improvement": bool(
            stable and median_gain >= 0.05 and positive_fraction >= 0.75
        ),
        "consistent_plateau": bool(
            stable and abs(median_gain) < 0.03 and near_zero_fraction >= 0.75
        ),
        "consistent_regression": bool(
            stable and median_gain <= -0.03 and positive_fraction <= 0.25
        ),
    }


def build_curve(runs: dict[str, Any], worker_counts: tuple[int, ...]) -> list[dict[str, Any]]:
    """Build one timing curve from completed runs."""
    baseline = float(runs[str(worker_counts[0])]["median_wall_seconds"])
    return [
        {
            "workers": count,
            "seconds": float(runs[str(count)]["median_wall_seconds"]),
            "speedup_vs_first": baseline / float(runs[str(count)]["median_wall_seconds"]),
            "relative_mad": runs[str(count)].get("timing_stability", {}).get("relative_mad"),
            "samples": runs[str(count)].get("timing_stability", {}).get("samples"),
            "diagnosis": runs[str(count)]["combined_diagnosis"]["primary"],
        }
        for count in worker_counts
    ]


def _gain(runs: dict[str, Any], low: int, high: int) -> float | None:
    if str(low) not in runs or str(high) not in runs:
        return None
    low_s = float(runs[str(low)]["median_wall_seconds"])
    high_s = float(runs[str(high)]["median_wall_seconds"])
    return low_s / high_s - 1.0 if high_s > 0 else None


def _native_diagnosis(runs: dict[str, Any], workers: int) -> dict[str, Any]:
    return runs.get(str(workers), {}).get("representative_native", {}).get("diagnosis", {})


def recommend_frontier(workloads: dict[str, Any], worker_counts: tuple[int, ...]) -> dict[str, Any]:
    """Choose the next experiment from paired Arrow-only and full-pipeline evidence."""
    low, high = worker_counts[-2:]
    arrow_runs = workloads.get("arrow_stream", {}).get("runs", {})
    full_runs = workloads.get("jsonl_to_jsonl", {}).get("runs", {})
    adjacent_gains = [
        {
            "low_workers": adjacent_low,
            "high_workers": adjacent_high,
            "arrow_gain": _gain(arrow_runs, adjacent_low, adjacent_high),
            "full_pipeline_gain": _gain(full_runs, adjacent_low, adjacent_high),
        }
        for adjacent_low, adjacent_high in zip(worker_counts, worker_counts[1:], strict=False)
    ]
    arrow_gain = _gain(arrow_runs, low, high)
    full_gain = _gain(full_runs, low, high)
    arrow_paired = paired_gain_evidence(arrow_runs, low, high)
    full_paired = paired_gain_evidence(full_runs, low, high)
    paired_available = arrow_paired["status"] == "paired" and full_paired["status"] == "paired"
    paired_stable = bool(paired_available and arrow_paired["stable"] and full_paired["stable"])
    high_full = full_runs.get(str(high), {})
    high_arrow = arrow_runs.get(str(high), {})
    full_native = high_full.get("representative_native", {})
    diagnosis = full_native.get("diagnosis", {})
    full_seconds = float(high_full.get("median_wall_seconds", 0.0))
    arrow_seconds = float(high_arrow.get("median_wall_seconds", 0.0))
    output_gap = max(0.0, full_seconds - arrow_seconds)
    output_gap_share = output_gap / full_seconds if full_seconds > 0 else None
    dram_proven = bool(
        high_full.get("combined_diagnosis", {}).get("memory_bandwidth_proven")
        or high_arrow.get("combined_diagnosis", {}).get("memory_bandwidth_proven")
    )
    high_primary = str(
        high_full.get("combined_diagnosis", {}).get("primary", "mixed_or_unresolved")
    )
    frontend_share = float(diagnosis.get("frontend_share", 0.0))
    wait_share = float(diagnosis.get("coordinator_wait_share", 0.0))

    primary = "measurement_incomplete"
    action = "collect_both_workloads_at_the_same_affinities"
    confidence = "low"
    if arrow_gain is not None and full_gain is not None:
        if paired_available and not paired_stable:
            primary = "measurement_unstable"
            action = "repeat_interleaved_isolated_matrix_before_production_changes"
            confidence = "high"
        elif (arrow_gain <= -0.03 or full_gain <= -0.03) and (
            not paired_available
            or arrow_paired["consistent_regression"]
            or full_paired["consistent_regression"]
        ):
            primary = "high_width_regression"
            action = "inspect_smt_numa_frequency_and_contention_before_code_changes"
            confidence = "high"
        elif (
            arrow_gain >= 0.05
            and full_gain >= 0.05
            and (
                not paired_available
                or (
                    arrow_paired["consistent_improvement"] and full_paired["consistent_improvement"]
                )
            )
        ):
            primary = "scaling_still_useful"
            action = "retain_current_architecture_and_measure_larger_sustained_inputs"
            confidence = "high"
        elif (
            arrow_gain >= 0.05
            and full_gain < 0.03
            and (output_gap_share or 0.0) >= 0.20
            and (
                not paired_available
                or (arrow_paired["consistent_improvement"] and full_paired["consistent_plateau"])
            )
        ):
            primary = "output_path_plateau"
            action = "prototype_partitioned_or_pipelined_output"
            confidence = "high"
        elif (
            frontend_share >= 0.35
            and full_gain < 0.03
            and (not paired_available or full_paired["consistent_plateau"])
        ):
            primary = "frontend_input_plateau"
            action = "prototype_bounded_worker_local_json_preflight"
            confidence = "high" if frontend_share >= 0.55 else "medium"
        elif (
            dram_proven
            and max(arrow_gain, full_gain) < 0.03
            and (
                not paired_available
                or (arrow_paired["consistent_plateau"] and full_paired["consistent_plateau"])
            )
        ):
            primary = "proven_dram_bandwidth_plateau"
            action = "reduce_memory_traffic_then_compare_fixed_numa_placement"
            confidence = "high"
        elif high_primary == "cache_or_memory_latency" and not dram_proven:
            primary = "cache_or_latency_plateau"
            action = "profile_cache_lines_allocator_and_working_set_before_numa_changes"
            confidence = "medium"
        elif (
            wait_share >= 0.25
            and full_gain < 0.03
            and (not paired_available or full_paired["consistent_plateau"])
        ):
            primary = "coordination_or_imbalance_plateau"
            action = "reduce_ordered_wait_or_worker_imbalance"
            confidence = "medium"
        else:
            primary = "mixed_plateau_unresolved"
            action = "collect_platform_dram_and_cache_counters_before_code_changes"
            confidence = "low"

    return {
        "comparison": {"low_workers": low, "high_workers": high},
        "adjacent_gains": adjacent_gains,
        "primary": primary,
        "recommended_action": action,
        "confidence": confidence,
        "evidence": {
            "arrow_gain_low_to_high": arrow_gain,
            "full_pipeline_gain_low_to_high": full_gain,
            "output_gap_share_at_high": output_gap_share,
            "frontend_share_at_high": frontend_share,
            "coordinator_wait_share_at_high": wait_share,
            "dram_bandwidth_proven_at_high": dram_proven,
            "full_pipeline_high_diagnosis": high_primary,
            "paired_samples_available": paired_available,
            "paired_samples_stable": paired_stable if paired_available else None,
            "arrow_paired_gain": arrow_paired,
            "full_pipeline_paired_gain": full_paired,
        },
    }


def _format_optional(value: Any, *, percent: bool = False) -> str:
    if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        return "n/a"
    return f"{100.0 * value:.1f}%" if percent else f"{value:.3f}"


def markdown_summary(report: dict[str, Any]) -> str:
    """Render a compact, reviewable summary beside the machine-readable report."""
    lines = ["# Concurrency telemetry host matrix", ""]
    host = report.get("host", {})
    lines.extend(
        [
            f"- Affinity visible to runner: `{host.get('cpu_affinity_list', 'unknown')}`",
            f"- NUMA nodes: `{json.dumps(host.get('numa_nodes', {}), sort_keys=True)}`",
            f"- Sampling: `{report.get('sampling_mode', 'unknown')}`",
            "",
        ]
    )
    for name, workload in report.get("workloads", {}).items():
        lines.extend(
            [
                f"## {name}",
                "",
                "| workers | median s | speedup | rel. MAD | diagnosis |",
                "|---:|---:|---:|---:|---|",
            ]
        )
        for point in workload.get("curve", []):
            lines.append(
                f"| {point['workers']} | {point['seconds']:.6f} | "
                f"{point['speedup_vs_first']:.2f}x | "
                f"{_format_optional(point.get('relative_mad'), percent=True)} | "
                f"{point['diagnosis']} |"
            )
        lines.append("")
    recommendation = report.get("next_frontier", {})
    evidence = recommendation.get("evidence", {})
    lines.extend(
        [
            "## Evidence-based next frontier",
            "",
            f"**{recommendation.get('primary', 'unresolved')}** — "
            f"`{recommendation.get('recommended_action', 'collect_more_evidence')}`",
            "",
            "- Arrow gain: "
            f"{_format_optional(evidence.get('arrow_gain_low_to_high'), percent=True)}",
            "- Full-pipeline gain: "
            f"{_format_optional(evidence.get('full_pipeline_gain_low_to_high'), percent=True)}",
            "- Output gap at high width: "
            f"{_format_optional(evidence.get('output_gap_share_at_high'), percent=True)}",
            "- Frontend share at high width: "
            f"{_format_optional(evidence.get('frontend_share_at_high'), percent=True)}",
            f"- DRAM saturation proven: {bool(evidence.get('dram_bandwidth_proven_at_high'))}",
            f"- Paired samples stable: {evidence.get('paired_samples_stable')}",
            "",
        ]
    )
    return "\n".join(lines)
