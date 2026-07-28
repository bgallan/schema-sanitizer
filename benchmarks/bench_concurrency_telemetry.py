#!/usr/bin/env python3
"""Run evidence-driven concurrency scaling with fixed CPU/NUMA placement."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_SOURCE_ROOT = _REPOSITORY_ROOT / "src"
for _path in (_REPOSITORY_ROOT, _SOURCE_ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from benchmarks.concurrency_telemetry_analysis import (  # noqa: E402
    build_curve,
    classify_run,
    dram_entry,
    load_dram_payload,
    markdown_summary,
    recommend_frontier,
)
from benchmarks.concurrency_telemetry_runner import (  # noqa: E402
    child_report,
    run_perf_child,
    run_timing_matrix,
)
from benchmarks.concurrency_telemetry_support import (  # noqa: E402
    format_cpu_list,
    host_snapshot,
    load_cpu_sets,
)

_DEFAULT_PERF_EVENTS = (
    "task-clock,cycles,instructions,cache-references,cache-misses,"
    "branches,branch-misses,context-switches,cpu-migrations,page-faults"
)
_WORKLOADS = frozenset({"arrow_stream", "jsonl_to_jsonl"})


def _parse_positive_csv(raw: str, *, name: str) -> tuple[int, ...]:
    """Return sorted unique positive integers from a comma-separated option."""
    values = tuple(sorted({int(item.strip()) for item in raw.split(",") if item.strip()}))
    if not values or values[0] <= 0:
        raise ValueError(f"{name} must contain positive integers")
    return values


def _parse_workloads(raw: str) -> tuple[str, ...]:
    """Return ordered unique workload identifiers."""
    values = tuple(dict.fromkeys(item.strip() for item in raw.split(",") if item.strip()))
    unknown = set(values) - _WORKLOADS
    if not values or unknown:
        raise ValueError(f"workloads must be a subset of {sorted(_WORKLOADS)}")
    return values


def _write_fixture(path: Path, *, rows: int, columns: int) -> None:
    """Write deterministic wide scalar JSONL outside measured execution."""
    names = tuple(f"column_{index:04d}" for index in range(columns))
    with path.open("w", encoding="utf-8", newline="") as handle:
        for row_index in range(rows):
            row = {name: row_index + ordinal for ordinal, name in enumerate(names)}
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")


def _collect_workload(
    args: argparse.Namespace,
    worker_counts: tuple[int, ...],
    cpu_sets: dict[int, tuple[int, ...]],
    workload: str,
    dram_payload: dict[str, Any],
    directory: Path,
) -> dict[str, Any]:
    """Collect timing, generic counters, and combined diagnosis for one workload."""
    runs = run_timing_matrix(args, worker_counts, cpu_sets, workload, directory)
    for count in worker_counts:
        perf = (
            run_perf_child(args, count, cpu_sets[count], workload, directory)
            if args.hardware_counters
            else {"status": "disabled", "counters": {}}
        )
        representative = runs[str(count)]["representative_native"]
        runs[str(count)]["hardware"] = perf
        runs[str(count)]["combined_diagnosis"] = classify_run(
            representative, perf, dram_entry(dram_payload, workload, count)
        )
    return {"curve": build_curve(runs, worker_counts), "runs": runs}


def _dram_high_width_coverage(
    payload: dict[str, Any],
    workloads: tuple[str, ...],
    worker_counts: tuple[int, ...],
) -> dict[str, Any]:
    """Report whether every workload has a high-width DRAM measurement."""
    high = max(worker_counts)
    missing = [workload for workload in workloads if dram_entry(payload, workload, high) is None]
    return {
        "high_workers": high,
        "complete": not missing,
        "missing_workloads": missing,
    }


def _host_readiness(
    host: dict[str, Any],
    worker_counts: tuple[int, ...],
    args: argparse.Namespace,
    *,
    dram_supplied: bool,
) -> dict[str, Any]:
    """Summarize timing readiness separately from full diagnostic evidence."""
    timing_warnings: list[str] = []
    evidence_warnings: list[str] = []
    maximum = max(worker_counts)
    visible = len(host.get("cpu_affinity", []))
    if maximum < 16:
        timing_warnings.append("matrix_does_not_include_16_workers")
    if visible < maximum:
        timing_warnings.append("visible_affinity_is_smaller_than_requested_matrix")
    if args.numa_node is not None and host.get("numactl") is None:
        timing_warnings.append("numactl_unavailable_for_memory_binding")
    if args.hardware_counters and host.get("perf") is None:
        evidence_warnings.append("perf_requested_but_unavailable")
    paranoid = host.get("perf_event_paranoid")
    if args.hardware_counters and paranoid is not None:
        try:
            if int(paranoid) > 2:
                evidence_warnings.append("perf_event_paranoid_may_block_counters")
        except ValueError:
            evidence_warnings.append("perf_event_paranoid_unreadable")
    if len(host.get("numa_nodes", {})) > 1 and args.numa_node is None:
        timing_warnings.append("multi_numa_host_without_fixed_node")
    if not args.hardware_counters:
        evidence_warnings.append("generic_hardware_counters_disabled")
    if not dram_supplied:
        evidence_warnings.append("platform_dram_measurement_and_baseline_missing")
    timing_ready = maximum >= 16 and visible >= 16 and not timing_warnings
    return {
        "ready_for_8_16": timing_ready,
        "ready_for_timing_8_16": timing_ready,
        "ready_for_full_evidence": timing_ready and not evidence_warnings,
        "timing_warnings": timing_warnings,
        "evidence_warnings": evidence_warnings,
        "warnings": [*timing_warnings, *evidence_warnings],
    }


def _parent_report(args: argparse.Namespace) -> dict[str, Any]:
    """Build a plan or execute the complete host matrix."""
    worker_counts = _parse_positive_csv(args.workers, name="workers")
    workloads = _parse_workloads(args.workloads)
    if args.require_high_core and max(worker_counts) < 16:
        raise ValueError("--require-high-core requires a matrix containing 16 workers")
    cpu_sets = load_cpu_sets(args.cpu_affinity_json, worker_counts, node=args.numa_node)
    dram_payload = load_dram_payload(args.dram_bandwidth_json)
    dram_coverage = _dram_high_width_coverage(dram_payload, workloads, worker_counts)
    host = host_snapshot()
    base: dict[str, Any] = {
        "schema_version": 3,
        "host": host,
        "host_readiness": _host_readiness(
            host,
            worker_counts,
            args,
            dram_supplied=bool(dram_coverage["complete"]),
        ),
        "sampling_mode": args.sampling_mode,
        "numa_node": args.numa_node,
        "cpu_sets": {str(count): format_cpu_list(cpu_sets[count]) for count in worker_counts},
        "workload": {
            "format": "wide_scalar_jsonl",
            "rows": args.rows,
            "columns": args.columns,
            "memory_mib": args.memory_mib,
            "output_mode": args.output_mode,
            "warmups_per_process": args.warmups,
            "samples_per_affinity": args.repeats,
        },
        "workers": list(worker_counts),
        "hardware_counters_requested": args.hardware_counters,
        "dram_baseline_supplied": bool(dram_payload),
        "dram_high_width_coverage": dram_coverage,
    }
    if args.plan_only:
        return {**base, "plan_only": True, "workloads": {}}

    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-telemetry-") as raw:
        directory = Path(raw)
        collected = {
            workload: _collect_workload(
                args,
                worker_counts,
                cpu_sets,
                workload,
                dram_payload,
                directory,
            )
            for workload in workloads
        }
    return {
        **base,
        "plan_only": False,
        "workloads": collected,
        "next_frontier": recommend_frontier(collected, worker_counts),
    }


def _parser() -> argparse.ArgumentParser:
    """Build the public and hidden child CLI."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", default="1,2,4,8,16")
    parser.add_argument("--workloads", default="arrow_stream,jsonl_to_jsonl")
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--columns", type=int, default=128)
    parser.add_argument("--memory-mib", type=int, default=256)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--sampling-mode",
        choices=("grouped", "interleaved-isolated"),
        default="interleaved-isolated",
    )
    parser.add_argument("--output-mode", choices=("devnull", "file"), default="file")
    parser.add_argument("--hardware-counters", action="store_true")
    parser.add_argument("--perf-events", default=_DEFAULT_PERF_EVENTS)
    parser.add_argument("--numa-node", type=int)
    parser.add_argument("--require-numa-binding", action="store_true")
    parser.add_argument("--require-high-core", action="store_true")
    parser.add_argument("--cpu-affinity-json", type=Path)
    parser.add_argument("--dram-bandwidth-json", type=Path)
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--summary-output", type=Path)
    parser.add_argument("--fixture", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--child-workers", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--child-cpus", help=argparse.SUPPRESS)
    parser.add_argument("--child-workload", help=argparse.SUPPRESS)
    parser.add_argument("--child-output", type=Path, help=argparse.SUPPRESS)
    return parser


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Reject invalid dimensions before creating fixtures or child processes."""
    if (
        args.rows <= 0
        or args.columns < 2
        or args.memory_mib <= 0
        or args.warmups < 0
        or args.repeats <= 0
    ):
        parser.error("rows, columns, memory and repeats must be positive")


def _emit_report(args: argparse.Namespace, report: dict[str, Any]) -> None:
    """Print and optionally persist JSON and Markdown outputs."""
    encoded = json.dumps(report, indent=2, sort_keys=True)
    print(encoded)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")
    if args.summary_output is not None:
        args.summary_output.parent.mkdir(parents=True, exist_ok=True)
        args.summary_output.write_text(markdown_summary(report), encoding="utf-8")


def main() -> None:
    """Parse arguments and emit a machine-readable host matrix."""
    parser = _parser()
    args = parser.parse_args()
    _validate_args(parser, args)
    args.benchmark_script = Path(__file__).resolve()

    if args.child_workers is not None:
        if not all((args.fixture, args.child_output, args.child_cpus, args.child_workload)):
            parser.error("internal child mode requires fixture, CPUs, workload and output")
        report = child_report(args)
        args.child_output.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return

    try:
        if args.plan_only:
            report = _parent_report(args)
        else:
            with tempfile.TemporaryDirectory(prefix="schema-sanitizer-fixture-") as raw:
                fixture = Path(raw) / "wide.jsonl"
                _write_fixture(fixture, rows=args.rows, columns=args.columns)
                args.fixture = fixture
                report = _parent_report(args)
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        parser.error(str(error))
    _emit_report(args, report)


if __name__ == "__main__":
    main()
