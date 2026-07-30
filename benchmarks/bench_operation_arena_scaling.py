#!/usr/bin/env python3
"""Measure whole-pipeline scaling while preserving one operation worker budget."""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

PIPELINE_CASES = (
    "pipeline_scalar_jsonl_to_csv",
    "pipeline_scalar_jsonl_to_jsonl",
    "pipeline_scalar_jsonl_to_parquet",
    "pipeline_nested_jsonl_to_csv",
    "pipeline_nested_jsonl_to_jsonl",
    "pipeline_nested_jsonl_to_parquet",
)


def _parse_worker_counts(raw: str) -> tuple[int, ...]:
    """Return sorted unique positive worker counts from one CLI value."""
    counts = tuple(sorted({int(item.strip()) for item in raw.split(",") if item.strip()}))
    if not counts or counts[0] <= 0:
        raise ValueError("workers must contain positive integers")
    return counts


def _run_worker_case(
    workers: int,
    *,
    rows: int,
    sources: int,
    memory_mib: int,
    warmups: int,
    repeats: int,
    pipeline_shape: str,
    pipeline_format: str,
    selected_cases: tuple[str, ...],
    directory: Path,
) -> dict[str, Any]:
    """Run the existing verified benchmark under one process CPU affinity."""
    output = directory / f"workers-{workers}.json"
    command = [
        sys.executable,
        str(Path(__file__).with_name("bench_threading_modes.py")),
        "--rows",
        str(rows),
        "--source-count",
        str(sources),
        "--memory-mib",
        str(memory_mib),
        "--cpu-quota",
        str(workers),
        "--warmups",
        str(warmups),
        "--repeats",
        str(repeats),
        "--only",
        "pipeline",
        "--pipeline-shape",
        pipeline_shape,
        "--pipeline-format",
        pipeline_format,
        "--output",
        str(output),
    ]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
    report = json.loads(output.read_text(encoding="utf-8"))
    selected = {name: report["cases"][name] for name in selected_cases}
    if not all(bool(result["equivalent"]) for result in selected.values()):
        raise RuntimeError(f"workers={workers}: logical output mismatch")
    return {
        "requested_workers": workers,
        "applied_cpu_quota": report["environment"]["applied_cpu_quota"],
        "effective_workers": report["environment"]["effective_workers_multi"],
        "cases": selected,
    }


def _summarize(
    results: dict[int, dict[str, Any]], selected_cases: tuple[str, ...]
) -> dict[str, Any]:
    """Compute speedup against the one-worker multi baseline for each pipeline."""
    baseline = results[min(results)]
    summaries: dict[str, Any] = {}
    for name in selected_cases:
        baseline_seconds = float(baseline["cases"][name]["multi_seconds"])
        curve = []
        previous_seconds: float | None = None
        previous_effective_workers: int | None = None
        for workers, result in sorted(results.items()):
            seconds = float(result["cases"][name]["multi_seconds"])
            effective_workers = int(result["effective_workers"])
            marginal_speedup = None if previous_seconds is None else previous_seconds / seconds
            added_workers = (
                None
                if previous_effective_workers is None
                else effective_workers - previous_effective_workers
            )
            gain_per_added_worker = (
                None
                if marginal_speedup is None or added_workers is None or added_workers <= 0
                else (marginal_speedup - 1.0) / added_workers
            )
            curve.append(
                {
                    "workers": workers,
                    "effective_workers": effective_workers,
                    "seconds": seconds,
                    "speedup_vs_one_worker": baseline_seconds / seconds,
                    "marginal_speedup_vs_previous": marginal_speedup,
                    "added_effective_workers": added_workers,
                    "speedup_gain_per_added_worker": gain_per_added_worker,
                }
            )
            previous_seconds = seconds
            previous_effective_workers = effective_workers
        marginal_points = [
            point["marginal_speedup_vs_previous"]
            for point in curve
            if point["marginal_speedup_vs_previous"] is not None
        ]
        summaries[name] = {
            "curve": curve,
            "best_speedup": max(point["speedup_vs_one_worker"] for point in curve),
            "median_seconds": statistics.median(point["seconds"] for point in curve),
            "monotonic_non_regression": all(value >= 1.0 for value in marginal_points),
            "minimum_marginal_speedup": min(marginal_points, default=1.0),
        }
    return summaries


def run(args: argparse.Namespace) -> dict[str, Any]:
    """Execute every affinity in a fresh process and return one verified report."""
    worker_counts = _parse_worker_counts(args.workers)
    selected_cases = tuple(
        name
        for name in PIPELINE_CASES
        if (args.pipeline_shape == "all" or f"pipeline_{args.pipeline_shape}_" in name)
        and (args.pipeline_format == "all" or name.endswith(f"_to_{args.pipeline_format}"))
    )
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-arena-scaling-") as raw:
        directory = Path(raw)
        results = {
            workers: _run_worker_case(
                workers,
                rows=args.rows,
                sources=args.sources,
                memory_mib=args.memory_mib,
                warmups=args.warmups,
                repeats=args.repeats,
                pipeline_shape=args.pipeline_shape,
                pipeline_format=args.pipeline_format,
                selected_cases=selected_cases,
                directory=directory,
            )
            for workers in worker_counts
        }
    return {
        "schema_version": 1,
        "worker_counts": list(worker_counts),
        "rows": args.rows,
        "sources": args.sources,
        "memory_mib": args.memory_mib,
        "warmups": args.warmups,
        "repeats": args.repeats,
        "pipeline_shape": args.pipeline_shape,
        "pipeline_format": args.pipeline_format,
        "logical_outputs_equivalent": True,
        "runs": {str(key): value for key, value in sorted(results.items())},
        "pipelines": _summarize(results, selected_cases),
    }


def main() -> None:
    """Parse controls, execute the scaling matrix, and emit JSON."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", default="1,2,4,8")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--sources", type=int, default=8)
    parser.add_argument("--memory-mib", type=int, default=256)
    parser.add_argument("--pipeline-shape", choices=("all", "scalar", "nested"), default="all")
    parser.add_argument(
        "--pipeline-format", choices=("all", "csv", "jsonl", "parquet"), default="all"
    )
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (
        args.rows <= 0
        or args.sources <= 0
        or args.sources > args.rows
        or args.memory_mib <= 0
        or args.warmups < 0
        or args.repeats <= 0
    ):
        parser.error("rows, sources, memory and repeats must be positive")
    try:
        report = run(args)
    except ValueError as error:
        parser.error(str(error))
    encoded = json.dumps(report, indent=2, sort_keys=True)
    print(encoded)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
