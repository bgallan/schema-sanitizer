"""Operation and subprocess runners for concurrency telemetry benchmarks."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from benchmarks.concurrency_telemetry_analysis import parse_perf_stat
from benchmarks.concurrency_telemetry_support import (
    apply_exact_affinity,
    binding_snapshot,
    consume_arrow_c_stream,
    format_cpu_list,
    numactl_prefix,
    parse_cpu_list,
)


def run_operation(
    source: Path,
    output: Path,
    *,
    workload: str,
    expected_rows: int,
    memory_limit_bytes: int,
) -> tuple[float, dict[str, Any], dict[str, int]]:
    """Run one complete operation and return wall time, telemetry, and output stats."""
    from schema_sanitizer.api_impl.execution_context import ExecutionContext
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_jsonl_native_first_stream,
    )
    from schema_sanitizer.api_impl.stream_output import write_raw_stream_to_file
    from schema_sanitizer.core_impl.native_symbols import JSONL_STREAM_WRITE
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    options = normalize_call_options(
        threading_mode="multi",
        memory_limit_bytes=memory_limit_bytes,
        on_error="stop",
    )
    context = ExecutionContext()
    sink = None
    started = time.perf_counter_ns()
    try:
        sink = context.to_sink(
            source,
            sink="stream",
            options=options,
            format="jsonl",
            source="path",
        )
        if workload == "arrow_stream":
            output_stats = consume_arrow_c_stream(sink.raw)
        elif output == Path(os.devnull):
            native_output_stats = JSONL_STREAM_WRITE(
                sink.raw,
                os.fspath(output),
                memory_limit_bytes,
                1,
            )
            output_stats = {
                "rows": int(native_output_stats.get("materialized_rows", 0)),
                "batches": int(native_output_stats.get("batches", 0)),
            }
        else:
            write_raw_stream_to_file(
                sink.raw,
                output,
                writer=write_jsonl_native_first_stream,
                feature="concurrency telemetry benchmark",
                first_row_columns=None,
                memory_limit_bytes=memory_limit_bytes,
                threading_mode="multi",
            )
            output_stats = {"rows": expected_rows, "batches": 0}
    finally:
        if sink is not None:
            sink.close()
    elapsed_seconds = (time.perf_counter_ns() - started) / 1_000_000_000.0
    report = context.performance_stats()
    if not report.get("finished"):
        raise RuntimeError("native telemetry did not reach a completed state")
    if output_stats["rows"] != expected_rows:
        raise RuntimeError(
            f"workload consumed {output_stats['rows']} rows, expected {expected_rows}"
        )
    return elapsed_seconds, report, output_stats


def child_report(args: argparse.Namespace) -> dict[str, Any]:
    """Execute one workload/affinity in an isolated process."""
    cpus = parse_cpu_list(args.child_cpus)
    applied = apply_exact_affinity(cpus)
    if len(applied) != args.child_workers:
        raise RuntimeError("child affinity width differs from requested worker count")
    memory_limit_bytes = args.memory_mib * 1024 * 1024
    output = (
        Path(os.devnull)
        if args.output_mode == "devnull"
        else args.child_output.parent / f"output-{args.child_workload}-{args.child_workers}.jsonl"
    )
    for _ in range(args.warmups):
        run_operation(
            args.fixture,
            output,
            workload=args.child_workload,
            expected_rows=args.rows,
            memory_limit_bytes=memory_limit_bytes,
        )

    samples: list[dict[str, Any]] = []
    for _ in range(args.repeats):
        elapsed, native, output_stats = run_operation(
            args.fixture,
            output,
            workload=args.child_workload,
            expected_rows=args.rows,
            memory_limit_bytes=memory_limit_bytes,
        )
        samples.append({"wall_seconds": elapsed, "native": native, "output": output_stats})
    ordered = sorted(samples, key=lambda sample: float(sample["wall_seconds"]))
    representative = ordered[len(ordered) // 2]
    return {
        "requested_workers": args.child_workers,
        "applied_workers": len(applied),
        "cpu_set": list(applied),
        "cpu_set_list": format_cpu_list(applied),
        "workload": args.child_workload,
        "binding": binding_snapshot(),
        "wall_seconds": [sample["wall_seconds"] for sample in samples],
        "median_wall_seconds": statistics.median(
            float(sample["wall_seconds"]) for sample in samples
        ),
        "representative_native": representative["native"],
        "representative_output": representative["output"],
    }


def child_command(
    args: argparse.Namespace,
    *,
    workers: int,
    cpus: tuple[int, ...],
    workload: str,
    output: Path,
    warmups: int,
    repeats: int,
) -> list[str]:
    """Build one direct child-process command."""
    return [
        sys.executable,
        str(Path(args.benchmark_script).resolve()),
        "--fixture",
        str(args.fixture),
        "--rows",
        str(args.rows),
        "--memory-mib",
        str(args.memory_mib),
        "--output-mode",
        args.output_mode,
        "--warmups",
        str(warmups),
        "--repeats",
        str(repeats),
        "--child-workers",
        str(workers),
        "--child-cpus",
        format_cpu_list(cpus),
        "--child-workload",
        workload,
        "--child-output",
        str(output),
    ]


def launch_command(
    args: argparse.Namespace,
    *,
    workers: int,
    cpus: tuple[int, ...],
    workload: str,
    output: Path,
    warmups: int,
    repeats: int,
) -> tuple[list[str], str]:
    """Wrap one child in the requested fixed NUMA policy."""
    prefix, binding_status = numactl_prefix(
        cpus=cpus,
        node=args.numa_node,
        require_binding=args.require_numa_binding,
    )
    return (
        [
            *prefix,
            *child_command(
                args,
                workers=workers,
                cpus=cpus,
                workload=workload,
                output=output,
                warmups=warmups,
                repeats=repeats,
            ),
        ],
        binding_status,
    )


def run_child(
    args: argparse.Namespace,
    workers: int,
    cpus: tuple[int, ...],
    workload: str,
    directory: Path,
    *,
    suffix: str = "grouped",
    warmups: int | None = None,
    repeats: int | None = None,
) -> dict[str, Any]:
    """Run one child and load its native report."""
    output = directory / f"native-{workload}-{workers}-{suffix}.json"
    command, binding_status = launch_command(
        args,
        workers=workers,
        cpus=cpus,
        workload=workload,
        output=output,
        warmups=args.warmups if warmups is None else warmups,
        repeats=args.repeats if repeats is None else repeats,
    )
    subprocess.run(command, check=True)
    report = json.loads(output.read_text(encoding="utf-8"))
    report["numa_binding_status"] = binding_status
    return report


def _robust_timing_stats(values: list[float]) -> dict[str, float | int | None]:
    """Return robust dispersion metrics without introducing heavy dependencies."""
    if not values:
        return {
            "samples": 0,
            "median_seconds": None,
            "mad_seconds": None,
            "relative_mad": None,
            "minimum_seconds": None,
            "maximum_seconds": None,
        }
    median = statistics.median(values)
    mad = statistics.median(abs(value - median) for value in values)
    return {
        "samples": len(values),
        "median_seconds": median,
        "mad_seconds": mad,
        "relative_mad": mad / median if median > 0 else None,
        "minimum_seconds": min(values),
        "maximum_seconds": max(values),
    }


def _grouped_run_with_stats(report: dict[str, Any]) -> dict[str, Any]:
    """Attach stability metadata to one grouped-process report."""
    values = [float(value) for value in report.get("wall_seconds", [])]
    return {
        **report,
        "sample_records": [
            {
                "round_index": index,
                "order_direction": "grouped",
                "order_position": index,
                "wall_seconds": value,
            }
            for index, value in enumerate(values)
        ],
        "timing_stability": _robust_timing_stats(values),
    }


def run_timing_matrix(
    args: argparse.Namespace,
    worker_counts: tuple[int, ...],
    cpu_sets: dict[int, tuple[int, ...]],
    workload: str,
    directory: Path,
) -> dict[str, Any]:
    """Run grouped or ABBA-style isolated timing samples."""
    if args.sampling_mode == "grouped":
        return {
            str(count): _grouped_run_with_stats(
                run_child(args, count, cpu_sets[count], workload, directory)
            )
            for count in worker_counts
        }

    samples: dict[int, list[dict[str, Any]]] = {count: [] for count in worker_counts}
    records: dict[int, list[dict[str, Any]]] = {count: [] for count in worker_counts}
    for round_index in range(args.repeats):
        ascending = round_index % 2 == 0
        order = worker_counts if ascending else tuple(reversed(worker_counts))
        direction = "ascending" if ascending else "descending"
        for position, count in enumerate(order):
            point = run_child(
                args,
                count,
                cpu_sets[count],
                workload,
                directory,
                suffix=f"round-{round_index}",
                warmups=args.warmups,
                repeats=1,
            )
            samples[count].append(point)
            records[count].append(
                {
                    "round_index": round_index,
                    "order_direction": direction,
                    "order_position": position,
                    "wall_seconds": float(point["median_wall_seconds"]),
                }
            )
    runs: dict[str, Any] = {}
    for count in worker_counts:
        points = samples[count]
        values = [float(point["median_wall_seconds"]) for point in points]
        median = statistics.median(values)
        representative = min(
            points, key=lambda point: abs(float(point["median_wall_seconds"]) - median)
        )
        runs[str(count)] = {
            **representative,
            "wall_seconds": values,
            "median_wall_seconds": median,
            "isolated_processes": len(points),
            "sample_records": records[count],
            "timing_stability": _robust_timing_stats(values),
        }
    return runs


def run_perf_child(
    args: argparse.Namespace,
    workers: int,
    cpus: tuple[int, ...],
    workload: str,
    directory: Path,
) -> dict[str, Any]:
    """Collect generic perf counters around one isolated operation."""
    perf = shutil.which("perf")
    if perf is None:
        return {"status": "perf_not_found", "counters": {}}
    perf_output = directory / f"perf-{workload}-{workers}.txt"
    child_output = directory / f"perf-native-{workload}-{workers}.json"
    direct, binding_status = launch_command(
        args,
        workers=workers,
        cpus=cpus,
        workload=workload,
        output=child_output,
        warmups=0,
        repeats=1,
    )
    command = [
        perf,
        "stat",
        "-x",
        ";",
        "-o",
        str(perf_output),
        "-e",
        args.perf_events,
        "--",
        *direct,
    ]
    completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    parsed = parse_perf_stat(perf_output)
    parsed["returncode"] = completed.returncode
    parsed["numa_binding_status"] = binding_status
    if completed.returncode != 0:
        parsed["status"] = "permission_denied_or_unsupported"
        parsed["stderr"] = completed.stderr.decode("utf-8", errors="replace")[-2_000:]
    return parsed
