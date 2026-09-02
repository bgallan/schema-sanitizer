"""Shared workload dimensions for the single-versus-multi benchmark harness.

It resolves nested workload sizes, applies CPU quotas, validates arguments, and writes
deterministic pipeline fixtures.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, cast

_SOURCE_SHAPES = ("scalar", "nested")
_PIPELINE_FORMATS = ("jsonl", "csv", "parquet")


def benchmark_dimensions(
    *,
    rows: int,
    memory_mib: int,
    wide_columns: int,
    nested_depth: int,
    source_count: int,
    parquet_compression: str,
    cpu_quota: int | None,
    warmups: int,
    repeats: int,
    selection: str,
    pipeline_shape: str,
    pipeline_format: str,
) -> dict[str, int | str | None]:
    """Return every requested semantic benchmark dimension in canonical form."""
    return {
        "rows": rows,
        "memory_mib": memory_mib,
        "wide_columns": wide_columns,
        "nested_depth": nested_depth,
        "source_count": source_count,
        "parquet_compression": parquet_compression,
        "cpu_quota": cpu_quota,
        "warmups": warmups,
        "repeats": repeats,
        "selection": selection,
        "pipeline_shape": pipeline_shape,
        "pipeline_format": pipeline_format,
    }


def validate_benchmark_dimensions(
    report: object,
    expected: dict[str, int | str | None],
) -> None:
    """Require a child report to echo the exact requested dimension contract."""
    actual = report.get("dimensions") if isinstance(report, dict) else None
    try:
        actual_canonical = json.dumps(actual, allow_nan=False, sort_keys=True)
        expected_canonical = json.dumps(expected, allow_nan=False, sort_keys=True)
    except (TypeError, ValueError) as error:
        raise RuntimeError("benchmark report dimensions are not canonical JSON") from error
    if actual_canonical != expected_canonical:
        raise RuntimeError(
            "benchmark dimension contract mismatch: "
            f"expected={expected_canonical}, actual={actual_canonical}"
        )


def expected_benchmark_case_names(
    selection: str,
    *,
    pipeline_shape: str = "all",
    pipeline_format: str = "all",
) -> tuple[str, ...]:
    """Return the exact ordered case-name contract for one benchmark selection."""
    if selection not in {"all", "pipeline", "parquet"}:
        raise ValueError(f"unsupported benchmark selection: {selection}")
    if pipeline_shape not in {"all", *_SOURCE_SHAPES}:
        raise ValueError(f"unsupported pipeline shape: {pipeline_shape}")
    if pipeline_format not in {"all", *_PIPELINE_FORMATS}:
        raise ValueError(f"unsupported pipeline format: {pipeline_format}")

    names: list[str] = []
    if selection == "all":
        names.extend(("inference_jsonl_scalar", "inference_jsonl_nested"))
        names.extend(
            f"output_{format_name}_{shape}"
            for format_name in ("jsonl", "csv")
            for shape in _SOURCE_SHAPES
        )
    if selection in {"all", "parquet"}:
        names.extend(f"output_parquet_{shape}" for shape in ("scalar", "wide_scalar", "nested"))

    source_shapes = _SOURCE_SHAPES if pipeline_shape == "all" else (pipeline_shape,)
    requested_format = "parquet" if selection == "parquet" else pipeline_format
    formats = _PIPELINE_FORMATS if requested_format == "all" else (requested_format,)
    names.extend(
        f"pipeline_{shape}_jsonl_to_{format_name}"
        for shape in source_shapes
        for format_name in formats
    )
    return tuple(names)


def validate_benchmark_case_results(
    cases: object,
    *,
    selection: str,
    pipeline_shape: str = "all",
    pipeline_format: str = "all",
) -> dict[str, dict[str, Any]]:
    """Return results that exactly match the case and strict-equivalence contract."""
    if not isinstance(cases, dict):
        raise RuntimeError("benchmark report cases must be an object")
    for name, result in cases.items():
        if not isinstance(name, str) or not isinstance(result, dict):
            raise RuntimeError("benchmark case names and results must be objects")
        equivalent = result.get("equivalent")
        if type(equivalent) is not bool:
            raise RuntimeError(f"{name}: benchmark equivalence must be a boolean")
        if equivalent is not True:
            raise RuntimeError(f"{name}: benchmark reported a cross-mode mismatch")

    expected = set(
        expected_benchmark_case_names(
            selection,
            pipeline_shape=pipeline_shape,
            pipeline_format=pipeline_format,
        )
    )
    actual = set(cases)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise RuntimeError(
            f"benchmark case contract mismatch: missing={missing!r}, extra={extra!r}"
        )
    return cast(dict[str, dict[str, Any]], cases)


def nested_value(index: int, depth: int) -> dict[str, Any]:
    """Return one deterministic object with the requested nesting depth."""
    value: dict[str, Any] = {
        "rank": index % 17,
        "label": str(index % 9),
        "flags": [index % 2 == 0, index % 3 == 0],
    }
    for level in range(depth):
        value = {
            "level": level,
            "name": f"row-{index}-level-{level}",
            "child": value,
            "items": [
                {"kind": "a", "value": index + level},
                {"kind": "b", "value": index + level + 1},
            ],
        }
    return value


def apply_cpu_quota(cpu_quota: int | None) -> int | None:
    """Restrict this benchmark process to an explicit CPU affinity when supported."""
    if cpu_quota is None:
        return None
    if hasattr(os, "sched_getaffinity") and hasattr(os, "sched_setaffinity"):
        available = sorted(os.sched_getaffinity(0))
        if cpu_quota > len(available):
            raise ValueError(
                f"cpu-quota {cpu_quota} exceeds the {len(available)} CPUs in current affinity"
            )
        os.sched_setaffinity(0, set(available[:cpu_quota]))
        return len(os.sched_getaffinity(0))
    if os.name == "nt":
        import ctypes

        if cpu_quota > 64:
            raise ValueError("Windows benchmark affinity supports at most 64 CPUs")
        mask = (1 << cpu_quota) - 1
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        process = kernel32.GetCurrentProcess()
        if not kernel32.SetProcessAffinityMask(process, ctypes.c_size_t(mask)):
            raise OSError(ctypes.get_last_error(), "SetProcessAffinityMask failed")
        return cpu_quota
    detected = os.cpu_count() or 1
    if cpu_quota < detected:
        raise RuntimeError("cpu-quota affinity is unsupported on this platform")
    return detected


def benchmark_argument_error(args: Any) -> str | None:
    """Return a validation error for shared benchmark dimensions, if any."""
    invalid = (
        args.rows <= 0
        or args.memory_mib <= 0
        or args.wide_columns < 2
        or args.nested_depth <= 0
        or args.source_count <= 0
        or args.source_count > args.rows
        or (args.cpu_quota is not None and args.cpu_quota <= 0)
        or args.warmups < 0
        or args.repeats <= 0
    )
    if not invalid:
        return None
    return (
        "rows, memory-mib, nested-depth, source-count, cpu-quota, and repeats "
        "must be positive; wide-columns must be >= 2; source-count must not "
        "exceed rows; warmups must be non-negative"
    )


def _write_json_source(path: Path, payload: str) -> None:
    """Write one deterministic JSONL source outside benchmark timing."""
    path.write_text(payload + "\n", encoding="utf-8", newline="")


def write_pipeline_source(
    directory: Path,
    *,
    shape: str,
    payload: str,
    source_count: int,
) -> tuple[Path, str]:
    """Write one file or a deterministic directory of ordered JSONL sources."""
    if source_count == 1:
        source = directory / f"pipeline-{shape}-source.jsonl"
        _write_json_source(source, payload)
        return source, "single_file"
    root = directory / f"pipeline-{shape}-sources"
    root.mkdir()
    lines = payload.splitlines()
    for source_ordinal in range(source_count):
        first = len(lines) * source_ordinal // source_count
        last = len(lines) * (source_ordinal + 1) // source_count
        _write_json_source(
            root / f"part-{source_ordinal:04d}.jsonl",
            "\n".join(lines[first:last]),
        )
    return root, "directory"
