"""Shared workload dimensions for the single-versus-multi benchmark harness.

It resolves nested workload sizes, applies CPU quotas, validates arguments, and writes
deterministic pipeline fixtures.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


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
