"""Record comparable, non-secret platform-runner evidence for CI timings.

It gathers normalized operating-system, CPU, memory, toolchain, and runner metadata
without recording secrets.
"""

from __future__ import annotations

import json
import os
import platform
import sys
from importlib import metadata
from pathlib import Path


def _optional_text(path: str) -> str | None:
    """Read a host-control file when the current runner exposes it."""
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return None


def _optional_key_values(path: str) -> dict[str, int] | None:
    """Parse a whitespace-delimited Linux controller file when available."""
    text = _optional_text(path)
    if text is None:
        return None
    values: dict[str, int] = {}
    try:
        for line in text.splitlines():
            key, value = line.split()
            values[key] = int(value)
    except (TypeError, ValueError):
        return None
    return values


def _linux_cpu_quota_capacity(cpu_max: str | None) -> int | None:
    """Convert a cgroup-v2 CPU quota into a positive rounded-up capacity."""
    if cpu_max is None:
        return None
    fields = cpu_max.split()
    if len(fields) != 2 or fields[0] == "max":
        return None
    try:
        quota, period = map(int, fields)
    except ValueError:
        return None
    if quota <= 0 or period <= 0:
        return None
    return max(1, (quota + period - 1) // period)


def _effective_cpu_capacity(
    logical_count: int | None,
    affinity_count: int | None,
    linux_cpu_max: str | None,
) -> int:
    """Combine one coherent CPU snapshot into a positive capacity."""
    candidates = [max(1, logical_count or 1)]
    if affinity_count is not None:
        candidates.append(max(1, affinity_count))
    quota_capacity = _linux_cpu_quota_capacity(linux_cpu_max)
    if quota_capacity is not None:
        candidates.append(quota_capacity)
    return min(candidates)


def effective_cpu_capacity() -> int:
    """Return a positive CPU bound from hardware, affinity, and cgroup quota."""
    affinity_count: int | None = None
    if hasattr(os, "sched_getaffinity"):
        try:
            affinity_count = len(os.sched_getaffinity(0))
        except OSError:
            pass
    linux_cpu_max = (
        _optional_text("/sys/fs/cgroup/cpu.max") if sys.platform.startswith("linux") else None
    )
    return _effective_cpu_capacity(os.cpu_count(), affinity_count, linux_cpu_max)


def bounded_build_parallelism(limit: int = 4) -> int:
    """Cap build parallelism while adapting to the runner's effective CPUs."""
    if limit < 1:
        raise ValueError("build parallelism limit must be positive")
    return min(limit, effective_cpu_capacity())


def runner_environment() -> dict[str, object]:
    """Build evidence without copying environment variables into artifacts."""
    affinity: list[int] | None = None
    if hasattr(os, "sched_getaffinity"):
        try:
            affinity = sorted(os.sched_getaffinity(0))
        except OSError:
            pass
    is_linux = sys.platform.startswith("linux")
    logical_count = os.cpu_count()
    affinity_count = len(affinity) if affinity is not None else None
    linux_cpu_max = _optional_text("/sys/fs/cgroup/cpu.max") if is_linux else None
    return {
        "schema_version": 2,
        "python": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
        },
        "installed_distributions": {
            name: metadata.version(name)
            for name in (
                "aiohttp",
                "duckdb",
                "pandas",
                "polars",
                "pyarrow",
                "pytest",
                "schema-sanitizer",
            )
        },
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "cpu": {
            "logical_count": logical_count,
            "affinity": affinity,
            "affinity_count": affinity_count,
            "effective_count": _effective_cpu_capacity(
                logical_count,
                affinity_count,
                linux_cpu_max,
            ),
            "linux_cgroup_v2_cpu_max": linux_cpu_max,
            "linux_cgroup_v2_cpu_stat": (
                _optional_key_values("/sys/fs/cgroup/cpu.stat") if is_linux else None
            ),
        },
    }


def main(argv: list[str] | None = None) -> int:
    """Write the runner evidence JSON to the requested output path."""
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 1:
        print("usage: record_runner_environment.py OUTPUT", file=sys.stderr)
        return 2
    output = Path(arguments[0])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(runner_environment(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(output.read_text(encoding="utf-8"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
