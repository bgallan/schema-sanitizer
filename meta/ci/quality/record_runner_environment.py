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


def runner_environment() -> dict[str, object]:
    """Build evidence without copying environment variables into artifacts."""
    affinity: list[int] | None = None
    if hasattr(os, "sched_getaffinity"):
        try:
            affinity = sorted(os.sched_getaffinity(0))
        except OSError:
            pass
    is_linux = sys.platform.startswith("linux")
    return {
        "schema_version": 1,
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
            "logical_count": os.cpu_count(),
            "affinity": affinity,
            "affinity_count": len(affinity) if affinity is not None else None,
            "linux_cgroup_v2_cpu_max": (
                _optional_text("/sys/fs/cgroup/cpu.max") if is_linux else None
            ),
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
