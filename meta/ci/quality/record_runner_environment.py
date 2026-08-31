"""Record comparable, non-secret platform-runner evidence for CI timings.

It gathers normalized operating-system, CPU, memory, toolchain, and runner metadata
without recording secrets.
"""

from __future__ import annotations

import importlib.util
import json
import os
import platform
import re
import sys
from collections.abc import Callable
from importlib import metadata
from pathlib import Path
from types import ModuleType

_CGROUP_VIEW: ModuleType | None = None
_MAX_SIGNED_64 = (1 << 63) - 1


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


def _strict_nonnegative_integer(raw: str) -> int | None:
    """Parse one canonical unsigned decimal within the signed 64-bit range."""
    if re.fullmatch(r"[0-9]+", raw) is None:
        return None
    value = int(raw, 10)
    return value if value <= _MAX_SIGNED_64 else None


def _linux_cpu_quota_capacity(cpu_max: str | None) -> int | None:
    """Convert a cgroup-v2 CPU quota into a conservative whole-CPU capacity."""
    if cpu_max is None:
        return None
    fields = cpu_max.split()
    if len(fields) != 2 or fields[0] == "max":
        return None
    quota = _strict_nonnegative_integer(fields[0])
    period = _strict_nonnegative_integer(fields[1])
    if quota is None or period is None:
        return None
    if quota <= 0 or period <= 0:
        return None
    return max(1, quota // period)


def _cpuset_capacity(raw: str | None) -> int | None:
    """Count one canonical Linux cpuset list without allocating per-CPU state."""
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    count = 0
    previous_end = -1
    for field in text.split(","):
        endpoints = field.split("-", 1)
        start = _strict_nonnegative_integer(endpoints[0])
        end = _strict_nonnegative_integer(endpoints[-1])
        if start is None or end is None:
            return None
        if start < 0 or end < start or start <= previous_end:
            return None
        count += end - start + 1
        previous_end = end
    return count or None


def _minimum_known_capacity(
    values: tuple[str, ...] | None,
    parser: Callable[[str], int | None],
    *,
    empty_is_inherited: bool = False,
) -> tuple[bool, int | None]:
    """Return whether a hierarchy is valid and its smallest finite capacity."""
    if values is None:
        return False, None
    finite: list[int] = []
    for raw in values:
        if empty_is_inherited and not raw.strip():
            continue
        capacity = parser(raw)
        if capacity is None:
            return False, None
        finite.append(capacity)
    return True, min(finite) if finite else None


def _v2_quota_hierarchy_capacity(values: tuple[str, ...] | None) -> tuple[bool, int | None]:
    """Parse every visible cgroup-v2 cpu.max value."""
    if values is None:
        return False, None
    finite: list[int] = []
    for raw in values:
        fields = raw.split()
        if len(fields) != 2:
            return False, None
        if fields[0] == "max":
            period = _strict_nonnegative_integer(fields[1])
            if period is None or period <= 0:
                return False, None
            continue
        capacity = _linux_cpu_quota_capacity(raw)
        if capacity is None:
            return False, None
        finite.append(capacity)
    return True, min(finite) if finite else None


def _v1_quota_hierarchy_capacity(
    quotas: tuple[str, ...] | None,
    periods: tuple[str, ...] | None,
) -> tuple[bool, int | None]:
    """Parse aligned cgroup-v1 quota/period ancestor values."""
    if quotas is None or periods is None or len(quotas) != len(periods):
        return False, None
    finite: list[int] = []
    for raw_quota, raw_period in zip(quotas, periods, strict=True):
        quota = -1 if raw_quota == "-1" else _strict_nonnegative_integer(raw_quota)
        period = _strict_nonnegative_integer(raw_period)
        if quota is None or period is None:
            return False, None
        if period <= 0 or quota == 0:
            return False, None
        if quota == -1:
            continue
        finite.append(max(1, quota // period))
    return True, min(finite) if finite else None


def _load_cgroup_view() -> ModuleType:
    """Load the project's hardened cgroup resolver without importing the package."""
    global _CGROUP_VIEW
    if _CGROUP_VIEW is not None:
        return _CGROUP_VIEW
    source = Path(__file__).resolve().parents[3] / "src/schema_sanitizer/core_impl/cgroup_view.py"
    spec = importlib.util.spec_from_file_location("_schema_sanitizer_ci_cgroup_view", source)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load cgroup resolver: {source}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    _CGROUP_VIEW = module
    return module


def _linux_cgroup_cpu_capacity() -> int | None:
    """Resolve nested v2/v1 CPU quotas and cpusets for the current process."""
    try:
        cgroup_view = _load_cgroup_view()
        before = cgroup_view._sample_membership_before()
        view = cgroup_view.current_cgroup_view(refresh=True)
        if not view.resolution_known:
            return 1
        if view.version == 0:
            return None

        cpu_version = view.controller_version("cpu")
        if not view.hierarchy_is_complete(controller="cpu"):
            return 1
        if cpu_version == 2:
            quota_known, quota_capacity = _v2_quota_hierarchy_capacity(
                cgroup_view.read_cgroup_hierarchy_texts("cpu.max", controller="cpu")
            )
        else:
            quota_known, quota_capacity = _v1_quota_hierarchy_capacity(
                cgroup_view.read_cgroup_hierarchy_texts("cpu.cfs_quota_us", controller="cpu"),
                cgroup_view.read_cgroup_hierarchy_texts("cpu.cfs_period_us", controller="cpu"),
            )
        if not quota_known:
            return 1

        cpuset_version = view.controller_version("cpuset")
        if not view.hierarchy_is_complete(controller="cpuset"):
            return 1
        cpuset_name = "cpuset.cpus.effective" if cpuset_version == 2 else "cpuset.cpus"
        cpuset_known, cpuset_capacity = _minimum_known_capacity(
            cgroup_view.read_cgroup_hierarchy_texts(cpuset_name, controller="cpuset"),
            _cpuset_capacity,
            empty_is_inherited=cpuset_version == 1,
        )
        if not cpuset_known or not cgroup_view._membership_sample_stable(before):
            return 1
    except (ImportError, OSError, RuntimeError, ValueError):
        return 1

    limits = [value for value in (quota_capacity, cpuset_capacity) if value is not None]
    return min(limits) if limits else None


def _effective_cpu_capacity(
    logical_count: int | None,
    affinity_count: int | None,
    linux_cgroup_capacity: int | None,
) -> int:
    """Combine one coherent CPU snapshot into a positive capacity."""
    candidates = [max(1, logical_count or 1)]
    if affinity_count is not None:
        candidates.append(max(1, affinity_count))
    if linux_cgroup_capacity is not None:
        candidates.append(max(1, linux_cgroup_capacity))
    return min(candidates)


def effective_cpu_capacity() -> int:
    """Return a positive CPU bound from hardware, affinity, and cgroup quota."""
    affinity_count: int | None = None
    if hasattr(os, "sched_getaffinity"):
        try:
            affinity_count = len(os.sched_getaffinity(0))
        except OSError:
            pass
    linux_cgroup_capacity = (
        _linux_cgroup_cpu_capacity() if sys.platform.startswith("linux") else None
    )
    return _effective_cpu_capacity(os.cpu_count(), affinity_count, linux_cgroup_capacity)


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
    linux_cgroup_capacity = _linux_cgroup_cpu_capacity() if is_linux else None
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
                linux_cgroup_capacity,
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
