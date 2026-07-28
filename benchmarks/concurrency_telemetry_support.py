"""Host, Arrow C Stream, and matrix helpers for concurrency telemetry benchmarks."""

from __future__ import annotations

import ctypes
import json
import os
import platform
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, Iterable


def parse_cpu_list(raw: str) -> tuple[int, ...]:
    """Parse Linux cpulist syntax into sorted unique CPU identifiers."""
    cpus: set[int] = set()
    for item in raw.strip().split(","):
        token = item.strip()
        if not token:
            continue
        if "-" in token:
            first_raw, last_raw = token.split("-", 1)
            first, last = int(first_raw), int(last_raw)
            if first < 0 or last < first:
                raise ValueError(f"invalid CPU range: {token!r}")
            cpus.update(range(first, last + 1))
        else:
            cpu = int(token)
            if cpu < 0:
                raise ValueError(f"invalid CPU identifier: {token!r}")
            cpus.add(cpu)
    if not cpus:
        raise ValueError("CPU list must not be empty")
    return tuple(sorted(cpus))


def format_cpu_list(cpus: Iterable[int]) -> str:
    """Format sorted CPU identifiers using compact Linux cpulist syntax."""
    values = sorted(set(int(cpu) for cpu in cpus))
    if not values:
        return ""
    ranges: list[str] = []
    start = previous = values[0]
    for cpu in values[1:]:
        if cpu == previous + 1:
            previous = cpu
            continue
        ranges.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = cpu
    ranges.append(str(start) if start == previous else f"{start}-{previous}")
    return ",".join(ranges)


def current_affinity() -> tuple[int, ...]:
    """Return the process CPU affinity, falling back to all detected CPUs."""
    if hasattr(os, "sched_getaffinity"):
        return tuple(sorted(os.sched_getaffinity(0)))
    return tuple(range(os.cpu_count() or 1))


def apply_exact_affinity(cpus: tuple[int, ...]) -> tuple[int, ...]:
    """Apply an exact CPU affinity and return the affinity observed afterwards."""
    if not cpus:
        raise ValueError("an exact CPU affinity must contain at least one CPU")
    if hasattr(os, "sched_setaffinity") and hasattr(os, "sched_getaffinity"):
        available = set(os.sched_getaffinity(0))
        if not set(cpus).issubset(available):
            raise ValueError(
                f"requested CPU set {format_cpu_list(cpus)} is outside inherited "
                f"affinity {format_cpu_list(available)}"
            )
        os.sched_setaffinity(0, set(cpus))
        return tuple(sorted(os.sched_getaffinity(0)))
    if os.name == "nt":
        if max(cpus) >= 64:
            raise ValueError("Windows affinity supports one group of at most 64 CPUs")
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        mask_value = sum(1 << cpu for cpu in cpus)
        if not kernel32.SetProcessAffinityMask(
            kernel32.GetCurrentProcess(), ctypes.c_size_t(mask_value)
        ):
            raise OSError(ctypes.get_last_error(), "SetProcessAffinityMask failed")
        return cpus
    if tuple(range(os.cpu_count() or 1)) != cpus:
        raise RuntimeError("exact CPU affinity is unsupported on this platform")
    return cpus


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def numa_nodes() -> dict[int, tuple[int, ...]]:
    """Return NUMA-node CPU membership visible through Linux sysfs."""
    nodes: dict[int, tuple[int, ...]] = {}
    for path in sorted(Path("/sys/devices/system/node").glob("node[0-9]*/cpulist")):
        match = re.fullmatch(r"node(\d+)", path.parent.name)
        raw = _read_text(path)
        if match and raw:
            nodes[int(match.group(1))] = parse_cpu_list(raw)
    return nodes


def _core_key(cpu: int) -> tuple[int, int]:
    root = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
    package = _read_text(root / "physical_package_id")
    core = _read_text(root / "core_id")
    return (int(package or 0), int(core or cpu))


def nested_cpu_sets(
    worker_counts: tuple[int, ...], *, node: int | None = None
) -> dict[int, tuple[int, ...]]:
    """Choose nested CPU sets, preferring one logical CPU per physical core."""
    available = set(current_affinity())
    if node is not None:
        nodes = numa_nodes()
        if node not in nodes:
            raise ValueError(f"NUMA node {node} is not visible on this host")
        available.intersection_update(nodes[node])
    if not available:
        raise ValueError("no CPUs remain after applying the requested NUMA node")

    by_core: dict[tuple[int, int], list[int]] = {}
    for cpu in sorted(available):
        by_core.setdefault(_core_key(cpu), []).append(cpu)
    ordered: list[int] = []
    max_siblings = max(len(siblings) for siblings in by_core.values())
    for sibling_index in range(max_siblings):
        for key in sorted(by_core):
            siblings = by_core[key]
            if sibling_index < len(siblings):
                ordered.append(siblings[sibling_index])

    maximum = max(worker_counts)
    if maximum > len(ordered):
        raise ValueError(
            f"workers={maximum} exceeds the {len(ordered)} CPUs available "
            f"in NUMA node {node if node is not None else 'selection'}"
        )
    return {count: tuple(ordered[:count]) for count in worker_counts}


def load_cpu_sets(
    path: Path | None, worker_counts: tuple[int, ...], *, node: int | None
) -> dict[int, tuple[int, ...]]:
    """Load exact CPU sets or derive nested sets from the visible topology."""
    if path is None:
        return nested_cpu_sets(worker_counts, node=node)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("cpu-affinity-json must contain an object")
    if isinstance(payload.get("cpu_sets"), dict):
        payload = payload["cpu_sets"]
    result: dict[int, tuple[int, ...]] = {}
    inherited = set(current_affinity())
    nodes = numa_nodes()
    for count in worker_counts:
        raw = payload.get(str(count))
        if not isinstance(raw, str):
            raise ValueError(f"cpu-affinity-json is missing string key {count!r}")
        cpus = parse_cpu_list(raw)
        if len(cpus) != count:
            raise ValueError(f"CPU set for {count} workers contains {len(cpus)} CPUs")
        if not set(cpus).issubset(inherited):
            raise ValueError(f"CPU set for {count} is outside inherited affinity")
        if node is not None and not set(cpus).issubset(set(nodes.get(node, ()))):
            raise ValueError(f"CPU set for {count} crosses NUMA node {node}")
        result[count] = cpus
    previous: set[int] = set()
    for count in sorted(result):
        current = set(result[count])
        if not previous.issubset(current):
            raise ValueError("CPU sets must be nested as worker counts increase")
        previous = current
    return result


def binding_snapshot() -> dict[str, Any]:
    """Return observable process CPU and memory-node binding state."""
    status = _read_text(Path("/proc/self/status")) or ""
    fields: dict[str, str] = {}
    for line in status.splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            if key in {"Cpus_allowed_list", "Mems_allowed_list"}:
                fields[key] = value.strip()
    numactl_policy = None
    executable = shutil.which("numactl")
    if executable is not None:
        try:
            completed = subprocess.run(
                [executable, "--show"],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=5,
            )
            numactl_policy = completed.stdout.strip() or None
        except (OSError, subprocess.SubprocessError):
            numactl_policy = None
    return {
        "cpu_affinity": list(current_affinity()),
        "cpu_affinity_list": format_cpu_list(current_affinity()),
        "cpus_allowed_list": fields.get("Cpus_allowed_list"),
        "mems_allowed_list": fields.get("Mems_allowed_list"),
        "numactl_policy": numactl_policy,
    }


def host_snapshot() -> dict[str, Any]:
    """Capture stable host facts required to interpret a scaling matrix."""
    governors = {
        value
        for cpu in current_affinity()
        if (value := _read_text(Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor")))
    }
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_affinity": list(current_affinity()),
        "cpu_affinity_list": format_cpu_list(current_affinity()),
        "numa_nodes": {str(node): format_cpu_list(cpus) for node, cpus in numa_nodes().items()},
        "scaling_governors": sorted(governors),
        "perf_event_paranoid": _read_text(Path("/proc/sys/kernel/perf_event_paranoid")),
        "numactl": shutil.which("numactl"),
        "perf": shutil.which("perf"),
    }


class ArrowSchema(ctypes.Structure):
    """Arrow C Data Interface schema layout."""


class ArrowArray(ctypes.Structure):
    """Arrow C Data Interface array layout."""


class ArrowArrayStream(ctypes.Structure):
    """Arrow C Stream Interface layout."""


ArrowSchemaRelease = ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowSchema))
ArrowArrayRelease = ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowArray))
StreamGetSchema = ctypes.CFUNCTYPE(
    ctypes.c_int, ctypes.POINTER(ArrowArrayStream), ctypes.POINTER(ArrowSchema)
)
StreamGetNext = ctypes.CFUNCTYPE(
    ctypes.c_int, ctypes.POINTER(ArrowArrayStream), ctypes.POINTER(ArrowArray)
)
StreamGetLastError = ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.POINTER(ArrowArrayStream))
StreamRelease = ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowArrayStream))

ArrowSchema._fields_ = [
    ("format", ctypes.c_char_p),
    ("name", ctypes.c_char_p),
    ("metadata", ctypes.c_char_p),
    ("flags", ctypes.c_int64),
    ("n_children", ctypes.c_int64),
    ("children", ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
    ("dictionary", ctypes.POINTER(ArrowSchema)),
    ("release", ArrowSchemaRelease),
    ("private_data", ctypes.c_void_p),
]
ArrowArray._fields_ = [
    ("length", ctypes.c_int64),
    ("null_count", ctypes.c_int64),
    ("offset", ctypes.c_int64),
    ("n_buffers", ctypes.c_int64),
    ("n_children", ctypes.c_int64),
    ("buffers", ctypes.POINTER(ctypes.c_void_p)),
    ("children", ctypes.POINTER(ctypes.POINTER(ArrowArray))),
    ("dictionary", ctypes.POINTER(ArrowArray)),
    ("release", ArrowArrayRelease),
    ("private_data", ctypes.c_void_p),
]
ArrowArrayStream._fields_ = [
    ("get_schema", StreamGetSchema),
    ("get_next", StreamGetNext),
    ("get_last_error", StreamGetLastError),
    ("release", StreamRelease),
    ("private_data", ctypes.c_void_p),
]


def _stream_error(stream: ctypes.POINTER(ArrowArrayStream), operation: str) -> RuntimeError:
    message = stream.contents.get_last_error(stream) if stream.contents.get_last_error else None
    detail = message.decode("utf-8", errors="replace") if message else "unknown error"
    return RuntimeError(f"Arrow C Stream {operation} failed: {detail}")


def consume_arrow_c_stream(owner: Any) -> dict[str, int]:
    """Consume and release an Arrow C Stream without PyArrow or serialization."""
    capsule = owner.__arrow_c_stream__()
    get_pointer = ctypes.pythonapi.PyCapsule_GetPointer
    get_pointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    get_pointer.restype = ctypes.c_void_p
    address = get_pointer(capsule, b"arrow_array_stream")
    if not address:
        raise RuntimeError("object did not export a valid arrow_array_stream capsule")
    stream = ctypes.cast(address, ctypes.POINTER(ArrowArrayStream))
    rows = batches = 0
    try:
        schema = ArrowSchema()
        if stream.contents.get_schema(stream, ctypes.byref(schema)) != 0:
            raise _stream_error(stream, "get_schema")
        if schema.release:
            schema.release(ctypes.byref(schema))
        while True:
            array = ArrowArray()
            if stream.contents.get_next(stream, ctypes.byref(array)) != 0:
                raise _stream_error(stream, "get_next")
            if not array.release:
                break
            rows += int(array.length)
            batches += 1
            array.release(ctypes.byref(array))
    finally:
        if stream.contents.release:
            stream.contents.release(stream)
        close_main = getattr(owner, "close_main_stream", None)
        if callable(close_main):
            close_main()
    return {"rows": rows, "batches": batches}


def numactl_prefix(
    *, cpus: tuple[int, ...], node: int | None, require_binding: bool
) -> tuple[list[str], str]:
    """Return a numactl launch prefix and an explicit binding status."""
    if node is None:
        return [], "not_requested"
    executable = shutil.which("numactl")
    if executable is None:
        if require_binding:
            raise RuntimeError("--require-numa-binding requested but numactl is unavailable")
        return [], "requested_but_numactl_unavailable"
    return [
        executable,
        f"--physcpubind={format_cpu_list(cpus)}",
        f"--membind={node}",
    ], "enforced_with_numactl"
