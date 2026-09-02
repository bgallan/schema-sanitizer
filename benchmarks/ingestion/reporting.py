"""Machine-readable reporting helpers for ingestion benchmarks.

It captures benchmark records, host CPU and memory metadata, and serializes the final
report as JSON.
"""

from __future__ import annotations

import json
import os
import platform
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class BenchmarkRecord:
    """One summarized benchmark case."""

    label: str
    rows: int
    input_bytes: int | None
    output_bytes: int | None
    warmups: int
    repeats: int
    median_seconds: float
    p95_seconds: float
    median_rows_per_second: float
    median_bytes_per_second: float | None
    process_peak_rss_bytes: int | None
    detail: str


def process_peak_rss_bytes() -> int | None:
    """Return peak resident-set size for the current process when available."""
    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            class ProcessMemoryCounters(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(counters)
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            psapi = ctypes.WinDLL("psapi", use_last_error=True)
            get_current_process = kernel32.GetCurrentProcess
            get_current_process.restype = wintypes.HANDLE
            get_process_memory_info = psapi.GetProcessMemoryInfo
            get_process_memory_info.argtypes = [
                wintypes.HANDLE,
                ctypes.POINTER(ProcessMemoryCounters),
                wintypes.DWORD,
            ]
            get_process_memory_info.restype = wintypes.BOOL
            ok = get_process_memory_info(
                get_current_process(),
                ctypes.byref(counters),
                counters.cb,
            )
            return int(counters.PeakWorkingSetSize) if ok else None
        except (AttributeError, OSError, ValueError):
            return None

    try:
        import resource

        value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        return value if sys.platform == "darwin" else value * 1024
    except (ImportError, OSError, ValueError):
        return None


def cpu_description() -> str:
    """Return a stable best-effort CPU description."""
    value = platform.processor().strip()
    if value:
        return value
    if sys.platform.startswith("linux"):
        try:
            for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
                if line.lower().startswith("model name"):
                    return line.split(":", 1)[1].strip()
        except OSError:
            pass
    return platform.machine() or "unknown"


def write_report(
    path: Path,
    records: list[BenchmarkRecord],
    *,
    fixture_metadata: dict[str, Any],
) -> None:
    """Write benchmark records and execution metadata as deterministic JSON."""
    try:
        import schema_sanitizer as ss

        package_version = ss.__version__
    except (ImportError, AttributeError):
        package_version = "unknown"

    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "package_version": package_version,
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "cpu": cpu_description(),
            "cpu_count": os.cpu_count(),
        },
        "fixture": fixture_metadata,
        "benchmarks": [asdict(record) for record in records],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
