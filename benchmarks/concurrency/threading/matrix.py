#!/usr/bin/env python3
"""Run a reproducible multidimensional single-versus-multi benchmark matrix."""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class MatrixCase:
    """One bounded benchmark dimension combination."""

    label: str
    memory_mib: int
    wide_columns: int
    nested_depth: int
    source_count: int
    compression: str
    cpu_quota: int | None


def _cases(profile: str) -> list[MatrixCase]:
    """Return a focused matrix that varies every requested dimension."""
    baseline = MatrixCase("baseline", 128, 16, 2, 1, "snappy", None)
    if profile == "ci":
        return [
            MatrixCase("ci-baseline", 64, 4, 1, 1, "snappy", None),
            MatrixCase("ci-wide-deep", 96, 24, 3, 2, "gzip", None),
        ]
    if profile == "standard":
        return [
            baseline,
            MatrixCase("width-4", 128, 4, 2, 1, "snappy", None),
            MatrixCase("width-64", 128, 64, 2, 1, "snappy", None),
            MatrixCase("depth-1", 128, 16, 1, 1, "snappy", None),
            MatrixCase("depth-4", 128, 16, 4, 1, "snappy", None),
            MatrixCase("sources-8", 128, 16, 2, 8, "snappy", None),
            MatrixCase("memory-64", 64, 16, 2, 1, "snappy", None),
            MatrixCase("memory-512", 512, 16, 2, 1, "snappy", None),
            MatrixCase("compression-none", 128, 16, 2, 1, "uncompressed", None),
            MatrixCase("compression-gzip", 128, 16, 2, 1, "gzip", None),
        ]
    if profile == "full":
        cases = _cases("standard")
        if sys.platform.startswith("linux") or sys.platform == "win32":
            cases.extend(
                [
                    MatrixCase("cpu-1", 128, 16, 2, 1, "snappy", 1),
                    MatrixCase("cpu-2", 128, 16, 2, 1, "snappy", 2),
                    MatrixCase("cpu-4", 128, 16, 2, 1, "snappy", 4),
                ]
            )
        return cases
    raise ValueError(f"unsupported benchmark profile: {profile}")


def _run_case(
    case: MatrixCase,
    *,
    rows: int,
    warmups: int,
    repeats: int,
    selection: str,
    directory: Path,
) -> dict[str, Any]:
    """Run one child benchmark and return its verified JSON report."""
    output = directory / f"{case.label}.json"
    command = [
        sys.executable,
        "-m",
        "benchmarks.concurrency.threading.modes",
        "--rows",
        str(rows),
        "--memory-mib",
        str(case.memory_mib),
        "--wide-columns",
        str(case.wide_columns),
        "--nested-depth",
        str(case.nested_depth),
        "--source-count",
        str(case.source_count),
        "--parquet-compression",
        case.compression,
        "--warmups",
        str(warmups),
        "--repeats",
        str(repeats),
        "--only",
        selection,
        "--output",
        str(output),
    ]
    if case.cpu_quota is not None:
        command.extend(("--cpu-quota", str(case.cpu_quota)))
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
    report = json.loads(output.read_text(encoding="utf-8"))
    if not all(bool(result.get("equivalent")) for result in report["cases"].values()):
        raise RuntimeError(f"{case.label}: benchmark reported a cross-mode mismatch")
    return report


def run_matrix(args: argparse.Namespace) -> dict[str, Any]:
    """Execute every selected dimension in a fresh child process."""
    cases = _cases(args.profile)
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-threading-matrix-") as raw:
        directory = Path(raw)
        results = {
            case.label: {
                "dimensions": asdict(case),
                "report": _run_case(
                    case,
                    rows=args.rows,
                    warmups=args.warmups,
                    repeats=args.repeats,
                    selection=args.only,
                    directory=directory,
                ),
            }
            for case in cases
        }
    return {
        "schema_version": 1,
        "profile": args.profile,
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "rows": args.rows,
        "warmups": args.warmups,
        "repeats": args.repeats,
        "selection": args.only,
        "cases": results,
        "logical_outputs_equivalent": True,
    }


def main() -> None:
    """Parse matrix controls, execute children, and write one report."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=("ci", "standard", "full"), default="standard")
    parser.add_argument("--rows", type=int, default=120_000)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--only", choices=("all", "parquet"), default="all")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.rows <= 0 or args.warmups < 0 or args.repeats <= 0:
        parser.error("rows and repeats must be positive; warmups must be non-negative")

    report = run_matrix(args)
    encoded = json.dumps(report, indent=2, sort_keys=True)
    print(encoded)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
