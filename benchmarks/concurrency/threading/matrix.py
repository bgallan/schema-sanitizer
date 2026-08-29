#!/usr/bin/env python3
"""Run a reproducible multidimensional single-versus-multi benchmark matrix.

It expands workload cases, launches each isolated mode, and emits a platform-stamped
JSON matrix.
"""

from __future__ import annotations

import argparse
import json
import platform
import shlex
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

_REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from benchmarks.support.command import DISCARD, run_command  # noqa: E402


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


def _case_command(
    case: MatrixCase,
    *,
    rows: int,
    warmups: int,
    repeats: int,
    selection: str,
    directory: Path,
) -> tuple[list[str], Path]:
    """Return one isolated benchmark argv and its report path."""
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
    return command, output


def _load_case(case: MatrixCase, output: Path) -> dict[str, Any]:
    """Load one completed child report and verify logical equivalence."""
    report = json.loads(output.read_text(encoding="utf-8"))
    if not all(bool(result.get("equivalent")) for result in report["cases"].values()):
        raise RuntimeError(f"{case.label}: benchmark reported a cross-mode mismatch")
    return report


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
    command, output = _case_command(
        case,
        rows=rows,
        warmups=warmups,
        repeats=repeats,
        selection=selection,
        directory=directory,
    )
    run_command(command, check=True, stdout=DISCARD)
    return _load_case(case, output)


def _report(args: argparse.Namespace, directory: Path) -> dict[str, Any]:
    """Aggregate already completed case reports."""
    cases = _cases(args.profile)
    results = {
        case.label: {
            "dimensions": asdict(case),
            "report": _load_case(case, directory / f"{case.label}.json"),
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


def run_matrix(args: argparse.Namespace) -> dict[str, Any]:
    """Execute every selected dimension in a fresh child process."""
    cases = _cases(args.profile)
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-threading-matrix-") as raw:
        directory = Path(raw)
        for case in cases:
            _run_case(
                case,
                rows=args.rows,
                warmups=args.warmups,
                repeats=args.repeats,
                selection=args.only,
                directory=directory,
            )
        return _report(args, directory)


def _shell_plan(args: argparse.Namespace, work_root: Path) -> str:
    """Return a same-job shell plan for GitHub Actions."""
    work_root.mkdir(parents=True, exist_ok=True)
    directory = Path(
        tempfile.mkdtemp(prefix="schema-sanitizer-threading-matrix-", dir=work_root)
    ).resolve()
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"matrix_root={shlex.quote(directory.as_posix())}",
        'cleanup_matrix() { rm -rf -- "${matrix_root}"; }',
        "trap cleanup_matrix EXIT",
    ]
    for case in _cases(args.profile):
        command, _output = _case_command(
            case,
            rows=args.rows,
            warmups=args.warmups,
            repeats=args.repeats,
            selection=args.only,
            directory=directory,
        )
        lines.append(shlex.join(command) + " > /dev/null")
    aggregate = [
        sys.executable,
        "-m",
        "benchmarks.concurrency.threading.matrix",
        "--profile",
        args.profile,
        "--rows",
        str(args.rows),
        "--warmups",
        str(args.warmups),
        "--repeats",
        str(args.repeats),
        "--only",
        args.only,
        "--assemble-root",
        directory.as_posix(),
    ]
    if args.output is not None:
        aggregate.extend(("--output", args.output.as_posix()))
    lines.append(shlex.join(aggregate))
    return "\n".join(lines) + "\n"


def main() -> None:
    """Parse matrix controls, execute children, and write one report."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=("ci", "standard", "full"), default="standard")
    parser.add_argument("--rows", type=int, default=120_000)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--only", choices=("all", "parquet"), default="all")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--work-root", type=Path)
    parser.add_argument("--command-output", type=Path)
    parser.add_argument("--assemble-root", type=Path)
    args = parser.parse_args()
    if args.rows <= 0 or args.warmups < 0 or args.repeats <= 0:
        parser.error("rows and repeats must be positive; warmups must be non-negative")

    if (args.work_root is None) != (args.command_output is None):
        parser.error("work-root and command-output must be provided together")
    if args.command_output is not None:
        args.command_output.parent.mkdir(parents=True, exist_ok=True)
        args.command_output.write_text(
            _shell_plan(args, args.work_root), encoding="utf-8", newline="\n"
        )
        return

    report = (
        _report(args, args.assemble_root) if args.assemble_root is not None else run_matrix(args)
    )
    encoded = json.dumps(report, indent=2, sort_keys=True)
    print(encoded)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
