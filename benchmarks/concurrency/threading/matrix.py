#!/usr/bin/env python3
"""Run a reproducible multidimensional single-versus-multi benchmark matrix.

It expands workload cases, launches each isolated mode, and emits a platform-stamped
JSON matrix.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shlex
import shutil
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

_REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from benchmarks.concurrency.threading.dimensions import (  # noqa: E402
    benchmark_dimensions,
    validate_benchmark_case_results,
    validate_benchmark_dimensions,
)
from benchmarks.support.command import DISCARD, run_command  # noqa: E402

_PLAN_DIRECTORY = "matrix"


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
    pipeline_shape: str,
    pipeline_format: str,
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
        "--pipeline-shape",
        pipeline_shape,
        "--pipeline-format",
        pipeline_format,
        "--output",
        str(output),
    ]
    if case.cpu_quota is not None:
        command.extend(("--cpu-quota", str(case.cpu_quota)))
    return command, output


def _case_dimensions(
    case: MatrixCase,
    *,
    rows: int,
    warmups: int,
    repeats: int,
    selection: str,
    pipeline_shape: str,
    pipeline_format: str,
) -> dict[str, int | str | None]:
    """Return the exact child dimension contract for one matrix case."""
    return benchmark_dimensions(
        rows=rows,
        memory_mib=case.memory_mib,
        wide_columns=case.wide_columns,
        nested_depth=case.nested_depth,
        source_count=case.source_count,
        parquet_compression=case.compression,
        cpu_quota=case.cpu_quota,
        warmups=warmups,
        repeats=repeats,
        selection=selection,
        pipeline_shape=pipeline_shape,
        pipeline_format=pipeline_format,
    )


def _load_case(
    case: MatrixCase,
    output: Path,
    *,
    rows: int,
    warmups: int,
    repeats: int,
    selection: str,
    pipeline_shape: str,
    pipeline_format: str,
) -> dict[str, Any]:
    """Load one child report and verify its dimensions and logical equivalence."""
    if output.is_symlink() or not output.is_file():
        raise RuntimeError(f"{case.label}: missing regular benchmark report: {output}")
    report = json.loads(output.read_text(encoding="utf-8"))
    results = report.get("cases") if isinstance(report, dict) else None
    try:
        validate_benchmark_dimensions(
            report,
            _case_dimensions(
                case,
                rows=rows,
                warmups=warmups,
                repeats=repeats,
                selection=selection,
                pipeline_shape=pipeline_shape,
                pipeline_format=pipeline_format,
            ),
        )
        validate_benchmark_case_results(
            results,
            selection=selection,
            pipeline_shape=pipeline_shape,
            pipeline_format=pipeline_format,
        )
    except RuntimeError as error:
        raise RuntimeError(f"{case.label}: {error}") from error
    return report


def _run_case(
    case: MatrixCase,
    *,
    rows: int,
    warmups: int,
    repeats: int,
    selection: str,
    pipeline_shape: str,
    pipeline_format: str,
    directory: Path,
) -> dict[str, Any]:
    """Run one child benchmark and return its verified JSON report."""
    command, output = _case_command(
        case,
        rows=rows,
        warmups=warmups,
        repeats=repeats,
        selection=selection,
        pipeline_shape=pipeline_shape,
        pipeline_format=pipeline_format,
        directory=directory,
    )
    run_command(command, check=True, stdout=DISCARD, timeout=3_600)
    return _load_case(
        case,
        output,
        rows=rows,
        warmups=warmups,
        repeats=repeats,
        selection=selection,
        pipeline_shape=pipeline_shape,
        pipeline_format=pipeline_format,
    )


def _report(args: argparse.Namespace, directory: Path) -> dict[str, Any]:
    """Aggregate already completed case reports."""
    cases = _cases(args.profile)
    results = {
        case.label: {
            "dimensions": asdict(case),
            "report": _load_case(
                case,
                directory / f"{case.label}.json",
                rows=args.rows,
                warmups=args.warmups,
                repeats=args.repeats,
                selection=args.only,
                pipeline_shape=args.pipeline_shape,
                pipeline_format=args.pipeline_format,
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
        "pipeline_shape": args.pipeline_shape,
        "pipeline_format": args.pipeline_format,
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
                pipeline_shape=args.pipeline_shape,
                pipeline_format=args.pipeline_format,
                directory=directory,
            )
        return _report(args, directory)


def _fresh_plan_directory(work_root: Path) -> Path:
    """Create one fixed owned directory after removing stale rerun state."""
    if work_root.is_symlink():
        raise ValueError(f"matrix work root must be a regular directory: {work_root}")
    work_root.mkdir(parents=True, exist_ok=True)
    work_root = work_root.resolve()
    if not work_root.is_dir():
        raise ValueError(f"matrix work root must be a regular directory: {work_root}")
    directory = work_root / _PLAN_DIRECTORY
    if directory.is_symlink():
        raise ValueError(f"refusing symlinked matrix plan directory: {directory}")
    if directory.exists():
        if not directory.is_dir():
            raise ValueError(f"matrix plan path is not a directory: {directory}")
        shutil.rmtree(directory)
    directory.mkdir()
    return directory


def _paths_overlap(left: Path, right: Path) -> bool:
    """Return whether two resolved locations contain or equal one another."""
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def _validate_plan_paths(
    work_root: Path,
    command_output: Path,
    report_output: Path | None,
) -> None:
    """Reject outputs that overlap each other or the cleanup-owned plan tree."""
    locations = {
        "owned plan root": work_root.resolve() / _PLAN_DIRECTORY,
        "command output": command_output.resolve(),
    }
    if report_output is not None:
        locations["report output"] = report_output.resolve()
    items = tuple(locations.items())
    for index, (left_name, left) in enumerate(items):
        for right_name, right in items[index + 1 :]:
            if _paths_overlap(left, right):
                raise ValueError(
                    f"benchmark {left_name} and {right_name} must be disjoint: {left} and {right}"
                )


def _write_text_atomically(destination: Path, content: str) -> None:
    """Replace one regular text output atomically and skip unchanged bytes."""
    payload = content.encode("utf-8")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise ValueError(f"refusing symlinked benchmark output: {destination}")
    if destination.exists() and not destination.is_file():
        raise ValueError(f"benchmark output must be a regular file: {destination}")
    if destination.is_file() and destination.read_bytes() == payload:
        return
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _shell_plan(args: argparse.Namespace, work_root: Path, command_output: Path) -> str:
    """Return a same-job shell plan for GitHub Actions."""
    _validate_plan_paths(work_root, command_output, args.output)
    directory = _fresh_plan_directory(work_root)
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "umask 077",
        f"matrix_root={shlex.quote(directory.as_posix())}",
        "cleanup_matrix() {",
        "  local status=$?",
        "  local cleanup_status=0",
        "  trap - EXIT",
        '  rm -rf -- "${matrix_root}" || cleanup_status=$?',
        "  if (( status != 0 )); then",
        '    exit "${status}"',
        "  fi",
        '  exit "${cleanup_status}"',
        "}",
        "trap cleanup_matrix EXIT",
    ]
    for case in _cases(args.profile):
        command, _output = _case_command(
            case,
            rows=args.rows,
            warmups=args.warmups,
            repeats=args.repeats,
            selection=args.only,
            pipeline_shape=args.pipeline_shape,
            pipeline_format=args.pipeline_format,
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
        "--pipeline-shape",
        args.pipeline_shape,
        "--pipeline-format",
        args.pipeline_format,
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
    parser.add_argument("--pipeline-shape", choices=("all", "scalar", "nested"), default="all")
    parser.add_argument(
        "--pipeline-format", choices=("all", "csv", "jsonl", "parquet"), default="all"
    )
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
        plan = _shell_plan(args, args.work_root, args.command_output)
        try:
            _write_text_atomically(args.command_output, plan)
        except Exception:
            if not args.work_root.is_symlink():
                shutil.rmtree(args.work_root.resolve() / _PLAN_DIRECTORY, ignore_errors=True)
            raise
        return

    report = (
        _report(args, args.assemble_root) if args.assemble_root is not None else run_matrix(args)
    )
    encoded = json.dumps(report, indent=2, sort_keys=True, allow_nan=False)
    print(encoded)
    if args.output is not None:
        _write_text_atomically(args.output, encoded + "\n")


if __name__ == "__main__":
    main()
