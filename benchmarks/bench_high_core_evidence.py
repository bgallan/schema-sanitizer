#!/usr/bin/env python3
"""Run short and sustained concurrency telemetry on one reviewed host plan."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
for _path in (_REPOSITORY_ROOT, _REPOSITORY_ROOT / "src"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from benchmarks.concurrency_high_core_suite import (  # noqa: E402
    recommend_suite_frontier,
    suite_markdown,
)

_DEFAULT_PERF_EVENTS = (
    "task-clock,cycles,instructions,cache-references,cache-misses,"
    "branches,branch-misses,context-switches,cpu-migrations,page-faults"
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", default="1,2,4,8,16")
    parser.add_argument("--columns", type=int, default=128)
    parser.add_argument("--memory-mib", type=int, default=256)
    parser.add_argument("--short-rows", type=int, default=20_000)
    parser.add_argument("--sustained-rows", type=int, default=500_000)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--numa-node", type=int, default=0)
    parser.add_argument("--output-mode", choices=("devnull", "file"), default="devnull")
    parser.add_argument("--perf-events", default=_DEFAULT_PERF_EVENTS)
    parser.add_argument("--cpu-affinity-json", type=Path)
    parser.add_argument("--short-dram-json", type=Path)
    parser.add_argument("--sustained-dram-json", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("high-core-evidence"))
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--profiles", default="short,sustained")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--allow-low-core", action="store_true")
    parser.add_argument("--allow-unbound", action="store_true")
    parser.add_argument("--no-hardware-counters", action="store_true")
    return parser


def _validate(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if (
        args.columns < 2
        or args.memory_mib <= 0
        or args.short_rows <= 0
        or args.sustained_rows <= 0
        or args.sustained_rows < args.short_rows
        or args.warmups < 0
        or args.repeats < 5
    ):
        parser.error(
            "columns, memory and rows must be positive; sustained rows must be at "
            "least short rows; repeats must be at least five"
        )


def _selected_profiles(raw: str) -> tuple[str, ...]:
    values = tuple(dict.fromkeys(item.strip() for item in raw.split(",") if item.strip()))
    if not values or set(values) - {"short", "sustained"}:
        raise ValueError("profiles must contain short, sustained, or both")
    return values


def _source_fingerprint() -> str:
    """Hash production and benchmark sources so resume never crosses revisions."""
    digest = hashlib.sha256()
    roots = (
        (
            _REPOSITORY_ROOT / "cpp" / "src",
            {".c", ".cc", ".cpp", ".h", ".hh", ".hpp", ".inc", ".inl"},
        ),
        (_REPOSITORY_ROOT / "src" / "schema_sanitizer", {".py"}),
        (_REPOSITORY_ROOT / "benchmarks", {".py"}),
    )
    files = [
        _REPOSITORY_ROOT / "CMakeLists.txt",
        _REPOSITORY_ROOT / "cmake" / "SchemaSanitizerSources.cmake",
        _REPOSITORY_ROOT / "meta" / "VERSION",
    ]
    for root, suffixes in roots:
        files.extend(
            path for path in root.rglob("*") if path.is_file() and path.suffix.lower() in suffixes
        )
    for path in sorted(set(files)):
        relative = path.relative_to(_REPOSITORY_ROOT).as_posix().encode()
        digest.update(len(relative).to_bytes(4, "little"))
        digest.update(relative)
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()


def _plan_fingerprint(plan: dict[str, Any], args: argparse.Namespace) -> str:
    payload = {
        "cpu_sets": plan.get("cpu_sets", {}),
        "workers": args.workers,
        "numa_node": args.numa_node,
        "columns": args.columns,
        "memory_mib": args.memory_mib,
        "output_mode": args.output_mode,
        "repeats": args.repeats,
        "warmups": args.warmups,
        "hardware_counters": not args.no_hardware_counters,
        "perf_events": args.perf_events,
        "host": {
            key: plan.get("host", {}).get(key)
            for key in (
                "platform",
                "machine",
                "processor",
                "cpu_affinity_list",
                "numa_nodes",
            )
        },
        "source_fingerprint": _source_fingerprint(),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _profile_meta_path(output: Path) -> Path:
    return output.with_suffix(".meta.json")


def _resume_profile(output: Path, *, fingerprint: str, command: list[str]) -> dict[str, Any] | None:
    meta_path = _profile_meta_path(output)
    if not output.exists() or not meta_path.exists():
        return None
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if meta.get("plan_fingerprint") != fingerprint or meta.get("command") != command:
        return None
    return json.loads(output.read_text(encoding="utf-8"))


def _write_profile_meta(output: Path, *, fingerprint: str, command: list[str]) -> None:
    _profile_meta_path(output).write_text(
        json.dumps(
            {"plan_fingerprint": fingerprint, "command": command},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _base_command(args: argparse.Namespace) -> list[str]:
    script = _REPOSITORY_ROOT / "benchmarks" / "bench_concurrency_telemetry.py"
    command = [
        sys.executable,
        str(script),
        "--workers",
        args.workers,
        "--workloads",
        "arrow_stream,jsonl_to_jsonl",
        "--columns",
        str(args.columns),
        "--memory-mib",
        str(args.memory_mib),
        "--warmups",
        str(args.warmups),
        "--repeats",
        str(args.repeats),
        "--sampling-mode",
        "interleaved-isolated",
        "--output-mode",
        args.output_mode,
        "--numa-node",
        str(args.numa_node),
        "--perf-events",
        args.perf_events,
    ]
    if not args.allow_low_core:
        command.append("--require-high-core")
    if not args.allow_unbound:
        command.append("--require-numa-binding")
    if not args.no_hardware_counters:
        command.append("--hardware-counters")
    return command


def _run_json(command: list[str], *, output: Path | None = None) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(detail[-4_000:] or "telemetry child failed without output")
    if output is not None and output.exists():
        return json.loads(output.read_text(encoding="utf-8"))
    return json.loads(completed.stdout)


def _plan(args: argparse.Namespace, directory: Path) -> tuple[dict[str, Any], Path, list[str]]:
    plan_path = directory / "host-plan.json"
    command = [
        *_base_command(args),
        "--rows",
        str(args.short_rows),
        "--plan-only",
        "--output",
        str(plan_path),
    ]
    if args.cpu_affinity_json is not None:
        command.extend(["--cpu-affinity-json", str(args.cpu_affinity_json.resolve())])
    return _run_json(command, output=plan_path), plan_path, command


def _profile_command(
    args: argparse.Namespace,
    *,
    name: str,
    rows: int,
    plan_path: Path,
    directory: Path,
    dram_path: Path | None,
) -> tuple[list[str], Path, Path]:
    output = directory / f"{name}.json"
    summary = directory / f"{name}.md"
    command = [
        *_base_command(args),
        "--rows",
        str(rows),
        "--cpu-affinity-json",
        str(plan_path),
        "--output",
        str(output),
        "--summary-output",
        str(summary),
    ]
    if dram_path is not None:
        command.extend(["--dram-bandwidth-json", str(dram_path.resolve())])
    return command, output, summary


def _profile_plan(
    *, name: str, rows: int, command: list[str], output: Path, summary: Path
) -> dict[str, Any]:
    return {
        "name": name,
        "rows": rows,
        "command": command,
        "output": str(output),
        "summary": str(summary),
    }


def _execute(args: argparse.Namespace) -> dict[str, Any]:
    directory = args.output_dir.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    plan, plan_path, plan_command = _plan(args, directory)
    fingerprint = _plan_fingerprint(plan, args)
    selected = _selected_profiles(args.profiles)
    profile_specs = (
        ("short", args.short_rows, args.short_dram_json),
        ("sustained", args.sustained_rows, args.sustained_dram_json),
    )
    commands: dict[str, dict[str, Any]] = {}
    profiles: dict[str, dict[str, Any]] = {}
    for name, rows, dram_path in profile_specs:
        command, output, summary = _profile_command(
            args,
            name=name,
            rows=rows,
            plan_path=plan_path,
            directory=directory,
            dram_path=dram_path,
        )
        commands[name] = _profile_plan(
            name=name, rows=rows, command=command, output=output, summary=summary
        )
        if not args.plan_only and name in selected:
            resumed = (
                _resume_profile(output, fingerprint=fingerprint, command=command)
                if args.resume
                else None
            )
            profiles[name] = resumed or _run_json(command, output=output)
            if resumed is None:
                _write_profile_meta(output, fingerprint=fingerprint, command=command)
        elif not args.plan_only and args.resume:
            resumed = _resume_profile(output, fingerprint=fingerprint, command=command)
            if resumed is not None:
                profiles[name] = resumed

    report: dict[str, Any] = {
        "schema_version": 1,
        "plan_only": args.plan_only,
        "workers": [int(value) for value in args.workers.split(",")],
        "numa_node": args.numa_node,
        "selected_profiles": list(selected),
        "plan_fingerprint": fingerprint,
        "plan": plan,
        "plan_command": plan_command,
        "profile_commands": commands,
        "profiles": profiles,
    }
    if not args.plan_only and {"short", "sustained"}.issubset(profiles):
        report["suite_frontier"] = recommend_suite_frontier(
            profiles["short"], profiles["sustained"]
        )
    elif not args.plan_only:
        report["suite_frontier"] = {
            "primary": "measurement_incomplete",
            "recommended_action": "resume_missing_profile_on_the_same_locked_plan",
            "confidence": "high",
            "evidence": {"completed_profiles": sorted(profiles)},
        }
    suite_path = directory / "suite.json"
    markdown_path = directory / "suite.md"
    report["suite_output"] = str(suite_path)
    report["suite_summary"] = str(markdown_path)
    suite_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(suite_markdown(report), encoding="utf-8")
    return report


def main() -> None:
    parser = _parser()
    args = parser.parse_args()
    _validate(parser, args)
    try:
        report = _execute(args)
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        parser.error(str(error))
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
