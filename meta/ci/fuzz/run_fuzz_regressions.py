#!/usr/bin/env python3
"""Prepare deterministic native-fuzz commands for the CI shell wrapper.

The planner materializes packed and loose inputs outside the checkout, applies
bounded engine guards, and emits a safely quoted fail-fast execution plan.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import shutil
import tempfile
from pathlib import Path
from typing import NamedTuple

try:
    from meta.ci.fuzz.corpus_io import TARGETS, FuzzInput, target_inputs
except ModuleNotFoundError:
    from corpus_io import TARGETS, FuzzInput, target_inputs

DEFAULT_MAX_INPUT_MS = 5_000
DEFAULT_MAX_RSS_MB = 2_048
ENGINES = ("standalone", "libfuzzer")


class FuzzPlan(NamedTuple):
    """Commands, evidence counts, and owned staging for one bounded fuzz run."""

    commands: tuple[tuple[str, ...], ...]
    regression_inputs: int
    mutation_runs: int
    staging_root: Path


def guard_arguments(engine: str, *, max_input_ms: int, max_rss_mb: int) -> list[str]:
    """Return engine-specific per-input time and process-memory guards."""
    if engine == "standalone":
        return [f"-max_input_ms={max_input_ms}", f"-max_rss_mb={max_rss_mb}"]
    if engine == "libfuzzer":
        timeout_seconds = max(1, (max_input_ms + 999) // 1_000)
        return [f"-timeout={timeout_seconds}", f"-rss_limit_mb={max_rss_mb}"]
    raise ValueError(f"unsupported fuzz engine: {engine}")


def target_cases(role_root: Path, target: str) -> list[FuzzInput]:
    """Return stable logical inputs from one loose-or-packed role target."""
    try:
        return target_inputs(role_root, target)
    except ValueError as error:
        if "missing regular fuzz target directory" in str(error):
            raise FileNotFoundError(str(error)) from error
        raise


def _safe_case_name(case: FuzzInput) -> str:
    """Return a single-component staging name for one validated logical input."""
    name = case.name
    if not name or name in {".", ".."} or "/" in name or "\\" in name or "\0" in name:
        raise ValueError(f"unsafe fuzz input name: {name!r}")
    return name


def _stage_regression_inputs(
    destination_root: Path,
    regression_root: Path,
    target: str,
) -> list[Path]:
    """Materialize one target's logical regressions in owned staging."""
    cases = target_cases(regression_root, target)
    if not cases:
        raise RuntimeError(f"no fuzz regression inputs found under {regression_root / target}")
    destination = destination_root / target
    destination.mkdir(parents=True)
    staged: list[Path] = []
    for case in cases:
        path = destination / _safe_case_name(case)
        path.write_bytes(case.data)
        staged.append(path)
    return staged


def _stage_campaign_corpus(
    destination_root: Path,
    regression_root: Path,
    corpus_root: Path | None,
    target: str,
) -> Path:
    """Materialize a byte-deduplicated campaign corpus outside the checkout."""
    destination = destination_root / target
    destination.mkdir(parents=True)
    seen: set[bytes] = set()
    source_roots = (regression_root,) if corpus_root is None else (regression_root, corpus_root)
    for source_root in source_roots:
        cases = target_cases(source_root, target)
        if not cases:
            raise RuntimeError(f"no fuzz inputs found under {source_root / target}")
        for case in cases:
            digest = hashlib.sha256(case.data).digest()
            if digest in seen:
                continue
            seen.add(digest)
            (destination / digest.hex()).write_bytes(case.data)
    print(f"[fuzz-corpus] {target}: staged {len(seen)} unique inputs", flush=True)
    return destination


def fuzzer_binary(build_root: Path, target: str) -> Path:
    """Return the platform-specific fuzzer executable path."""
    suffix = ".exe" if os.name == "nt" else ""
    return build_root / f"schema_sanitizer_fuzz_{target}{suffix}"


def _require_fuzzer_binary(build_root: Path, target: str) -> Path:
    """Return one regular non-symlink fuzzer binary or fail closed."""
    binary = fuzzer_binary(build_root, target)
    if binary.is_symlink() or not binary.is_file():
        raise FileNotFoundError(f"missing regular fuzzer binary: {binary}")
    return binary


def _command_path(path: Path) -> str:
    """Return a path accepted by GitHub's Bash shell on every runner OS."""
    return path.as_posix()


def regression_commands(
    build_root: Path,
    regression_root: Path,
    staging_root: Path,
    *,
    max_input_ms: int = DEFAULT_MAX_INPUT_MS,
    max_rss_mb: int = DEFAULT_MAX_RSS_MB,
    engine: str = "standalone",
) -> list[tuple[str, ...]]:
    """Stage and plan one invocation for every promoted regression input."""
    commands: list[tuple[str, ...]] = []
    for target in TARGETS:
        binary = _require_fuzzer_binary(build_root, target)
        for case in _stage_regression_inputs(staging_root, regression_root, target):
            commands.append(
                (
                    _command_path(binary),
                    "-runs=1",
                    *guard_arguments(
                        engine,
                        max_input_ms=max_input_ms,
                        max_rss_mb=max_rss_mb,
                    ),
                    _command_path(case),
                )
            )
    return commands


def campaign_commands(
    build_root: Path,
    regression_root: Path,
    campaign_root: Path,
    *,
    corpus_root: Path | None = None,
    runs: int,
    seed: int,
    max_length: int,
    max_input_ms: int = DEFAULT_MAX_INPUT_MS,
    max_rss_mb: int = DEFAULT_MAX_RSS_MB,
    engine: str = "standalone",
) -> list[tuple[str, ...]]:
    """Stage corpora and plan one deterministic campaign per parser target."""
    if runs <= 0:
        return []
    for target in TARGETS:
        _stage_campaign_corpus(campaign_root, regression_root, corpus_root, target)

    commands: list[tuple[str, ...]] = []
    for ordinal, target in enumerate(TARGETS):
        binary = _require_fuzzer_binary(build_root, target)
        commands.append(
            (
                _command_path(binary),
                f"-runs={runs}",
                f"-seed={seed + ordinal}",
                f"-max_len={max_length}",
                *guard_arguments(
                    engine,
                    max_input_ms=max_input_ms,
                    max_rss_mb=max_rss_mb,
                ),
                _command_path(campaign_root / target),
            )
        )
    return commands


def build_plan(args: argparse.Namespace) -> FuzzPlan:
    """Validate inputs, stage all corpora, and return the complete command plan."""
    work_root = args.work_root.resolve()
    work_root.mkdir(parents=True, exist_ok=True)
    staging_root = Path(tempfile.mkdtemp(prefix="schema-sanitizer-fuzz-", dir=work_root)).resolve()
    try:
        regressions = regression_commands(
            args.build_root,
            args.regression_root,
            staging_root / "regressions",
            max_input_ms=args.max_input_ms,
            max_rss_mb=args.max_rss_mb,
            engine=args.engine,
        )
        campaigns = campaign_commands(
            args.build_root,
            args.regression_root,
            staging_root / "campaigns",
            corpus_root=args.corpus_root,
            runs=args.campaign_runs,
            seed=args.seed,
            max_length=args.max_len,
            max_input_ms=args.max_input_ms,
            max_rss_mb=args.max_rss_mb,
            engine=args.engine,
        )
    except Exception:
        shutil.rmtree(staging_root)
        raise
    return FuzzPlan(
        commands=tuple([*regressions, *campaigns]),
        regression_inputs=len(regressions),
        mutation_runs=args.campaign_runs * len(campaigns),
        staging_root=staging_root,
    )


def write_shell_plan(
    plan: FuzzPlan,
    destination: Path,
    *,
    evidence_output: Path | None,
    evidence: dict[str, object],
) -> None:
    """Write a fail-fast shell program containing only validated argv commands."""
    if plan.staging_root.is_symlink() or not plan.staging_root.is_dir():
        raise ValueError(f"missing regular fuzz staging directory: {plan.staging_root}")
    quoted_root = shlex.quote(_command_path(plan.staging_root))
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "umask 077",
        f"staging_root={quoted_root}",
        'cleanup_staging() { rm -rf -- "${staging_root}"; }',
        "trap cleanup_staging EXIT",
    ]
    for command in plan.commands:
        campaign = any(value.startswith("-seed=") for value in command)
        label = "fuzz-campaign" if campaign else "fuzz-regression"
        lines.append("printf '%s\\n' " + shlex.quote(f"[{label}] {Path(command[0]).name}"))
        lines.append(shlex.join(command))
    lines.append(
        "printf '%s\\n' "
        + shlex.quote(
            f"Executed {plan.regression_inputs} deterministic regression inputs and "
            f"{plan.mutation_runs} mutation runs."
        )
    )
    if evidence_output is not None:
        encoded = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
        lines.extend(
            (
                f"mkdir -p -- {shlex.quote(_command_path(evidence_output.parent))}",
                "printf '%s' "
                + shlex.quote(encoded)
                + " > "
                + shlex.quote(_command_path(evidence_output)),
            )
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise ValueError(f"refusing symlinked command output: {destination}")
    destination.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def parse_args() -> argparse.Namespace:
    """Parse command-plan locations and bounded campaign settings."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-root", type=Path, default=Path("build-fuzz/fuzz"))
    parser.add_argument("--regression-root", type=Path, default=Path("fuzz/regressions"))
    parser.add_argument("--corpus-root", type=Path, default=Path("fuzz/corpus"))
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--command-output", type=Path, required=True)
    parser.add_argument("--campaign-runs", type=int, default=0)
    parser.add_argument("--seed", type=int, default=15_172_026)
    parser.add_argument("--max-len", type=int, default=1 << 20)
    parser.add_argument("--max-input-ms", type=int, default=DEFAULT_MAX_INPUT_MS)
    parser.add_argument("--max-rss-mb", type=int, default=DEFAULT_MAX_RSS_MB)
    parser.add_argument("--engine", choices=ENGINES, default="standalone")
    parser.add_argument("--evidence-output", type=Path)
    args = parser.parse_args()
    if args.campaign_runs < 0:
        parser.error("campaign-runs must be non-negative")
    if args.seed < 0:
        parser.error("seed must be non-negative")
    if args.max_len <= 0:
        parser.error("max-len must be positive")
    if args.max_input_ms <= 0:
        parser.error("max-input-ms must be positive")
    if args.max_rss_mb < 0:
        parser.error("max-rss-mb must be non-negative")
    return args


def main() -> None:
    """Create the shell plan consumed by the sanitizer composite actions."""
    args = parse_args()
    plan = build_plan(args)
    evidence: dict[str, object] = {
        "schema_version": 1,
        "status": "passed",
        "engine": args.engine,
        "targets": list(TARGETS),
        "regression_inputs": plan.regression_inputs,
        "mutation_runs": plan.mutation_runs,
        "campaign_runs_per_target": args.campaign_runs,
        "seed": args.seed,
        "max_len": args.max_len,
        "max_input_ms": args.max_input_ms,
        "max_rss_mb": args.max_rss_mb,
        "sanitizer_findings": 0,
    }
    try:
        write_shell_plan(
            plan,
            args.command_output,
            evidence_output=args.evidence_output,
            evidence=evidence,
        )
    except Exception:
        shutil.rmtree(plan.staging_root)
        raise


if __name__ == "__main__":
    main()
