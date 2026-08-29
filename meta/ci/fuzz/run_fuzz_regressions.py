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
_STAGING_DIRECTORY = "staging"


class FuzzPlan(NamedTuple):
    """Commands, evidence counts, and owned staging for one bounded fuzz run."""

    commands: tuple[tuple[str, ...], ...]
    regression_inputs: int
    mutation_runs: int
    staging_root: Path
    source_roots: tuple[tuple[str, Path], ...] = ()


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


def _paths_overlap(left: Path, right: Path) -> bool:
    """Return whether two resolved locations contain or equal one another."""
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def _source_roots(args: argparse.Namespace) -> tuple[tuple[str, Path], ...]:
    """Return the resolved read-only roots consumed by one fuzz plan."""
    return tuple(
        (name, root.resolve())
        for name, root in (
            ("build root", args.build_root),
            ("regression root", args.regression_root),
            ("corpus root", args.corpus_root),
        )
        if root is not None
    )


def _validate_staging_sources(args: argparse.Namespace, staging_root: Path) -> None:
    """Reject source or executable roots that overlap cleanup-owned staging."""
    resolved_staging = staging_root.resolve()
    for name, source in _source_roots(args):
        if _paths_overlap(resolved_staging, source):
            raise ValueError(f"fuzz {name} must stay outside owned staging: {source}")


def _validate_requested_outputs(
    staging_root: Path,
    command_output: Path,
    evidence_output: Path | None,
    *,
    source_roots: tuple[tuple[str, Path], ...] = (),
) -> None:
    """Reject outputs overlapping each other, staging, or read-only source trees."""
    resolved_staging = staging_root.resolve()
    outputs = {"command output": command_output.resolve()}
    if evidence_output is not None:
        outputs["evidence output"] = evidence_output.resolve()
    for name, output in outputs.items():
        if _paths_overlap(output, resolved_staging):
            raise ValueError(f"fuzz {name} must stay outside owned staging: {output}")
        for source_name, source_root in source_roots:
            resolved_source = source_root.resolve()
            if output == resolved_source or output.is_relative_to(resolved_source):
                raise ValueError(f"fuzz {name} must stay outside {source_name}: {output}")
    if len(outputs) == 2:
        command, evidence = outputs.values()
        if _paths_overlap(command, evidence):
            raise ValueError("fuzz evidence and command outputs must be disjoint")


def _validate_plan_outputs(
    plan: FuzzPlan,
    destination: Path,
    evidence_output: Path | None,
) -> None:
    """Reject outputs overlapping any planned executable or input location."""
    _validate_requested_outputs(
        plan.staging_root,
        destination,
        evidence_output,
        source_roots=plan.source_roots,
    )
    outputs = {"command output": destination.resolve()}
    if evidence_output is not None:
        outputs["evidence output"] = evidence_output.resolve()
    protected: list[tuple[str, Path]] = []
    for ordinal, command in enumerate(plan.commands):
        if not command:
            raise ValueError(f"fuzz command {ordinal} must not be empty")
        protected.extend(
            (
                (f"command {ordinal} executable", Path(command[0]).resolve()),
                (f"command {ordinal} input", Path(command[-1]).resolve()),
            )
        )
    for output_name, output in outputs.items():
        for protected_name, protected_path in protected:
            if _paths_overlap(output, protected_path):
                raise ValueError(
                    f"fuzz {output_name} must be disjoint from {protected_name}: "
                    f"{output} and {protected_path}"
                )


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
    if args.work_root.is_symlink():
        raise ValueError(f"fuzz work root must be a regular directory: {args.work_root}")
    staging_root = args.work_root.resolve() / _STAGING_DIRECTORY
    _validate_staging_sources(args, staging_root)
    args.work_root.mkdir(parents=True, exist_ok=True)
    work_root = args.work_root.resolve()
    if not work_root.is_dir():
        raise ValueError(f"fuzz work root must be a regular directory: {args.work_root}")
    staging_root = work_root / _STAGING_DIRECTORY
    if staging_root.is_symlink():
        raise ValueError(f"refusing symlinked fuzz staging directory: {staging_root}")
    if staging_root.exists():
        if not staging_root.is_dir():
            raise ValueError(f"fuzz staging path is not a directory: {staging_root}")
        shutil.rmtree(staging_root)
    staging_root.mkdir()
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
        source_roots=_source_roots(args),
    )


def _write_text_atomically(destination: Path, content: str) -> None:
    """Replace one regular text output atomically and skip unchanged bytes."""
    payload = content.encode("utf-8")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise ValueError(f"refusing symlinked command output: {destination}")
    if destination.exists() and not destination.is_file():
        raise ValueError(f"command output must be a regular file: {destination}")
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
    _validate_plan_outputs(plan, destination, evidence_output)
    quoted_root = shlex.quote(_command_path(plan.staging_root))
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "umask 077",
        f"staging_root={quoted_root}",
        "evidence_tmp=",
        "cleanup_staging() {",
        "  local status=$?",
        "  local cleanup_status=0",
        "  trap - EXIT",
        '  if [[ -n "${evidence_tmp}" ]]; then',
        '    rm -f -- "${evidence_tmp}" || cleanup_status=$?',
        "  fi",
        '  rm -rf -- "${staging_root}" || cleanup_status=$?',
        "  if (( status != 0 )); then",
        '    exit "${status}"',
        "  fi",
        '  exit "${cleanup_status}"',
        "}",
        "trap cleanup_staging EXIT",
    ]
    if evidence_output is not None:
        evidence_path = _command_path(evidence_output)
        evidence_parent = _command_path(evidence_output.parent)
        lines.extend(
            (
                f"evidence_output={shlex.quote(evidence_path)}",
                f"evidence_parent={shlex.quote(evidence_parent)}",
                'mkdir -p -- "${evidence_parent}"',
                'if [[ -L "${evidence_output}" || ( -e "${evidence_output}" '
                '&& ! -f "${evidence_output}" ) ]]; then',
                "  printf '%s\\n' 'fuzz evidence output must be a regular file' >&2",
                "  exit 1",
                "fi",
                'rm -f -- "${evidence_output}"',
            )
        )
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
                'evidence_tmp=$(mktemp "${evidence_parent}/.fuzz-evidence.XXXXXX")',
                "printf '%s' " + shlex.quote(encoded) + ' > "${evidence_tmp}"',
                'mv -f -- "${evidence_tmp}" "${evidence_output}"',
                "evidence_tmp=",
            )
        )
    _write_text_atomically(destination, "\n".join(lines) + "\n")


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
    _validate_requested_outputs(
        args.work_root.resolve() / _STAGING_DIRECTORY,
        args.command_output,
        args.evidence_output,
        source_roots=_source_roots(args),
    )
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
