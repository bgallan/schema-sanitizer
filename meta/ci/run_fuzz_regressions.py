"""Execute native fuzz regressions and bounded deterministic campaigns."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

TARGETS = ("json", "csv", "xml", "parquet")
DEFAULT_MAX_INPUT_MS = 5_000
DEFAULT_MAX_RSS_MB = 2_048
ENGINES = ("standalone", "libfuzzer")


def guard_arguments(engine: str, *, max_input_ms: int, max_rss_mb: int) -> list[str]:
    """Return engine-specific per-input time and process-memory guards."""
    if engine == "standalone":
        return [
            f"-max_input_ms={max_input_ms}",
            f"-max_rss_mb={max_rss_mb}",
        ]
    if engine == "libfuzzer":
        timeout_seconds = max(1, (max_input_ms + 999) // 1_000)
        return [
            f"-timeout={timeout_seconds}",
            f"-rss_limit_mb={max_rss_mb}",
        ]
    raise ValueError(f"unsupported fuzz engine: {engine}")


def regression_cases(regression_root: Path, target: str) -> list[Path]:
    """Return stable regular-file inputs for one fuzzer target."""
    root = regression_root / target
    if not root.is_dir():
        return []
    return sorted(
        path for path in root.iterdir() if path.is_file() and not path.name.startswith(".")
    )


def fuzzer_binary(build_root: Path, target: str) -> Path:
    """Return the platform-specific fuzzer executable path."""
    suffix = ".exe" if os.name == "nt" else ""
    return build_root / f"schema_sanitizer_fuzz_{target}{suffix}"


def run_regressions(
    build_root: Path,
    regression_root: Path,
    *,
    max_input_ms: int = DEFAULT_MAX_INPUT_MS,
    max_rss_mb: int = DEFAULT_MAX_RSS_MB,
    engine: str = "standalone",
) -> int:
    """Run every promoted input once and return the number executed."""
    executed = 0
    for target in TARGETS:
        binary = fuzzer_binary(build_root, target)
        if not binary.is_file():
            raise FileNotFoundError(f"missing fuzzer binary: {binary}")
        for case in regression_cases(regression_root, target):
            print(f"[fuzz-regression] {target}: {case.name}", flush=True)
            subprocess.run(
                [
                    os.fspath(binary),
                    "-runs=1",
                    *guard_arguments(
                        engine,
                        max_input_ms=max_input_ms,
                        max_rss_mb=max_rss_mb,
                    ),
                    os.fspath(case),
                ],
                check=True,
            )
            executed += 1
    if executed == 0:
        raise RuntimeError(f"no fuzz regression inputs found under {regression_root}")
    return executed


def run_campaigns(
    build_root: Path,
    regression_root: Path,
    *,
    runs: int,
    seed: int,
    max_length: int,
    max_input_ms: int = DEFAULT_MAX_INPUT_MS,
    max_rss_mb: int = DEFAULT_MAX_RSS_MB,
    engine: str = "standalone",
) -> int:
    """Run one deterministic mutation campaign for every parser target."""
    if runs <= 0:
        return 0
    executed = 0
    for ordinal, target in enumerate(TARGETS):
        binary = fuzzer_binary(build_root, target)
        corpus = regression_root / target
        if not binary.is_file():
            raise FileNotFoundError(f"missing fuzzer binary: {binary}")
        if not corpus.is_dir():
            raise FileNotFoundError(f"missing fuzz corpus directory: {corpus}")
        target_seed = seed + ordinal
        print(
            f"[fuzz-campaign] {target}: runs={runs} seed={target_seed} max_len={max_length}",
            flush=True,
        )
        subprocess.run(
            [
                os.fspath(binary),
                f"-runs={runs}",
                f"-seed={target_seed}",
                f"-max_len={max_length}",
                *guard_arguments(
                    engine,
                    max_input_ms=max_input_ms,
                    max_rss_mb=max_rss_mb,
                ),
                os.fspath(corpus),
            ],
            check=True,
        )
        executed += runs
    return executed


def parse_args() -> argparse.Namespace:
    """Parse command-line locations and bounded campaign settings."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-root", type=Path, default=Path("build-fuzz/fuzz"))
    parser.add_argument("--regression-root", type=Path, default=Path("fuzz/regressions"))
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
    """Run promoted regressions followed by optional mutation campaigns."""
    args = parse_args()
    regressions = run_regressions(
        args.build_root,
        args.regression_root,
        max_input_ms=args.max_input_ms,
        max_rss_mb=args.max_rss_mb,
        engine=args.engine,
    )
    mutations = run_campaigns(
        args.build_root,
        args.regression_root,
        runs=args.campaign_runs,
        seed=args.seed,
        max_length=args.max_len,
        max_input_ms=args.max_input_ms,
        max_rss_mb=args.max_rss_mb,
        engine=args.engine,
    )
    print(f"Executed {regressions} deterministic regression inputs and {mutations} mutation runs.")
    if args.evidence_output is not None:
        evidence = {
            "schema_version": 1,
            "status": "passed",
            "engine": args.engine,
            "targets": list(TARGETS),
            "regression_inputs": regressions,
            "mutation_runs": mutations,
            "campaign_runs_per_target": args.campaign_runs,
            "seed": args.seed,
            "max_len": args.max_len,
            "max_input_ms": args.max_input_ms,
            "max_rss_mb": args.max_rss_mb,
            "sanitizer_findings": 0,
        }
        args.evidence_output.parent.mkdir(parents=True, exist_ok=True)
        args.evidence_output.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
