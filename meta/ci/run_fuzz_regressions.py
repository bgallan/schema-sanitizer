"""Execute native fuzz regressions and bounded deterministic campaigns."""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path

TARGETS = ("json", "csv", "xml", "parquet")


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


def run_regressions(build_root: Path, regression_root: Path) -> int:
    """Run every promoted input once and return the number executed."""
    executed = 0
    for target in TARGETS:
        binary = fuzzer_binary(build_root, target)
        if not binary.is_file():
            raise FileNotFoundError(f"missing fuzzer binary: {binary}")
        for case in regression_cases(regression_root, target):
            print(f"[fuzz-regression] {target}: {case.name}", flush=True)
            subprocess.run(
                [os.fspath(binary), "-runs=1", os.fspath(case)],
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
    args = parser.parse_args()
    if args.campaign_runs < 0:
        parser.error("campaign-runs must be non-negative")
    if args.seed < 0:
        parser.error("seed must be non-negative")
    if args.max_len <= 0:
        parser.error("max-len must be positive")
    return args


def main() -> None:
    """Run promoted regressions followed by optional mutation campaigns."""
    args = parse_args()
    regressions = run_regressions(args.build_root, args.regression_root)
    mutations = run_campaigns(
        args.build_root,
        args.regression_root,
        runs=args.campaign_runs,
        seed=args.seed,
        max_length=args.max_len,
    )
    print(f"Executed {regressions} deterministic regression inputs and {mutations} mutation runs.")


if __name__ == "__main__":
    main()
