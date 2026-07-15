"""Execute promoted native fuzz crash inputs as deterministic regressions."""

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


def parse_args() -> argparse.Namespace:
    """Parse command-line locations."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-root", type=Path, default=Path("build-fuzz/fuzz"))
    parser.add_argument("--regression-root", type=Path, default=Path("fuzz/regressions"))
    return parser.parse_args()


def main() -> None:
    """Run promoted fuzz regressions."""
    args = parse_args()
    count = run_regressions(args.build_root, args.regression_root)
    print(f"Executed {count} deterministic fuzz regression inputs.")


if __name__ == "__main__":
    main()
