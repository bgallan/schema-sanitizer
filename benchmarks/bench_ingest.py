"""Run small local ingestion benchmarks against the public Python API.

The harness generates temporary JSONL/CSV fixtures and measures end-to-end
reader throughput. It is meant for comparing code changes, not for publishing
absolute performance numbers.
"""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from bench_read_cases import run_read_cases
from bench_write_cases import run_write_cases
from cases import ALL_CASES


def run(rows: int, width: int, case: str, repeats: int) -> None:
    """Generate requested fixtures and run the selected benchmark case."""
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-bench-") as tmp:
        root = Path(tmp)
        run_read_cases(root, rows, width, repeats, case)
        run_write_cases(root, rows, width, repeats, case)


def main() -> None:
    """Parse CLI arguments and run synthetic ingestion benchmarks."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--width", type=int, default=32)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument(
        "--case",
        choices=ALL_CASES,
        default="all",
    )
    args = parser.parse_args()
    run(rows=args.rows, width=args.width, case=args.case, repeats=max(1, args.repeats))


if __name__ == "__main__":
    main()
