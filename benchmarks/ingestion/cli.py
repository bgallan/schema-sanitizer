"""Run local ingestion benchmarks against the public Python API.

The command generates temporary fixtures, measures median and p95 throughput, and
optionally writes machine-readable history for stable runners.
"""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from benchmarks.ingestion.cases import ALL_CASES
from benchmarks.ingestion.read_cases import run_read_cases
from benchmarks.ingestion.reporting import write_report
from benchmarks.ingestion.timing import records, reset_records, set_default_warmups
from benchmarks.ingestion.write_cases import run_write_cases


def run(
    rows: int,
    width: int,
    case: str,
    repeats: int,
    *,
    warmups: int = 1,
    json_output: Path | None = None,
) -> None:
    """Generate requested fixtures and run the selected benchmark case."""
    reset_records()
    set_default_warmups(warmups)
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-bench-") as tmp:
        root = Path(tmp)
        run_read_cases(root, rows, width, repeats, case)
        run_write_cases(root, rows, width, repeats, case)
    if json_output is not None:
        write_report(
            json_output,
            records(),
            fixture_metadata={
                "rows": rows,
                "width": width,
                "case": case,
                "warmups": warmups,
                "repeats": repeats,
            },
        )


def main() -> None:
    """Parse CLI arguments and run synthetic ingestion benchmarks."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--width", type=int, default=32)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument(
        "--case",
        choices=ALL_CASES,
        default="all",
    )
    args = parser.parse_args()
    run(
        rows=max(1, args.rows),
        width=max(1, args.width),
        case=args.case,
        repeats=max(1, args.repeats),
        warmups=max(0, args.warmups),
        json_output=args.json_output,
    )


if __name__ == "__main__":
    main()
