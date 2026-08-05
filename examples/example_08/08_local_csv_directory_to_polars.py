"""Validate example 08 against a local directory of heterogeneous CSV files."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:  # pragma: no cover - direct source checkout execution
    _REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(_REPOSITORY_ROOT))
    sys.path.insert(0, str(_REPOSITORY_ROOT / "src"))

try:
    from examples.example_08.local_validation import load_local_csv_directory_to_polars
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from local_validation import load_local_csv_directory_to_polars


def build_parser() -> argparse.ArgumentParser:
    """Build the local validation command-line parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Read all CSVs in one directory, reconcile their headers, sanitize "
            "them into one Polars DataFrame, and collapse <id>/<text> columns."
        )
    )
    parser.add_argument("source_directory", type=Path)
    parser.add_argument("--output-parquet", type=Path)
    parser.add_argument("--csv-delimiter", default=",")
    parser.add_argument("--csv-escape-char", default="\\")
    parser.add_argument("--event-separator", default="/")
    parser.add_argument("--event-column", default="event")
    parser.add_argument("--include-null-payloads", action="store_true")
    parser.add_argument(
        "--multi-threading",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    return parser


def main() -> int:
    """Load the complete directory and report structural validation only."""
    args = build_parser().parse_args()
    result = load_local_csv_directory_to_polars(
        args.source_directory,
        event_separator=args.event_separator,
        event_column=args.event_column,
        omit_null_payloads=not args.include_null_payloads,
        csv_delimiter=args.csv_delimiter,
        csv_escape_char=args.csv_escape_char,
        multi_threading=args.multi_threading,
    )
    frame = result.frame
    if args.output_parquet is not None:
        args.output_parquet.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(args.output_parquet)
    csv_count = sum(1 for path in args.source_directory.iterdir() if path.suffix.lower() == ".csv")
    print(f"CSV files: {csv_count}")
    print(f"Rows: {frame.height}")
    print(f"Event source columns: {len(result.event_columns)}")
    print(f"Normalized columns: {frame.width}")
    print(f"event dtype: {frame.schema[args.event_column]}")
    if args.output_parquet is not None:
        print(f"Parquet: {args.output_parquet}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
