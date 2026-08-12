"""Command-line contract for example 08."""

from __future__ import annotations

import argparse
from datetime import date


def _iso_date(raw: str) -> date:
    """Parse one ISO calendar date for an inclusive UTC daily range."""
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date {raw!r}; expected YYYY-MM-DD") from exc


def parse_positive_int(raw: str) -> int:
    """Parse a strictly positive integer CLI value."""
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected an integer, got {raw!r}") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return value


def build_parser() -> argparse.ArgumentParser:
    """Build the complete flat-prefix modified-time ingestion parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Read a flat GCS CSV prefix once, group exact object generations by "
            "UTC modification day, normalize event columns with Polars, and "
            "publish validated year/month/day Hive Parquet partitions."
        )
    )
    source = parser.add_argument_group("source and windows")
    source.add_argument("--source-csv-prefix", required=True)
    source.add_argument("--start-date", required=True, type=_iso_date)
    source.add_argument("--end-date", required=True, type=_iso_date)
    source.add_argument("--csv-delimiter", default=",")
    source.add_argument(
        "--csv-escape-char",
        default="\\",
        help=r"Escape used inside quoted fields (default: backslash).",
    )

    output = parser.add_argument_group("silver output")
    output.add_argument("--silver-parquet-prefix", required=True)
    output.add_argument(
        "--partition-timestamp-column",
        required=True,
        help="Timestamp column used to derive UTC year/month/day Hive partitions.",
    )
    output.add_argument(
        "--parquet-file-prefix",
        required=True,
        help=(
            "Filename prefix used in "
            "<prefix>_<partition YYYYMMDD>_<source window YYYYMMDD>.gz.parquet."
        ),
    )

    target = parser.add_argument_group("BigQuery target")
    target.add_argument("--target-table", required=True)
    target.add_argument("--bigquery-project")
    target.add_argument("--bigquery-location")
    credentials = target.add_mutually_exclusive_group()
    credentials.add_argument("--credentials-file")
    credentials.add_argument("--credentials-json")

    event = parser.add_argument_group("event normalization")
    event.add_argument("--event-separator", default="/")
    event.add_argument("--event-column", default="event")
    event.add_argument("--omit-null-payloads", action="store_true")

    sanitizer = parser.add_argument_group("schema-sanitizer")
    sanitizer.add_argument(
        "--on-error",
        default="stop",
        choices=("stop", "skip_row", "emit_null_row"),
    )
    sanitizer.add_argument("--memory-limit-bytes", type=parse_positive_int)
    sanitizer.add_argument(
        "--multi-threading",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    sanitizer.add_argument(
        "--field-name-policy",
        default="preserve",
        choices=("preserve",),
        help=(
            "Event headers must remain byte-for-byte recognizable; this "
            "workflow therefore requires the preserve policy."
        ),
    )
    sanitizer.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return parser


__all__ = ["build_parser", "parse_positive_int"]
