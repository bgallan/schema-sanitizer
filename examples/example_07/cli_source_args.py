"""Source/output URI argument builders for example 07.

It defines source, output, date-range, prefix, discovery, and schema-warm-up arguments
for the pipeline.
"""

from __future__ import annotations

import argparse

from schema_sanitizer.pipeline.advanced import parse_hour as _parse_hour
from schema_sanitizer.pipeline.advanced import parse_iso_date as _parse_iso_date


def add_source_args(parser: argparse.ArgumentParser) -> None:
    """Add source/output URI planning arguments."""
    parser.add_argument(
        "--input-format",
        choices=("csv", "json", "json_array", "jsonl", "xml", "parquet"),
        default="json_array",
        help=(
            "Source format passed to schema_sanitizer.to_parquet. It also controls "
            "the required source extension. Default: json_array."
        ),
    )
    parser.add_argument(
        "--input-mode",
        choices=("single_file", "directory"),
        default="single_file",
        help=(
            "single_file processes one generated source object per Hive partition. "
            "directory non-recursively combines every matching source file directly "
            "inside each Hive partition into one output Parquet. Default: single_file."
        ),
    )
    parser.add_argument(
        "--partition-granularity",
        choices=("daily", "hourly"),
        default="daily",
        help=(
            "Hive partition granularity generated in prefix/range mode. hourly adds "
            "hour=HH below date=YYYY-MM-DD. Default: daily."
        ),
    )
    parser.add_argument(
        "--start-hour",
        type=_parse_hour,
        help=(
            "First hourly partition processed per normal date. Requires "
            "--partition-granularity hourly. Default in hourly mode: 0."
        ),
    )
    parser.add_argument(
        "--end-hour",
        type=_parse_hour,
        help=(
            "Last hourly partition processed per normal date. Requires "
            "--partition-granularity hourly. Default in hourly mode: 23."
        ),
    )
    parser.add_argument(
        "--start-date-warm-up",
        type=_parse_iso_date,
        help=(
            "Optional inclusive warm-up start date, in YYYY-MM-DD format. "
            "When set, --end-date-warm-up is also required. The warm-up range "
            "is scanned additively as one logical source before normal writes."
        ),
    )
    parser.add_argument(
        "--end-date-warm-up",
        type=_parse_iso_date,
        help=(
            "Optional inclusive warm-up end date, in YYYY-MM-DD format. "
            "When set, --start-date-warm-up is also required."
        ),
    )
    parser.add_argument(
        "--start-hour-warm-up",
        type=_parse_hour,
        help=(
            "First hourly warm-up partition processed per warm-up date. Requires "
            "--partition-granularity hourly and warm-up dates. Default in hourly "
            "warm-up mode: 0."
        ),
    )
    parser.add_argument(
        "--end-hour-warm-up",
        type=_parse_hour,
        help=(
            "Last hourly warm-up partition processed per warm-up date. Requires "
            "--partition-granularity hourly and warm-up dates. Default in hourly mode: 23."
        ),
    )
    parser.add_argument(
        "--source-jsonl-prefix",
        help=(
            "Input base prefix for range mode, e.g. gs://raw/events/rt. "
            "The script appends daily or hourly Hive partition folders. "
            "single_file mode also appends a generated source filename."
        ),
    )
    parser.add_argument(
        "--silver-parquet-prefix",
        help=(
            "Output Parquet base prefix for range mode, e.g. gs://silver/events/test/rt. "
            "The script appends daily or hourly Hive partition folders and one generated Parquet "
            "filename. If --external-table-hive-uri-prefix is omitted, this prefix is used."
        ),
    )
    parser.add_argument(
        "--file-name-prefix",
        help=(
            "Generated source/output file name prefix. If omitted, it is inferred "
            "from --source-jsonl-prefix. Daily names use _YYYYMMDD; hourly names use _YYYYMMDD_HH."
        ),
    )
    parser.add_argument(
        "--source-file-extension",
        help=(
            "Optional source extension override without a leading dot. By default "
            "it is derived from --input-format. The extension must remain valid for "
            "the selected format. Available only in single_file mode."
        ),
    )
    parser.add_argument(
        "--silver-parquet-extension",
        default="parquet",
        help="Output file extension used in prefix mode. Default: parquet.",
    )
    parser.add_argument(
        "--source-jsonl-uri",
        help=(
            "Full input file or directory URI. With --start-date/--end-date this "
            "becomes a daily/hourly template. Use this instead of --source-jsonl-prefix for custom layouts."
        ),
    )
    parser.add_argument(
        "--silver-parquet-uri",
        help=(
            "Full output Parquet URI. With --start-date/--end-date this becomes a template. "
            "Use this instead of --silver-parquet-prefix for custom daily or hourly layouts."
        ),
    )
    parser.add_argument(
        "--start-date",
        type=_parse_iso_date,
        help=(
            "Optional inclusive start date for range mode, in YYYY-MM-DD format. "
            "When set, --end-date is also required."
        ),
    )
    parser.add_argument(
        "--end-date",
        type=_parse_iso_date,
        help=(
            "Optional inclusive end date for range mode, in YYYY-MM-DD format. "
            "When set, --start-date is also required."
        ),
    )
