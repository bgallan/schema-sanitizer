"""Argument-group builders for the BigQuery range-prefix example CLI."""

from __future__ import annotations

import argparse

from schema_sanitizer.integrations.bigquery import (
    parse_hive_partition_column as _parse_hive_partition_column_value,
)
from schema_sanitizer.pipeline import parse_hour as _parse_hour
from schema_sanitizer.pipeline import parse_iso_date as _parse_iso_date

DEFAULT_BATCH_MEMORY_LIMIT_BYTES = 64 * 1024 * 1024
DEFAULT_READ_CHUNK_BYTES = 256 * 1024


def _parse_hive_partition_column(raw: str) -> tuple[str, str]:
    """Parse a CLI partition column declaration."""
    try:
        return _parse_hive_partition_column_value(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def add_source_args(parser: argparse.ArgumentParser) -> None:
    """Add source/output URI planning arguments."""
    parser.add_argument(
        "--input-format",
        choices=("csv", "json", "json_array", "jsonl", "ndjson", "xml", "parquet"),
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
            "--partition-granularity hourly and warm-up dates. Default in hourly "
            "warm-up mode: 23."
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
            "Output Parquet base prefix for range mode, e.g. "
            "gs://silver/events/test/rt. The script appends daily or hourly Hive "
            "partition folders and one generated Parquet filename. "
            "If --external-table-hive-uri-prefix is omitted, this prefix is used."
        ),
    )
    parser.add_argument(
        "--file-name-prefix",
        help=(
            "Generated source/output file name prefix. If omitted, it is inferred "
            "from --source-jsonl-prefix. Daily names use _YYYYMMDD; hourly names "
            "use _YYYYMMDD_HH."
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
            "becomes a daily/hourly template. Use this instead of "
            "--source-jsonl-prefix for custom layouts."
        ),
    )
    parser.add_argument(
        "--silver-parquet-uri",
        help=(
            "Full output Parquet URI. With --start-date/--end-date this becomes "
            "a template. Use this instead of --silver-parquet-prefix for custom "
            "daily or hourly layouts."
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


def add_bigquery_args(parser: argparse.ArgumentParser) -> None:
    """Add BigQuery connection and target-table arguments."""
    parser.add_argument(
        "--target-table",
        required=True,
        help="BigQuery external table to create/update: project.dataset.table",
    )
    parser.add_argument(
        "--bigquery-project",
        help="Default BigQuery project. Required when --target-table is dataset.table.",
    )
    parser.add_argument(
        "--bigquery-location",
        help="Optional BigQuery location, e.g. EU or US.",
    )
    parser.add_argument(
        "--credentials-file",
        help="Optional service-account JSON file for ADBC BigQuery authentication.",
    )
    parser.add_argument(
        "--credentials-json",
        help="Optional service-account JSON string for ADBC BigQuery authentication.",
    )
    parser.add_argument(
        "--bigquery-registry-sidecar-table",
        help=(
            "Optional native BigQuery sidecar table storing the latest ingested "
            "partition per external table. When present, schema-registry lookup "
            "uses this table to avoid a full external-table scan. The table is "
            "created/updated after successful writes."
        ),
    )


def add_external_table_args(parser: argparse.ArgumentParser) -> None:
    """Add BigQuery external-table layout arguments."""
    parser.add_argument(
        "--external-table-source-uri",
        action="append",
        help=(
            "GCS URI used by the BigQuery external table. Can be passed multiple times. "
            "If omitted, the script derives <external-table-hive-uri-prefix>/*."
        ),
    )
    parser.add_argument(
        "--external-table-hive-uri-prefix",
        help=(
            "Hive partition URI prefix for the BigQuery external table, e.g. "
            "gs://bucket/silver/events. It must be the path immediately before "
            "the first key=value partition folder."
        ),
    )
    parser.add_argument(
        "--hive-partition-column",
        action="append",
        type=_parse_hive_partition_column,
        help=(
            "Hive partition column in name:TYPE format. Can be repeated. "
            "Order must match the GCS path. Daily default: year:INT64 "
            "month:INT64 date:DATE. Hourly default also adds hour:INT64."
        ),
    )
    parser.add_argument(
        "--external-table-format",
        default="PARQUET",
        help="BigQuery external table file format. Default: PARQUET.",
    )
    parser.add_argument(
        "--external-table-require-partition-filter",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Whether BigQuery should require a filter on Hive partition columns when querying "
            "the external table. Default: false."
        ),
    )
    parser.add_argument(
        "--parquet-enable-list-inference",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Enable BigQuery Parquet LIST logical type inference for external tables. "
            "Default: true. Use --no-parquet-enable-list-inference to disable."
        ),
    )


def add_sanitizer_args(parser: argparse.ArgumentParser) -> None:
    """Add schema-sanitizer conversion arguments."""
    parser.add_argument(
        "--schema-mode",
        choices=("strict", "additive"),
        default="strict",
        help=(
            "strict enforces the existing BigQuery schema exactly; additive allows "
            "new observed fields. On the first run, use additive because there is "
            "no existing BigQuery schema yet."
        ),
    )
    parser.add_argument(
        "--on-error",
        choices=("stop", "skip_row", "emit_null_row"),
        default="emit_null_row",
        help=(
            "Row-level policy for values that cannot be coerced into the schema registry contract. "
            "Default: emit_null_row, which keeps row count stable."
        ),
    )
    parser.add_argument(
        "--column-order",
        choices=("alphabetically", "schema_contract_first"),
        default="alphabetically",
        help=(
            "Output field ordering. Default: alphabetically. "
            "Use schema_contract_first when preserving an existing registry schema order matters more."
        ),
    )
    parser.add_argument(
        "--field-name-policy",
        choices=("lower_alpha", "lower_snake", "preserve"),
        default="lower_snake",
        help=(
            "Output field-name policy. lower_snake keeps lowercase a-z, numbers, "
            "and underscores, which supports registry fields such as "
            "sentences_v2_struct_array."
        ),
    )
    parser.add_argument(
        "--timestamp-precision",
        choices=("TIMESTAMP_MILLIS", "TIMESTAMP_MICROS", "TIMESTAMP_NANOS"),
        default="TIMESTAMP_MICROS",
        help=(
            "Output Arrow/Parquet timestamp precision. TIMESTAMP_MICROS is the "
            "default because BigQuery external tables do not support Parquet "
            "TIMESTAMP_NANOS."
        ),
    )
    parser.add_argument(
        "--parquet-compression",
        choices=("gzip", "uncompressed"),
        default="gzip",
        help=("Parquet output compression passed to schema_sanitizer.to_parquet. Default: gzip."),
    )
    parser.add_argument(
        "--parquet-gzip-level",
        type=int,
        choices=range(0, 10),
        metavar="0..9",
        help=(
            "Optional gzip compression level for Parquet output. Valid only when "
            "--parquet-compression gzip is used. Default: writer/zlib default."
        ),
    )
    parser.add_argument(
        "--parse-integers",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Parse integer-looking strings as integers during inference and "
            "materialization. Default: true."
        ),
    )
    parser.add_argument(
        "--parse-floats",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Parse float-looking strings as floats during inference and "
            "materialization. Default: true."
        ),
    )
    parser.add_argument(
        "--parse-float-decimal-separator",
        default=".",
        help="Decimal separator used for float-looking strings. Default: '.'.",
    )
    parser.add_argument(
        "--parse-float-thousands-separator",
        default=",",
        help=("Thousands separator used for grouped float-looking strings. Default: ','."),
    )
    parser.add_argument(
        "--parse-iso-timestamps",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Parse built-in ISO timestamp strings. Default: true.",
    )
    parser.add_argument(
        "--parse-iso-dates",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Parse built-in ISO date strings. Default: true.",
    )
    parser.add_argument(
        "--parse-iso-times",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Parse built-in ISO time strings. Default: true.",
    )
    parser.add_argument(
        "--batch-memory-limit-bytes",
        type=int,
        default=DEFAULT_BATCH_MEMORY_LIMIT_BYTES,
        help=(
            "Best-effort native per-batch memory budget. Default: "
            f"{DEFAULT_BATCH_MEMORY_LIMIT_BYTES} bytes (64 MiB). Keep this well "
            "below machine/container memory because Arrow, Parquet, GCS, and "
            "Python overhead add to process RSS."
        ),
    )
    parser.add_argument(
        "--read-chunk-bytes",
        type=int,
        default=DEFAULT_READ_CHUNK_BYTES,
        help=(
            "Streaming read chunk size. Default: "
            f"{DEFAULT_READ_CHUNK_BYTES} bytes (256 KiB), conservative for large GCS JSON arrays."
        ),
    )
    parser.add_argument("--arrow-max-depth", type=int, default=32)
    parser.add_argument("--parquet-max-depth", type=int, default=15)
    parser.add_argument("--input-text-encoding", default="utf-8")


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    """Add logging verbosity arguments."""
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Logging level. Default: INFO.",
    )
