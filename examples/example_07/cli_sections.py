"""BigQuery and logging argument builders for example 07."""

from __future__ import annotations

import argparse

from schema_sanitizer.integrations.bigquery.advanced import (
    parse_hive_partition_column as _parse_hive_partition_column_value,
)


def _parse_hive_partition_column(raw: str) -> tuple[str, str]:
    """Parse a CLI partition column declaration."""
    try:
        return _parse_hive_partition_column_value(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


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


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    """Add logging verbosity arguments."""
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Logging level. Default: INFO.",
    )
