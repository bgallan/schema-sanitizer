"""Command-line parser for the BigQuery range-prefix example."""

from __future__ import annotations

import argparse

NORMAL_HOUR_ARGS = ("start_hour", "end_hour")
WARM_UP_DATE_ARGS = ("start_date_warm_up", "end_date_warm_up")
WARM_UP_HOUR_ARGS = ("start_hour_warm_up", "end_hour_warm_up")

try:
    from examples.example_07.cli_sections import (
        add_bigquery_args,
        add_external_table_args,
        add_logging_args,
    )
    from examples.example_07.cli_sanitizer_args import add_sanitizer_args
    from examples.example_07.cli_source_args import add_source_args
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from cli_sections import (
        add_bigquery_args,
        add_external_table_args,
        add_logging_args,
    )
    from cli_sanitizer_args import add_sanitizer_args
    from cli_source_args import add_source_args


def _any_set(args: argparse.Namespace, names: tuple[str, ...]) -> bool:
    """Return whether any argparse destination is present and non-None."""
    return any(getattr(args, name, None) is not None for name in names)


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Validate cross-option consistency for date/hour range arguments."""
    normal_hours_set = _any_set(args, NORMAL_HOUR_ARGS)
    warm_up_dates_set = _any_set(args, WARM_UP_DATE_ARGS)
    warm_up_hours_set = _any_set(args, WARM_UP_HOUR_ARGS)

    if (normal_hours_set or warm_up_hours_set) and args.partition_granularity != "hourly":
        parser.error(
            "--start-hour/--end-hour and --start-hour-warm-up/--end-hour-warm-up "
            "require --partition-granularity hourly."
        )

    if warm_up_hours_set and not warm_up_dates_set:
        parser.error(
            "--start-hour-warm-up/--end-hour-warm-up require "
            "--start-date-warm-up and --end-date-warm-up."
        )

    if warm_up_dates_set and (
        args.start_date_warm_up is None or args.end_date_warm_up is None
    ):
        parser.error("Pass both --start-date-warm-up and --end-date-warm-up, or neither.")

    if args.partition_granularity != "hourly":
        return

    start_hour = 0 if args.start_hour is None else args.start_hour
    end_hour = 23 if args.end_hour is None else args.end_hour
    if start_hour > end_hour:
        parser.error(f"--start-hour must be <= --end-hour. Got {start_hour} > {end_hour}.")

    if warm_up_dates_set:
        start_hour_warm_up = (
            0 if args.start_hour_warm_up is None else args.start_hour_warm_up
        )
        end_hour_warm_up = 23 if args.end_hour_warm_up is None else args.end_hour_warm_up
        if start_hour_warm_up > end_hour_warm_up:
            parser.error(
                "--start-hour-warm-up must be <= --end-hour-warm-up. "
                f"Got {start_hour_warm_up} > {end_hour_warm_up}."
            )


class ExampleArgumentParser(argparse.ArgumentParser):
    """ArgumentParser with example-specific cross-option validation."""

    def parse_args(self, args=None, namespace=None):  # type: ignore[override]
        """Parse arguments and enforce stable date/hour combinations."""
        parsed = super().parse_args(args=args, namespace=namespace)
        _validate_args(self, parsed)
        return parsed


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = ExampleArgumentParser(
        description=(
            "Read one source file/directory or a range of daily/hourly Hive "
            "partitions from GCS, write sanitized Parquet to silver GCS, and create/update a "
            "Hive-partitioned BigQuery external table over the final Parquet output."
        )
    )
    add_source_args(parser)
    add_bigquery_args(parser)
    add_external_table_args(parser)
    add_sanitizer_args(parser)
    add_logging_args(parser)
    return parser
