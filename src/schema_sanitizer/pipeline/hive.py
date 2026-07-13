"""Hive-style partition planning and argparse adapters."""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta

from ..core_impl.hive_uris import (
    build_partition_directory_uri,
    build_partitioned_file_uri,
    normalize_file_extension,
    render_uri_for_partition,
    uri_path_segments,
)
from ..input_impl.selection import input_format_extensions
from .types import PartitionRunPlan


@dataclass(frozen=True)
class HiveRangeConfig:
    """Configuration for a partitioned source/output URI range."""

    source_prefix: str | None = None
    output_prefix: str | None = None
    source_uri: str | None = None
    output_uri: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    start_hour: int = 0
    end_hour: int = 23
    partition_granularity: str = "daily"
    input_format: str = "json_array"
    input_mode: str = "single_file"
    file_name_prefix: str | None = None
    source_file_extension: str | None = None
    output_file_extension: str = "parquet"


def parse_iso_date(raw: str) -> date:
    """Parse an ISO yyyy-mm-dd CLI date."""
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date {raw!r}. Use ISO format YYYY-MM-DD, e.g. 2026-01-01."
        ) from exc


def parse_hour(raw: str) -> int:
    """Parse one inclusive hour bound in the range 0-23."""
    try:
        hour = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid hour {raw!r}. Use an integer from 0 to 23."
        ) from exc
    if not 0 <= hour <= 23:
        raise argparse.ArgumentTypeError(f"Invalid hour {raw!r}. Use an integer from 0 to 23.")
    return hour


def _iter_partition_points(
    start_date: date,
    end_date: date,
    *,
    granularity: str,
    start_hour: int,
    end_hour: int,
) -> Iterator[tuple[date, int | None]]:
    """Yield daily or hourly logical partition points without an intermediate list."""
    if start_date > end_date:
        raise ValueError(f"--start-date must be <= --end-date. Got {start_date} > {end_date}.")
    if granularity not in {"daily", "hourly"}:
        raise ValueError(f"Unsupported partition granularity: {granularity!r}")
    if granularity == "hourly":
        if not 0 <= start_hour <= 23 or not 0 <= end_hour <= 23:
            raise ValueError("Hourly partition bounds must be between 0 and 23.")
        if start_hour > end_hour:
            raise ValueError(f"--start-hour must be <= --end-hour. Got {start_hour} > {end_hour}.")

    logical_date = start_date
    while logical_date <= end_date:
        if granularity == "daily":
            yield logical_date, None
        else:
            for hour in range(start_hour, end_hour + 1):
                yield logical_date, hour
        logical_date += timedelta(days=1)


def _source_file_extensions(config: HiveRangeConfig) -> tuple[str, ...]:
    """Return source extensions accepted by this plan."""
    valid_extensions = input_format_extensions(config.input_format)
    if not config.source_file_extension:
        return valid_extensions
    extension = normalize_file_extension(config.source_file_extension)
    if extension not in valid_extensions:
        expected = ", ".join(f".{value}" for value in valid_extensions)
        raise ValueError(
            f"--input-format={config.input_format!r} requires extension "
            f"{expected}; got .{extension}."
        )
    return (extension,)


def _file_name_prefix(config: HiveRangeConfig) -> str:
    """Return the explicit or path-derived generated filename prefix."""
    if config.file_name_prefix:
        return config.file_name_prefix.strip()
    if not config.source_prefix:
        raise ValueError("--file-name-prefix is required when no source prefix is configured")
    segments = uri_path_segments(config.source_prefix)
    if not segments:
        raise ValueError(
            "Could not infer --file-name-prefix from --source-jsonl-prefix. "
            "Pass --file-name-prefix explicitly, e.g. --file-name-prefix events."
        )
    mode_like_segments = {"rt", "full", "batch", "daily", "hourly", "streaming"}
    if len(segments) >= 2 and segments[-1].lower() in mode_like_segments:
        return segments[-2]
    return segments[-1]


def _uri_selection_mode(config: HiveRangeConfig) -> str:
    """Return uri or prefix after validating mutually exclusive URI inputs."""
    has_uri = bool(config.source_uri or config.output_uri)
    has_prefix = bool(config.source_prefix or config.output_prefix)
    if has_uri and has_prefix:
        raise ValueError(
            "Use either full URI arguments (--source-jsonl-uri/--silver-parquet-uri) "
            "or prefix arguments (--source-jsonl-prefix/--silver-parquet-prefix), not both."
        )
    if has_prefix:
        missing = [
            name
            for value, name in (
                (config.source_prefix, "--source-jsonl-prefix"),
                (config.output_prefix, "--silver-parquet-prefix"),
            )
            if not value
        ]
        if missing:
            raise ValueError("Prefix mode requires: " + ", ".join(missing))
        return "prefix"
    missing = [
        name
        for value, name in (
            (config.source_uri, "--source-jsonl-uri"),
            (config.output_uri, "--silver-parquet-uri"),
        )
        if not value
    ]
    if missing:
        raise ValueError(
            "URI mode requires: "
            + ", ".join(missing)
            + "; or use --source-jsonl-prefix and --silver-parquet-prefix."
        )
    return "uri"


def _validate_unique_outputs(plans: list[PartitionRunPlan]) -> None:
    """Reject plans that would overwrite the same partition output in one pass."""
    seen: set[str] = set()
    duplicates: set[str] = set()
    for plan in plans:
        if plan.output_uri in seen:
            duplicates.add(plan.output_uri)
        else:
            seen.add(plan.output_uri)
    if duplicates:
        raise ValueError(
            "Partition range produced duplicate output Parquet URIs. "
            f"Check your URI template/prefix. Duplicates: {sorted(duplicates)}"
        )


def build_hive_range_plan(config: HiveRangeConfig) -> list[PartitionRunPlan]:
    """Build the single-partition or date/hour-range execution plan."""
    source_extensions = _source_file_extensions(config)
    if config.input_mode == "directory" and config.source_file_extension:
        raise ValueError(
            "--source-file-extension is only valid with --input-mode=single_file. "
            "Directory mode processes every extension accepted by --input-format."
        )
    selection_mode = _uri_selection_mode(config)
    if selection_mode == "uri" and config.start_date is None and config.end_date is None:
        return [
            PartitionRunPlan(
                logical_date=None,
                logical_hour=None,
                source_uri=str(config.source_uri),
                output_uri=str(config.output_uri),
            )
        ]
    if config.start_date is None or config.end_date is None:
        message = (
            "Prefix mode requires both --start-date and --end-date so Hive partition paths "
            "can be generated."
            if selection_mode == "prefix"
            else "Pass both --start-date and --end-date, or neither."
        )
        raise ValueError(message)

    points = _iter_partition_points(
        config.start_date,
        config.end_date,
        granularity=config.partition_granularity,
        start_hour=config.start_hour,
        end_hour=config.end_hour,
    )
    if selection_mode == "uri":
        source_uri = str(config.source_uri)
        output_uri = str(config.output_uri)
        plans = [
            PartitionRunPlan(
                logical_date=logical_date,
                logical_hour=logical_hour,
                source_uri=render_uri_for_partition(source_uri, logical_date, logical_hour),
                output_uri=render_uri_for_partition(output_uri, logical_date, logical_hour),
            )
            for logical_date, logical_hour in points
        ]
    else:
        generated_file_prefix = _file_name_prefix(config)
        source_prefix = str(config.source_prefix)
        output_prefix = str(config.output_prefix)
        source_extension = source_extensions[0]
        output_extension = normalize_file_extension(config.output_file_extension)
        plans = [
            PartitionRunPlan(
                logical_date=logical_date,
                logical_hour=logical_hour,
                source_uri=(
                    build_partition_directory_uri(
                        source_prefix, logical_date, logical_hour=logical_hour
                    )
                    if config.input_mode == "directory"
                    else build_partitioned_file_uri(
                        source_prefix,
                        logical_date,
                        logical_hour=logical_hour,
                        file_name_prefix=generated_file_prefix,
                        extension=source_extension,
                    )
                ),
                output_uri=build_partitioned_file_uri(
                    output_prefix,
                    logical_date,
                    logical_hour=logical_hour,
                    file_name_prefix=generated_file_prefix,
                    extension=output_extension,
                ),
            )
            for logical_date, logical_hour in points
        ]
    _validate_unique_outputs(plans)
    return plans


def hive_config_from_namespace(args: argparse.Namespace) -> HiveRangeConfig:
    """Build a HiveRangeConfig from the example-compatible CLI namespace."""
    return HiveRangeConfig(
        source_prefix=getattr(args, "source_jsonl_prefix", None),
        output_prefix=getattr(args, "silver_parquet_prefix", None),
        source_uri=getattr(args, "source_jsonl_uri", None),
        output_uri=getattr(args, "silver_parquet_uri", None),
        start_date=getattr(args, "start_date", None),
        end_date=getattr(args, "end_date", None),
        start_hour=0 if getattr(args, "start_hour", None) is None else args.start_hour,
        end_hour=23 if getattr(args, "end_hour", None) is None else args.end_hour,
        partition_granularity=getattr(args, "partition_granularity", "daily"),
        input_format=getattr(args, "input_format", "json_array"),
        input_mode=getattr(args, "input_mode", "single_file"),
        file_name_prefix=getattr(args, "file_name_prefix", None),
        source_file_extension=getattr(args, "source_file_extension", None),
        output_file_extension=getattr(args, "silver_parquet_extension", "parquet"),
    )


def build_hive_range_plan_from_namespace(args: argparse.Namespace) -> list[PartitionRunPlan]:
    """Build a Hive range plan from an argparse namespace."""
    return build_hive_range_plan(hive_config_from_namespace(args))


def build_warm_up_hive_range_plan_from_namespace(
    args: argparse.Namespace,
) -> list[PartitionRunPlan]:
    """Build a warm-up range plan from warm-up date/hour namespace fields."""
    start_date = getattr(args, "start_date_warm_up", None)
    end_date = getattr(args, "end_date_warm_up", None)
    if start_date is None and end_date is None:
        return []
    if start_date is None or end_date is None:
        raise ValueError("Pass both --start-date-warm-up and --end-date-warm-up, or neither.")
    warm_up_args = argparse.Namespace(**vars(args))
    warm_up_args.start_date = start_date
    warm_up_args.end_date = end_date
    warm_up_args.start_hour = getattr(args, "start_hour_warm_up", None)
    warm_up_args.end_hour = getattr(args, "end_hour_warm_up", None)
    return build_hive_range_plan_from_namespace(warm_up_args)
