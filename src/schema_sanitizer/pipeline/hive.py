"""Hive-style date/hour partition planning helpers."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import date, timedelta

from .types import PartitionRunPlan

FORMAT_EXTENSIONS: dict[str, tuple[str, ...]] = {
    "csv": ("csv",),
    "json": ("json",),
    "json_array": ("json",),
    "jsonl": ("jsonl",),
    "ndjson": ("ndjson",),
    "xml": ("xml",),
    "parquet": ("parquet", "pq"),
}


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


def iter_inclusive_dates(start_date: date, end_date: date) -> list[date]:
    """Return all dates in the inclusive [start_date, end_date] range."""
    if start_date > end_date:
        raise ValueError(f"--start-date must be <= --end-date. Got {start_date} > {end_date}.")
    days = (end_date - start_date).days
    return [start_date + timedelta(days=offset) for offset in range(days + 1)]


def iter_partition_points(
    start_date: date,
    end_date: date,
    *,
    granularity: str,
    start_hour: int,
    end_hour: int,
) -> list[tuple[date, int | None]]:
    """Return daily or hourly logical partition points."""
    dates = iter_inclusive_dates(start_date, end_date)
    if granularity == "daily":
        return [(logical_date, None) for logical_date in dates]
    if granularity != "hourly":
        raise ValueError(f"Unsupported partition granularity: {granularity!r}")
    if not 0 <= start_hour <= 23 or not 0 <= end_hour <= 23:
        raise ValueError("Hourly partition bounds must be between 0 and 23.")
    if start_hour > end_hour:
        raise ValueError(f"--start-hour must be <= --end-hour. Got {start_hour} > {end_hour}.")
    return [
        (logical_date, hour) for logical_date in dates for hour in range(start_hour, end_hour + 1)
    ]


def format_uri_template_values(
    logical_date: date,
    logical_hour: int | None = None,
) -> dict[str, str]:
    """Return supported date/hour placeholders for URI templates."""
    values = {
        "date": logical_date.isoformat(),
        "yyyymmdd": logical_date.strftime("%Y%m%d"),
        "yyyy": logical_date.strftime("%Y"),
        "year": logical_date.strftime("%Y"),
        "mm": logical_date.strftime("%m"),
        "month": logical_date.strftime("%m"),
        "dd": logical_date.strftime("%d"),
        "day": logical_date.strftime("%d"),
    }
    if logical_hour is not None:
        hour = f"{logical_hour:02d}"
        values.update(
            {
                "hour": hour,
                "hh": hour,
                "yyyymmddhh": logical_date.strftime("%Y%m%d") + hour,
            }
        )
    return values


def normalize_uri_prefix(prefix: str) -> str:
    """Normalize a URI prefix by removing trailing slashes."""
    normalized = prefix.strip().rstrip("/")
    if "://" not in normalized:
        raise ValueError(f"Expected a URI prefix, got {prefix!r}")
    return normalized


def normalize_file_extension(extension: str) -> str:
    """Normalize a file extension passed as json, .json, parquet, etc."""
    normalized = extension.strip().lstrip(".")
    if not normalized or "/" in normalized:
        raise ValueError(f"Invalid file extension: {extension!r}")
    return normalized


def source_file_extension(config: HiveRangeConfig) -> str:
    """Return a source extension valid for the selected input format."""
    valid_extensions = FORMAT_EXTENSIONS[config.input_format]
    if config.source_file_extension:
        extension = normalize_file_extension(config.source_file_extension)
        if extension not in valid_extensions:
            expected = ", ".join(f".{value}" for value in valid_extensions)
            raise ValueError(
                f"--input-format={config.input_format!r} requires extension "
                f"{expected}; got .{extension}."
            )
        return extension
    return valid_extensions[0]


def source_file_extensions(config: HiveRangeConfig) -> tuple[str, ...]:
    """Return source extensions accepted during directory discovery."""
    if config.source_file_extension:
        return (source_file_extension(config),)
    return FORMAT_EXTENSIONS[config.input_format]


def uri_path_segments(uri: str) -> list[str]:
    """Return non-empty path segments from a URI."""
    if "://" not in uri:
        return []
    without_scheme = uri.split("://", 1)[1]
    if "/" not in without_scheme:
        return []
    _authority, path = without_scheme.split("/", 1)
    return [segment for segment in path.split("/") if segment]


def gcs_uri_path_segments(uri: str) -> list[str]:
    """Return non-empty path segments from a gs:// URI."""
    if not uri.startswith("gs://"):
        return []
    return uri_path_segments(uri)


def infer_file_name_prefix(source_prefix: str) -> str:
    """Infer the generated file prefix, e.g. events from gs://.../events/rt."""
    segments = uri_path_segments(source_prefix)
    if not segments:
        raise ValueError(
            "Could not infer --file-name-prefix from --source-jsonl-prefix. "
            "Pass --file-name-prefix explicitly, e.g. --file-name-prefix events."
        )
    mode_like_segments = {"rt", "full", "batch", "daily", "hourly", "streaming"}
    if len(segments) >= 2 and segments[-1].lower() in mode_like_segments:
        return segments[-2]
    return segments[-1]


def file_name_prefix(config: HiveRangeConfig) -> str:
    """Return the filename prefix for generated partition files."""
    if config.file_name_prefix:
        return config.file_name_prefix.strip()
    if not config.source_prefix:
        raise ValueError("--file-name-prefix is required when no source prefix is configured")
    return infer_file_name_prefix(config.source_prefix)


def build_partition_directory_uri(
    prefix: str,
    logical_date: date,
    *,
    logical_hour: int | None,
) -> str:
    """Build a daily or hourly Hive partition directory URI."""
    values = format_uri_template_values(logical_date, logical_hour)
    clean_prefix = normalize_uri_prefix(prefix)
    uri = f"{clean_prefix}/year={values['year']}/month={values['month']}/date={values['date']}"
    if logical_hour is not None:
        uri += f"/hour={values['hour']}"
    return uri


def build_partitioned_file_uri(
    prefix: str,
    logical_date: date,
    *,
    logical_hour: int | None,
    file_name_prefix: str,
    extension: str,
) -> str:
    """Build one generated file URI inside a daily or hourly partition."""
    values = format_uri_template_values(logical_date, logical_hour)
    partition_uri = build_partition_directory_uri(
        prefix,
        logical_date,
        logical_hour=logical_hour,
    )
    suffix = values["yyyymmdd"]
    if logical_hour is not None:
        suffix += f"_{values['hour']}"
    return f"{partition_uri}/{file_name_prefix}_{suffix}.{normalize_file_extension(extension)}"


def uri_selection_mode(config: HiveRangeConfig) -> str:
    """Return uri or prefix after validating mutually exclusive URI inputs."""
    has_uri = bool(config.source_uri or config.output_uri)
    has_prefix = bool(config.source_prefix or config.output_prefix)
    if has_uri and has_prefix:
        raise ValueError(
            "Use either full URI arguments (--source-jsonl-uri/--silver-parquet-uri) "
            "or prefix arguments (--source-jsonl-prefix/--silver-parquet-prefix), not both."
        )
    if has_prefix:
        missing = []
        if not config.source_prefix:
            missing.append("--source-jsonl-prefix")
        if not config.output_prefix:
            missing.append("--silver-parquet-prefix")
        if missing:
            raise ValueError("Prefix mode requires: " + ", ".join(missing))
        return "prefix"
    missing = []
    if not config.source_uri:
        missing.append("--source-jsonl-uri")
    if not config.output_uri:
        missing.append("--silver-parquet-uri")
    if missing:
        raise ValueError(
            "URI mode requires: "
            + ", ".join(missing)
            + "; or use --source-jsonl-prefix and --silver-parquet-prefix."
        )
    return "uri"


def render_explicit_partition_placeholders(
    uri_template: str,
    logical_date: date,
    logical_hour: int | None,
) -> tuple[str, bool]:
    """Render explicit URI placeholders for one logical partition."""
    rendered = uri_template
    changed = False
    for name, value in format_uri_template_values(logical_date, logical_hour).items():
        placeholder = "{" + name + "}"
        if placeholder in rendered:
            rendered = rendered.replace(placeholder, value)
            changed = True
    return rendered, changed


def infer_anchor_date_from_uri(uri: str) -> date | None:
    """Infer the template anchor date from date=YYYY-MM-DD or any ISO date in a URI."""
    match = re.search(r"(?:^|/)date=(\d{4}-\d{2}-\d{2})(?:/|$)", uri)
    if match:
        return date.fromisoformat(match.group(1))
    match = re.search(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)", uri)
    if match:
        return date.fromisoformat(match.group(1))
    return None


def infer_anchor_hour_from_uri(uri: str) -> int | None:
    """Infer an anchor hour from an hour=HH Hive path."""
    match = re.search(r"(?:^|/)hour=(\d{2})(?:/|$)", uri)
    if not match:
        return None
    hour = int(match.group(1))
    return hour if 0 <= hour <= 23 else None


def roll_hive_partition_uri(
    uri_template: str,
    logical_date: date,
    logical_hour: int | None,
) -> str:
    """Roll a concrete Hive-style URI to another logical partition."""
    anchor_date = infer_anchor_date_from_uri(uri_template)
    if anchor_date is None:
        raise ValueError(
            "When --start-date/--end-date are used, --source-jsonl-uri and "
            "--silver-parquet-uri must either contain explicit placeholders "
            "({date}, {yyyymmdd}, {year}, {month}, {day}, {hour}) or include a "
            "date=YYYY-MM-DD Hive partition path that can be rolled per day."
        )
    target_values = format_uri_template_values(logical_date, logical_hour)
    anchor_compact = anchor_date.strftime("%Y%m%d")
    target_compact = target_values["yyyymmdd"]
    rendered = uri_template
    rendered = re.sub(r"year=\d{4}", f"year={target_values['year']}", rendered)
    rendered = re.sub(r"month=\d{2}", f"month={target_values['month']}", rendered)
    rendered = re.sub(r"date=\d{4}-\d{2}-\d{2}", f"date={target_values['date']}", rendered)
    rendered = rendered.replace(anchor_date.isoformat(), target_values["date"])
    if logical_hour is not None:
        anchor_hour = infer_anchor_hour_from_uri(uri_template)
        if anchor_hour is None:
            raise ValueError(
                "Hourly URI range mode requires an {hour}/{hh}/{yyyymmddhh} "
                "placeholder or an hour=HH Hive partition in both URI templates."
            )
        rendered = rendered.replace(
            anchor_compact + f"{anchor_hour:02d}",
            target_values["yyyymmddhh"],
        )
        rendered = re.sub(r"hour=\d{2}", f"hour={target_values['hour']}", rendered)
    rendered = rendered.replace(anchor_compact, target_compact)
    return rendered


def render_uri_for_partition(
    uri_template: str,
    logical_date: date,
    logical_hour: int | None,
) -> str:
    """Render a source/output URI for one logical partition."""
    rendered, used_placeholders = render_explicit_partition_placeholders(
        uri_template,
        logical_date,
        logical_hour,
    )
    if used_placeholders:
        if logical_hour is not None and not any(
            placeholder in uri_template for placeholder in ("{hour}", "{hh}", "{yyyymmddhh}")
        ):
            raise ValueError("Hourly URI templates must include {hour}, {hh}, or {yyyymmddhh}.")
        return rendered
    return roll_hive_partition_uri(uri_template, logical_date, logical_hour)


def build_hive_range_plan(config: HiveRangeConfig) -> list[PartitionRunPlan]:
    """Build the single-partition or date/hour-range execution plan."""
    source_file_extensions(config)
    if config.input_mode == "directory" and config.source_file_extension:
        raise ValueError(
            "--source-file-extension is only valid with --input-mode=single_file. "
            "Directory mode processes every extension accepted by --input-format."
        )
    selection_mode = uri_selection_mode(config)
    if selection_mode == "uri":
        if config.start_date is None and config.end_date is None:
            return [
                PartitionRunPlan(
                    logical_date=None,
                    logical_hour=None,
                    source_uri=str(config.source_uri),
                    output_uri=str(config.output_uri),
                )
            ]
        if config.start_date is None or config.end_date is None:
            raise ValueError("Pass both --start-date and --end-date, or neither.")
        points = iter_partition_points(
            config.start_date,
            config.end_date,
            granularity=config.partition_granularity,
            start_hour=config.start_hour,
            end_hour=config.end_hour,
        )
        plans = [
            PartitionRunPlan(
                logical_date=logical_date,
                logical_hour=logical_hour,
                source_uri=render_uri_for_partition(
                    str(config.source_uri),
                    logical_date,
                    logical_hour,
                ),
                output_uri=render_uri_for_partition(
                    str(config.output_uri),
                    logical_date,
                    logical_hour,
                ),
            )
            for logical_date, logical_hour in points
        ]
    else:
        if config.start_date is None or config.end_date is None:
            raise ValueError(
                "Prefix mode requires both --start-date and --end-date so Hive "
                "partition paths can be generated."
            )
        generated_file_prefix = file_name_prefix(config)
        points = iter_partition_points(
            config.start_date,
            config.end_date,
            granularity=config.partition_granularity,
            start_hour=config.start_hour,
            end_hour=config.end_hour,
        )
        plans = [
            PartitionRunPlan(
                logical_date=logical_date,
                logical_hour=logical_hour,
                source_uri=(
                    build_partition_directory_uri(
                        str(config.source_prefix),
                        logical_date,
                        logical_hour=logical_hour,
                    )
                    if config.input_mode == "directory"
                    else build_partitioned_file_uri(
                        str(config.source_prefix),
                        logical_date,
                        logical_hour=logical_hour,
                        file_name_prefix=generated_file_prefix,
                        extension=source_file_extension(config),
                    )
                ),
                output_uri=build_partitioned_file_uri(
                    str(config.output_prefix),
                    logical_date,
                    logical_hour=logical_hour,
                    file_name_prefix=generated_file_prefix,
                    extension=config.output_file_extension,
                ),
            )
            for logical_date, logical_hour in points
        ]
    duplicate_outputs = sorted(
        uri
        for uri in {plan.output_uri for plan in plans}
        if sum(plan.output_uri == uri for plan in plans) > 1
    )
    if duplicate_outputs:
        raise ValueError(
            "Partition range produced duplicate output Parquet URIs. "
            f"Check your URI template/prefix. Duplicates: {duplicate_outputs}"
        )
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


def has_warm_up_date_range(args: argparse.Namespace) -> bool:
    """Return whether a schema warm-up range was requested."""
    return (
        getattr(args, "start_date_warm_up", None) is not None
        or getattr(args, "end_date_warm_up", None) is not None
    )


def build_warm_up_hive_range_plan_from_namespace(
    args: argparse.Namespace,
) -> list[PartitionRunPlan]:
    """Build a warm-up range plan from warm-up date/hour namespace fields."""
    if not has_warm_up_date_range(args):
        return []
    if (
        getattr(args, "start_date_warm_up", None) is None
        or getattr(args, "end_date_warm_up", None) is None
    ):
        raise ValueError("Pass both --start-date-warm-up and --end-date-warm-up, or neither.")
    warm_up_args = argparse.Namespace(**vars(args))
    warm_up_args.start_date = args.start_date_warm_up
    warm_up_args.end_date = args.end_date_warm_up
    warm_up_args.start_hour = (
        0 if getattr(args, "start_hour_warm_up", None) is None else args.start_hour_warm_up
    )
    warm_up_args.end_hour = (
        23 if getattr(args, "end_hour_warm_up", None) is None else args.end_hour_warm_up
    )
    return build_hive_range_plan_from_namespace(warm_up_args)
