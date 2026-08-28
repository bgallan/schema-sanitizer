"""Construct, normalize, and roll Hive-style partition URIs.

Extensions and prefixes are validated before partition directories and filenames are rendered,
including explicit placeholders and extraction of anchor dates or hours from existing paths.
"""

from __future__ import annotations

import re
from datetime import date
from functools import lru_cache

_HIVE_DATE_PATH_RE = re.compile(r"(?:^|/)date=(\d{4}-\d{2}-\d{2})(?:/|$)")
_ISO_DATE_RE = re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)")
_HIVE_HOUR_PATH_RE = re.compile(r"(?:^|/)hour=(\d{2})(?:/|$)")
_HIVE_YEAR_RE = re.compile(r"year=\d{4}")
_HIVE_MONTH_RE = re.compile(r"month=\d{2}")
_HIVE_DATE_RE = re.compile(r"date=\d{4}-\d{2}-\d{2}")
_HIVE_HOUR_RE = re.compile(r"hour=\d{2}")
_TEMPLATE_FIELD_NAMES = (
    "date",
    "yyyymmdd",
    "yyyy",
    "year",
    "mm",
    "month",
    "dd",
    "day",
    "hour",
    "hh",
    "yyyymmddhh",
)
_HOURLY_TEMPLATE_FIELDS = frozenset({"hour", "hh", "yyyymmddhh"})


@lru_cache(maxsize=4096)
def _uri_template_values(
    logical_date: date,
    logical_hour: int | None = None,
) -> dict[str, str]:
    """Return cached date/hour placeholder values for URI templates."""
    year = f"{logical_date.year:04d}"
    month = f"{logical_date.month:02d}"
    day = f"{logical_date.day:02d}"
    iso_date = f"{year}-{month}-{day}"
    compact_date = f"{year}{month}{day}"
    values = {
        "date": iso_date,
        "yyyymmdd": compact_date,
        "yyyy": year,
        "year": year,
        "mm": month,
        "month": month,
        "dd": day,
        "day": day,
    }
    if logical_hour is not None:
        hour = f"{logical_hour:02d}"
        values.update({"hour": hour, "hh": hour, "yyyymmddhh": compact_date + hour})
    return values


@lru_cache(maxsize=128)
def normalize_file_extension(extension: str) -> str:
    """Normalize a file extension passed as json, .json, parquet, etc."""
    normalized = extension.strip().lstrip(".")
    if not normalized or "/" in normalized:
        raise ValueError(f"Invalid file extension: {extension!r}")
    return normalized


@lru_cache(maxsize=128)
def normalize_uri_prefix(prefix: str) -> str:
    """Normalize a URI prefix by removing trailing slashes."""
    normalized = prefix.strip().rstrip("/")
    if "://" not in normalized:
        raise ValueError(f"Expected a URI prefix, got {prefix!r}")
    return normalized


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
    return uri_path_segments(uri) if uri.startswith("gs://") else []


def _partition_directory_uri(
    prefix: str,
    values: dict[str, str],
    *,
    include_hour: bool,
) -> str:
    """Build a partition directory from already-normalized values."""
    uri = (
        f"{normalize_uri_prefix(prefix)}/year={values['year']}"
        f"/month={values['month']}/date={values['date']}"
    )
    return f"{uri}/hour={values['hour']}" if include_hour else uri


def build_partition_directory_uri(
    prefix: str,
    logical_date: date,
    *,
    logical_hour: int | None,
) -> str:
    """Build a daily or hourly Hive partition directory URI."""
    values = _uri_template_values(logical_date, logical_hour)
    return _partition_directory_uri(prefix, values, include_hour=logical_hour is not None)


def build_partitioned_file_uri(
    prefix: str,
    logical_date: date,
    *,
    logical_hour: int | None,
    file_name_prefix: str,
    extension: str,
) -> str:
    """Build one generated file URI inside a daily or hourly partition."""
    values = _uri_template_values(logical_date, logical_hour)
    partition_uri = _partition_directory_uri(
        prefix,
        values,
        include_hour=logical_hour is not None,
    )
    suffix = values["yyyymmdd"]
    if logical_hour is not None:
        suffix += f"_{values['hour']}"
    return f"{partition_uri}/{file_name_prefix}_{suffix}.{normalize_file_extension(extension)}"


@lru_cache(maxsize=256)
def _template_fields(uri_template: str) -> tuple[str, ...]:
    """Return supported placeholder names present in one URI template."""
    return tuple(name for name in _TEMPLATE_FIELD_NAMES if "{" + name + "}" in uri_template)


def render_explicit_partition_placeholders(
    uri_template: str,
    logical_date: date,
    logical_hour: int | None,
) -> tuple[str, bool]:
    """Render explicit URI placeholders for one logical partition."""
    fields = _template_fields(uri_template)
    if not fields:
        return uri_template, False
    values = _uri_template_values(logical_date, logical_hour)
    rendered = uri_template
    changed = False
    for name in fields:
        value = values.get(name)
        if value is not None:
            rendered = rendered.replace("{" + name + "}", value)
            changed = True
    return rendered, changed


def infer_anchor_date_from_uri(uri: str) -> date | None:
    """Infer an anchor date from a concrete Hive or ISO URI."""
    match = _HIVE_DATE_PATH_RE.search(uri)
    if match:
        return date.fromisoformat(match.group(1))
    match = _ISO_DATE_RE.search(uri)
    return date.fromisoformat(match.group(1)) if match else None


def infer_anchor_hour_from_uri(uri: str) -> int | None:
    """Infer an anchor hour from an hour=HH Hive path."""
    match = _HIVE_HOUR_PATH_RE.search(uri)
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
    target_values = _uri_template_values(logical_date, logical_hour)
    anchor_compact = f"{anchor_date.year:04d}{anchor_date.month:02d}{anchor_date.day:02d}"
    rendered = uri_template
    rendered = _HIVE_YEAR_RE.sub(f"year={target_values['year']}", rendered)
    rendered = _HIVE_MONTH_RE.sub(f"month={target_values['month']}", rendered)
    rendered = _HIVE_DATE_RE.sub(
        f"date={target_values['date']}",
        rendered,
    )
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
        rendered = _HIVE_HOUR_RE.sub(f"hour={target_values['hour']}", rendered)
    return rendered.replace(anchor_compact, target_values["yyyymmdd"])


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
        if logical_hour is not None and _HOURLY_TEMPLATE_FIELDS.isdisjoint(
            _template_fields(uri_template)
        ):
            raise ValueError("Hourly URI templates must include {hour}, {hh}, or {yyyymmddhh}.")
        return rendered
    return roll_hive_partition_uri(uri_template, logical_date, logical_hour)
