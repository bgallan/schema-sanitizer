"""Validated Polars-to-Parquet Hive output for example 08."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from importlib import import_module
from pathlib import Path, PurePosixPath
from typing import Any

import schema_sanitizer as ss

HIVE_PARTITION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("year", "INT64"),
    ("month", "INT64"),
    ("day", "INT64"),
)
HIVE_PARTITION_NAMES = frozenset(name for name, _data_type in HIVE_PARTITION_COLUMNS)
_MAX_FILE_PREFIX_CHARACTERS = 128


@dataclass(frozen=True, slots=True)
class HiveParquetFile:
    """One validated local Parquet object and its Hive partition values."""

    local_path: Path
    relative_path: str
    year: int
    month: int
    day: int
    row_count: int
    size_bytes: int


def validate_parquet_file_prefix(file_prefix: str) -> str:
    """Validate one safe, bounded filename prefix and return it unchanged."""
    if not isinstance(file_prefix, str):
        raise TypeError("parquet_file_prefix must be a string")
    if (
        not file_prefix
        or len(file_prefix) > _MAX_FILE_PREFIX_CHARACTERS
        or not file_prefix[0].isalnum()
        or any(
            not (character.isalnum() or character in {".", "_", "-"}) for character in file_prefix
        )
    ):
        raise ValueError(
            "parquet_file_prefix must contain at most 128 characters, start with an "
            "alphanumeric character, and contain only alphanumeric characters, dots, "
            "underscores, or hyphens"
        )
    return file_prefix


def partitioned_output_uri(prefix: str, relative_path: str) -> str:
    """Join one validated relative Hive path to an object-store prefix."""
    relative = PurePosixPath(relative_path)
    if relative.is_absolute() or ".." in relative.parts or relative.suffix != ".parquet":
        raise ValueError(f"invalid relative Parquet path: {relative_path!r}")
    return f"{prefix.rstrip('/')}/{relative.as_posix()}"


def prepare_hive_parquet_schema(target_schema: Any, timestamp_column: str) -> Any:
    """Remove path partition fields and validate the configured timestamp."""
    pa = import_module("pyarrow")
    if not isinstance(target_schema, pa.Schema):
        raise TypeError("target schema must be a pyarrow.Schema")
    for name in HIVE_PARTITION_NAMES & set(target_schema.names):
        if not pa.types.is_int64(target_schema.field(name).type):
            raise ValueError(f"Hive partition field {name!r} must use int64")
    parquet_schema = pa.schema(
        [field for field in target_schema if field.name not in HIVE_PARTITION_NAMES],
        metadata=target_schema.metadata,
    )
    if timestamp_column not in parquet_schema.names:
        raise ValueError(f"partition timestamp column {timestamp_column!r} is missing")
    if not pa.types.is_timestamp(parquet_schema.field(timestamp_column).type):
        raise ValueError(f"partition column {timestamp_column!r} must be a timestamp")
    return parquet_schema


def write_hive_parquet_dataset(
    table: Any,
    parquet_schema: Any,
    base_path: Path,
    *,
    file_prefix: str,
    timestamp_column: str,
    source_window_date: date,
) -> tuple[HiveParquetFile, ...]:
    """Write and validate one deterministic Parquet per UTC Hive partition."""
    file_prefix = validate_parquet_file_prefix(file_prefix)
    pl = import_module("polars")
    pa = import_module("pyarrow")
    parquet = import_module("pyarrow.parquet")
    if not isinstance(table, pa.Table):
        raise TypeError("table must be a pyarrow.Table")
    ss.validate_analytical_result(table, parquet_schema)
    if table.num_rows == 0:
        raise ValueError("cannot partition an empty analytical result by timestamp")

    frame = pl.from_arrow(table)
    timestamp_dtype = frame.schema[timestamp_column]
    if not isinstance(timestamp_dtype, pl.Datetime):
        raise ValueError(f"partition column {timestamp_column!r} must be a timestamp")
    null_count = frame.get_column(timestamp_column).null_count()
    if null_count:
        raise ValueError(
            f"partition timestamp column {timestamp_column!r} contains {null_count} null value(s)"
        )

    timestamp = pl.col(timestamp_column)
    if timestamp_dtype.time_zone is None:
        timestamp = timestamp.dt.replace_time_zone("UTC")
    else:
        timestamp = timestamp.dt.convert_time_zone("UTC")
    partitioned = frame.with_columns(
        timestamp.dt.year().cast(pl.Int64).alias("year"),
        timestamp.dt.month().cast(pl.Int64).alias("month"),
        timestamp.dt.day().cast(pl.Int64).alias("day"),
    )
    window_token = source_window_date.strftime("%Y%m%d")
    base_path.mkdir(parents=True, exist_ok=True)
    groups = partitioned.partition_by(
        ["year", "month", "day"],
        maintain_order=True,
        include_key=False,
        as_dict=True,
    )

    outputs: list[HiveParquetFile] = []
    total_rows = 0
    for raw_key, group in sorted(groups.items()):
        key = raw_key if isinstance(raw_key, tuple) else (raw_key,)
        if len(key) != 3:
            raise RuntimeError(f"unexpected Polars partition key: {raw_key!r}")
        year, month, day = (int(value) for value in key)
        partition_token = f"{year:04d}{month:02d}{day:02d}"
        relative_path = (
            f"year={year}/month={month}/day={day}/"
            f"{file_prefix}_{partition_token}_{window_token}.gz.parquet"
        )
        local_path = base_path / relative_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        arrow_group = group.to_arrow().cast(parquet_schema, safe=True)
        parquet.write_table(
            arrow_group,
            local_path,
            compression="gzip",
        )
        relative_path = local_path.relative_to(base_path).as_posix()
        parsed_partition = _partition_values(relative_path)
        if parsed_partition != (year, month, day):
            raise RuntimeError(f"generated Hive path does not match key: {relative_path!r}")
        validated = parquet.ParquetFile(local_path).read()
        ss.validate_analytical_result(validated, parquet_schema)
        _validate_timestamp_partition(
            validated.column(timestamp_column).to_pylist(),
            timestamp_column=timestamp_column,
            expected=(year, month, day),
        )
        total_rows += validated.num_rows
        outputs.append(
            HiveParquetFile(
                local_path=local_path,
                relative_path=relative_path,
                year=year,
                month=month,
                day=day,
                row_count=validated.num_rows,
                size_bytes=local_path.stat().st_size,
            )
        )
    if not outputs:
        raise RuntimeError("Polars did not produce any Hive Parquet files")
    if total_rows != table.num_rows:
        raise ValueError(
            f"partitioned Parquet row-count mismatch: expected={table.num_rows}, actual={total_rows}"
        )
    return tuple(outputs)


def _partition_values(relative_path: str) -> tuple[int, int, int]:
    """Parse the strict year/month/day path generated by this module."""
    parts = PurePosixPath(relative_path).parts
    if len(parts) != 4:
        raise ValueError(f"unexpected Hive output path: {relative_path!r}")
    expected_names = ("year", "month", "day")
    values: list[int] = []
    for part, expected_name in zip(parts[:3], expected_names, strict=True):
        name, separator, raw_value = part.partition("=")
        if separator != "=" or name != expected_name:
            raise ValueError(f"unexpected Hive output path: {relative_path!r}")
        values.append(int(raw_value))
    return values[0], values[1], values[2]


def _validate_timestamp_partition(
    values: list[datetime | None],
    *,
    timestamp_column: str,
    expected: tuple[int, int, int],
) -> None:
    """Require every timestamp to resolve to its path partition in UTC."""
    for value in values:
        if value is None:
            raise ValueError(f"partition timestamp column {timestamp_column!r} contains nulls")
        normalized = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        actual = (normalized.year, normalized.month, normalized.day)
        if actual != expected:
            raise ValueError(
                f"timestamp {value!r} belongs to {actual!r}, not Hive partition {expected!r}"
            )


__all__ = [
    "HIVE_PARTITION_COLUMNS",
    "HIVE_PARTITION_NAMES",
    "HiveParquetFile",
    "partitioned_output_uri",
    "prepare_hive_parquet_schema",
    "validate_parquet_file_prefix",
    "write_hive_parquet_dataset",
]
