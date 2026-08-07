"""Stable, reusable configuration for public sanitizer operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

CsvHeaderMode = Literal["exact", "union"]


@dataclass(frozen=True, slots=True)
class CsvOptions:
    """CSV dialect and multi-file header behavior."""

    has_header: bool = True
    delimiter: str = ","
    escape_char: str | None = None
    header_mode: CsvHeaderMode = "exact"

    def __post_init__(self) -> None:
        """Validate values before they reach a conversion."""
        if len(self.delimiter.encode("utf-8")) != 1:
            raise ValueError("CsvOptions.delimiter must be exactly one UTF-8 byte")
        if self.header_mode not in {"exact", "union"}:
            raise ValueError("CsvOptions.header_mode must be 'exact' or 'union'")


@dataclass(frozen=True, slots=True)
class ParsingOptions:
    """Optional scalar parsing rules shared by every reader."""

    integers: bool = False
    floats: bool = False
    float_decimal_separator: str = "."
    float_thousands_separator: str = ","
    iso_timestamps: bool = False
    iso_dates: bool = False
    iso_times: bool = False
    true_tokens: tuple[str, ...] = ()
    false_tokens: tuple[str, ...] = ()
    timestamp_patterns: tuple[str, ...] = ()
    date_patterns: tuple[str, ...] = ()
    time_patterns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ResourceOptions:
    """Concurrency and process-wide operation memory settings."""

    multi_threading: bool = False
    memory_limit_bytes: int | None = None

    def __post_init__(self) -> None:
        """Reject ambiguous booleans and invalid limits early."""
        if not isinstance(self.multi_threading, bool):
            raise TypeError("ResourceOptions.multi_threading must be a bool")
        if self.memory_limit_bytes is not None:
            if isinstance(self.memory_limit_bytes, bool) or self.memory_limit_bytes <= 0:
                raise ValueError("ResourceOptions.memory_limit_bytes must be greater than zero")


@dataclass(frozen=True, slots=True)
class ParquetOptions:
    """Options that apply only to Parquet file output."""

    compression: str | None = "gzip"
    gzip_level: int | None = None


@dataclass(frozen=True, slots=True)
class SanitizeOptions:
    """Reusable configuration accepted by :class:`schema_sanitizer.Sanitizer`."""

    input_format: str | None = None
    input_mode: str = "single_file"
    schema_mode: str = "additive"
    column_order: str = "alphabetically"
    field_name_policy: str = "lower_alpha"
    timestamp_precision: str = "TIMESTAMP_MICROS"
    arrow_max_depth: int = 32
    parquet_max_depth: int = 15
    scalar_object_key: str = "default_key"
    input_text_encoding: str = "utf-8"
    xml_row_tag: str | None = None
    on_error: str = "emit_null_row"
    csv: CsvOptions = field(default_factory=CsvOptions)
    parsing: ParsingOptions = field(default_factory=ParsingOptions)
    resources: ResourceOptions = field(default_factory=ResourceOptions)

    def to_kwargs(self) -> dict[str, object]:
        """Return converter keyword arguments without per-call schema state."""
        return {
            "input_format": self.input_format,
            "input_mode": self.input_mode,
            "schema_mode": self.schema_mode,
            "column_order": self.column_order,
            "field_name_policy": self.field_name_policy,
            "timestamp_precision": self.timestamp_precision,
            "parse_integers": self.parsing.integers,
            "parse_floats": self.parsing.floats,
            "parse_float_decimal_separator": self.parsing.float_decimal_separator,
            "parse_float_thousands_separator": self.parsing.float_thousands_separator,
            "parse_iso_timestamps": self.parsing.iso_timestamps,
            "parse_iso_dates": self.parsing.iso_dates,
            "parse_iso_times": self.parsing.iso_times,
            "true_tokens": self.parsing.true_tokens,
            "false_tokens": self.parsing.false_tokens,
            "custom_timestamp_patterns": self.parsing.timestamp_patterns,
            "custom_date_patterns": self.parsing.date_patterns,
            "custom_time_patterns": self.parsing.time_patterns,
            "arrow_max_depth": self.arrow_max_depth,
            "parquet_max_depth": self.parquet_max_depth,
            "scalar_object_key": self.scalar_object_key,
            "csv_has_header": self.csv.has_header,
            "csv_delimiter": self.csv.delimiter,
            "csv_escape_char": self.csv.escape_char,
            "csv_header_mode": self.csv.header_mode,
            "input_text_encoding": self.input_text_encoding,
            "xml_row_tag": self.xml_row_tag,
            "on_error": self.on_error,
            "multi_threading": self.resources.multi_threading,
            "memory_limit_bytes": self.resources.memory_limit_bytes,
        }


__all__ = [
    "CsvHeaderMode",
    "CsvOptions",
    "ParquetOptions",
    "ParsingOptions",
    "ResourceOptions",
    "SanitizeOptions",
]
