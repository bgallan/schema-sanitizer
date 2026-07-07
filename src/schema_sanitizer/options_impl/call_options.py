"""Per-call option validation for schema_sanitizer."""

from __future__ import annotations

from dataclasses import dataclass, fields
from functools import lru_cache
from typing import Any

from .call_option_validators import (
    coerce_string_tuple,
    is_strict_schema_mode,
    normalize_field_name_policy_option,
    normalize_float_separator_options,
    normalize_input_text_encoding_option,
    normalize_int_option,
    normalize_optional_string_option,
    normalize_schema_contract_option,
    normalize_timestamp_precision_option,
)
from .options import Options


@dataclass(slots=True)
class _CallOptions:
    """Internal flat option set used to validate public call kwargs."""

    schema_contract: Any = None
    schema_mode: str = "additive"
    column_order: str = "alphabetically"
    field_name_policy: str = "lower_alpha"
    timestamp_precision: str = "TIMESTAMP_MICROS"

    parse_integers: bool = False
    parse_floats: bool = False
    parse_float_decimal_separator: str = "."
    parse_float_thousands_separator: str = ","
    parse_iso_timestamps: bool = False
    parse_iso_dates: bool = False
    parse_iso_times: bool = False
    true_tokens: tuple[str, ...] = ()
    false_tokens: tuple[str, ...] = ()
    custom_timestamp_patterns: tuple[str, ...] = ()
    custom_date_patterns: tuple[str, ...] = ()
    custom_time_patterns: tuple[str, ...] = ()
    arrow_max_depth: int = 32
    parquet_max_depth: int = 15
    scalar_object_key: str = "default_key"

    csv_has_header: bool = True
    csv_delimiter: str = ","
    input_text_encoding: str = "utf-8"
    xml_row_tag: str | None = None

    on_error: str = "emit_null_row"
    batch_memory_limit_bytes: int | None = None
    read_chunk_bytes: int = 1 << 20

    def __post_init__(self) -> None:
        """Normalize and validate all call option values."""
        for name in _SEQUENCE_OPTION_NAMES:
            value = getattr(self, name)
            object.__setattr__(self, name, coerce_string_tuple(name, value))

        for name in _BOOLEAN_OPTION_NAMES:
            value = getattr(self, name)
            if not isinstance(value, bool):
                raise TypeError(f"Option {name!r} must be a bool")

        for name in _STRING_OPTION_NAMES:
            value = getattr(self, name)
            if not isinstance(value, str):
                raise TypeError(f"Option {name!r} must be a string")

        if is_strict_schema_mode(self.schema_mode) and self.schema_contract is None:
            raise ValueError(
                "Option 'schema_mode=\"strict\"' requires a registry-derived schema contract"
            )
        object.__setattr__(
            self,
            "schema_contract",
            normalize_schema_contract_option(self.schema_contract),
        )

        object.__setattr__(
            self,
            "input_text_encoding",
            normalize_input_text_encoding_option(self.input_text_encoding),
        )
        object.__setattr__(
            self,
            "xml_row_tag",
            normalize_optional_string_option("xml_row_tag", self.xml_row_tag),
        )
        object.__setattr__(
            self,
            "timestamp_precision",
            normalize_timestamp_precision_option(self.timestamp_precision),
        )
        object.__setattr__(
            self,
            "field_name_policy",
            normalize_field_name_policy_option(self.field_name_policy),
        )
        decimal_separator, thousands_separator = normalize_float_separator_options(
            self.parse_float_decimal_separator,
            self.parse_float_thousands_separator,
        )
        object.__setattr__(self, "parse_float_decimal_separator", decimal_separator)
        object.__setattr__(self, "parse_float_thousands_separator", thousands_separator)
        object.__setattr__(
            self,
            "batch_memory_limit_bytes",
            normalize_int_option(
                "batch_memory_limit_bytes",
                self.batch_memory_limit_bytes,
                none_ok=True,
                min_value=0,
                min_inclusive=False,
            ),
        )
        object.__setattr__(
            self,
            "arrow_max_depth",
            normalize_int_option("arrow_max_depth", self.arrow_max_depth, min_value=0),
        )
        object.__setattr__(
            self,
            "parquet_max_depth",
            normalize_int_option("parquet_max_depth", self.parquet_max_depth, min_value=0),
        )
        object.__setattr__(
            self,
            "read_chunk_bytes",
            normalize_int_option(
                "read_chunk_bytes", self.read_chunk_bytes, min_value=0, min_inclusive=False
            ),
        )

    def to_options(self) -> Options:
        """Convert flat call options to grouped internal options."""
        perf: dict[str, Any] = {}
        if self.batch_memory_limit_bytes is not None:
            perf["memory_limit_bytes"] = self.batch_memory_limit_bytes

        return Options(
            schema={
                "arrow_schema_contract": self.schema_contract,
                "schema_evolution": self.schema_mode,
                "field_order": self.column_order,
                "field_name_policy": self.field_name_policy,
                "timestamp_precision": self.timestamp_precision,
            },
            inference={
                "parse_integers": self.parse_integers,
                "parse_floats": self.parse_floats,
                "parse_float_decimal_separator": self.parse_float_decimal_separator,
                "parse_float_thousands_separator": self.parse_float_thousands_separator,
                "parse_iso_timestamps": self.parse_iso_timestamps,
                "parse_iso_dates": self.parse_iso_dates,
                "parse_iso_times": self.parse_iso_times,
                "true_tokens": list(self.true_tokens),
                "false_tokens": list(self.false_tokens),
                "timestamp_regexps": list(self.custom_timestamp_patterns),
                "date_regexps": list(self.custom_date_patterns),
                "time_regexps": list(self.custom_time_patterns),
                "arrow_max_depth": self.arrow_max_depth,
                "parquet_max_depth": self.parquet_max_depth,
                "default_key_name": self.scalar_object_key,
            },
            io={
                "io_chunk_bytes": self.read_chunk_bytes,
                "input_text_encoding": self.input_text_encoding,
            },
            csv={
                "csv_has_header": self.csv_has_header,
                "csv_delimiter": self.csv_delimiter,
            },
            xml={"xml_row_tag": self.xml_row_tag},
            errors={"on_error": self.on_error},
            performance=perf,
        )

    @classmethod
    def from_kwargs(cls, d: dict[str, Any]) -> _CallOptions:
        """Create call options from validated keyword arguments."""
        if not isinstance(d, dict):
            raise TypeError("Internal option normalization expects a dict")
        unknown = sorted(set(d) - _VALID_CALL_OPTION_NAMES)
        if unknown:
            raise TypeError(f"Unknown option(s): {unknown}")
        return cls(**d)


_CALL_OPTION_FIELDS = fields(_CallOptions)
_SEQUENCE_OPTION_NAMES = tuple(f.name for f in _CALL_OPTION_FIELDS if isinstance(f.default, tuple))
_BOOLEAN_OPTION_NAMES = tuple(f.name for f in _CALL_OPTION_FIELDS if isinstance(f.default, bool))
_STRING_OPTION_NAMES = tuple(f.name for f in _CALL_OPTION_FIELDS if isinstance(f.default, str))
_VALID_CALL_OPTION_NAMES = frozenset(f.name for f in _CALL_OPTION_FIELDS)
_DEFAULT_CALL_OPTION_VALUES = {f.name: f.default for f in _CALL_OPTION_FIELDS}


def _hashable_option_value(value: Any) -> Any:
    """Return a cache-safe representation for simple option values."""
    if isinstance(value, list):
        return ("list", tuple(value))
    if isinstance(value, tuple):
        return ("tuple", value)
    if isinstance(value, (str, int, bool, type(None))):
        return (type(value).__name__, value)
    return None


def _call_options_cache_key(kwargs: dict[str, Any]) -> tuple[tuple[str, Any], ...] | None:
    """Return a stable cache key for simple call-option dictionaries."""
    items: list[tuple[str, Any]] = []
    for name, value in sorted(kwargs.items()):
        cached = _hashable_option_value(value)
        if cached is None and value is not None:
            return None
        items.append((name, cached))
    return tuple(items)


def _kwargs_are_default_call_options(kwargs: dict[str, Any]) -> bool:
    """Return whether provided public call options equal API defaults."""
    unknown = sorted(set(kwargs) - _VALID_CALL_OPTION_NAMES)
    if unknown:
        raise TypeError(f"Unknown option(s): {unknown}")
    for name, value in kwargs.items():
        default = _DEFAULT_CALL_OPTION_VALUES[name]
        if isinstance(default, bool):
            if not isinstance(value, bool):
                return False
        elif isinstance(default, int):
            if isinstance(value, bool) or not isinstance(value, int):
                return False
        elif isinstance(default, str):
            if not isinstance(value, str):
                return False
        elif isinstance(default, tuple):
            if isinstance(value, list):
                value = tuple(value)
            if not isinstance(value, tuple) or not all(isinstance(item, str) for item in value):
                return False
        if value != default:
            return False
    return True


@lru_cache(maxsize=128)
def _cached_call_options(key: tuple[tuple[str, Any], ...]) -> _CallOptions:
    """Return normalized immutable call options for a simple cache key."""
    decoded = {}
    for name, tagged in key:
        kind, value = tagged
        decoded[name] = list(value) if kind == "list" else value
    return _CallOptions.from_kwargs(decoded)


def normalize_call_options(**kwargs: Any) -> Options:
    """Normalize public call option keywords into internal options."""
    if not kwargs:
        return _cached_call_options(()).to_options()
    key = _call_options_cache_key(kwargs)
    if key is not None:
        return _cached_call_options(key).to_options()
    return _CallOptions.from_kwargs(kwargs).to_options()


def normalize_call_options_or_none(**kwargs: Any) -> Options | None:
    """Normalize public call options, returning None when they are all defaults."""
    if _kwargs_are_default_call_options(kwargs):
        return None
    return normalize_call_options(**kwargs)


__all__ = ["normalize_call_options", "normalize_call_options_or_none"]
