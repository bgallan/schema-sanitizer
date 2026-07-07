"""Small validators used by public per-call option normalization."""

from __future__ import annotations

import codecs
from typing import Any

from ..adapters.pyarrow_common import ensure_pyarrow
from ..core_impl.options_logical_schema import LogicalSchemaPayload


def coerce_string_tuple(name: str, value: Any) -> tuple[str, ...]:
    """Coerce a sequence option to an immutable string tuple."""
    if isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"Option {name!r} must be a sequence of strings, not a string")
    try:
        coerced = tuple(value)
    except TypeError as e:
        raise TypeError(f"Option {name!r} must be a sequence of strings") from e
    if not all(isinstance(item, str) for item in coerced):
        raise TypeError(f"Option {name!r} must contain only strings")
    return coerced


def normalize_input_text_encoding_option(value: str) -> str:
    """Validate and canonicalize the input text encoding option."""
    input_text_encoding = value.strip()
    if not input_text_encoding:
        raise ValueError("Option 'input_text_encoding' must not be empty")
    try:
        return codecs.lookup(input_text_encoding).name
    except LookupError as e:
        raise ValueError(f"Unknown input_text_encoding: {value!r}") from e


def is_strict_schema_mode(value: str) -> bool:
    """Return whether a schema mode string requests strict schema evolution."""
    mode = value.strip().lower()
    return mode == "strict" or mode == "kstrict"


def normalize_schema_contract_option(value: Any) -> Any:
    """Validate the internal registry-derived schema contract option."""
    if value is None:
        return None
    if isinstance(value, LogicalSchemaPayload):
        return value
    pa = ensure_pyarrow(feature="schema_contract")
    if not isinstance(value, pa.Schema):
        raise TypeError(
            "Internal option 'schema_contract' must be a pyarrow.Schema "
            "or native logical schema payload"
        )
    return value


def normalize_optional_string_option(name: str, value: Any) -> str:
    """Validate an optional public string option and encode unset as empty."""
    if value is None:
        return ""
    if not isinstance(value, str):
        raise TypeError(f"Option {name!r} must be a string or None")
    value = value.strip()
    if not value:
        raise ValueError(f"Option {name!r} must not be empty when provided")
    if any(ch.isspace() or ch in "<>/=" for ch in value):
        raise ValueError(f"Option {name!r} must be an XML element tag name")
    return value


def normalize_timestamp_precision_option(value: Any) -> str:
    """Validate and normalize public timestamp precision values."""
    if not isinstance(value, str):
        raise TypeError("Option 'timestamp_precision' must be a string")
    key = value.strip().upper().replace("-", "_")
    aliases = {
        "MS": "TIMESTAMP_MILLIS",
        "MILLI": "TIMESTAMP_MILLIS",
        "MILLIS": "TIMESTAMP_MILLIS",
        "MILLISECOND": "TIMESTAMP_MILLIS",
        "MILLISECONDS": "TIMESTAMP_MILLIS",
        "TIMESTAMP_MILLIS": "TIMESTAMP_MILLIS",
        "US": "TIMESTAMP_MICROS",
        "MICRO": "TIMESTAMP_MICROS",
        "MICROS": "TIMESTAMP_MICROS",
        "MICROSECOND": "TIMESTAMP_MICROS",
        "MICROSECONDS": "TIMESTAMP_MICROS",
        "TIMESTAMP_MICROS": "TIMESTAMP_MICROS",
        "NS": "TIMESTAMP_NANOS",
        "NANO": "TIMESTAMP_NANOS",
        "NANOS": "TIMESTAMP_NANOS",
        "NANOSECOND": "TIMESTAMP_NANOS",
        "NANOSECONDS": "TIMESTAMP_NANOS",
        "TIMESTAMP_NANOS": "TIMESTAMP_NANOS",
    }
    try:
        return aliases[key]
    except KeyError as exc:
        raise ValueError(
            "Option 'timestamp_precision' must be one of "
            "'TIMESTAMP_MILLIS', 'TIMESTAMP_MICROS', or 'TIMESTAMP_NANOS'"
        ) from exc


def normalize_field_name_policy_option(value: Any) -> str:
    """Validate and normalize public field-name policy values."""
    if not isinstance(value, str):
        raise TypeError("Option 'field_name_policy' must be a string")
    key = value.strip().lower().replace("-", "_")
    aliases = {
        "loweralpha": "lower_alpha",
        "lower_alpha": "lower_alpha",
        "lowersnake": "lower_snake",
        "lower_snake": "lower_snake",
        "preserve": "preserve",
    }
    try:
        return aliases[key]
    except KeyError as exc:
        raise ValueError(
            "Option 'field_name_policy' must be 'lower_alpha', 'lower_snake', or 'preserve'"
        ) from exc


def normalize_float_separator_options(decimal: str, thousands: str) -> tuple[str, str]:
    """Validate locale-independent float parsing separator characters."""
    separators = {
        "parse_float_decimal_separator": decimal,
        "parse_float_thousands_separator": thousands,
    }
    for name, value in separators.items():
        if not isinstance(value, str):
            raise TypeError(f"Option {name!r} must be a string")
        if len(value) != 1 or not value.isascii():
            raise ValueError(f"Option {name!r} must be one ASCII character")
        if value.isspace() or value.isdigit() or value in "+-eE":
            raise ValueError(f"Option {name!r} must be an ASCII punctuation character")
    if decimal == thousands:
        raise ValueError(
            "Options 'parse_float_decimal_separator' and "
            "'parse_float_thousands_separator' must differ"
        )
    return decimal, thousands


def require_int_option(name: str, value: Any, *, none_ok: bool = False) -> int | None:
    """Validate an integer option, optionally accepting None."""
    if value is None and none_ok:
        return None
    suffix = " or None" if none_ok else ""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"Option {name!r} must be an integer{suffix}")
    return value


def normalize_int_option(
    name: str,
    value: Any,
    *,
    none_ok: bool = False,
    min_value: int | None = None,
    min_inclusive: bool = True,
) -> int | None:
    """Validate an integer option and its lower bound."""
    value = require_int_option(name, value, none_ok=none_ok)
    if value is None or min_value is None:
        return value
    if (value < min_value) if min_inclusive else (value <= min_value):
        op = ">=" if min_inclusive else ">"
        raise ValueError(f"Option {name!r} must be {op} {min_value}")
    return value
