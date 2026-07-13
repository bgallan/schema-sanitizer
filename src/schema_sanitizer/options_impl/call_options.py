"""Per-call option model, validation, grouping, and normalization."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass, fields
from functools import lru_cache
from typing import Any

from ..core_impl.dependencies import ensure_pyarrow
from ..core_impl.logical_schema import LogicalSchemaPayload
from ..core_impl.native_options import Options as NativeOptions
from ..core_impl.native_options import normalize_field_name_policy_option
from .options import Options

FILE_CONVERSION_HELPER_KEYS = frozenset(
    {
        "input_path",
        "output_path",
        "input_format",
        "input_mode",
        "schema_registry",
        "parquet_compression",
        "parquet_gzip_level",
    }
)
ANALYTICAL_HELPER_KEYS = frozenset(
    {
        "input_path",
        "target",
        "input_format",
        "input_mode",
        "schema_registry",
    }
)


def call_options_from_locals(
    values: dict[str, Any],
    excluded: frozenset[str],
) -> dict[str, Any]:
    """Remove wrapper-only arguments from a public conversion call."""
    options = values.copy()
    for key in excluded:
        options.pop(key, None)
    return options


_INPUT_ENCODINGS = frozenset({"utf-8", "utf-16", "utf-16-le", "utf-16-be", "iso8859-1"})
_SCHEMA_MODES = frozenset({"strict", "additive"})
_COLUMN_ORDERS = frozenset({"alphabetically", "schema_contract_first"})
_ERROR_MODES = frozenset({"stop", "skip_row", "emit_null_row"})
_TIMESTAMP_PRECISIONS = frozenset({"TIMESTAMP_MILLIS", "TIMESTAMP_MICROS", "TIMESTAMP_NANOS"})


def _coerce_string_tuple(name: str, value: Any) -> tuple[str, ...]:
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


def _normalize_choice(name: str, value: str, accepted: Collection[str]) -> str:
    """Validate one canonical lower-case string choice without aliases."""
    normalized = value.strip()
    if normalized not in accepted:
        choices = ", ".join(repr(item) for item in sorted(accepted))
        raise ValueError(f"Option {name!r} must be one of {choices}")
    return normalized


def _normalize_schema_contract(value: Any) -> Any:
    """Validate the internal registry-derived schema contract option."""
    if value is None or isinstance(value, LogicalSchemaPayload):
        return value
    pa = ensure_pyarrow(feature="schema_contract")
    if not isinstance(value, pa.Schema):
        raise TypeError(
            "Internal option 'schema_contract' must be a pyarrow.Schema "
            "or native logical schema payload"
        )
    return value


def _normalize_optional_tag(name: str, value: Any) -> str:
    """Validate an optional XML tag and encode unset as an empty string."""
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


def _normalize_float_separators(decimal: str, thousands: str) -> tuple[str, str]:
    """Validate locale-independent float separator characters."""
    for name, value in (
        ("parse_float_decimal_separator", decimal),
        ("parse_float_thousands_separator", thousands),
    ):
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


def _normalize_int(
    name: str,
    value: Any,
    *,
    none_ok: bool = False,
    min_value: int | None = None,
    min_inclusive: bool = True,
) -> int | None:
    """Validate an integer option and optional lower bound."""
    if value is None and none_ok:
        return None
    suffix = " or None" if none_ok else ""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"Option {name!r} must be an integer{suffix}")
    if min_value is not None and ((value < min_value) if min_inclusive else (value <= min_value)):
        op = ">=" if min_inclusive else ">"
        raise ValueError(f"Option {name!r} must be {op} {min_value}")
    return value


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
        _normalize_call_option_values(self)

    def to_options(self) -> Options:
        """Convert flat call options to grouped internal options."""
        performance: dict[str, Any] = {}
        if self.batch_memory_limit_bytes is not None:
            performance["memory_limit_bytes"] = self.batch_memory_limit_bytes

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
            performance=performance,
        )

    @classmethod
    def from_kwargs(cls, values: dict[str, Any]) -> _CallOptions:
        """Create call options from validated keyword arguments."""
        if not isinstance(values, dict):
            raise TypeError("Internal option normalization expects a dict")
        unknown = sorted(set(values) - VALID_CALL_OPTION_NAMES)
        if unknown:
            raise TypeError(f"Unknown option(s): {unknown}")
        return cls(**values)


_CALL_OPTION_FIELDS = fields(_CallOptions)
_SEQUENCE_OPTION_NAMES = tuple(
    field.name for field in _CALL_OPTION_FIELDS if isinstance(field.default, tuple)
)
_BOOLEAN_OPTION_NAMES = tuple(
    field.name for field in _CALL_OPTION_FIELDS if isinstance(field.default, bool)
)
_STRING_OPTION_NAMES = tuple(
    field.name for field in _CALL_OPTION_FIELDS if isinstance(field.default, str)
)
VALID_CALL_OPTION_NAMES = frozenset(field.name for field in _CALL_OPTION_FIELDS)
_DEFAULT_CALL_OPTION_VALUES = {field.name: field.default for field in _CALL_OPTION_FIELDS}


def _normalize_call_option_values(options: _CallOptions) -> None:
    """Normalize one mutable dataclass instance in place."""
    for name in _SEQUENCE_OPTION_NAMES:
        object.__setattr__(
            options,
            name,
            _coerce_string_tuple(name, getattr(options, name)),
        )

    for name in _BOOLEAN_OPTION_NAMES:
        if not isinstance(getattr(options, name), bool):
            raise TypeError(f"Option {name!r} must be a bool")

    for name in _STRING_OPTION_NAMES:
        if not isinstance(getattr(options, name), str):
            raise TypeError(f"Option {name!r} must be a string")

    if options.schema_mode.strip().lower() == "strict" and options.schema_contract is None:
        raise ValueError(
            "Option 'schema_mode=\"strict\"' requires a registry-derived schema contract"
        )
    object.__setattr__(
        options,
        "schema_contract",
        _normalize_schema_contract(options.schema_contract),
    )
    object.__setattr__(
        options,
        "schema_mode",
        _normalize_choice("schema_mode", options.schema_mode, _SCHEMA_MODES),
    )
    object.__setattr__(
        options,
        "column_order",
        _normalize_choice("column_order", options.column_order, _COLUMN_ORDERS),
    )
    object.__setattr__(
        options,
        "on_error",
        _normalize_choice("on_error", options.on_error, _ERROR_MODES),
    )
    object.__setattr__(
        options,
        "input_text_encoding",
        _normalize_choice("input_text_encoding", options.input_text_encoding, _INPUT_ENCODINGS),
    )
    object.__setattr__(
        options,
        "xml_row_tag",
        _normalize_optional_tag("xml_row_tag", options.xml_row_tag),
    )
    object.__setattr__(
        options,
        "timestamp_precision",
        _normalize_choice(
            "timestamp_precision",
            options.timestamp_precision.strip().upper(),
            _TIMESTAMP_PRECISIONS,
        ),
    )
    object.__setattr__(
        options,
        "field_name_policy",
        normalize_field_name_policy_option(options.field_name_policy),
    )
    decimal_separator, thousands_separator = _normalize_float_separators(
        options.parse_float_decimal_separator,
        options.parse_float_thousands_separator,
    )
    object.__setattr__(options, "parse_float_decimal_separator", decimal_separator)
    object.__setattr__(options, "parse_float_thousands_separator", thousands_separator)
    object.__setattr__(
        options,
        "batch_memory_limit_bytes",
        _normalize_int(
            "batch_memory_limit_bytes",
            options.batch_memory_limit_bytes,
            none_ok=True,
            min_value=0,
            min_inclusive=False,
        ),
    )
    object.__setattr__(
        options,
        "arrow_max_depth",
        _normalize_int("arrow_max_depth", options.arrow_max_depth, min_value=0),
    )
    object.__setattr__(
        options,
        "parquet_max_depth",
        _normalize_int("parquet_max_depth", options.parquet_max_depth, min_value=0),
    )
    object.__setattr__(
        options,
        "read_chunk_bytes",
        _normalize_int(
            "read_chunk_bytes",
            options.read_chunk_bytes,
            min_value=0,
            min_inclusive=False,
        ),
    )


def _hashable_option_value(value: Any) -> Any:
    """Return a cache-safe representation for simple option values."""
    if isinstance(value, list):
        return ("list", tuple(value))
    if isinstance(value, tuple):
        return ("tuple", value)
    if isinstance(value, (str, int, bool, type(None))):
        return (type(value).__name__, value)
    return None


def _call_options_cache_key(
    kwargs: dict[str, Any],
) -> tuple[tuple[str, Any], ...] | None:
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
    unknown = sorted(set(kwargs) - VALID_CALL_OPTION_NAMES)
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
    decoded: dict[str, Any] = {}
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


def unwrap_options(options: Any) -> Any:
    """Return the payload expected by the native ABI."""
    if options is None:
        return None
    if isinstance(options, Options):
        return options.raw
    if isinstance(options, NativeOptions):
        raise TypeError(
            "Passing raw native option objects to the high-level API is not supported. "
            "Use per-call option keywords."
        )
    raise TypeError("options must be None or internal call options")
