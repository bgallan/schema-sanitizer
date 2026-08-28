"""Grouped wrapper around the native C++ options catalog."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal, TypeAlias, cast

from ..core_impl.logical_schema import LogicalSchemaPayload
from ..core_impl.native_options import ENUM_BY_KIND, OPTIONS, coerce_enum_member
from ..core_impl.native_options import Options as _RawOptions
from ..core_impl.native_options import validate_options as _validate_options

CsvHeaderMode: TypeAlias = Literal["exact", "union"]
_CSV_HEADER_MODES = frozenset({"exact", "union"})


def normalize_csv_header_mode(value: object) -> CsvHeaderMode:
    """Return one canonical CSV header reconciliation mode."""
    if not isinstance(value, str):
        raise TypeError("Option 'csv_header_mode' must be a string")
    normalized = value.strip().lower()
    if normalized not in _CSV_HEADER_MODES:
        raise ValueError("Option 'csv_header_mode' must be one of 'exact', 'union'")
    return cast(CsvHeaderMode, normalized)


def normalize_csv_escape_char(value: object, delimiter: str) -> str:
    """Validate an opt-in one-byte escape used inside quoted CSV fields."""
    if value is None:
        return ""
    if not isinstance(value, str):
        raise TypeError("Option 'csv_escape_char' must be a string or None")
    if len(value) != 1 or not value.isascii():
        raise ValueError("Option 'csv_escape_char' must be one ASCII character or None")
    if value in {delimiter, '"', "\r", "\n", "\0"}:
        raise ValueError(
            "Option 'csv_escape_char' must differ from the delimiter and quote, "
            "and must not be a line break or NUL"
        )
    return value


_BOOL_OPTION_NAMES = frozenset(spec.name for spec in OPTIONS if spec.kind == "bool")
_INT_OPTION_NAMES = frozenset(spec.name for spec in OPTIONS if spec.kind in {"i32", "i64"})
_STRING_OPTION_NAMES = frozenset(spec.name for spec in OPTIONS if spec.kind == "string")
_STRING_LIST_OPTION_NAMES = frozenset(spec.name for spec in OPTIONS if spec.kind == "string_list")
_ENUM_BY_OPTION_NAME: dict[str, Any] = {
    spec.name: ENUM_BY_KIND[spec.kind] for spec in OPTIONS if spec.kind in ENUM_BY_KIND
}
_ALLOWED_BY_GROUP: dict[str, frozenset[str]] = {
    group: frozenset(spec.name for spec in OPTIONS if spec.group == group)
    for group in {spec.group for spec in OPTIONS}
}
_GROUP_NAMES = frozenset(_ALLOWED_BY_GROUP)
_GROUP_BY_OPTION_NAME = {
    option_name: group_name
    for group_name, option_names in _ALLOWED_BY_GROUP.items()
    for option_name in option_names
}
_OPTION_NAMES = frozenset(_GROUP_BY_OPTION_NAME)
_READ_ONLY_OPTION_ATTRS = frozenset({"_raw"}) | _GROUP_NAMES


def _coerce_enum_if_needed(option_name: str, value: Any) -> Any:
    """Coerce a catalog-backed enum option through the canonical helper."""
    enum_type = _ENUM_BY_OPTION_NAME.get(option_name)
    if enum_type is None:
        return value
    return coerce_enum_member(enum_type, value, label=f"option '{option_name}'")


def _coerce_option_value_if_needed(option_name: str, value: Any) -> Any:
    """Normalize and type-check one catalog-backed option value."""
    if option_name in _BOOL_OPTION_NAMES and not isinstance(value, bool):
        raise TypeError(f"Option '{option_name}' must be a bool")
    if option_name in _INT_OPTION_NAMES and (isinstance(value, bool) or not isinstance(value, int)):
        raise TypeError(f"Option '{option_name}' must be an integer")
    if option_name in _STRING_OPTION_NAMES and not isinstance(value, str):
        raise TypeError(f"Option '{option_name}' must be a string")
    if option_name not in _STRING_LIST_OPTION_NAMES:
        return value
    if value is None:
        return []
    if isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"Option '{option_name}' must be a sequence of strings, not a string")
    try:
        out = list(value)
    except TypeError as e:
        raise TypeError(f"Option '{option_name}' must be a sequence of strings") from e
    if not all(isinstance(item, str) for item in out):
        raise TypeError(f"Option '{option_name}' must contain only strings")
    return out


class _Proxy:
    """Attribute proxy exposing one canonical native option group."""

    __slots__ = ("_allowed", "_raw")

    def __init__(self, raw: Any, allowed: frozenset[str]) -> None:
        """Create a proxy over an immutable set of option names."""
        object.__setattr__(self, "_raw", raw)
        object.__setattr__(self, "_allowed", allowed)

    def _resolve_target(self, option_name: str) -> str:
        """Resolve an allowed option name or raise a clear error."""
        if option_name in self._allowed:
            return option_name
        raise AttributeError(
            f"Unknown option '{option_name}' for this group. "
            "If you recently changed native options, rebuild the native extension."
        )

    def __getattr__(self, name: str) -> Any:
        """Return an option value from the raw options object."""
        return getattr(self._raw, self._resolve_target(name))

    def __setattr__(self, name: str, value: Any) -> None:
        """Normalize and set an option value on the raw object."""
        if name in _Proxy.__slots__:
            object.__setattr__(self, name, value)
            return
        target = self._resolve_target(name)
        value = _coerce_option_value_if_needed(target, value)
        setattr(self._raw, target, _coerce_enum_if_needed(target, value))

    def __dir__(self) -> list[str]:
        """Return the option names exposed by this proxy."""
        return sorted(self._allowed)


class Options:
    """Grouped wrapper for the flat native C++ options object.

    Construction accepts canonical group dictionaries such as
    ``Options(schema={"schema_evolution": "STRICT"})``. Unknown groups and
    unknown option names are rejected rather than translated through aliases.
    """

    _raw: _RawOptions

    def __init__(self, **kwargs: Any) -> None:
        """Create grouped options from canonical option dictionaries."""
        self._bind_groups(_RawOptions())
        for group_name, values in kwargs.items():
            if group_name not in _GROUP_NAMES:
                raise TypeError(
                    f"Unknown option {group_name!r}. "
                    "Use grouped kwargs (schema=..., inference=..., ...)."
                )
            if not isinstance(values, dict):
                raise TypeError(f"Options.{group_name} must be a dict of group fields")
            proxy = getattr(self, group_name)
            for option_name, value in values.items():
                setattr(proxy, option_name, value)

    def _bind_groups(self, raw: _RawOptions) -> None:
        """Bind this wrapper and all grouped proxies to a raw options instance."""
        object.__setattr__(self, "_raw", raw)
        for group_name, allowed in _ALLOWED_BY_GROUP.items():
            object.__setattr__(self, group_name, _Proxy(raw, allowed))

    @property
    def raw(self) -> _RawOptions:
        """Return the backing native options instance."""
        return self._raw

    def __getattr__(self, name: str) -> Any:
        """Return a canonical raw option value."""
        if name not in _OPTION_NAMES:
            raise AttributeError(f"Unknown option attribute {name!r}")
        return getattr(self._raw, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Set and normalize a canonical raw option value."""
        if name in _READ_ONLY_OPTION_ATTRS:
            raise AttributeError(f"'{type(self).__name__}' attribute '{name}' is read-only")
        if name not in _OPTION_NAMES:
            raise AttributeError(f"Unknown option attribute {name!r}")
        value = _coerce_option_value_if_needed(name, value)
        setattr(self._raw, name, _coerce_enum_if_needed(name, value))

    def __repr__(self) -> str:
        """Return a compact grouped options representation."""
        return "Options(schema=..., inference=..., io=..., performance=...)"

    @staticmethod
    def _encode_value_for_json(value: Any) -> Any:
        """Convert an option value to a JSON-friendly representation."""
        if isinstance(value, Enum):
            return value.name
        if isinstance(value, (list, tuple)):
            return [Options._encode_value_for_json(item) for item in value]
        if isinstance(value, dict):
            return {str(key): Options._encode_value_for_json(item) for key, item in value.items()}
        if isinstance(value, LogicalSchemaPayload):
            return value
        if value is not None and hasattr(value, "__arrow_c_schema__"):
            raise TypeError(
                "arrow_schema_contract cannot be serialized to JSON. "
                "Pass it programmatically (pyarrow.Schema) instead."
            )
        return value

    def to_flat_dict(self, *, include_defaults: bool = False) -> dict[str, Any]:
        """Serialize the native options as a flat dictionary."""
        default = _RawOptions() if not include_defaults else None
        out: dict[str, Any] = {}
        for name in sorted(_OPTION_NAMES):
            value = getattr(self._raw, name)
            if default is not None and value == getattr(default, name):
                continue
            out[name] = self._encode_value_for_json(value)
        return out

    def to_dict(self, *, include_defaults: bool = False) -> dict[str, Any]:
        """Serialize options into the grouped form accepted by ``Options``."""
        grouped: dict[str, dict[str, Any]] = {}
        for name, value in self.to_flat_dict(include_defaults=include_defaults).items():
            grouped.setdefault(_GROUP_BY_OPTION_NAME[name], {})[name] = value
        return {group: grouped[group] for group in sorted(grouped)}

    def validate_native(self) -> None:
        """Validate options against the native engine."""
        _validate_options(self.raw)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Options:
        """Create options from a grouped dictionary."""
        if not isinstance(d, dict):
            raise TypeError("Options.from_dict expects a dict")
        return cls(**d)


def memory_limit_bytes_or_none(options: Options | None) -> int | None:
    """Return the configured memory limit, translating the native unset sentinel."""
    if options is None:
        return None
    value = options.performance.memory_limit_bytes
    return None if value == -1 else value


__all__ = ["Options", "memory_limit_bytes_or_none"]
