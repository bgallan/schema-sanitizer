"""Implements `schema_sanitizer.options_impl.options_groups`."""

from __future__ import annotations

from typing import Any

from ..core_impl.options_enum_metadata import (
    ENUM_ALIASES_BY_TYPE,
    ENUM_BY_CXX_TYPE,
    ENUM_VALUES_BY_TYPE,
    norm_enum_name,
)
from ..public_impl.options_catalog import OPTIONS_CATALOG


def _norm_enum(v: Any) -> Any:
    """Best-effort normalization for enum-like inputs.

    Option normalization accepts either:
      - the enum value (preferred)
      - a string name like "STRICT" / "strict" / "kStrict"

    For non-strings, returns the input unchanged.
    """

    if not isinstance(v, str):
        return v
    return norm_enum_name(v)


_BOOL_OPTION_NAMES: set[str] = {
    spec["name"] for spec in OPTIONS_CATALOG if spec["cxx_type"] == "bool"
}
_INT_OPTION_NAMES: set[str] = {
    spec["name"] for spec in OPTIONS_CATALOG if spec["cxx_type"] in {"int32_t", "int64_t"}
}
_STRING_OPTION_NAMES: set[str] = {
    spec["name"] for spec in OPTIONS_CATALOG if spec["cxx_type"] == "std::string"
}
_STRING_LIST_OPTION_NAMES: set[str] = {
    spec["name"] for spec in OPTIONS_CATALOG if spec["cxx_type"] == "std::vector<std::string>"
}


_ENUM_BY_OPTION_NAME: dict[str, Any] = {
    spec["name"]: ENUM_BY_CXX_TYPE[spec["cxx_type"]]
    for spec in OPTIONS_CATALOG
    if spec["cxx_type"] in ENUM_BY_CXX_TYPE
}
_ENUM_VALUES_BY_OPTION_NAME: dict[str, set[int]] = {
    option_name: ENUM_VALUES_BY_TYPE[enum_type]
    for option_name, enum_type in _ENUM_BY_OPTION_NAME.items()
}
_ENUM_ALIASES_BY_OPTION_NAME: dict[str, dict[str, str]] = {
    spec["name"]: ENUM_ALIASES_BY_TYPE[ENUM_BY_CXX_TYPE[spec["cxx_type"]]]
    for spec in OPTIONS_CATALOG
    if spec["cxx_type"] in ENUM_BY_CXX_TYPE
    and ENUM_BY_CXX_TYPE[spec["cxx_type"]] in ENUM_ALIASES_BY_TYPE
}


def _coerce_enum_if_needed(option_name: str, value: Any) -> Any:
    """If `value` is a string and `option_name` has a known enum type, map it."""
    enum_type = _ENUM_BY_OPTION_NAME.get(option_name)
    if enum_type is None:
        return value
    v = _norm_enum(value)
    if isinstance(v, enum_type):
        return v
    if isinstance(v, int):
        if isinstance(v, bool) or v not in _ENUM_VALUES_BY_OPTION_NAME[option_name]:
            raise ValueError(f"Invalid value for option '{option_name}': {value!r}")
        return v
    if not isinstance(v, str):
        raise TypeError(f"Option '{option_name}' must be an enum value or string name")
    v = _ENUM_ALIASES_BY_OPTION_NAME.get(option_name, {}).get(v, v)
    member = enum_type.__members__.get(v)
    if member is not None:
        return member
    raise ValueError(f"Invalid value for option '{option_name}': {value!r}")


def _coerce_option_value_if_needed(option_name: str, value: Any) -> Any:
    """Normalize and type-check catalog-backed option values."""
    if option_name in _BOOL_OPTION_NAMES and not isinstance(value, bool):
        raise TypeError(f"Option '{option_name}' must be a bool")
    if option_name in _INT_OPTION_NAMES and (isinstance(value, bool) or not isinstance(value, int)):
        raise TypeError(f"Option '{option_name}' must be an integer")
    if option_name in _STRING_OPTION_NAMES and not isinstance(value, str):
        raise TypeError(f"Option '{option_name}' must be a string")

    if option_name in _STRING_LIST_OPTION_NAMES:
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
    return value


class _Proxy:
    """A tiny attribute proxy for a group of *canonical* option names.

    This wrapper accepts canonical names from OPTIONS_CATALOG.
    """

    __slots__ = ("_allowed", "_raw")

    def __init__(self, raw: Any, allowed: set[str]):
        """Create a proxy over an allowed set of option names."""
        object.__setattr__(self, "_raw", raw)
        object.__setattr__(self, "_allowed", allowed)

    def _resolve_target(self, option_name: str) -> str:
        """Resolve an allowed option name or raise a clear error."""
        if option_name in self._allowed:
            return option_name

        raise AttributeError(
            f"Unknown option '{option_name}' for this group. "
            "If you recently changed native options, regenerate the Python catalog and rebuild."
        )

    def __getattr__(self, name: str) -> Any:
        """Return an option value from the raw options object."""
        target = self._resolve_target(name)
        return getattr(self._raw, target)

    def __setattr__(self, name: str, value: Any) -> None:
        """Normalize and set an option value on the raw object."""
        if name in _Proxy.__slots__:
            object.__setattr__(self, name, value)
            return
        target = self._resolve_target(name)

        value = _coerce_option_value_if_needed(target, value)

        v = _coerce_enum_if_needed(target, value)
        setattr(self._raw, target, v)

    def __dir__(self) -> list[str]:
        """Return the option names exposed by this proxy."""
        return sorted(self._allowed)


# ---- Group specifications (catalog-driven) ---------------------------------

# Build allowed sets from the generated catalog's `group` field.
_ALLOWED_BY_GROUP: dict[str, set[str]] = {}
for _s in OPTIONS_CATALOG:
    _ALLOWED_BY_GROUP.setdefault(_s["group"], set()).add(_s["name"])

# Internal group names used by Options(**kwargs) and exposed proxies.
_GROUP_NAMES: frozenset[str] = frozenset(_ALLOWED_BY_GROUP)
_GROUP_BY_OPTION_NAME: dict[str, str] = {
    option_name: group_name
    for group_name, option_names in _ALLOWED_BY_GROUP.items()
    for option_name in option_names
}
