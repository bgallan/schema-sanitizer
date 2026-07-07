"""Implements `schema_sanitizer.core_impl.options_bytes`."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from ..public_impl.options_catalog import OPTIONS_CATALOG as _OPTIONS_CATALOG
from .native import _native
from .options_bytes_codec import (
    _append_i32,
    _append_i64,
    _append_string,
    _append_u8,
    _append_u32,
    _append_vec_string,
)
from .options_bytes_defaults import (
    _coerce_enum_value,
    _parse_default_expr,
)
from .options_enum_metadata import ENUM_BY_CXX_TYPE
from .options_logical_schema import _append_schema

_CATALOG_NAMES = frozenset(spec["name"] for spec in _OPTIONS_CATALOG)
_CATALOG_DEFAULTS = tuple(
    (
        spec["name"],
        _parse_default_expr(spec["cxx_type"], spec["default_expr"]),
    )
    for spec in _OPTIONS_CATALOG
)


def _clone_default_value(value: Any) -> Any:
    """Return an instance-safe option default value."""
    if isinstance(value, list):
        return list(value)
    return value


class Options:
    """ABI3-friendly Options object.

    In ABI3 builds, we keep
    it as a pure-Python object and serialize it using the stable SZOPT16 format.
    """

    __slots__ = ("__dict__",)

    def __init__(self) -> None:
        """Populate all options from catalog defaults."""
        for name, value in _CATALOG_DEFAULTS:
            setattr(self, name, _clone_default_value(value))

    def __setattr__(self, name: str, value: Any) -> None:
        """Set a catalog-defined option value."""
        if name not in _CATALOG_NAMES:
            raise AttributeError(f"Unknown option attribute {name!r}")
        object.__setattr__(self, name, value)


def _require_option_int(name: str, value: Any) -> int:
    """Validate an integer option before serialization."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"options serialization: {name} must be an integer")
    return value


def _append_option_value(out: bytearray, *, name: str, cxx_type: str, value: Any) -> None:
    """Append one catalog option value to an SZOPT payload."""
    if cxx_type == "bool":
        if not isinstance(value, bool):
            raise TypeError(f"options serialization: {name} must be a bool")
        _append_u8(out, 1 if value else 0)
    elif cxx_type == "int32_t":
        _append_i32(out, _require_option_int(name, value))
    elif cxx_type == "int64_t":
        _append_i64(out, _require_option_int(name, value))
    elif cxx_type == "std::string":
        if not isinstance(value, str):
            raise TypeError(f"options serialization: {name} must be a string")
        _append_string(out, value)
    elif cxx_type == "std::vector<std::string>":
        if value is None or isinstance(value, (str, bytes, bytearray)):
            raise TypeError(f"options serialization: {name} must be a sequence of strings")
        _append_vec_string(out, value)
    elif cxx_type == "std::optional<sanitize::LogicalSchema>":
        _append_schema(out, value)
    elif cxx_type in ENUM_BY_CXX_TYPE:
        _append_i32(out, _coerce_enum_value(ENUM_BY_CXX_TYPE[cxx_type], value))
    else:
        raise RuntimeError(f"Unsupported option type for ABI3 lane: {cxx_type} ({name})")


def _encode_options_bytes(opts: Options) -> bytes:
    """Encode options in the stable SZOPT16 wire format."""
    out = bytearray()
    out.extend(b"SZOPT16")
    _append_u32(out, 16)

    for spec in _OPTIONS_CATALOG:
        name = spec["name"]
        cxx_type = spec["cxx_type"]
        _append_option_value(out, name=name, cxx_type=cxx_type, value=getattr(opts, name))

    return bytes(out)


@lru_cache(maxsize=128)
def _cached_options_capsule(encoded: bytes) -> Any:
    """Return a cached native prepared-options capsule for encoded options."""
    return _native.options_prepare_bytes(encoded)


def _options_capsule(options: Any) -> Any:
    """Compile Options into an internal native capsule for one runtime call."""

    if options is None:
        return None
    if not isinstance(options, Options):
        raise TypeError("options must be None or an Options object")
    encoded = _encode_options_bytes(options)
    if len(encoded) <= 262_144:
        return _cached_options_capsule(encoded)
    return _native.options_prepare_bytes(encoded)


def validate_options(options: Any) -> None:
    """Validate Options against the native runtime without exposing prepared state."""

    if options is None:
        _native.options_prepare_bytes(b"")
        return
    if not isinstance(options, Options):
        raise TypeError("validate_options expects an Options object or None")
    _native.options_prepare_bytes(_encode_options_bytes(options))
