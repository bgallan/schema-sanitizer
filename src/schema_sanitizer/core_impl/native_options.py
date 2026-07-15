"""Native option catalog, model, validation, and SZOPT wire protocol."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from threading import Lock
from typing import Any

from .logical_schema import LogicalSchemaPayload, encode_arrow_schema_payload
from .native_runtime import native_core as _native

_U8_MAX = 0xFF
_U32_MAX = 0xFFFFFFFF
_I32_MIN = -(1 << 31)
_I32_MAX = (1 << 31) - 1
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1
_MAX_STRING_LIST_ITEMS = 1 << 20
_MAX_OPTIONS_WIRE_BYTES = 64 * 1024 * 1024
_MAX_PREPARED_OPTIONS_CACHE_BYTES = 1 * 1024 * 1024
_MAX_PREPARED_OPTIONS_CACHE_ENTRIES = 128
_MAX_STRING_LIST_FINGERPRINT_BYTES = 64 * 1024
_MAX_STRING_LIST_FINGERPRINT_ITEMS = 4096


def optional_memory_limit_arg(memory_limit_bytes: int | None) -> int:
    """Return the native ABI sentinel for an optional memory limit."""
    return -1 if memory_limit_bytes is None else memory_limit_bytes


class SchemaEvolutionMode(Enum):
    """Schema evolution policies exposed to option normalization."""

    STRICT = 0
    ADDITIVE = 2


class FieldOrderPolicy(Enum):
    """Field ordering policies exposed to option normalization."""

    ALPHABETICALLY = 1
    SCHEMA_CONTRACT_FIRST = 2


class OnErrorPolicy(Enum):
    """Row error handling policies exposed to option normalization."""

    STOP = 0
    SKIP_ROW = 1
    EMIT_NULL_ROW = 2


ENUM_BY_KIND: dict[str, Any] = {
    "schema_evolution": SchemaEvolutionMode,
    "field_order": FieldOrderPolicy,
    "on_error": OnErrorPolicy,
}
_ENUM_VALUES_BY_TYPE: dict[Any, frozenset[int]] = {
    enum_type: frozenset(int(member.value) for member in enum_type)
    for enum_type in ENUM_BY_KIND.values()
}


def _norm_enum_name(value: str) -> str:
    """Normalize canonical enum member names without accepting aliases."""
    return value.strip().upper()


def normalize_field_name_policy_option(value: Any) -> str:
    """Validate one canonical public field-name policy value."""
    if not isinstance(value, str):
        raise TypeError("Option 'field_name_policy' must be a string")
    policy = value.strip().lower()
    if policy not in {"lower_alpha", "lower_snake", "preserve"}:
        raise ValueError(
            "Option 'field_name_policy' must be 'lower_alpha', 'lower_snake', or 'preserve'"
        )
    return policy


def coerce_enum_member(enum_type: Any, value: Any, *, label: str | None = None) -> Any:
    """Coerce an enum-like value to one canonical enum member."""
    subject = label or enum_type.__name__
    if isinstance(value, enum_type):
        return value
    if isinstance(value, int):
        if isinstance(value, bool) or value not in _ENUM_VALUES_BY_TYPE[enum_type]:
            raise ValueError(f"invalid enum value for {subject}: {value!r}")
        return enum_type(value)
    if isinstance(value, str):
        member = enum_type.__members__.get(_norm_enum_name(value))
        if member is not None:
            return member
        raise ValueError(f"invalid enum value for {subject}: {value!r}")
    raise TypeError(f"invalid enum value type for {subject}: {type(value)}")


def _coerce_enum_value(enum_type: Any, value: Any) -> int:
    """Coerce an enum-like option value to its stable integer representation."""
    return int(coerce_enum_member(enum_type, value).value)


@dataclass(frozen=True, slots=True)
class OptionSpec:
    """Describe one option in stable wire order."""

    name: str
    kind: str
    default: Any
    group: str


def _normalize_default(kind: str, value: Any) -> Any:
    """Convert one native metadata default to its Python model value."""
    enum_type = ENUM_BY_KIND.get(kind)
    if enum_type is not None:
        return enum_type(value)
    if kind == "string_list":
        return tuple(value)
    return value


def _load_options() -> tuple[OptionSpec, ...]:
    """Load and validate the native option catalog once."""
    specs: list[OptionSpec] = []
    for raw in _native.options_catalog():
        if not isinstance(raw, tuple) or len(raw) != 4:
            raise RuntimeError("native options catalog returned an invalid descriptor")
        name, kind, default, group = raw
        if not all(isinstance(value, str) for value in (name, kind, group)):
            raise RuntimeError("native options catalog returned invalid metadata")
        specs.append(OptionSpec(name, kind, _normalize_default(kind, default), group))
    return tuple(specs)


OPTIONS: tuple[OptionSpec, ...] = _load_options()
OPTION_NAMES: frozenset[str] = frozenset(spec.name for spec in OPTIONS)
_STRING_LIST_OPTION_NAMES: tuple[str, ...] = tuple(
    spec.name for spec in OPTIONS if spec.kind == "string_list"
)


def _clone_default_value(value: Any) -> Any:
    """Return an instance-safe option default value."""
    return list(value) if isinstance(value, tuple) else value


class Options:
    """Option values serialized through the package-owned ABI3 wire protocol."""

    _prepared_capsule: Any
    _prepared_string_lists: tuple[tuple[Any, ...], ...] | None
    __slots__ = ("__dict__",)

    def __init__(self) -> None:
        """Populate all options from native catalog defaults."""
        object.__setattr__(self, "_prepared_capsule", None)
        object.__setattr__(self, "_prepared_string_lists", None)
        for spec in OPTIONS:
            object.__setattr__(self, spec.name, _clone_default_value(spec.default))

    def __setattr__(self, name: str, value: Any) -> None:
        """Set a catalog-defined option value and invalidate compiled state."""
        if name not in OPTION_NAMES:
            raise AttributeError(f"Unknown option attribute {name!r}")
        object.__setattr__(self, name, value)
        object.__setattr__(self, "_prepared_capsule", None)
        object.__setattr__(self, "_prepared_string_lists", None)


def _require_int_value(name: str, value: Any) -> int:
    """Validate a primitive integer codec value."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"options serialization: {name} must be an integer")
    return value


def _ensure_wire_growth(out: bytearray, additional: int) -> None:
    """Reject an options payload before its backing buffer grows too large."""
    if additional < 0 or len(out) > _MAX_OPTIONS_WIRE_BYTES - additional:
        raise ValueError("options serialization: payload exceeds safety limit")


def _append_u8(out: bytearray, value: int) -> None:
    """Append an unsigned 8-bit integer."""
    value = _require_int_value("u8", value)
    if not (0 <= value <= _U8_MAX):
        raise ValueError("options serialization: u8 out of range")
    _ensure_wire_growth(out, 1)
    out.append(value)


def _append_u32(out: bytearray, value: int) -> None:
    """Append a little-endian unsigned 32-bit integer."""
    value = _require_int_value("u32", value)
    if not (0 <= value <= _U32_MAX):
        raise ValueError("options serialization: u32 out of range")
    _ensure_wire_growth(out, 4)
    out.extend(value.to_bytes(4, "little", signed=False))


def _append_i32(out: bytearray, value: int) -> None:
    """Append a little-endian signed 32-bit integer."""
    value = _require_int_value("i32", value)
    if not (_I32_MIN <= value <= _I32_MAX):
        raise ValueError("options serialization: i32 out of range")
    _ensure_wire_growth(out, 4)
    out.extend(value.to_bytes(4, "little", signed=True))


def _append_i64(out: bytearray, value: int) -> None:
    """Append a little-endian signed 64-bit integer."""
    value = _require_int_value("i64", value)
    if not (_I64_MIN <= value <= _I64_MAX):
        raise ValueError("options serialization: i64 out of range")
    _ensure_wire_growth(out, 8)
    out.extend(value.to_bytes(8, "little", signed=True))


def _append_string(out: bytearray, value: str) -> None:
    """Append a length-prefixed UTF-8 string."""
    encoded = value.encode("utf-8")
    if len(encoded) > _U32_MAX:
        raise ValueError("options serialization: string too large")
    _append_u32(out, len(encoded))
    _ensure_wire_growth(out, len(encoded))
    out.extend(encoded)


def _append_vec_string(out: bytearray, values: Iterable[str]) -> None:
    """Append strings without first retaining an arbitrary iterable in a list."""
    count_offset = len(out)
    _append_u32(out, 0)
    count = 0
    for value in values:
        if count >= _MAX_STRING_LIST_ITEMS:
            raise ValueError("options serialization: vector<string> exceeds safety limit")
        if not isinstance(value, str):
            raise TypeError("options serialization: vector<string> items must be strings")
        _append_string(out, value)
        count += 1
    out[count_offset : count_offset + 4] = count.to_bytes(4, "little")


def _read_u32(data: memoryview, pos: int) -> tuple[int, int]:
    """Read an unsigned 32-bit integer and return the next position."""
    if pos + 4 > len(data):
        raise ValueError("options deserialization: truncated u32")
    return int.from_bytes(data[pos : pos + 4], "little", signed=False), pos + 4


def _append_schema(out: bytearray, schema: Any) -> None:
    """Append an optional logical schema to the options wire payload."""
    if schema is None:
        _append_u8(out, 0)
        return
    payload = (
        schema.payload
        if isinstance(schema, LogicalSchemaPayload)
        else encode_arrow_schema_payload(schema)
    )
    _append_u8(out, 1)
    _append_u32(out, len(payload))
    _ensure_wire_growth(out, len(payload))
    out.extend(payload)


def _append_option_value(out: bytearray, spec: OptionSpec, value: Any) -> None:
    """Append one catalog option value to an SZOPT payload."""
    name, kind = spec.name, spec.kind
    if kind == "bool":
        if not isinstance(value, bool):
            raise TypeError(f"options serialization: {name} must be a bool")
        _append_u8(out, int(value))
    elif kind == "i32":
        _append_i32(out, _require_int_value(name, value))
    elif kind == "i64":
        _append_i64(out, _require_int_value(name, value))
    elif kind == "string":
        if not isinstance(value, str):
            raise TypeError(f"options serialization: {name} must be a string")
        _append_string(out, value)
    elif kind == "string_list":
        if value is None or isinstance(value, (str, bytes, bytearray)):
            raise TypeError(f"options serialization: {name} must be a sequence of strings")
        _append_vec_string(out, value)
    elif kind == "logical_schema":
        _append_schema(out, value)
    elif enum_type := ENUM_BY_KIND.get(kind):
        _append_i32(out, _coerce_enum_value(enum_type, value))
    else:
        raise RuntimeError(f"Unsupported native option kind: {kind} ({name})")


def _encode_options_bytes(options: Options) -> bytes:
    """Encode options in stable native wire order."""
    out = bytearray(b"SZOPT16")
    _append_u32(out, 16)
    for spec in OPTIONS:
        _append_option_value(out, spec, getattr(options, spec.name))
    return bytes(out)


_PREPARED_OPTIONS_CACHE: OrderedDict[bytes, Any] = OrderedDict()
_PREPARED_OPTIONS_CACHE_BYTES = 0
_PREPARED_OPTIONS_CACHE_LOCK = Lock()


def _cached_options_capsule(encoded: bytes) -> Any:
    """Return compiled option state from a byte- and entry-bounded LRU cache."""
    global _PREPARED_OPTIONS_CACHE_BYTES
    if len(encoded) > _MAX_PREPARED_OPTIONS_CACHE_BYTES:
        return _native.options_prepare_bytes(encoded)
    with _PREPARED_OPTIONS_CACHE_LOCK:
        cached = _PREPARED_OPTIONS_CACHE.get(encoded)
        if cached is not None:
            _PREPARED_OPTIONS_CACHE.move_to_end(encoded)
            return cached
    capsule = _native.options_prepare_bytes(encoded)
    with _PREPARED_OPTIONS_CACHE_LOCK:
        cached = _PREPARED_OPTIONS_CACHE.get(encoded)
        if cached is not None:
            _PREPARED_OPTIONS_CACHE.move_to_end(encoded)
            return cached
        while _PREPARED_OPTIONS_CACHE and (
            len(_PREPARED_OPTIONS_CACHE) >= _MAX_PREPARED_OPTIONS_CACHE_ENTRIES
            or _PREPARED_OPTIONS_CACHE_BYTES
            > _MAX_PREPARED_OPTIONS_CACHE_BYTES - len(encoded)
        ):
            evicted_key, _evicted_capsule = _PREPARED_OPTIONS_CACHE.popitem(last=False)
            _PREPARED_OPTIONS_CACHE_BYTES -= len(evicted_key)
        _PREPARED_OPTIONS_CACHE[encoded] = capsule
        _PREPARED_OPTIONS_CACHE_BYTES += len(encoded)
    return capsule


def _bounded_fingerprint_string_bytes(value: str, remaining: int) -> int | None:
    """Return an exact small UTF-8 size without copying an oversized value."""
    if remaining < 0 or len(value) > remaining:
        return None
    size = len(value.encode("utf-8"))
    return size if size <= remaining else None


def _string_list_fingerprint(options: Options) -> tuple[tuple[Any, ...], ...] | None:
    """Return compact mutation state only when retaining it is inexpensive."""
    values: list[tuple[Any, ...]] = []
    used_bytes = 0
    used_items = 0
    for name in _STRING_LIST_OPTION_NAMES:
        value = getattr(options, name)
        if not isinstance(value, (list, tuple)):
            return None
        if len(value) > _MAX_STRING_LIST_FINGERPRINT_ITEMS - used_items:
            return None
        retained: list[str] = []
        for item in value:
            if not isinstance(item, str):
                return None
            item_bytes = _bounded_fingerprint_string_bytes(
                item, _MAX_STRING_LIST_FINGERPRINT_BYTES - used_bytes
            )
            if item_bytes is None:
                return None
            retained.append(item)
            used_bytes += item_bytes
            used_items += 1
        values.append(tuple(retained))
    return tuple(values)


def _options_capsule(options: Any) -> Any:
    """Return compiled native option state, reusing unchanged objects."""
    if options is None:
        return None
    if not isinstance(options, Options):
        raise TypeError("options must be None or an Options object")
    fingerprint = _string_list_fingerprint(options)
    capsule = options._prepared_capsule
    if (
        capsule is not None
        and fingerprint is not None
        and fingerprint == options._prepared_string_lists
    ):
        return capsule
    encoded = _encode_options_bytes(options)
    capsule = _cached_options_capsule(encoded)
    if fingerprint is not None:
        object.__setattr__(options, "_prepared_capsule", capsule)
        object.__setattr__(options, "_prepared_string_lists", fingerprint)
    else:
        # A very large or exotic mutable sequence cannot be tracked cheaply.
        # Do not retain a second compiled representation on the Options object.
        object.__setattr__(options, "_prepared_capsule", None)
        object.__setattr__(options, "_prepared_string_lists", None)
    return capsule


def validate_options(options: Any) -> None:
    """Validate Options against the native runtime."""
    if options is None:
        _native.options_prepare_bytes(b"")
        return
    _options_capsule(options)
