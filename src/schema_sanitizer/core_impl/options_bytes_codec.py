"""Primitive SZOPT byte codec helpers."""

from __future__ import annotations

from collections.abc import Iterable

_U8_MAX = 0xFF
_U32_MAX = 0xFFFFFFFF
_I32_MIN = -(1 << 31)
_I32_MAX = (1 << 31) - 1
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1


def _require_int_value(name: str, x: int) -> int:
    """Validate a primitive integer codec value."""
    if isinstance(x, bool) or not isinstance(x, int):
        raise TypeError(f"options serialization: {name} must be an integer")
    return x


def _append_u8(out: bytearray, x: int) -> None:
    """Append an unsigned 8-bit integer."""
    x = _require_int_value("u8", x)
    if not (0 <= x <= _U8_MAX):
        raise ValueError("options serialization: u8 out of range")
    out.append(x & _U8_MAX)


def _append_u32(out: bytearray, x: int) -> None:
    """Append a little-endian unsigned 32-bit integer."""
    x = _require_int_value("u32", x)
    if not (0 <= x <= _U32_MAX):
        raise ValueError("options serialization: u32 out of range")
    out.extend(x.to_bytes(4, "little", signed=False))


def _append_i32(out: bytearray, x: int) -> None:
    """Append a little-endian signed 32-bit integer."""
    x = _require_int_value("i32", x)
    if not (_I32_MIN <= x <= _I32_MAX):
        raise ValueError("options serialization: i32 out of range")
    out.extend(x.to_bytes(4, "little", signed=True))


def _append_i64(out: bytearray, x: int) -> None:
    """Append a little-endian signed 64-bit integer."""
    x = _require_int_value("i64", x)
    if not (_I64_MIN <= x <= _I64_MAX):
        raise ValueError("options serialization: i64 out of range")
    out.extend(x.to_bytes(8, "little", signed=True))


def _append_string(out: bytearray, s: str) -> None:
    """Append a length-prefixed UTF-8 string."""
    b = s.encode("utf-8")
    if len(b) > _U32_MAX:
        raise ValueError("options serialization: string too large")
    _append_u32(out, len(b))
    out.extend(b)


def _append_vec_string(out: bytearray, v: Iterable[str]) -> None:
    """Append a length-prefixed vector of strings."""
    items = list(v)
    if len(items) > _U32_MAX:
        raise ValueError("options serialization: vector<string> too large")
    _append_u32(out, len(items))
    for s in items:
        if not isinstance(s, str):
            raise TypeError("options serialization: vector<string> items must be strings")
        _append_string(out, s)


def _read_u8(data: memoryview, pos: int) -> tuple[int, int]:
    """Read an unsigned 8-bit integer and return the next position."""
    if pos + 1 > len(data):
        raise ValueError("options deserialization: truncated u8")
    return data[pos], pos + 1


def _read_u32(data: memoryview, pos: int) -> tuple[int, int]:
    """Read an unsigned 32-bit integer and return the next position."""
    if pos + 4 > len(data):
        raise ValueError("options deserialization: truncated u32")
    return int.from_bytes(data[pos : pos + 4], "little", signed=False), pos + 4


def _read_string(data: memoryview, pos: int) -> tuple[str, int]:
    """Read a length-prefixed UTF-8 string and return the next position."""
    n, pos = _read_u32(data, pos)
    if pos + n > len(data):
        raise ValueError("options deserialization: truncated string")
    b = bytes(data[pos : pos + n])
    return b.decode("utf-8"), pos + n
