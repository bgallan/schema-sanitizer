"""Detect callbacks whose payloads hide large retained owner graphs.

Closure cells, bound arguments, partials, and callable objects are traversed under strict depth
and item limits so compact callbacks can be retained safely.
"""

from __future__ import annotations

import threading
from enum import Enum
from types import FunctionType
from typing import Any, Callable

_MAX_COMPACT_TEXT_BYTES = 4096
_MAX_COMPACT_SEQUENCE_ITEMS = 16
_MAX_COMPACT_INT_BITS = _MAX_COMPACT_TEXT_BYTES * 8


def _is_compact_value(value: Any, *, depth: int = 0) -> bool:
    """Return whether *value* cannot recursively own an unbounded Python graph."""
    value_type = type(value)
    if value is None or value_type in (bool, float, complex):
        return True
    if value_type is int:
        # PyLong is arbitrary precision. bit_length() is constant-space and
        # prevents a multi-megabyte integer from masquerading as a scalar.
        return value.bit_length() <= _MAX_COMPACT_INT_BITS
    if value_type is object:
        return True
    # Standard synchronization handles have fixed, small interpreter state and
    # do not recursively own application payload graphs. They are common in
    # completion callbacks and safe to retain as control metadata.
    if value_type in (threading.Event,):
        return True
    if value_type is str:
        # Avoid allocating an encoded copy just to validate admission. Four
        # bytes per code point is a conservative UTF-8 upper bound.
        return len(value) <= (_MAX_COMPACT_TEXT_BYTES // 4)
    if value_type in (bytes, bytearray):
        return len(value) <= _MAX_COMPACT_TEXT_BYTES
    if isinstance(value, Enum):
        # Enum members can carry arbitrary rich Python values. Only accept the
        # member when its payload is itself compact.
        try:
            enum_value = value.value
        except BaseException:
            return False
        return depth < 2 and _is_compact_value(enum_value, depth=depth + 1)
    if depth >= 2:
        return False
    if value_type in (tuple, frozenset, list):
        if len(value) > _MAX_COMPACT_SEQUENCE_ITEMS:
            return False
        return all(_is_compact_value(item, depth=depth + 1) for item in value)
    if value_type is dict:
        if len(value) > _MAX_COMPACT_SEQUENCE_ITEMS:
            return False
        return all(
            _is_compact_value(key, depth=depth + 1) and _is_compact_value(item, depth=depth + 1)
            for key, item in value.items()
        )
    # Stateless callable/control objects are compact. Any instance field makes
    # the owner explicit and therefore requires a subsystem token/capsule.
    namespace = getattr(value, "__dict__", None)
    if type(namespace) is dict and not namespace:
        slots = getattr(value_type, "__slots__", ())
        if not slots:
            return True
    return False


def callback_retains_hidden_owner(callback: Callable[..., Any], args: tuple[Any, ...] = ()) -> bool:
    """Return True when retaining a callback/args can hide arbitrary object ownership.

    This deliberately does not try to estimate ``sys.getsizeof`` recursively.  It
    accepts only a compact closed set of immutable/scalar payload shapes; every
    richer owner must live in a bounded subsystem registry and be referenced by
    a compact token.
    """
    owner = getattr(callback, "__self__", None)
    # Bound methods always retain their owner.  Even an owner that is empty
    # today may grow state after scheduling, so the retention contract must
    # not depend on a mutable snapshot of ``__dict__``.
    if owner is not None:
        # ``threading.Event.set`` is a fixed-size control signal routinely used
        # by retry completion paths. It cannot recursively own application
        # payload state, unlike arbitrary bound methods. Keep the whitelist
        # intentionally exact so rich owners still require tokens/capsules.
        if type(owner) is threading.Event and getattr(callback, "__name__", None) == "set":
            return any(not _is_compact_value(value) for value in args)
        return True

    if isinstance(callback, FunctionType):
        closure = callback.__closure__ or ()
        for cell in closure:
            try:
                value = cell.cell_contents
            except ValueError:
                continue
            if not _is_compact_value(value):
                return True
        defaults = callback.__defaults__ or ()
        if any(not _is_compact_value(value) for value in defaults):
            return True
        kwdefaults = callback.__kwdefaults__ or {}
        if any(not _is_compact_value(value) for value in kwdefaults.values()):
            return True
    elif owner is None:
        # Callable objects/partials may own arbitrary state. Stateless callable
        # objects are compact; richer objects must be tokenized.
        callback_type = type(callback)
        if callback_type.__module__ != "builtins" and not _is_compact_value(callback):
            return True

    return any(not _is_compact_value(value) for value in args)


__all__ = ["callback_retains_hidden_owner"]
