"""Side-effect-free callable contract fingerprints for bounded runtime registries.

It fingerprints code, defaults, closures, and bounded safe containers while representing opaque
owners by identity instead of invoking user behavior.
"""

from __future__ import annotations

import types
from typing import Callable

_EMPTY = ("empty-cell",)
_UNSAFE = ("opaque",)
_MAX_DEPTH = 4
_MAX_ITEMS = 64


def _value_fingerprint(value: object, *, depth: int = 0) -> tuple[object, ...]:
    """Return a bounded fingerprint without arbitrary equality/hash/repr calls."""
    if depth > _MAX_DEPTH:
        return _UNSAFE + (type(value).__module__, type(value).__qualname__, id(value))
    if value is None or type(value) in (bool, int, float, str, bytes):
        return (type(value).__name__, value)
    if type(value) is tuple:
        if len(value) > _MAX_ITEMS:
            return ("tuple", len(value), "oversized")
        return ("tuple",) + tuple(_value_fingerprint(item, depth=depth + 1) for item in value)
    if type(value) is dict:
        if len(value) > _MAX_ITEMS:
            return ("dict", len(value), "oversized")
        entries: list[tuple[tuple[object, ...], tuple[object, ...]]] = []
        for key, item in value.items():
            key_fp = _value_fingerprint(key, depth=depth + 1)
            if key_fp and key_fp[0] == "opaque":
                return _UNSAFE + ("dict-key", id(value))
            entries.append((key_fp, _value_fingerprint(item, depth=depth + 1)))
        # Keys admitted above are bounded structural/scalar fingerprints; sorting
        # their textual tags never executes code from the captured objects.
        entries.sort(key=lambda pair: str(pair[0]))
        return ("dict",) + tuple(entries)
    if type(value) is frozenset:
        if len(value) > _MAX_ITEMS:
            return ("frozenset", len(value), "oversized")
        items: list[tuple[object, ...]] = []
        for item in value:
            fp = _value_fingerprint(item, depth=depth + 1)
            if fp and fp[0] in {"opaque", "tuple", "frozenset", "dict", "code"}:
                return _UNSAFE + ("frozenset", id(value))
            items.append(fp)
        items.sort(key=str)
        return ("frozenset",) + tuple(items)
    if isinstance(value, types.CodeType):
        return (
            "code",
            value.co_code,
            _value_fingerprint(value.co_consts, depth=depth + 1),
            value.co_names,
            value.co_varnames,
            value.co_argcount,
            value.co_kwonlyargcount,
        )
    # Distinct opaque owners are never inferred equivalent. Registries that
    # intentionally replace a singleton on module reload use an explicit role
    # name/generation and may normalize bound-owner identity themselves.
    return _UNSAFE + (type(value).__module__, type(value).__qualname__, id(value))


def callable_contract(fn: Callable[..., object] | None) -> tuple[object, ...] | None:
    """Return a stable structural identity for a callable when one is available."""
    if fn is None:
        return None
    bound_owner = getattr(fn, "__self__", None)
    target = getattr(fn, "__func__", fn)
    code = getattr(target, "__code__", None)
    closure = getattr(target, "__closure__", None) or ()
    closure_fp: list[tuple[object, ...]] = []
    for cell in closure:
        try:
            value = cell.cell_contents
        except ValueError:
            closure_fp.append(_EMPTY)
        else:
            closure_fp.append(_value_fingerprint(value))
    owner_contract: tuple[object, ...] | None = None
    if bound_owner is not None:
        static_kind = getattr(bound_owner, "_static_kind", None)
        capacity = getattr(bound_owner, "capacity", None)
        if type(static_kind) is str and type(capacity) is int:
            owner_contract = (
                "static-owner",
                type(bound_owner).__module__,
                type(bound_owner).__qualname__,
                static_kind,
                capacity,
            )
        else:
            owner_contract = (
                "bound-owner",
                type(bound_owner).__module__,
                type(bound_owner).__qualname__,
                id(bound_owner),
            )
    return (
        getattr(target, "__module__", None),
        getattr(target, "__qualname__", None),
        owner_contract,
        _value_fingerprint(code) if code is not None else None,
        _value_fingerprint(getattr(target, "__defaults__", None)),
        _value_fingerprint(getattr(target, "__kwdefaults__", None)),
        tuple(closure_fp),
    )


__all__ = ["callable_contract"]
