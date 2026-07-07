"""Default-expression and enum coercion helpers for SZOPT options."""

from __future__ import annotations

import ast
from collections.abc import Callable
from typing import Any

from .options_enum_metadata import (
    ENUM_ALIASES_BY_TYPE,
    ENUM_BY_CXX_TYPE,
    ENUM_VALUES_BY_TYPE,
    norm_enum_name,
)

_UNARY_INT_OPS: dict[type[ast.unaryop], Callable[[int], int]] = {
    ast.UAdd: lambda v: v,
    ast.USub: lambda v: -v,
}
_BINARY_INT_OPS: dict[type[ast.operator], Callable[[int, int], int]] = {
    ast.Add: lambda lhs, rhs: lhs + rhs,
    ast.Sub: lambda lhs, rhs: lhs - rhs,
    ast.Mult: lambda lhs, rhs: lhs * rhs,
}
_DEFAULT_LITERALS: dict[str, Any] = {
    "true": True,
    "false": False,
    "std::nullopt": None,
}


def _coerce_enum_value(enum_type: Any, v: Any) -> int:
    """Coerce an enum-like value to its integer representation."""
    if v is None:
        raise ValueError(f"enum value cannot be None for {enum_type.__name__}")
    if isinstance(v, enum_type):
        return int(v.value)
    if isinstance(v, int):
        if isinstance(v, bool) or v not in ENUM_VALUES_BY_TYPE[enum_type]:
            raise ValueError(f"invalid enum value for {enum_type.__name__}: {v!r}")
        return int(v)
    if isinstance(v, str):
        key = norm_enum_name(v)
        key = ENUM_ALIASES_BY_TYPE.get(enum_type, {}).get(key, key)
        member = enum_type.__members__.get(key)
        if member is not None:
            return int(member.value)
        raise ValueError(f"invalid enum value for {enum_type.__name__}: {v!r}")
    raise TypeError(f"invalid enum value type for {enum_type.__name__}: {type(v)}")


def _safe_int_expr(expr: str) -> int:
    """Evaluate a tiny integer-only arithmetic expression safely."""

    def _eval(node: ast.AST) -> int:
        """Evaluate one supported integer expression node."""
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, int)
            and not isinstance(node.value, bool)
        ):
            return node.value
        if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_INT_OPS:
            return _UNARY_INT_OPS[type(node.op)](_eval(node.operand))
        if isinstance(node, ast.BinOp):
            op = _BINARY_INT_OPS.get(type(node.op))
            if op is not None:
                return op(_eval(node.left), _eval(node.right))
        raise ValueError("unsupported expression")

    tree = ast.parse(expr, mode="eval")
    return _eval(tree)


def _parse_string_default(e: str) -> str:
    """Parse a catalog std::string default expression."""
    inner = e[len("std::string(") : -1].strip()
    if inner.startswith('"') and inner.endswith('"'):
        return inner[1:-1]
    raise ValueError(f"Unsupported string default: {e!r}")


def _parse_enum_default(cxx_type: str, expr: str, e: str) -> Any:
    """Parse a catalog enum default expression."""
    enum_type = ENUM_BY_CXX_TYPE[cxx_type]
    last = e.rsplit("::", maxsplit=1)[-1]
    last = norm_enum_name(last)
    member = enum_type.__members__.get(last)
    if member is not None:
        return member
    raise ValueError(f"Unsupported enum default: {expr!r} for {cxx_type}")


def _parse_integer_default(cxx_type: str, expr: str, e: str) -> int:
    """Parse a catalog integer default expression."""
    try:
        cleaned = e.replace("LL", "").replace("ULL", "").replace("u", "").replace("U", "")
        if any(c.isalpha() for c in cleaned):
            raise ValueError
        return _safe_int_expr(cleaned)
    except Exception as exc:
        raise ValueError(f"Unsupported option default: {expr!r} for {cxx_type}") from exc


def _parse_default_expr(cxx_type: str, expr: str) -> Any:
    """Parse a supported C++ catalog default expression."""
    e = expr.strip()
    if e in _DEFAULT_LITERALS:
        return _DEFAULT_LITERALS[e]
    if e == "{}":
        if cxx_type == "std::vector<std::string>":
            return []
        raise ValueError(f"Unsupported option default: {expr!r} for {cxx_type}")

    if e.startswith("std::string(") and e.endswith(")"):
        return _parse_string_default(e)
    if cxx_type in ENUM_BY_CXX_TYPE and "::" in e:
        return _parse_enum_default(cxx_type, expr, e)

    return _parse_integer_default(cxx_type, expr, e)
