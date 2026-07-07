"""Shared enum metadata for Python option normalization and SZOPT encoding."""

from __future__ import annotations

from typing import Any

from .native import FieldOrderPolicy, OnErrorPolicy, SchemaEvolutionMode


def norm_enum_name(value: str) -> str:
    """Normalize a C++ enum spelling to a Python member name."""
    text = value.strip()
    if not text:
        return value
    if len(text) > 1 and text[0] in {"k", "K"} and text[1].isalpha():
        text = text[1:]

    out: list[str] = []
    for i, ch in enumerate(text.replace("-", "_")):
        if ch == "_":
            if out and out[-1] != "_":
                out.append("_")
            continue
        if ch.isupper() and i > 0:
            prev = text[i - 1]
            next_ch = text[i + 1] if i + 1 < len(text) else ""
            if (
                prev != "_"
                and (prev.islower() or prev.isdigit() or next_ch.islower())
                and out
                and out[-1] != "_"
            ):
                out.append("_")
        out.append(ch.upper())
    return "".join(out).strip("_")


ENUM_BY_CXX_TYPE: dict[str, Any] = {
    "sanitize::SchemaEvolutionMode": SchemaEvolutionMode,
    "sanitize::FieldOrderPolicy": FieldOrderPolicy,
    "sanitize::OnErrorPolicy": OnErrorPolicy,
}
ENUM_VALUES_BY_TYPE: dict[Any, set[int]] = {
    enum_type: {int(member.value) for member in enum_type}
    for enum_type in ENUM_BY_CXX_TYPE.values()
}
ENUM_ALIASES_BY_TYPE: dict[Any, dict[str, str]] = {
    FieldOrderPolicy: {
        "SORTED": "ALPHABETICALLY",
    },
}
