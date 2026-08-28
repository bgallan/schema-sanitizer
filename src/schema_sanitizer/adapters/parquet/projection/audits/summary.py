"""Shared normalization and fingerprint helpers for projection audits.

It normalizes layout summaries and centralizes duplicate-name, fingerprint, mismatch,
and diagnostic-note handling for every audit mode.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from typing import Any


def duplicate_names(values: Iterable[str]) -> list[str]:
    """Return duplicate names in deterministic order using one counting pass."""
    return sorted(name for name, count in Counter(values).items() if count > 1)


def note_mismatch(audit: dict[str, Any], message: str) -> None:
    """Mark an audit unstable and append one diagnostic."""
    audit["stable"] = False
    audit["mismatches"].append(message)


def summary_list(summary: dict[str, Any] | None, key: str) -> list[str]:
    """Return a normalized string list from one layout summary field."""
    if not isinstance(summary, dict):
        return []
    return [str(value) for value in list(summary.get(key) or [])]


def summary_dict(summary: dict[str, Any] | None, key: str) -> dict[str, str]:
    """Return a normalized string mapping from one layout summary field."""
    if not isinstance(summary, dict):
        return {}
    raw = summary.get(key) or {}
    if not isinstance(raw, dict):
        return {}
    return {str(name): str(value) for name, value in raw.items()}


def ordered_fingerprint(mapping: dict[str, str], names: list[str]) -> str:
    """Return a fingerprint that preserves the requested projection order."""
    return ";".join(f"{name}={mapping[name]}" for name in names if name in mapping)


def canonical_fingerprint(mapping: dict[str, str], names: list[str]) -> str:
    """Return a deterministic fingerprint independent of projection order."""
    return ";".join(f"{name}={mapping[name]}" for name in sorted(set(names)) if name in mapping)
