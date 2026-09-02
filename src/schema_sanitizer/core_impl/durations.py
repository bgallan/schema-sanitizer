"""Normalize runtime durations and deadlines without side effects.

Boolean, nonfinite, and negative inputs are rejected before timeouts are converted to monotonic
deadlines and bounded remaining-wait values.
"""

from __future__ import annotations

import math
from time import monotonic, monotonic_ns

_MAX_DEADLINE_NS = (1 << 63) - 1
_MAX_DURATION_SECONDS = _MAX_DEADLINE_NS / 1_000_000_000


def _validated_builtin_duration(
    value: int | float | None,
    *,
    name: str,
    allow_none: bool,
    allow_zero: bool,
) -> int | float | None:
    """Validate and normalize a built-in numeric duration."""
    if value is None:
        if allow_none:
            return None
        raise TypeError(f"{name} must be a finite number")
    if type(value) not in (int, float):
        raise TypeError(f"{name} must be an int or float")
    if type(value) is float and not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    if value < 0 or (value == 0 and not allow_zero):
        comparator = ">= 0" if allow_zero else "> 0"
        raise ValueError(f"{name} must be {comparator}")
    return value


def normalize_duration(
    value: int | float | None,
    *,
    name: str,
    allow_none: bool = False,
    allow_zero: bool = True,
    saturate_seconds: float | None = None,
) -> float | None:
    """Return one finite non-negative duration without invoking conversion hooks.

    Huge exact integers are clamped before conversion to float, avoiding the
    ``OverflowError`` that ``float(10 ** 10000)`` would otherwise raise.
    """
    validated = _validated_builtin_duration(
        value, name=name, allow_none=allow_none, allow_zero=allow_zero
    )
    if validated is None:
        return None
    limit = _MAX_DURATION_SECONDS
    if saturate_seconds is not None:
        if type(saturate_seconds) not in (int, float):
            raise TypeError("duration saturation limit must be an int or float")
        if type(saturate_seconds) is float and not math.isfinite(saturate_seconds):
            raise ValueError("duration saturation limit must be finite")
        limit = min(limit, max(0.0, float(saturate_seconds)))
    if type(validated) is int:
        if validated >= int(limit) + 1:
            return float(limit)
        return min(float(validated), float(limit))
    return min(validated, float(limit))


def deadline_ns_from_timeout(
    value: int | float,
    *,
    name: str,
    allow_zero: bool = True,
) -> int:
    """Convert one duration to a saturated absolute monotonic deadline."""
    validated = _validated_builtin_duration(
        value, name=name, allow_none=False, allow_zero=allow_zero
    )
    if validated is None:
        raise AssertionError("required deadline duration cannot be absent")
    now = monotonic_ns()
    remaining_ns = max(0, _MAX_DEADLINE_NS - now)
    if type(validated) is int:
        if validated > remaining_ns // 1_000_000_000:
            return _MAX_DEADLINE_NS
        duration_ns = validated * 1_000_000_000
    else:
        maximum_seconds = remaining_ns / 1_000_000_000
        if validated >= maximum_seconds:
            return _MAX_DEADLINE_NS
        duration_ns = int(validated * 1_000_000_000)
    return min(_MAX_DEADLINE_NS, now + duration_ns)


def deadline_from_timeout(
    value: int | float,
    *,
    name: str,
    allow_zero: bool = True,
) -> float:
    """Return a finite monotonic float deadline for APIs requiring seconds."""
    seconds = normalize_duration(
        value,
        name=name,
        allow_zero=allow_zero,
        saturate_seconds=_MAX_DURATION_SECONDS,
    )
    if seconds is None:
        raise AssertionError("required monotonic duration cannot be absent")
    return min(float(_MAX_DEADLINE_NS), monotonic() + seconds)


def remaining_seconds(deadline_ns: int) -> float:
    """Return non-negative seconds remaining until one monotonic deadline."""
    return max(0.0, (int(deadline_ns) - monotonic_ns()) / 1_000_000_000)


__all__ = [
    "deadline_from_timeout",
    "deadline_ns_from_timeout",
    "normalize_duration",
    "remaining_seconds",
]
