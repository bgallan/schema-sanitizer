"""No-throw exception summaries and bounded cleanup notes."""

from __future__ import annotations

from typing import Any

_DEFAULT_MAX_CHARS = 512


def _safe_type_name(value: object) -> str:
    try:
        name = type(value).__name__
    except BaseException:
        return "BaseException"
    return name if type(name) is str and name else "BaseException"


def safe_exception_summary(
    exc: BaseException,
    *,
    max_chars: int = _DEFAULT_MAX_CHARS,
) -> str:
    """Return a bounded exact string even for hostile exception implementations."""
    maximum = max(16, int(max_chars))
    name = _safe_type_name(exc)
    text = ""
    try:
        candidate = str(exc)
        if type(candidate) is str:
            text = candidate
    except BaseException:
        try:
            candidate = repr(exc)
            if type(candidate) is str:
                text = candidate
        except BaseException:
            text = "<unprintable exception>"
    prefix = f"{name}: "
    try:
        return (prefix + text)[:maximum]
    except BaseException:
        return "BaseException: <unprintable exception>"[:maximum]


def safe_object_summary(value: Any, *, max_chars: int = 256) -> str:
    """Return a bounded representation without allowing formatting failures through."""
    maximum = max(16, int(max_chars))
    try:
        candidate = repr(value)
        if type(candidate) is str:
            return candidate[:maximum]
    except BaseException:
        pass
    return f"<{_safe_type_name(value)} unavailable>"[:maximum]


def add_bounded_note(
    primary: BaseException,
    label: str,
    secondary: BaseException | object,
    *,
    max_chars: int = _DEFAULT_MAX_CHARS,
) -> None:
    """Attach one bounded note without ever masking the primary exception."""
    try:
        if isinstance(secondary, BaseException):
            detail = safe_exception_summary(secondary, max_chars=max_chars)
        else:
            detail = safe_object_summary(secondary, max_chars=max_chars)
        safe_label = label if type(label) is str else "cleanup failure"
        primary.add_note(f"{safe_label}: {detail}"[:max_chars])
    except BaseException:
        return


def clear_exception_traceback(exc: BaseException) -> None:
    """Best-effort cycle breaking for an exception consumed internally."""
    try:
        exc.__traceback__ = None
    except BaseException:
        return


__all__ = [
    "add_bounded_note",
    "clear_exception_traceback",
    "safe_exception_summary",
    "safe_object_summary",
]
