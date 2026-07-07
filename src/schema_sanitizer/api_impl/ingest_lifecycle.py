"""Private cleanup helpers for ingest runtime wrappers."""

from __future__ import annotations

from contextlib import suppress
from typing import Any


def _close_suppressing_errors(obj: Any, *, main_stream_only: bool = False) -> None:
    """Best-effort close an object or only its primary stream."""
    if obj is None:
        return
    fn = None
    if main_stream_only:
        fn = getattr(obj, "close_main_stream", None)
    if fn is None:
        fn = getattr(obj, "close", None)
    if callable(fn):
        with suppress(Exception):
            fn()


def _close_keepalive_attr(owner: Any) -> None:
    """Close and remove an owner's keepalive resource."""
    keepalive = getattr(owner, "_keepalive", None)
    if keepalive is None:
        return
    _close_suppressing_errors(keepalive)
    with suppress(Exception):
        delattr(owner, "_keepalive")


def _close_and_clear_attrs(owner: Any, *attrs: str) -> None:
    """Close unique resources stored in attributes and clear them."""
    seen: set[int] = set()
    for attr in attrs:
        obj = getattr(owner, attr, None)
        if obj is not None:
            ident = id(obj)
            if ident not in seen:
                seen.add(ident)
                _close_suppressing_errors(obj)
        with suppress(Exception):
            object.__setattr__(owner, attr, None)


def _close_resource_owner_attr(owner: Any) -> None:
    """Close and remove an owner's retained resource owner."""
    resource_owner = getattr(owner, "_resource_owner", None)
    if resource_owner is None:
        return
    _close_suppressing_errors(resource_owner)
    with suppress(Exception):
        delattr(owner, "_resource_owner")
