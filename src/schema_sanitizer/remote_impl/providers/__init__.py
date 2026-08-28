"""Transport-neutral semantics shared by remote object providers."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any

from ...core_impl.uris import name_matches
from ...input_impl.directory_inputs import split_parent_child


def direct_child_name(name: Any, prefix: str, suffixes: tuple[str, ...]) -> str | None:
    """Return a matching direct-child name relative to its listing prefix."""
    if not isinstance(name, str):
        return None
    relative = name[len(prefix) :] if name.startswith(prefix) else name
    return (
        relative if relative and "/" not in relative and name_matches(relative, suffixes) else None
    )


def requested_child(
    name: Any,
    prefix: str,
    children: dict[str, list[str]],
    suffixes: tuple[str, ...],
) -> tuple[list[str], str] | None:
    """Return the requested-directory association for one listed object."""
    if not isinstance(name, str) or not name.startswith(prefix):
        return None
    child, separator, filename = name[len(prefix) :].partition("/")
    child_uris = children.get(child) if separator else None
    if child_uris and "/" not in filename and name_matches(filename, suffixes):
        return child_uris, filename
    return None


def direct_child_items(
    items: Iterable[Any],
    prefix: str,
    suffixes: tuple[str, ...],
    name_field: str,
) -> Iterator[tuple[Any, str]]:
    """Yield provider items representing matching direct children."""
    for item in items:
        name = (
            item.get(name_field) if isinstance(item, Mapping) else getattr(item, name_field, None)
        )
        relative = direct_child_name(name, prefix, suffixes)
        if relative is not None:
            yield item, relative


def requested_child_items(
    items: Iterable[Any],
    prefix: str,
    children: dict[str, list[str]],
    suffixes: tuple[str, ...],
    name_field: str,
) -> Iterator[tuple[Any, list[str], str]]:
    """Yield provider items belonging to requested child directories."""
    for item in items:
        name = (
            item.get(name_field) if isinstance(item, Mapping) else getattr(item, name_field, None)
        )
        match = requested_child(name, prefix, children, suffixes)
        if match is not None:
            child_uris, filename = match
            yield item, child_uris, filename


def requested_directory_groups(
    uris: Iterable[str],
    discovery: Any,
    locate: Callable[[str], tuple[tuple[Any, ...], str]],
) -> dict[tuple[Any, ...], dict[str, list[str]]]:
    """Group requested directories by provider location and parent prefix."""
    groups: dict[tuple[Any, ...], dict[str, list[str]]] = {}
    for uri in uris:
        key, object_name = locate(uri)
        parsed = split_parent_child(object_name)
        if parsed is None:
            continue
        parent_prefix, child = parsed
        group_key = (*key, parent_prefix)
        discovery.publish_group_association(
            lambda group_key=group_key, child=child, uri=uri: (
                groups.setdefault(group_key, {}).setdefault(child, []).append(uri)
            )
        )
    return groups


def next_page_token(
    payload: dict[str, Any],
    token_key: str,
    *,
    truncated_key: str | None = None,
    missing_error: str = "paginated response omitted its continuation token",
) -> str | None:
    """Return a valid continuation token or reject a truncated response."""
    if truncated_key is not None and not payload.get(truncated_key):
        return None
    token = payload.get(token_key)
    if isinstance(token, str) and token:
        return token
    if truncated_key is not None:
        raise RuntimeError(missing_error)
    return None


def sdk_error_identity(exc: Exception) -> tuple[Any, Any]:
    """Return a cloud SDK error's HTTP status and service code."""
    status = getattr(exc, "status_code", None)
    code = getattr(exc, "error_code", None)
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return status, code
    metadata = response.get("ResponseMetadata")
    if isinstance(metadata, dict):
        status = metadata.get("HTTPStatusCode", status)
    error = response.get("Error")
    if isinstance(error, dict):
        code = error.get("Code", code)
    return status, code


def retryable_sdk_error(
    exc: Exception,
    *,
    transport_modules: frozenset[str] = frozenset(),
) -> bool:
    """Return whether an idempotent cloud SDK request is transient."""
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    if exc.__class__.__module__.split(".", 1)[0] in transport_modules:
        return True
    status, code = sdk_error_identity(exc)
    return (
        status == 429
        or (isinstance(status, int) and status >= 500)
        or code in {"InternalError", "RequestTimeout", "ServiceUnavailable", "SlowDown"}
    )


__all__ = [
    "direct_child_items",
    "direct_child_name",
    "next_page_token",
    "requested_child_items",
    "requested_child",
    "requested_directory_groups",
    "retryable_sdk_error",
    "sdk_error_identity",
]
