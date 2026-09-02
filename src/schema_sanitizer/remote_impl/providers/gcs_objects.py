"""GCS object references and immutable metadata decoding.

It parses immutable object references and decodes size, generation, ETag, and
modification-time metadata from GCS responses.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ...sources.models import RemoteFile


@dataclass(frozen=True, slots=True)
class GcsRef:
    """Parsed GCS object reference."""

    bucket: str
    object_name: str


def parse_uri(uri: str) -> GcsRef:
    """Parse a gs:// or gcs:// URI."""
    parsed = urlparse(uri)
    if parsed.scheme.lower() not in {"gs", "gcs"} or not parsed.netloc:
        raise ValueError(f"not a GCS URI: {uri!r}")
    return GcsRef(parsed.netloc, parsed.path.lstrip("/"))


def object_uri(bucket: str, object_name: str) -> str:
    """Render a GCS object URI."""
    return f"gs://{bucket}/{object_name}"


def directory_prefix(object_name: str) -> str:
    """Return a JSON API prefix without turning a bucket root into ``/``."""
    normalized = object_name.rstrip("/")
    return f"{normalized}/" if normalized else ""


def parse_timestamp(value: object, *, field: str) -> datetime | None:
    """Parse one RFC3339 GCS timestamp as an aware UTC datetime."""
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"GCS metadata field {field!r} must be an RFC3339 string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(
            f"GCS metadata field {field!r} is not a valid RFC3339 timestamp: {value!r}"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"GCS metadata field {field!r} must include a timezone")
    return parsed.astimezone(UTC)


def optional_text(payload: dict[str, Any], key: str) -> str | None:
    """Return one optional scalar metadata field as text."""
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, (str, int)):
        return str(value)
    raise ValueError(f"GCS metadata field {key!r} must be scalar text")


def remote_file_from_metadata(
    bucket: str,
    payload: dict[str, Any],
    *,
    display_name: str | None = None,
    uri: str | None = None,
) -> RemoteFile:
    """Build one immutable remote object from a GCS JSON API item."""
    name = payload.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError("GCS object metadata is missing a non-empty name")
    raw_size = payload.get("size")
    try:
        size = int(raw_size) if isinstance(raw_size, (str, int)) else None
    except ValueError:
        size = None
    return RemoteFile(
        uri or object_uri(bucket, name),
        display_name if display_name is not None else Path(name).name,
        size,
        updated=parse_timestamp(payload.get("updated"), field="updated"),
        time_created=parse_timestamp(payload.get("timeCreated"), field="timeCreated"),
        generation=optional_text(payload, "generation"),
        metageneration=optional_text(payload, "metageneration"),
        etag=optional_text(payload, "etag"),
        crc32c=optional_text(payload, "crc32c"),
    )


def remote_file_sort_key(file: RemoteFile) -> tuple[str, str]:
    """Sort selected contents deterministically by URI and generation."""
    return (file.uri, file.generation or "")


__all__ = [
    "GcsRef",
    "directory_prefix",
    "object_uri",
    "optional_text",
    "parse_timestamp",
    "parse_uri",
    "remote_file_from_metadata",
    "remote_file_sort_key",
]
