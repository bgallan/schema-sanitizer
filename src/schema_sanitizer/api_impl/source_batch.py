"""Shared native source descriptors for public and warm-up ingestion."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SourceDescriptor:
    """One local source file and the native reader kind that should consume it."""

    kind: str
    path: str
    source_file: str


@dataclass(frozen=True, slots=True)
class PreparedSourceBatch:
    """A native-readable group of local source files plus shared input options."""

    sources: tuple[SourceDescriptor, ...]
    input_format: str
    input_mode: str | None = None
    csv_delimiter: str = ","
    csv_has_header: bool = True
    xml_row_tag: str | None = None
    memory_limit_bytes: int | None = None

    def path_source_tuples(self) -> list[tuple[str, str, str]]:
        """Return the ABI representation expected by native path-source calls."""
        return path_source_tuples(self)


def source_kind_for_format(input_format: str) -> str | None:
    """Return the native path-source kind for one public input format."""
    if input_format in {"json", "jsonl", "ndjson"}:
        return "json"
    if input_format in {"csv", "json_array", "xml"}:
        return input_format
    return None


def path_source_tuples(
    batch_or_sources: PreparedSourceBatch | Iterable[SourceDescriptor],
) -> list[tuple[str, str, str]]:
    """Return native ABI path-source tuples for a batch or source iterable."""
    sources = (
        batch_or_sources.sources
        if isinstance(batch_or_sources, PreparedSourceBatch)
        else batch_or_sources
    )
    return [(source.kind, source.path, source.source_file) for source in sources]
