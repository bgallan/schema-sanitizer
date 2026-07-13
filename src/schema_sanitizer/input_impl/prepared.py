"""Prepared-input value objects shared by API and pipeline layers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from schema_sanitizer.input_impl.source_plan import PreparedSourceBatch


@dataclass(slots=True)
class PreparedPublicInput:
    """Resolved public input payload and native selectors."""

    data: Any
    format: str
    source: str
    keepalive: Any = None
    xml_row_tag: str | None = None
    source_file: str | None = None
    source_file_spans: Any = None

    def close(self) -> None:
        """Close any generated reader."""
        close = getattr(self.keepalive, "close", None)
        if callable(close):
            close()
        self.keepalive = None


@dataclass(frozen=True, slots=True)
class NativeDirectorySourceManifest:
    """Canonical local directory source batch for native ingestion."""

    source_batch: PreparedSourceBatch


class StagedNativeDirectoryManifest:
    """Own one locally staged chunk of a remote native directory manifest."""

    def __init__(self, manifest: NativeDirectorySourceManifest, keepalive: Any):
        """Store the native manifest and its staged temporary files."""
        self.manifest = manifest
        self.keepalive = keepalive

    def close(self) -> None:
        """Remove the staged files for this chunk."""
        close = getattr(self.keepalive, "close", None)
        if callable(close):
            close()


class ChainedKeepalive:
    """Close multiple keepalive resources in reverse acquisition order."""

    def __init__(self, *items: Any):
        """Store resources that may expose close()."""
        self._items = list(items)

    def close(self) -> None:
        """Close every retained resource."""
        while self._items:
            item = self._items.pop()
            close = getattr(item, "close", None)
            if callable(close):
                close()


class NativeDirectoryManifestCarrier:
    """Minimal object used only to carry native directory manifest metadata."""

    def close(self) -> None:
        """Satisfy prepared-input keepalive cleanup."""
