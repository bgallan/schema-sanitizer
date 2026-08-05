"""Prepared-input value objects shared by API and pipeline layers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from schema_sanitizer.input_impl.source_manifest import SourceManifest
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
    source_manifest: SourceManifest | None = None

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
        """Remove staged files and clear ownership only after success."""
        keepalive = self.keepalive
        close = getattr(keepalive, "close", None)
        if callable(close):
            close()
        if self.keepalive is keepalive:
            self.keepalive = None


class ChainedKeepalive:
    """Close multiple keepalive resources in reverse acquisition order."""

    def __init__(self, *items: Any):
        """Store resources that may expose close()."""
        self._items = list(items)

    def close(self) -> None:
        """Close every retained resource."""
        while self._items:
            item = self._items[-1]
            close = getattr(item, "close", None)
            if callable(close):
                close()
            if self._items and self._items[-1] is item:
                self._items.pop()


class NativeDirectoryManifestCarrier:
    """Own attached local, remote, or Parquet source manifests."""

    def close(self) -> None:
        """Close any attached manifest that owns staged resources."""
        for attribute in (
            "remote_native_multisource_manifest",
            "native_multisource_manifest",
            "native_parquet_multisource_manifest",
        ):
            manifest = getattr(self, attribute, None)
            close = getattr(manifest, "close", None)
            if callable(close):
                close()
            if manifest is not None:
                delattr(self, attribute)
