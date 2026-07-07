"""Prepare non-recursive Parquet directory inputs for native Arrow ingestion."""

from __future__ import annotations

import os

from .folder_listing import FolderFile, folder_files


class ParquetDirectoryInput:
    """Carry deterministic Parquet directory children for lazy native ingestion."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        memory_limit_bytes: int | None,
    ):
        """List Parquet children without opening file-level Arrow factories."""
        del memory_limit_bytes
        self._files = folder_files(
            path,
            suffix=(".parquet", ".pq"),
            reader_name="parquet directory input",
        )

    @property
    def files(self) -> list[FolderFile]:
        """Return direct Parquet child descriptors."""
        return self._files

    def close(self) -> None:
        """Satisfy PreparedPublicInput keepalive cleanup."""
