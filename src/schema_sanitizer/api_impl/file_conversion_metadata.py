"""Generated metadata helpers for public file conversion APIs."""

from __future__ import annotations

import os

SCHEMA_REGISTRY_COLUMN = "schema_registry"
SCHEMA_DRIFTS_COLUMN = "schema_drifts"
SOURCE_FILE_COLUMN = "source_file"
INGESTION_TIMESTAMP_COLUMN = "ingestion_timestamp"
ETL_GENERATED_COLUMN_NAMES = (
    SCHEMA_REGISTRY_COLUMN,
    SCHEMA_DRIFTS_COLUMN,
    SOURCE_FILE_COLUMN,
    INGESTION_TIMESTAMP_COLUMN,
)


def generated_file_metadata_columns(
    input_path: str | os.PathLike[str],
) -> dict[str, str]:
    """Build generated per-file metadata columns."""
    from ..core_impl.native import _native
    from .shared import _call_core

    return _call_core(
        _native.file_metadata_columns,
        input_path,
    )
