"""Flat-prefix modified-time CSV to analytical Parquet example.

The package groups cloud and local workflow components without opening provider sessions
or processing files during import.
"""

from __future__ import annotations

from .local_validation import load_local_csv_directory_to_polars
from .runtime_support import (
    AdbcBigQueryWorkflowClient,
    Example08Config,
    Example08RunResult,
    NativeGcsWorkflowClient,
    run_modified_time_csv_workflow,
)

__all__ = [
    "AdbcBigQueryWorkflowClient",
    "Example08Config",
    "Example08RunResult",
    "NativeGcsWorkflowClient",
    "load_local_csv_directory_to_polars",
    "run_modified_time_csv_workflow",
]
