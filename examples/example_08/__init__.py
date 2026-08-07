"""Flat-prefix modified-time CSV to analytical Parquet example."""

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
