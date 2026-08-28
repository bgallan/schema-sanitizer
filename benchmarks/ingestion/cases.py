"""Benchmark case names grouped by workload type.

The constants provide the canonical read, support, and write case groups consumed by the
ingestion command.
"""

from __future__ import annotations

READ_CASES = (
    "options",
    "options-default",
    "schema-support",
    "jsonl",
    "dirty-jsonl",
    "nested-jsonl",
    "wide-jsonl",
    "deep-jsonl",
    "all-null-jsonl",
    "empty-container-jsonl",
    "json-folder",
    "json-folder-many",
    "xml-folder",
    "csv",
)
WRITE_CASES = (
    "write-jsonl",
    "write-csv",
    "parquet-jsonl",
    "parquet-jsonl-direct",
    "parquet-jsonl-wide",
    "registry-parquet-jsonl",
)
FALLBACK_CASES = (
    "dirty-jsonl",
    "nested-jsonl",
    "wide-jsonl",
    "deep-jsonl",
    "all-null-jsonl",
    "empty-container-jsonl",
)
ALL_CASES = ("all", *READ_CASES, *WRITE_CASES)
