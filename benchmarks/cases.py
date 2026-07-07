"""Benchmark case names grouped by workload type."""

from __future__ import annotations

READ_CASES = (
    "options",
    "options-default",
    "schema-support",
    "jsonl",
    "dirty-jsonl",
    "nested-jsonl",
    "json-folder",
    "json-folder-many",
    "xml-folder",
    "python-rows",
    "python-rows-nested",
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
)
ALL_CASES = ("all", *READ_CASES, *WRITE_CASES)
