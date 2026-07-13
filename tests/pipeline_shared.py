"""Tests for reusable partition pipeline helpers."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from schema_sanitizer.pipeline import (
    HiveRangeConfig,
    PartitionRunPlan,
    SchemaRegistryState,
    build_hive_range_plan,
    compact_stats_for_log,
    diff_flat_schema_paths,
    discover_existing_source_plans,
    format_duration,
    infer_warm_up_schema_registry,
    infer_warm_up_schema_registry_json,
    infer_warm_up_schema_registry_state,
    parse_final_schema_registry,
    read_parquet_schema,
    run_partitioned_to_parquet,
    run_partitioned_to_parquet_registry_json,
    run_partitioned_to_parquet_registry_state,
)


def _write_warm_up_source(
    tmp_path: Path,
    input_format: str,
    input_mode: str,
    name: str,
    field_name: str,
) -> Path:
    """Write one warm-up source for a public input format/mode pair."""
    folder = tmp_path / f"{input_format}-{input_mode}-{name}"
    if input_mode == "directory":
        folder.mkdir()
        path = folder / f"part.{_input_suffix(input_format)}"
    else:
        path = folder.with_suffix(f".{_input_suffix(input_format)}")

    if input_format == "csv":
        path.write_text(
            f"alpha,beta\n{1 if field_name == 'alpha' else ''},{2 if field_name == 'beta' else ''}\n",
            encoding="utf-8",
        )
    elif input_format == "json":
        path.write_text(f'{{"{field_name}": 1}}', encoding="utf-8")
    elif input_format == "json_array":
        path.write_text(f'[{{"{field_name}": 1}}]', encoding="utf-8")
    elif input_format in {"jsonl", "ndjson"}:
        path.write_text(f'{{"{field_name}": 1}}\n', encoding="utf-8")
    elif input_format == "xml":
        path.write_text(f"<row><{field_name}>1</{field_name}></row>", encoding="utf-8")
    elif input_format == "parquet":
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        table = pa.table({field_name: [1]})
        pq.write_table(table, path)
    else:  # pragma: no cover - exhaustive guard for future parametrization changes
        raise AssertionError(f"Unhandled input_format={input_format!r}")
    return path if input_mode == "single_file" else folder


def _input_suffix(input_format: str) -> str:
    """Return the public suffix for one input format."""
    if input_format == "json_array":
        return "json"
    if input_format == "parquet":
        return "parquet"
    return input_format


__all__ = [
    "HiveRangeConfig",
    "PartitionRunPlan",
    "SchemaRegistryState",
    "SimpleNamespace",
    "_input_suffix",
    "_write_warm_up_source",
    "build_hive_range_plan",
    "compact_stats_for_log",
    "date",
    "diff_flat_schema_paths",
    "discover_existing_source_plans",
    "format_duration",
    "infer_warm_up_schema_registry",
    "infer_warm_up_schema_registry_json",
    "infer_warm_up_schema_registry_state",
    "json",
    "logging",
    "parse_final_schema_registry",
    "pytest",
    "read_parquet_schema",
    "run_partitioned_to_parquet",
    "run_partitioned_to_parquet_registry_json",
    "run_partitioned_to_parquet_registry_state",
]
