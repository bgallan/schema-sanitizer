"""Reusable cross-mode golden comparison helpers."""

from __future__ import annotations

import csv
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

_FIXED_CLOCK_SENTINEL = "<fixed-operation-clock>"
_GENERATED_JSON_COLUMNS = frozenset({"schema_registry", "schema_drifts"})


def _canonical_json_value(value: Any) -> Any:
    """Normalize JSON recursively without discarding generated metadata."""
    if isinstance(value, dict):
        return {key: _canonical_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_canonical_json_value(item) for item in value]
    return value


def canonical_json(raw: str | None, *, empty: Any) -> str:
    """Return canonical JSON while preserving every metadata value."""
    value = empty if raw is None or raw == "" else json.loads(raw)
    return json.dumps(
        _canonical_json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _normalize_value(value: Any) -> Any:
    """Return a deterministic recursively comparable logical value."""
    if isinstance(value, Mapping):
        return {key: _normalize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize_value(item) for item in value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return value


def normalize_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize generated metadata without weakening user-column comparison."""
    out: dict[str, Any] = {}
    for key, value in row.items():
        if key == "ingestion_timestamp":
            out[key] = _FIXED_CLOCK_SENTINEL
            continue
        if key in _GENERATED_JSON_COLUMNS and isinstance(value, str):
            out[key] = canonical_json(value, empty={} if key == "schema_registry" else [])
            continue
        out[key] = _normalize_value(value)
    return out


def normalized_rows(table: Any) -> list[dict[str, Any]]:
    """Return ordered normalized rows from an Arrow-compatible table."""
    return [normalize_row(row) for row in table.to_pylist()]


@dataclass(frozen=True)
class ResultGolden:
    """Stable logical result state shared by single and multi execution."""

    schema: Any
    rows: list[dict[str, Any]]
    registry_json: str
    drifts_json: str
    diagnostics: dict[str, Any]


def result_golden(result: Any) -> ResultGolden:
    """Capture schema, ordered rows, registry, drifts, and diagnostics."""
    table = result.clean_data
    schema = None if table is None else table.schema
    rows = [] if table is None else normalized_rows(table)
    return ResultGolden(
        schema=schema,
        rows=rows,
        registry_json=canonical_json(result.schema_registry_json, empty={}),
        drifts_json=canonical_json(result.schema_drifts_json, empty=[]),
        diagnostics=dict(result.stats),
    )


def assert_results_equivalent(single: Any, multi: Any) -> None:
    """Require complete logical equivalence between execution modes."""
    left = result_golden(single)
    right = result_golden(multi)
    if left.schema is None or right.schema is None:
        assert right.schema is left.schema
    else:
        assert right.schema.equals(left.schema, check_metadata=True)
    assert right.rows == left.rows
    assert right.registry_json == left.registry_json
    assert right.drifts_json == left.drifts_json
    assert right.diagnostics == left.diagnostics


def exception_golden(operation: Callable[[], Any]) -> tuple[type[BaseException], str]:
    """Capture the public exception type and message from one operation."""
    try:
        operation()
    except BaseException as exc:
        return type(exc), str(exc)
    raise AssertionError("operation unexpectedly succeeded")


def assert_exceptions_equivalent(single: Callable[[], Any], multi: Callable[[], Any]) -> None:
    """Require deterministic exception type and text across execution modes."""
    assert exception_golden(multi) == exception_golden(single)


def logical_file_rows(path: Path) -> tuple[tuple[str, ...], list[dict[str, Any]]]:
    """Read one supported output into ordered logical rows."""
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        import pyarrow.parquet as pq

        table = pq.read_table(path, use_threads=False)
        return tuple(table.schema.names), normalized_rows(table)
    if suffix in {".jsonl", ".ndjson"}:
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
        names = tuple(rows[0]) if rows else ()
        return names, [normalize_row(row) for row in rows]
    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as file_handle:
            reader = csv.DictReader(file_handle)
            rows = [normalize_row(row) for row in reader]
            return tuple(reader.fieldnames or ()), rows
    raise ValueError(f"unsupported golden output suffix: {suffix!r}")


def assert_logical_files_equivalent(single: Path, multi: Path) -> None:
    """Require identical ordered logical columns and rows in two output files."""
    assert logical_file_rows(multi) == logical_file_rows(single)
