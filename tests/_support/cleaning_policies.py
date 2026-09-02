"""Define representative public inputs and execution helpers for cleaning-policy tests.

The utilities run identical policies through Python, JSON, and JSONL sources so callers can
compare normalized schemas and rows.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from conftest import read_test_json, read_test_jsonl, read_test_python

pa = pytest.importorskip("pyarrow")

from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.core_impl.schema_registry import merge_schema_registry
from schema_sanitizer.options_impl.call_options import normalize_call_options

INPUT_CASES = [
    "python_obj",
    "json_path",
    "json_path_auto",
    "jsonl_path",
    "jsonl_path_auto",
]


def table_signature(table: Any) -> tuple[str, list[dict[str, object]]]:
    """Return the stable schema-and-rows signature of a table."""
    return (
        table.schema.to_string(show_field_metadata=False, show_schema_metadata=False),
        table.to_pylist(),
    )


def nested_depth_rows() -> list[dict[str, object]]:
    """Return rows used to exercise nested-depth policies."""
    return [
        {"id": 1, "a": {"b": {"c": 3}}},
        {"id": 2, "a": {"b": {"c": 4}}},
    ]


def nested_contract_rows() -> list[dict[str, object]]:
    """Return rows containing one nested contract violation."""
    return [
        {"id": 1, "user": {"id": 10, "name": "a"}},
        {"id": 2, "user": {"id": "oops", "name": "b"}},
        {"id": 3, "user": {"id": 30, "name": "c"}},
    ]


def nested_contract_schema() -> Any:
    """Return the schema used by nested contract-policy tests."""
    return pa.schema(
        [
            ("id", pa.int64()),
            (
                "user",
                pa.struct(
                    [
                        ("id", pa.int64()),
                        ("name", pa.string()),
                    ]
                ),
            ),
        ]
    )


def field_names(struct_type: Any) -> list[str]:
    """Return field names from a PyArrow schema or struct type."""
    return [field.name for field in struct_type]


def versioned_scalar_registry(field_name: str, types: list[Any]) -> dict[str, object]:
    """Build a registry containing one scalar variant for each supplied type."""
    registry = None
    for typ in types:
        registry = merge_schema_registry(
            inferred_schema=pa.schema([pa.field(field_name, typ)]),
            schema_registry=registry,
            field_name_policy="lower_snake",
        ).schema_registry
    assert registry is not None
    return registry


def prepare_input(
    rows: list[dict[str, object]],
    case: str,
    tmp_path: Path | None,
) -> tuple[object, str]:
    """Prepare one supported in-memory or file-based input representation."""
    json_text = json.dumps(rows)
    jsonl_text = "\n".join(json.dumps(row) for row in rows) + "\n"

    if case == "python_obj":
        return rows, "python"
    if tmp_path is None:
        raise ValueError(f"tmp_path is required for input case: {case}")
    if case == "json_path":
        path = tmp_path / "rows.json"
        path.write_text(json_text, encoding="utf-8")
        return path, "json"
    if case == "json_path_auto":
        path = tmp_path / "rows.auto.json"
        path.write_text(json_text, encoding="utf-8")
        return path, "auto"
    if case == "jsonl_path":
        path = tmp_path / "rows.jsonl"
        path.write_text(jsonl_text, encoding="utf-8")
        return path, "jsonl"
    if case == "jsonl_path_auto":
        path = tmp_path / "rows.auto.jsonl"
        path.write_text(jsonl_text, encoding="utf-8")
        return path, "auto"
    raise ValueError(f"unsupported input case: {case}")


def read_result(
    rows: list[dict[str, object]],
    case: str,
    tmp_path: Path | None,
    options: dict[str, object] | None = None,
    **option_kwargs: object,
) -> Any:
    """Read prepared rows through the appropriate public test adapter."""
    data, fmt = prepare_input(rows, case, tmp_path)
    merged_options = {**(options or {}), **option_kwargs}
    schema_contract = merged_options.pop("schema_contract", None)
    if schema_contract is not None:
        return ExecutionContext().to_table(
            data,
            options=normalize_call_options(
                schema_contract=schema_contract,
                **merged_options,
            ),
            format=fmt,
            source="python" if fmt == "python" else "auto",
        )
    if fmt == "python":
        return read_test_python(data, output_format="pyarrow", **merged_options)
    if case.startswith("jsonl"):
        return read_test_jsonl(data, output_format="pyarrow", **merged_options)
    return read_test_json(data, output_format="pyarrow", **merged_options)
