"""Shared exact single/multi JSONL materialization harness."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.api_impl.file_conversion.writers import write_jsonl_native_first_stream
from schema_sanitizer.api_impl.stream_output import write_raw_stream_to_file
from schema_sanitizer.core_impl.schema_registry import schema_contract_from_registry_json
from schema_sanitizer.options_impl.call_options import normalize_call_options

DEFAULT_MEMORY_LIMIT = 64 * 1024 * 1024


def wide_column_names(prefix: str, count: int = 128) -> tuple[str, ...]:
    """Return deterministic identifier-safe column names."""
    return tuple(
        f"{prefix}{chr(ord('a') + index // 26)}{chr(ord('a') + index % 26)}"
        for index in range(count)
    )


def write_wide_integer_rows(path: Path, columns: tuple[str, ...], rows: int) -> None:
    """Write deterministic wide integer JSONL rows."""
    with path.open("w", encoding="utf-8") as handle:
        for row_index in range(rows):
            row = {name: row_index + column for column, name in enumerate(columns)}
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")


def strict_contract(
    source: Path,
    output: Path,
    *,
    memory_limit: int = DEFAULT_MEMORY_LIMIT,
) -> Any:
    """Build one frozen scalar contract through the single-thread oracle."""
    result = ss.to_jsonl(
        source,
        output,
        input_format="jsonl",
        parse_integers=True,
        field_name_policy="preserve",
        multi_threading=False,
        memory_limit_bytes=memory_limit,
    )
    contract = schema_contract_from_registry_json(result.schema_registry_json)
    assert contract is not None
    return contract


def consume_strict(
    source: Path,
    output: Path,
    *,
    mode: str,
    contract: Any,
    feature: str,
    memory_limit: int = DEFAULT_MEMORY_LIMIT,
):
    """Consume a strict contract through the native streaming surface."""
    options = normalize_call_options(
        schema_contract=contract,
        schema_mode="strict",
        on_error="stop",
        parse_integers=True,
        field_name_policy="preserve",
        multi_threading=mode == "multi",
        memory_limit_bytes=memory_limit,
    )
    context = ExecutionContext()
    sink = context.to_sink(source, sink="stream", options=options, format="jsonl", source="path")
    result = write_raw_stream_to_file(
        sink.raw,
        output,
        writer=write_jsonl_native_first_stream,
        feature=feature,
        first_row_columns=None,
        memory_limit_bytes=memory_limit,
        threading_mode=mode,
    )
    return result, context
