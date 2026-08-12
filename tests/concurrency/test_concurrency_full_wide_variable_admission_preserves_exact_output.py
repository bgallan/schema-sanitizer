"""Regression coverage for concurrency full wide variable admission preserves exact output."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss

pytestmark = pytest.mark.usefixtures("fixed_operation_clock")

_MEMORY_LIMIT = 128 * 1024 * 1024


def test_full_wide_variable_admission_preserves_exact_output(
    tmp_path: Path,
) -> None:
    """Cross-batch output overlap preserves order, nulls, UTF-8, and escapes."""
    require_native()
    values: tuple[str | None, ...] = (
        "ordinary-mañana-café-漢字-🙂",
        'quoted"value\\path',
        "line\nfeed\ttab\rreturn",
        "\x00\x1f",
        None,
    )
    rows = [
        {f"field{column:03d}": values[(row + column) % len(values)] for column in range(32)}
        for row in range(3_072)
    ]
    source = tmp_path / "wide-variable.jsonl"
    source.write_text(
        "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )

    common = dict(
        input_format="jsonl",
        field_name_policy="preserve",
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    single = tmp_path / "single.jsonl"
    multi = tmp_path / "multi.jsonl"
    ss.to_jsonl(source, single, multi_threading=False, **common)
    ss.to_jsonl(source, multi, multi_threading=True, **common)

    payload = multi.read_bytes()
    assert payload == single.read_bytes()
    decoded = [json.loads(line) for line in payload.decode("utf-8").splitlines()]
    projected = [{key: record[key] for key in rows[index]} for index, record in enumerate(decoded)]
    assert projected == rows


def test_full_admission_is_limited_to_bounded_wide_variable_jsonl() -> None:
    """Only bounded variable-width packets bypass per-batch narrowing."""
    root = Path(__file__).resolve().parents[2]
    writer = (root / "cpp/src/internal/json_output/jsonl_stream_writer.cc").read_text(
        encoding="utf-8"
    )

    assert "admit_full_wide_variable_output" in writer
    assert "reclaim_wide_variable_packet_window" in writer
    assert "scale_wide_fixed_output || admit_full_wide_variable_output" in writer
    assert "wide_flat && !wide_fixed_flat" in writer
