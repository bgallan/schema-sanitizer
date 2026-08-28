"""Regression coverage for concurrency string runs preserve exact single multi json escaping."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import schema_sanitizer as ss

pytestmark = pytest.mark.usefixtures("fixed_operation_clock")

_MEMORY_LIMIT = 128 * 1024 * 1024


def test_string_runs_preserve_exact_single_multi_json_escaping(
    tmp_path: Path,
    require_native: None,
) -> None:
    """Run appends preserve UTF-8 and every JSON control escape exactly."""
    values = (
        "ordinary-mañana-café-漢字-🙂-abcdefghijklmnopqrstuvwxyz",
        'quoted"value\\path',
        "line\nfeed\ttab\rreturn\bbackspace\fformfeed",
        "".join(chr(value) for value in range(0x20)),
    )
    source = tmp_path / "strings.jsonl"
    rows = [
        {f"field{column:03d}": values[(row + column) % len(values)] for column in range(32)}
        for row in range(2_048)
    ]
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
    assert b"ma\xc3\xb1ana-caf\xc3\xa9" in payload
    assert b'quoted\\"value\\\\path' in payload
    assert b"\\b" in payload
    assert b"\\f" in payload
    assert b"\\n" in payload
    assert b"\\r" in payload
    assert b"\\t" in payload
    assert b"\\u0000" in payload
    assert b"\\u001f" in payload

    decoded = [json.loads(line) for line in payload.decode("utf-8").splitlines()]
    projected = [{key: record[key] for key in rows[index]} for index, record in enumerate(decoded)]
    assert projected == rows


def test_token_writer_appends_safe_runs_instead_of_each_byte() -> None:
    """The canonical string writer copies safe spans and escapes only exceptions."""
    root = Path(__file__).resolve().parents[2]
    owner = (root / "cpp/src/internal/json_encoding/token_writer.cc").read_text(encoding="utf-8")

    assert "requires_json_escape" in owner
    assert "append_escaped_byte" in owner
    assert "run_start" in owner
    assert "out.append(value.data() + run_start" in owner
    assert "for (unsigned char byte : value)" not in owner
