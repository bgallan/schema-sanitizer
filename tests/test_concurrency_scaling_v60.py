"""Regression coverage for v60 pair-digit JSONL integer formatting."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.api_impl import operation_context

_FIXED_TIME_NS = 1_700_000_000_123_456_000
_MEMORY_LIMIT = 128 * 1024 * 1024


@pytest.fixture(autouse=True)
def fixed_operation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep generated registry metadata identical across execution modes."""
    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_TIME_NS)


def test_v60_integer_boundaries_preserve_exact_single_multi_output(
    tmp_path: Path,
) -> None:
    """Pair-digit formatting preserves signs and exact int64 boundaries."""
    require_native()
    boundaries = (-(2**63), -101, -100, -99, -10, -9, -1, 0, 1, 9, 10, 99, 100, 101, 2**63 - 1)
    rows: list[dict[str, int]] = []
    for row in range(4_096):
        record = {
            f"field{column:03d}": boundaries[(row + column) % len(boundaries)]
            for column in range(128)
        }
        rows.append(record)
    source = tmp_path / "integer-boundaries.jsonl"
    source.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )

    common = dict(
        input_format="jsonl",
        parse_integers=True,
        field_name_policy="preserve",
        memory_limit_bytes=_MEMORY_LIMIT,
    )
    single = tmp_path / "single.jsonl"
    multi = tmp_path / "multi.jsonl"
    ss.to_jsonl(source, single, threading_mode="single", **common)
    ss.to_jsonl(source, multi, threading_mode="multi", **common)

    payload = multi.read_bytes()
    assert payload == single.read_bytes()
    assert b"-9223372036854775808" in payload
    assert b"9223372036854775807" in payload


def test_v60_integer_writer_uses_one_canonical_pair_digit_formatter() -> None:
    """All integer widths share the overflow-safe pair-digit implementation."""
    root = Path(__file__).resolve().parents[1]
    owner = (root / "cpp/src/internal/json_output/jsonl_value_writer_integer.cc").read_text(
        encoding="utf-8"
    )

    assert "kDecimalDigitPairs" in owner
    assert "append_unsigned_decimal" in owner
    assert "std::to_chars" not in owner
    assert "static_cast<uint64_t>(-(value + 1)) + 1" in owner
    assert owner.count("append_unsigned_decimal(") == 4
