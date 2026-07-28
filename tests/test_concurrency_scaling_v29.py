"""Regression coverage for the v29 integral-concurrency changes."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")

from threading_golden import assert_results_equivalent

import schema_sanitizer as ss

_FIXED_TIME_NS = 1_700_000_000_123_456_000
_FIXED_TIME = datetime(2023, 11, 14, 22, 13, 20, 123456)
_MEMORY_LIMIT = 256 * 1024 * 1024


@pytest.fixture(autouse=True)
def fixed_operation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep generated operation metadata identical across execution modes."""
    from schema_sanitizer.api_impl import operation_context

    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_TIME_NS)


def _jsonl_line(value: dict[str, object]) -> str:
    """Serialize one compact deterministic JSONL record."""
    return json.dumps(value, separators=(",", ":"))


def test_grouped_jsonl_prefetch_preserves_global_order_and_source_file(
    tmp_path: Path,
) -> None:
    """Asynchronous first-block prefetch must remain canonically ordered."""
    folder = tmp_path / "parts"
    folder.mkdir()
    row_counts = (0, 1, 511, 1_025, 2_049, 3_073, 4_097, 6_145)
    expected_sources: list[str] = []
    global_ordinal = 0

    for partition, row_count in enumerate(row_counts):
        source = folder / f"part-{partition:02d}.jsonl"
        lines = []
        for local_row in range(row_count):
            lines.append(
                _jsonl_line(
                    {
                        "ordinal": global_ordinal,
                        "partition": partition,
                        "local_row": local_row,
                        "a": global_ordinal % 7,
                        "b": f"value-{global_ordinal % 19}",
                        "c": global_ordinal % 2 == 0,
                        "d": None if global_ordinal % 11 == 0 else global_ordinal,
                        "e": global_ordinal / 10.0,
                    }
                )
            )
            expected_sources.append(str(source.resolve()))
            global_ordinal += 1
        source.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

    common = {
        "input_format": "jsonl",
        "input_mode": "directory",
        "memory_limit_bytes": _MEMORY_LIMIT,
    }
    single = ss.to_pyarrow(folder, threading_mode="single", **common)
    multi = ss.to_pyarrow(folder, threading_mode="multi", **common)

    assert_results_equivalent(single, multi)
    rows = multi.clean_data.to_pylist()
    assert [row["ordinal"] for row in rows] == list(range(global_ordinal))
    assert [row["source_file"] for row in rows] == expected_sources
    assert {row["ingestion_timestamp"] for row in rows} == {_FIXED_TIME}
    assert multi.stats["inferred_rows"] == global_ordinal
    assert multi.stats["materialized_rows"] == global_ordinal


def test_recycled_worker_buffers_preserve_wide_null_heavy_packets(
    tmp_path: Path,
) -> None:
    """Exact-block reuse must not leak values across wide Arrow packets."""
    source = tmp_path / "wide.jsonl"
    row_count = 24_576
    lines = []
    for ordinal in range(row_count):
        null_band = 8_192 <= ordinal < 16_384
        lines.append(
            _jsonl_line(
                {
                    "ordinal": ordinal,
                    "int_value": None if null_band else ordinal * 3,
                    "float_value": None if null_band else ordinal / 7.0,
                    "bool_value": None if null_band else ordinal % 2 == 0,
                    "text_value": None if null_band else (f"row-{ordinal:06d}-" + "x" * 48),
                    "extra_a": ordinal % 5,
                    "extra_b": f"bucket-{ordinal % 13}",
                    "extra_c": None if ordinal % 17 == 0 else ordinal % 23,
                    "extra_d": ordinal % 29,
                }
            )
        )
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")

    common = {
        "input_format": "jsonl",
        "memory_limit_bytes": _MEMORY_LIMIT,
    }
    single = ss.to_pyarrow(source, threading_mode="single", **common)
    multi_first = ss.to_pyarrow(source, threading_mode="multi", **common)
    multi_second = ss.to_pyarrow(source, threading_mode="multi", **common)

    assert_results_equivalent(single, multi_first)
    assert_results_equivalent(multi_first, multi_second)

    null_rows = multi_second.clean_data.slice(8_192, 8_192).to_pylist()
    for row in null_rows:
        assert row["intvalue"] is None
        assert row["floatvalue"] is None
        assert row["boolvalue"] is None
        assert row["textvalue"] is None
