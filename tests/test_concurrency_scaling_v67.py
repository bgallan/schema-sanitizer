"""Regression coverage for v67 direct scalar CSV output and packet sizing."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import require_native
from threading_golden import semantic_stats

ROOT = Path(__file__).resolve().parents[1]
CSV_WRITER = ROOT / "cpp/src/internal/csv/csv_stream_writer.cc"
ESTIMATOR = ROOT / "cpp/src/internal/output/text_output_estimator.hh"


def test_v67_csv_direct_path_avoids_json_round_trip_for_scalars_and_strings() -> None:
    """Primitive CSV cells bypass temporary JSON serialization and decoding."""
    source = CSV_WRITER.read_text(encoding="utf-8")

    assert "is_direct_csv_scalar" in source
    assert "return jsonl::append_value(out, field, array, row);" in source
    assert "append_csv_string<std::int32_t>" in source
    assert "append_csv_string<std::int64_t>" in source
    assert "array_is_null(array, row)" in source
    assert "append_csv_cell_from_json" in source  # generic fallback retained
    assert "getenv" not in source
    assert len(source.splitlines()) <= 500


def test_v67_csv_estimator_uses_csv_cells_and_keeps_interleave_margin() -> None:
    """Packet estimates omit JSON object keys while retaining a strict bound."""
    source = ESTIMATOR.read_text(encoding="utf-8")

    assert "estimate_csv_cell_bytes" in source
    assert "direct_csv_scalar_kind" in source
    assert "multiply_capped(raw, 2, cap)" in source
    assert "array.offset + row" in source
    assert "return multiply_capped(total, 2, cap);" in source
    assert "estimate_jsonl_row_bytes(root, array, row, cap)" in source
    assert len(source.splitlines()) <= 500


def test_v67_csv_single_multi_and_legacy_bytes_are_identical(tmp_path: Path) -> None:
    """Direct scalar/string rendering preserves quoting, nulls, and row order."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.pyarrow.csv_sink import write_csv_stream

    rows = 8_193
    table = pa.table(
        {
            "ordinal": pa.array(range(rows), type=pa.int64()),
            "signed": pa.array((index - 4_000 for index in range(rows)), type=pa.int64()),
            "flag": pa.array((index % 3 == 0 for index in range(rows)), type=pa.bool_()),
            "ratio": pa.array(((index - 777) / 13.0 for index in range(rows)), type=pa.float64()),
            "text": pa.array(
                [
                    None
                    if index % 17 == 0
                    else (
                        f'row,{index} "quoted"\\slash\nline-{index % 7}'
                        if index % 5 == 0
                        else f"plain-{index}-ümlaut-😀"
                    )
                    for index in range(rows)
                ],
                type=pa.string(),
            ),
        }
    )
    batches = table.to_batches(max_chunksize=1_137)

    outputs: dict[str, bytes] = {}
    stats = {}
    for mode in ("single", "multi"):
        reader = pa.RecordBatchReader.from_batches(table.schema, batches)
        path = tmp_path / f"{mode}.csv"
        stats[mode] = write_csv_stream(
            reader,
            path,
            feature="v67 direct CSV parity",
            memory_limit_bytes=64 << 20,
            threading_mode=mode,
        )
        outputs[mode] = path.read_bytes()

    assert outputs["single"] == outputs["multi"]
    assert semantic_stats(stats["single"]) == semantic_stats(stats["multi"])
    assert stats["multi"]["materialized_rows"] == rows
    assert b'"row,5 ""quoted""\\slash\nline-5"' in outputs["multi"]
    assert (
        outputs["multi"].splitlines()[1].endswith(b",")
    )  # null final string remains an empty CSV cell
