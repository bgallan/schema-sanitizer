"""End-to-end contracts for ordered native materialization workers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")

from schema_sanitizer.api_impl.execution_context import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options


def _read_python(rows, *, threading_mode: str, **options):
    """Materialize Python rows with one explicit execution model."""
    return ExecutionContext().to_table(
        rows,
        options=normalize_call_options(threading_mode=threading_mode, **options),
        format="python",
        source="python",
    )


def _read_jsonl(path: Path, *, threading_mode: str, **options):
    """Materialize one JSONL path with one explicit execution model."""
    return ExecutionContext().to_table(
        path,
        options=normalize_call_options(threading_mode=threading_mode, **options),
        format="jsonl",
        source="path",
    )


def test_multi_materialization_preserves_rows_stats_and_batch_boundaries() -> None:
    """Ordered commit keeps data, diagnostics, and 65K row batches identical."""
    rows = [{"ordinal": index, "value": f"row-{index}"} for index in range(70_000)]

    single = _read_python(rows, threading_mode="single")
    multi = _read_python(rows, threading_mode="multi")

    assert multi.clean_data.equals(single.clean_data)
    assert [batch.num_rows for batch in single.clean_data.to_batches()] == [65_536, 4_464]
    assert [batch.num_rows for batch in multi.clean_data.to_batches()] == [65_536, 4_464]
    assert multi.stats == single.stats


@pytest.mark.parametrize("on_error", ["skip_row", "emit_null_row"])
def test_multi_materialization_preserves_error_policy_diagnostics(on_error: str) -> None:
    """Worker-local conversion counters merge in source order exactly once."""
    schema = pa.schema(
        [
            pa.field("ordinal", pa.int64(), nullable=False),
            pa.field("value", pa.int64(), nullable=False),
        ]
    )
    rows = [
        {"ordinal": 0, "value": 10},
        {"ordinal": 1, "value": "bad"},
        {"ordinal": 2, "value": 30},
        {"ordinal": 3, "value": "also-bad"},
        {"ordinal": 4, "value": 50},
    ]
    options = {
        "schema_contract": schema,
        "schema_mode": "strict",
        "on_error": on_error,
        "parse_integers": False,
    }

    single = _read_python(rows, threading_mode="single", **options)
    multi = _read_python(rows, threading_mode="multi", **options)

    assert multi.clean_data.equals(single.clean_data)
    assert multi.stats == single.stats
    if on_error == "skip_row":
        assert multi.clean_data.column("ordinal").to_pylist() == [0, 2, 4]
        assert multi.stats["skipped_rows"] == 2
    else:
        assert multi.clean_data.column("ordinal").to_pylist() == [0, None, 2, None, 4]
        assert multi.stats["skipped_rows"] == 0


def test_multi_materialization_reports_earliest_slow_source_failure() -> None:
    """A fast later failure cannot overtake a slower earlier failing row."""
    schema = pa.schema(
        [
            pa.field("ordinal", pa.int64(), nullable=False),
            pa.field("payload", pa.list_(pa.int64()), nullable=False),
            pa.field("value", pa.int64(), nullable=False),
        ]
    )
    rows = [{"ordinal": index, "payload": [index], "value": index} for index in range(24)]
    # This earlier row performs substantial valid list conversion before its
    # final field fails. The next row fails almost immediately for a different
    # reason, making physical completion order intentionally unfavorable.
    rows[3] = {
        "ordinal": 3,
        "payload": list(range(200_000)),
        "value": "earlier-coercion-error",
    }
    rows[4] = {"ordinal": 4, "payload": [4]}
    options = {
        "schema_contract": schema,
        "schema_mode": "strict",
        "on_error": "stop",
        "parse_integers": False,
        "memory_limit_bytes": 512 * 1024 * 1024,
    }

    messages: dict[str, str] = {}
    for mode in ("single", "multi"):
        with pytest.raises(Exception) as caught:
            _read_python(rows, threading_mode=mode, **options)
        messages[mode] = str(caught.value)

    assert messages["multi"] == messages["single"]
    assert "failed to coerce string to int64 for field 'value'" in messages["multi"]


def test_multi_raw_jsonl_materializer_preserves_source_order(tmp_path: Path) -> None:
    """Worker-local JSON parsers produce the same stable user columns."""
    path = tmp_path / "rows.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "ordinal": index,
                    "label": f"row-{index}",
                    "nested": {"items": [index, index + 1]},
                },
                separators=(",", ":"),
            )
            for index in range(8_000)
        ),
        encoding="utf-8",
    )

    from schema_sanitizer import to_pyarrow

    single = to_pyarrow(path, input_format="jsonl", threading_mode="single")
    multi = to_pyarrow(path, input_format="jsonl", threading_mode="multi")

    stable_columns = ["ordinal", "label", "nested", "source_file"]
    assert multi.clean_data.select(stable_columns).equals(single.clean_data.select(stable_columns))
    assert multi.clean_data.column("ordinal").to_pylist() == list(range(8_000))
    assert multi.stats == single.stats
    assert multi.execution_policy["effective_workers"] > 1


def test_multi_packet_accounting_isolates_large_nested_python_rows() -> None:
    """One expanded container cannot retain hundreds of neighbors in one result."""
    rows = [{"ordinal": index, "payload": [index, index + 1]} for index in range(520)]
    rows[257] = {"ordinal": 257, "payload": list(range(20_000))}

    single = _read_python(
        rows,
        threading_mode="single",
        memory_limit_bytes=256 * 1024 * 1024,
    )
    multi = _read_python(
        rows,
        threading_mode="multi",
        memory_limit_bytes=256 * 1024 * 1024,
    )

    assert multi.clean_data.equals(single.clean_data)
    assert multi.clean_data.column("ordinal").to_pylist() == list(range(520))
    assert multi.stats == single.stats


def test_multi_packet_accounting_isolates_large_raw_json_row(tmp_path: Path) -> None:
    """An oversized raw slice remains ordered between normal packets."""
    path = tmp_path / "oversized.jsonl"
    rows = [
        {"ordinal": 0, "payload": "before"},
        {"ordinal": 1, "payload": "x" * (1024 * 1024 + 17)},
        {"ordinal": 2, "payload": "after"},
    ]
    path.write_text(
        "\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n",
        encoding="utf-8",
    )

    from schema_sanitizer import to_pyarrow

    single = to_pyarrow(
        path,
        input_format="jsonl",
        threading_mode="single",
        memory_limit_bytes=256 * 1024 * 1024,
    )
    multi = to_pyarrow(
        path,
        input_format="jsonl",
        threading_mode="multi",
        memory_limit_bytes=256 * 1024 * 1024,
    )

    stable_columns = ["ordinal", "payload", "source_file"]
    assert multi.clean_data.select(stable_columns).equals(single.clean_data.select(stable_columns))
    assert multi.clean_data.column("ordinal").to_pylist() == [0, 1, 2]
    assert multi.stats == single.stats


def test_multi_raw_jsonl_direct_scalars_preserve_utf8_and_nulls(tmp_path: Path) -> None:
    """Borrowed UTF-8 views remain valid until their immediate Arrow append."""
    path = tmp_path / "direct-scalars.jsonl"
    rows = []
    for index in range(12_500):
        rows.append(
            {
                "ordinal": index,
                "enabled": index % 3 == 0,
                "ratio": index / 7.0,
                "label": None
                if index % 17 == 0
                else ("" if index % 19 == 0 else f'fila-{index}-ñ-\\"-\\n'),
            }
        )
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in rows)
        + "\n",
        encoding="utf-8",
    )

    schema = pa.schema(
        [
            pa.field("ordinal", pa.int64(), nullable=False),
            pa.field("enabled", pa.bool_(), nullable=False),
            pa.field("ratio", pa.float64(), nullable=False),
            pa.field("label", pa.string(), nullable=True),
        ]
    )
    options = {
        "schema_contract": schema,
        "schema_mode": "strict",
        "memory_limit_bytes": 256 * 1024 * 1024,
    }
    single = _read_jsonl(path, threading_mode="single", **options)
    multi = _read_jsonl(path, threading_mode="multi", **options)

    stable_columns = ["ordinal", "enabled", "ratio", "label"]
    assert multi.clean_data.select(stable_columns).equals(single.clean_data.select(stable_columns))
    assert multi.clean_data.column("label").to_pylist() == [row["label"] for row in rows]
    assert multi.stats == single.stats


@pytest.mark.parametrize("on_error", ["skip_row", "emit_null_row"])
def test_multi_raw_jsonl_direct_scalars_preserve_error_policy(
    tmp_path: Path, on_error: str
) -> None:
    """Direct scalar conversion preserves ordered error-policy semantics."""
    path = tmp_path / f"direct-errors-{on_error}.jsonl"
    rows = [
        {"ordinal": 0, "value": 10, "label": "ok-0"},
        {"ordinal": 1, "value": "bad", "label": "bad-1"},
        {"ordinal": 2, "value": 30, "label": "ok-2"},
        {"ordinal": 3, "value": "also-bad", "label": "bad-3"},
        {"ordinal": 4, "value": 50, "label": "ok-4"},
    ]
    path.write_text(
        "\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n",
        encoding="utf-8",
    )

    schema = pa.schema(
        [
            pa.field("ordinal", pa.int64(), nullable=False),
            pa.field("value", pa.int64(), nullable=False),
            pa.field("label", pa.string(), nullable=False),
        ]
    )
    options = {
        "schema_contract": schema,
        "schema_mode": "strict",
        "on_error": on_error,
        "parse_integers": False,
    }
    single = _read_jsonl(path, threading_mode="single", **options)
    multi = _read_jsonl(path, threading_mode="multi", **options)

    stable_columns = ["ordinal", "value", "label"]
    assert multi.clean_data.select(stable_columns).equals(single.clean_data.select(stable_columns))
    assert multi.stats == single.stats


@pytest.mark.parametrize("utf8_columns", [0, 8])
def test_wide_flat_adaptive_materialization_preserves_single_oracle(
    tmp_path: Path, utf8_columns: int
) -> None:
    """Wide fixed and UTF-8-heavy plans keep identical ordered results."""
    path = tmp_path / f"wide-{utf8_columns}.jsonl"
    rows: list[dict[str, object]] = []
    for row_index in range(4_096):
        row: dict[str, object] = {
            f"n{column:02d}": str(row_index + column) for column in range(24 - utf8_columns)
        }
        row.update(
            {
                f"s{column:02d}": f"value-{row_index % 257}-{column}"
                for column in range(utf8_columns)
            }
        )
        rows.append(row)
    path.write_text(
        "\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n",
        encoding="utf-8",
    )

    options = {
        "parse_integers": True,
        "memory_limit_bytes": 256 * 1024 * 1024,
    }
    single = _read_jsonl(path, threading_mode="single", **options)
    multi = _read_jsonl(path, threading_mode="multi", **options)

    assert multi.clean_data.equals(single.clean_data)
    assert multi.stats == single.stats
