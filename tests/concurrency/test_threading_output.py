"""Exercise ordered multi-worker JSONL and CSV text output.

Output must remain byte-identical, isolate oversized rows, avoid files or threads for invalid and
single-worker paths, promote after a small first batch, and reuse escaped member-name prefixes.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
from _support.threading_goldens import semantic_stats

from schema_sanitizer.adapters.pyarrow.csv_sink import write_csv_stream
from schema_sanitizer.adapters.pyarrow.jsonl_sink import write_jsonl_stream

_MEMORY_LIMIT = 256 * 1024 * 1024


def _reader(batches: list[object]) -> object:
    """Return a fresh reader over immutable record batches."""
    return pa.RecordBatchReader.from_batches(batches[0].schema, batches)


def _mixed_batches() -> list[object]:
    """Build multiple batches containing escaping, nulls, lists, and structs."""
    rows = 4_097
    schema = pa.schema(
        [
            pa.field("ordinal", pa.int64()),
            pa.field("text", pa.string()),
            pa.field("items", pa.list_(pa.int64())),
            pa.field(
                "payload",
                pa.struct(
                    [
                        pa.field("name", pa.string()),
                        pa.field("active", pa.bool_()),
                    ]
                ),
            ),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array(range(rows), type=pa.int64()),
            pa.array(
                [
                    None if index % 31 == 0 else f'row,{index} "quoted"\\slash\nline-{index % 7}'
                    for index in range(rows)
                ],
                type=pa.string(),
            ),
            pa.array(
                [None if index % 29 == 0 else [index, index + 1] for index in range(rows)],
                type=pa.list_(pa.int64()),
            ),
            pa.array(
                [
                    None if index % 37 == 0 else {"name": f"name-{index}", "active": index % 2 == 0}
                    for index in range(rows)
                ],
                type=schema.field("payload").type,
            ),
        ],
        schema=schema,
    )
    return table.to_batches(max_chunksize=1_137)


@pytest.mark.parametrize(
    ("suffix", "writer"),
    [("jsonl", write_jsonl_stream), ("csv", write_csv_stream)],
)
def test_text_output_multi_is_byte_identical_and_ordered(
    tmp_path: Path,
    suffix: str,
    writer: object,
) -> None:
    """Parallel encoding must preserve every byte, row, and batch statistic."""
    batches = _mixed_batches()
    paths = {mode: tmp_path / f"{mode}.{suffix}" for mode in ("single", "multi")}
    stats = {}
    for mode, path in paths.items():
        stats[mode] = writer(
            _reader(batches),
            path,
            feature="threaded output contract",
            memory_limit_bytes=_MEMORY_LIMIT,
            threading_mode=mode,
        )

    assert paths["multi"].read_bytes() == paths["single"].read_bytes()
    assert semantic_stats(stats["multi"]) == semantic_stats(stats["single"])
    assert stats["multi"]["materialized_rows"] == 4_097
    assert stats["multi"]["batches"] == len(batches)


@pytest.mark.parametrize(
    ("suffix", "writer"),
    [("jsonl", write_jsonl_stream), ("csv", write_csv_stream)],
)
def test_text_output_isolates_oversized_rows(
    tmp_path: Path,
    suffix: str,
    writer: object,
) -> None:
    """A row larger than the packet target remains bounded and correctly ordered."""
    large = "x" * (2 * 1024 * 1024 + 17)
    batch = pa.record_batch(
        {
            "ordinal": pa.array([0, 1, 2]),
            "payload": pa.array(["before", large, "after"]),
        }
    )
    single = tmp_path / f"large-single.{suffix}"
    multi = tmp_path / f"large-multi.{suffix}"

    writer(
        _reader([batch]),
        single,
        feature="oversized output contract",
        memory_limit_bytes=_MEMORY_LIMIT,
        threading_mode="single",
    )
    writer(
        _reader([batch]),
        multi,
        feature="oversized output contract",
        memory_limit_bytes=_MEMORY_LIMIT,
        threading_mode="multi",
    )

    assert multi.read_bytes() == single.read_bytes()
    assert multi.stat().st_size > len(large)


@pytest.mark.parametrize(
    ("suffix", "writer"),
    [("jsonl", write_jsonl_stream), ("csv", write_csv_stream)],
)
def test_invalid_output_threading_mode_does_not_create_file(
    tmp_path: Path,
    suffix: str,
    writer: object,
) -> None:
    """Validation happens before opening or truncating the destination."""
    batch = pa.record_batch({"value": pa.array([1])})
    path = tmp_path / f"invalid.{suffix}"

    with pytest.raises(ValueError, match="threading_mode"):
        writer(
            _reader([batch]),
            path,
            feature="invalid output mode contract",
            threading_mode="invalid",
        )

    assert not path.exists()


def test_single_text_output_leaves_native_thread_ledger_empty(tmp_path: Path) -> None:
    """The single output route retires every native arena and worker permit."""
    from schema_sanitizer.core_impl.runtime_diagnostics import _native_arena_snapshot

    batch = pa.record_batch(
        {
            "ordinal": pa.array(range(20_000)),
            "text": pa.array([f"value-{index}" for index in range(20_000)]),
        }
    )
    write_jsonl_stream(
        _reader([batch]),
        tmp_path / "single.jsonl",
        feature="single output thread contract",
        memory_limit_bytes=_MEMORY_LIMIT,
        threading_mode="single",
    )

    snapshot = _native_arena_snapshot()
    assert snapshot["live_arenas"] == 0
    assert snapshot["detached_workers"] == 0
    assert snapshot["native_physical_threads"] == 0


@pytest.mark.parametrize(
    ("suffix", "writer"),
    [("jsonl", write_jsonl_stream), ("csv", write_csv_stream)],
)
def test_text_output_promotes_after_small_first_batch(
    tmp_path: Path,
    suffix: str,
    writer: object,
) -> None:
    """A tiny first batch may stay inline without trapping later work there."""
    schema = pa.schema([pa.field("ordinal", pa.int64()), pa.field("text", pa.string())])
    first = pa.record_batch(
        [pa.array([0], type=pa.int64()), pa.array(["first"], type=pa.string())],
        schema=schema,
    )
    rows = 12_000
    second = pa.record_batch(
        [
            pa.array(range(1, rows + 1), type=pa.int64()),
            pa.array(
                [f'row-{index}, "quoted"\\line\n{index % 11}' for index in range(rows)],
                type=pa.string(),
            ),
        ],
        schema=schema,
    )
    paths = {mode: tmp_path / f"promote-{mode}.{suffix}" for mode in ("single", "multi")}
    stats = {}
    for mode, path in paths.items():
        stats[mode] = writer(
            _reader([first, second]),
            path,
            feature="adaptive output promotion contract",
            memory_limit_bytes=_MEMORY_LIMIT,
            threading_mode=mode,
        )

    assert paths["multi"].read_bytes() == paths["single"].read_bytes()
    assert semantic_stats(stats["multi"]) == semantic_stats(stats["single"])
    assert stats["multi"]["batches"] == 2
    assert stats["multi"]["materialized_rows"] == rows + 1


def test_jsonl_cached_member_prefixes_preserve_escaped_names(
    tmp_path: Path,
) -> None:
    """Precompiled root and nested member prefixes preserve exact JSON names."""
    nested_type = pa.struct(
        [
            pa.field('quote"name', pa.string()),
            pa.field("slash\\name", pa.int64()),
            pa.field("line\nname", pa.bool_()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array(["value"]),
            pa.array(
                [
                    {
                        'quote"name': 'quoted"value',
                        "slash\\name": 7,
                        "line\nname": True,
                    }
                ],
                type=nested_type,
            ),
        ],
        names=['root"\\\nname', "payload"],
    )
    outputs = {mode: tmp_path / f"escaped-names-{mode}.jsonl" for mode in ("single", "multi")}
    for mode, path in outputs.items():
        write_jsonl_stream(
            _reader([batch]),
            path,
            feature="cached JSON member prefix contract",
            memory_limit_bytes=_MEMORY_LIMIT,
            threading_mode=mode,
        )

    assert outputs["multi"].read_bytes() == outputs["single"].read_bytes()
    assert __import__("json").loads(outputs["multi"].read_text(encoding="utf-8")) == {
        'root"\\\nname': "value",
        "payload": {
            'quote"name': 'quoted"value',
            "slash\\name": 7,
            "line\nname": True,
        },
    }
