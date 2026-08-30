"""Compare native Parquet writing across single- and multi-worker modes.

Wide scalar and nested columns must remain logically equivalent after round-trip, while strict
single mode must not create any host worker thread.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.usefixtures("require_native")


def _reader(table: object, *, max_chunksize: int = 4096) -> object:
    """Return a fresh multi-batch Arrow reader for one table."""
    pa = pytest.importorskip("pyarrow")
    batches = table.to_batches(max_chunksize=max_chunksize)
    return pa.RecordBatchReader.from_batches(table.schema, batches)


def _write_native(table: object, path: Path, *, mode: str, compression: str) -> None:
    """Write one table through the native-first Parquet path."""
    from schema_sanitizer.api_impl.file_conversion import writers

    writers.write_parquet_native_first_stream(
        _reader(table),
        path,
        feature="threading Parquet output contract",
        parquet_compression=compression,
        memory_limit_bytes=256 << 20,
        threading_mode=mode,
    )


def _assert_logically_equal(left: Path, right: Path) -> None:
    """Require identical schema, rows, order, and values across Parquet files."""
    pq = pytest.importorskip("pyarrow.parquet")
    # Keep third-party Arrow worker pools out of the native-writer TSan gate.
    # This test validates Schema-Sanitizer concurrency; PyArrow's reader is only
    # the logical oracle and must stay single-threaded here.
    left_table = pq.read_table(left, use_threads=False)
    right_table = pq.read_table(right, use_threads=False)
    assert left_table.schema == right_table.schema
    assert left_table.equals(right_table)
    assert left_table.to_pylist() == right_table.to_pylist()


@pytest.mark.parametrize("compression", ["uncompressed", "snappy", "gzip"])
def test_native_parquet_multi_matches_single_for_wide_scalars(
    tmp_path: Path,
    compression: str,
) -> None:
    """Wide scalar output must remain logically identical under column workers."""
    pa = pytest.importorskip("pyarrow")
    rows = 12_000
    columns: dict[str, object] = {
        "ordinal": pa.array(range(rows), type=pa.int64()),
        "label": pa.array([f'row-{index},"quoted"\\tail' for index in range(rows)]),
    }
    for column in range(14):
        columns[f"metric_{column}"] = pa.array(
            [index * (column + 1) for index in range(rows)],
            type=pa.int64(),
        )
    table = pa.table(columns)
    single = tmp_path / f"wide-single-{compression}.parquet"
    multi = tmp_path / f"wide-multi-{compression}.parquet"

    try:
        _write_native(table, single, mode="single", compression=compression)
        _write_native(table, multi, mode="multi", compression=compression)
    except RuntimeError as exc:
        if compression == "gzip" and "zlib is not available" in str(exc):
            pytest.skip("native writer was built without zlib")
        raise

    _assert_logically_equal(single, multi)


def test_native_parquet_multi_matches_single_for_nested_columns(tmp_path: Path) -> None:
    """Nested lists and structs retain null semantics and row order."""
    pa = pytest.importorskip("pyarrow")
    rows = 8_000
    payload_type = pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("score", pa.int64()),
            pa.field("active", pa.bool_()),
        ]
    )
    table = pa.table(
        {
            "ordinal": pa.array(range(rows), type=pa.int64()),
            "values": pa.array(
                [
                    None if index % 17 == 0 else [index, index + 1, index + 2]
                    for index in range(rows)
                ]
            ),
            "payload": pa.array(
                [
                    None
                    if index % 19 == 0
                    else {
                        "name": f"nested-{index}",
                        "score": index % 101,
                        "active": index % 2 == 0,
                    }
                    for index in range(rows)
                ],
                type=payload_type,
            ),
        }
    )
    single = tmp_path / "nested-single.parquet"
    multi = tmp_path / "nested-multi.parquet"

    _write_native(table, single, mode="single", compression="snappy")
    _write_native(table, multi, mode="multi", compression="snappy")

    _assert_logically_equal(single, multi)


def test_native_parquet_single_leaves_native_thread_ledger_empty(tmp_path: Path) -> None:
    """The Parquet single path leaves no native arena or worker ownership."""
    from schema_sanitizer.core_impl.runtime_diagnostics import _native_arena_snapshot

    pa = pytest.importorskip("pyarrow")
    rows = 16_000
    table = pa.table(
        {
            f"column_{column}": pa.array(
                [index * (column + 1) for index in range(rows)],
                type=pa.int64(),
            )
            for column in range(12)
        }
    )
    _write_native(
        table,
        tmp_path / "single-thread-reference.parquet",
        mode="single",
        compression="snappy",
    )

    snapshot = _native_arena_snapshot()
    assert snapshot["live_arenas"] == 0
    assert snapshot["detached_workers"] == 0
    assert snapshot["native_physical_threads"] == 0
