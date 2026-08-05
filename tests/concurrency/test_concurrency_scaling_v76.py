"""Regression coverage for v76 pure-Python input concurrency."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import default_pool
from schema_sanitizer.core_impl.concurrency_coverage import (
    INPUT_CONCURRENCY_COVERAGE,
    OUTPUT_CONCURRENCY_COVERAGE,
    concurrency_guarantees,
)
from schema_sanitizer.core_impl.execution import (
    PythonRowsJsonlByteReader,
)
from schema_sanitizer.core_impl.native_runtime import native_core
from schema_sanitizer.core_impl.python_rows import last_python_rows_route
from schema_sanitizer.input_impl.selection import resolve_source_and_format

_GENERATED_COLUMNS = {
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
}


def _rows(count: int) -> Iterator[dict[str, object]]:
    """Yield deterministic Python rows without materializing them."""
    for index in range(count):
        yield {
            "ordinal": index,
            "value": index * 3,
            "text": f'row-{index}-"quoted",value',
            "nested": {"parity": index & 1},
        }


def _csv_user_rows(path: Path) -> list[dict[str, str]]:
    """Read user-visible CSV values without invocation metadata."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [
            {key: value for key, value in row.items() if key not in _GENERATED_COLUMNS}
            for row in csv.DictReader(handle)
        ]


def _jsonl_user_rows(path: Path) -> list[dict[str, object]]:
    """Read user-visible JSONL values without invocation metadata."""
    rows: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        rows.append({key: value for key, value in row.items() if key not in _GENERATED_COLUMNS})
    return rows


def _materialization_tasks() -> int:
    """Return materialization submissions from the latest public operation."""
    stats = default_pool().get().performance_stats()
    return int(stats.get("tasks", {}).get("materialization", {}).get("submitted", 0))


def test_v76_python_is_a_first_class_concurrency_input() -> None:
    """Pure-Python rows declare honest parallel work and their GIL boundary."""
    guarantees = concurrency_guarantees()
    assert len(INPUT_CONCURRENCY_COVERAGE) == 8
    assert len(OUTPUT_CONCURRENCY_COVERAGE) == 7
    assert INPUT_CONCURRENCY_COVERAGE["python"] == (
        "native_iterator_batching",
        "single_encode_progressive_replay",
        "source_prefetch",
        "inference",
        "materialization",
    )
    assert guarantees["inputs"]["python"]["serial_boundaries"] == (
        "gil_bound_python_object_iteration",
        "ordered_replay_spool",
    )
    assert guarantees["inputs"]["python"]["benefit_proof"] == (
        "single_encode_progressive_replay_plus_parallel_pipeline_runtime"
    )


def test_v76_auto_source_accepts_sequences_and_one_shot_iterables() -> None:
    """Tuples and generators resolve to the public Python-row route."""
    tuple_rows = ({"a": 1}, {"a": 2})
    data, source, fmt = resolve_source_and_format(tuple_rows, format="auto", source="auto")
    assert data is tuple_rows
    assert source == "python"
    assert fmt == "python"

    generator = ({"a": index} for index in range(3))
    data, source, fmt = resolve_source_and_format(generator, format="auto", source="auto")
    assert data is generator
    assert source == "python"
    assert fmt == "python"


def test_v76_native_iterator_encoder_batches_without_retaining_rows() -> None:
    """One ABI call consumes a bounded iterator chunk and reports exact progress."""
    require_native()
    payload, next_index, exhausted = native_core.python_iter_rows_jsonl_bytes(
        iter({"a": index} for index in range(10_000)),
        0,
        1 << 30,
        4_096,
    )
    assert next_index == 4_096
    assert exhausted is False
    assert payload.count(b"\n") == 4_096


def test_v76_generator_reader_batches_replays_and_preserves_ordinal_errors() -> None:
    """One-shot rows are batched, replayed once, and validated by source ordinal."""
    require_native()
    yielded = 0

    def source() -> Iterator[dict[str, int]]:
        """Track the one and only traversal of the source generator."""
        nonlocal yielded
        for index in range(5_000):
            yielded += 1
            yield {"a": index}

    reader = PythonRowsJsonlByteReader(source(), memory_limit_bytes=32 << 20)
    first = reader.read(1 << 20)
    assert first.count(b"\n") > 1
    assert last_python_rows_route() == "native_iterator_batch"
    while reader.read(1 << 20):
        pass
    reader.seek(0)
    replay = reader.read(len(first))
    reader.close()
    assert replay == first
    assert yielded == 5_000

    invalid = PythonRowsJsonlByteReader(iter([{"a": 1}, {"a": 2}, 3]))
    with pytest.raises(TypeError, match="row 2 is not a dict"):
        invalid.read(1 << 20)
    invalid.close()


@pytest.mark.parametrize("mode", ["single", "multi"])
def test_v76_public_csv_accepts_generator_and_auto_detects_python(
    tmp_path: Path, mode: str
) -> None:
    """The public file API consumes a generator without a path-only facade."""
    require_native()
    output = tmp_path / f"rows-{mode}.csv"
    result = ss.to_csv(
        _rows(8_000),
        output,
        multi_threading=mode == "multi",
        memory_limit_bytes=64 << 20,
    )
    assert result.stats["materialized_rows"] == 8_000
    assert len(_csv_user_rows(output)) == 8_000
    if mode == "single":
        assert _materialization_tasks() == 0
    else:
        assert _materialization_tasks() > 0


def test_v76_python_generator_has_exact_single_multi_values(tmp_path: Path) -> None:
    """Pure-Python generators preserve order and values in both execution models."""
    require_native()
    outputs: dict[str, Path] = {}
    for mode in ("single", "multi"):
        output = tmp_path / f"rows-{mode}.jsonl"
        outputs[mode] = output
        ss.to_jsonl(
            _rows(4_000),
            output,
            input_format="python",
            multi_threading=mode == "multi",
            memory_limit_bytes=64 << 20,
        )
    assert _jsonl_user_rows(outputs["single"]) == _jsonl_user_rows(outputs["multi"])


def test_v76_python_input_reaches_every_native_file_output(tmp_path: Path) -> None:
    """CSV, JSONL, and Parquet outputs accept the same Python generator route."""
    require_native()
    csv_path = tmp_path / "rows.csv"
    jsonl_path = tmp_path / "rows.jsonl"
    parquet_path = tmp_path / "rows.parquet"

    ss.to_csv(_rows(2_000), csv_path, input_format="python", multi_threading=True)
    ss.to_jsonl(_rows(2_000), jsonl_path, input_format="python", multi_threading=True)
    parquet_result = ss.to_parquet(
        _rows(2_000),
        parquet_path,
        input_format="python",
        multi_threading=True,
        parquet_compression="snappy",
    )
    assert parquet_result.stats["materialized_rows"] == 2_000
    footer = json.loads(native_core.parquet_footer_info_json(str(parquet_path)))
    assert footer["native_reader_ready"] == 1
    assert footer["num_rows"] == 2_000
    assert len(_csv_user_rows(csv_path)) == 2_000
    assert len(_jsonl_user_rows(jsonl_path)) == 2_000


@pytest.mark.parametrize("width", [16, 32])
def test_python_generator_file_outputs_do_not_deadlock_at_high_width(
    tmp_path: Path,
    width: int,
) -> None:
    """Python callbacks can acquire the GIL while native writers await the arena."""
    require_native()
    if not hasattr(os, "sched_getaffinity") or not hasattr(os, "sched_setaffinity"):
        pytest.skip("exact CPU affinity is unavailable")
    available = sorted(os.sched_getaffinity(0))
    if len(available) < width:
        pytest.skip(f"requires at least {width} visible CPUs")
    output_dir = tmp_path / f"width-{width}"
    output_dir.mkdir()
    child = """
import os
import sys
from pathlib import Path

os.sched_setaffinity(0, {int(cpu) for cpu in sys.argv[1].split(",")})

import schema_sanitizer as ss

root = Path(sys.argv[2])

def rows():
    for ordinal in range(2_000):
        yield {"ordinal": ordinal, "value": ordinal * 3, "text": f"row-{ordinal}"}

ss.to_csv(rows(), root / "rows.csv", input_format="python", multi_threading=True)
ss.to_jsonl(rows(), root / "rows.jsonl", input_format="python", multi_threading=True)
ss.to_parquet(
    rows(),
    root / "rows.parquet",
    input_format="python",
    multi_threading=True,
    parquet_compression="snappy",
)
assert all(path.stat().st_size > 0 for path in root.iterdir())
"""
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            child,
            ",".join(str(cpu) for cpu in available[:width]),
            str(output_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    ("converter_name", "target"),
    [
        ("to_pyarrow", "pyarrow"),
        ("to_pandas", "pandas"),
        ("to_polars", "polars"),
        ("to_duckdb", "duckdb"),
    ],
)
def test_v76_python_input_reaches_every_analytical_output_without_adapter_import(
    monkeypatch: pytest.MonkeyPatch,
    converter_name: str,
    target: str,
) -> None:
    """Each analytical API opens the same native Python-row stream before its adapter."""
    require_native()
    from schema_sanitizer.api_impl import analytical

    seen: list[tuple[str, str]] = []

    def materialize(opened: object, *, target: str, threading_mode: str) -> object:
        """Record the adapter boundary and close the already-built native stream."""
        seen.append((target, threading_mode))
        opened.close()  # type: ignore[attr-defined]
        return SimpleNamespace(clean_data=target, execution_policy=None)

    monkeypatch.setattr(analytical, "materialize_opened_registry_stream", materialize)
    converter = getattr(ss, converter_name)
    result = converter(
        _rows(2_000),
        input_format="python",
        multi_threading=True,
        memory_limit_bytes=64 << 20,
    )
    assert result.clean_data == target
    assert seen == [(target, "multi")]


def test_v76_public_analytical_python_input_when_pyarrow_is_available() -> None:
    """All analytical adapters start from the same Python-row native stream."""
    pytest.importorskip("pyarrow")
    require_native()
    result = ss.to_pyarrow(
        ({"a": index} for index in range(2_000)),
        input_format="python",
        multi_threading=True,
        memory_limit_bytes=64 << 20,
    )
    assert result.clean_data.num_rows == 2_000
