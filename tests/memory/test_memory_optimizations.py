"""Regression tests for bounded-memory native execution paths."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from conftest import read_test_xml, require_native


class _CapsuleStream:
    """Expose an owned Arrow C Stream capsule to another native entry point."""

    def __init__(self, capsule: Any):
        """Retain the capsule for one downstream native consumer."""
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Return the wrapped stream capsule."""
        del requested_schema
        return self._capsule


def _footer(path: Path) -> dict[str, Any]:
    """Read native Parquet footer information without importing PyArrow."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    return json.loads(native_core.parquet_footer_info_json(str(path)))


def test_native_reader_restreams_multiple_row_groups_without_pyarrow(
    tmp_path: Path,
) -> None:
    """The lazy reader must consume every row group through the native writer."""
    require_native()
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_runtime import native_core
    from schema_sanitizer.core_impl.native_symbols import PARQUET_STREAM_WRITE

    source = tmp_path / "source.parquet"
    rewritten = tmp_path / "rewritten.parquet"
    rows = (
        {"id": index, "group": f"g{index % 17}", "payload": "x" * 24} for index in range(70_000)
    )
    stream = ExecutionContext().to_sink_python("stream", rows, None)
    PARQUET_STREAM_WRITE(stream, str(source), "uncompressed", -1, 128 << 20)

    source_info = _footer(source)
    native_capsule = native_core.parquet_stream_read(str(source))
    PARQUET_STREAM_WRITE(
        _CapsuleStream(native_capsule), str(rewritten), "uncompressed", -1, 128 << 20
    )
    rewritten_info = _footer(rewritten)

    assert source_info["num_rows"] == rewritten_info["num_rows"] == 70_000
    assert len(source_info["row_groups"]) == 2
    assert len(rewritten_info["row_groups"]) == 2


def test_native_coalescer_stops_on_retained_byte_budget(tmp_path: Path) -> None:
    """Very wide batches must not be coalesced solely by their row count."""
    require_native()
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import (
        COALESCING_STREAM_WRAP,
        PARQUET_STREAM_WRITE,
    )

    source = ExecutionContext().to_sink_python(
        "stream",
        ({"id": index, "payload": "y" * 64} for index in range(70_000)),
        None,
    )
    capsule = COALESCING_STREAM_WRAP(source, 8 << 20)
    assert capsule is not None

    out = tmp_path / "byte-bounded-coalesce.parquet"
    PARQUET_STREAM_WRITE(_CapsuleStream(capsule), str(out), "uncompressed", -1, 128 << 20)
    info = _footer(out)

    assert info["num_rows"] == 70_000
    assert len(info["row_groups"]) > 2
    assert max(group["num_rows"] for group in info["row_groups"]) < 40_000


def test_native_stream_preflight_is_bounded_and_minimal(tmp_path: Path) -> None:
    """Stream readiness must not retain page diagnostics for every row group."""
    require_native()
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_runtime import native_core
    from schema_sanitizer.core_impl.native_symbols import PARQUET_STREAM_WRITE

    out = tmp_path / "preflight.parquet"
    rows = ({"id": index, "payload": "z" * 32} for index in range(70_000))
    stream = ExecutionContext().to_sink_python("stream", rows, None)
    PARQUET_STREAM_WRITE(stream, str(out), "uncompressed", -1, 128 << 20)

    info = json.loads(native_core.parquet_stream_preflight_json(str(out)))

    assert info["bounded_preflight"] == 1
    assert info["native_reader_ready"] == 1
    assert info["row_group_count"] == 2
    assert len(info["row_groups"]) == 2
    assert all(
        set(row_group) <= {"num_rows", "total_byte_size"} for row_group in info["row_groups"]
    )


def test_public_parquet_reader_skips_deep_footer_diagnostics(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Opening the production stream must use bounded preflight, not full diagnostics."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.adapters.parquet import status
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.core_impl.native_symbols import PARQUET_STREAM_WRITE

    out = tmp_path / "public-reader.parquet"
    rows = ({"id": index, "payload": f"v{index % 13}"} for index in range(70_000))
    stream = ExecutionContext().to_sink_python("stream", rows, None)
    PARQUET_STREAM_WRITE(stream, str(out), "uncompressed", -1, 128 << 20)

    def fail_deep_footer(*args: Any, **kwargs: Any) -> str:
        """Reject accidental use of the deep footer diagnostic path."""
        del args, kwargs
        raise AssertionError("deep footer diagnostics must not run on stream open")

    monkeypatch.setattr(status, "PARQUET_FOOTER_INFO_JSON", fail_deep_footer)
    factory = open_parquet_record_batch_stream_factory(
        out, source="path", feature="memory regression"
    )
    table = pa.RecordBatchReader.from_stream(factory).read_all()

    assert table.num_rows == 70_000


def test_parquet_source_provider_does_not_materialize_all_descriptors(
    monkeypatch: Any,
) -> None:
    """Large source iterables must be consumed only one configured chunk at a time."""
    from schema_sanitizer.api_impl.parquet import arrow_sources

    consumed = 0

    def sources() -> Any:
        """Yield source descriptors while exposing eager materialization."""
        nonlocal consumed
        for index in range(5):
            consumed += 1
            yield arrow_sources.ParquetArrowSource(f"file-{index}.parquet", "path", f"file-{index}")

    def open_chunk(chunk: Any, **kwargs: Any) -> list[tuple[Any, str]]:
        """Return lightweight stand-ins for an opened source chunk."""
        del kwargs
        return [(item.data, item.source_file) for item in chunk]

    monkeypatch.setattr(arrow_sources, "parquet_arrow_sources_or_none", open_chunk)
    monkeypatch.setattr(arrow_sources, "parquet_arrow_source_chunk_size", lambda _options: 2)
    provider = arrow_sources.ParquetArrowSourceChunkProvider(
        sources(), call_options=None, feature="memory regression"
    )

    assert consumed == 0
    assert provider.next_sources() == [
        ("file-0.parquet", "file-0"),
        ("file-1.parquet", "file-1"),
    ]
    assert consumed == 2
    assert provider.next_sources() == [
        ("file-2.parquet", "file-2"),
        ("file-3.parquet", "file-3"),
    ]
    assert consumed == 4
    provider.close()
    assert consumed == 4


def test_native_coalescer_skips_empty_live_batches(tmp_path: Path) -> None:
    """A live zero-row Arrow batch must not force an invalid one-row slice."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.core_impl.native_symbols import (
        COALESCING_STREAM_WRAP,
        PARQUET_STREAM_WRITE,
    )

    schema = pa.schema([pa.field("value", pa.string())])
    empty = pa.record_batch([pa.array([], type=pa.string())], schema=schema)
    populated = pa.record_batch([pa.array(["a", "b"])], schema=schema)
    source = pa.RecordBatchReader.from_batches(schema, [empty, populated])
    capsule = COALESCING_STREAM_WRAP(source, 1 << 20)
    assert capsule is not None

    out = tmp_path / "empty-then-populated.parquet"
    PARQUET_STREAM_WRITE(_CapsuleStream(capsule), str(out), "uncompressed", -1, 128 << 20)
    info = _footer(out)

    assert info["num_rows"] == 2


def test_context_memory_stats_expose_hardening_counters() -> None:
    """Context diagnostics must expose allocator integrity and quota fields."""
    require_native()
    from schema_sanitizer.core_impl.execution import ExecutionContext

    stats = ExecutionContext().memory_stats()

    assert stats["backend_name"] == "schema_sanitizer::DefaultMemoryPool"
    assert stats["bytes_allocated"] >= 0
    assert stats["max_memory"] >= stats["bytes_allocated"]
    assert stats["allocation_count"] >= 0
    assert stats["invalid_free_count"] == 0
    assert stats["size_mismatch_count"] == 0
    assert stats["corruption_count"] == 0
    assert stats["limit_bytes"] == -1


def test_native_flat_parquet_reader_slices_large_row_group(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Flat row groups must stream in bounded windows without changing rows."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    row_count = 5_000
    table = pa.table(
        {
            "id": pa.array(range(row_count), type=pa.int64()),
            "payload": pa.array([f"value-{index % 37}" for index in range(row_count)]),
        }
    )
    path = tmp_path / "flat-window.parquet"
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, [table.to_batches()[0]]),
        path,
        feature="memory window regression",
        parquet_compression="snappy",
    )
    assert len(_footer(path)["row_groups"]) == 1

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="memory window regression",
        memory_limit_bytes=1 << 20,
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    batches = list(reader)

    assert len(batches) == 10
    assert max(batch.num_rows for batch in batches) <= 512
    assert pa.Table.from_batches(batches).to_pylist() == table.to_pylist()


def test_python_generator_spool_encodes_incrementally() -> None:
    """One-shot iterables use bounded native batches without a Python row list."""
    from schema_sanitizer.core_impl.execution import PythonRowsJsonlByteReader

    reader = PythonRowsJsonlByteReader(iter(range(4)), memory_limit_bytes=256)
    calls: list[tuple[int, int, int]] = []

    def encode(
        rows: object, index: int, target_bytes: int, max_rows: int
    ) -> tuple[bytes, int, bool]:
        """Consume one synthetic row directly from the retained iterator."""
        calls.append((index, target_bytes, max_rows))
        value = next(rows)  # type: ignore[arg-type]
        return f'{{"value":{value}}}\n'.encode(), index + 1, False

    reader._native_iter_batch = encode  # type: ignore[method-assign]
    try:
        first = reader._produce_and_record(1)
        second = reader._produce_and_record(1)
    finally:
        reader.close()

    assert first == b'{"value":0}\n'
    assert second == b'{"value":1}\n'
    assert calls == [(0, 1, 4_096), (1, 1, 4_096)]


def test_python_generator_spool_checks_temp_disk_capacity(monkeypatch: Any) -> None:
    """Disk-backed replay must fail before consuming the temp filesystem."""
    from schema_sanitizer.core_impl import execution

    reader = execution.PythonRowsJsonlByteReader(iter([1]), memory_limit_bytes=256)
    reader._spool_memory_bytes = 1
    reader._MIN_FREE_DISK_BYTES = 16
    reader._native_iter_batch = (  # type: ignore[method-assign]
        lambda rows, index, target, max_rows: (b"payload", index + 1, True)
    )

    class _DiskUsage:
        """Expose a fake exhausted filesystem result."""

        free = 0

    from schema_sanitizer.core_impl import python_rows

    monkeypatch.setattr(python_rows.shutil, "disk_usage", lambda path: _DiskUsage())
    try:
        import pytest

        with pytest.raises(OSError, match="insufficient free space"):
            reader._produce_and_record(8)
    finally:
        reader.close()


def test_xml_memory_map_respects_address_space_limit(tmp_path: Path) -> None:
    """Whole-document XML views reject files above the derived materialization budget."""
    require_native()
    pytest.importorskip("pyarrow")

    import schema_sanitizer as ss

    path = tmp_path / "mapped-limit.xml"
    path.write_text(
        "<root><value>" + ("x" * (2 * 1024 * 1024)) + "</value></root>",
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerResourceError):
        read_test_xml(path, memory_limit_bytes=1024 * 1024)


def test_native_struct_parquet_reader_slices_large_row_group(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Non-repeated nested structs must use the same bounded row windows as leaves."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    row_count = 2_049
    payload_type = pa.struct([pa.field("id", pa.int64()), pa.field("label", pa.string())])
    values = [
        None if index % 11 == 0 else {"id": index, "label": f"v-{index % 29}"}
        for index in range(row_count)
    ]
    table = pa.table({"payload": pa.array(values, type=payload_type)})
    path = tmp_path / "struct-window.parquet"
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="nested memory window regression",
        parquet_compression="snappy",
    )
    assert len(_footer(path)["row_groups"]) == 1

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="nested memory window regression",
        memory_limit_bytes=514 * 1024,
    )
    batches = list(pa.RecordBatchReader.from_stream(factory))

    assert len(batches) == 8
    assert max(batch.num_rows for batch in batches) <= 257
    assert pa.Table.from_batches(batches).to_pylist() == table.to_pylist()


def test_native_list_parquet_reader_slices_large_row_group(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Repeated list leaves must be sliced by the selected top-level row window."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    row_count = 1_037
    list_type = pa.list_(pa.field("item", pa.string(), nullable=True))
    values: list[list[str | None] | None] = []
    for index in range(row_count):
        if index % 17 == 0:
            values.append(None)
        elif index % 13 == 0:
            values.append([])
        else:
            values.append([f"v-{index}-{item}" if item % 3 else None for item in range(index % 7)])
    table = pa.table({"values": pa.array(values, type=list_type)})
    path = tmp_path / "list-window.parquet"
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="repeated memory window regression",
        parquet_compression="snappy",
    )
    assert len(_footer(path)["row_groups"]) == 1

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="repeated memory window regression",
        memory_limit_bytes=226 * 1024,
    )
    batches = list(pa.RecordBatchReader.from_stream(factory))

    assert len(batches) == 10
    assert max(batch.num_rows for batch in batches) <= 113
    assert pa.Table.from_batches(batches).to_pylist() == table.to_pylist()


def test_native_nested_repeated_parquet_reader_slices_large_row_group(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Lists of structs and nested lists must preserve offsets across windows."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    row_count = 769
    element_type = pa.struct(
        [
            pa.field("id", pa.int64()),
            pa.field("tags", pa.list_(pa.field("tag", pa.string(), nullable=True))),
        ]
    )
    value_type = pa.list_(pa.field("entry", element_type, nullable=True))
    values: list[list[dict[str, Any] | None] | None] = []
    for index in range(row_count):
        if index % 19 == 0:
            values.append(None)
            continue
        entries: list[dict[str, Any] | None] = []
        for item in range(index % 5):
            if item == 2 and index % 7 == 0:
                entries.append(None)
            else:
                entries.append(
                    {
                        "id": index * 10 + item,
                        "tags": [] if item % 3 == 0 else [f"t-{index % 11}", None, f"i-{item}"],
                    }
                )
        values.append(entries)
    table = pa.table({"entries": pa.array(values, type=value_type)})
    path = tmp_path / "nested-repeated-window.parquet"
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="nested repeated memory window regression",
        parquet_compression="snappy",
    )
    assert len(_footer(path)["row_groups"]) == 1

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="nested repeated memory window regression",
        memory_limit_bytes=194 * 1024,
    )
    batches = list(pa.RecordBatchReader.from_stream(factory))

    assert len(batches) == 8
    assert max(batch.num_rows for batch in batches) <= 97
    assert pa.Table.from_batches(batches).to_pylist() == table.to_pylist()


def test_native_map_parquet_reader_slices_large_row_group(tmp_path: Path, monkeypatch: Any) -> None:
    """Map entry offsets and nullable values must remain valid across windows."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    row_count = 613
    map_type = pa.map_(pa.string(), pa.list_(pa.int64()))
    values: list[list[tuple[str, list[int] | None]] | None] = []
    for index in range(row_count):
        if index % 23 == 0:
            values.append(None)
        else:
            values.append(
                [(f"k-{item}", [] if item % 2 == 0 else [index, item]) for item in range(index % 4)]
            )
    table = pa.table({"attributes": pa.array(values, type=map_type)})
    path = tmp_path / "map-window.parquet"
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="map memory window regression",
        parquet_compression="snappy",
    )
    assert len(_footer(path)["row_groups"]) == 1

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="map memory window regression",
        memory_limit_bytes=142 * 1024,
    )
    batches = list(pa.RecordBatchReader.from_stream(factory))

    assert len(batches) == 9
    assert max(batch.num_rows for batch in batches) <= 71
    assert pa.Table.from_batches(batches).to_pylist() == table.to_pylist()


def test_arrow_direct_rejects_batches_above_logical_slot_limit() -> None:
    """External Arrow metadata is bounded by the derived logical-slot budget."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    logical_offset = 1_000_000
    values = pa.allocate_buffer((logical_offset + 1) * 8)
    array = pa.Array.from_buffers(pa.int64(), 1, [None, values], offset=logical_offset)
    batch = pa.record_batch([array], names=["value"])
    source = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    options = normalize_call_options(memory_limit_bytes=1).raw

    output = ExecutionContext().to_sink_arrow_stream("stream", "arrow", source, options)
    with pytest.raises(pa.ArrowMemoryError, match="logical range exceeds slot limit"):
        pa.RecordBatchReader.from_stream(output).read_all()


def test_arrow_direct_rejects_variable_values_above_logical_byte_limit() -> None:
    """Declared UTF-8 ranges respect the derived logical-byte budget."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.core_impl.execution import ExecutionContext
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    batch = pa.record_batch({"value": pa.array(["x" * (2 * 1024 * 1024)], type=pa.string())})
    source = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    options = normalize_call_options(memory_limit_bytes=1024 * 1024).raw

    output = ExecutionContext().to_sink_arrow_stream("stream", "arrow", source, options)
    with pytest.raises(pa.ArrowMemoryError, match="logical byte limit"):
        pa.RecordBatchReader.from_stream(output).read_all()


def test_native_parquet_reader_enforces_actual_retained_capacity(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Vector capacity and Arrow shells must not escape the estimated window budget."""
    require_native()
    pa = pytest.importorskip("pyarrow")

    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    path = tmp_path / "actual-capacity.parquet"
    table = pa.table({"value": pa.array([1], type=pa.int64())})
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="actual retained capacity regression",
        parquet_compression="snappy",
    )
    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="actual retained capacity regression",
        memory_limit_bytes=64,
    )

    import schema_sanitizer as ss

    with pytest.raises(ss.SchemaSanitizerResourceError, match="memory_limit_bytes"):
        list(pa.RecordBatchReader.from_stream(factory))
