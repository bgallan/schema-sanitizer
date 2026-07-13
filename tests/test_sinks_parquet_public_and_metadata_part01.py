"""Tests read adapters and file-to-file converters."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.core_impl.schema_registry import merge_schema_registry

_GENERATED_METADATA_COLUMNS = {
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
}


def _write_csv(path: Path, text: str = "a,b\n1,2\n3,4\n") -> Path:
    """Write csv."""
    path.write_text(text, encoding="utf-8")
    return path


def _without_generated_metadata(row: dict[str, object]) -> dict[str, object]:
    """Return row data excluding generated file-converter metadata columns."""
    return {k: v for k, v in row.items() if k not in _GENERATED_METADATA_COLUMNS}


def _without_generated_metadata_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Return rows excluding generated file-converter metadata columns."""
    return [_without_generated_metadata(row) for row in rows]


def _native_parquet_zlib_available(pa: object, tmp_path: Path) -> bool:
    """Return whether the compiled native Parquet writer can emit gzip pages."""
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output

    write = native_parquet_output.PARQUET_STREAM_WRITE
    if write is None:
        return False
    batch = pa.record_batch({"text": pa.array(["probe"], type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    try:
        write(stream, str(tmp_path / "native-zlib-probe.parquet"))
    except RuntimeError as exc:
        if "zlib is not available" in str(exc):
            return False
        raise
    return True


# Split from test_sinks_parquet_public_and_metadata.py: test_to_parquet_writes_file, test_parquet_sink_native_coalesces_flat_arrow_batches, test_parquet_sink_native_coalesces_nested_arrow_batches, ...


def test_to_parquet_writes_file(tmp_path: Path) -> None:
    """Verify to parquet writes file."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")

    out = tmp_path / "out.parquet"
    result = ss.to_parquet(
        _write_csv(tmp_path / "rows.csv", "a,b\n1,2\n"),
        out,
        input_format="csv",
    )
    assert isinstance(result, ss.Result)
    assert result.clean_data is None
    rows = pq.read_table(out).to_pylist()
    assert _without_generated_metadata_rows(rows) == [{"a": "1", "b": "2"}]


def test_parquet_sink_native_coalesces_flat_arrow_batches(tmp_path: Path) -> None:
    """Verify the Parquet sink uses native coalescing for supported flat streams."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.parquet.sink import (
        _write_coalesced_batches,
        last_parquet_coalesce_route,
    )

    batches = [
        pa.record_batch(
            {
                "id": pa.array([index], type=pa.int64()),
                "name": pa.array([f"name-{index}"], type=pa.string()),
            }
        )
        for index in range(8)
    ]
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    out = tmp_path / "flat.parquet"
    writer = pq.ParquetWriter(out, batches[0].schema)
    try:
        _write_coalesced_batches(
            writer,
            reader,
            schema=batches[0].schema,
            pa=pa,
            row_group_rows=1024,
        )
    finally:
        writer.close()

    parquet_file = pq.ParquetFile(out)
    assert last_parquet_coalesce_route() == "native"
    assert parquet_file.metadata.num_rows == 8
    assert parquet_file.metadata.num_row_groups == 1
    assert pq.read_table(out).to_pylist() == [
        {"id": index, "name": f"name-{index}"} for index in range(8)
    ]


def test_parquet_sink_native_coalesces_nested_arrow_batches(tmp_path: Path) -> None:
    """Verify nested streams coalesce through the native path."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.parquet.sink import (
        _write_coalesced_batches,
        last_parquet_coalesce_route,
    )

    payload_type = pa.struct(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("flags", pa.list_(pa.bool_())),
        ]
    )
    item_type = pa.struct([pa.field("score", pa.float64()), pa.field("label", pa.string())])
    schema = pa.schema(
        [
            pa.field("payload", payload_type),
            pa.field("items", pa.list_(item_type)),
        ]
    )
    rows = [
        {
            "payload": (
                None
                if index == 5
                else {
                    "id": index,
                    "name": None if index == 2 else f"name-{index}",
                    "flags": None if index == 1 else [True, False] if index % 2 == 0 else [],
                }
            ),
            "items": (
                None
                if index == 4
                else (
                    []
                    if index % 3 == 0
                    else [
                        {"score": index + 0.5, "label": f"a-{index}"},
                        {"score": index + 1.5, "label": None if index == 7 else f"b-{index}"},
                    ]
                )
            ),
        }
        for index in range(8)
    ]
    table = pa.Table.from_pylist(rows, schema=schema)
    batches = [table.slice(index, 1).to_batches()[0] for index in range(8)]
    reader = pa.RecordBatchReader.from_batches(schema, batches)
    out = tmp_path / "nested.parquet"
    writer = pq.ParquetWriter(out, schema)
    try:
        _write_coalesced_batches(
            writer,
            reader,
            schema=schema,
            pa=pa,
            row_group_rows=1024,
        )
    finally:
        writer.close()

    parquet_file = pq.ParquetFile(out)
    assert last_parquet_coalesce_route() == "native"
    assert parquet_file.metadata.num_rows == 8
    assert parquet_file.metadata.num_row_groups == 1
    assert pq.read_table(out).to_pylist() == rows


def test_parquet_sink_native_coalesces_dictionary_arrow_batches(
    tmp_path: Path,
) -> None:
    """Verify dictionary streams coalesce through the native path."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.adapters.parquet.sink import (
        _write_coalesced_batches,
        last_parquet_coalesce_route,
    )

    schema = pa.schema([pa.field("coded", pa.dictionary(pa.int8(), pa.string()))])
    dictionary = pa.array(["value-0", "value-1"], type=pa.string())
    batches = [
        pa.record_batch(
            [pa.DictionaryArray.from_arrays(pa.array([index % 2], type=pa.int8()), dictionary)],
            schema=schema,
        )
        for index in range(8)
    ]
    reader = pa.RecordBatchReader.from_batches(schema, batches)
    out = tmp_path / "dictionary.parquet"
    writer = pq.ParquetWriter(out, schema)
    try:
        _write_coalesced_batches(
            writer,
            reader,
            schema=schema,
            pa=pa,
            row_group_rows=1024,
        )
    finally:
        writer.close()

    parquet_file = pq.ParquetFile(out)
    assert last_parquet_coalesce_route() == "native"
    assert parquet_file.metadata.num_rows == 8
    assert parquet_file.metadata.num_row_groups == 1
    assert pq.read_table(out).to_pylist() == [{"coded": f"value-{index % 2}"} for index in range(8)]


def test_parquet_sink_rejects_changed_dictionary_during_native_coalescing() -> None:
    """Verify native dictionary coalescing fails before remapping unsafe indices."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    from schema_sanitizer.adapters.parquet.sink import _write_coalesced_batches

    schema = pa.schema([pa.field("coded", pa.dictionary(pa.int8(), pa.string()))])
    batches = [
        pa.record_batch(
            [
                pa.DictionaryArray.from_arrays(
                    pa.array([0], type=pa.int8()),
                    pa.array(["a", "b"], type=pa.string()),
                )
            ],
            schema=schema,
        ),
        pa.record_batch(
            [
                pa.DictionaryArray.from_arrays(
                    pa.array([0], type=pa.int8()),
                    pa.array(["b", "a"], type=pa.string()),
                )
            ],
            schema=schema,
        ),
    ]
    reader = pa.RecordBatchReader.from_batches(schema, batches)

    class Writer:
        """Track whether unsafe dictionary coalescing writes any batch."""

        wrote = False

        def write_batch(self, _batch: object) -> None:
            """Mark unexpected writes."""
            self.wrote = True

    writer = Writer()
    with pytest.raises(Exception, match="dictionary values changed"):
        _write_coalesced_batches(writer, reader, schema=schema, pa=pa, row_group_rows=1024)

    assert writer.wrote is False


def test_to_parquet_omits_empty_container_only_fields(tmp_path: Path) -> None:
    """Verify empty objects and lists do not create inferred source columns."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text(
        '{"writer":{},"items":[],"wrapper":{"child":{}},"nested_items":[{}]}\n',
        encoding="utf-8",
    )
    out = tmp_path / "out.parquet"

    ss.to_parquet(source, out, input_format="jsonl")

    table = pq.read_table(out)
    assert "writer" not in table.schema.names
    assert "items" not in table.schema.names
    assert "wrapper" not in table.schema.names
    assert "nesteditems" not in table.schema.names
    assert _without_generated_metadata_rows(table.to_pylist()) == [{}]


def test_to_parquet_writes_mixed_empty_and_populated_objects(tmp_path: Path) -> None:
    """Verify established fields materialize empty containers as null."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    source = tmp_path / "rows.jsonl"
    source.write_text(
        '{"writer":{},"items":[]}\n{"writer":{"name":"Alex"},"items":[1,2]}\n',
        encoding="utf-8",
    )
    out = tmp_path / "out.parquet"

    ss.to_parquet(source, out, input_format="jsonl")

    table = pq.read_table(out)
    assert pa.types.is_struct(table.schema.field("writer").type)
    assert pa.types.is_list(table.schema.field("items").type)
    assert _without_generated_metadata_rows(table.to_pylist()) == [
        {"items": None, "writer": None},
        {"items": [1, 2], "writer": {"name": "Alex"}},
    ]


def test_registry_keeps_existing_fields_for_empty_containers(tmp_path: Path) -> None:
    """Verify empty values neither remove registered fields nor create drift."""
    require_native()
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    previous = merge_schema_registry(
        inferred_schema=pa.schema(
            [
                pa.field("items", pa.list_(pa.int64())),
                pa.field("writer", pa.struct([pa.field("id", pa.int64())])),
            ]
        ),
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    source = tmp_path / "rows.jsonl"
    source.write_text('{"items":[],"writer":{}}\n', encoding="utf-8")
    out = tmp_path / "out.parquet"

    result = ss.to_parquet(
        source,
        out,
        input_format="jsonl",
        schema_mode="strict",
        field_name_policy="lower_snake",
        schema_registry=previous.schema_registry,
    )

    row = _without_generated_metadata_rows(pq.read_table(out).to_pylist())[0]
    assert row == {"items": None, "writer": None}
    assert result.schema_drifts == []
    assert (
        result.schema_registry["schema_generation"] == previous.schema_registry["schema_generation"]
    )


def test_empty_first_partition_does_not_destabilize_registry(tmp_path: Path) -> None:
    """Verify later evidence gets the original name and replays stay stable."""
    require_native()
    pq = pytest.importorskip("pyarrow.parquet")

    empty_source = tmp_path / "empty.jsonl"
    empty_source.write_text('{"items":[],"writer":{}}\n', encoding="utf-8")
    empty_out = tmp_path / "empty.parquet"
    empty_result = ss.to_parquet(
        empty_source,
        empty_out,
        input_format="jsonl",
        field_name_policy="lower_snake",
    )

    populated_source = tmp_path / "populated.jsonl"
    populated_source.write_text(
        '{"items":[1],"writer":{"id":2}}\n',
        encoding="utf-8",
    )
    populated_out = tmp_path / "populated.parquet"
    populated_result = ss.to_parquet(
        populated_source,
        populated_out,
        input_format="jsonl",
        field_name_policy="lower_snake",
        schema_registry=empty_result.schema_registry,
    )

    populated_names = pq.read_table(populated_out).schema.names
    assert {"items", "writer"}.issubset(populated_names)
    assert not any(name.startswith(("items_v", "writer_v")) for name in populated_names)
    assert [drift["output_name"] for drift in populated_result.schema_drifts] == [
        "items",
        "writer",
    ]

    replay_out = tmp_path / "replay.parquet"
    replay_result = ss.to_parquet(
        empty_source,
        replay_out,
        input_format="jsonl",
        schema_mode="strict",
        field_name_policy="lower_snake",
        schema_registry=populated_result.schema_registry,
    )

    replay_row = _without_generated_metadata_rows(pq.read_table(replay_out).to_pylist())[0]
    assert replay_row == {"items": None, "writer": None}
    assert replay_result.schema_drifts == []
    assert (
        replay_result.schema_registry["schema_generation"]
        == populated_result.schema_registry["schema_generation"]
    )
