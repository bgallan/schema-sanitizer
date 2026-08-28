"""Direct Parquet directory, schema, and batch runtime tests."""

from __future__ import annotations

import datetime as dt
import gc
from decimal import Decimal
from pathlib import Path

from _support.parquet_runtime import pa, pq
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import read_test_parquet

import schema_sanitizer as ss


@_requires_pyarrow
def test_parquet_directory_converter_uses_direct_arrow_path(
    tmp_path: Path, require_native: None
) -> None:
    folder = tmp_path / "parquet"
    folder.mkdir()
    pq.write_table(pa.table({"id": [1, 2]}), folder / "a.parquet")
    pq.write_table(pa.table({"id": [3]}), folder / "b.parquet")
    out = tmp_path / "out.parquet"

    ss.to_parquet(folder, out, input_format="parquet", input_mode="directory")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = pq.read_table(out).to_pylist()
    assert [{k: v for k, v in row.items() if k not in generated} for row in rows] == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]


@_requires_pyarrow
def test_parquet_directory_mismatched_schemas_use_native_child_arrow_path(
    tmp_path: Path, require_native: None
) -> None:
    folder = tmp_path / "parquet"
    folder.mkdir()
    pq.write_table(pa.table({"id": [1]}), folder / "a.parquet")
    pq.write_table(pa.table({"name": ["two"]}), folder / "b.parquet")

    result = ss.to_pyarrow(folder, input_format="parquet", input_mode="directory")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = result.clean_data.to_pylist()
    assert [{key: value for key, value in row.items() if key not in generated} for row in rows] == [
        {"id": 1, "name": None},
        {"id": None, "name": "two"},
    ]
    assert [row["source_file"] for row in rows] == [
        str((folder / "a.parquet").resolve()),
        str((folder / "b.parquet").resolve()),
    ]


@_requires_pyarrow
def test_parquet_directory_mismatched_schema_converter_uses_native_child_arrow_path(
    tmp_path: Path,
    require_native: None,
) -> None:
    folder = tmp_path / "parquet"
    folder.mkdir()
    pq.write_table(pa.table({"id": [1]}), folder / "a.parquet")
    pq.write_table(pa.table({"name": ["two"]}), folder / "b.parquet")
    out = tmp_path / "out.parquet"

    ss.to_parquet(folder, out, input_format="parquet", input_mode="directory")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = pq.read_table(out).to_pylist()
    assert [{key: value for key, value in row.items() if key not in generated} for row in rows] == [
        {"id": 1, "name": None},
        {"id": None, "name": "two"},
    ]
    assert [row["source_file"] for row in rows] == [
        str((folder / "a.parquet").resolve()),
        str((folder / "b.parquet").resolve()),
    ]


@_requires_pyarrow
def test_direct_parquet_supports_nested_and_explicit_scalar_types() -> None:
    from schema_sanitizer.adapters.parquet import status as pyarrow_adapter

    schema = pa.schema(
        [
            pa.field("items", pa.list_(pa.struct([pa.field("score", pa.int64())]))),
            pa.field("binary_value", pa.binary()),
            pa.field("large_binary_value", pa.large_binary()),
            pa.field("uint64_value", pa.uint64()),
            pa.field("date64_value", pa.date64()),
            pa.field("decimal_value", pa.decimal128(10, 2)),
            pa.field(
                "dictionary_value",
                pa.dictionary(pa.int32(), pa.string()),
            ),
        ]
    )

    assert pyarrow_adapter.parquet_schema_is_direct_native_eligible(
        schema,
        pa=pa,
        timestamp_precision="TIMESTAMP_MICROS",
    )


@_requires_pyarrow
def test_direct_parquet_schema_support_uses_native_payload_cache(monkeypatch) -> None:
    from schema_sanitizer.adapters.parquet import status as direct_schema_support

    direct_schema_support._DIRECT_SCHEMA_SUPPORT_CACHE = direct_schema_support.SchemaDecisionCache()
    calls = 0

    def fake_payload(schema):
        """Return one stable native logical-schema fingerprint."""
        nonlocal calls
        calls += 1
        assert schema.names == ["items"]
        return b"native-logical-schema"

    monkeypatch.setattr(
        direct_schema_support,
        "ARROW_SCHEMA_CONTRACT_PAYLOAD",
        fake_payload,
    )

    schema_one = pa.schema([pa.field("items", pa.list_(pa.struct([pa.field("id", pa.int64())])))])
    schema_two = pa.schema([pa.field("items", pa.list_(pa.struct([pa.field("id", pa.int64())])))])

    assert direct_schema_support.parquet_schema_is_direct_native_eligible(
        schema_one,
        pa=pa,
        timestamp_precision="TIMESTAMP_MICROS",
    )
    assert direct_schema_support.parquet_schema_is_direct_native_eligible(
        schema_two,
        pa=pa,
        timestamp_precision="TIMESTAMP_MICROS",
    )
    assert calls == 1


def test_schema_decision_cache_retains_exact_object_identity() -> None:
    import weakref

    from schema_sanitizer.adapters.pyarrow.schema_decision_cache import SchemaDecisionCache

    class SchemaToken:
        """Weak-referenceable stand-in for a PyArrow schema object."""

        def __str__(self) -> str:
            return "schema-token"

    cache = SchemaDecisionCache(max_size=2)
    schema = SchemaToken()
    schema_ref = weakref.ref(schema)
    cache.set(schema, False, include_text=False)
    del schema
    gc.collect()

    assert schema_ref() is not None


@_requires_pyarrow
def test_direct_parquet_schema_support_rejects_empty_native_payload(monkeypatch) -> None:
    from schema_sanitizer.adapters.parquet import status as direct_schema_support

    direct_schema_support._DIRECT_SCHEMA_SUPPORT_CACHE = direct_schema_support.SchemaDecisionCache()
    monkeypatch.setattr(
        direct_schema_support,
        "ARROW_SCHEMA_CONTRACT_PAYLOAD",
        lambda _schema: b"",
    )

    assert not direct_schema_support.parquet_schema_is_direct_native_eligible(
        pa.schema([pa.field("value", pa.int64())]),
        pa=pa,
        timestamp_precision="TIMESTAMP_MICROS",
    )


@_requires_pyarrow
def test_direct_parquet_record_batch_reader_keeps_iterable_lazy() -> None:
    from schema_sanitizer.adapters.pyarrow.streams import record_batch_reader_from_iterable

    batch = pa.record_batch({"a": [1, 2, 3]})
    consumed = 0

    def batches():
        """Yield one batch and record when iteration starts."""
        nonlocal consumed
        consumed += 1
        yield batch

    reader = record_batch_reader_from_iterable(pa, batch.schema, batches())

    assert consumed == 0
    assert reader.read_next_batch().to_pylist() == batch.to_pylist()
    assert consumed == 1


@_requires_pyarrow
def test_nested_read_parquet_uses_direct_arrow_path(tmp_path: Path, require_native: None) -> None:
    path = tmp_path / "nested.parquet"
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "profile": pa.array(
                [{"name": "a"}, {"name": "b"}],
                type=pa.struct([pa.field("name", pa.string())]),
            ),
            "scores": pa.array([[1, 2], [3]], type=pa.list_(pa.int64())),
        }
    )
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.stats["direct_arrow_input"] == 1
    assert result.clean_data.to_pylist() == table.to_pylist()


@_requires_pyarrow
def test_direct_parquet_normalizes_empty_lists_to_null(
    tmp_path: Path, require_native: None
) -> None:

    path = tmp_path / "empty-list.parquet"
    pq.write_table(
        pa.table({"items": pa.array([[], [1]], type=pa.list_(pa.int64()))}),
        path,
    )

    result = read_test_parquet(path)

    assert result.stats["direct_arrow_input"] == 1
    assert result.clean_data.schema.field("items").type == pa.list_(pa.int64())
    assert result.clean_data.to_pylist() == [{"items": None}, {"items": [1]}]


@_requires_pyarrow
def test_direct_parquet_scales_timestamp_units(tmp_path: Path, require_native: None) -> None:
    path = tmp_path / "timestamps.parquet"
    values = [dt.datetime(2024, 1, 2, 3, 4, 5, 123456)]
    table = pa.table({"ts": pa.array(values, type=pa.timestamp("ns"))})
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.clean_data.schema.field("ts").type == pa.timestamp("us")
    assert result.clean_data.to_pylist() == [{"ts": values[0]}]


@_requires_pyarrow
def test_direct_parquet_binary_and_uint64_have_explicit_text_semantics(
    tmp_path: Path, require_native: None
) -> None:
    path = tmp_path / "scalars.parquet"
    table = pa.table(
        {
            "payload": pa.array([b"\xff"], type=pa.binary()),
            "u": pa.array([2**64 - 1], type=pa.uint64()),
        }
    )
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == [{"payload": "/w==", "u": str(2**64 - 1)}]


@_requires_pyarrow
def test_native_parquet_stream_materializes_deep_requiredness_level_matrix(
    tmp_path: Path,
    require_native: None,
) -> None:
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import (
        native_parquet_recursive_layout_summary,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    path = tmp_path / "native-recursive-requiredness-level-matrix.parquet"
    required_score = pa.struct(
        [
            pa.field("score", pa.int64(), nullable=False),
            pa.field("tag", pa.string()),
        ]
    )
    optional_note = pa.struct(
        [
            pa.field("note", pa.string()),
            pa.field("ok", pa.bool_(), nullable=False),
        ]
    )
    payload_type = pa.struct(
        [
            pa.field(
                "required_items",
                pa.list_(pa.field("item", required_score, nullable=False)),
                nullable=False,
            ),
            pa.field(
                "optional_items",
                pa.list_(pa.field("item", optional_note, nullable=True)),
            ),
            pa.field(
                "deep_required_chain",
                pa.list_(
                    pa.field(
                        "item",
                        pa.list_(pa.field("item", pa.int64(), nullable=False)),
                        nullable=False,
                    )
                ),
            ),
        ]
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("payload", payload_type, nullable=False),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {
                "id": 1,
                "payload": {
                    "required_items": [
                        {"score": 10, "tag": "a"},
                        {"score": 11, "tag": None},
                    ],
                    "optional_items": [
                        {"note": "x", "ok": True},
                        None,
                        {"note": None, "ok": False},
                    ],
                    "deep_required_chain": [[1, 2], [], [3]],
                },
            },
            {
                "id": 2,
                "payload": {
                    "required_items": [],
                    "optional_items": None,
                    "deep_required_chain": None,
                },
            },
            {
                "id": 3,
                "payload": {
                    "required_items": [{"score": 30, "tag": "z"}],
                    "optional_items": [],
                    "deep_required_chain": [[]],
                },
            },
        ],
        schema=schema,
    )
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=1)),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    summary = native_parquet_recursive_layout_summary(path, columns=["payload"])

    assert summary is not None
    assert summary["native_reader_ready"] == 1
    assert summary["stable_across_row_groups"] is True
    assert summary["decoded_row_group_count"] == 3
    assert summary["field_order"] == ["payload"]
    assert summary["fields"][0]["leaf_max_definition_levels"]
    assert summary["fields"][0]["leaf_max_repetition_levels"]
    assert summary["fields"][0]["leaf_path_definition_levels_stable"] is True
    assert summary["fields"][0]["leaf_path_repetition_levels"]
    assert summary["fields"][0]["leaf_path_repetition_levels_stable"] is True
    assert "payload=" in summary["canonical_leaf_level_fingerprint"]
    assert "payload=" in summary["canonical_leaf_repetition_path_fingerprint"]

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["payload"],
    )
    out = pa.RecordBatchReader.from_stream(factory).read_all()

    assert out.schema.equals(table.select(["payload"]).schema)
    assert out.to_pylist() == table.select(["payload"]).to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_direct_parquet_decimal_values_are_lossless_strings(
    tmp_path: Path, require_native: None
) -> None:
    path = tmp_path / "decimal.parquet"
    table = pa.table(
        {
            "amount": pa.array(
                [Decimal("123.45"), Decimal("-0.10")],
                type=pa.decimal128(10, 2),
            )
        }
    )
    pq.write_table(table, path)

    result = read_test_parquet(path)

    assert result.clean_data.schema.field("amount").type == pa.string()
    assert result.clean_data.to_pylist() == [
        {"amount": "123.45"},
        {"amount": "-0.10"},
    ]
