"""Parquet API/runtime tests split by contract area."""

from __future__ import annotations

import datetime as dt
import logging
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import read_test_parquet, require_native
from parquet_runtime_support import sample_table

import schema_sanitizer as ss

try:
    import pyarrow as pa
    import pyarrow.feather as feather
    import pyarrow.parquet as pq

    _HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = feather = pq = None
    _HAVE_PYARROW = False

_requires_pyarrow = pytest.mark.skipif(not _HAVE_PYARROW, reason="pyarrow not installed")


@_requires_pyarrow
def test_native_parquet_reader_memory_budget_blocks_native_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify native Parquet reader refuses row groups over its buffer budget."""
    from schema_sanitizer.adapters.parquet.status import (
        native_parquet_footer_info,
        native_parquet_stream_preflight_info,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    require_native()
    path = tmp_path / "budget.parquet"
    table = pa.table(
        {
            "a": pa.array([1, 2, 3], type=pa.int64()),
            "b": pa.array(["wide-value-000", "wide-value-001", "wide-value-002"]),
        }
    )
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 1

    limited_info = native_parquet_stream_preflight_info(path, memory_limit_bytes=1)

    assert limited_info is not None
    assert limited_info["native_reader_ready"] == 0
    assert any(
        "native buffer estimate" in blocker and "exceeds configured limit 1" in blocker
        for blocker in limited_info["native_reader_blockers"]
    )

    with pytest.raises(ss.SchemaSanitizerResourceError, match="memory_limit_bytes"):
        read_test_parquet(path, memory_limit_bytes=1)


@_requires_pyarrow
def test_read_parquet_retries_pyarrow_after_native_reader_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify native Parquet reader failure falls back to a PyArrow stream."""
    from schema_sanitizer.api_impl.parquet import direct_routes as parquet_direct_routes
    from schema_sanitizer.api_impl.parquet.direct_routes import last_parquet_direct_route

    require_native()
    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)

    def fail_native_reader(*_args: object, **_kwargs: object) -> object:
        """Simulate a fatal native direct Parquet reader failure."""
        raise RuntimeError("native Parquet reader: simulated fatal bug")

    monkeypatch.setattr(parquet_direct_routes, "call_core", fail_native_reader)
    caplog.set_level(logging.ERROR, logger="schema_sanitizer.api_impl.parquet.direct_routes")

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == sample_table(pa).to_pylist()
    assert last_parquet_direct_route() == "pyarrow"
    assert "retrying input with PyArrow" in caplog.text


@_requires_pyarrow
def test_to_parquet_retries_pyarrow_after_native_reader_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify Parquet conversion retries with PyArrow after native reader failure."""
    from schema_sanitizer.api_impl.parquet import direct_routes as parquet_direct_routes

    require_native()
    path = tmp_path / "data.parquet"
    out = tmp_path / "out.parquet"
    pq.write_table(sample_table(pa), path)

    def fail_native_reader(*_args: object, **_kwargs: object) -> object:
        """Simulate a fatal native direct Parquet reader failure."""
        raise RuntimeError("native Parquet reader: simulated fatal conversion bug")

    monkeypatch.setattr(parquet_direct_routes, "call_core", fail_native_reader)
    caplog.set_level(logging.ERROR, logger="schema_sanitizer.api_impl.parquet.direct_routes")

    ss.to_parquet(
        path,
        out,
        input_format="parquet",
        parquet_compression="uncompressed",
    )

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = pq.read_table(out).to_pylist()
    assert [{key: value for key, value in row.items() if key not in generated} for row in rows] == [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
        {"a": 3, "b": "z"},
    ]
    assert "schema_registry" in rows[0]
    assert "schema_drifts" in rows[0]
    assert "retrying input with PyArrow" in caplog.text


@_requires_pyarrow
def test_native_parquet_footer_info_reads_pyarrow_file(tmp_path: Path) -> None:
    """Verify native Parquet footer parsing reads bounded file metadata."""
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info

    require_native()
    path = tmp_path / "data.parquet"
    pq.write_table(sample_table(pa), path)

    info = native_parquet_footer_info(path)

    assert info is not None
    assert info["num_rows"] == 3
    assert info["row_group_count"] == 1
    assert info["schema_element_count"] >= 3
    assert isinstance(info["created_by"], str)
    assert info["native_reader_ready"] == 0
    assert (
        "file was not written by schema-sanitizer native parquet writer"
        in info["native_reader_blockers"]
    )
    assert [element["name"] for element in info["schema_elements"]] == [
        "schema",
        "a",
        "b",
    ]
    assert info["schema_elements"][1]["physical_type"] == 2
    assert info["schema_elements"][2]["physical_type"] == 6
    assert info["row_groups"][0]["num_rows"] == 3
    assert [column["path_in_schema"] for column in info["row_groups"][0]["columns"]] == [
        ["a"],
        ["b"],
    ]
    assert all(column["num_values"] == 3 for column in info["row_groups"][0]["columns"])
    assert all("data_page_offset" in column for column in info["row_groups"][0]["columns"])
    for column in info["row_groups"][0]["columns"]:
        assert column["pages"][0]["type"] == 2
        assert column["pages"][0]["is_dictionary_page"] == 1
        assert column["pages"][0]["value_encoding"] == 0
        assert column["pages"][1]["type"] == 0
        assert column["pages"][1]["is_dictionary_page"] == 0
        assert column["pages"][1]["num_values"] == 3
        assert column["pages"][1]["value_encoding"] == 8
        assert column["pages"][1]["payload_verified"] == 1


@_requires_pyarrow
def test_spark_int96_parquet_uses_pyarrow_fallback(tmp_path: Path) -> None:
    """Verify Spark-style INT96 timestamps stay readable through fallback."""
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()
    path = tmp_path / "spark-int96.parquet"
    value = dt.datetime(2024, 1, 1, 1, 2, 3, 123456)
    pq.write_table(
        pa.table({"ts": pa.array([value], type=pa.timestamp("ns"))}),
        path,
        flavor="spark",
        use_deprecated_int96_timestamps=True,
    )

    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 0
    assert info["schema_elements"][1]["physical_type"] == 3
    assert any("unsupported physical type" in blocker for blocker in info["native_reader_blockers"])

    result = read_test_parquet(path)

    assert result.clean_data.schema.field("ts").type == pa.timestamp("us")
    assert result.clean_data.to_pylist() == [{"ts": value}]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert any("unsupported physical type" in blocker for blocker in diagnostics["blockers"])


@_requires_pyarrow
def test_spark_flavored_nested_parquet_uses_pyarrow_fallback(tmp_path: Path) -> None:
    """Verify Spark-flavored nested Parquet remains readable through fallback."""
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()
    path = tmp_path / "spark-flavored-nested.parquet"
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "profile": pa.array(
                [{"name": "a", "score": 1.5}, {"name": "b", "score": None}],
                type=pa.struct(
                    [
                        pa.field("name", pa.string()),
                        pa.field("score", pa.float64()),
                    ]
                ),
            ),
            "tags": pa.array([["alpha", "beta"], ["gamma"]], type=pa.list_(pa.string())),
        }
    )
    pq.write_table(table, path, flavor="spark", compression="snappy")

    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 0
    assert any(
        "file was not written by schema-sanitizer native parquet writer" in blocker
        for blocker in info["native_reader_blockers"]
    )
    assert not any(
        "unsupported compression" in blocker for blocker in info["native_reader_blockers"]
    )

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == table.to_pylist()
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"


@_requires_pyarrow
def test_pyarrow_legacy_nested_list_map_encoding_uses_pyarrow_fallback(
    tmp_path: Path,
) -> None:
    """Verify legacy nested encodings use the canonical sanitized representation."""
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()
    path = tmp_path / "pyarrow-legacy-nested-list-map.parquet"
    item_type = pa.struct(
        [
            pa.field("sku", pa.string()),
            pa.field("attrs", pa.map_(pa.string(), pa.list_(pa.int64()))),
        ]
    )
    table = pa.table(
        {
            "orders": pa.array(
                [
                    [
                        {"sku": "a", "attrs": [("color", [1, 2]), ("size", [])]},
                        {"sku": "b", "attrs": None},
                    ],
                    None,
                    [],
                    [{"sku": None, "attrs": [("missing", None)]}],
                ],
                type=pa.list_(item_type),
            )
        }
    )
    pq.write_table(
        table,
        path,
        store_schema=False,
        compression="snappy",
        use_compliant_nested_type=False,
    )

    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 0
    assert any(
        "file was not written by schema-sanitizer native parquet writer" in blocker
        for blocker in info["native_reader_blockers"]
    )

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == [
        {
            "orders": [
                {
                    "attrs": [
                        {"key": "color", "value": [1, 2]},
                        {"key": "size", "value": None},
                    ],
                    "sku": "a",
                },
                {"attrs": None, "sku": "b"},
            ]
        },
        {"orders": None},
        {"orders": None},
        {
            "orders": [
                {
                    "attrs": [{"key": "missing", "value": None}],
                    "sku": None,
                }
            ]
        },
    ]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert any(
        "file was not written by schema-sanitizer native parquet writer" in blocker
        for blocker in diagnostics["blockers"]
    )


@_requires_pyarrow
def test_bigquery_compatible_standard_parquet_uses_pyarrow_fallback(
    tmp_path: Path,
) -> None:
    """Verify BigQuery-style logical scalars without Arrow metadata stay readable."""
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()
    path = tmp_path / "bigquery-compatible.parquet"
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["a", "b"], type=pa.string()),
            "active": pa.array([True, None], type=pa.bool_()),
            "amount": pa.array(
                [Decimal("12.34"), Decimal("56.78")],
                type=pa.decimal128(10, 2),
            ),
            "event_date": pa.array(
                [dt.date(2024, 1, 1), dt.date(2024, 1, 2)],
                type=pa.date32(),
            ),
            "event_ts": pa.array(
                [
                    dt.datetime(2024, 1, 1, 1, 2, 3, 123456),
                    dt.datetime(2024, 1, 2, 1, 2, 3, 123456),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )
    pq.write_table(
        table,
        path,
        store_schema=False,
        compression="snappy",
        coerce_timestamps="us",
    )

    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 0
    assert info["schema_elements"][4]["logical_type"] == "decimal"
    assert info["schema_elements"][5]["logical_type"] == "date"
    assert info["schema_elements"][6]["logical_type"] == "timestamp"
    assert any(
        "file was not written by schema-sanitizer native parquet writer" in blocker
        for blocker in info["native_reader_blockers"]
    )
    assert not any(
        "unsupported compression" in blocker for blocker in info["native_reader_blockers"]
    )
    for column in info["row_groups"][0]["columns"]:
        for page in column["pages"]:
            if page["is_dictionary_page"] == 0:
                assert page["payload_verified"] == 1
                assert page["values_decoded"] == 1

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == [
        {
            "active": True,
            "amount": "12.34",
            "eventdate": dt.date(2024, 1, 1),
            "eventts": dt.datetime(2024, 1, 1, 1, 2, 3, 123456),
            "id": 1,
            "name": "a",
        },
        {
            "active": None,
            "amount": "56.78",
            "eventdate": dt.date(2024, 1, 2),
            "eventts": dt.datetime(2024, 1, 2, 1, 2, 3, 123456),
            "id": 2,
            "name": "b",
        },
    ]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"


@_requires_pyarrow
def test_bigquery_export_like_nested_parquet_uses_pyarrow_fallback(
    tmp_path: Path,
) -> None:
    """Verify BigQuery-export-like nested/repeated Parquet stays readable."""
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()
    path = tmp_path / "bigquery-export-like.parquet"
    table = pa.table(
        {
            "user_id": pa.array(["u1", "u2"], type=pa.string()),
            "event_date": pa.array(
                [dt.date(2024, 2, 1), dt.date(2024, 2, 2)],
                type=pa.date32(),
            ),
            "event_ts": pa.array(
                [
                    dt.datetime(2024, 2, 1, 12, 0, 0, 123456),
                    dt.datetime(2024, 2, 2, 12, 0, 0, 123456),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "metrics": pa.array(
                [
                    {"score": Decimal("12.34"), "rank": 1},
                    {"score": Decimal("56.78"), "rank": 2},
                ],
                type=pa.struct(
                    [
                        pa.field("score", pa.decimal128(10, 2)),
                        pa.field("rank", pa.int64()),
                    ]
                ),
            ),
            "items": pa.array(
                [
                    [{"sku": "a", "quantity": 2}, {"sku": "b", "quantity": 1}],
                    [{"sku": "c", "quantity": 3}],
                ],
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("sku", pa.string()),
                            pa.field("quantity", pa.int64()),
                        ]
                    )
                ),
            ),
        }
    )
    pq.write_table(
        table,
        path,
        store_schema=False,
        compression="snappy",
        coerce_timestamps="us",
    )

    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 0
    assert any(
        "file was not written by schema-sanitizer native parquet writer" in blocker
        or "nested or repeated column is not yet native materializable" in blocker
        for blocker in info["native_reader_blockers"]
    )
    assert not any(
        "unsupported compression" in blocker for blocker in info["native_reader_blockers"]
    )

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == [
        {
            "eventdate": dt.date(2024, 2, 1),
            "eventts": dt.datetime(2024, 2, 1, 12, 0, 0, 123456),
            "items": [{"quantity": 2, "sku": "a"}, {"quantity": 1, "sku": "b"}],
            "metrics": {"rank": 1, "score": "12.34"},
            "userid": "u1",
        },
        {
            "eventdate": dt.date(2024, 2, 2),
            "eventts": dt.datetime(2024, 2, 2, 12, 0, 0, 123456),
            "items": [{"quantity": 3, "sku": "c"}],
            "metrics": {"rank": 2, "score": "56.78"},
            "userid": "u2",
        },
    ]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"


@_requires_pyarrow
def test_duckdb_written_parquet_uses_pyarrow_fallback(tmp_path: Path) -> None:
    """Verify DuckDB-written Parquet stays readable through the safe fallback."""
    duckdb = pytest.importorskip("duckdb")
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_native_reader_diagnostics,
        last_parquet_stream_factory_route,
    )

    require_native()
    path = tmp_path / "duckdb.parquet"
    with duckdb.connect() as connection:
        connection.execute(
            "COPY (SELECT 1::BIGINT AS a, 'x' AS b UNION ALL SELECT 2, 'y') TO ? (FORMAT PARQUET)",
            [str(path)],
        )

    info = native_parquet_footer_info(path)
    assert info is not None
    assert "DuckDB" in info["created_by"]
    assert info["native_reader_ready"] == 0
    assert (
        "file was not written by schema-sanitizer native parquet writer"
        in info["native_reader_blockers"]
    )

    result = read_test_parquet(path)

    assert result.clean_data.to_pylist() == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    assert last_parquet_stream_factory_route() == "pyarrow_dataset_scanner"
    diagnostics = last_parquet_native_reader_diagnostics()
    assert diagnostics["attempted"] is True
    assert diagnostics["ready"] is False
    assert diagnostics["reason"] == "not_ready"
    assert (
        "file was not written by schema-sanitizer native parquet writer" in diagnostics["blockers"]
    )
