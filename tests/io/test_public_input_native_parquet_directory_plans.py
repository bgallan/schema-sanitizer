"""Tests explicit public input formats, extensions, and directory mode."""

from __future__ import annotations

from pathlib import Path

import pytest
from _support.public_input_modes import GENERATED_COLUMNS as GENERATED
from _support.public_input_modes import data_rows as _data_rows
from conftest import require_native

import schema_sanitizer as ss


def test_native_directory_source_plan_is_native_first(tmp_path: Path) -> None:
    """Verify source plans keep native path-source plans without retaining tuple payloads."""
    require_native()
    from schema_sanitizer.api_impl.input.preparation import prepare_public_input
    from schema_sanitizer.api_impl.source_plan.attached import source_plan_from_data

    folder = tmp_path / "jsonl"
    folder.mkdir()
    source = folder / "a.jsonl"
    source.write_text('{"id":1}\n', encoding="utf-8")

    prepared = prepare_public_input(
        folder,
        input_format="jsonl",
        input_mode="directory",
        input_text_encoding="utf-8",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
    )
    try:
        plan = source_plan_from_data(prepared.data)
        assert plan is not None
        assert plan.native_payload is not None
        assert plan.payload is None
        assert [item.path for item in plan.source_batch.sources] == [str(source)]
    finally:
        prepared.close()


@pytest.mark.parametrize("input_format", ["csv", "json_array", "xml"])
def test_directory_conversion_uses_self_bootstrapping_native_path_sources(
    tmp_path: Path, monkeypatch, input_format: str
) -> None:
    """Verify directory conversion avoids the old Python registry pre-probe."""
    from schema_sanitizer.api_impl.source_plan import probing as source_plan_probe

    folder = tmp_path / input_format
    folder.mkdir()
    options = {"input_format": input_format, "input_mode": "directory"}
    if input_format == "csv":
        (folder / "a.csv").write_text("id,name\n1,Ana\n", encoding="utf-8")
        (folder / "b.csv").write_text("id,name\n2,Luis\n", encoding="utf-8")
        expected_ids = ["1", "2"]
    elif input_format == "json_array":
        (folder / "a.json").write_text('[{"id":1,"name":"Ana"}]', encoding="utf-8")
        (folder / "b.json").write_text('[{"id":2,"score":10}]', encoding="utf-8")
        expected_ids = [1, 2]
    else:
        (folder / "a.xml").write_text(
            "<row><id>1</id><name>Ana</name></row>",
            encoding="utf-8",
        )
        (folder / "b.xml").write_text(
            "<row><id>2</id><score>10</score></row>",
            encoding="utf-8",
        )
        options["xml_row_tag"] = "row"
        expected_ids = ["1", "2"]

    def fail_python_probe(*_args, **_kwargs):
        """Fail when the old Python registry pre-probe path is used."""
        raise AssertionError("directory conversion should self-bootstrap in native code")

    monkeypatch.setattr(
        source_plan_probe,
        "infer_native_multisource_registry",
        fail_python_probe,
    )

    result = ss.to_pyarrow(folder, **options)

    rows = _data_rows(result)
    assert [row["id"] for row in rows] == expected_ids
    if input_format in {"json_array", "xml"}:
        assert rows[1]["score"] == (10 if input_format == "json_array" else "10")


def test_parquet_directory_source_file_tracks_each_child(tmp_path: Path) -> None:
    """Verify Parquet directory rows carry the child Parquet path."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    folder = tmp_path / "parquet"
    folder.mkdir()
    pq.write_table(pa.table({"id": [1, 2]}), folder / "a.parquet")
    pq.write_table(pa.table({"id": [3]}), folder / "b.parquet")

    result = ss.to_pyarrow(folder, input_format="parquet", input_mode="directory")

    assert _data_rows(result) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.parquet").resolve()),
        str((folder / "a.parquet").resolve()),
        str((folder / "b.parquet").resolve()),
    ]


def test_parquet_directory_source_file_is_native_tracked(tmp_path: Path) -> None:
    """Verify native Parquet directory conversion owns source_file tracking."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.parquet.multisource import (
        last_parquet_multisource_route,
    )

    folder = tmp_path / "parquet"
    folder.mkdir()
    pq.write_table(pa.table({"id": [1, 2]}), folder / "a.parquet")
    pq.write_table(pa.table({"id": [3]}), folder / "b.parquet")

    result = ss.to_pyarrow(folder, input_format="parquet", input_mode="directory")

    assert _data_rows(result) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.parquet").resolve()),
        str((folder / "a.parquet").resolve()),
        str((folder / "b.parquet").resolve()),
    ]
    assert last_parquet_multisource_route() == "native_arrow_source_chunk_provider_auto_registry"


def test_parquet_directory_writer_source_file_does_not_precount_rows(
    tmp_path: Path,
) -> None:
    """Verify Parquet directory file writers use native source_file tracking."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    folder = tmp_path / "parquet"
    folder.mkdir()
    pq.write_table(pa.table({"id": [1, 2]}), folder / "a.parquet")
    pq.write_table(pa.table({"id": [3]}), folder / "b.parquet")
    out = tmp_path / "out.parquet"

    result = ss.to_parquet(folder, out, input_format="parquet", input_mode="directory")

    assert result.schema_registry is not None
    table = pq.read_table(out)
    assert table.column("source_file").to_pylist() == [
        str((folder / "a.parquet").resolve()),
        str((folder / "a.parquet").resolve()),
        str((folder / "b.parquet").resolve()),
    ]
    assert result.stats["inferred_rows"] == 3
    assert result.stats["materialized_rows"] == 3
    assert result.stats["batches"] >= 1


def test_parquet_directory_writer_uses_arrow_source_auto_registry(
    tmp_path: Path,
) -> None:
    """Verify Parquet directory file writers use native Arrow-source auto registry."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.parquet.multisource import (
        last_parquet_multisource_route,
    )

    folder = tmp_path / "parquet-auto-registry"
    folder.mkdir()
    pq.write_table(pa.table({"id": [1]}), folder / "a.parquet")
    pq.write_table(pa.table({"name": ["two"]}), folder / "b.parquet")
    out = tmp_path / "out.parquet"

    result = ss.to_parquet(folder, out, input_format="parquet", input_mode="directory")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = pq.read_table(out).to_pylist()
    assert result.schema_registry is not None
    assert last_parquet_multisource_route() == "native_arrow_source_chunk_provider_auto_registry"
    assert [{key: value for key, value in row.items() if key not in generated} for row in rows] == [
        {"id": 1, "name": None},
        {"id": None, "name": "two"},
    ]
    assert result.stats["inferred_rows"] == 2
    assert result.stats["materialized_rows"] == 2
    assert result.stats["batches"] >= 1


def test_parquet_directory_writer_accepts_previous_native_registry_state(
    tmp_path: Path,
) -> None:
    """Verify Parquet directory writers can start from compiled registry state."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.core_impl.schema_registry import native_registry_state_context

    warm = tmp_path / "parquet-warm"
    warm.mkdir()
    pq.write_table(pa.table({"id": [1], "warm_only": ["seen"]}), warm / "warm.parquet")
    warm_result = ss.to_parquet(
        warm, tmp_path / "warm.parquet", input_format="parquet", input_mode="directory"
    )

    current = tmp_path / "parquet-current"
    current.mkdir()
    pq.write_table(pa.table({"id": [2]}), current / "a.parquet")
    pq.write_table(pa.table({"name": ["two"]}), current / "b.parquet")
    out = tmp_path / "out.parquet"

    assert warm_result.native_registry_state is not None
    with native_registry_state_context(warm_result.native_registry_state):
        result = ss.to_parquet(current, out, input_format="parquet", input_mode="directory")

    rows = pq.read_table(out).to_pylist()
    assert result.native_registry_state is not None
    assert [{key: value for key, value in row.items() if key not in GENERATED} for row in rows] == [
        {"id": 2, "warm_only": None, "name": None},
        {"id": None, "warm_only": None, "name": "two"},
    ]


def test_parquet_directory_source_plan_does_not_preopen_factories(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Verify Parquet source planning is lazy and leaves factory opening to execution."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    import schema_sanitizer.input_impl.source_plan as source_plan_model
    from schema_sanitizer.api_impl.input import preparation as public_input
    from schema_sanitizer.api_impl.parquet import arrow_sources as parquet_arrow_sources
    from schema_sanitizer.api_impl.source_plan import preparation as source_plan_prepared

    folder = tmp_path / "lazy-parquet-plan"
    folder.mkdir()
    pq.write_table(pa.table({"id": [1]}), folder / "a.parquet")
    pq.write_table(pa.table({"id": [2]}), folder / "b.parquet")

    prepared = public_input.prepare_public_input(
        folder,
        input_format="parquet",
        input_mode="directory",
        input_text_encoding="utf-8",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
    )

    def fail_factory_open(*args, **kwargs):
        """Fail if planning opens per-file Arrow factories."""
        raise AssertionError("Parquet planning should not open Arrow factories")

    monkeypatch.setattr(
        parquet_arrow_sources,
        "parquet_arrow_stream_factory_or_none",
        fail_factory_open,
    )

    plan = source_plan_prepared.source_plan_from_prepared_inputs(
        [prepared],
        input_format="parquet",
        input_mode="directory",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
    )

    assert plan is not None
    assert plan.kind == source_plan_model.PARQUET_ARROW_SOURCES
    assert [source.path for source in plan.payload.files] == [
        str(folder / "a.parquet"),
        str(folder / "b.parquet"),
    ]


def test_parquet_arrow_source_chunk_provider_opens_bounded_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Parquet Arrow-source chunks are opened lazily and closed between chunks."""
    from schema_sanitizer.api_impl.parquet import arrow_sources as parquet_arrow_sources
    from schema_sanitizer.api_impl.parquet.arrow_sources import ParquetArrowSource

    opened: list[str] = []
    closed: list[str] = []

    class Factory:
        """Fake Arrow stream factory with close tracking."""

        def __init__(self, path: str) -> None:
            """Store the source path."""
            self.path = path
            self.schema = object()

        def close(self) -> None:
            """Record factory closure."""
            closed.append(self.path)

    def fake_factory_or_none(data, **_kwargs):
        """Return a fake factory and record lazy open order."""
        opened.append(data)
        return Factory(data)

    monkeypatch.setattr(
        parquet_arrow_sources,
        "parquet_arrow_stream_factory_or_none",
        fake_factory_or_none,
    )
    monkeypatch.setattr(
        parquet_arrow_sources,
        "parquet_arrow_source_chunk_size",
        lambda _options: 2,
    )
    sources = [ParquetArrowSource(f"file-{idx}.parquet", "path", f"file-{idx}") for idx in range(5)]
    provider = parquet_arrow_sources.ParquetArrowSourceChunkProvider(
        sources,
        call_options=None,
        feature="test",
    )

    first = provider.next_sources()
    assert [source_file for _factory, source_file in first or []] == ["file-0", "file-1"]
    assert opened == ["file-0.parquet", "file-1.parquet"]
    assert closed == []

    second = provider.next_sources()
    assert [source_file for _factory, source_file in second or []] == ["file-2", "file-3"]
    assert opened == ["file-0.parquet", "file-1.parquet", "file-2.parquet", "file-3.parquet"]
    assert closed == ["file-1.parquet", "file-0.parquet"]

    third = provider.next_sources()
    assert [source_file for _factory, source_file in third or []] == ["file-4"]
    assert opened == [
        "file-0.parquet",
        "file-1.parquet",
        "file-2.parquet",
        "file-3.parquet",
        "file-4.parquet",
    ]
    assert closed == ["file-1.parquet", "file-0.parquet", "file-3.parquet", "file-2.parquet"]

    assert provider.next_sources() is None
    assert closed == [
        "file-1.parquet",
        "file-0.parquet",
        "file-3.parquet",
        "file-2.parquet",
        "file-4.parquet",
    ]


def test_file_and_analytical_functions_share_keyword_contract() -> None:
    """Verify all seven public converters expose the same input/options contract."""
    import inspect

    analytical = set(inspect.signature(ss.to_pyarrow).parameters) - {"input_path"}
    parquet_output_options = {"parquet_compression", "parquet_gzip_level"}
    for function in (ss.to_pandas, ss.to_polars, ss.to_duckdb):
        assert set(inspect.signature(function).parameters) - {"input_path"} == analytical
    for function in (ss.to_csv, ss.to_jsonl):
        assert (
            set(inspect.signature(function).parameters)
            - {
                "input_path",
                "output_path",
            }
            == analytical
        )
    assert (
        set(inspect.signature(ss.to_parquet).parameters)
        - {
            "input_path",
            "output_path",
        }
        == analytical | parquet_output_options
    )
