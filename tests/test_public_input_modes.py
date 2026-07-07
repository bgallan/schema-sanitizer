"""Tests explicit public input formats, extensions, and directory mode."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import require_native

import schema_sanitizer as ss

GENERATED = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}


def _data_rows(result) -> list[dict]:
    """Return analytical rows without generated metadata columns."""
    return [
        {key: value for key, value in row.items() if key not in GENERATED}
        for row in result.clean_data.to_pylist()
    ]


def test_auto_input_format_is_rejected(tmp_path: Path) -> None:
    """Verify the removed auto selector is not accepted."""
    path = tmp_path / "rows.jsonl"
    path.write_text('{"a":1}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="auto"):
        ss.to_pyarrow(path, input_format="auto")


def test_none_input_format_is_rejected(tmp_path: Path) -> None:
    """Verify the default None selector never infers from the extension."""
    path = tmp_path / "rows.jsonl"
    path.write_text('{"a":1}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="input_format is required"):
        ss.to_pyarrow(path)


@pytest.mark.parametrize(
    ("input_format", "filename"),
    [
        ("csv", "rows.jsonl"),
        ("json", "rows.jsonl"),
        ("json_array", "rows.jsonl"),
        ("jsonl", "rows.ndjson"),
        ("ndjson", "rows.jsonl"),
        ("xml", "rows.json"),
        ("parquet", "rows.json"),
    ],
)
def test_single_file_requires_matching_extension(
    tmp_path: Path, input_format: str, filename: str
) -> None:
    """Verify every explicit format checks its source extension."""
    path = tmp_path / filename
    path.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="requires extension"):
        ss.to_pyarrow(path, input_format=input_format)


def test_json_array_materializes_top_level_objects(tmp_path: Path) -> None:
    """Verify json_array emits one row per top-level object."""
    path = tmp_path / "rows.json"
    path.write_text(
        '[\n  {"id":1,"name":"Ana"},\n  {"id":2,"name":"Luis"}\n]\n',
        encoding="utf-8",
    )

    assert _data_rows(ss.to_pyarrow(path, input_format="json_array")) == [
        {"id": 1, "name": "Ana"},
        {"id": 2, "name": "Luis"},
    ]


def test_json_array_single_file_uses_native_path_source(tmp_path: Path) -> None:
    """Verify single-file json_array remains native path input."""
    from schema_sanitizer.api_impl.public_input import prepare_public_input

    path = tmp_path / "rows.json"
    path.write_text('[{"id":1},{"id":2}]', encoding="utf-8")

    prepared = prepare_public_input(
        path,
        input_format="json_array",
        input_mode="single_file",
        input_text_encoding="utf-8",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
    )

    assert prepared.source == "path"
    assert prepared.format == "json_array"
    assert _data_rows(ss.to_pyarrow(path, input_format="json_array")) == [
        {"id": 1},
        {"id": 2},
    ]


def test_json_array_rejects_non_object_elements(tmp_path: Path) -> None:
    """Verify json_array only accepts object rows."""
    path = tmp_path / "rows.json"
    path.write_text('[{"id":1}, 2]\n', encoding="utf-8")

    with pytest.raises(ValueError, match="object elements"):
        ss.to_pyarrow(path, input_format="json_array")


def test_json_array_rejects_trailing_characters(tmp_path: Path) -> None:
    """Verify json_array rejects non-whitespace after the top-level array."""
    path = tmp_path / "rows.json"
    path.write_text('[{"id":1}] trailing\n', encoding="utf-8")

    with pytest.raises(ValueError, match="trailing characters"):
        ss.to_pyarrow(path, input_format="json_array")


def test_json_top_level_array_rejects_trailing_characters(tmp_path: Path) -> None:
    """Verify native JSON array scanning validates the document tail."""
    path = tmp_path / "rows.json"
    path.write_text('[{"id":1}] trailing\n', encoding="utf-8")

    with pytest.raises(ValueError, match="trailing characters"):
        ss.to_pyarrow(path, input_format="json")


def test_json_wide_top_level_rows_materialize_all_fields(tmp_path: Path) -> None:
    """Verify wide top-level rows materialize through the native root snapshot."""
    path = tmp_path / "rows.json"
    path.write_text(
        '[{"a":1,"b":2,"c":3,"d":4,"e":5,"f":6,"g":7,"h":8}]',
        encoding="utf-8",
    )

    assert _data_rows(ss.to_pyarrow(path, input_format="json")) == [
        {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8}
    ]


def test_json_directory_source_file_tracks_multirow_documents(tmp_path: Path) -> None:
    """Verify native JSON directory source_file tracking remains per produced row."""
    folder = tmp_path / "json"
    folder.mkdir()
    (folder / "a.json").write_text('[{"id":1},{"id":2}]', encoding="utf-8")
    (folder / "b.json").write_text('{"id":3}', encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="json", input_mode="directory")

    assert _data_rows(result) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.json").resolve()),
        str((folder / "a.json").resolve()),
        str((folder / "b.json").resolve()),
    ]
    assert result.stats["batches"] == 2


def test_jsonl_directory_coalesces_with_native_source_file_tracking(tmp_path: Path) -> None:
    """Verify JSONL directory materialization groups files without losing row origins."""
    folder = tmp_path / "jsonl"
    folder.mkdir()
    (folder / "a.jsonl").write_text('{"id":1}\n{"id":2}\n', encoding="utf-8")
    (folder / "b.jsonl").write_text('{"id":3}\n', encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="jsonl", input_mode="directory")

    assert _data_rows(result) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.jsonl").resolve()),
        str((folder / "a.jsonl").resolve()),
        str((folder / "b.jsonl").resolve()),
    ]
    assert result.stats["inferred_rows"] == 3
    assert result.stats["materialized_rows"] == 3
    assert result.stats["batches"] == 1


def test_json_directory_coalesces_object_documents_with_source_file_tracking(
    tmp_path: Path,
) -> None:
    """Verify object-style JSON documents can share one native stream."""
    folder = tmp_path / "json"
    folder.mkdir()
    (folder / "a.json").write_text('{"id":1}', encoding="utf-8")
    (folder / "b.json").write_text('{"id":2}', encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="json", input_mode="directory")

    assert _data_rows(result) == [{"id": 1}, {"id": 2}]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.json").resolve()),
        str((folder / "b.json").resolve()),
    ]
    assert result.stats["batches"] == 1


def test_json_directory_coalesces_array_documents_with_source_file_tracking(
    tmp_path: Path,
) -> None:
    """Verify JSON array documents can share one native stream in json mode."""
    folder = tmp_path / "json"
    folder.mkdir()
    (folder / "a.json").write_text('[{"id":1},{"id":2}]', encoding="utf-8")
    (folder / "b.json").write_text('[{"id":3}]', encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="json", input_mode="directory")

    assert _data_rows(result) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.json").resolve()),
        str((folder / "a.json").resolve()),
        str((folder / "b.json").resolve()),
    ]
    assert result.stats["batches"] == 1


def test_json_directory_preserves_scalar_array_documents(tmp_path: Path) -> None:
    """Verify json-mode scalar array documents keep default-key semantics."""
    folder = tmp_path / "json"
    folder.mkdir()
    (folder / "a.json").write_text("[1,2]", encoding="utf-8")
    (folder / "b.json").write_text("[3]", encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="json", input_mode="directory")

    assert _data_rows(result) == [
        {"default_key": 1},
        {"default_key": 2},
        {"default_key": 3},
    ]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.json").resolve()),
        str((folder / "a.json").resolve()),
        str((folder / "b.json").resolve()),
    ]
    assert result.stats["batches"] == 2


def test_json_directory_scalar_array_documents_preserve_values_with_registry(
    tmp_path: Path,
) -> None:
    """Verify scalar array documents keep default-key values with an existing registry."""
    seed = tmp_path / "seed.json"
    seed.write_text("1", encoding="utf-8")
    registry = ss.to_pyarrow(seed, input_format="json").schema_registry

    folder = tmp_path / "json"
    folder.mkdir()
    (folder / "a.json").write_text("[1,2]", encoding="utf-8")
    (folder / "b.json").write_text("[3]", encoding="utf-8")

    result = ss.to_pyarrow(
        folder,
        input_format="json",
        input_mode="directory",
        schema_registry=registry,
    )

    assert _data_rows(result) == [
        {"default_key": 1},
        {"default_key": 2},
        {"default_key": 3},
    ]
    assert result.stats["batches"] == 2


def test_native_json_path_source_probe_coalesces_with_best_effort_fallback(
    tmp_path: Path,
) -> None:
    """Verify grouped JSON registry probing falls back to per-file invalid skips."""
    from schema_sanitizer.api_impl.context import ExecutionContext

    folder = tmp_path / "json"
    folder.mkdir()
    (folder / "a.json").write_text('{"id":1}', encoding="utf-8")
    (folder / "bad.json").write_text("{bad", encoding="utf-8")
    (folder / "b.json").write_text('{"name":"Ada"}', encoding="utf-8")
    sources = [
        ("json", str(path), str(path))
        for path in (folder / "a.json", folder / "bad.json", folder / "b.json")
    ]

    probe = ExecutionContext()._raw.registry_probe_path_sources_best_effort(
        sources,
        None,
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )

    registry = json.loads(probe.schema_registry_json)
    assert [field["name"] for field in registry["canonical_schema"]["fields"]] == [
        "id",
        "name",
    ]
    assert json.loads(probe.diagnostics.to_json())["inferred_rows"] == 2


def test_native_json_probe_and_materialization_share_array_boundary_plan(
    tmp_path: Path,
) -> None:
    """Verify JSON array documents and object documents use one source-plan rule."""
    from schema_sanitizer.api_impl.context import ExecutionContext

    folder = tmp_path / "json"
    folder.mkdir()
    (folder / "a.json").write_text('[{"id":1},{"id":2}]', encoding="utf-8")
    (folder / "b.json").write_text('{"name":"Ada"}', encoding="utf-8")
    (folder / "c.json").write_text('{"score":3}', encoding="utf-8")
    paths = [folder / "a.json", folder / "b.json", folder / "c.json"]
    sources = [("json", str(path), str(path)) for path in paths]

    probe = ExecutionContext()._raw.registry_probe_path_sources(
        sources,
        None,
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )
    result = ss.to_pyarrow(folder, input_format="json", input_mode="directory")

    registry = json.loads(probe.schema_registry_json)
    assert {field["name"] for field in registry["canonical_schema"]["fields"]} >= {
        "id",
        "name",
        "score",
    }
    assert _data_rows(result) == [
        {"id": 1, "name": None, "score": None},
        {"id": 2, "name": None, "score": None},
        {"id": None, "name": "Ada", "score": None},
        {"id": None, "name": None, "score": 3},
    ]
    assert json.loads(probe.diagnostics.to_json())["inferred_rows"] == 4


def test_native_path_source_probe_state_feeds_materialization(tmp_path: Path) -> None:
    """Verify two-phase native materialization reuses compiled registry state."""
    from schema_sanitizer.api_impl.context import ExecutionContext
    from schema_sanitizer.api_impl.ingest_runtime_types import Stream

    folder = tmp_path / "json"
    folder.mkdir()
    source = folder / "a.json"
    source.write_text('{"id":1}', encoding="utf-8")
    sources = [("json", str(source), str(source))]
    ctx = ExecutionContext()

    probe = ctx._raw.registry_probe_path_sources(
        sources,
        None,
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )
    assert probe.native_registry_state is not None

    raw = ctx._raw.to_registry_sink_path_sources(
        "stream",
        sources,
        None,
        registry_json=probe.schema_registry_json,
        drifts_json=probe.schema_drifts_json,
        conversion_timestamp=probe.conversion_timestamp,
        field_name_policy="lower_snake",
        schema_mode="additive",
        first_row_columns={},
        timestamp_columns=(),
        native_registry_state=probe.native_registry_state,
    )
    assert raw.native_registry_state is not None

    with Stream(raw) as stream:
        assert stream.to_table().to_pylist() == [{"id": 1, "source_file": str(source)}]


@pytest.mark.parametrize(("input_format", "suffix"), [("jsonl", ".jsonl"), ("ndjson", ".ndjson")])
def test_line_delimited_json_formats_share_content_model(
    tmp_path: Path, input_format: str, suffix: str
) -> None:
    """Verify JSONL and NDJSON differ only by extension validation."""
    path = tmp_path / f"rows{suffix}"
    path.write_text('{"a":1}\n{"a":2}\n', encoding="utf-8")

    assert _data_rows(ss.to_pyarrow(path, input_format=input_format)) == [{"a": 1}, {"a": 2}]


def test_directory_mode_is_non_recursive_and_deterministic(tmp_path: Path) -> None:
    """Verify directory mode reads matching direct children only."""
    folder = tmp_path / "rows"
    folder.mkdir()
    (folder / "b.jsonl").write_text('{"a":2}\n', encoding="utf-8")
    (folder / "a.jsonl").write_text('{"a":1}\n', encoding="utf-8")
    nested = folder / "nested"
    nested.mkdir()
    (nested / "ignored.jsonl").write_text('{"a":99}\n', encoding="utf-8")
    (folder / "ignored.ndjson").write_text('{"a":98}\n', encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="jsonl", input_mode="directory")
    assert _data_rows(result) == [{"a": 1}, {"a": 2}]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.jsonl").resolve()),
        str((folder / "b.jsonl").resolve()),
    ]


def test_directory_mode_requires_explicit_format(tmp_path: Path) -> None:
    """Verify directory mode also rejects the default None format."""
    folder = tmp_path / "rows"
    folder.mkdir()

    with pytest.raises(ValueError, match="input_format is required"):
        ss.to_pyarrow(folder, input_mode="directory")


def test_csv_directory_validates_shared_header(tmp_path: Path) -> None:
    """Verify CSV directory mode removes matching repeated headers."""
    folder = tmp_path / "csv"
    folder.mkdir()
    (folder / "a.csv").write_text("id,name\n1,Ana\n", encoding="utf-8")
    (folder / "b.csv").write_text("id,name\n2,Luis\n", encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="csv", input_mode="directory")
    assert _data_rows(result) == [
        {"id": "1", "name": "Ana"},
        {"id": "2", "name": "Luis"},
    ]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.csv").resolve()),
        str((folder / "b.csv").resolve()),
    ]

    (folder / "b.csv").write_text("id,title\n2,Luis\n", encoding="utf-8")
    with pytest.raises(ValueError, match="header mismatch"):
        ss.to_pyarrow(folder, input_format="csv", input_mode="directory")


def test_csv_directory_source_file_spans_count_quoted_newlines(tmp_path: Path) -> None:
    """Verify CSV directory source_file spans follow parsed rows, not physical lines."""
    folder = tmp_path / "csv"
    folder.mkdir()
    (folder / "a.csv").write_text('id,note\n1,"hello\nworld"\n', encoding="utf-8")
    (folder / "b.csv").write_text("id,note\n2,two\n", encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="csv", input_mode="directory")

    assert _data_rows(result) == [
        {"id": "1", "note": "hello\nworld"},
        {"id": "2", "note": "two"},
    ]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.csv").resolve()),
        str((folder / "b.csv").resolve()),
    ]
    assert result.stats["batches"] == 1


def test_native_csv_path_source_probe_coalesces_and_skips_child_headers(
    tmp_path: Path,
) -> None:
    """Verify grouped CSV registry probing treats each child header as metadata."""
    from schema_sanitizer.api_impl.context import ExecutionContext

    folder = tmp_path / "csv"
    folder.mkdir()
    (folder / "a.csv").write_text("id,name\n1,Ana\n", encoding="utf-8")
    (folder / "b.csv").write_text("id,name\n2,Bia\n", encoding="utf-8")
    sources = [("csv", str(path), str(path)) for path in (folder / "a.csv", folder / "b.csv")]

    probe = ExecutionContext()._raw.registry_probe_path_sources(
        sources,
        None,
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )

    registry = json.loads(probe.schema_registry_json)
    assert [field["name"] for field in registry["canonical_schema"]["fields"]] == [
        "id",
        "name",
    ]
    assert json.loads(probe.diagnostics.to_json())["inferred_rows"] == 2


def test_csv_directory_source_file_does_not_precount_rows(tmp_path: Path) -> None:
    """Verify native multi-source directory conversion owns source_file tracking."""
    from schema_sanitizer.api_impl.source_plan import last_native_multisource_route

    folder = tmp_path / "csv"
    folder.mkdir()
    (folder / "a.csv").write_text('id,note\n1,"hello\nworld"\n', encoding="utf-8")
    (folder / "b.csv").write_text("id,note\n2,two\n", encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="csv", input_mode="directory")

    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.csv").resolve()),
        str((folder / "b.csv").resolve()),
    ]
    assert last_native_multisource_route() == "cxx_path_sources"


def test_csv_directory_writer_source_file_does_not_precount_rows(tmp_path: Path) -> None:
    """Verify file writers use native multi-source source_file tracking."""
    from schema_sanitizer.api_impl.source_plan import last_native_multisource_route

    folder = tmp_path / "csv"
    folder.mkdir()
    (folder / "a.csv").write_text("id,note\n1,one\n", encoding="utf-8")
    (folder / "b.csv").write_text("id,note\n2,two\n", encoding="utf-8")
    out = tmp_path / "out.parquet"

    result = ss.to_parquet(folder, out, input_format="csv", input_mode="directory")

    assert result.schema_registry is not None
    table = pytest.importorskip("pyarrow.parquet").read_table(out)
    assert table.column("source_file").to_pylist() == [
        str((folder / "a.csv").resolve()),
        str((folder / "b.csv").resolve()),
    ]
    assert result.stats["inferred_rows"] == 2
    assert result.stats["materialized_rows"] == 2
    assert result.stats["batches"] >= 1
    assert last_native_multisource_route() == "cxx_path_sources"


def test_jsonl_directory_file_writer_reports_nonzero_stats(tmp_path: Path) -> None:
    """Verify native JSONL file output patches row stats after streaming."""
    folder = tmp_path / "jsonl"
    folder.mkdir()
    (folder / "a.jsonl").write_text('{"id":1}\n{"id":2}\n', encoding="utf-8")
    (folder / "b.jsonl").write_text('{"id":3}\n', encoding="utf-8")
    out = tmp_path / "out.jsonl"

    result = ss.to_jsonl(folder, out, input_format="jsonl", input_mode="directory")

    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert [{key: value for key, value in row.items() if key not in GENERATED} for row in rows] == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]
    assert result.stats["inferred_rows"] == 3
    assert result.stats["materialized_rows"] == 3
    assert result.stats["batches"] >= 1


def test_csv_directory_file_writer_reports_logical_row_stats(tmp_path: Path) -> None:
    """Verify native CSV file output counts logical CSV records, not physical lines."""
    folder = tmp_path / "csv"
    folder.mkdir()
    (folder / "a.csv").write_text('id,note\n1,"hello\nworld"\n', encoding="utf-8")
    (folder / "b.csv").write_text("id,note\n2,two\n", encoding="utf-8")
    out = tmp_path / "out.csv"

    result = ss.to_csv(folder, out, input_format="csv", input_mode="directory")

    assert result.stats["inferred_rows"] == 2
    assert result.stats["materialized_rows"] == 2
    assert result.stats["batches"] >= 1


def test_registry_directory_analysis_and_writer_share_source_plan_opener(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Verify analytical and file registry routes use the same source-plan opener."""
    pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import analytical_core, source_plan

    folder = tmp_path / "jsonl-shared-open"
    folder.mkdir()
    (folder / "a.jsonl").write_text('{"id":1}\n', encoding="utf-8")
    (folder / "b.jsonl").write_text('{"id":2}\n', encoding="utf-8")

    calls: list[tuple[str, str]] = []
    real_open = source_plan.open_source_plan_registry_stream

    def tracking_open(raw_context, plan, call_options, **kwargs):
        """Track source-plan opener use while preserving real behavior."""
        calls.append((plan.kind, kwargs["feature"]))
        return real_open(raw_context, plan, call_options, **kwargs)

    monkeypatch.setattr(source_plan, "open_source_plan_registry_stream", tracking_open)
    monkeypatch.setattr(analytical_core, "open_source_plan_registry_stream", tracking_open)

    result = ss.to_pyarrow(
        folder,
        input_format="jsonl",
        input_mode="directory",
        schema_registry={},
    )
    out = tmp_path / "shared-open.parquet"
    ss.to_parquet(
        folder,
        out,
        input_format="jsonl",
        input_mode="directory",
        schema_registry={},
    )

    assert _data_rows(result) == [{"id": 1}, {"id": 2}]
    assert calls == [
        (source_plan.PATH_SOURCES, "to_pyarrow"),
        (source_plan.PATH_SOURCES, "to_parquet"),
    ]


def test_directory_file_conversion_uses_core_source_plan_fast_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify public file conversion opens source plans before writer fallback."""
    pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import registry_file_writers

    folder = tmp_path / "jsonl-core-source-plan"
    folder.mkdir()
    (folder / "a.jsonl").write_text('{"id":1}\n', encoding="utf-8")
    out = tmp_path / "core-source-plan.parquet"

    def fail_writer_fallback(*_args: object, **_kwargs: object) -> None:
        """Fail if source-plan conversion reaches the older writer-level branch."""
        raise AssertionError("source-plan file conversion should be handled in core")

    monkeypatch.setattr(registry_file_writers, "_write_registry_to_file", fail_writer_fallback)

    ss.to_parquet(
        folder,
        out,
        input_format="jsonl",
        input_mode="directory",
        schema_registry={},
    )

    assert out.exists()


def test_jsonl_directory_writer_accepts_previous_native_registry_state(
    tmp_path: Path,
) -> None:
    """Verify path-source writers can seed inference from native registry state."""
    from schema_sanitizer.api_impl.file_convert_core import schema_registry_native_state_context

    pq = pytest.importorskip("pyarrow.parquet")

    first = tmp_path / "first"
    first.mkdir()
    (first / "a.jsonl").write_text('{"id":1}\n', encoding="utf-8")
    out_first = tmp_path / "first.parquet"
    first_result = ss.to_parquet(first, out_first, input_format="jsonl", input_mode="directory")

    assert first_result.native_registry_state is not None

    second = tmp_path / "second"
    second.mkdir()
    (second / "b.jsonl").write_text('{"id":2,"name":"Ada"}\n', encoding="utf-8")
    out_second = tmp_path / "second.parquet"
    with schema_registry_native_state_context(first_result.native_registry_state):
        second_result = ss.to_parquet(
            second,
            out_second,
            input_format="jsonl",
            input_mode="directory",
            schema_registry=first_result.schema_registry_json,
        )

    assert second_result.native_registry_state is not None
    rows = pq.read_table(out_second).to_pylist()
    assert [{"id": row["id"], "name": row["name"]} for row in rows] == [{"id": 2, "name": "Ada"}]


def test_json_array_directory_flattens_each_file(tmp_path: Path) -> None:
    """Verify directory json_array mode flattens all direct arrays."""
    from schema_sanitizer.api_impl.source_plan import last_native_multisource_route

    folder = tmp_path / "arrays"
    folder.mkdir()
    (folder / "a.json").write_text('[{"a":1},{"a":2}]', encoding="utf-8")
    (folder / "b.json").write_text('[{"a":3}]', encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="json_array", input_mode="directory")
    assert _data_rows(result) == [
        {"a": 1},
        {"a": 2},
        {"a": 3},
    ]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.json").resolve()),
        str((folder / "a.json").resolve()),
        str((folder / "b.json").resolve()),
    ]
    assert result.stats["batches"] == 1
    assert last_native_multisource_route() == "cxx_path_sources"


def test_native_json_array_path_source_probe_coalesces_each_array_file(
    tmp_path: Path,
) -> None:
    """Verify grouped json_array probing scans each file as its own array."""
    from schema_sanitizer.api_impl.context import ExecutionContext

    folder = tmp_path / "arrays"
    folder.mkdir()
    (folder / "a.json").write_text('[{"id":1},{"id":2}]', encoding="utf-8")
    (folder / "b.json").write_text('[{"score":3.5}]', encoding="utf-8")
    sources = [
        ("json_array", str(path), str(path)) for path in (folder / "a.json", folder / "b.json")
    ]

    probe = ExecutionContext()._raw.registry_probe_path_sources(
        sources,
        None,
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )

    registry = json.loads(probe.schema_registry_json)
    assert [field["name"] for field in registry["canonical_schema"]["fields"]] == [
        "id",
        "score",
    ]
    assert json.loads(probe.diagnostics.to_json())["inferred_rows"] == 3


def test_json_array_directory_does_not_preconvert_or_precount(tmp_path: Path) -> None:
    """Verify directory json_array source_file tracking is native."""
    from schema_sanitizer.api_impl.source_plan import last_native_multisource_route

    folder = tmp_path / "arrays"
    folder.mkdir()
    (folder / "a.json").write_text('[{"id":1},{"id":2}]', encoding="utf-8")
    (folder / "b.json").write_text('[{"id":3}]', encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="json_array", input_mode="directory")

    assert _data_rows(result) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.json").resolve()),
        str((folder / "a.json").resolve()),
        str((folder / "b.json").resolve()),
    ]
    assert result.stats["batches"] == 1
    assert last_native_multisource_route() == "cxx_path_sources"


def test_xml_directory_source_file_tracks_each_child(tmp_path: Path) -> None:
    """Verify XML directory rows carry the child XML path."""
    from schema_sanitizer.api_impl.source_plan import last_native_multisource_route

    folder = tmp_path / "xml"
    folder.mkdir()
    (folder / "a.xml").write_text('<?xml version="1.0"?><row><id>1</id></row>', encoding="utf-8")
    (folder / "b.xml").write_text("<row><id>2</id></row>", encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="xml", input_mode="directory", xml_row_tag="row")

    assert _data_rows(result) == [{"id": "1"}, {"id": "2"}]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.xml").resolve()),
        str((folder / "b.xml").resolve()),
    ]
    assert last_native_multisource_route() == "cxx_path_sources"


def test_xml_directory_does_not_wrap_child_files(tmp_path: Path) -> None:
    """Verify native XML directory conversion reads child paths directly."""
    from schema_sanitizer.api_impl.source_plan import last_native_multisource_route

    folder = tmp_path / "xml"
    folder.mkdir()
    (folder / "a.xml").write_text('<?xml version="1.0"?><row><id>1</id></row>', encoding="utf-8")
    (folder / "b.xml").write_text("<row><id>2</id></row>", encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="xml", input_mode="directory", xml_row_tag="row")

    assert _data_rows(result) == [{"id": "1"}, {"id": "2"}]
    assert last_native_multisource_route() == "cxx_path_sources"


@pytest.mark.parametrize(
    ("input_format", "expected_ids"),
    [
        ("json", [1, 2]),
        ("json_array", [1, 2]),
        ("jsonl", [1, 2]),
        ("ndjson", [1, 2]),
        ("csv", ["1", "2"]),
        ("xml", ["1", "2"]),
    ],
)
def test_native_directory_preparation_uses_path_sources(
    tmp_path: Path,
    input_format: str,
    expected_ids: list,
) -> None:
    """Verify native-compatible directories prepare source-plan path inputs."""
    from schema_sanitizer.api_impl.source_plan import last_native_multisource_route

    folder = tmp_path / input_format
    folder.mkdir()
    options = {"input_format": input_format, "input_mode": "directory"}
    if input_format == "json":
        (folder / "a.json").write_text('{"id":1}\n', encoding="utf-8")
        (folder / "b.json").write_text('{"id":2}\n', encoding="utf-8")
    elif input_format == "json_array":
        (folder / "a.json").write_text('[{"id":1}]', encoding="utf-8")
        (folder / "b.json").write_text('[{"id":2}]', encoding="utf-8")
    elif input_format == "jsonl":
        (folder / "a.jsonl").write_text('{"id":1}\n', encoding="utf-8")
        (folder / "b.jsonl").write_text('{"id":2}\n', encoding="utf-8")
    elif input_format == "ndjson":
        (folder / "a.ndjson").write_text('{"id":1}\n', encoding="utf-8")
        (folder / "b.ndjson").write_text('{"id":2}\n', encoding="utf-8")
    elif input_format == "csv":
        (folder / "a.csv").write_text("id\n1\n", encoding="utf-8")
        (folder / "b.csv").write_text("id\n2\n", encoding="utf-8")
    else:
        (folder / "a.xml").write_text("<row><id>1</id></row>", encoding="utf-8")
        (folder / "b.xml").write_text("<row><id>2</id></row>", encoding="utf-8")
        options["xml_row_tag"] = "row"

    result = ss.to_pyarrow(folder, **options)

    assert [row["id"] for row in _data_rows(result)] == expected_ids
    assert last_native_multisource_route() == "cxx_path_sources"


def test_native_directory_source_plan_is_native_first(tmp_path: Path) -> None:
    """Verify source plans keep native path-source plans without retaining tuple payloads."""
    require_native()
    from schema_sanitizer.api_impl.public_input import prepare_public_input
    from schema_sanitizer.api_impl.source_plan import source_plan_from_data

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
    from schema_sanitizer.api_impl import source_plan

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
        source_plan,
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
    from schema_sanitizer.api_impl.parquet_multisource import (
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
    from schema_sanitizer.api_impl.parquet_multisource import (
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
    from schema_sanitizer.api_impl.file_convert_core import schema_registry_native_state_context

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
    with schema_registry_native_state_context(warm_result.native_registry_state):
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
    from schema_sanitizer.api_impl import parquet_arrow_sources, public_input, source_plan

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

    plan = source_plan.source_plan_from_prepared_inputs(
        [prepared],
        input_format="parquet",
        input_mode="directory",
        input_text_encoding="utf-8",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
        call_options=None,
    )

    assert plan is not None
    assert plan.kind == source_plan.PARQUET_ARROW_SOURCES
    assert [source.path for source in plan.payload.files] == [
        str(folder / "a.parquet"),
        str(folder / "b.parquet"),
    ]


def test_parquet_arrow_source_chunk_provider_opens_bounded_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Parquet Arrow-source chunks are opened lazily and closed between chunks."""
    from schema_sanitizer.api_impl import parquet_arrow_sources

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
    sources = [
        parquet_arrow_sources.ParquetArrowSource(f"file-{idx}.parquet", "path", f"file-{idx}")
        for idx in range(5)
    ]
    provider = parquet_arrow_sources.ParquetArrowSourceChunkProvider(
        sources,
        call_options=None,
        feature="test",
        chunk_size=2,
    )

    first = provider.next_sources()
    assert [source_file for _factory, source_file in first or []] == ["file-0", "file-1"]
    assert opened == ["file-0.parquet", "file-1.parquet"]
    assert closed == []

    second = provider.next_sources()
    assert [source_file for _factory, source_file in second or []] == ["file-2", "file-3"]
    assert opened == ["file-0.parquet", "file-1.parquet", "file-2.parquet", "file-3.parquet"]
    assert closed == ["file-0.parquet", "file-1.parquet"]

    third = provider.next_sources()
    assert [source_file for _factory, source_file in third or []] == ["file-4"]
    assert opened == [
        "file-0.parquet",
        "file-1.parquet",
        "file-2.parquet",
        "file-3.parquet",
        "file-4.parquet",
    ]
    assert closed == ["file-0.parquet", "file-1.parquet", "file-2.parquet", "file-3.parquet"]

    assert provider.next_sources() is None
    assert closed == [
        "file-0.parquet",
        "file-1.parquet",
        "file-2.parquet",
        "file-3.parquet",
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


def test_file_conversion_core_filters_helper_and_writer_options_before_schema_options(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify internal file conversion ignores non-schema options defensively."""
    from schema_sanitizer.api_impl import file_convert_core
    from schema_sanitizer.api_impl.ingest_runtime_types import Result

    source = tmp_path / "rows.jsonl"
    source.write_text('{"alpha":1}\n', encoding="utf-8")
    captured_options = []

    def fake_normalize(**kwargs):
        """Capture schema option input."""
        captured_options.append(kwargs)
        return None

    def fake_writer(_data, output_path, **_kwargs):
        """Write a marker and return a minimal result."""
        Path(output_path).write_text("ok", encoding="utf-8")
        return Result(SimpleNamespace(diagnostics=None), schema_registry_json="{}")

    monkeypatch.setattr(file_convert_core, "normalize_call_options_or_none", fake_normalize)

    file_convert_core.convert_file_with_options(
        source,
        tmp_path / "out.parquet",
        input_format="jsonl",
        input_mode="single_file",
        options={
            "input_path": source,
            "output_path": tmp_path / "out.parquet",
            "input_format": "jsonl",
            "input_mode": "single_file",
            "field_name_policy": "lower_snake",
            "parquet_compression": "gzip",
            "parquet_gzip_level": 6,
        },
        writer=fake_writer,
        schema_registry={},
        writer_options={"parquet_compression": "gzip", "parquet_gzip_level": 6},
    )

    assert captured_options == [
        {
            "field_name_policy": "lower_snake",
            "schema_contract": None,
            "schema_mode": "additive",
        }
    ]


def test_analytical_core_filters_helper_options_before_schema_options(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify internal analytical conversion ignores helper options defensively."""
    require_native()
    from schema_sanitizer.api_impl import analytical_core

    source = tmp_path / "rows.jsonl"
    source.write_text('{"alpha":1}\n', encoding="utf-8")
    captured_options = []
    real_normalize = analytical_core.normalize_call_options_or_none

    def tracking_normalize(**kwargs):
        """Capture schema option input and preserve behavior."""
        captured_options.append(kwargs)
        return real_normalize(**kwargs)

    monkeypatch.setattr(analytical_core, "normalize_call_options_or_none", tracking_normalize)

    result = analytical_core.convert_analytical_with_options(
        source,
        target="pyarrow",
        input_format="jsonl",
        input_mode="single_file",
        options={
            "input_path": source,
            "target": "pyarrow",
            "input_format": "jsonl",
            "input_mode": "single_file",
            "field_name_policy": "lower_snake",
        },
        schema_registry=None,
    )

    assert _data_rows(result) == [{"alpha": 1}]
    assert captured_options == [
        {
            "field_name_policy": "lower_snake",
            "schema_contract": None,
            "schema_mode": "additive",
        }
    ]


def test_file_converter_accepts_json_array(tmp_path: Path) -> None:
    """Verify JSON-array input works with a file output sink."""
    source = tmp_path / "rows.json"
    output = tmp_path / "rows.jsonl"
    source.write_text('[{"a":1},{"a":2}]', encoding="utf-8")

    ss.to_jsonl(source, output, input_format="json_array")
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert [{key: value for key, value in row.items() if key not in GENERATED} for row in rows] == [
        {"a": 1},
        {"a": 2},
    ]
