"""Tests explicit public input formats, extensions, and directory mode."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from public_input_modes_shared import GENERATED_COLUMNS as GENERATED
from public_input_modes_shared import data_rows as _data_rows

import schema_sanitizer as ss

# Split from test_public_input_modes.py: test_csv_directory_source_file_spans_count_quoted_newlines, test_native_csv_path_source_probe_coalesces_and_skips_child_headers, test_csv_directory_source_file_does_not_precount_rows, ...


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
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

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
    from schema_sanitizer.input_impl.source_plan import (
        last_native_multisource_route,
    )

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
    from schema_sanitizer.input_impl.source_plan import (
        last_native_multisource_route,
    )

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
    import schema_sanitizer.api_impl.analytical as analytical_conversion
    import schema_sanitizer.input_impl.source_plan as source_plan_model
    from schema_sanitizer.api_impl.source_plan import registry as source_plan_registry_stream

    folder = tmp_path / "jsonl-shared-open"
    folder.mkdir()
    (folder / "a.jsonl").write_text('{"id":1}\n', encoding="utf-8")
    (folder / "b.jsonl").write_text('{"id":2}\n', encoding="utf-8")

    calls: list[str] = []
    real_open = source_plan_registry_stream.open_source_plan_registry_stream

    def tracking_open(raw_context, plan, call_options, **kwargs):
        """Track source-plan opener use while preserving real behavior."""
        calls.append(plan.kind)
        return real_open(raw_context, plan, call_options, **kwargs)

    monkeypatch.setattr(
        source_plan_registry_stream, "open_source_plan_registry_stream", tracking_open
    )
    monkeypatch.setattr(analytical_conversion, "open_source_plan_registry_stream", tracking_open)

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
    assert calls == [source_plan_model.PATH_SOURCES, source_plan_model.PATH_SOURCES]


def test_directory_file_conversion_uses_core_source_plan_fast_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify public file conversion opens source plans before writer fallback."""
    pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import registry_output as registry_file_execution

    folder = tmp_path / "jsonl-core-source-plan"
    folder.mkdir()
    (folder / "a.jsonl").write_text('{"id":1}\n', encoding="utf-8")
    out = tmp_path / "core-source-plan.parquet"

    def fail_writer_fallback(*_args: object, **_kwargs: object) -> None:
        """Fail if source-plan conversion reaches the older writer-level branch."""
        raise AssertionError("source-plan file conversion should be handled in core")

    monkeypatch.setattr(registry_file_execution, "_write_registry_file", fail_writer_fallback)

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
    from schema_sanitizer.core_impl.schema_registry import native_registry_state_context

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
    with native_registry_state_context(first_result.native_registry_state):
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
    from schema_sanitizer.input_impl.source_plan import (
        last_native_multisource_route,
    )

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
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

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
    from schema_sanitizer.input_impl.source_plan import (
        last_native_multisource_route,
    )

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
    from schema_sanitizer.input_impl.source_plan import (
        last_native_multisource_route,
    )

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
    from schema_sanitizer.input_impl.source_plan import (
        last_native_multisource_route,
    )

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
    from schema_sanitizer.input_impl.source_plan import (
        last_native_multisource_route,
    )

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
