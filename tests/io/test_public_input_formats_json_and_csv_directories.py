"""Tests explicit public input formats, extensions, and directory mode.

It covers explicit format and suffix validation, JSON arrays, directory coalescing,
source tracking, CSV headers, and malformed input cleanup.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from _support.public_input_modes import data_rows as _data_rows

import schema_sanitizer as ss


def test_none_input_format_is_rejected(tmp_path: Path) -> None:
    """Verify none input format is rejected."""
    path = tmp_path / "rows.jsonl"
    path.write_text('{"a":1}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="input_format is required"):
        ss.to_pyarrow(path)


@pytest.mark.parametrize(
    ("input_format", "failure"),
    [("jsonl", MemoryError), (" parquet ", KeyboardInterrupt)],
)
def test_format_normalization_fault_precedes_prepared_ownership(
    monkeypatch: pytest.MonkeyPatch,
    input_format: str,
    failure: type[BaseException],
) -> None:
    """Format validation fails before an input owner can be acquired."""
    from schema_sanitizer.api_impl.input import preparation as public_input
    from schema_sanitizer.input_impl.prepared import PreparedPublicInput

    acquired: list[object] = []

    class Keepalive:
        """Track whether an acquired preparation owner is closed."""

        def __init__(self) -> None:
            """Initialize keepalive state for closed."""
            self.closed = False

        def close(self) -> None:
            """Close the keepalive and update closed."""
            self.closed = True

    def materialize(*_args: object, **_kwargs: object) -> PreparedPublicInput:
        """Materialize the prepared fake result."""
        owner = Keepalive()
        acquired.append(owner)
        return PreparedPublicInput(b"{}\n", "jsonl", "stream", owner)

    def fail_normalization(_input_format: str | None) -> str:
        """Raise the normalization error used to verify ownership cleanup."""
        raise failure("injected format-normalization fault")

    monkeypatch.setattr(public_input, "_prepare_public_input_impl", materialize)
    monkeypatch.setattr(public_input, "normalize_public_input_format", fail_normalization)

    with pytest.raises(failure, match="format-normalization fault"):
        public_input.prepare_public_input(
            "unused.jsonl",
            input_format=input_format,
            input_mode="single_file",
            input_text_encoding="utf-8",
            xml_row_tag=None,
            csv_delimiter=",",
            csv_has_header=True,
            memory_limit_bytes=None,
        )

    assert acquired == []


@pytest.mark.parametrize(
    ("input_format", "filename"),
    [
        ("csv", "rows.jsonl"),
        ("json", "rows.jsonl"),
        ("json_array", "rows.jsonl"),
        ("jsonl", "rows.json"),
        ("xml", "rows.json"),
        ("parquet", "rows.json"),
    ],
)
def test_single_file_requires_matching_extension(
    tmp_path: Path, input_format: str, filename: str
) -> None:
    """Verify single file requires matching extension."""
    path = tmp_path / filename
    path.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="requires extension"):
        ss.to_pyarrow(path, input_format=input_format)


def test_json_array_materializes_top_level_objects(tmp_path: Path) -> None:
    """Verify JSON array materializes top level objects."""
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
    """Verify JSON array single file uses native path source."""
    from schema_sanitizer.api_impl.input.preparation import prepare_public_input

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
    """Verify JSON array rejects non object elements."""
    path = tmp_path / "rows.json"
    path.write_text('[{"id":1}, 2]\n', encoding="utf-8")

    with pytest.raises(ValueError, match="object elements"):
        ss.to_pyarrow(path, input_format="json_array")


def test_json_array_rejects_trailing_characters(tmp_path: Path) -> None:
    """Verify JSON array rejects trailing characters."""
    path = tmp_path / "rows.json"
    path.write_text('[{"id":1}] trailing\n', encoding="utf-8")

    with pytest.raises(ValueError, match="trailing characters"):
        ss.to_pyarrow(path, input_format="json_array")


def test_json_top_level_array_rejects_trailing_characters(tmp_path: Path) -> None:
    """Verify JSON top level array rejects trailing characters."""
    path = tmp_path / "rows.json"
    path.write_text('[{"id":1}] trailing\n', encoding="utf-8")

    with pytest.raises(ValueError, match="trailing characters"):
        ss.to_pyarrow(path, input_format="json")


def test_json_wide_top_level_rows_materialize_all_fields(tmp_path: Path) -> None:
    """Verify JSON wide top level rows materialize all fields."""
    path = tmp_path / "rows.json"
    path.write_text(
        '[{"a":1,"b":2,"c":3,"d":4,"e":5,"f":6,"g":7,"h":8}]',
        encoding="utf-8",
    )

    assert _data_rows(ss.to_pyarrow(path, input_format="json")) == [
        {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8}
    ]


def test_json_directory_source_file_tracks_multirow_documents(tmp_path: Path) -> None:
    """Verify JSON directory source file tracks multirow documents."""
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
    """Verify JSONL directory coalesces with native source file tracking."""
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
    """Verify JSON directory coalesces object documents with source file tracking."""
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
    """Verify JSON directory coalesces array documents with source file tracking."""
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
    """Verify JSON directory preserves scalar array documents."""
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
    """Verify JSON directory scalar array documents preserve values with registry."""
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
    """Verify native JSON path source probe coalesces with best effort fallback."""
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

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
    """Verify native JSON probe and materialization share array boundary plan."""
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

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
    """Verify native path source probe state feeds materialization."""
    from schema_sanitizer.api_impl.execution_context import ExecutionContext
    from schema_sanitizer.api_impl.streams import Stream

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


def test_jsonl_materializes_line_delimited_objects(tmp_path: Path) -> None:
    """Verify JSONL materializes line delimited objects."""
    path = tmp_path / "rows.jsonl"
    path.write_text('{"a":1}\n{"a":2}\n', encoding="utf-8")

    assert _data_rows(ss.to_pyarrow(path, input_format="jsonl")) == [{"a": 1}, {"a": 2}]


def test_directory_mode_is_non_recursive_and_deterministic(tmp_path: Path) -> None:
    """Verify directory mode is non recursive and deterministic."""
    folder = tmp_path / "rows"
    folder.mkdir()
    (folder / "b.jsonl").write_text('{"a":2}\n', encoding="utf-8")
    (folder / "a.jsonl").write_text('{"a":1}\n', encoding="utf-8")
    nested = folder / "nested"
    nested.mkdir()
    (nested / "ignored.jsonl").write_text('{"a":99}\n', encoding="utf-8")
    (folder / "ignored.txt").write_text('{"a":98}\n', encoding="utf-8")

    result = ss.to_pyarrow(folder, input_format="jsonl", input_mode="directory")
    assert _data_rows(result) == [{"a": 1}, {"a": 2}]
    assert [row["source_file"] for row in result.clean_data.to_pylist()] == [
        str((folder / "a.jsonl").resolve()),
        str((folder / "b.jsonl").resolve()),
    ]


def test_directory_mode_requires_explicit_format(tmp_path: Path) -> None:
    """Verify directory mode requires explicit format."""
    folder = tmp_path / "rows"
    folder.mkdir()

    with pytest.raises(ValueError, match="input_format is required"):
        ss.to_pyarrow(folder, input_mode="directory")


def test_csv_directory_validates_shared_header(tmp_path: Path) -> None:
    """Verify CSV directory validates shared header."""
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
