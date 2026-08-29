"""Python and local input contract tests.

It covers replayable Python rows, generators and spool limits, native row encoding,
local paths, in-memory payloads, and rejection of unsupported values.
"""

from __future__ import annotations

import io
import json
import signal
from collections.abc import Iterator
from pathlib import Path

import pytest
from conftest import (
    read_test_csv,
    read_test_json,
    read_test_json_folder,
    read_test_jsonl,
    read_test_parquet,
    read_test_python,
    read_test_xml,
    read_test_xml_folder,
)

import schema_sanitizer as ss
from schema_sanitizer.core_impl.execution import PythonRowsJsonlByteReader


def test_list_of_dicts_is_supported() -> None:
    """Verify list of dicts is supported."""
    pytest.importorskip("pyarrow")

    result = read_test_python([{"a": 1}, {"a": 2}])

    assert result.clean_data.to_pylist() == [{"a": 1}, {"a": 2}]


def test_python_rows_jsonl_reader_is_replayable_and_chunked(require_native: None) -> None:
    """Verify python rows JSONL reader is replayable and chunked."""
    reader = PythonRowsJsonlByteReader([{"a": 1}, {"a": "ñ"}])

    first = reader.read(7)
    second = reader.read(1024)
    reader.seek(0)

    assert first == b'{"a":1}'
    assert second == '\n{"a":"ñ"}\n'.encode()
    assert reader.read(1024) == first + second


def test_python_rows_generator_is_spooled_and_replayable(require_native: None) -> None:
    """Verify python rows generator is spooled and replayable."""
    iterations = 0

    def rows() -> Iterator[dict[str, int]]:
        """Yield rows once while tracking generator traversal."""
        nonlocal iterations
        iterations += 1
        yield from ({"a": index} for index in range(5_000))

    reader = PythonRowsJsonlByteReader(rows())
    first = reader.read(257)
    assert iterations == 1
    while reader.read(4096):
        pass
    reader.seek(0)

    assert reader.read(257) == first
    assert iterations == 1


def test_python_rows_generator_spools_incrementally(require_native: None) -> None:
    """A small first read must not consume an entire one-shot iterable."""
    yielded = 0

    def rows() -> Iterator[dict[str, int]]:
        """Track how much of the generator has been requested."""
        nonlocal yielded
        for index in range(10_000):
            yielded += 1
            yield {"a": index}

    reader = PythonRowsJsonlByteReader(rows())
    assert reader.read(32)
    assert 0 < yielded < 10_000
    assert yielded <= reader._MAX_ITERABLE_ROWS_PER_BATCH
    assert reader._iterable_index == yielded
    first_yielded = yielded
    reader.seek(0)
    assert yielded == first_yielded
    assert reader.read(32)
    assert yielded == first_yielded
    while reader.read(1 << 20):
        pass
    assert yielded == 10_000
    reader.close()


def test_python_rows_jsonl_reader_rejects_unsupported_values_without_fallback(
    require_native: None,
) -> None:
    """Verify python rows JSONL reader rejects unsupported values without fallback."""
    reader = PythonRowsJsonlByteReader([{"bad": object()}])

    with pytest.raises(RuntimeError, match="Native Python row JSONL encoding failed"):
        reader.read(1024)


def test_python_rows_reject_non_mapping_rows_in_native_loop(require_native: None) -> None:
    """Verify python rows reject non mapping rows in native loop."""
    reader = PythonRowsJsonlByteReader([{"a": 1}, 2])

    with pytest.raises(TypeError, match="row 1 is not a dict"):
        reader.read(1024)


def test_native_python_row_encoder_matches_reader_payload(require_native: None) -> None:
    """Verify native python row encoder matches reader payload."""
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    payload = _native.python_row_json_bytes({"b": [True, None], "a": "ñ"})

    assert payload == '{"b":[true,null],"a":"ñ"}'.encode()


def test_native_python_rows_batch_encoder_returns_next_index(require_native: None) -> None:
    """Verify native python rows batch encoder returns next index."""
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    payload, next_index = _native.python_rows_jsonl_bytes(
        [{"a": 1}, {"a": "ñ"}],
        0,
        8,
    )

    assert payload == b'{"a":1}\n'
    assert next_index == 1


@pytest.mark.skipif(
    not hasattr(signal, "setitimer") or not hasattr(signal, "SIGALRM"),
    reason="requires POSIX interval timers",
)
def test_native_python_rows_batch_encoder_checks_pending_signals(require_native: None) -> None:
    """Verify native python rows batch encoder checks pending signals."""
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    old_handler = signal.getsignal(signal.SIGALRM)

    def raise_keyboard_interrupt(signum, frame):
        """Raise the same exception Ctrl+C would surface to Python callers."""
        del signum, frame
        raise KeyboardInterrupt

    rows = [{"value": "x" * 256}] * 2_000_000
    signal.signal(signal.SIGALRM, raise_keyboard_interrupt)
    try:
        with pytest.raises(KeyboardInterrupt):
            # Arm the timer only after pytest owns the BaseException boundary.
            # Coverage tracing can otherwise consume the entire short delay
            # between setitimer() and entering the context manager.
            signal.setitimer(signal.ITIMER_REAL, 0.01)
            _native.python_rows_jsonl_bytes(rows, 0, 1 << 30)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)


def test_local_csv_path_is_supported(tmp_path) -> None:
    """Verify local CSV path is supported."""
    pytest.importorskip("pyarrow")
    path = tmp_path / "data.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")

    result = read_test_csv(path)

    assert result.clean_data.to_pylist() == [{"a": "1", "b": "2"}]


def test_format_specific_readers_are_supported(tmp_path, require_native: None) -> None:
    """Verify format specific readers are supported."""
    pytest.importorskip("pyarrow")

    csv_path = tmp_path / "data.csv"
    json_path = tmp_path / "data.json"
    json_folder = tmp_path / "json-folder"
    jsonl_path = tmp_path / "data.jsonl"
    xml_path = tmp_path / "data.xml"
    xml_folder = tmp_path / "xml-folder"
    csv_path.write_text("a;b\n1;yes\n", encoding="utf-8")
    json_path.write_text('[{"a": 1}]', encoding="utf-8")
    json_folder.mkdir()
    (json_folder / "row.json").write_text('{"a": 1}', encoding="utf-8")
    jsonl_path.write_text('{"a": 1}\n', encoding="utf-8")
    xml_path.write_text("<rows><row><a>1</a></row></rows>", encoding="utf-8")
    xml_folder.mkdir()
    (xml_folder / "row.xml").write_text("<row><a>1</a></row>", encoding="utf-8")

    csv_result = read_test_csv(csv_path, csv_delimiter=";", true_tokens=("yes",))
    assert csv_result.clean_data.to_pylist() == [{"a": "1", "b": True}]
    assert read_test_json(json_path).clean_data.to_pylist() == [{"a": 1}]
    assert read_test_json_folder(json_folder).clean_data.to_pylist() == [{"a": 1}]
    assert read_test_jsonl(jsonl_path).clean_data.to_pylist() == [{"a": 1}]
    assert read_test_xml(xml_path, xml_row_tag="row").clean_data.to_pylist() == [{"a": "1"}]
    assert read_test_xml_folder(xml_folder).clean_data.to_pylist() == [{"a": "1"}]
    assert read_test_python([{"a": "yes"}], true_tokens=("yes",)).clean_data.to_pylist() == [
        {"a": True}
    ]


def test_read_parquet_is_supported(tmp_path, require_native: None) -> None:
    """Verify read Parquet is supported."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"a": [1]}), path)

    assert read_test_parquet(path).clean_data.to_pylist() == [{"a": 1}]


def test_read_json_folder_compacts_non_recursive_json_files(tmp_path, require_native: None) -> None:
    """Verify read JSON folder compacts non recursive JSON files."""
    pytest.importorskip("pyarrow")

    folder = tmp_path / "events"
    nested = folder / "nested"
    folder.mkdir()
    nested.mkdir()
    (folder / "b.json").write_text(json.dumps({"id": 2}, indent=2), encoding="utf-8")
    (folder / "a.json").write_text(json.dumps({"id": 1}, indent=2), encoding="utf-8")
    (folder / "ignore.txt").write_text('{"id": 99}', encoding="utf-8")
    (nested / "c.json").write_text('{"id": 3}', encoding="utf-8")

    result = read_test_json_folder(folder)

    assert result.clean_data.to_pylist() == [{"id": 1}, {"id": 2}]


def test_read_json_folder_uses_native_path_sources(tmp_path, require_native: None) -> None:
    """Verify read JSON folder uses native path sources."""
    pytest.importorskip("pyarrow")

    folder = tmp_path / "events"
    folder.mkdir()
    (folder / "a.json").write_text('{"id": 1}', encoding="utf-8")
    (folder / "b.json").write_text('{"id": 2}', encoding="utf-8")

    result = read_test_json_folder(folder)

    assert result.clean_data.to_pylist() == [{"id": 1}, {"id": 2}]
    assert result.stats["input_source_route"] == "path_sources"
    assert result.stats["input_plan_route"] == "native_manifest_paths"


def test_input_route_diagnostics_are_owned_by_each_result(
    tmp_path: Path, require_native: None
) -> None:
    """A later directory read cannot overwrite an earlier JSONL result's routes."""
    pytest.importorskip("pyarrow")
    jsonl_path = tmp_path / "events.jsonl"
    jsonl_path.write_text('{"id":1}\n', encoding="utf-8")
    folder = tmp_path / "events"
    folder.mkdir()
    (folder / "a.json").write_text('{"id":2}', encoding="utf-8")
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    parquet_path = tmp_path / "events.parquet"
    pq.write_table(pa.table({"id": [3]}), parquet_path)

    jsonl_result = ss.to_pyarrow(jsonl_path, input_format="jsonl")
    directory_result = ss.to_pyarrow(folder, input_format="json", input_mode="directory")
    parquet_result = ss.to_jsonl(
        parquet_path,
        tmp_path / "parquet.jsonl",
        input_format="parquet",
    )

    assert jsonl_result.stats["input_source_route"] == "path"
    assert jsonl_result.stats["input_plan_route"] == ""
    assert jsonl_result.stats["parquet_input_route"] == ""
    assert directory_result.stats["input_source_route"] == "path_sources"
    assert directory_result.stats["input_plan_route"] == "native_manifest_paths"
    assert directory_result.stats["parquet_input_route"] == ""
    assert parquet_result.stats["input_source_route"] == "arrow"
    assert parquet_result.stats["parquet_input_route"] == "native_registry"


@pytest.mark.parametrize(
    ("writer_name", "suffix"),
    (("to_jsonl", ".jsonl"), ("to_csv", ".csv"), ("to_parquet", ".parquet")),
)
def test_single_file_writers_report_their_result_owned_input_route(
    tmp_path: Path,
    require_native: None,
    writer_name: str,
    suffix: str,
) -> None:
    """Every normal public writer reports its successful input source on its result."""
    source = tmp_path / "events.jsonl"
    source.write_text('{"id":1}\n', encoding="utf-8")

    result = getattr(ss, writer_name)(
        source,
        tmp_path / f"output{suffix}",
        input_format="jsonl",
    )

    assert result.stats["input_source_route"] == "path"
    assert result.stats["input_plan_route"] == ""
    assert result.stats["parquet_input_route"] == ""
    assert result.stats["parquet_input_fallback_reason"] == ""


def test_python_row_writer_reports_its_result_owned_input_route(
    tmp_path: Path, require_native: None
) -> None:
    """A Python-row writer reports its selected source without global state."""
    result = ss.to_jsonl(
        [{"id": 1}],
        tmp_path / "output.jsonl",
        input_format="python",
    )

    assert result.stats["input_source_route"] == "python"
    assert result.stats["input_plan_route"] == ""
    assert result.stats["parquet_input_route"] == ""
    assert result.stats["parquet_input_fallback_reason"] == ""


def test_folder_listing_accepts_suffixes_without_leading_dot(tmp_path) -> None:
    """Verify folder listing accepts suffixes without leading dot."""
    from schema_sanitizer.input_impl.directory_inputs import folder_files

    folder = tmp_path / "events"
    folder.mkdir()
    (folder / "a.json").write_text('{"id": 1}', encoding="utf-8")
    (folder / "ignore.txt").write_text('{"id": 2}', encoding="utf-8")

    files = folder_files(folder, suffix="json", reader_name="test directory input")

    assert [file.name for file in files] == ["a.json"]


def test_native_json_compactor_returns_compact_utf8_bytes(tmp_path, require_native: None) -> None:
    """Verify native JSON compactor returns compact utf8 bytes."""
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    payload = _native.json_compact_bytes('{"b": [true, null], "a": "ñ"}'.encode())
    del tmp_path

    assert payload == '{"b":[true,null],"a":"ñ"}'.encode()


def test_native_json_array_to_jsonl_bytes(require_native: None) -> None:
    """Verify native JSON array to JSONL bytes."""
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    payload = b'[\n {"b": [true, null], "a": "\xc3\xb1"}, {"c": 3}\n]'

    assert _native.json_array_to_jsonl_bytes(payload) == (
        '{"b":[true,null],"a":"ñ"}\n{"c":3}\n'.encode()
    )

    with pytest.raises(ValueError, match="object elements"):
        _native.json_array_to_jsonl_bytes(b'[{"ok":true}, 1]')


def test_native_json_array_files_to_jsonl_bytes(tmp_path, require_native: None) -> None:
    """Verify native JSON array files to JSONL bytes."""
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    first = tmp_path / "a.json"
    second = tmp_path / "b.json"
    first.write_text('[{"a":1},{"a":2}]', encoding="utf-8")
    second.write_text('[{"b":true}]', encoding="utf-8")

    assert _native.json_array_files_to_jsonl_bytes([first, second], -1) == (
        b'{"a":1}\n{"a":2}\n{"b":true}\n'
    )

    bad = tmp_path / "bad.json"
    bad.write_text('[{"ok":true}, 1]', encoding="utf-8")
    with pytest.raises(ValueError, match="bad.json"):
        _native.json_array_files_to_jsonl_bytes([bad], -1)


def test_read_json_folder_supports_file_uri(tmp_path, require_native: None) -> None:
    """Verify read JSON folder supports file URI."""
    pytest.importorskip("pyarrow")

    folder = tmp_path / "events"
    nested = folder / "nested"
    folder.mkdir()
    nested.mkdir()
    (folder / "b.json").write_text('{"id": 2}', encoding="utf-8")
    (folder / "a.json").write_text('{"id": 1}', encoding="utf-8")
    (folder / "ignore.txt").write_text('{"id": 99}', encoding="utf-8")
    (nested / "c.json").write_text('{"id": 3}', encoding="utf-8")

    result = read_test_json_folder(folder.as_uri())

    assert result.clean_data.to_pylist() == [{"id": 1}, {"id": 2}]


def test_read_json_folder_supports_native_non_utf8_directory_input(
    tmp_path, require_native: None
) -> None:
    """Verify read JSON folder supports native non utf8 directory input."""
    pytest.importorskip("pyarrow")

    folder = tmp_path / "latin1"
    folder.mkdir()
    (folder / "row.json").write_bytes('{"name":"café"}'.encode("latin-1"))

    result = read_test_json_folder(folder, input_text_encoding="iso8859-1")

    assert result.clean_data.to_pylist() == [{"name": "café"}]


def test_read_json_folder_rejects_missing_or_empty_folder(tmp_path) -> None:
    """Verify read JSON folder rejects missing or empty folder."""
    with pytest.raises(NotADirectoryError):
        read_test_json_folder(tmp_path / "missing")

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ValueError, match="no .json files"):
        read_test_json_folder(empty)


def test_read_json_folder_reports_invalid_json_file(tmp_path) -> None:
    """Verify read JSON folder reports invalid JSON file."""
    pytest.importorskip("pyarrow")
    folder = tmp_path / "bad"
    folder.mkdir()
    bad = folder / "bad.json"
    bad.write_text('{"a":', encoding="utf-8")

    with pytest.raises(ValueError, match="bad.json"):
        read_test_json_folder(folder)


def test_read_json_folder_memory_limit_rejects_large_document(tmp_path) -> None:
    """Verify read JSON folder memory limit rejects large document."""
    folder = tmp_path / "large"
    folder.mkdir()
    (folder / "row.json").write_text(
        json.dumps({"payload": "x" * 300}),
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerResourceError) as excinfo:
        read_test_json_folder(folder, memory_limit_bytes=128)

    err = excinfo.value
    assert err.detail is not None
    assert err.detail["stage"] == "json_parse"
    assert err.detail["limit_name"] == "memory_limit_bytes"
    assert err.detail["file"].endswith("row.json")


def test_read_json_folder_memory_limit_bounds_unknown_size_remote_child(monkeypatch) -> None:
    """Verify read JSON folder memory limit bounds unknown size remote child."""
    pytest.importorskip("pyarrow")
    from contextlib import contextmanager

    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.remote_impl.providers import s3_sync
    from schema_sanitizer.sources import RemoteFile

    def fake_list(uri, suffixes, *, memory_limit_bytes=None):
        """Return one child without a known size."""
        del uri, suffixes
        return [RemoteFile("s3://bucket/events/row.json", "row.json", None)]

    @contextmanager
    def fake_client():
        """Yield one inert blocking S3 client."""
        yield object()

    def fake_download(
        _context,
        file,
        local_path,
        *,
        memory_limit_bytes,
        storage_reservation=None,
    ):
        """Write an oversized payload below the canonical size check."""
        del memory_limit_bytes, storage_reservation
        assert file.name == "row.json"
        Path(local_path).write_bytes(b"x" * 10_000)

    monkeypatch.setattr(sync_backend, "list_remote_directory", fake_list)
    monkeypatch.setattr(s3_sync, "open_client", fake_client)
    monkeypatch.setattr(sync_backend, "_download_with_context", fake_download)

    with pytest.raises(ss.SchemaSanitizerResourceError) as excinfo:
        read_test_json_folder("s3://bucket/events/", memory_limit_bytes=128)

    err = excinfo.value
    assert err.detail is not None
    assert err.detail["stage"] == "remote_download"
    assert err.detail["actual_bytes"] == 10_000


def test_read_xml_folder_compacts_non_recursive_xml_files(tmp_path, require_native: None) -> None:
    """Verify read XML folder compacts non recursive XML files."""
    pytest.importorskip("pyarrow")

    folder = tmp_path / "events"
    nested = folder / "nested"
    folder.mkdir()
    nested.mkdir()
    (folder / "b.xml").write_text('<event id="2"><country>US</country></event>', encoding="utf-8")
    (folder / "a.xml").write_text(
        '<?xml version="1.0"?><event id="1"><country>ES</country></event>', encoding="utf-8"
    )
    (folder / "ignore.txt").write_text("<event><id>99</id></event>", encoding="utf-8")
    (nested / "c.xml").write_text("<event><id>3</id></event>", encoding="utf-8")

    result = read_test_xml_folder(folder)

    assert result.clean_data.to_pylist() == [
        {"id": "1", "country": "ES"},
        {"id": "2", "country": "US"},
    ]


def test_read_xml_folder_propagates_native_root_tag_failure(tmp_path, monkeypatch) -> None:
    """Verify read XML folder propagates native root tag failure."""
    pytest.importorskip("pyarrow")

    def fail_native_row_tag(*_args: object) -> str:
        """Simulate a native XML row-tag detection failure."""
        raise RuntimeError("native XML row-tag detection failed")

    folder = tmp_path / "events"
    folder.mkdir()
    (folder / "a.xml").write_text("<event><id>1</id></event>", encoding="utf-8")

    import schema_sanitizer.api_impl.input.directory_preparation as native_directory_sources

    monkeypatch.setattr(
        native_directory_sources, "XML_FOLDER_EFFECTIVE_ROW_TAG", fail_native_row_tag
    )

    with pytest.raises(RuntimeError, match="native XML row-tag detection failed"):
        read_test_xml_folder(folder)


def test_native_xml_folder_helpers_strip_declarations(tmp_path, require_native: None) -> None:
    """Verify native XML folder helpers strip declarations."""
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    first = tmp_path / "a.xml"
    second = tmp_path / "b.xml"
    first.write_text('<?xml version="1.0"?><event><id>1</id></event>', encoding="utf-8")
    second.write_text("<event><id>2</id></event>", encoding="utf-8")

    paths = [first, second]
    for sequence in (paths, tuple(paths), iter(paths)):
        assert _native.xml_folder_effective_row_tag(sequence, "", -1) == "event"


def test_read_xml_folder_supports_file_uri(tmp_path, require_native: None) -> None:
    """Verify read XML folder supports file URI."""
    pytest.importorskip("pyarrow")

    folder = tmp_path / "events"
    nested = folder / "nested"
    folder.mkdir()
    nested.mkdir()
    (folder / "b.xml").write_text('<event id="2"><country>US</country></event>', encoding="utf-8")
    (folder / "a.xml").write_text('<event id="1"><country>ES</country></event>', encoding="utf-8")
    (folder / "ignore.txt").write_text("<event><id>99</id></event>", encoding="utf-8")
    (nested / "c.xml").write_text("<event><id>3</id></event>", encoding="utf-8")

    result = read_test_xml_folder(folder.as_uri())

    assert result.clean_data.to_pylist() == [
        {"id": "1", "country": "ES"},
        {"id": "2", "country": "US"},
    ]


def test_read_xml_folder_rejects_non_utf8_directory_input(tmp_path, require_native: None) -> None:
    """Verify read XML folder rejects non utf8 directory input."""
    pytest.importorskip("pyarrow")

    folder = tmp_path / "latin1"
    folder.mkdir()
    (folder / "row.xml").write_bytes("<event><name>café</name></event>".encode("latin-1"))

    with pytest.raises(RuntimeError, match="native C\\+\\+ path-source"):
        read_test_xml_folder(folder, input_text_encoding="iso8859-1")


def test_read_xml_folder_rejects_missing_or_empty_folder(tmp_path) -> None:
    """Verify read XML folder rejects missing or empty folder."""
    with pytest.raises(NotADirectoryError):
        read_test_xml_folder(tmp_path / "missing")

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ValueError, match="no .xml files"):
        read_test_xml_folder(empty)


def test_read_xml_folder_rejects_mismatched_root_tags(tmp_path) -> None:
    """Verify read XML folder rejects mismatched root tags."""
    folder = tmp_path / "mixed"
    folder.mkdir()
    (folder / "a.xml").write_text("<event><id>1</id></event>", encoding="utf-8")
    (folder / "b.xml").write_text("<record><id>2</id></record>", encoding="utf-8")

    with pytest.raises(ValueError, match="expected root tag"):
        read_test_xml_folder(folder)


def test_read_xml_folder_memory_limit_rejects_large_document(
    tmp_path, require_native: None
) -> None:
    """Verify read XML folder memory limit rejects large document."""
    folder = tmp_path / "large"
    folder.mkdir()
    (folder / "row.xml").write_text(
        "<event><payload>" + ("x" * 300) + "</payload></event>",
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerResourceError) as excinfo:
        read_test_xml_folder(folder, memory_limit_bytes=128)

    err = excinfo.value
    assert err.detail is not None
    assert err.detail["stage"] == "xml_parse"
    assert err.detail["limit_name"] == "memory_limit_bytes"


def test_discovered_file_rejects_known_oversize_before_opening() -> None:
    """Known file sizes should fail without opening or reading the stream."""
    from schema_sanitizer.errors import SchemaSanitizerResourceError
    from schema_sanitizer.input_impl.directory_inputs import FolderFile, read_folder_file_bytes

    opened = False

    def open_binary():
        """Record an unexpected stream open."""
        nonlocal opened
        opened = True
        return io.BytesIO(b"payload")

    file = FolderFile(
        display_name="oversize.json",
        name="oversize.json",
        size=8,
        open_binary=open_binary,
    )
    with pytest.raises(SchemaSanitizerResourceError, match="oversize.json"):
        read_folder_file_bytes(
            file,
            memory_limit_bytes=4,
            stage="directory read",
        )
    assert not opened


def test_python_rows_generator_rejects_replay_spool_limit(require_native: None) -> None:
    """One-shot Python iterables must not grow replay storage without a bound."""
    reader = PythonRowsJsonlByteReader(
        ({"payload": "x" * 256} for _ in range(100)),
        memory_limit_bytes=256,
    )

    with pytest.raises(RuntimeError, match="max_replay_spool_bytes limit exceeded"):
        while reader.read(4096):
            pass
    reader.close()
