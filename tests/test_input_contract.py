"""Tests the supported public input contract."""

from __future__ import annotations

import asyncio
import inspect
import io
import json
import signal
import threading
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
    require_native,
)

import schema_sanitizer as ss
from schema_sanitizer.core_impl.python_rows import (
    PythonRowsJsonlByteReader,
    last_python_rows_route,
)


class _TrackingByteReader:
    """Seekable byte reader used to verify native streaming reads."""

    def __init__(self, data: bytes):
        """Store the byte payload and reset read tracking."""
        self._data = data
        self._pos = 0
        self.requests: list[int] = []

    def read(self, max_bytes: int) -> bytes:
        """Return at most the requested bytes and record the requested size."""
        self.requests.append(max_bytes)
        if self._pos >= len(self._data):
            return b""
        end = min(len(self._data), self._pos + max_bytes)
        chunk = self._data[self._pos : end]
        self._pos = end
        return chunk

    def seek(self, offset: int) -> int:
        """Reset the reader to the start of the byte payload."""
        if offset != 0:
            raise ValueError("test reader only supports seek(0)")
        self._pos = 0
        return self._pos

    def close(self) -> None:
        """Match the pyarrow reader close API."""
        pass


class _OversizedByteReader:
    """Byte reader that verifies bounded folder reads stop at the memory limit."""

    def __init__(self, size: int):
        """Store the virtual byte size and reset read tracking."""
        self._remaining = size
        self.requests: list[int] = []
        self.bytes_returned = 0

    def read(self, max_bytes: int) -> bytes:
        """Return virtual bytes up to the requested size."""
        self.requests.append(max_bytes)
        if self._remaining <= 0:
            return b""
        size = min(max_bytes, self._remaining)
        self._remaining -= size
        self.bytes_returned += size
        return b"x" * size

    def close(self) -> None:
        """Match the pyarrow reader close API."""
        pass


def test_list_of_dicts_is_supported() -> None:
    """Verify list of dicts is supported."""
    pytest.importorskip("pyarrow")

    result = read_test_python([{"a": 1}, {"a": 2}])

    assert result.clean_data.to_pylist() == [{"a": 1}, {"a": 2}]
    assert last_python_rows_route() == "native_batch"


def test_python_rows_jsonl_reader_is_replayable_and_chunked() -> None:
    """Verify Python rows are exposed to native ingestion as replayable chunks."""
    require_native()
    reader = PythonRowsJsonlByteReader([{"a": 1}, {"a": "ñ"}])

    first = reader.read(7)
    second = reader.read(1024)
    reader.seek(0)

    assert first == b'{"a":1}'
    assert second == '\n{"a":"ñ"}\n'.encode()
    assert reader.read(1024) == first + second


def test_python_rows_jsonl_reader_rejects_unsupported_values_without_fallback() -> None:
    """Verify Python row ingestion fails instead of using Python JSON fallback."""
    require_native()
    reader = PythonRowsJsonlByteReader([{"bad": object()}])

    with pytest.raises(RuntimeError, match="Native Python row JSONL encoding failed"):
        reader.read(1024)


def test_native_python_row_encoder_matches_reader_payload() -> None:
    """Verify the native Python row encoder can feed the row reader."""
    require_native()
    from schema_sanitizer.core_impl.native import _native

    payload = _native.python_row_json_bytes({"b": [True, None], "a": "ñ"})

    assert payload == '{"b":[true,null],"a":"ñ"}'.encode()


def test_native_python_rows_batch_encoder_returns_next_index() -> None:
    """Verify the native Python row batch encoder returns JSONL and progress."""
    require_native()
    from schema_sanitizer.core_impl.native import _native

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
def test_native_python_rows_batch_encoder_checks_pending_signals() -> None:
    """Verify long native row encoding polls Python signal handlers."""
    require_native()
    from schema_sanitizer.core_impl.native import _native

    old_handler = signal.getsignal(signal.SIGALRM)

    def raise_keyboard_interrupt(signum, frame):
        """Raise the same exception Ctrl+C would surface to Python callers."""
        del signum, frame
        raise KeyboardInterrupt

    rows = [{"value": "x" * 256}] * 2_000_000
    signal.signal(signal.SIGALRM, raise_keyboard_interrupt)
    signal.setitimer(signal.ITIMER_REAL, 0.01)
    try:
        with pytest.raises(KeyboardInterrupt):
            _native.python_rows_jsonl_bytes(rows, 0, 1 << 30)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)


def test_local_csv_path_is_supported(tmp_path) -> None:
    """Verify local csv path is supported."""
    pytest.importorskip("pyarrow")
    path = tmp_path / "data.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")

    result = read_test_csv(path)

    assert result.clean_data.to_pylist() == [{"a": "1", "b": "2"}]


def test_format_specific_readers_are_supported(tmp_path) -> None:
    """Verify format specific readers are supported."""
    pytest.importorskip("pyarrow")
    require_native()

    csv_path = tmp_path / "data.csv"
    json_path = tmp_path / "data.json"
    json_folder = tmp_path / "json-folder"
    jsonl_path = tmp_path / "data.jsonl"
    ndjson_path = tmp_path / "data.ndjson"
    xml_path = tmp_path / "data.xml"
    xml_folder = tmp_path / "xml-folder"
    csv_path.write_text("a;b\n1;yes\n", encoding="utf-8")
    json_path.write_text('[{"a": 1}]', encoding="utf-8")
    json_folder.mkdir()
    (json_folder / "row.json").write_text('{"a": 1}', encoding="utf-8")
    jsonl_path.write_text('{"a": 1}\n', encoding="utf-8")
    ndjson_path.write_text('{"a": 1}\n', encoding="utf-8")
    xml_path.write_text("<rows><row><a>1</a></row></rows>", encoding="utf-8")
    xml_folder.mkdir()
    (xml_folder / "row.xml").write_text("<row><a>1</a></row>", encoding="utf-8")

    csv_result = read_test_csv(csv_path, csv_delimiter=";", true_tokens=("yes",))
    assert csv_result.clean_data.to_pylist() == [{"a": "1", "b": True}]
    assert read_test_json(json_path).clean_data.to_pylist() == [{"a": 1}]
    assert read_test_json_folder(json_folder).clean_data.to_pylist() == [{"a": 1}]
    assert read_test_jsonl(jsonl_path).clean_data.to_pylist() == [{"a": 1}]
    assert read_test_jsonl(ndjson_path).clean_data.to_pylist() == [{"a": 1}]
    assert read_test_xml(xml_path, xml_row_tag="row").clean_data.to_pylist() == [{"a": "1"}]
    assert read_test_xml_folder(xml_folder).clean_data.to_pylist() == [{"a": "1"}]
    assert read_test_python([{"a": "yes"}], true_tokens=("yes",)).clean_data.to_pylist() == [
        {"a": True}
    ]


def test_read_parquet_is_supported(tmp_path) -> None:
    """Verify read parquet is supported."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    require_native()

    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"a": [1]}), path)

    assert read_test_parquet(path).clean_data.to_pylist() == [{"a": 1}]


def test_read_json_folder_compacts_non_recursive_json_files(tmp_path) -> None:
    """Verify read json folder compacts direct json children only."""
    pytest.importorskip("pyarrow")
    require_native()

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


def test_read_json_folder_uses_native_path_sources(tmp_path) -> None:
    """Verify low-level table reads use native path sources."""
    pytest.importorskip("pyarrow")
    require_native()
    from schema_sanitizer.api_impl.source_plan import last_native_multisource_route
    from schema_sanitizer.core_impl.runtime import last_sink_source_route

    folder = tmp_path / "events"
    folder.mkdir()
    (folder / "a.json").write_text('{"id": 1}', encoding="utf-8")
    (folder / "b.json").write_text('{"id": 2}', encoding="utf-8")

    result = read_test_json_folder(folder)

    assert result.clean_data.to_pylist() == [{"id": 1}, {"id": 2}]
    assert last_sink_source_route() == "path_sources"
    assert last_native_multisource_route() == "cxx_path_sources"


def test_folder_listing_accepts_suffixes_without_leading_dot(tmp_path) -> None:
    """Verify shared folder listing normalizes dotted and bare suffixes equally."""
    from schema_sanitizer.api_impl.folder_listing import folder_files

    folder = tmp_path / "events"
    folder.mkdir()
    (folder / "a.json").write_text('{"id": 1}', encoding="utf-8")
    (folder / "ignore.txt").write_text('{"id": 2}', encoding="utf-8")

    files = folder_files(folder, suffix="json", reader_name="test directory input")

    assert [file.name for file in files] == ["a.json"]


def test_native_json_compactor_returns_compact_utf8_bytes(tmp_path) -> None:
    """Verify the native JSON compactor used by registry normalization."""
    require_native()
    from schema_sanitizer.core_impl.native import _native

    payload = _native.json_compact_bytes('{"b": [true, null], "a": "ñ"}'.encode())
    del tmp_path

    assert payload == '{"b":[true,null],"a":"ñ"}'.encode()


def test_native_json_array_to_jsonl_bytes() -> None:
    """Verify native JSON array splitting returns compact JSONL object rows."""
    require_native()
    from schema_sanitizer.core_impl.native import _native

    payload = b'[\n {"b": [true, null], "a": "\xc3\xb1"}, {"c": 3}\n]'

    assert _native.json_array_to_jsonl_bytes(payload) == (
        '{"b":[true,null],"a":"ñ"}\n{"c":3}\n'.encode()
    )

    with pytest.raises(ValueError, match="object elements"):
        _native.json_array_to_jsonl_bytes(b'[{"ok":true}, 1]')


def test_native_json_array_files_to_jsonl_bytes(tmp_path) -> None:
    """Verify native local JSON-array file batching returns JSONL rows."""
    require_native()
    from schema_sanitizer.core_impl.native import _native

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


def test_json_array_reader_uses_native_bytes_for_non_path_source() -> None:
    """Verify non-path JSON-array readers still use native byte splitting."""
    require_native()
    from schema_sanitizer.api_impl.folder_listing import FolderFile
    from schema_sanitizer.api_impl.json_array_reader import JsonArrayJsonlByteReader

    payload = b'[{"a":1},{"b":2}]'
    reader = JsonArrayJsonlByteReader(
        FolderFile(
            display_name="memory.json",
            name="memory.json",
            size=len(payload),
            open_binary=lambda: io.BytesIO(payload),
            native_path=None,
        ),
        memory_limit_bytes=len(payload),
    )

    assert reader.read(1024) == b'{"a":1}\n{"b":2}\n'


def test_json_array_reader_requires_bounded_non_path_source() -> None:
    """Verify non-path JSON-array conversion cannot materialize without a memory limit."""
    from schema_sanitizer.api_impl.folder_listing import FolderFile
    from schema_sanitizer.api_impl.json_array_reader import JsonArrayJsonlByteReader

    payload = b'[{"a":1}]'
    with pytest.raises(RuntimeError, match="requires memory_limit_bytes"):
        JsonArrayJsonlByteReader(
            FolderFile(
                display_name="memory.json",
                name="memory.json",
                size=len(payload),
                open_binary=lambda: io.BytesIO(payload),
                native_path=None,
            )
        )


def test_json_array_reader_rejects_missing_native_bytes_support(monkeypatch) -> None:
    """Verify JSON-array conversion has no Python parser fallback."""
    from schema_sanitizer.api_impl import json_array_reader
    from schema_sanitizer.api_impl.folder_listing import FolderFile
    from schema_sanitizer.api_impl.json_array_reader import JsonArrayJsonlByteReader

    payload = b'[{"a":1}]'
    monkeypatch.setattr(json_array_reader.JSON_ARRAY_TO_JSONL_BYTES, "get", lambda: None)
    reader = JsonArrayJsonlByteReader(
        FolderFile(
            display_name="memory.json",
            name="memory.json",
            size=len(payload),
            open_binary=lambda: io.BytesIO(payload),
            native_path=None,
        ),
        memory_limit_bytes=len(payload),
    )

    with pytest.raises(RuntimeError, match="native JSON-array bytes support"):
        reader.read(1024)


def test_json_array_reader_rejects_missing_native_file_support(tmp_path, monkeypatch) -> None:
    """Verify path JSON-array conversion does not fall back to raw byte conversion."""
    from schema_sanitizer.api_impl import json_array_reader
    from schema_sanitizer.api_impl.json_array_reader import JsonArrayJsonlByteReader
    from schema_sanitizer.api_impl.public_input import _single_file_descriptor

    path = tmp_path / "rows.json"
    path.write_text('[{"a":1}]', encoding="utf-8")
    monkeypatch.setattr(json_array_reader.JSON_ARRAY_FILES_TO_JSONL_BYTES, "get", lambda: None)
    reader = JsonArrayJsonlByteReader(_single_file_descriptor(path))

    with pytest.raises(RuntimeError, match="native JSON-array file support"):
        reader.read(1024)


def test_read_json_folder_supports_file_uri(tmp_path) -> None:
    """Verify read json folder can list children through file URIs."""
    pytest.importorskip("pyarrow")
    require_native()

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


def test_read_json_folder_supports_native_non_utf8_directory_input(tmp_path) -> None:
    """Verify JSON directory input can use native path-source transcoding."""
    pytest.importorskip("pyarrow")
    require_native()

    folder = tmp_path / "latin1"
    folder.mkdir()
    (folder / "row.json").write_bytes('{"name":"café"}'.encode("latin-1"))

    result = read_test_json_folder(folder, input_text_encoding="latin-1")

    assert result.clean_data.to_pylist() == [{"name": "café"}]


def test_read_json_folder_rejects_missing_or_empty_folder(tmp_path) -> None:
    """Verify read json folder rejects invalid folder inputs."""
    with pytest.raises(NotADirectoryError):
        read_test_json_folder(tmp_path / "missing")

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ValueError, match="no .json files"):
        read_test_json_folder(empty)


def test_read_json_folder_reports_invalid_json_file(tmp_path) -> None:
    """Verify read json folder reports invalid source file path."""
    folder = tmp_path / "bad"
    folder.mkdir()
    bad = folder / "bad.json"
    bad.write_text('{"a":', encoding="utf-8")

    with pytest.raises(ValueError, match="bad.json"):
        read_test_json_folder(folder)


def test_read_json_folder_memory_limit_rejects_large_document(tmp_path) -> None:
    """Verify read json folder respects configured document-row memory limits."""
    folder = tmp_path / "large"
    folder.mkdir()
    (folder / "row.json").write_text(
        json.dumps({"payload": "x" * 300}),
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerResourceError) as excinfo:
        read_test_json_folder(folder, batch_memory_limit_bytes=128)

    err = excinfo.value
    assert err.detail is not None
    assert err.detail["stage"] == "json_parse"
    assert err.detail["limit_name"] == "memory_limit_bytes"
    assert err.detail["file"].endswith("row.json")


def test_read_json_folder_memory_limit_bounds_unknown_size_remote_child(monkeypatch) -> None:
    """Verify remote folder staging rejects oversized children with unknown size."""
    from schema_sanitizer.api_impl import async_remote_io

    async def fake_list(uri, suffixes):
        """Return one child without a known size."""
        del uri, suffixes
        return [async_remote_io.RemoteFile("s3://bucket/events/row.json", "row.json", None)]

    async def fake_download(client, file, local_path):
        """Write an oversized payload."""
        del client, file
        Path(local_path).write_bytes(b"x" * 10_000)

    async def fake_client(files):
        """Return no reusable provider client."""
        del files
        return None

    async def fake_close(client):
        """Close no reusable provider client."""
        del client

    monkeypatch.setattr(async_remote_io, "_list_remote_directory", fake_list)
    monkeypatch.setattr(async_remote_io, "_provider_client_for_downloads", fake_client)
    monkeypatch.setattr(async_remote_io, "_close_provider_client", fake_close)
    monkeypatch.setattr(async_remote_io, "_download_one_file_to_path", fake_download)

    with pytest.raises(ss.SchemaSanitizerResourceError) as excinfo:
        read_test_json_folder("s3://bucket/events/", batch_memory_limit_bytes=128)

    err = excinfo.value
    assert err.detail is not None
    assert err.detail["stage"] == "remote_download"
    assert err.detail["actual_bytes"] == 10_000


def test_read_xml_folder_compacts_non_recursive_xml_files(tmp_path) -> None:
    """Verify read xml folder compacts direct xml children only."""
    pytest.importorskip("pyarrow")
    require_native()
    from schema_sanitizer.api_impl.source_plan import last_native_multisource_route

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
    assert last_native_multisource_route() == "cxx_path_sources"


def test_read_xml_folder_requires_native_root_tag_detection(tmp_path, monkeypatch) -> None:
    """Verify XML directory input has no Python root-tag fallback."""
    pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl import public_input

    folder = tmp_path / "events"
    folder.mkdir()
    (folder / "a.xml").write_text("<event><id>1</id></event>", encoding="utf-8")

    monkeypatch.setattr(public_input.XML_FOLDER_EFFECTIVE_ROW_TAG, "get", lambda: None)

    with pytest.raises(RuntimeError, match="native XML folder helper"):
        read_test_xml_folder(folder)


def test_native_xml_folder_helpers_strip_declarations(tmp_path) -> None:
    """Verify native XML folder helper validates stable roots."""
    require_native()
    from schema_sanitizer.core_impl.native import _native

    first = tmp_path / "a.xml"
    second = tmp_path / "b.xml"
    first.write_text('<?xml version="1.0"?><event><id>1</id></event>', encoding="utf-8")
    second.write_text("<event><id>2</id></event>", encoding="utf-8")

    paths = [first, second]
    assert _native.xml_folder_effective_row_tag(paths, "", -1) == "event"


def test_read_xml_folder_supports_file_uri(tmp_path) -> None:
    """Verify read xml folder can list children through file URIs."""
    pytest.importorskip("pyarrow")
    require_native()

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


def test_read_xml_folder_rejects_non_utf8_directory_input(tmp_path) -> None:
    """Verify XML directory input requires native UTF-8 path-source ingestion."""
    pytest.importorskip("pyarrow")
    require_native()

    folder = tmp_path / "latin1"
    folder.mkdir()
    (folder / "row.xml").write_bytes("<event><name>café</name></event>".encode("latin-1"))

    with pytest.raises(RuntimeError, match="native C\\+\\+ path-source"):
        read_test_xml_folder(folder, input_text_encoding="latin-1")


def test_read_xml_folder_rejects_missing_or_empty_folder(tmp_path) -> None:
    """Verify read xml folder rejects invalid folder inputs."""
    with pytest.raises(NotADirectoryError):
        read_test_xml_folder(tmp_path / "missing")

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ValueError, match="no .xml files"):
        read_test_xml_folder(empty)


def test_read_xml_folder_rejects_mismatched_root_tags(tmp_path) -> None:
    """Verify read xml folder rejects mixed document root tags."""
    folder = tmp_path / "mixed"
    folder.mkdir()
    (folder / "a.xml").write_text("<event><id>1</id></event>", encoding="utf-8")
    (folder / "b.xml").write_text("<order><id>2</id></order>", encoding="utf-8")

    with pytest.raises(ValueError, match="expected root tag"):
        read_test_xml_folder(folder)


def test_read_xml_folder_memory_limit_rejects_large_document(tmp_path) -> None:
    """Verify read xml folder respects configured document-row memory limits."""
    require_native()

    folder = tmp_path / "large"
    folder.mkdir()
    (folder / "row.xml").write_text(
        "<event><payload>" + ("x" * 300) + "</payload></event>",
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerResourceError) as excinfo:
        read_test_xml_folder(folder, batch_memory_limit_bytes=128)

    err = excinfo.value
    assert err.detail is not None
    assert err.detail["stage"] == "xml_parse"
    assert err.detail["limit_name"] == "memory_limit_bytes"


def test_uri_input_uses_async_local_staging(monkeypatch, tmp_path) -> None:
    """Verify URI inputs are staged to a replayable local file."""
    pytest.importorskip("pyarrow")
    require_native()

    staged_paths: list[str] = []

    class _Stage:
        """Minimal staged-path stand-in."""

        def __init__(self, path: Path):
            """Store the local staged path."""
            self.path = str(path)

        def close(self) -> None:
            """Keep the staged file available for test assertions."""
            pass

    def fake_stage(uri: str, *, memory_limit_bytes: int | None) -> _Stage:
        """Write the remote payload to a local staged file."""
        del memory_limit_bytes
        assert uri == "s3://bucket/events.jsonl"
        path = tmp_path / "staged.jsonl"
        path.write_bytes(b'{"a": 1}\n{"a": 2}\n')
        staged_paths.append(str(path))
        return _Stage(path)

    from schema_sanitizer.api_impl import public_input

    monkeypatch.setattr(public_input, "stage_remote_single_file", fake_stage)

    result = read_test_jsonl("s3://bucket/events.jsonl", read_chunk_bytes=5)

    assert result.clean_data.to_pylist() == [{"a": 1}, {"a": 2}]
    assert staged_paths == [str(tmp_path / "staged.jsonl")]


def test_uri_input_staging_works_with_converters(monkeypatch, tmp_path) -> None:
    """Verify converter inputs stage remote files before native conversion."""
    pytest.importorskip("pyarrow")
    require_native()

    out = tmp_path / "out.jsonl"

    class _Stage:
        """Minimal staged-path stand-in."""

        def __init__(self, path: Path):
            """Store the local staged path."""
            self.path = str(path)

        def close(self) -> None:
            """Keep the staged file available for test assertions."""
            pass

    def fake_stage(uri: str, *, memory_limit_bytes: int | None) -> _Stage:
        """Write one remote payload to a local staged file."""
        del memory_limit_bytes
        assert uri == "s3://bucket/events.jsonl"
        path = tmp_path / "converter-staged.jsonl"
        path.write_bytes(b'{"a": 1}\n{"a": 2}\n')
        return _Stage(path)

    from schema_sanitizer.api_impl import public_input

    monkeypatch.setattr(public_input, "stage_remote_single_file", fake_stage)

    ss.to_jsonl(
        "s3://bucket/events.jsonl",
        out,
        input_format="jsonl",
        read_chunk_bytes=6,
    )

    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert [
        {
            k: v
            for k, v in row.items()
            if k
            not in {
                "schema_registry",
                "schema_drifts",
                "source_file",
                "ingestion_timestamp",
            }
        }
        for row in rows
    ] == [{"a": 1}, {"a": 2}]


def test_uri_input_allows_non_utf8_after_local_staging(monkeypatch, tmp_path) -> None:
    """Verify URI text inputs can be transcoded after local staging."""
    require_native()

    class _Stage:
        """Minimal staged-path stand-in."""

        def __init__(self, path: Path):
            """Store the local staged path."""
            self.path = str(path)

        def close(self) -> None:
            """Keep the staged file available for test assertions."""
            pass

    def fake_stage(uri: str, *, memory_limit_bytes: int | None) -> _Stage:
        """Write Latin-1 JSONL to a local staged file."""
        del memory_limit_bytes
        assert uri == "s3://bucket/events.jsonl"
        path = tmp_path / "latin1.jsonl"
        path.write_bytes('{"name":"café"}\n'.encode("latin-1"))
        return _Stage(path)

    from schema_sanitizer.api_impl import public_input

    monkeypatch.setattr(public_input, "stage_remote_single_file", fake_stage)

    result = read_test_jsonl("s3://bucket/events.jsonl", input_text_encoding="latin-1")
    assert result.clean_data.to_pylist() == [{"name": "café"}]


def test_remote_parquet_directory_stages_children_concurrently(monkeypatch, tmp_path) -> None:
    """Verify remote Parquet directory staging downloads every listed child."""
    from schema_sanitizer.api_impl import async_remote_io

    async def fake_list(uri, suffixes):
        """Return deterministic remote Parquet children."""
        assert uri == "s3://bucket/partition/"
        assert ".parquet" in suffixes
        return [
            async_remote_io.RemoteFile("s3://bucket/partition/a.parquet", "a.parquet", None),
            async_remote_io.RemoteFile("s3://bucket/partition/b.parquet", "b.parquet", None),
        ]

    async def fake_client(files):
        """Return a reusable fake provider client."""
        assert len(files) == 2
        return object()

    async def fake_close(client):
        """Accept closing the fake provider client."""
        assert client is not None

    async def fake_download(client, file, local_path):
        """Write a staged payload for one remote child."""
        assert client is not None
        Path(local_path).write_bytes(file.name.encode("utf-8"))

    monkeypatch.setattr(async_remote_io, "_list_remote_directory", fake_list)
    monkeypatch.setattr(async_remote_io, "_provider_client_for_downloads", fake_client)
    monkeypatch.setattr(async_remote_io, "_close_provider_client", fake_close)
    monkeypatch.setattr(async_remote_io, "_download_one_file_to_path", fake_download)

    staged = async_remote_io.stage_remote_parquet_directory(
        "s3://bucket/partition/",
        suffixes=(".parquet", ".pq"),
        memory_limit_bytes=None,
    )
    try:
        root = Path(staged.path)
        assert (root / "a.parquet").read_bytes() == b"a.parquet"
        assert (root / "b.parquet").read_bytes() == b"b.parquet"
    finally:
        staged.close()


def test_remote_parquet_directory_public_reader_uses_staged_arrow_path(
    monkeypatch,
    tmp_path,
) -> None:
    """Verify remote Parquet directories stage locally and preserve source URIs."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import public_input
    from schema_sanitizer.api_impl.async_remote_io import StagedPath

    require_native()

    def fake_stage_remote_parquet_directory(uri, *, suffixes, memory_limit_bytes):
        """Return a local staged Parquet directory for a remote URI."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".parquet", ".pq")
        assert memory_limit_bytes is None
        staged_dir = tmp_path / "staged-parquet"
        staged_dir.mkdir()
        pq.write_table(pa.table({"id": [1, 2]}), staged_dir / "a.parquet")
        pq.write_table(pa.table({"id": [3]}), staged_dir / "b.parquet")
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={
                "a.parquet": "s3://bucket/partition/a.parquet",
                "b.parquet": "s3://bucket/partition/b.parquet",
            },
        )

    monkeypatch.setattr(
        public_input,
        "stage_remote_parquet_directory",
        fake_stage_remote_parquet_directory,
    )

    result = ss.to_pyarrow(
        "s3://bucket/partition/",
        input_format="parquet",
        input_mode="directory",
    )

    rows = result.clean_data.to_pylist()
    assert [{key: row[key] for key in ("id",)} for row in rows] == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]
    assert [row["source_file"] for row in rows] == [
        "s3://bucket/partition/a.parquet",
        "s3://bucket/partition/a.parquet",
        "s3://bucket/partition/b.parquet",
    ]


def test_remote_text_directory_stages_child_sources_concurrently(monkeypatch) -> None:
    """Verify remote text directory staging preserves child files and source URIs."""
    from schema_sanitizer.api_impl import async_remote_io

    files = [
        async_remote_io.RemoteFile("s3://bucket/partition/a.jsonl", "a.jsonl", None),
        async_remote_io.RemoteFile("s3://bucket/partition/b.jsonl", "b.jsonl", None),
    ]

    async def fake_client(files):
        """Return a reusable fake provider client."""
        assert len(files) == 2
        return object()

    async def fake_close(client):
        """Accept closing the fake provider client."""
        assert client is not None

    async def fake_download(client, file, local_path):
        """Write a staged payload for one remote child."""
        assert client is not None
        Path(local_path).write_bytes(file.uri.encode("utf-8"))

    monkeypatch.setattr(async_remote_io, "_provider_client_for_downloads", fake_client)
    monkeypatch.setattr(async_remote_io, "_close_provider_client", fake_close)
    monkeypatch.setattr(async_remote_io, "_download_one_file_to_path", fake_download)

    staged = async_remote_io.stage_remote_files_to_directory(
        files,
        memory_limit_bytes=None,
    )
    try:
        root = Path(staged.path)
        assert (root / "a.jsonl").read_bytes() == b"s3://bucket/partition/a.jsonl"
        assert (root / "b.jsonl").read_bytes() == b"s3://bucket/partition/b.jsonl"
        assert staged.source_file_by_name == {
            "a.jsonl": "s3://bucket/partition/a.jsonl",
            "b.jsonl": "s3://bucket/partition/b.jsonl",
        }
    finally:
        staged.close()


def test_remote_gcs_directory_listing_reads_all_pages(monkeypatch) -> None:
    """Verify GCS remote directory listing follows nextPageToken pages."""
    from schema_sanitizer.api_impl import async_remote_io

    class FakeResponse:
        """Minimal aiohttp-like response for one GCS list page."""

        status = 200

        def __init__(self, payload: dict[str, object]):
            """Store one JSON response payload."""
            self._payload = payload

        async def __aenter__(self):
            """Return this fake response."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Close this fake response."""
            return None

        async def text(self) -> str:
            """Return the JSON response body."""
            return json.dumps(self._payload)

    class FakeSession:
        """Minimal aiohttp-like session with paginated GCS responses."""

        def __init__(self):
            """Seed two pages where only page two has matching files."""
            self.params: list[dict[str, str]] = []
            self.pages = [
                {"items": [{"name": "events/ignore.txt"}], "nextPageToken": "page-2"},
                {"items": [{"name": "events/row.json", "size": "7"}]},
            ]

        async def __aenter__(self):
            """Return this fake session."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Close this fake session."""
            return None

        def get(self, _url, *, params):
            """Return the next fake page and record request parameters."""
            self.params.append(dict(params))
            return FakeResponse(self.pages.pop(0))

    fake_session = FakeSession()

    async def fake_session_factory(headers):
        """Return the fake GCS session."""
        assert headers["Authorization"] == "Bearer token"
        return fake_session

    monkeypatch.setattr(async_remote_io, "_gcs_token", lambda: "token")
    monkeypatch.setattr(async_remote_io, "_aiohttp_session", fake_session_factory)

    files = async_remote_io._run_async(async_remote_io._gcs_list("gs://bucket/events/", (".json",)))

    assert files == [async_remote_io.RemoteFile("gs://bucket/events/row.json", "row.json", 7)]
    assert fake_session.params[0]["prefix"] == "events/"
    assert "pageToken" not in fake_session.params[0]
    assert fake_session.params[1]["pageToken"] == "page-2"


def test_remote_gcs_bulk_directory_discovery_groups_parent_prefixes(monkeypatch) -> None:
    """Verify GCS source discovery can check sibling partition directories in one listing."""
    from schema_sanitizer.api_impl import async_remote_io

    class FakeResponse:
        """Minimal aiohttp-like response for one GCS list page."""

        status = 200

        def __init__(self, payload: dict[str, object]):
            """Store one JSON response payload."""
            self._payload = payload

        async def __aenter__(self):
            """Return this fake response."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Close this fake response."""
            return None

        async def text(self) -> str:
            """Return the JSON response body."""
            return json.dumps(self._payload)

    class FakeSession:
        """Minimal aiohttp-like session with one parent-prefix listing."""

        def __init__(self):
            """Seed one page containing two requested child directories."""
            self.params: list[dict[str, str]] = []

        async def __aenter__(self):
            """Return this fake session."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Close this fake session."""
            return None

        def get(self, _url, *, params):
            """Return one fake page and record request parameters."""
            self.params.append(dict(params))
            return FakeResponse(
                {
                    "items": [
                        {"name": "events/date=2026-01-01/hour=00/a.json"},
                        {"name": "events/date=2026-01-01/hour=00/nested/ignored.json"},
                        {"name": "events/date=2026-01-01/hour=01/b.txt"},
                        {"name": "events/date=2026-01-01/hour=02/c.json"},
                    ]
                }
            )

    fake_session = FakeSession()

    async def fake_session_factory(headers):
        """Return the fake GCS session."""
        assert headers["Authorization"] == "Bearer token"
        return fake_session

    monkeypatch.setattr(async_remote_io, "_gcs_token", lambda: "token")
    monkeypatch.setattr(async_remote_io, "_aiohttp_session", fake_session_factory)

    result = async_remote_io._run_async(
        async_remote_io._gcs_directories_containing_files(
            [
                "gs://bucket/events/date=2026-01-01/hour=00",
                "gs://bucket/events/date=2026-01-01/hour=01",
                "gs://bucket/events/date=2026-01-01/hour=02",
            ],
            (".json",),
        )
    )

    assert result == {
        "gs://bucket/events/date=2026-01-01/hour=00": True,
        "gs://bucket/events/date=2026-01-01/hour=01": False,
        "gs://bucket/events/date=2026-01-01/hour=02": True,
    }
    assert len(fake_session.params) == 1
    assert fake_session.params[0]["prefix"] == "events/date=2026-01-01/"


def test_remote_s3_bulk_directory_discovery_groups_parent_prefixes(monkeypatch) -> None:
    """Verify S3 source discovery can check sibling partition directories in one listing."""
    from schema_sanitizer.api_impl import async_remote_io

    class FakeS3Client:
        """Minimal async S3 client with one parent-prefix listing."""

        def __init__(self):
            """Initialize captured calls."""
            self.calls: list[dict[str, object]] = []

        async def __aenter__(self):
            """Return this fake client."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Close this fake client."""
            return None

        async def list_objects_v2(self, **kwargs):
            """Return one fake page and record request parameters."""
            self.calls.append(dict(kwargs))
            return {
                "Contents": [
                    {"Key": "events/date=2026-01-01/hour=00/a.json"},
                    {"Key": "events/date=2026-01-01/hour=00/nested/ignored.json"},
                    {"Key": "events/date=2026-01-01/hour=01/b.txt"},
                    {"Key": "events/date=2026-01-01/hour=02/c.json"},
                ],
                "IsTruncated": False,
            }

    fake_client = FakeS3Client()

    async def fake_s3_client():
        """Return the fake S3 client."""
        return fake_client

    monkeypatch.setattr(async_remote_io, "_s3_client", fake_s3_client)

    result = async_remote_io._run_async(
        async_remote_io._s3_directories_containing_files(
            [
                "s3://bucket/events/date=2026-01-01/hour=00",
                "s3://bucket/events/date=2026-01-01/hour=01",
                "s3://bucket/events/date=2026-01-01/hour=02",
            ],
            (".json",),
        )
    )

    assert result == {
        "s3://bucket/events/date=2026-01-01/hour=00": True,
        "s3://bucket/events/date=2026-01-01/hour=01": False,
        "s3://bucket/events/date=2026-01-01/hour=02": True,
    }
    assert fake_client.calls == [
        {
            "Bucket": "bucket",
            "Prefix": "events/date=2026-01-01/",
            "MaxKeys": 1000,
        }
    ]


def test_remote_azure_bulk_directory_discovery_groups_parent_prefixes(monkeypatch) -> None:
    """Verify Azure source discovery can check sibling partition directories in one listing."""
    from types import SimpleNamespace

    from schema_sanitizer.api_impl import async_remote_io

    class FakeContainer:
        """Minimal async Azure container client."""

        def __init__(self):
            """Initialize captured prefixes."""
            self.prefixes: list[str] = []

        async def list_blobs(self, *, name_starts_with):
            """Yield fake blobs and record request prefix."""
            self.prefixes.append(name_starts_with)
            for name in [
                "events/date=2026-01-01/hour=00/a.json",
                "events/date=2026-01-01/hour=00/nested/ignored.json",
                "events/date=2026-01-01/hour=01/b.txt",
                "events/date=2026-01-01/hour=02/c.json",
            ]:
                yield SimpleNamespace(name=name)

    class FakeService:
        """Minimal async Azure blob service."""

        def __init__(self):
            """Initialize fake container and close flag."""
            self.container = FakeContainer()
            self.closed = False

        def get_container_client(self, container_name):
            """Return the fake container."""
            assert container_name == "container"
            return self.container

        async def close(self):
            """Mark the fake service closed."""
            self.closed = True

    fake_service = FakeService()

    async def fake_azure_service(ref):
        """Return the fake Azure service."""
        assert ref.account_url == "https://account.blob.core.windows.net"
        return fake_service

    monkeypatch.setattr(async_remote_io, "_azure_service", fake_azure_service)

    result = async_remote_io._run_async(
        async_remote_io._azure_directories_containing_files(
            [
                "az://account/container/events/date=2026-01-01/hour=00",
                "az://account/container/events/date=2026-01-01/hour=01",
                "az://account/container/events/date=2026-01-01/hour=02",
            ],
            (".json",),
        )
    )

    assert result == {
        "az://account/container/events/date=2026-01-01/hour=00": True,
        "az://account/container/events/date=2026-01-01/hour=01": False,
        "az://account/container/events/date=2026-01-01/hour=02": True,
    }
    assert fake_service.container.prefixes == ["events/date=2026-01-01/"]
    assert fake_service.closed is True


def test_remote_s3_directory_listing_reads_all_pages(monkeypatch) -> None:
    """Verify S3 remote directory listing follows continuation tokens."""
    from schema_sanitizer.api_impl import async_remote_io

    class FakeS3Client:
        """Minimal async S3 client with paginated list_objects_v2 responses."""

        def __init__(self):
            """Seed two pages where only page two has matching files."""
            self.calls: list[dict[str, object]] = []

        async def __aenter__(self):
            """Return this fake client."""
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            """Close this fake client."""
            return None

        async def list_objects_v2(self, **kwargs):
            """Return one fake S3 list page."""
            self.calls.append(kwargs)
            if "ContinuationToken" not in kwargs:
                return {
                    "IsTruncated": True,
                    "NextContinuationToken": "page-2",
                    "Contents": [{"Key": "events/ignore.txt", "Size": 4}],
                }
            assert kwargs["ContinuationToken"] == "page-2"
            return {
                "IsTruncated": False,
                "Contents": [{"Key": "events/row.json", "Size": 7}],
            }

    fake_client = FakeS3Client()

    async def fake_s3_client():
        """Return the fake S3 client context manager."""
        return fake_client

    monkeypatch.setattr(async_remote_io, "_s3_client", fake_s3_client)

    files = async_remote_io._run_async(async_remote_io._s3_list("s3://bucket/events/", (".json",)))

    assert files == [async_remote_io.RemoteFile("s3://bucket/events/row.json", "row.json", 7)]
    assert fake_client.calls[0]["Prefix"] == "events/"
    assert "ContinuationToken" not in fake_client.calls[0]
    assert fake_client.calls[1]["ContinuationToken"] == "page-2"


def test_remote_json_directory_preparation_uses_lazy_native_source_stage(
    monkeypatch, tmp_path
) -> None:
    """Verify UTF-8 remote JSON directories stage native child sources lazily."""
    from schema_sanitizer.api_impl import public_input
    from schema_sanitizer.api_impl.async_remote_io import RemoteFile, StagedPath
    from schema_sanitizer.api_impl.source_batch import path_source_tuples
    from schema_sanitizer.api_impl.source_plan import remote_native_multisource_manifest_from_data

    staged_calls = []

    def fake_list_remote_directory_files(uri, suffixes):
        """Return deterministic remote children without staging them."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".json",)
        return [
            RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
            RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
        ]

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage only the requested remote chunk."""
        assert kwargs["memory_limit_bytes"] is None
        staged_calls.append([file.name for file in files])
        staged_dir = tmp_path / f"staged-{len(staged_calls)}"
        staged_dir.mkdir()
        for file in files:
            (staged_dir / file.name).write_text('{"a":1}\n', encoding="utf-8")
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    monkeypatch.setattr(
        public_input,
        "list_remote_directory_files",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        public_input,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )
    monkeypatch.setenv("SCHEMA_SANITIZER_REMOTE_STAGE_FILES", "1")

    prepared = public_input.prepare_public_input(
        "s3://bucket/partition/",
        input_format="json",
        input_mode="directory",
        input_text_encoding="utf-8",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
    )
    try:
        assert prepared.format == "json"
        assert prepared.source == "stream"
        manifest = remote_native_multisource_manifest_from_data(prepared.data)
        assert manifest is not None
        assert staged_calls == []
        first = manifest.stage_chunk(0)
        assert first is not None
        assert staged_calls == [["a.json"]]
        assert first.manifest.source_batch is not None
        assert path_source_tuples(first.manifest.source_batch) == [
            (
                "json",
                str(tmp_path / "staged-1" / "a.json"),
                "s3://bucket/partition/a.json",
            )
        ]
        first.close()
    finally:
        prepared.close()


def test_discovered_remote_json_directory_uses_same_lazy_source_plan(
    monkeypatch,
) -> None:
    """Verify pre-discovered remote directories reuse the canonical remote source-plan path."""
    from schema_sanitizer.api_impl import public_input, source_plan
    from schema_sanitizer.api_impl.async_remote_io import RemoteFile
    from schema_sanitizer.api_impl.public_input import (
        DiscoveredDirectoryInput,
        discovered_directory_inputs,
    )

    files = (
        RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
        RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
    )

    def fail_listing(*_args, **_kwargs):
        """Fail if discovered remote inputs are listed again."""
        raise AssertionError("discovered remote directory should not be relisted")

    monkeypatch.setattr(public_input, "list_remote_directory_files", fail_listing)
    monkeypatch.setenv("SCHEMA_SANITIZER_REMOTE_STAGE_FILES", "7")

    with discovered_directory_inputs(
        {
            "s3://bucket/partition/": DiscoveredDirectoryInput(
                input_format="json",
                remote_files=files,
            )
        }
    ):
        prepared = public_input.prepare_public_input(
            "s3://bucket/partition/",
            input_format="json",
            input_mode="directory",
            input_text_encoding="utf-8",
            xml_row_tag=None,
            csv_delimiter=",",
            csv_has_header=True,
            memory_limit_bytes=None,
        )

    try:
        plan = source_plan.source_plan_from_data(prepared.data)
        manifest = source_plan.remote_native_multisource_manifest_from_data(prepared.data)
        assert prepared.format == "json"
        assert prepared.source == "stream"
        assert plan is not None
        assert plan.kind == source_plan.REMOTE_CHUNKS
        assert plan.route_name == "remote_native_manifest_chunks"
        assert manifest is not None
        assert manifest.files == list(files)
        assert manifest.chunk_size == 7
    finally:
        prepared.close()


def test_remote_chunk_prefetch_iterator_stages_next_chunk_and_cleans_up() -> None:
    """Verify remote chunk prefetch overlaps staging and closes unused chunks."""
    from schema_sanitizer.api_impl.public_input import iter_staged_remote_chunks

    class FakeStaged:
        """Fake staged chunk with cleanup tracking."""

        def __init__(self, start: int):
            """Store chunk start."""
            self.start = start
            self.closed = False

        def close(self) -> None:
            """Mark the staged chunk as closed."""
            self.closed = True

    class FakeManifest:
        """Fake remote manifest with chunk staging hooks."""

        chunk_size = 1
        files = [object(), object()]
        input_format = "json"

        def __init__(self) -> None:
            """Initialize call tracking."""
            self.calls: list[int] = []
            self.staged: dict[int, FakeStaged] = {}
            self.second_started = threading.Event()

        def stage_chunk(self, start: int) -> FakeStaged:
            """Return one fake staged chunk."""
            self.calls.append(start)
            if start == 1:
                self.second_started.set()
            staged = FakeStaged(start)
            self.staged[start] = staged
            return staged

    manifest = FakeManifest()
    with iter_staged_remote_chunks(manifest, prefetch_chunks=1) as chunks:
        first = next(chunks)
        assert first.start == 0
        assert manifest.second_started.wait(timeout=2.0)
        assert manifest.calls == [0, 1]
        assert manifest.staged[1].closed is False

    assert manifest.staged[0].closed is False
    assert manifest.staged[1].closed is True


def test_remote_json_directory_to_jsonl_uses_bounded_registry_staging(
    monkeypatch, tmp_path
) -> None:
    """Verify remote JSONL output retains only bounded staged registry chunks."""
    require_native()
    pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl import public_input
    from schema_sanitizer.api_impl.async_remote_io import RemoteFile, StagedPath

    staged_calls = []

    def fake_list_remote_directory_files(uri, suffixes):
        """Return two remote JSON children."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".json",)
        return [
            RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
            RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
        ]

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage only the requested chunk into local child files."""
        assert kwargs["memory_limit_bytes"] is None
        staged_calls.append([file.name for file in files])
        staged_dir = tmp_path / f"remote-stage-{len(staged_calls)}"
        staged_dir.mkdir()
        for file in files:
            value = file.name.removesuffix(".json")
            (staged_dir / file.name).write_text(
                json.dumps({"id": value}) + "\n",
                encoding="utf-8",
            )
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    monkeypatch.setattr(
        public_input,
        "list_remote_directory_files",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        public_input,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )
    monkeypatch.setenv("SCHEMA_SANITIZER_REMOTE_STAGE_FILES", "1")
    monkeypatch.setenv("SCHEMA_SANITIZER_REMOTE_RETAINED_STAGE_CHUNKS", "1")

    out_path = tmp_path / "out.jsonl"
    ss.to_jsonl(
        "s3://bucket/partition/",
        out_path,
        input_format="json",
        input_mode="directory",
    )

    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
    assert [row["id"] for row in rows] == ["a", "b"]
    assert [row["source_file"] for row in rows] == [
        "s3://bucket/partition/a.json",
        "s3://bucket/partition/b.json",
    ]
    assert staged_calls == [["a.json"], ["b.json"], ["a.json"], ["b.json"]]


def test_remote_json_directory_to_pyarrow_uses_bounded_registry_staging(
    monkeypatch,
    tmp_path,
) -> None:
    """Verify remote analytical conversion stages chunks but streams them natively."""
    require_native()
    pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl import public_input
    from schema_sanitizer.api_impl.async_remote_io import RemoteFile, StagedPath

    staged_calls = []

    def fake_list_remote_directory_files(uri, suffixes):
        """Return two remote JSON children."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".json",)
        return [
            RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
            RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
        ]

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage each requested chunk into local child files."""
        assert kwargs["memory_limit_bytes"] is None
        staged_calls.append([file.name for file in files])
        staged_dir = tmp_path / f"remote-arrow-stage-{len(staged_calls)}"
        staged_dir.mkdir()
        for file in files:
            value = file.name.removesuffix(".json")
            (staged_dir / file.name).write_text(
                json.dumps({"id": value}) + "\n",
                encoding="utf-8",
            )
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    monkeypatch.setattr(
        public_input,
        "list_remote_directory_files",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        public_input,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )

    monkeypatch.setenv("SCHEMA_SANITIZER_REMOTE_STAGE_FILES", "1")
    monkeypatch.setenv("SCHEMA_SANITIZER_REMOTE_RETAINED_STAGE_CHUNKS", "1")

    result = ss.to_pyarrow(
        "s3://bucket/partition/",
        input_format="json",
        input_mode="directory",
    )

    rows = result.clean_data.to_pylist()
    assert [row["id"] for row in rows] == ["a", "b"]
    assert [row["source_file"] for row in rows] == [
        "s3://bucket/partition/a.json",
        "s3://bucket/partition/b.json",
    ]
    assert staged_calls == [["a.json"], ["b.json"], ["a.json"], ["b.json"]]


def test_remote_json_directory_to_jsonl_uses_bounded_staging_with_registry(
    monkeypatch,
    tmp_path,
) -> None:
    """Verify remote registry-backed writes avoid all-chunks staged retention."""
    require_native()
    pytest.importorskip("pyarrow")
    from schema_sanitizer.api_impl import public_input, source_plan
    from schema_sanitizer.api_impl.async_remote_io import RemoteFile, StagedPath

    registry_seed = tmp_path / "registry-seed.json"
    registry_seed.write_text('{"id": "seed"}\n', encoding="utf-8")
    registry_json = ss.to_pyarrow(registry_seed, input_format="json").schema_registry_json
    staged_calls = []

    def fake_list_remote_directory_files(uri, suffixes):
        """Return two remote JSON children."""
        assert uri == "s3://bucket/partition/"
        assert suffixes == (".json",)
        return [
            RemoteFile("s3://bucket/partition/a.json", "a.json", 8),
            RemoteFile("s3://bucket/partition/b.json", "b.json", 8),
        ]

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage each requested remote chunk into local child files."""
        assert kwargs["memory_limit_bytes"] is None
        staged_calls.append([file.name for file in files])
        staged_dir = tmp_path / f"remote-single-pass-stage-{len(staged_calls)}"
        staged_dir.mkdir()
        for file in files:
            value = file.name.removesuffix(".json")
            (staged_dir / file.name).write_text(
                json.dumps({"id": value}) + "\n",
                encoding="utf-8",
            )
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    registry_stream_calls = 0
    real_registry_stream = source_plan.open_source_plan_registry_stream

    def tracking_registry_stream(*args, **kwargs):
        """Track native registry stream use while preserving behavior."""
        nonlocal registry_stream_calls
        registry_stream_calls += 1
        return real_registry_stream(*args, **kwargs)

    monkeypatch.setattr(
        public_input,
        "list_remote_directory_files",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        public_input,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )
    monkeypatch.setattr(
        source_plan,
        "open_source_plan_registry_stream",
        tracking_registry_stream,
    )
    monkeypatch.setenv("SCHEMA_SANITIZER_REMOTE_STAGE_FILES", "1")
    monkeypatch.setenv("SCHEMA_SANITIZER_REMOTE_RETAINED_STAGE_CHUNKS", "1")

    out_path = tmp_path / "remote-single-pass.jsonl"
    ss.to_jsonl(
        "s3://bucket/partition/",
        out_path,
        input_format="json",
        input_mode="directory",
        schema_registry=registry_json,
    )

    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
    assert registry_stream_calls == 1
    assert [row["id"] for row in rows] == ["a", "b"]
    assert staged_calls == [["a.json"], ["b.json"], ["a.json"], ["b.json"]]


def test_remote_registry_stream_prefers_bounded_retention_without_auto_registry(
    monkeypatch,
) -> None:
    """Verify remote registry streams retain bounded chunks instead of spooling all."""
    from types import SimpleNamespace

    from schema_sanitizer.api_impl import source_plan
    from schema_sanitizer.api_impl.async_remote_io import RemoteFile
    from schema_sanitizer.api_impl.public_input import (
        NativeDirectorySourceFile,
        NativeDirectorySourceManifest,
        RemoteNativeDirectorySourceManifest,
    )

    closed: list[str] = []

    class FakeRaw:
        """Minimal raw stream returned by the registry sink."""

        diagnostics = {"route": "spooled"}
        native_registry_state = "provider-state"

        def __init__(self, provider=None) -> None:
            """Initialize idempotent close tracking."""
            self._provider = provider
            self._closed = False

        def close(self) -> None:
            """Record one raw close."""
            if self._closed:
                return
            self._closed = True
            closed.append("raw")
            if self._provider is not None:
                self._provider.close()

    class FakeStage:
        """One staged remote chunk."""

        def __init__(self, name: str) -> None:
            """Build a fake staged manifest for one source file."""
            self.name = name
            self.manifest = NativeDirectorySourceManifest(
                files=[
                    NativeDirectorySourceFile(
                        path=f"/tmp/{name}.json",
                        source_file=f"s3://bucket/{name}.json",
                    )
                ],
                input_format="json",
            )

        def close(self) -> None:
            """Record staged chunk cleanup."""
            closed.append(self.name)

    class FakeStagedChunks:
        """Context manager yielding fake staged chunks."""

        def __init__(self, stages: list[FakeStage]) -> None:
            """Store staged chunks."""
            self._stages = stages

        def __enter__(self) -> FakeStagedChunks:
            """Return this iterator."""
            return self

        def __exit__(self, _exc_type, _exc, _tb) -> bool:
            """Propagate exceptions."""
            return False

        def __iter__(self):
            """Iterate over staged chunks."""
            return iter(self._stages)

    class FakeRawContext:
        """Raw context without auto-registry support."""

        _accepts_native_path_source_plan = False

        def __init__(self) -> None:
            """Initialize captured native source calls."""
            self.probe_sources: list[list[tuple[str, str, str]]] = []
            self.probe_registries: list[str] = []
            self.probe_states: list[object | None] = []
            self.provider_calls = 0

        def registry_probe_path_sources_best_effort(
            self,
            sources,
            call_options,
            *,
            registry_json,
            field_name_policy,
            schema_mode,
            native_registry_state=None,
        ):
            """Capture the probe source list."""
            assert call_options == "options"
            assert field_name_policy == "lower_snake"
            assert schema_mode == "additive"
            self.probe_sources.append(list(sources))
            self.probe_registries.append(registry_json)
            self.probe_states.append(native_registry_state)
            return SimpleNamespace(
                schema_registry_json='{"bounded":true}',
                schema_drifts_json="[]",
                conversion_timestamp="2026-07-05T00:00:00Z",
                field_names=("id",),
                native_registry_state=f"compiled-registry-{len(self.probe_sources)}",
            )

        @staticmethod
        def supports_path_source_chunk_provider():
            """Report native provider support."""
            return True

        def to_registry_sink_path_source_chunk_provider(
            self,
            sink,
            provider,
            call_options,
            *,
            native_registry_state,
            schema_mode,
            first_row_columns,
            timestamp_columns,
        ):
            """Capture the provider handoff."""
            assert sink == "stream"
            assert call_options == "options"
            assert native_registry_state == "compiled-registry-2"
            assert schema_mode == "additive"
            assert first_row_columns["schema_registry"] == '{"bounded":true}'
            assert first_row_columns["schema_drifts"] == "[]"
            assert timestamp_columns == ("ingestion_timestamp",)
            self.provider_calls += 1
            return FakeRaw(provider)

    stages = [FakeStage("a"), FakeStage("b")]
    raw_context = FakeRawContext()
    manifest = RemoteNativeDirectorySourceManifest(
        [
            RemoteFile("s3://bucket/a.json", "a.json", None),
            RemoteFile("s3://bucket/b.json", "b.json", None),
        ],
        input_format="json",
        chunk_size=1,
    )
    plan = source_plan.NativeSourcePlan(
        kind=source_plan.REMOTE_CHUNKS,
        payload=manifest,
        input_format="json",
        route_name="remote_native_manifest_chunks",
    )

    monkeypatch.setattr(
        source_plan,
        "iter_staged_remote_chunks",
        lambda _manifest: FakeStagedChunks(stages),
    )
    monkeypatch.setenv("SCHEMA_SANITIZER_REMOTE_RETAINED_STAGE_CHUNKS", "1")

    opened = source_plan.open_source_plan_registry_stream(
        raw_context,
        plan,
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
        first_row_columns={},
        timestamp_columns=("ingestion_timestamp",),
        feature="test",
    )

    expected_sources = [
        ("json", "/tmp/a.json", "s3://bucket/a.json"),
        ("json", "/tmp/b.json", "s3://bucket/b.json"),
    ]
    assert opened is not None
    assert opened.schema_registry_json == '{"bounded":true}'
    assert raw_context.probe_sources == [[expected_sources[0]], [expected_sources[1]]]
    assert raw_context.probe_registries == ["{}", '{"bounded":true}']
    assert raw_context.probe_states == [None, "compiled-registry-1"]
    assert raw_context.provider_calls == 1
    assert opened.native_registry_state == "provider-state"
    assert closed == ["b"]
    opened.close()
    assert closed == ["b", "raw", "a"]


def test_remote_registry_stream_prefers_native_auto_chunk_provider(
    monkeypatch,
) -> None:
    """Remote registry output should prefer native paired-provider auto-registry."""
    from schema_sanitizer.api_impl import source_plan
    from schema_sanitizer.api_impl.async_remote_io import RemoteFile
    from schema_sanitizer.api_impl.public_input import (
        NativeDirectorySourceFile,
        NativeDirectorySourceManifest,
        RemoteNativeDirectorySourceManifest,
    )

    events: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []

    def fake_path_source_plan_create(sources):
        """Return a visible native-plan stand-in."""
        chunk = tuple(sources)
        native_chunks.append(chunk)
        return ("native-plan", chunk)

    monkeypatch.setattr(
        source_plan.PATH_SOURCE_PLAN_CREATE,
        "get",
        lambda: fake_path_source_plan_create,
    )

    class FakeRaw:
        """Minimal raw stream returned by the auto-registry provider sink."""

        diagnostics = {"route": "auto-provider"}
        native_registry_state = "auto-state"
        schema_registry_json = '{"auto":true}'
        schema_drifts_json = "[]"

        def __init__(self, stream_provider) -> None:
            """Store the provider owned by the raw stream."""
            self._stream_provider = stream_provider

        def close(self) -> None:
            """Close the stream provider."""
            events.append("raw-close")
            self._stream_provider.close()

    class FakeStage:
        """One staged remote chunk."""

        def __init__(self, name: str) -> None:
            """Build a fake staged manifest for one source file."""
            self.name = name
            self.manifest = NativeDirectorySourceManifest(
                files=[
                    NativeDirectorySourceFile(
                        path=f"/tmp/{name}.json",
                        source_file=f"s3://bucket/{name}.json",
                    )
                ],
                input_format="json",
            )

        def close(self) -> None:
            """Record staged chunk cleanup."""
            events.append(f"close:{self.name}")

    class FakeStagedChunks:
        """Context manager yielding fake staged chunks."""

        def __init__(self, label: str, stages: list[FakeStage]) -> None:
            """Store staged chunks."""
            self._label = label
            self._stages = stages

        def __enter__(self):
            """Return the iterator used by the provider."""
            events.append(f"enter:{self._label}")
            return iter(self._stages)

        def __exit__(self, _exc_type, _exc, _tb) -> bool:
            """Record context cleanup."""
            events.append(f"exit:{self._label}")
            return False

    class FakeRawContext:
        """Raw context exposing native paired-provider auto-registry."""

        auto_calls = 0

        @staticmethod
        def supports_path_source_chunk_provider_auto_registry() -> bool:
            """Report auto-provider support."""
            return True

        def to_registry_sink_path_source_chunk_provider_auto_registry(
            self,
            sink,
            probe_provider,
            stream_provider,
            call_options,
            *,
            registry_json,
            field_name_policy,
            schema_mode,
            first_row_columns,
            timestamp_columns,
            native_registry_state=None,
            skip_invalid_json_sources=True,
        ):
            """Capture native auto-registry provider handoff."""
            assert sink == "stream"
            assert call_options == "options"
            assert registry_json == "{}"
            assert field_name_policy == "lower_snake"
            assert schema_mode == "additive"
            assert first_row_columns == {}
            assert timestamp_columns == ("ingestion_timestamp",)
            assert native_registry_state is None
            assert skip_invalid_json_sources is True
            self.auto_calls += 1
            while probe_provider.next_sources() is not None:
                pass
            probe_provider.close()
            return FakeRaw(stream_provider)

        def registry_probe_path_source_chunk_provider(self, *_args, **_kwargs):
            """Fail if the older provider-probe route is used."""
            raise AssertionError("provider auto-registry should run first")

    probe_stages = [FakeStage("probe-a"), FakeStage("probe-b")]
    stream_stages = [FakeStage("stream-a"), FakeStage("stream-b")]
    contexts = [FakeStagedChunks("probe", probe_stages), FakeStagedChunks("stream", stream_stages)]
    raw_context = FakeRawContext()
    manifest = RemoteNativeDirectorySourceManifest(
        [
            RemoteFile("s3://bucket/a.json", "a.json", None),
            RemoteFile("s3://bucket/b.json", "b.json", None),
        ],
        input_format="json",
        chunk_size=1,
    )
    plan = source_plan.NativeSourcePlan(
        kind=source_plan.REMOTE_CHUNKS,
        payload=manifest,
        input_format="json",
        route_name="remote_native_manifest_chunks",
    )

    monkeypatch.setattr(
        source_plan,
        "iter_staged_remote_chunks",
        lambda _manifest: contexts.pop(0),
    )

    opened = source_plan.open_source_plan_registry_stream(
        raw_context,
        plan,
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
        first_row_columns={},
        timestamp_columns=("ingestion_timestamp",),
        feature="test",
    )

    assert opened is not None
    assert opened.schema_registry_json == '{"auto":true}'
    assert opened.schema_drifts_json == "[]"
    assert opened.native_registry_state == "auto-state"
    assert raw_context.auto_calls == 1
    assert events == ["enter:probe", "close:probe-a", "close:probe-b", "exit:probe"]
    opened.close()
    assert events == [
        "enter:probe",
        "close:probe-a",
        "close:probe-b",
        "exit:probe",
        "raw-close",
    ]


def test_remote_registry_stream_uses_native_probe_chunk_provider(
    monkeypatch,
) -> None:
    """Remote registry inference should hand lazy chunks to native when supported."""
    from types import SimpleNamespace

    from schema_sanitizer.api_impl import source_plan
    from schema_sanitizer.api_impl.async_remote_io import RemoteFile
    from schema_sanitizer.api_impl.public_input import (
        NativeDirectorySourceFile,
        NativeDirectorySourceManifest,
        RemoteNativeDirectorySourceManifest,
    )

    events: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []

    def fake_path_source_plan_create(sources):
        """Return a visible native-plan stand-in."""
        chunk = tuple(sources)
        native_chunks.append(chunk)
        return ("native-plan", chunk)

    monkeypatch.setattr(
        source_plan.PATH_SOURCE_PLAN_CREATE,
        "get",
        lambda: fake_path_source_plan_create,
    )

    class FakeRaw:
        """Minimal raw stream returned by the registry sink."""

        diagnostics = {"route": "provider-probe"}
        native_registry_state = "stream-state"

        def __init__(self, provider=None) -> None:
            """Store provider for close propagation."""
            self._provider = provider

        def close(self) -> None:
            """Close raw resources."""
            events.append("raw-close")
            if self._provider is not None:
                self._provider.close()

    class FakeStage:
        """One staged remote chunk."""

        def __init__(self, name: str) -> None:
            """Build a fake staged manifest for one source file."""
            self.name = name
            self.manifest = NativeDirectorySourceManifest(
                files=[
                    NativeDirectorySourceFile(
                        path=f"/tmp/{name}.json",
                        source_file=f"s3://bucket/{name}.json",
                    )
                ],
                input_format="json",
            )

        def close(self) -> None:
            """Record staged chunk cleanup."""
            events.append(f"close:{self.name}")

    class FakeStagedChunks:
        """Context manager yielding fake staged chunks."""

        def __init__(self, stages: list[FakeStage]) -> None:
            """Store staged chunks."""
            self._stages = stages

        def __enter__(self):
            """Return the iterator used by the provider."""
            events.append("enter")
            return iter(self._stages)

        def __exit__(self, _exc_type, _exc, _tb) -> bool:
            """Record context cleanup."""
            events.append("exit")
            return False

    class FakeRawContext:
        """Raw context exposing native probe and output providers."""

        def __init__(self) -> None:
            """Initialize captured calls."""
            self.probe_sources: list[list[tuple[str, str, str]]] = []
            self.output_provider_calls = 0

        @staticmethod
        def supports_registry_probe_path_source_chunk_provider() -> bool:
            """Report native provider-probe support."""
            return True

        @staticmethod
        def supports_path_source_chunk_provider() -> bool:
            """Report native output-provider support."""
            return True

        def registry_probe_path_source_chunk_provider(
            self,
            provider,
            call_options,
            *,
            registry_json,
            field_name_policy,
            schema_mode,
            native_registry_state=None,
            skip_invalid_json_sources=True,
        ):
            """Simulate native consuming all probe chunks."""
            assert call_options == "options"
            assert registry_json == "{}"
            assert field_name_policy == "lower_snake"
            assert schema_mode == "additive"
            assert native_registry_state is None
            assert skip_invalid_json_sources is True
            while True:
                sources = provider.next_sources()
                if sources is None:
                    break
                self.probe_sources.append(sources)
            provider.close()
            return SimpleNamespace(
                schema_registry_json='{"native_provider":true}',
                schema_drifts_json="[]",
                conversion_timestamp="2026-07-05T00:00:00Z",
                field_names=("id",),
                native_registry_state="compiled-provider-registry",
            )

        def to_registry_sink_path_source_chunk_provider(
            self,
            sink,
            provider,
            call_options,
            *,
            native_registry_state,
            schema_mode,
            first_row_columns,
            timestamp_columns,
        ):
            """Capture native output provider handoff."""
            assert sink == "stream"
            assert call_options == "options"
            assert native_registry_state == "compiled-provider-registry"
            assert schema_mode == "additive"
            assert first_row_columns["schema_registry"] == '{"native_provider":true}'
            assert first_row_columns["schema_drifts"] == "[]"
            assert timestamp_columns == ("ingestion_timestamp",)
            self.output_provider_calls += 1
            return FakeRaw(provider)

    stages = [FakeStage("a"), FakeStage("b")]
    raw_context = FakeRawContext()
    manifest = RemoteNativeDirectorySourceManifest(
        [
            RemoteFile("s3://bucket/a.json", "a.json", None),
            RemoteFile("s3://bucket/b.json", "b.json", None),
        ],
        input_format="json",
        chunk_size=1,
    )
    plan = source_plan.NativeSourcePlan(
        kind=source_plan.REMOTE_CHUNKS,
        payload=manifest,
        input_format="json",
        route_name="remote_native_manifest_chunks",
    )

    monkeypatch.setattr(
        source_plan,
        "iter_staged_remote_chunks",
        lambda _manifest: FakeStagedChunks(stages),
    )

    opened = source_plan.open_source_plan_registry_stream(
        raw_context,
        plan,
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
        first_row_columns={},
        timestamp_columns=("ingestion_timestamp",),
        feature="test",
    )

    assert opened is not None
    assert opened.schema_registry_json == '{"native_provider":true}'
    assert raw_context.probe_sources == [
        ("native-plan", (("json", "/tmp/a.json", "s3://bucket/a.json"),)),
        ("native-plan", (("json", "/tmp/b.json", "s3://bucket/b.json"),)),
    ]
    assert native_chunks[:2] == [
        (("json", "/tmp/a.json", "s3://bucket/a.json"),),
        (("json", "/tmp/b.json", "s3://bucket/b.json"),),
    ]
    assert raw_context.output_provider_calls == 1
    assert events == ["enter", "close:b", "exit"]
    opened.close()
    assert events == ["enter", "close:b", "exit", "raw-close", "close:a"]


def test_remote_registry_stream_uses_native_retained_window_when_complete(
    monkeypatch,
) -> None:
    """Verify fully retained remote chunks stream through one native path-source sink."""
    from types import SimpleNamespace

    from schema_sanitizer.api_impl import source_plan
    from schema_sanitizer.api_impl.async_remote_io import RemoteFile
    from schema_sanitizer.api_impl.public_input import (
        NativeDirectorySourceFile,
        NativeDirectorySourceManifest,
        RemoteNativeDirectorySourceManifest,
    )

    closed: list[str] = []

    class FakeRaw:
        """Minimal raw stream returned by the native registry sink."""

        diagnostics = {"route": "native-retained"}
        native_registry_state = "stream-state"

        def __init__(self, provider=None) -> None:
            """Initialize idempotent close tracking."""
            self._provider = provider
            self._closed = False

        def close(self) -> None:
            """Record one raw close."""
            if self._closed:
                return
            self._closed = True
            closed.append("raw")
            if self._provider is not None:
                self._provider.close()

    class FakeStage:
        """One retained staged remote chunk."""

        def __init__(self, name: str) -> None:
            """Build a fake staged manifest for one source file."""
            self.name = name
            self.manifest = NativeDirectorySourceManifest(
                files=[
                    NativeDirectorySourceFile(
                        path=f"/tmp/{name}.json",
                        source_file=f"s3://bucket/{name}.json",
                    )
                ],
                input_format="json",
            )

        def close(self) -> None:
            """Record staged chunk cleanup."""
            closed.append(self.name)

    class FakeStagedChunks:
        """Context manager yielding fake staged chunks."""

        def __init__(self, stages: list[FakeStage]) -> None:
            """Store staged chunks."""
            self._stages = stages

        def __enter__(self) -> FakeStagedChunks:
            """Return this iterator."""
            return self

        def __exit__(self, _exc_type, _exc, _tb) -> bool:
            """Propagate exceptions."""
            return False

        def __iter__(self):
            """Iterate over staged chunks."""
            return iter(self._stages)

    class FakeRawContext:
        """Raw context without auto-registry support."""

        _accepts_native_path_source_plan = False

        def __init__(self) -> None:
            """Initialize captured native calls."""
            self.probe_sources: list[list[tuple[str, str, str]]] = []
            self.provider_calls = 0

        def registry_probe_path_sources_best_effort(
            self,
            sources,
            call_options,
            *,
            registry_json,
            field_name_policy,
            schema_mode,
            native_registry_state=None,
        ):
            """Capture the probe source list."""
            assert call_options == "options"
            assert field_name_policy == "lower_snake"
            assert schema_mode == "additive"
            assert registry_json in {"{}", '{"bounded":true}'}
            self.probe_sources.append(list(sources))
            return SimpleNamespace(
                schema_registry_json='{"bounded":true}',
                schema_drifts_json="[]",
                conversion_timestamp="2026-07-05T00:00:00Z",
                field_names=("id",),
                native_registry_state=f"compiled-registry-{len(self.probe_sources)}",
            )

        @staticmethod
        def supports_path_source_chunk_provider():
            """Report native provider support."""
            return True

        def to_registry_sink_path_source_chunk_provider(
            self,
            sink,
            provider,
            call_options,
            *,
            native_registry_state,
            schema_mode,
            first_row_columns,
            timestamp_columns,
        ):
            """Capture the provider handoff."""
            assert sink == "stream"
            assert call_options == "options"
            assert native_registry_state == "compiled-registry-2"
            assert schema_mode == "additive"
            assert first_row_columns["schema_registry"] == '{"bounded":true}'
            assert first_row_columns["schema_drifts"] == "[]"
            assert timestamp_columns == ("ingestion_timestamp",)
            self.provider_calls += 1
            return FakeRaw(provider)

    stages = [FakeStage("a"), FakeStage("b")]
    raw_context = FakeRawContext()
    manifest = RemoteNativeDirectorySourceManifest(
        [
            RemoteFile("s3://bucket/a.json", "a.json", None),
            RemoteFile("s3://bucket/b.json", "b.json", None),
        ],
        input_format="json",
        chunk_size=1,
    )
    plan = source_plan.NativeSourcePlan(
        kind=source_plan.REMOTE_CHUNKS,
        payload=manifest,
        input_format="json",
        route_name="remote_native_manifest_chunks",
    )

    monkeypatch.setattr(
        source_plan,
        "iter_staged_remote_chunks",
        lambda _manifest: FakeStagedChunks(stages),
    )
    monkeypatch.setenv("SCHEMA_SANITIZER_REMOTE_RETAINED_STAGE_CHUNKS", "2")

    opened = source_plan.open_source_plan_registry_stream(
        raw_context,
        plan,
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
        first_row_columns={},
        timestamp_columns=("ingestion_timestamp",),
        feature="test",
    )

    expected_sources = [
        ("json", "/tmp/a.json", "s3://bucket/a.json"),
        ("json", "/tmp/b.json", "s3://bucket/b.json"),
    ]
    assert opened is not None
    assert opened.schema_registry_json == '{"bounded":true}'
    assert opened.native_registry_state == "stream-state"
    assert raw_context.probe_sources == [[expected_sources[0]], [expected_sources[1]]]
    assert raw_context.provider_calls == 1
    opened.close()
    assert closed == ["raw", "b", "a"]


def test_source_plan_sequence_probe_flattens_path_sources_once() -> None:
    """Pure path-source sequences should use one native probe, not a Python merge loop."""
    from types import SimpleNamespace

    from schema_sanitizer.api_impl import source_plan

    calls: list[list[tuple[str, str, str]]] = []

    class FakeRawContext:
        """Raw context that captures one native path-source probe."""

        _accepts_native_path_source_plan = False

        def registry_probe_path_sources_best_effort(
            self,
            sources,
            _call_options,
            *,
            registry_json,
            field_name_policy,
            schema_mode,
        ):
            """Capture the flattened source list."""
            calls.append(list(sources))
            assert registry_json == "{}"
            assert field_name_policy == "lower_snake"
            assert schema_mode == "additive"
            return SimpleNamespace(
                schema_registry_json='{"flattened":true}',
                schema_drifts_json="[]",
                conversion_timestamp="2026-07-05T00:00:00Z",
                field_names=("id",),
            )

    plan = source_plan.NativeSourcePlan(
        kind=source_plan.SEQUENCE,
        payload=(
            source_plan.NativeSourcePlan(
                kind=source_plan.PATH_SOURCES,
                payload=[("json", "/tmp/a.json", "gs://bucket/a.json")],
                input_format="json",
                route_name="child-a",
            ),
            source_plan.NativeSourcePlan(
                kind=source_plan.PATH_SOURCES,
                payload=[("json", "/tmp/b.json", "gs://bucket/b.json")],
                input_format="json",
                route_name="child-b",
            ),
        ),
        input_format="json",
        route_name="sequence",
    )

    raw = source_plan.probe_source_plan_registry(
        FakeRawContext(),
        plan,
        "options",
        registry_json="{}",
        field_name_policy="lower_snake",
        schema_mode="additive",
    )

    assert raw.schema_registry_json == '{"flattened":true}'
    assert calls == [
        [
            ("json", "/tmp/a.json", "gs://bucket/a.json"),
            ("json", "/tmp/b.json", "gs://bucket/b.json"),
        ]
    ]


def test_source_plan_plain_stream_uses_native_path_source_payload() -> None:
    """Plain stream source plans should pass the reusable native payload capsule."""
    from schema_sanitizer.api_impl import source_plan

    native_payload = object()
    captured_sources: list[object] = []

    class FakeRawContext:
        """Raw context that supports the native path-source capsule."""

        _accepts_native_path_source_plan = True

        @staticmethod
        def supports_sink_path_sources() -> bool:
            """Report native path-source sink support."""
            return True

        def to_sink_path_sources(
            self,
            _sink,
            sources,
            _call_options,
            *,
            include_source_file,
            first_row_columns,
            timestamp_columns,
        ):
            """Capture the exact source payload passed to native."""
            assert include_source_file is True
            assert first_row_columns == {}
            assert timestamp_columns == ()
            captured_sources.append(sources)
            return "raw-stream"

    plan = source_plan.NativeSourcePlan(
        kind=source_plan.PATH_SOURCES,
        payload=[("json", "/tmp/a.json", "gs://bucket/a.json")],
        input_format="json",
        route_name="native_manifest_paths",
        native_payload=native_payload,
    )

    raw = source_plan.open_source_plan_sink_stream_or_none(
        FakeRawContext(),
        plan,
        "options",
        sink="stream",
        include_source_file=True,
        field_name_policy="lower_snake",
        feature="test",
    )

    assert raw == "raw-stream"
    assert captured_sources == [native_payload]


def test_remote_source_plan_stream_uses_native_chunk_provider(monkeypatch) -> None:
    """Remote source-plan streams should pull chunks lazily instead of flattening all chunks."""
    from schema_sanitizer.api_impl import source_plan
    from schema_sanitizer.api_impl.public_input import (
        NativeDirectorySourceFile,
        NativeDirectorySourceManifest,
        RemoteFile,
        RemoteNativeDirectorySourceManifest,
    )

    events: list[str] = []
    native_chunks: list[tuple[tuple[str, str, str], ...]] = []

    def fake_path_source_plan_create(sources):
        """Return a visible native-plan stand-in."""
        chunk = tuple(sources)
        native_chunks.append(chunk)
        return ("native-plan", chunk)

    monkeypatch.setattr(
        source_plan.PATH_SOURCE_PLAN_CREATE,
        "get",
        lambda: fake_path_source_plan_create,
    )

    class FakeStage:
        """Fake staged remote chunk."""

        def __init__(self, name: str) -> None:
            """Create a one-file staged native manifest."""
            self.name = name
            self.manifest = NativeDirectorySourceManifest(
                files=[
                    NativeDirectorySourceFile(
                        path=f"/tmp/{name}.json",
                        source_file=f"s3://bucket/{name}.json",
                    )
                ],
                input_format="json",
            )

        def close(self) -> None:
            """Record staged chunk cleanup."""
            events.append(f"close:{self.name}")

    class FakeStagedChunks:
        """Context manager yielding fake staged chunks."""

        def __init__(self, stages: list[FakeStage]) -> None:
            """Store staged chunks."""
            self._stages = stages

        def __enter__(self):
            """Record when lazy staging starts."""
            events.append("enter")
            return iter(self._stages)

        def __exit__(self, _exc_type, _exc, _tb) -> bool:
            """Record context cleanup."""
            events.append("exit")
            return False

        def __iter__(self):
            """Iterate over staged chunks."""
            return iter(self._stages)

    class FakeRaw:
        """Fake raw sink that owns the provider like native does."""

        def __init__(self, provider) -> None:
            """Store provider."""
            self.provider = provider

        def close(self) -> None:
            """Close provider through the raw sink."""
            self.provider.close()
            events.append("raw-close")

    class FakeRawContext:
        """Raw context exposing the plain native chunk-provider sink."""

        @staticmethod
        def supports_sink_path_source_chunk_provider() -> bool:
            """Report provider support."""
            return True

        def to_sink_path_source_chunk_provider(
            self,
            sink,
            provider,
            call_options,
            *,
            include_source_file,
            first_row_columns,
            timestamp_columns,
        ):
            """Capture the provider handoff."""
            assert sink == "stream"
            assert call_options == "options"
            assert include_source_file is True
            assert first_row_columns == {}
            assert timestamp_columns == ()
            events.append("provider-open")
            return FakeRaw(provider)

    stages = [FakeStage("a"), FakeStage("b")]
    plan = source_plan.NativeSourcePlan(
        kind=source_plan.REMOTE_CHUNKS,
        payload=RemoteNativeDirectorySourceManifest(
            [
                RemoteFile("s3://bucket/a.json", "a.json", None),
                RemoteFile("s3://bucket/b.json", "b.json", None),
            ],
            input_format="json",
            chunk_size=1,
        ),
        input_format="json",
        route_name="remote_native_manifest_chunks",
    )
    monkeypatch.setattr(
        source_plan,
        "iter_staged_remote_chunks",
        lambda _manifest: FakeStagedChunks(stages),
    )

    raw = source_plan.open_source_plan_sink_stream_or_none(
        FakeRawContext(),
        plan,
        "options",
        sink="stream",
        include_source_file=True,
        field_name_policy="lower_snake",
        feature="test",
    )

    assert isinstance(raw, FakeRaw)
    assert events == ["provider-open"]
    assert raw.provider.next_sources() == (
        "native-plan",
        (("json", "/tmp/a.json", "s3://bucket/a.json"),),
    )
    assert events == ["provider-open", "enter"]
    assert raw.provider.next_sources() == (
        "native-plan",
        (("json", "/tmp/b.json", "s3://bucket/b.json"),),
    )
    assert events == ["provider-open", "enter", "close:a"]
    assert native_chunks == [
        (("json", "/tmp/a.json", "s3://bucket/a.json"),),
        (("json", "/tmp/b.json", "s3://bucket/b.json"),),
    ]
    raw.close()
    assert events == [
        "provider-open",
        "enter",
        "close:a",
        "close:b",
        "exit",
        "raw-close",
    ]


def test_remote_json_directory_preparation_allows_native_non_utf8_directory(
    monkeypatch,
) -> None:
    """Verify non-UTF-8 remote directories prepare a lazy native source plan."""
    from schema_sanitizer.api_impl import async_remote_io, public_input, source_plan

    def fail_native_stage(*args, **kwargs):
        """Fail if remote directories stage eagerly during preparation."""
        raise AssertionError("remote directories should not stage during preparation")

    monkeypatch.setattr(
        public_input,
        "list_remote_directory_files",
        lambda *_args, **_kwargs: (
            async_remote_io.RemoteFile("s3://bucket/partition/row.json", "row.json"),
        ),
    )
    monkeypatch.setattr(
        public_input,
        "stage_remote_files_to_directory",
        fail_native_stage,
    )

    prepared = public_input.prepare_public_input(
        "s3://bucket/partition/",
        input_format="json",
        input_mode="directory",
        input_text_encoding="latin-1",
        xml_row_tag=None,
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
    )

    manifest = source_plan.remote_native_multisource_manifest_from_data(prepared.data)
    assert manifest is not None
    assert manifest.input_text_encoding == "latin-1"


def test_remote_directory_staging_respects_download_concurrency(monkeypatch) -> None:
    """Verify remote directory staging caps active downloads while preserving row order."""
    from schema_sanitizer.api_impl import async_remote_io

    active_downloads = 0
    max_active_downloads = 0

    files = [
        async_remote_io.RemoteFile(f"s3://bucket/partition/{index}.jsonl", f"{index}.jsonl")
        for index in range(5)
    ]

    async def fake_client(files):
        """Return a reusable fake provider client."""
        assert len(files) == 5
        return object()

    async def fake_close(client):
        """Accept closing the fake provider client."""
        assert client is not None

    async def fake_download(client, file, local_path):
        """Write a payload while tracking active download count."""
        nonlocal active_downloads, max_active_downloads
        assert client is not None
        active_downloads += 1
        max_active_downloads = max(max_active_downloads, active_downloads)
        await asyncio.sleep(0.01)
        Path(local_path).write_text(f'{{"file":"{file.name}"}}\n', encoding="utf-8")
        active_downloads -= 1

    monkeypatch.setenv("SCHEMA_SANITIZER_ASYNC_CONCURRENCY", "2")
    monkeypatch.setenv("SCHEMA_SANITIZER_ASYNC_PREFETCH_FILES", "5")
    monkeypatch.setattr(async_remote_io, "_provider_client_for_downloads", fake_client)
    monkeypatch.setattr(async_remote_io, "_close_provider_client", fake_close)
    monkeypatch.setattr(async_remote_io, "_download_one_file_to_path", fake_download)

    staged = async_remote_io.stage_remote_files_to_directory(
        files,
        memory_limit_bytes=None,
    )
    try:
        assert max_active_downloads == 2
        root = Path(staged.path)
        assert [
            (root / f"{index}.jsonl").read_text(encoding="utf-8").strip() for index in range(5)
        ] == [
            '{"file":"0.jsonl"}',
            '{"file":"1.jsonl"}',
            '{"file":"2.jsonl"}',
            '{"file":"3.jsonl"}',
            '{"file":"4.jsonl"}',
        ]
    finally:
        staged.close()


def test_remote_directory_staging_does_not_retry_memory_limit_failure(monkeypatch) -> None:
    """Verify post-download memory-limit failures are not retried as remote I/O."""
    from schema_sanitizer.api_impl import async_remote_io
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    downloads = 0

    files = [async_remote_io.RemoteFile("s3://bucket/partition/row.jsonl", "row.jsonl", None)]

    async def fake_client(files):
        """Return a reusable fake provider client."""
        assert len(files) == 1
        return object()

    async def fake_close(client):
        """Accept closing the fake provider client."""
        assert client is not None

    async def fake_download(client, file, local_path):
        """Write an oversized payload."""
        nonlocal downloads
        assert client is not None
        assert file.name == "row.jsonl"
        downloads += 1
        Path(local_path).write_bytes(b'{"payload":"too large"}\n')

    monkeypatch.setenv("SCHEMA_SANITIZER_ASYNC_RETRIES", "3")
    monkeypatch.setattr(async_remote_io, "_provider_client_for_downloads", fake_client)
    monkeypatch.setattr(async_remote_io, "_close_provider_client", fake_close)
    monkeypatch.setattr(async_remote_io, "_download_one_file_to_path", fake_download)

    with pytest.raises(SchemaSanitizerResourceError, match="memory_limit_bytes"):
        async_remote_io.stage_remote_files_to_directory(
            files,
            memory_limit_bytes=8,
        )
    assert downloads == 1


def test_read_xml_without_row_tag_emits_document_row(tmp_path) -> None:
    """Verify read xml defaults to one row for the whole document."""
    pytest.importorskip("pyarrow")
    require_native()

    path = tmp_path / "document.xml"
    path.write_text("<rows><row><a>1</a></row><row><a>2</a></row></rows>", encoding="utf-8")

    result = read_test_xml(path)

    assert result.clean_data.to_pylist() == [{"row": [{"a": "1"}, {"a": "2"}]}]


def test_read_xml_row_tag_maps_attributes_text_and_repeated_children(tmp_path) -> None:
    """Verify xml_row_tag emits deterministic JSON-like rows."""
    pytest.importorskip("pyarrow")
    require_native()

    path = tmp_path / "rows.xml"
    path.write_text(
        """
        <?xml version="1.0"?>
        <rows>
          <row id="1"><name>Ana</name><tag>x</tag><tag>y</tag></row>
          <row id="2"><name>Alex</name><note lang="en">hello</note></row>
        </rows>
        """,
        encoding="utf-8",
    )

    result = read_test_xml(path, xml_row_tag="row")

    assert result.clean_data.to_pylist() == [
        {"id": "1", "name": "Ana", "tag": ["x", "y"], "note": None},
        {"id": "2", "name": "Alex", "tag": None, "note": {"lang": "en", "text": "hello"}},
    ]


def test_xml_input_format_is_supported_by_converters(tmp_path) -> None:
    """Verify converters accept explicit xml input format."""
    require_native()

    path = tmp_path / "rows.xml"
    out = tmp_path / "rows.jsonl"
    path.write_text("<rows><row><a>1</a></row><row><a>2</a></row></rows>", encoding="utf-8")

    ss.to_jsonl(path, out, input_format="xml", xml_row_tag="row")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert [{k: v for k, v in row.items() if k not in generated} for row in rows] == [
        {"a": "1"},
        {"a": "2"},
    ]


def test_read_xml_memory_limit_rejects_large_dom_input(tmp_path) -> None:
    """Verify xml parsing respects configured memory limit."""
    require_native()

    path = tmp_path / "large.xml"
    path.write_text(
        "<rows>" + "".join(f"<row><a>{i}</a></row>" for i in range(50)) + "</rows>",
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerResourceError) as excinfo:
        read_test_xml(path, batch_memory_limit_bytes=64)

    err = excinfo.value
    assert err.detail is not None
    assert err.detail["stage"] == "xml_parse"
    assert err.detail["limit_name"] == "memory_limit_bytes"


def test_read_xml_row_tag_streams_file_larger_than_memory_limit(tmp_path) -> None:
    """Verify xml row streaming allows large files made of small row elements."""
    pytest.importorskip("pyarrow")
    require_native()

    path = tmp_path / "stream.xml"
    path.write_text(
        "<rows>" + "".join(f"<row><a>{i}</a></row>" for i in range(30)) + "</rows>",
        encoding="utf-8",
    )

    result = read_test_xml(path, xml_row_tag="row", batch_memory_limit_bytes=128)

    assert result.clean_data.num_rows == 30
    assert result.clean_data.to_pylist()[0] == {"a": "0"}
    assert result.clean_data.to_pylist()[-1] == {"a": "29"}


def test_read_xml_row_tag_streams_rows_split_across_chunks(tmp_path) -> None:
    """Verify xml row streaming handles row spans split across tiny chunks."""
    pytest.importorskip("pyarrow")
    require_native()

    path = tmp_path / "tiny_chunks.xml"
    path.write_text(
        "<rows>"
        "<row><id>1</id><payload>abcdefghijklmnopqrstuvwxyz</payload></row>"
        "<row><id>2</id><payload>ABCDEFGHIJKLMNOPQRSTUVWXYZ</payload></row>"
        "</rows>",
        encoding="utf-8",
    )

    result = read_test_xml(
        path,
        xml_row_tag="row",
        read_chunk_bytes=7,
        batch_memory_limit_bytes=256,
    )

    assert result.clean_data.to_pylist() == [
        {"id": "1", "payload": "abcdefghijklmnopqrstuvwxyz"},
        {"id": "2", "payload": "ABCDEFGHIJKLMNOPQRSTUVWXYZ"},
    ]


def test_read_xml_row_tag_rejects_single_row_larger_than_memory_limit(tmp_path) -> None:
    """Verify xml row streaming rejects one row that exceeds the active buffer limit."""
    require_native()

    path = tmp_path / "huge_row.xml"
    path.write_text(
        "<rows><row><payload>" + ("x" * 300) + "</payload></row></rows>", encoding="utf-8"
    )

    with pytest.raises(ss.SchemaSanitizerResourceError) as excinfo:
        read_test_xml(path, xml_row_tag="row", batch_memory_limit_bytes=128)

    err = excinfo.value
    assert err.detail is not None
    assert err.detail["stage"] == "xml_parse"
    assert err.detail["limit_name"] == "memory_limit_bytes"


def test_read_xml_rejects_dtd_declarations(tmp_path) -> None:
    """Verify xml parser rejects DTD/entity declarations."""
    require_native()

    path = tmp_path / "dtd.xml"
    path.write_text(
        '<!DOCTYPE rows [<!ENTITY x "boom">]><rows><row><a>&x;</a></row></rows>',
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerInvalidArgumentError, match="DTD"):
        read_test_xml(path, xml_row_tag="row")


def test_read_xml_document_mode_rejects_dtd_declarations(tmp_path) -> None:
    """Verify xml document mode rejects DTD/entity declarations."""
    require_native()

    path = tmp_path / "document_dtd.xml"
    path.write_text(
        '<!DOCTYPE rows [<!ENTITY x "boom">]><rows><row><a>&x;</a></row></rows>',
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerInvalidArgumentError, match="DTD"):
        read_test_xml(path)


def test_all_public_to_functions_share_input_options() -> None:
    """Verify analytical and file converters expose one input option contract."""
    common = set(inspect.signature(ss.to_pyarrow).parameters) - {"input_path"}
    parquet_output_options = {"parquet_compression", "parquet_gzip_level"}
    assert "schema_drift_date" not in common
    assert "schema_registry_column" not in common
    assert "schema_drifts_column" not in common
    assert "source_file_column" not in common
    assert "constant_columns" not in common
    for function in (ss.to_pandas, ss.to_polars, ss.to_duckdb):
        assert set(inspect.signature(function).parameters) - {"input_path"} == common
    for function in (ss.to_csv, ss.to_jsonl):
        assert (
            set(inspect.signature(function).parameters)
            - {
                "input_path",
                "output_path",
            }
            == common
        )
    assert (
        set(inspect.signature(ss.to_parquet).parameters)
        - {
            "input_path",
            "output_path",
        }
        == common | parquet_output_options
    )


def test_file_input_can_exceed_memory_limit_bytes(tmp_path) -> None:
    """Verify file input can exceed memory limit bytes."""
    pytest.importorskip("pyarrow")
    path = tmp_path / "data.jsonl"
    path.write_text("".join(f'{{"a":{i}}}\n' for i in range(20)), encoding="utf-8")

    result = read_test_jsonl(path, batch_memory_limit_bytes=64)

    assert result.clean_data.num_rows == 20


def test_file_like_payload_is_rejected_by_format_reader() -> None:
    """Verify file like payload is rejected by format reader."""
    with pytest.raises(TypeError):
        read_test_csv(io.BytesIO(b"a,b\n1,2\n"))


def test_public_selector_arguments_must_be_strings(tmp_path) -> None:
    """Verify public selector arguments must be strings."""
    path = tmp_path / "data.jsonl"
    path.write_text('{"a": 1}\n', encoding="utf-8")
    out = tmp_path / "out.jsonl"

    with pytest.raises(ValueError, match="auto"):
        ss.to_jsonl(path, out, input_format="auto")
    with pytest.raises(TypeError, match="input_mode"):
        ss.to_jsonl(path, out, input_format="jsonl", input_mode=None)


def test_reader_returns_result_with_clean_data(tmp_path) -> None:
    """Verify reader returns result with clean data."""
    pytest.importorskip("pyarrow")
    path = tmp_path / "data.jsonl"
    path.write_text('{"a": 1}\n', encoding="utf-8")

    result = read_test_jsonl(path, output_format="pyarrow")
    assert isinstance(result, ss.Result)
    assert result.clean_data.to_pylist() == [{"a": 1}]


def test_analytical_function_selects_clean_data_type(tmp_path) -> None:
    """Verify analytical function names select the in-memory output type."""
    path = tmp_path / "data.jsonl"
    path.write_text('{"a": 1}\n', encoding="utf-8")

    assert ss.to_pyarrow(path, input_format="jsonl").clean_data.num_rows == 1


def test_unknown_reader_parameter_is_rejected() -> None:
    """Verify unknown reader parameter is rejected."""
    with pytest.raises(TypeError, match="unknown"):
        read_test_python([{"a": 1}], unknown=True)
