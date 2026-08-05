"""Python and local input contract tests."""

# ruff: noqa: F405

from __future__ import annotations

from collections.abc import Iterator

from input_contract_shared import *  # noqa: F403


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


def test_python_rows_generator_is_spooled_and_replayable() -> None:
    """Verify one-shot iterables are replayed without retaining all row objects."""
    require_native()
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


def test_python_rows_generator_spools_incrementally() -> None:
    """A small first read must not consume an entire one-shot iterable."""
    require_native()
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


def test_python_rows_jsonl_reader_rejects_unsupported_values_without_fallback() -> None:
    """Verify Python row ingestion fails instead of using Python JSON fallback."""
    require_native()
    reader = PythonRowsJsonlByteReader([{"bad": object()}])

    with pytest.raises(RuntimeError, match="Native Python row JSONL encoding failed"):
        reader.read(1024)


def test_python_rows_reject_non_mapping_rows_in_native_loop() -> None:
    """Verify row-shape validation runs in the native serialization loop."""
    require_native()
    reader = PythonRowsJsonlByteReader([{"a": 1}, 2])

    with pytest.raises(TypeError, match="row 1 is not a dict"):
        reader.read(1024)


def test_native_python_row_encoder_matches_reader_payload() -> None:
    """Verify the native Python row encoder can feed the row reader."""
    require_native()
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    payload = _native.python_row_json_bytes({"b": [True, None], "a": "ñ"})

    assert payload == '{"b":[true,null],"a":"ñ"}'.encode()


def test_native_python_rows_batch_encoder_returns_next_index() -> None:
    """Verify the native Python row batch encoder returns JSONL and progress."""
    require_native()
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
def test_native_python_rows_batch_encoder_checks_pending_signals() -> None:
    """Verify long native row encoding polls Python signal handlers."""
    require_native()
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

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
    from schema_sanitizer.core_impl.execution import last_sink_source_route
    from schema_sanitizer.input_impl.source_plan import (
        last_native_multisource_route,
    )

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
    from schema_sanitizer.input_impl.directory_inputs import folder_files

    folder = tmp_path / "events"
    folder.mkdir()
    (folder / "a.json").write_text('{"id": 1}', encoding="utf-8")
    (folder / "ignore.txt").write_text('{"id": 2}', encoding="utf-8")

    files = folder_files(folder, suffix="json", reader_name="test directory input")

    assert [file.name for file in files] == ["a.json"]


def test_native_json_compactor_returns_compact_utf8_bytes(tmp_path) -> None:
    """Verify the native JSON compactor used by registry normalization."""
    require_native()
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    payload = _native.json_compact_bytes('{"b": [true, null], "a": "ñ"}'.encode())
    del tmp_path

    assert payload == '{"b":[true,null],"a":"ñ"}'.encode()


def test_native_json_array_to_jsonl_bytes() -> None:
    """Verify native JSON array splitting returns compact JSONL object rows."""
    require_native()
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    payload = b'[\n {"b": [true, null], "a": "\xc3\xb1"}, {"c": 3}\n]'

    assert _native.json_array_to_jsonl_bytes(payload) == (
        '{"b":[true,null],"a":"ñ"}\n{"c":3}\n'.encode()
    )

    with pytest.raises(ValueError, match="object elements"):
        _native.json_array_to_jsonl_bytes(b'[{"ok":true}, 1]')


def test_native_json_array_files_to_jsonl_bytes(tmp_path) -> None:
    """Verify native local JSON-array file batching returns JSONL rows."""
    require_native()
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

    result = read_test_json_folder(folder, input_text_encoding="iso8859-1")

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
    pytest.importorskip("pyarrow")
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
        read_test_json_folder(folder, memory_limit_bytes=128)

    err = excinfo.value
    assert err.detail is not None
    assert err.detail["stage"] == "json_parse"
    assert err.detail["limit_name"] == "memory_limit_bytes"
    assert err.detail["file"].endswith("row.json")


def test_read_json_folder_memory_limit_bounds_unknown_size_remote_child(monkeypatch) -> None:
    """Verify remote folder staging rejects oversized children with unknown size."""
    pytest.importorskip("pyarrow")
    from contextlib import contextmanager

    from schema_sanitizer.input_impl.directory_inputs import RemoteFile
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.remote_impl.providers import s3_sync

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


def test_read_xml_folder_compacts_non_recursive_xml_files(tmp_path) -> None:
    """Verify read xml folder compacts direct xml children only."""
    pytest.importorskip("pyarrow")
    require_native()
    from schema_sanitizer.input_impl.source_plan import (
        last_native_multisource_route,
    )

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


def test_read_xml_folder_propagates_native_root_tag_failure(tmp_path, monkeypatch) -> None:
    """Verify XML directory input has no Python root-tag fallback."""
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


def test_native_xml_folder_helpers_strip_declarations(tmp_path) -> None:
    """Verify native XML folder helper validates stable roots."""
    require_native()
    from schema_sanitizer.core_impl.native_runtime import native_core as _native

    first = tmp_path / "a.xml"
    second = tmp_path / "b.xml"
    first.write_text('<?xml version="1.0"?><event><id>1</id></event>', encoding="utf-8")
    second.write_text("<event><id>2</id></event>", encoding="utf-8")

    paths = [first, second]
    for sequence in (paths, tuple(paths), iter(paths)):
        assert _native.xml_folder_effective_row_tag(sequence, "", -1) == "event"


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
        read_test_xml_folder(folder, input_text_encoding="iso8859-1")


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


def test_python_rows_generator_rejects_replay_spool_limit() -> None:
    """One-shot Python iterables must not grow replay storage without a bound."""
    require_native()
    reader = PythonRowsJsonlByteReader(
        ({"payload": "x" * 256} for _ in range(100)),
        memory_limit_bytes=256,
    )

    with pytest.raises(RuntimeError, match="max_replay_spool_bytes limit exceeded"):
        while reader.read(4096):
            pass
    reader.close()
