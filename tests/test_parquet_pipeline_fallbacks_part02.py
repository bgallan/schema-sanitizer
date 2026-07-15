"""Parquet API/runtime tests split by contract area."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from schema_sanitizer.adapters.parquet import telemetry as recording

try:
    import pyarrow as pa
    import pyarrow.feather as feather
    import pyarrow.parquet as pq

    _HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = feather = pq = None
    _HAVE_PYARROW = False

_requires_pyarrow = pytest.mark.skipif(not _HAVE_PYARROW, reason="pyarrow not installed")


def _make_native_try_factory(factory_module: object) -> object:
    """Build a minimal ParquetRecordBatchStreamFactory for _try_native_stream tests."""
    factory = object.__new__(factory_module.ParquetRecordBatchStreamFactory)
    factory._filters = None
    factory._local_path = "native-candidate.parquet"
    factory._source = "path"
    factory._native_source_kind = "path"
    factory._columns = None
    factory._batch_size = 1024
    factory._keepalive = ()
    return factory


# Split from test_parquet_pipeline_fallbacks.py: test_parquet_dataset_scanner_failure_ladders_to_iter_batches, test_parquet_dataset_filter_failure_is_observable, test_parquet_local_dataset_open_failure_uses_iter_batches_fallback, ...


def test_parquet_dataset_scanner_failure_ladders_to_iter_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify local path fallback tries ParquetFile when Dataset scanner fails."""
    from schema_sanitizer.adapters.parquet import record_batch_factory as stream_factory
    from schema_sanitizer.adapters.parquet import telemetry as observability

    class FailingDataset:
        """Internal test helper class."""

        schema = None

        def scanner(self, **kwargs: object) -> object:
            """Internal test helper."""
            raise ValueError("dataset scanner failed")

    class WorkingParquetFile:
        """Internal test helper class."""

        def iter_batches(self, **kwargs: object) -> object:
            """Internal test helper."""
            yield "batch"

    class WorkingReader:
        """Internal test helper class."""

        def __arrow_c_stream__(self) -> str:
            """Internal test helper."""
            return "iter-stream"

    def fake_record_batch_reader_from_iterable(
        pa_obj: object,
        schema: object,
        batches: object,
    ) -> WorkingReader:
        """Internal test helper."""
        assert list(batches) == ["batch"]
        return WorkingReader()

    observability.reset_parquet_stream_factory_observability()
    recording.set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=False,
        reason="not_ready",
        blockers=["external writer"],
        fallback_expected=True,
        fallback_route="pyarrow_dataset_scanner",
    )
    monkeypatch.setattr(
        stream_factory,
        "record_batch_reader_from_iterable",
        fake_record_batch_reader_from_iterable,
    )
    factory = object.__new__(stream_factory.ParquetRecordBatchStreamFactory)
    factory._dataset = FailingDataset()
    factory._try_native_stream = lambda: None
    factory._columns = None
    factory._filters = None
    factory._batch_size = 1024
    factory._use_threads = False
    factory._keepalive = ()
    factory._pending_parquet_file = WorkingParquetFile()
    factory._pending_opened_file = None
    factory._pa = object()
    factory.schema = object()

    assert factory.__arrow_c_stream__() == "iter-stream"

    snapshot = observability.parquet_stream_factory_observability()
    assert snapshot["last_route"] == "pyarrow_parquetfile_iter_batches"
    assert snapshot["route_counts"] == {"pyarrow_parquetfile_iter_batches": 1}
    diagnostics = snapshot["last_native_reader_diagnostics"]
    assert diagnostics["reason"] == "not_ready"
    assert diagnostics["fallback_attempted"] is True
    assert diagnostics["fallback_succeeded"] is True
    assert diagnostics["fallback_route"] == "pyarrow_parquetfile_iter_batches"
    assert diagnostics["fallback_error"] is None
    assert diagnostics["fallback_attempt_history"] == [
        {"route": "pyarrow_dataset_scanner", "status": "attempted"},
        {
            "route": "pyarrow_dataset_scanner",
            "status": "failed",
            "error": "ValueError: dataset scanner failed",
        },
        {"route": "pyarrow_parquetfile_iter_batches", "status": "attempted"},
        {"route": "pyarrow_parquetfile_iter_batches", "status": "succeeded"},
    ]


def test_parquet_dataset_filter_failure_is_observable() -> None:
    """Verify filters fail closed when Dataset fallback is unavailable."""
    from schema_sanitizer.adapters.parquet import record_batch_factory as stream_factory
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()
    recording.set_parquet_native_reader_diagnostics(
        attempted=False,
        ready=False,
        reason="filter_requires_dataset_scanner",
        fallback_expected=True,
        fallback_route="pyarrow_dataset_scanner",
    )
    factory = object.__new__(stream_factory.ParquetRecordBatchStreamFactory)
    factory._dataset = None
    factory._dataset_error = ValueError("dataset open failed")
    factory._try_native_stream = lambda: None
    factory._filters = object()

    with pytest.raises(ValueError, match="dataset open failed"):
        factory.__arrow_c_stream__()

    diagnostics = observability.last_parquet_native_reader_diagnostics()
    assert diagnostics["fallback_attempted"] is True
    assert diagnostics["fallback_succeeded"] is False
    assert diagnostics["fallback_route"] == "pyarrow_dataset_scanner"
    assert diagnostics["fallback_error"] == "ValueError: dataset open failed"


def test_parquet_local_dataset_open_failure_uses_iter_batches_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Dataset construction failures do not block ParquetFile fallback."""
    from schema_sanitizer.adapters.parquet import record_batch_factory as factory_schema
    from schema_sanitizer.adapters.parquet import record_batch_factory as stream_factory
    from schema_sanitizer.adapters.parquet import telemetry as observability

    class FakePA:
        """Internal test helper class."""

        def schema(self, fields: object, metadata: object = None) -> tuple[object, object]:
            """Internal test helper."""
            return (tuple(fields), metadata)

    class FakeSchema:
        """Internal test helper class."""

        metadata = {b"source": b"fake"}
        _names = ["keep", "drop"]

        def get_field_index(self, name: str) -> int:
            """Internal test helper."""
            try:
                return self._names.index(name)
            except ValueError:
                return -1

        def field(self, index: int) -> str:
            """Internal test helper."""
            return f"field:{self._names[index]}"

    class FakeDatasetModule:
        """Internal test helper class."""

        def dataset(self, path: str, *, format: str) -> object:
            """Internal test helper."""
            assert path == "/tmp/fallback.parquet"
            assert format == "parquet"
            raise ValueError("dataset open failed")

    class FakeParquetFile:
        """Internal test helper class."""

        schema_arrow = FakeSchema()

        def iter_batches(self, **kwargs: object) -> object:
            """Internal test helper."""
            assert kwargs["columns"] == ["keep"]
            yield "batch"

    class FakePQ:
        """Internal test helper class."""

        def ParquetFile(self, src: object) -> FakeParquetFile:
            """Internal test helper."""
            assert src == "opened-source"
            return FakeParquetFile()

    class FakeReader:
        """Internal test helper class."""

        def __arrow_c_stream__(self) -> str:
            """Internal test helper."""
            return "iter-stream"

    def fake_optional_dependency(name: str, **kwargs: object) -> object:
        """Internal test helper."""
        if name == "pyarrow.parquet":
            return FakePQ()
        if name == "pyarrow.dataset":
            return FakeDatasetModule()
        raise AssertionError(name)

    def fake_record_batch_reader_from_iterable(
        pa_obj: object,
        schema: object,
        batches: object,
    ) -> FakeReader:
        """Internal test helper."""
        assert schema == (("field:keep",), {b"source": b"fake"})
        assert list(batches) == ["batch"]
        return FakeReader()

    observability.reset_parquet_stream_factory_observability()
    monkeypatch.setattr(stream_factory, "ensure_pyarrow", lambda **kwargs: FakePA())
    monkeypatch.setattr(stream_factory, "ensure_optional_dependency", fake_optional_dependency)
    monkeypatch.setattr(factory_schema, "ensure_optional_dependency", fake_optional_dependency)
    monkeypatch.setattr(
        stream_factory,
        "local_parquet_path_or_none",
        lambda data, **kwargs: "/tmp/fallback.parquet",
    )
    monkeypatch.setattr(
        factory_schema,
        "open_parquet_source",
        lambda data, **kwargs: ("opened-source", None),
    )
    monkeypatch.setattr(
        stream_factory,
        "native_parquet_stream_preflight_info",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        stream_factory,
        "record_batch_reader_from_iterable",
        fake_record_batch_reader_from_iterable,
    )

    factory = stream_factory.ParquetRecordBatchStreamFactory(
        "input.parquet",
        source="path",
        feature="test",
        columns=["keep"],
    )

    assert factory._dataset is None
    assert isinstance(factory._dataset_error, ValueError)
    assert factory.schema == (("field:keep",), {b"source": b"fake"})
    assert factory.__arrow_c_stream__() == "iter-stream"

    snapshot = observability.parquet_stream_factory_observability()
    assert snapshot["last_route"] == "pyarrow_parquetfile_iter_batches"
    diagnostics = snapshot["last_native_reader_diagnostics"]
    assert diagnostics["reason"] == "footer_info_unavailable"
    assert diagnostics["fallback_succeeded"] is True
    assert diagnostics["fallback_route"] == "pyarrow_parquetfile_iter_batches"
    assert diagnostics["fallback_attempt_history"] == [
        {"route": "pyarrow_dataset_scanner", "status": "attempted"},
        {
            "route": "pyarrow_dataset_scanner",
            "status": "failed",
            "error": "ValueError: dataset open failed",
        },
        {"route": "pyarrow_parquetfile_iter_batches", "status": "attempted"},
        {"route": "pyarrow_parquetfile_iter_batches", "status": "succeeded"},
    ]


def test_parquet_iter_batches_fallback_failure_is_observable() -> None:
    """Verify ParquetFile.iter_batches fallback failures are annotated."""
    from schema_sanitizer.adapters.parquet import record_batch_factory as stream_factory
    from schema_sanitizer.adapters.parquet import telemetry as observability

    class FailingParquetFile:
        """Internal test helper class."""

        def iter_batches(self, **kwargs: object) -> object:
            """Internal test helper."""
            raise OSError("iter_batches failed")

    observability.reset_parquet_stream_factory_observability()
    factory = object.__new__(stream_factory.ParquetRecordBatchStreamFactory)
    factory._dataset = None
    factory._pending_parquet_file = FailingParquetFile()
    factory._pending_opened_file = None
    factory._local_path = None
    factory._source = "stream"
    factory._native_source_kind = "stream"
    factory._columns = None
    factory._filters = None
    factory._batch_size = 1024
    factory._use_threads = False
    factory._pa = object()
    factory.schema = object()
    factory._keepalive = ()

    with pytest.raises(OSError, match="iter_batches failed"):
        factory.__arrow_c_stream__()

    snapshot = observability.parquet_stream_factory_observability()
    assert snapshot["last_route"] == "none"
    assert snapshot["route_counts"] == {}
    diagnostics = snapshot["last_native_reader_diagnostics"]
    assert diagnostics["reason"] == "source_not_path"
    assert diagnostics["fallback_expected"] is True
    assert diagnostics["fallback_attempted"] is True
    assert diagnostics["fallback_succeeded"] is False
    assert diagnostics["fallback_route"] == "pyarrow_parquetfile_iter_batches"
    assert diagnostics["fallback_error"] == "OSError: iter_batches failed"


def test_parquet_snappy_compression_option_normalizes_without_pyarrow() -> None:
    """Verify Snappy is part of the public Parquet compression contract."""
    from schema_sanitizer.adapters.parquet.compression import (
        normalize_parquet_compression,
        pyarrow_parquet_writer_options,
    )

    assert normalize_parquet_compression("snappy") == "snappy"
    assert pyarrow_parquet_writer_options(
        parquet_compression="snappy",
        parquet_gzip_level=None,
    ) == {"compression": "snappy"}


def test_native_parquet_footer_info_forwards_projected_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify projected native footer planning does not require PyArrow."""
    from schema_sanitizer.adapters.parquet import status as parquet_footer

    calls: list[tuple[object, ...]] = []

    def fake_footer_info_json(*args: object) -> str:
        """Internal test helper."""
        calls.append(args)
        return '{"row_groups": []}'

    monkeypatch.setattr(parquet_footer, "PARQUET_FOOTER_INFO_JSON", fake_footer_info_json)

    assert parquet_footer.native_parquet_footer_info("data.parquet") == {"row_groups": []}
    assert parquet_footer.native_parquet_footer_info(
        "data.parquet",
        columns=("keep", "id"),
    ) == {"row_groups": []}
    assert calls == [("data.parquet",), ("data.parquet", ["keep", "id"])]


def test_parquet_bytes_native_staging_lifecycle() -> None:
    """Verify buffer-backed Parquet staging is local, exact, and cleaned up."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        remove_staged_parquet,
        stage_parquet_buffer,
    )

    payload = memoryview(b"PAR1test-payloadPAR1")
    path = Path(stage_parquet_buffer(payload))
    try:
        assert path.name.startswith("schema-sanitizer-parquet-")
        assert path.suffix == ".parquet"
        assert path.read_bytes() == payload.tobytes()
    finally:
        assert remove_staged_parquet(str(path)) is True

    assert not path.exists()
    assert remove_staged_parquet(str(path)) is True


def test_parquet_stream_native_path_detection(tmp_path: Path) -> None:
    """Verify file-backed streams can expose a native local path safely."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import local_stream_path

    path = tmp_path / "data.parquet"
    path.write_bytes(b"PAR1")

    with path.open("rb") as handle:
        assert local_stream_path(handle) == str(path)

    assert local_stream_path(SimpleNamespace(name="<stdin>")) is None
    assert local_stream_path(SimpleNamespace(name=tmp_path / "missing.parquet")) is None
