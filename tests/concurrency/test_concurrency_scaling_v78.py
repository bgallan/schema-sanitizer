"""Regression coverage for v78 stream-native analytical handoffs."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from schema_sanitizer.api_impl import results as result_adapters
from schema_sanitizer.api_impl import table_adapter_sink
from schema_sanitizer.api_impl.source_plan import registry as registry_streams
from schema_sanitizer.core_impl.concurrency_coverage import (
    INPUT_CONCURRENCY_COVERAGE,
    OUTPUT_CONCURRENCY_COVERAGE,
    concurrency_pair_guarantees,
)


class _FakeTable:
    """Minimal Arrow table used by stream-conversion tests."""

    def __init__(self, rows: int = 7) -> None:
        """Store the table row count."""
        self.num_rows = rows

    def to_batches(self) -> list[object]:
        """Return three logical batches for diagnostics."""
        return [object(), object(), object()]


class _FakePandasFrame:
    """Minimal pandas-like frame exposing a zero-copy row count."""

    def __init__(self, rows: int = 7) -> None:
        """Expose a pandas-compatible index of the requested size."""
        self.index = range(rows)


class _FakePolarsFrame:
    """Minimal Polars-like frame exposing its height."""

    def __init__(self, rows: int = 7) -> None:
        """Expose the Polars-compatible frame height."""
        self.height = rows


class _FakeBatch:
    """Minimal Arrow record batch preserving row-count metadata."""

    def __init__(self, rows: int) -> None:
        """Store the record-batch row count."""
        self.num_rows = rows


class _FakeReader:
    """Record-batch reader double recording every terminal operation."""

    num_record_batches = 3
    schema = "schema"

    def __init__(self) -> None:
        """Initialize reader operation and lifecycle telemetry."""
        self.closed = False
        self.read_all_calls = 0
        self.read_pandas_calls: list[dict[str, object]] = []
        self.batches = [_FakeBatch(2), _FakeBatch(2), _FakeBatch(3)]

    def read_all(self) -> _FakeTable:
        """Materialize the fake reader as one table."""
        self.read_all_calls += 1
        return _FakeTable()

    def read_pandas(self, **kwargs: object) -> _FakePandasFrame:
        """Record pandas conversion options and return a fake frame."""
        self.read_pandas_calls.append(dict(kwargs))
        return _FakePandasFrame()

    def __iter__(self):
        """Yield the retained record batches in source order."""
        return iter(self.batches)

    def close(self) -> None:
        """Mark the reader as closed."""
        self.closed = True


class _FakeDataset:
    """Chunk-preserving Arrow dataset double."""


@pytest.fixture
def reader_factory(monkeypatch: pytest.MonkeyPatch) -> list[_FakeReader]:
    """Replace PyArrow import with deterministic record-batch readers."""
    readers: list[_FakeReader] = []

    def open_reader(stream: object, *, feature: str) -> _FakeReader:
        """Create and retain one deterministic reader double."""
        assert stream == "native-stream"
        assert feature == "test"
        reader = _FakeReader()
        readers.append(reader)
        return reader

    monkeypatch.setattr(
        result_adapters._pyarrow_streams,
        "reader_from_stream_like",
        open_reader,
    )
    return readers


def test_v78_pyarrow_uses_explicit_record_batch_reader(
    reader_factory: list[_FakeReader],
) -> None:
    """PyArrow keeps its required table type but avoids generic pa.table dispatch."""
    conversion = result_adapters.convert_arrow_stream_output(
        "native-stream",
        "pyarrow",
        feature="test",
        threading_mode="multi",
    )

    reader = reader_factory[-1]
    assert conversion.clean_data.num_rows == 7
    assert conversion.diagnostics_shape is conversion.clean_data
    assert conversion.route == "record_batch_reader_to_pyarrow_table"
    assert reader.read_all_calls == 1
    assert reader.closed is True


@pytest.mark.parametrize(("mode", "use_threads"), [("single", False), ("multi", True)])
def test_v78_pandas_consumes_reader_without_arrow_table(
    monkeypatch: pytest.MonkeyPatch,
    reader_factory: list[_FakeReader],
    mode: str,
    use_threads: bool,
) -> None:
    """Pandas receives the reader directly and preserves the threading policy."""
    monkeypatch.setattr(
        result_adapters,
        "ensure_optional_dependency",
        lambda name, **_kwargs: SimpleNamespace() if name == "pandas" else None,
    )

    conversion = result_adapters.convert_arrow_stream_output(
        "native-stream",
        "pandas",
        feature="test",
        threading_mode=mode,
    )

    reader = reader_factory[-1]
    assert conversion.route == "record_batch_reader_to_pandas"
    assert conversion.diagnostics_shape.num_rows == 7
    assert conversion.diagnostics_shape.batch_count == 3
    assert reader.read_pandas_calls == [{"use_threads": use_threads}]
    assert reader.read_all_calls == 0
    assert reader.closed is True


def test_v78_polars_consumes_reader_without_arrow_table(
    monkeypatch: pytest.MonkeyPatch,
    reader_factory: list[_FakeReader],
) -> None:
    """Polars receives the record-batch reader instead of a full Arrow table."""
    seen: list[object] = []

    class FakePolars:
        """Minimal Polars module accepting Arrow reader inputs."""

        @staticmethod
        def from_arrow(value: object) -> _FakePolarsFrame:
            """Record the Arrow value and return a fake DataFrame."""
            seen.append(value)
            return _FakePolarsFrame()

    monkeypatch.setattr(
        result_adapters,
        "ensure_optional_dependency",
        lambda name, **_kwargs: FakePolars if name == "polars" else None,
    )

    conversion = result_adapters.convert_arrow_stream_output(
        "native-stream",
        "polars",
        feature="test",
        threading_mode="multi",
    )

    reader = reader_factory[-1]
    assert seen == [reader]
    assert conversion.route == "record_batch_reader_to_polars"
    assert conversion.diagnostics_shape.num_rows == 7
    assert reader.read_all_calls == 0
    assert reader.closed is True


def test_v78_duckdb_uses_chunk_preserving_dataset_not_arrow_table(
    monkeypatch: pytest.MonkeyPatch,
    reader_factory: list[_FakeReader],
) -> None:
    """DuckDB binds an in-memory Arrow dataset built directly from the reader."""
    dataset = _FakeDataset()
    dataset_inputs: list[tuple[object, object]] = []
    duckdb_inputs: list[object] = []
    relation = object()

    class FakeDatasetModule:
        """Minimal pyarrow.dataset module preserving construction inputs."""

        @staticmethod
        def dataset(value: object, *, schema: object) -> _FakeDataset:
            """Build a fake dataset from ordered batches and one schema."""
            dataset_inputs.append((value, schema))
            return dataset

    class FakeDuckDB:
        """Minimal DuckDB module binding one Arrow object."""

        @staticmethod
        def from_arrow(value: object) -> object:
            """Record the Arrow source and return a relation double."""
            duckdb_inputs.append(value)
            return relation

    def dependency(name: str, **_kwargs: object) -> object:
        """Resolve only the optional modules used by this test."""
        if name == "pyarrow.dataset":
            return FakeDatasetModule
        if name == "duckdb":
            return FakeDuckDB
        raise AssertionError(name)

    monkeypatch.setattr(result_adapters, "ensure_optional_dependency", dependency)

    conversion = result_adapters.convert_arrow_stream_output(
        "native-stream",
        "duckdb",
        feature="test",
        threading_mode="multi",
    )

    reader = reader_factory[-1]
    assert dataset_inputs == [(reader.batches, reader.schema)]
    assert duckdb_inputs == [dataset]
    assert conversion.clean_data is relation
    assert conversion.route == "record_batch_reader_to_arrow_dataset_to_duckdb"
    assert conversion.diagnostics_shape.num_rows == 7
    assert reader.read_all_calls == 0
    assert reader.closed is True


def test_v78_materializer_records_route_and_closes_opened_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Registry materialization retains metadata while delegating target conversion."""
    diagnostics = SimpleNamespace(
        inferred_rows=0,
        inferred_bytes=0,
        materialized_rows=0,
        batches=0,
        skipped_rows=0,
    )
    opened = SimpleNamespace(
        diagnostics=diagnostics,
        schema_registry_json="{}",
        schema_drifts_json="[]",
        native_registry_state="state",
        closed=False,
    )
    opened.materialization_stream = lambda: "native-stream"

    def close() -> None:
        """Record closure of the opened registry stream."""
        opened.closed = True

    opened.close = close
    conversion = result_adapters.AnalyticalOutputConversion(
        clean_data="frame",
        diagnostics_shape=result_adapters._AnalyticalShape(7, 3),
        route="direct-route",
    )
    monkeypatch.setattr(
        registry_streams,
        "convert_arrow_stream_output",
        lambda stream, target, *, feature, threading_mode: conversion,
    )

    result = registry_streams.materialize_opened_registry_stream(
        opened,
        target="polars",
        threading_mode="multi",
    )

    assert result.clean_data == "frame"
    assert result.conversion_route == "direct-route"
    assert result.schema_registry_json == "{}"
    assert result.native_registry_state == "state"
    assert result.stats["materialized_rows"] == 7
    assert result.stats["batches"] == 3
    assert opened.closed is True


def test_v78_internal_adapter_sink_uses_stream_not_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ExecutionContext adapter sinks no longer force context.to_table first."""
    output = SimpleNamespace(raw="native-stream", closed=False)

    def close() -> None:
        """Record closure of the internal stream sink output."""
        output.closed = True

    output.close = close

    class FakeContext:
        """Execution-context double exposing stream and forbidden table paths."""

        def to_sink(self, data: object, **kwargs: object) -> object:
            """Return the stream sink and verify routing arguments."""
            assert data == "rows"
            assert kwargs["sink"] == "stream"
            return output

        def to_table(self, *_args: object, **_kwargs: object) -> object:
            """Fail if the removed eager table path is called."""
            raise AssertionError("table barrier must not be used")

    monkeypatch.setattr(
        table_adapter_sink,
        "convert_arrow_stream_output",
        lambda *_args, **_kwargs: result_adapters.AnalyticalOutputConversion(
            "adapter-value",
            result_adapters._AnalyticalShape(1),
            "direct-route",
        ),
    )

    value = table_adapter_sink.materialize_table_adapter_sink(
        FakeContext(),
        "rows",
        sink="polars",
        options=None,
        format="python",
        source="python",
    )

    assert value == "adapter-value"
    assert output.closed is True


def test_v78_every_pair_declares_its_terminal_handoff_and_table_barrier() -> None:
    """All 56 pairs distinguish native sinks from direct analytical readers."""
    pairs = concurrency_pair_guarantees()
    assert set(pairs) == set(INPUT_CONCURRENCY_COVERAGE)
    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for outputs in pairs.values():
        assert set(outputs) == set(OUTPUT_CONCURRENCY_COVERAGE)
        for output_name, contract in outputs.items():
            if output_name in {"csv", "jsonl", "parquet"}:
                assert contract["terminal_handoff"] == "native_arrow_stream_sink"
            elif output_name == "pyarrow":
                assert contract["terminal_handoff"] == "arrow_c_stream_to_pyarrow_table"
            else:
                assert "record_batch_reader" in str(contract["terminal_handoff"])
            assert contract["full_arrow_table_barrier"] is (output_name in {"pyarrow", "pandas"})
            assert contract["explicit_pyarrow_table_output"] is (output_name == "pyarrow")
            assert contract["adapter_internal_full_table_materialization"] is (
                output_name == "pandas"
            )

    assert "direct_stream_adapter_conversion" in pairs["python"]["pandas"]["output_parallel_stages"]
    assert (
        "chunk_preserving_arrow_dataset_handoff"
        in pairs["parquet"]["duckdb"]["output_parallel_stages"]
    )
