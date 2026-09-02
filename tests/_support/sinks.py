"""Create inputs and normalize outputs shared by sink tests.

The helpers preserve logical rows and files while excluding only generated metadata from
cross-sink comparisons.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

GENERATED_METADATA_COLUMNS = {
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
}


def write_csv(path: Path, text: str = "a,b\n1,2\n3,4\n") -> Path:
    """Write one UTF-8 CSV test input and return its path."""
    path.write_text(text, encoding="utf-8")
    return path


def without_generated_metadata(row: dict[str, object]) -> dict[str, object]:
    """Return row data excluding generated file-converter metadata columns."""
    return {key: value for key, value in row.items() if key not in GENERATED_METADATA_COLUMNS}


def without_generated_metadata_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Return rows excluding generated file-converter metadata columns."""
    return [without_generated_metadata(row) for row in rows]


def native_parquet_zlib_available(pa: object, tmp_path: Path) -> bool:
    """Return whether the compiled native Parquet writer can emit gzip pages."""
    from schema_sanitizer.api_impl.file_conversion import direct_writers as native_parquet_output

    write = native_parquet_output.PARQUET_STREAM_WRITE
    if write is None:
        return False
    batch = pa.record_batch({"text": pa.array(["probe"], type=pa.string())})
    stream = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    try:
        write(stream, str(tmp_path / "native-zlib-probe.parquet"), "gzip", -1, -1)
    except RuntimeError as exc:
        if "zlib is not available" in str(exc):
            return False
        raise
    return True


def fail_pyarrow_sink(*_args: object, **_kwargs: object) -> None:
    """Fail when a native-writer test unexpectedly uses the PyArrow fallback."""
    raise AssertionError("PyArrow sink fallback should not be used")


class PythonStreamSinkPool:
    """Return one configured streaming sink from the execution-context API."""

    def __init__(self, output: object) -> None:
        """Initialize the Python stream sink pool test double."""
        self.output = output

    def get(self) -> object:
        """Return a context that routes Python rows to the configured stream output."""
        output = self.output

        class Context:
            def to_sink(
                self,
                data: object,
                *,
                sink: object,
                options: object,
                format: object,
                source: object,
            ) -> object:
                """Validate the Python stream route and return the configured output."""
                assert data is not None
                assert (sink, options, format, source) == (
                    "stream",
                    None,
                    "python",
                    "python",
                )
                return output

        return Context()


class SinkLifecycleRaw:
    """Count raw-stream closure calls while exposing diagnostics."""

    def __init__(self, diagnostics: object = None) -> None:
        """Initialize the sink lifecycle raw test double."""
        self.diagnostics = diagnostics
        self.main_close_calls = 0
        self.close_calls = 0

    def close_main_stream(self) -> None:
        """Close the primary stream while recording lifecycle calls."""
        self.main_close_calls += 1

    def close(self) -> None:
        """Close the resources owned by the sink lifecycle raw test double."""
        self.close_calls += 1


class SinkLifecycleStream:
    """Configurable stream closure double backed by a raw stream."""

    def __init__(
        self,
        raw: SinkLifecycleRaw | None = None,
        *,
        close_error: BaseException | None = None,
    ) -> None:
        """Initialize the sink lifecycle stream test double."""
        self.raw = raw
        self.close_error = close_error
        self.main_close_calls = 0
        self.close_calls = 0

    def close_main_stream(self) -> None:
        """Close the primary stream while recording lifecycle calls."""
        self.main_close_calls += 1
        if self.raw is not None:
            self.raw.close_main_stream()

    def close(self) -> None:
        """Close the resources owned by the sink lifecycle stream test double."""
        self.close_calls += 1
        if self.raw is not None:
            self.raw.close()
        if self.close_error is not None:
            raise self.close_error


class SinkLifecycleOutput:
    """Configurable raw/stream sink result for lifecycle branch tests."""

    def __init__(
        self,
        *,
        raw: object = None,
        stream: object = None,
        stream_error: BaseException | None = None,
        close_error: BaseException | None = None,
        cascade_close: bool = False,
        close_available: bool = True,
    ) -> None:
        """Initialize the sink lifecycle output test double."""
        self._raw = raw
        self._stream = stream
        self.stream_error = stream_error
        self.close_error = close_error
        self.cascade_close = cascade_close
        self.close_available = close_available
        self.close_calls = 0

    def __getattribute__(self, name: str) -> object:
        """Hide close when unavailable, otherwise return the requested attribute."""
        if name == "close" and not object.__getattribute__(self, "close_available"):
            raise AttributeError(name)
        return object.__getattribute__(self, name)

    @property
    def raw(self) -> object:
        """Return the raw stream owned by the sink output."""
        return self._raw

    @property
    def stream(self) -> object:
        """Return the configured stream or raise its injected access error."""
        if self.stream_error is not None:
            raise self.stream_error
        return self._stream

    def close(self) -> None:
        """Close the resources owned by the sink lifecycle output test double."""
        self.close_calls += 1
        if self.cascade_close:
            if self._stream is not None:
                self._stream.close()
            if self._raw is not None:
                self._raw.close()
        if self.close_error is not None:
            raise self.close_error


def skipped_rows_json_diagnostics(count: int) -> object:
    """Build diagnostics that exercise the JSON snapshot route."""

    class Diagnostics:
        def to_json(self) -> str:
            """Serialize the controlled diagnostics payload to JSON."""
            return f'{{"skipped_rows":{count}}}'

    return Diagnostics()


def skipped_rows_diagnostics(count: int) -> object:
    """Build diagnostics that exercise the attribute snapshot route."""
    return SimpleNamespace(skipped_rows=count)
