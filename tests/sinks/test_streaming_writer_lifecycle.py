"""Streaming writer cleanup and exception-precedence contracts."""

from __future__ import annotations

from pathlib import Path

import pytest
from _support.sinks import (
    PythonStreamSinkPool,
    SinkLifecycleOutput,
    SinkLifecycleRaw,
    SinkLifecycleStream,
    skipped_rows_diagnostics,
    skipped_rows_json_diagnostics,
)

from schema_sanitizer.errors import SchemaSanitizerError


def _write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    output: SinkLifecycleOutput,
    writer: object,
) -> object:
    from schema_sanitizer.api_impl import execution_context, stream_output

    monkeypatch.setattr(
        execution_context,
        "default_pool",
        lambda: PythonStreamSinkPool(output),
    )
    return stream_output.write_table_or_stream(
        object(),
        tmp_path / "out",
        call_options=None,
        format="python",
        source="python",
        write_stream=writer,
    )


def test_streaming_writer_result_closes_sink_after_diagnostics_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    raw = SinkLifecycleRaw(skipped_rows_diagnostics(2))
    stream = SinkLifecycleStream(raw)
    output = SinkLifecycleOutput(raw=raw, stream=stream, cascade_close=True)
    written: list[tuple[object, object]] = []

    result = _write(
        monkeypatch,
        tmp_path,
        output,
        lambda value, path: written.append((value, path)),
    )

    assert len(written) == 1
    assert raw.main_close_calls == 1
    assert raw.close_calls == 2
    assert output.close_calls == 1
    assert result.stats["skipped_rows"] == 2
    assert result.clean_data is None


def test_streaming_writer_stats_use_diagnostics_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    raw = SinkLifecycleRaw(skipped_rows_json_diagnostics(3))
    output = SinkLifecycleOutput(raw=raw, stream=SinkLifecycleStream())

    result = _write(monkeypatch, tmp_path, output, lambda _stream, _path: None)

    assert result.stats["skipped_rows"] == 3


def test_streaming_writer_missing_stream_closes_sink(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = SinkLifecycleOutput(raw=SinkLifecycleRaw())

    with pytest.raises(RuntimeError, match="did not produce a stream"):
        _write(monkeypatch, tmp_path, output, lambda _stream, _path: None)

    assert output.close_calls == 1


def test_streaming_writer_failure_closes_full_sink(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    raw = SinkLifecycleRaw(skipped_rows_diagnostics(0))
    output = SinkLifecycleOutput(
        raw=raw,
        stream=SinkLifecycleStream(raw),
        close_available=False,
    )

    def fail_writer(_stream: object, _path: object) -> None:
        raise RuntimeError("writer failed")

    with pytest.raises(RuntimeError, match="writer failed"):
        _write(monkeypatch, tmp_path, output, fail_writer)

    assert raw.close_calls == 1
    assert raw.main_close_calls == 0


def test_streaming_writer_failure_preserves_writer_exception_when_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = SinkLifecycleOutput(
        stream=SinkLifecycleStream(close_error=RuntimeError("stream cleanup failed")),
        close_error=RuntimeError("sink cleanup failed"),
    )

    def fail_writer(_stream: object, _path: object) -> None:
        raise ValueError("writer failed")

    with pytest.raises(SchemaSanitizerError, match="writer failed") as caught:
        _write(monkeypatch, tmp_path, output, fail_writer)

    assert isinstance(caught.value.__cause__, ValueError)


def test_streaming_writer_stream_property_error_closes_sink(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = SinkLifecycleOutput(stream_error=ValueError("stream construction failed"))

    with pytest.raises(ValueError, match="stream construction failed"):
        _write(monkeypatch, tmp_path, output, lambda _stream, _path: None)

    assert output.close_calls == 1


def test_streaming_writer_stream_property_error_preserved_when_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = SinkLifecycleOutput(
        stream_error=ValueError("stream construction failed"),
        close_error=RuntimeError("cleanup failed"),
    )

    with pytest.raises(ValueError, match="stream construction failed"):
        _write(monkeypatch, tmp_path, output, lambda _stream, _path: None)


def test_streaming_writer_missing_stream_preserves_contract_error_when_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = SinkLifecycleOutput(close_error=RuntimeError("cleanup failed"))

    with pytest.raises(RuntimeError, match="did not produce a stream"):
        _write(monkeypatch, tmp_path, output, lambda _stream, _path: None)
