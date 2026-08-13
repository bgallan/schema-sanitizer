"""Regression coverage for internal Parquet streams and safe replay fallback."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest


class _ReplayStream:
    """Minimal replay stream used to record lifecycle events."""

    def __init__(self, events: list[str]):
        """Store the shared event recorder."""
        self._events = events

    def close_main_stream(self) -> None:
        """Record closure of the replay reader."""
        self._events.append("stream-close")


class _Replay:
    """Minimal replay owner used by fallback lifecycle tests."""

    def __init__(self, events: list[str]):
        """Create a replay owner and its stream."""
        self._events = events
        self._stream = _ReplayStream(events)

    def reader(self) -> _ReplayStream:
        """Return the replay reader while recording access."""
        self._events.append("replay-reader")
        return self._stream

    def close(self) -> None:
        """Record release of the replay owner."""
        self._events.append("replay-close")


def test_supported_internal_parquet_stream_skips_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify operation-owned streams reach native Parquet without IPC replay."""
    from schema_sanitizer.api_impl import stream_output
    from schema_sanitizer.core_impl.native_results import SinkOutput

    raw = SinkOutput(sink="stream")
    calls: list[tuple[Any, bool]] = []

    def fake_native(raw_arg: Any, _path: Any, **kwargs: Any) -> bool:
        """Record the direct native writer invocation."""
        calls.append((raw_arg, kwargs["parquet_retry_is_safe"]))
        return True

    monkeypatch.setattr(stream_output, "try_write_raw_native_file_output", fake_native)
    monkeypatch.setattr(
        stream_output,
        "_make_parquet_replay",
        lambda *_args, **_kwargs: pytest.fail("internal native success must not spool"),
    )
    monkeypatch.setattr(stream_output, "patch_file_output_diagnostics", lambda *_a, **_k: None)

    stream_output.write_raw_stream_to_file(
        raw,
        tmp_path / "direct.parquet",
        writer=stream_output.write_parquet_native_first_stream,
        feature="internal direct Parquet",
        first_row_columns=None,
        parquet_compression="uncompressed",
    )

    assert calls == [(raw, False)]


def test_internal_parquet_replay_is_created_only_after_safe_decline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a pre-consumption native decline may still create one replay."""
    from schema_sanitizer.api_impl import stream_output
    from schema_sanitizer.core_impl.native_results import SinkOutput

    raw = SinkOutput(sink="stream")
    events: list[str] = []

    def fake_writer(stream: Any, _path: Any, **_kwargs: Any) -> dict[str, int]:
        """Record the fallback writer invocation."""
        events.append("fallback-writer")
        assert isinstance(stream, _ReplayStream)
        return {"materialized_rows": 0}

    def fake_native(raw_arg: Any, _path: Any, **kwargs: Any) -> bool:
        """Decline safely before consuming the internal stream."""
        assert raw_arg is raw
        assert kwargs["parquet_retry_is_safe"] is False
        events.append("native-decline")
        return False

    def fake_replay(raw_arg: Any, *, feature: str, memory_limit_bytes: int | None) -> _Replay:
        """Create and record the fallback replay."""
        assert raw_arg is raw
        assert feature == "internal fallback Parquet"
        assert memory_limit_bytes is None
        events.append("replay-create")
        return _Replay(events)

    monkeypatch.setattr(stream_output, "write_parquet_native_first_stream", fake_writer)
    monkeypatch.setattr(stream_output, "try_write_raw_native_file_output", fake_native)
    monkeypatch.setattr(stream_output, "_make_parquet_replay", fake_replay)
    monkeypatch.setattr(stream_output, "patch_file_output_diagnostics", lambda *_a, **_k: None)

    stream_output.write_raw_stream_to_file(
        raw,
        tmp_path / "fallback.parquet",
        writer=fake_writer,
        feature="internal fallback Parquet",
        first_row_columns=None,
    )

    assert events[:3] == ["native-decline", "replay-create", "replay-reader"]
    assert "fallback-writer" in events
    assert events[-1] == "replay-close"


def test_internal_parquet_late_failure_is_not_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify a possibly consuming native failure cannot fall back on the stream."""
    from schema_sanitizer.api_impl.file_conversion import direct_writers, writers

    def fail_after_consumption(*_args: Any, **_kwargs: Any) -> bool:
        """Simulate a native failure after possible stream consumption."""
        raise RuntimeError("native Parquet writer: unsupported column value kind")

    monkeypatch.setattr(
        direct_writers, "try_write_parquet_raw_direct_native", fail_after_consumption
    )
    with pytest.raises(RuntimeError, match="unsupported column value kind"):
        writers.try_write_raw_native_file_output(
            object(),
            tmp_path / "late-failure.parquet",
            writer=writers.write_parquet_native_first_stream,
            parquet_retry_is_safe=False,
        )
