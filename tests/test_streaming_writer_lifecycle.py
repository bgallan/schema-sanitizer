"""Tests streaming writer cleanup contracts."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest


def _assert_python_stream_sink_call(
    data: object,
    *,
    sink: object,
    options: object,
    format: object,
    source: object,
) -> None:
    """Assert python stream sink call."""
    assert data is not None
    assert sink == "stream"
    assert options is None
    assert format == "python"
    assert source == "python"


class _Pool:
    """Test helper for Pool."""

    def __init__(self, sink_out: object) -> None:
        """Initialize the test helper."""
        self._sink_out = sink_out

    def get(self) -> object:
        """Return the stored test value."""
        sink_out = self._sink_out

        class Ctx:
            """Test helper for Ctx."""

            def to_sink(
                self,
                data: object,
                *,
                sink: object,
                options: object,
                format: object,
                source: object,
            ) -> object:
                """Return the configured test sink."""
                _assert_python_stream_sink_call(
                    data,
                    sink=sink,
                    options=options,
                    format=format,
                    source=source,
                )
                return sink_out

        return Ctx()


def test_streaming_writer_result_closes_sink_after_diagnostics_snapshot(
    monkeypatch, tmp_path: Path
) -> None:
    """Verify streaming writer result closes sink after diagnostics snapshot."""
    from schema_sanitizer.api_impl import execution_context as pool
    from schema_sanitizer.api_impl import stream_output

    class Raw:
        """Test helper for Raw."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.diagnostics = SimpleNamespace(skipped_rows=2)
            self.main_closed = 0
            self.closed = 0

        def close_main_stream(self) -> None:
            """Close the main test stream."""
            self.main_closed += 1

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    class Stream:
        """Test helper for Stream."""

        def __init__(self, raw: Raw) -> None:
            """Initialize the test helper."""
            self.raw = raw
            self.main_closed = 0
            self.closed = 0

        def close_main_stream(self) -> None:
            """Close the main test stream."""
            self.main_closed += 1
            self.raw.close_main_stream()

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1
            self.raw.close()

    class SinkOut:
        """Test helper for SinkOut."""

        def __init__(self, raw: Raw) -> None:
            """Initialize the test helper."""
            self._raw = raw
            self._stream = Stream(raw)
            self.closed = 0

        @property
        def raw(self) -> Raw:
            """Return the raw test value."""
            return self._raw

        @property
        def stream(self) -> Stream:
            """Return the test stream."""
            return self._stream

        def close(self) -> None:
            """Close the test sink."""
            self.closed += 1
            self._stream.close()
            self._raw.close()

    raw = Raw()
    sink_out = SinkOut(raw)

    monkeypatch.setattr(pool, "default_pool", lambda: _Pool(sink_out))
    written = []

    result = stream_output.write_table_or_stream(
        object(),
        tmp_path / "out",
        call_options=None,
        format="python",
        source="python",
        write_stream=lambda stream, path: written.append((stream, path)),
    )

    assert len(written) == 1
    assert raw.main_closed == 1
    assert raw.closed == 2
    assert sink_out.closed == 1
    assert result.stats["skipped_rows"] == 2
    assert result.clean_data is None


def test_streaming_writer_stats_use_diagnostics_snapshot(monkeypatch, tmp_path: Path) -> None:
    """Verify writer stats use diagnostics snapshot."""
    from schema_sanitizer.api_impl import execution_context as pool
    from schema_sanitizer.api_impl import stream_output

    class Diagnostics:
        """Test helper for Diagnostics."""

        def to_json(self) -> str:
            """Return test diagnostics."""
            return '{"skipped_rows":3}'

    class Raw:
        """Test helper for Raw."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.diagnostics = Diagnostics()

        def close_main_stream(self) -> None:
            """Close the main test stream."""

    class Stream:
        """Test helper for Stream."""

        def close_main_stream(self) -> None:
            """Close the main test stream."""

    class SinkOut:
        """Test helper for SinkOut."""

        def __init__(self, raw: Raw) -> None:
            """Initialize the test helper."""
            self._raw = raw
            self._stream = Stream()

        @property
        def raw(self) -> Raw:
            """Return the raw test value."""
            return self._raw

        @property
        def stream(self) -> Stream:
            """Return the test stream."""
            return self._stream

        def close(self) -> None:
            """Close the test sink."""

    raw = Raw()
    monkeypatch.setattr(pool, "default_pool", lambda: _Pool(SinkOut(raw)))

    result = stream_output.write_table_or_stream(
        object(),
        tmp_path / "out",
        call_options=None,
        format="python",
        source="python",
        write_stream=lambda _stream, _path: None,
    )

    assert result.stats["skipped_rows"] == 3


def test_streaming_writer_missing_stream_closes_sink(monkeypatch, tmp_path: Path) -> None:
    """Verify streaming writer missing stream closes sink."""
    from schema_sanitizer.api_impl import execution_context as pool
    from schema_sanitizer.api_impl import stream_output

    class SinkOut:
        """Test helper for SinkOut."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self._raw = SimpleNamespace(diagnostics=None)
            self.closed = 0

        @property
        def raw(self):
            """Return the raw test value."""
            return self._raw

        @property
        def stream(self):
            """Return the test stream."""
            return None

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    sink_out = SinkOut()

    monkeypatch.setattr(pool, "default_pool", lambda: _Pool(sink_out))

    with pytest.raises(RuntimeError, match="did not produce a stream"):
        stream_output.write_table_or_stream(
            object(),
            tmp_path / "out",
            call_options=None,
            format="python",
            source="python",
            write_stream=lambda _stream, _path: None,
        )

    assert sink_out.closed == 1


def test_streaming_writer_failure_closes_full_sink(monkeypatch, tmp_path: Path) -> None:
    """Verify streaming writer failure closes full sink."""
    from schema_sanitizer.api_impl import execution_context as pool
    from schema_sanitizer.api_impl import stream_output

    class Raw:
        """Test helper for Raw."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.diagnostics = SimpleNamespace(skipped_rows=0)
            self.main_closed = 0
            self.closed = 0

        def close_main_stream(self) -> None:
            """Close the main test stream."""
            self.main_closed += 1

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    class Stream:
        """Test helper for Stream."""

        def __init__(self, raw: Raw) -> None:
            """Initialize the test helper."""
            self.raw = raw

        def close_main_stream(self) -> None:
            """Close the main test stream."""
            self.raw.close_main_stream()

        def close(self) -> None:
            """Close the test helper."""
            self.raw.close()

    class SinkOut:
        """Test helper for SinkOut."""

        def __init__(self, raw: Raw) -> None:
            """Initialize the test helper."""
            self._raw = raw
            self._stream = Stream(raw)

        @property
        def raw(self) -> Raw:
            """Return the raw test value."""
            return self._raw

        @property
        def stream(self) -> Stream:
            """Return the test stream."""
            return self._stream

    raw = Raw()
    sink_out = SinkOut(raw)

    monkeypatch.setattr(pool, "default_pool", lambda: _Pool(sink_out))

    def fail_writer(_stream, _path) -> None:
        """Return fail writer for the test."""
        raise RuntimeError("writer failed")

    with pytest.raises(RuntimeError, match="writer failed"):
        stream_output.write_table_or_stream(
            object(),
            tmp_path / "out",
            call_options=None,
            format="python",
            source="python",
            write_stream=fail_writer,
        )

    assert raw.closed == 1
    assert raw.main_closed == 0


def test_streaming_writer_failure_preserves_writer_exception_when_cleanup_fails(
    monkeypatch, tmp_path: Path
) -> None:
    """Verify streaming writer failure preserves writer exception when cleanup fails."""
    from schema_sanitizer.api_impl import execution_context as pool
    from schema_sanitizer.api_impl import stream_output

    class Stream:
        """Test helper for Stream."""

        def close(self) -> None:
            """Close the test helper."""
            raise RuntimeError("stream cleanup failed")

    class SinkOut:
        """Test helper for SinkOut."""

        @property
        def raw(self):
            """Return the raw test value."""
            return None

        @property
        def stream(self):
            """Return the test stream."""
            return Stream()

        def close(self) -> None:
            """Close the test helper."""
            raise RuntimeError("sink cleanup failed")

    monkeypatch.setattr(pool, "default_pool", lambda: _Pool(SinkOut()))

    def fail_writer(_stream, _path) -> None:
        """Return fail writer for the test."""
        raise ValueError("writer failed")

    with pytest.raises(ValueError, match="writer failed"):
        stream_output.write_table_or_stream(
            object(),
            tmp_path / "out",
            call_options=None,
            format="python",
            source="python",
            write_stream=fail_writer,
        )


def test_streaming_writer_stream_property_error_closes_sink(monkeypatch, tmp_path: Path) -> None:
    """Verify streaming writer stream property error closes sink."""
    from schema_sanitizer.api_impl import execution_context as pool
    from schema_sanitizer.api_impl import stream_output

    class SinkOut:
        """Test helper for SinkOut."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.closed = 0

        @property
        def raw(self):
            """Return the raw test value."""
            return None

        @property
        def stream(self):
            """Return the test stream."""
            raise ValueError("stream construction failed")

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    sink_out = SinkOut()

    monkeypatch.setattr(pool, "default_pool", lambda: _Pool(sink_out))

    with pytest.raises(ValueError, match="stream construction failed"):
        stream_output.write_table_or_stream(
            object(),
            tmp_path / "out",
            call_options=None,
            format="python",
            source="python",
            write_stream=lambda _stream, _path: None,
        )

    assert sink_out.closed == 1


def test_streaming_writer_stream_property_error_preserved_when_cleanup_fails(
    monkeypatch, tmp_path: Path
) -> None:
    """Verify streaming writer stream property error preserved when cleanup fails."""
    from schema_sanitizer.api_impl import execution_context as pool
    from schema_sanitizer.api_impl import stream_output

    class SinkOut:
        """Test helper for SinkOut."""

        @property
        def raw(self):
            """Return the raw test value."""
            return None

        @property
        def stream(self):
            """Return the test stream."""
            raise ValueError("stream construction failed")

        def close(self) -> None:
            """Close the test helper."""
            raise RuntimeError("cleanup failed")

    monkeypatch.setattr(pool, "default_pool", lambda: _Pool(SinkOut()))

    with pytest.raises(ValueError, match="stream construction failed"):
        stream_output.write_table_or_stream(
            object(),
            tmp_path / "out",
            call_options=None,
            format="python",
            source="python",
            write_stream=lambda _stream, _path: None,
        )


def test_streaming_writer_missing_stream_preserves_contract_error_when_cleanup_fails(
    monkeypatch, tmp_path: Path
) -> None:
    """Verify streaming writer missing stream preserves contract error when cleanup fails."""
    from schema_sanitizer.api_impl import execution_context as pool
    from schema_sanitizer.api_impl import stream_output

    class SinkOut:
        """Test helper for SinkOut."""

        @property
        def raw(self):
            """Return the raw test value."""
            return None

        @property
        def stream(self):
            """Return the test stream."""
            return None

        def close(self) -> None:
            """Close the test helper."""
            raise RuntimeError("cleanup failed")

    monkeypatch.setattr(pool, "default_pool", lambda: _Pool(SinkOut()))

    with pytest.raises(RuntimeError, match="did not produce a stream"):
        stream_output.write_table_or_stream(
            object(),
            tmp_path / "out",
            call_options=None,
            format="python",
            source="python",
            write_stream=lambda _stream, _path: None,
        )
