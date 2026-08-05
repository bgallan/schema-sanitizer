"""Tests stream and result resource lifecycle contracts."""

from __future__ import annotations

import gc
from types import SimpleNamespace

import pytest


class _CloseCounter:
    """Test double that counts idempotent lifecycle close calls."""

    def __init__(self) -> None:
        """Initialize the close counter."""
        self.closed = 0

    def close(self) -> None:
        """Record one close call."""
        self.closed += 1


class _CloseCountingReader(_CloseCounter):
    """Close-counting reader double with a minimal Arrow-like schema."""

    schema = None


# Split from test_runtime_lifecycle.py: test_abi3_sink_output_close_main_stream_preserves_diagnostics, test_arrow_c_stream_close_prefers_main_stream_only_close, test_arrow_c_stream_close_releases_keepalive_reference, ...


def test_abi3_sink_output_close_main_stream_preserves_diagnostics() -> None:
    """Verify abi3 sink output close main stream preserves diagnostics."""
    from schema_sanitizer.core_impl.native_results import SinkOutput

    main_capsule = object()
    out = SinkOutput(
        sink="stream",
        main_stream_capsule=main_capsule,
        diagnostics_json='{"skipped_rows":1}',
    )

    out.close_main_stream()

    with pytest.raises(AttributeError):
        out.__arrow_c_stream__()
    assert out.diagnostics.skipped_rows == 1


def test_arrow_c_stream_close_prefers_main_stream_only_close() -> None:
    """Verify arrow c stream close prefers main stream only close."""
    from schema_sanitizer.api_impl.streams import ArrowCStream

    class Raw:
        """Test helper for Raw."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.main_closed = 0
            self.closed = 0

        def close_main_stream(self) -> None:
            """Close the main test stream."""
            self.main_closed += 1

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

        def __arrow_c_stream__(self):
            """Return the Arrow C stream capsule."""
            return object()

    raw = Raw()
    stream = ArrowCStream(raw)

    stream.close()

    assert raw.main_closed == 1
    assert raw.closed == 0


def test_arrow_c_stream_close_releases_keepalive_reference() -> None:
    """Verify arrow c stream close releases keepalive reference."""
    from schema_sanitizer.api_impl.streams import ArrowCStream

    class Raw:
        """Test helper for Raw."""

        closed = False

        def close(self):
            """Close the test helper."""
            self.closed = True

        def __arrow_c_stream__(self):
            """Return the Arrow C stream capsule."""
            return object()

    class Keepalive:
        """Test helper for Keepalive."""

        closed = False

        def close(self):
            """Close the test helper."""
            self.closed = True

    raw = Raw()
    keepalive = Keepalive()
    stream = ArrowCStream(raw)
    object.__setattr__(stream, "_keepalive", keepalive)

    stream.close()

    assert raw.closed is True
    assert keepalive.closed is True
    assert not hasattr(stream, "_keepalive")


def test_sink_result_close_clears_owned_references() -> None:
    """Verify sink result close clears owned references."""
    from schema_sanitizer.api_impl.results import SinkResult

    class Closable:
        """Test helper for Closable."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.closed = 0

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    raw = Closable()
    stream = Closable()
    result = SinkResult(raw)
    result._stream = stream

    result.close()
    result.close()

    assert raw.closed == 1
    assert stream.closed == 1
    assert result.raw is None
    assert result._stream is None


def test_stream_close_deduplicates_reader_raw_close(monkeypatch) -> None:
    """Verify stream close deduplicates reader raw close."""
    from schema_sanitizer.api_impl import streams as stream_batches

    Stream = stream_batches.Stream

    reader = _CloseCountingReader()

    def fake_is_record_batch_reader(obj, *, feature):
        """Return fake is record batch reader for the test."""
        assert obj is reader
        assert feature == "Stream construction"
        return True

    monkeypatch.setattr(
        stream_batches._pyarrow_streams,
        "is_record_batch_reader",
        fake_is_record_batch_reader,
    )

    stream = Stream(reader)
    stream.close()
    stream.close()

    assert reader.closed == 1
    assert stream._raw is None
    assert stream._reader is None


def test_stream_close_main_stream_preserves_diagnostics(monkeypatch) -> None:
    """Verify stream close main stream preserves diagnostics."""
    from schema_sanitizer.api_impl import streams as stream_batches

    Stream = stream_batches.Stream

    class Raw:
        """Test helper for Raw."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.main_closed = 0
            self.closed = 0

        def __arrow_c_stream__(self):
            """Return the Arrow C stream capsule."""
            return object()

        def close_main_stream(self) -> None:
            """Close the main test stream."""
            self.main_closed += 1

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    reader = _CloseCountingReader()
    raw = Raw()

    def fake_is_record_batch_reader(obj, *, feature):
        """Return fake is record batch reader for the test."""
        assert obj is raw
        assert feature == "Stream construction"
        return False

    def fake_reader_from_stream_like(obj, *, feature):
        """Return fake reader from stream like for the test."""
        assert obj is raw
        assert feature == "Stream construction"
        return reader

    monkeypatch.setattr(
        stream_batches._pyarrow_streams,
        "is_record_batch_reader",
        fake_is_record_batch_reader,
    )
    monkeypatch.setattr(
        stream_batches._pyarrow_streams,
        "reader_from_stream_like",
        fake_reader_from_stream_like,
    )

    stream = Stream(raw)
    stream.close_main_stream()
    stream.close_main_stream()

    assert reader.closed == 1
    assert raw.main_closed == 1
    assert raw.closed == 0
    assert stream._reader is None
    assert stream._raw is None


def test_result_drop_closes_private_resource_owner() -> None:
    """Verify result drop closes private resource owner."""

    from schema_sanitizer.api_impl.results import Result

    class Owner:
        """Test helper for Owner."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.closed = 0

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    owner = Owner()
    result = Result(SimpleNamespace(diagnostics=None), clean_data=None)
    object.__setattr__(result, "_resource_owner", owner)

    del result
    gc.collect()

    assert owner.closed == 1


def test_arrow_c_stream_drop_closes_main_stream_and_keepalive() -> None:
    """Verify arrow c stream drop closes main stream and keepalive."""
    from schema_sanitizer.api_impl.streams import ArrowCStream

    class Raw:
        """Test helper for Raw."""

        def __init__(self) -> None:
            """Initialize the test helper."""
            self.main_closed = 0
            self.closed = 0

        def close_main_stream(self) -> None:
            """Close the main test stream."""
            self.main_closed += 1

        def close(self) -> None:
            """Close the test helper."""
            self.closed += 1

    raw = Raw()
    keepalive = _CloseCounter()
    stream = ArrowCStream(raw)
    object.__setattr__(stream, "_keepalive", keepalive)

    del stream
    gc.collect()

    assert raw.main_closed == 1
    assert raw.closed == 0
    assert keepalive.closed == 1


def test_result_drop_closes_private_keepalive() -> None:
    """Verify result drop closes private keepalive."""

    from schema_sanitizer.api_impl.results import Result

    keepalive = _CloseCounter()
    result = Result(SimpleNamespace(diagnostics=None), clean_data=None)
    object.__setattr__(result, "_keepalive", keepalive)

    del result
    gc.collect()

    assert keepalive.closed == 1


def test_abi3_runtime_support_destructors_suppress_cleanup_errors() -> None:
    """Verify abi3 runtime support destructors suppress cleanup errors."""
    from schema_sanitizer.core_impl.native_results import SinkOutput

    sink = SinkOutput(sink="stream", diagnostics_json="{}")
    sink.__del__()
