"""Combines robust process-stat parsing with bounded threaded Parquet telemetry and
retryable remote-provider cleanup. Spaces and parentheses parse correctly while
malformed input fails closed; telemetry bounds labels or history and resets across fork,
and provider resources stay rooted until all cleanup contexts close."""

from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Any

import pytest
from _support.synchronization import join_thread_or_fail

_NATIVE_STUB_MODULES = (
    "schema_sanitizer.core_impl.native_options",
    "schema_sanitizer.core_impl.execution_policy",
    "schema_sanitizer.api_impl.input.directory_preparation",
    "schema_sanitizer.api_impl.source_plan.attached",
    "schema_sanitizer.api_impl.source_plan.remote_cleanup",
    "schema_sanitizer.api_impl.source_plan.remote_runtime",
    "schema_sanitizer.api_impl.source_plan.remote",
)


class _FailOnceClose:
    """Close double that fails once with a configurable exception type."""

    def __init__(self, error_type: type[BaseException] = OSError) -> None:
        """Initialize the fail once close test double."""
        self.error_type = error_type
        self.calls = 0
        self.closed = False

    def close(self) -> None:
        """Close the resources owned by the fail once close test double."""
        self.calls += 1
        if self.calls == 1:
            raise self.error_type("transient cleanup failure")
        self.closed = True


class _Staged(_FailOnceClose):
    """Staged chunk with a minimal source-count manifest."""

    def __init__(self, *, fail_once: bool = True) -> None:
        """Initialize the staged test double."""
        super().__init__()
        if not fail_once:
            self.calls = 1
        self.manifest = SimpleNamespace(source_batch=SimpleNamespace(sources=(object(),)))


def test_proc_stat_parser_handles_spaces_and_parentheses() -> None:
    """Linux process names cannot shift the start-time field index."""
    from schema_sanitizer.core_impl.process_identity import (
        parse_linux_proc_start_token,
    )

    suffix = ["S", *("0" for _ in range(18)), "987654", "0"]
    raw = f"123 (worker name (nested)) {' '.join(suffix)}"
    assert parse_linux_proc_start_token(raw) == "987654"


def test_proc_stat_parser_fails_closed_on_malformed_input() -> None:
    """Malformed procfs records never create an unstable pseudo-token."""
    from schema_sanitizer.core_impl.process_identity import (
        parse_linux_proc_start_token,
    )

    assert parse_linux_proc_start_token("123 broken") == "unknown"
    assert parse_linux_proc_start_token("123 (name) S 1 2") == "unknown"
    assert (
        parse_linux_proc_start_token(
            "123 (name) " + " ".join(["S", *("0" for _ in range(18)), "not-a-number"])
        )
        == "unknown"
    )


def test_parquet_telemetry_counts_are_atomic_under_threads() -> None:
    """Concurrent route updates cannot lose increments or corrupt snapshots."""
    from schema_sanitizer.adapters.parquet import telemetry

    telemetry.reset_parquet_stream_factory_observability()
    workers = 8
    iterations = 500
    barrier = threading.Barrier(workers)

    def record() -> None:
        """Record one shared route repeatedly after simultaneous release."""
        barrier.wait()
        for _ in range(iterations):
            telemetry.set_parquet_stream_factory_route("native_parquet_stream")

    threads = [threading.Thread(target=record) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        join_thread_or_fail(thread)

    snapshot = telemetry.parquet_stream_factory_observability()
    assert snapshot["route_counts"] == {"native_parquet_stream": workers * iterations}


def test_parquet_telemetry_bounds_counter_cardinality_and_labels() -> None:
    """Unique attacker labels cannot grow route maps or remain verbatim."""
    from schema_sanitizer.adapters.parquet import telemetry

    telemetry.reset_parquet_stream_factory_observability()
    for index in range(400):
        telemetry.set_parquet_stream_factory_route(f"route-{index}-" + ("x" * 20_000))
    snapshot = telemetry.parquet_stream_factory_observability()
    counts = snapshot["route_counts"]
    assert len(counts) <= telemetry._MAX_COUNTER_KEYS
    assert counts[telemetry._OVERFLOW_KEY] > 0
    assert all(len(key) <= telemetry._MAX_LABEL_CHARS for key in counts)
    assert "x" * 1000 not in repr(counts)


def test_parquet_telemetry_bounds_history_and_cyclic_diagnostics() -> None:
    """Fallback history and recursive diagnostic graphs remain bounded."""
    from schema_sanitizer.adapters.parquet import telemetry

    telemetry.reset_parquet_stream_factory_observability()
    recursive: dict[str, object] = {}
    recursive["self"] = recursive
    telemetry.set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=False,
        reason="not_ready",
        recursive=recursive,
    )
    for index in range(100):
        telemetry.record_parquet_fallback_attempt(f"fallback-{index}")
    diagnostics = telemetry.last_parquet_native_reader_diagnostics()
    assert len(diagnostics["fallback_attempt_history"]) == telemetry._MAX_FALLBACK_HISTORY
    assert diagnostics["diagnostic_payload_truncated"] is True
    assert diagnostics["recursive"]["self"] == "<diagnostic-cycle>"


def test_parquet_telemetry_recovers_from_malformed_history_shape() -> None:
    """A hostile diagnostics update cannot break later fallback recording."""
    from schema_sanitizer.adapters.parquet import telemetry

    telemetry.reset_parquet_stream_factory_observability()
    telemetry.update_parquet_native_reader_diagnostics(
        fallback_attempt_history={"unexpected": "mapping"}
    )
    telemetry.record_parquet_fallback_attempt("bounded-route")
    diagnostics = telemetry.last_parquet_native_reader_diagnostics()
    assert diagnostics["fallback_attempt_history"] == [
        {"route": "bounded-route", "status": "attempted"}
    ]


def test_parquet_telemetry_does_not_trust_exception_text() -> None:
    """A hostile exception cannot prevent failure accounting or retain huge text."""
    from schema_sanitizer.adapters.parquet import telemetry

    class HostileError(Exception):
        """Exception whose string conversion is unsafe."""

        def __str__(self) -> str:
            """Raise when the test attempts to render the hostile value."""
            raise RuntimeError("hostile __str__")

    telemetry.reset_parquet_stream_factory_observability()
    telemetry.set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=False,
        reason="not_ready",
    )
    telemetry.record_parquet_fallback_failure("route", HostileError())
    diagnostics = telemetry.last_parquet_native_reader_diagnostics()
    assert "exception text unavailable" in diagnostics["fallback_error"]
    assert telemetry.parquet_stream_factory_observability()["fallback_failure_counts"] == {
        "route": 1
    }


def test_parquet_telemetry_fork_reset_replaces_lock_and_state() -> None:
    """A child starts without inherited telemetry objects or a parent lock."""
    from schema_sanitizer.adapters.parquet import telemetry

    telemetry.reset_parquet_stream_factory_observability()
    telemetry.set_parquet_stream_factory_route("parent")
    old_lock = telemetry._LOCK
    telemetry._reset_after_fork()
    assert telemetry._LOCK is not old_lock
    assert telemetry.parquet_stream_factory_observability()["route_counts"] == {}


def test_retryable_sequence_retains_base_exception_owner() -> None:
    """Control-flow exceptions during best-effort close cannot drop ownership."""
    from schema_sanitizer.core_impl.resource_lifecycle import (
        _close_sequence_retryably,
    )

    item = _FailOnceClose(KeyboardInterrupt)
    items: list[object] = [item]
    _close_sequence_retryably(items)
    assert items == [item]
    _close_sequence_retryably(items)
    assert items == []
    assert item.closed


def test_remote_provider_retains_failed_retained_chunk(native_stub: None) -> None:
    """A failed retained-chunk close remains owned and retryable."""
    from schema_sanitizer.api_impl.source_plan.remote import (
        RemotePathSourceChunkProvider,
    )

    staged = _Staged()
    provider = RemotePathSourceChunkProvider(
        retained_chunks=[staged],
        remaining_manifest=None,
    )
    with pytest.raises(OSError, match="transient cleanup failure"):
        provider.close()
    assert list(provider._retained_chunks) == [staged]
    assert not provider.is_closed
    provider.close()
    assert provider.is_closed
    assert staged.closed


def test_remote_provider_retains_current_after_planning_cleanup_failure(
    native_stub: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Planning failure cannot discard a staged chunk whose close also failed."""
    from schema_sanitizer.api_impl.source_plan import remote_runtime as remote_provider
    from schema_sanitizer.api_impl.source_plan.remote import (
        RemotePathSourceChunkProvider,
    )

    staged = _Staged()
    provider = RemotePathSourceChunkProvider(
        retained_chunks=[staged],
        remaining_manifest=None,
    )
    monkeypatch.setattr(
        remote_provider,
        "source_plan_from_native_manifest",
        lambda _manifest: (_ for _ in ()).throw(ValueError("planning failed")),
    )
    with pytest.raises(ValueError, match="planning failed") as captured:
        provider.next_sources()
    assert provider._current_staged is staged
    assert any("cleanup also failed" in note for note in captured.value.__notes__)
    provider.close_all()
    assert staged.closed


def test_remote_provider_retains_failed_context_exit(
    native_stub: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed iterator-context exit remains available for a second close."""
    from schema_sanitizer.api_impl.source_plan import remote as remote_module
    from schema_sanitizer.api_impl.source_plan.remote import (
        RemotePathSourceChunkProvider,
    )

    class Context:
        """Context whose exit fails once."""

        def __init__(self) -> None:
            """Initialize the context test double."""
            self.calls = 0

        def __enter__(self) -> Any:
            """Enter the context managed by the context test double."""
            return iter(())

        def __exit__(self, *_exc: object) -> None:
            """Exit the context managed by the context test double and run cleanup."""
            self.calls += 1
            if self.calls == 1:
                raise OSError("exit busy")

    context = Context()
    monkeypatch.setattr(
        remote_module,
        "open_staged_remote_chunks",
        lambda *_args, **_kwargs: context,
    )
    provider = RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=SimpleNamespace(),
    )
    with pytest.raises(OSError, match="exit busy"):
        provider._next_staged_chunk()
    assert provider._remaining_context is context
    provider.close()
    assert provider._remaining_context is None
    assert provider.is_closed


def test_remote_provider_rechecks_donor_until_it_closes(native_stub: None) -> None:
    """A donor observed early is not permanently skipped before it finishes."""
    from schema_sanitizer.api_impl.source_plan.remote import (
        RemotePathSourceChunkProvider,
    )

    staged = _Staged(fail_once=False)
    donor = RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=None,
        retain_consumed_chunks=1,
    )
    donor._current_staged = staged
    recipient = RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=None,
        retained_chunk_donor=donor,
    )
    recipient._adopt_preserved_probe_chunks()
    assert recipient._retained_chunk_donor is donor
    assert not recipient._donor_checked

    donor.close()
    recipient._adopt_preserved_probe_chunks()
    assert recipient._retained_chunk_donor is None
    assert list(recipient._retained_chunks) == [staged]
    assert recipient._remaining_start == 1
    recipient.close_all()


def test_remote_provider_preserved_cleanup_is_retryable(native_stub: None) -> None:
    """Failed preserved chunks remain owned with accurate file counts."""
    from schema_sanitizer.api_impl.source_plan.remote import (
        RemotePathSourceChunkProvider,
    )

    staged = _Staged()
    provider = RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=None,
    )
    provider._closed = True
    provider._preserved_chunks = [staged]
    provider._preserved_file_count = 1
    with pytest.raises(OSError, match="transient cleanup failure"):
        provider.close_all()
    assert provider._preserved_chunks == [staged]
    assert provider.preserved_file_count == 1
    provider.close_all()
    assert provider._preserved_chunks == []
    assert provider.preserved_file_count == 0


def test_remote_provider_rejects_child_before_touching_resources(
    native_stub: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fork child cannot consume inherited staged chunks or iterators."""
    from schema_sanitizer.api_impl.source_plan import remote_runtime as remote_provider
    from schema_sanitizer.api_impl.source_plan.remote import (
        RemotePathSourceChunkProvider,
    )

    provider = RemotePathSourceChunkProvider(
        retained_chunks=[object()],
        remaining_manifest=None,
    )
    parent_pid = provider._pid
    monkeypatch.setattr(remote_provider.os, "getpid", lambda: parent_pid + 1)
    with pytest.raises(RuntimeError, match="cannot be reused after fork"):
        provider.next_sources()
