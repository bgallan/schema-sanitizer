"""Regression coverage for memory ingest plan retains failed keepalive for retry."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest


class _FailOnceClose:
    """Close double that fails once before committing."""

    def __init__(self) -> None:
        """Initialize one pending failure."""
        self.calls = 0
        self.closed = False

    def close(self) -> None:
        """Fail once and then close successfully."""
        self.calls += 1
        if self.calls == 1:
            raise OSError("transient close failure")
        self.closed = True


class _CloseCounter:
    """Idempotent close counter."""

    def __init__(self) -> None:
        """Initialize the counter."""
        self.calls = 0

    def close(self) -> None:
        """Record one close call."""
        self.calls += 1


def test_ingest_plan_retains_failed_keepalive_for_retry() -> None:
    """Prepared input ownership survives a transient close failure."""
    from schema_sanitizer.api_impl.ingest import NativeIngestPlan

    keepalive = _FailOnceClose()
    plan = NativeIngestPlan(
        data=b"x",
        source="text",
        format="json",
        call_options=None,
        memory_limit_bytes=None,
        input_text_encoding="utf-8",
        keepalive=keepalive,
    )

    plan.close_keepalive()
    assert plan.keepalive is keepalive
    plan.close_keepalive()
    assert plan.keepalive is None
    assert keepalive.closed


def test_registry_stream_retains_failed_raw_and_closes_independent_items() -> None:
    """One failed backend does not lose ownership or block unrelated cleanup."""
    from schema_sanitizer.api_impl.source_plan.registry import (
        OpenedSourcePlanRegistryStream,
    )

    raw = _FailOnceClose()
    independent = _CloseCounter()
    opened = OpenedSourcePlanRegistryStream(
        stream=None,
        raw_stream=raw,
        schema_registry_json="{}",
        schema_drifts_json="[]",
        close_items=[raw, independent],
    )

    opened.close()
    assert opened.raw_stream is raw
    assert opened.close_items == [raw]
    assert independent.calls == 1

    opened.close()
    assert opened.raw_stream is None
    assert opened.close_items == []
    assert raw.calls == 2


def test_registry_wrapper_does_not_double_close_wrapped_raw() -> None:
    """A successful Python wrapper close consumes its wrapped raw ownership once."""
    from schema_sanitizer.api_impl.source_plan.registry import (
        OpenedSourcePlanRegistryStream,
    )

    raw = _CloseCounter()

    class Wrapper:
        """Minimal wrapper that owns one raw backend."""

        def __init__(self) -> None:
            """Retain the wrapped backend."""
            self._raw = raw
            self.calls = 0

        def close(self) -> None:
            """Close the backend exactly once."""
            self.calls += 1
            self._raw.close()

    wrapper = Wrapper()
    opened = OpenedSourcePlanRegistryStream(
        stream=wrapper,
        raw_stream=raw,
        schema_registry_json="{}",
        schema_drifts_json="[]",
        close_items=[raw],
    )

    opened.close()
    assert wrapper.calls == 1
    assert raw.calls == 1
    assert opened.stream is None
    assert opened.raw_stream is None
    assert opened.close_items == []


def test_replay_reader_retains_reader_and_keepalive_failures() -> None:
    """Reader ownership and mapped sources remain retryable independently."""
    from schema_sanitizer.api_impl.parquet.replay_stream import _ReplayReader

    reader = _FailOnceClose()
    reader.schema = object()
    keepalive = _FailOnceClose()
    replay = _ReplayReader(reader, keepalive=(keepalive,))

    replay.close()
    assert replay._reader is reader
    assert replay._keepalive == (keepalive,)
    replay.close()
    assert replay._reader is None
    assert replay._keepalive == (keepalive,)
    replay.close()
    assert replay._keepalive == ()


def test_arrow_stream_retains_raw_and_keepalive_after_failed_main_close() -> None:
    """Arrow wrapper cleanup does not orphan a failing main stream."""
    from schema_sanitizer.api_impl.streams import ArrowCStream

    raw = _FailOnceClose()
    keepalive = _CloseCounter()
    stream = ArrowCStream(raw)
    stream._keepalive = keepalive

    stream.close()
    assert stream._raw is raw
    assert stream._keepalive is keepalive
    assert keepalive.calls == 0
    stream.close()
    assert stream._raw is None
    assert not hasattr(stream, "_keepalive")
    assert keepalive.calls == 1


def test_stream_close_main_deduplicates_identical_reader_and_raw(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reader used as the raw backend is never closed twice in one teardown."""
    from schema_sanitizer.api_impl import streams as module

    raw = _FailOnceClose()
    raw.schema = None
    monkeypatch.setattr(
        module._pyarrow_streams,
        "is_record_batch_reader",
        lambda obj, *, feature: obj is raw,
    )
    stream = module.Stream(raw)

    stream.close_main_stream()
    assert stream._reader is raw
    assert stream._raw is raw
    assert raw.calls == 1
    stream.close_main_stream()
    assert stream._reader is None
    assert stream._raw is None
    assert raw.calls == 2


def test_sink_output_retains_failed_backend_and_keepalive() -> None:
    """Native output cleanup preserves every failed owner for another attempt."""
    from schema_sanitizer.core_impl.native_results import SinkOutput

    backend = _FailOnceClose()
    keepalive = _FailOnceClose()
    output = SinkOutput(sink="table", main_stream_capsule=object())
    output._table_backend = backend
    output._keepalive = [keepalive]

    output.close()
    assert output._table_backend is backend
    assert output._main is not None
    assert output._keepalive == [keepalive]

    output.close()
    assert output._table_backend is None
    assert output._main is None
    assert output._keepalive == [keepalive]

    output.close()
    assert output._keepalive == []


def test_schema_cache_replaces_inherited_lock_without_touching_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child identity clears retained schemas before acquiring any old lock."""
    from schema_sanitizer.adapters.pyarrow import schema_decision_cache as module

    class Schema:
        """Simple identity-cache token."""

    cache = module.SchemaDecisionCache(max_size=4)
    schema = Schema()
    cache.set(schema, True, include_text=False)
    old_lock = cache._lock
    old_lock.acquire()
    try:
        monkeypatch.setattr(module.os, "getpid", lambda: cache._pid + 1)
        assert cache.get_by_object(schema) is None
        assert cache._lock is not old_lock
        assert cache._by_object_id == {}
        assert cache._by_fingerprint == {}
        assert cache._by_schema_text == {}
    finally:
        old_lock.release()


def test_schema_cache_clear_releases_all_retained_key_budgets() -> None:
    """Explicit cache cleanup resets entries and byte accounting together."""
    from schema_sanitizer.adapters.pyarrow.schema_decision_cache import (
        SchemaDecisionCache,
    )

    schema = SimpleNamespace(to_string=lambda **_kwargs: "schema")
    cache = SchemaDecisionCache(max_size=4, max_key_bytes=64)
    cache.set(schema, True, include_text=True)
    cache.set_fingerprint(schema, b"fingerprint", True)

    cache.clear()

    assert cache._by_object_id == {}
    assert cache._by_fingerprint == {}
    assert cache._by_schema_text == {}
    assert cache._fingerprint_bytes == 0
    assert cache._schema_text_bytes == 0


def test_lazy_analytical_resources_retain_each_failed_owner() -> None:
    """Lazy stream teardown keeps independently failing owners retryable."""
    from schema_sanitizer.api_impl.batch_streaming import _AnalyticalStreamResources

    opened = _FailOnceClose()
    prepared = _FailOnceClose()
    operation = _FailOnceClose()
    resources = _AnalyticalStreamResources(opened, prepared, operation)

    resources.close()
    assert resources._opened is opened
    assert resources._prepared_input is prepared
    assert resources._operation_context is operation

    resources.close()
    assert resources._opened is None
    assert resources._prepared_input is None
    assert resources._operation_context is None


def test_sink_result_defers_keepalive_until_raw_cleanup_commits() -> None:
    """Sink keepalive ownership is not removed while its raw stream still fails."""
    from schema_sanitizer.api_impl.results import SinkResult

    raw = _FailOnceClose()
    keepalive = _CloseCounter()
    result = SinkResult(raw)
    result._keepalive = keepalive

    result.close()
    assert result._raw is raw
    assert result._keepalive is keepalive
    assert keepalive.calls == 0

    result.close()
    assert result._raw is None
    assert not hasattr(result, "_keepalive")
    assert keepalive.calls == 1


def test_result_finalizer_does_not_close_parent_owners_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child-side result finalizer leaves parent-owned resources untouched."""
    from schema_sanitizer.api_impl import results as module

    owner = _CloseCounter()
    result = module.Result(SimpleNamespace(diagnostics=None), clean_data=None)
    result._resource_owner = owner
    monkeypatch.setattr(module.os, "getpid", lambda: result._pid + 1)

    result.__del__()

    assert owner.calls == 0
    assert result._resource_owner is owner


def test_execution_context_rejects_direct_child_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """High-level native contexts fail before touching inherited ABI state."""
    from schema_sanitizer.api_impl import execution_context as module

    context = module.ExecutionContext()
    monkeypatch.setattr(module.os, "getpid", lambda: context._pid + 1)

    with pytest.raises(RuntimeError, match="cannot be reused after fork"):
        context.memory_stats()


def test_execution_context_pool_replaces_inherited_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The process-local default cache never returns a parent's native context."""
    from schema_sanitizer.api_impl import execution_context as module

    pool = module.ExecutionContextPool()
    inherited = object()
    replacement = object()
    pool._ctx = inherited
    parent_pid = pool._pid
    monkeypatch.setattr(module.os, "getpid", lambda: parent_pid + 1)
    monkeypatch.setattr(module, "ExecutionContext", lambda: replacement)

    assert pool.get() is replacement
    assert pool._ctx is replacement
    assert pool._pid == parent_pid + 1


def test_execution_context_pool_serializes_lazy_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent callers construct exactly one process-local native context."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier, Lock

    from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS

    from schema_sanitizer.api_impl import execution_context as module

    constructed = 0
    counter_lock = Lock()
    caller_barrier = Barrier(8)
    replacement = object()

    def construct() -> object:
        """Count one deliberately overlapping construction attempt."""
        nonlocal constructed
        with counter_lock:
            constructed += 1
        return replacement

    def get_context(_index: int) -> object:
        caller_barrier.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
        return pool.get()

    monkeypatch.setattr(module, "ExecutionContext", construct)
    pool = module.ExecutionContextPool()
    with ThreadPoolExecutor(max_workers=8) as executor:
        contexts = list(executor.map(get_context, range(32)))

    assert constructed == 1
    assert all(context is replacement for context in contexts)


def test_execution_context_pool_replaces_held_parent_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child never waits on a lock that could have been held at fork time."""
    from schema_sanitizer.api_impl import execution_context as module

    pool = module.ExecutionContextPool()
    inherited_lock = pool._lock
    inherited_lock.acquire()
    parent_pid = pool._pid
    replacement = object()
    monkeypatch.setattr(module.os, "getpid", lambda: parent_pid + 1)
    monkeypatch.setattr(module, "ExecutionContext", lambda: replacement)
    try:
        assert pool.get() is replacement
        assert pool._lock is not inherited_lock
    finally:
        inherited_lock.release()


def test_remote_manifest_retains_failed_prefetch_and_pool_for_retry() -> None:
    """Manifest cleanup never loses the only owner of a failed resource."""
    from threading import Lock

    from schema_sanitizer.api_impl.input.directory_preparation import (
        RemoteNativeDirectorySourceManifest,
    )

    class SourceBatch:
        """Minimal manifest batch double."""

        sources = (object(), object())

    chunk = _FailOnceClose()
    chunk.manifest = SimpleNamespace(source_batch=SourceBatch())
    pool_owner = _FailOnceClose()
    manifest = object.__new__(RemoteNativeDirectorySourceManifest)
    manifest._prefetch_lock = Lock()
    manifest._pid = os.getpid()
    manifest._prefetched_chunks = [chunk]
    manifest._prefetched_file_count = 2
    manifest._temporary_storage_pool = pool_owner

    with pytest.raises(OSError, match="transient close failure"):
        manifest.close()
    assert manifest._prefetched_chunks == [chunk]
    assert manifest._prefetched_file_count == 2
    assert manifest._temporary_storage_pool is pool_owner

    manifest.close()
    assert manifest._prefetched_chunks == []
    assert manifest._prefetched_file_count == 0
    assert manifest._temporary_storage_pool is None
    assert chunk.calls == 2
    assert pool_owner.calls == 2


def test_parquet_chunk_provider_retains_failed_factory_for_close_retry() -> None:
    """An active Parquet chunk is not discarded when one factory close fails."""
    from schema_sanitizer.api_impl.parquet.arrow_sources import (
        ParquetArrowSourceChunkProvider,
    )

    factory = _FailOnceClose()
    provider = object.__new__(ParquetArrowSourceChunkProvider)
    provider._pid = os.getpid()
    provider._closed = False
    provider._current = [(factory, "file.parquet")]

    provider.close()
    assert provider._closed
    assert provider._current == [(factory, "file.parquet")]

    provider.close()
    assert provider._current == []
    assert factory.calls == 2


def test_parquet_source_cleanup_deduplicates_factory_identity() -> None:
    """Duplicate entries cannot double-close one Parquet factory."""
    from schema_sanitizer.api_impl.parquet.arrow_sources import (
        close_parquet_arrow_sources,
    )

    factory = _CloseCounter()
    sources = [(factory, "a"), (factory, "b")]

    assert close_parquet_arrow_sources(sources) == []
    assert sources == []
    assert factory.calls == 1


def test_schema_cache_enforces_utf8_bytes_not_character_count() -> None:
    """Unicode schema keys cannot exceed the advertised byte budget."""
    from schema_sanitizer.adapters.pyarrow.schema_decision_cache import (
        SchemaDecisionCache,
    )

    class Schema:
        """Schema text double."""

        def __init__(self, text: str) -> None:
            """Store one textual key."""
            self.text = text

        def to_string(self, **_kwargs: object) -> str:
            """Return the key."""
            return self.text

    cache = SchemaDecisionCache(max_size=4, max_key_bytes=4)
    exact = Schema("💥")
    oversized = Schema("💥x")

    cache.set(exact, True, include_text=True)
    cache.set(oversized, False, include_text=True)

    assert cache.get_by_text(exact) is True
    assert cache.get_by_text(oversized) is None
    assert cache._schema_text_bytes == 4
    assert cache.get_by_object(oversized) is None


def test_bounded_utf8_measurement_handles_surrogates_without_full_copy() -> None:
    """The shared meter uses surrogatepass and stops just beyond its ceiling."""
    from schema_sanitizer.core_impl.bounded_text import utf8_size_bounded

    assert utf8_size_bounded("\ud800", 3) == 3
    assert utf8_size_bounded("💥" * 100_000, 32) == 33


def test_native_source_plan_closes_payload_and_releases_native_graph() -> None:
    """Remote plan ownership and native metadata remain until cleanup succeeds."""
    from schema_sanitizer.input_impl.source_plan import REMOTE_CHUNKS, NativeSourcePlan

    payload = _FailOnceClose()
    close_item = _FailOnceClose()
    native = object()
    batch = object()
    plan = NativeSourcePlan(
        kind=REMOTE_CHUNKS,
        payload=payload,
        input_format="json",
        route_name="remote",
        source_batch=batch,
        native_payload=native,
        close_items=[close_item],
    )

    plan.close()
    assert plan.payload is payload
    assert plan.close_items == [close_item]
    assert plan.native_payload is native
    assert plan.source_batch is batch

    plan.close()
    assert plan.payload is None
    assert plan.close_items == []
    assert plan.native_payload is None
    assert plan.source_batch is None


def test_native_source_sequence_retains_only_failed_children() -> None:
    """Closed child plans leave a sequence while failed children stay retryable."""
    from schema_sanitizer.input_impl.source_plan import SEQUENCE, NativeSourcePlan

    failing = _FailOnceClose()
    successful = _CloseCounter()
    plan = NativeSourcePlan(
        kind=SEQUENCE,
        payload=(successful, failing),
        input_format="json",
        route_name="sequence",
        native_payload=object(),
    )

    plan.close()
    assert plan.payload == (failing,)
    assert successful.calls == 1
    assert failing.calls == 1
    assert plan.native_payload is not None

    plan.close()
    assert plan.payload == ()
    assert failing.calls == 2
    assert plan.native_payload is None


def test_staged_manifest_clears_keepalive_only_after_success() -> None:
    """Staged manifests do not retain closed paths or orphan failed paths."""
    from schema_sanitizer.input_impl.prepared import StagedNativeDirectoryManifest

    keepalive = _FailOnceClose()
    staged = StagedNativeDirectoryManifest(object(), keepalive)

    with pytest.raises(OSError, match="transient close failure"):
        staged.close()
    assert staged.keepalive is keepalive

    staged.close()
    assert staged.keepalive is None


def test_transcoding_reader_retains_stream_after_close_failure() -> None:
    """A failed file-handle close remains retryable on the reader."""
    from schema_sanitizer.input_impl.selection import TranscodingPathByteReader

    stream = _FailOnceClose()
    reader = object.__new__(TranscodingPathByteReader)
    reader._stream = stream

    with pytest.raises(OSError, match="transient close failure"):
        reader._close_stream()
    assert reader._stream is stream

    reader._close_stream()
    assert reader._stream is None


def test_sync_directory_session_retains_exit_stack_after_failure() -> None:
    """Provider cleanup can be retried after an ExitStack close failure."""
    from schema_sanitizer.remote_impl.sync_backend import SyncDirectoryDownloadSession

    stack = _FailOnceClose()
    session = object.__new__(SyncDirectoryDownloadSession)
    session._pid = os.getpid()
    session._stack = stack
    session._context = object()

    with pytest.raises(OSError, match="transient close failure"):
        session.close()
    assert session._stack is stack
    assert session._context is not None

    session.close()
    assert session._stack is None
    assert session._context is None


def test_native_source_plan_finalizer_retries_abandoned_payload() -> None:
    """An abandoned plan transfers cleanup to a governed safe point."""
    from schema_sanitizer.core_impl.finalizer_cleanup import drain_finalizer_cleanup
    from schema_sanitizer.input_impl.source_plan import REMOTE_CHUNKS, NativeSourcePlan

    payload = _FailOnceClose()
    plan = NativeSourcePlan(
        kind=REMOTE_CHUNKS,
        payload=payload,
        input_format="json",
        route_name="remote",
    )

    plan.close()
    assert plan.payload is payload
    plan.__del__()
    drain_finalizer_cleanup()
    assert plan.payload is None
    assert payload.calls == 2


def test_registry_stream_finalizer_retries_failed_raw_cleanup() -> None:
    """An abandoned registry stream transfers cleanup to a safe point."""
    from schema_sanitizer.api_impl.source_plan.registry import (
        OpenedSourcePlanRegistryStream,
    )
    from schema_sanitizer.core_impl.finalizer_cleanup import drain_finalizer_cleanup

    raw = _FailOnceClose()
    opened = OpenedSourcePlanRegistryStream(
        stream=None,
        raw_stream=raw,
        schema_registry_json="{}",
        schema_drifts_json="[]",
        close_items=[raw],
    )

    opened.close()
    assert opened.raw_stream is raw
    opened.__del__()
    drain_finalizer_cleanup()
    assert opened.raw_stream is None
    assert opened.close_items == []


def test_remote_manifest_rejects_child_before_inherited_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child fails before acquiring a potentially locked parent manifest mutex."""
    from threading import Lock

    from schema_sanitizer.api_impl.input import directory_preparation as module

    manifest = object.__new__(module.RemoteNativeDirectorySourceManifest)
    parent_pid = os.getpid()
    manifest._pid = parent_pid
    manifest._prefetch_lock = Lock()
    manifest._prefetch_lock.acquire()
    monkeypatch.setattr(module.os, "getpid", lambda: parent_pid + 1)
    try:
        with pytest.raises(RuntimeError, match="cannot be reused after fork"):
            manifest.take_prefetched_chunks()
    finally:
        manifest._prefetch_lock.release()
