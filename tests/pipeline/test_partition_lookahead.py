"""One-partition source lookahead contracts.

It covers one-partition prefetch, result ordering, dynamic options, temporary-storage
contention, remote resume, and retained-resource finalization.
"""

from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS

from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
from schema_sanitizer.api_impl.source_plan import remote as remote_source_plan
from schema_sanitizer.core_impl.temporary_storage import TemporaryStoragePermitPool
from schema_sanitizer.errors import SchemaSanitizerResourceError
from schema_sanitizer.input_impl.prepared import PreparedPublicInput
from schema_sanitizer.pipeline import PartitionRunPlan
from schema_sanitizer.pipeline.advanced import run_partitioned_to_parquet
from schema_sanitizer.pipeline.partition_lookahead import PartitionSourceLookahead


def _jsonl_plan(tmp_path: Path, ordinal: int) -> PartitionRunPlan:
    """Create one local JSONL input/output partition."""
    source = tmp_path / f"source-{ordinal}.jsonl"
    source.write_text(f'{{"value": {ordinal}}}\n', encoding="utf-8")
    return PartitionRunPlan(
        None,
        str(source),
        str(tmp_path / f"output-{ordinal}.parquet"),
    )


def _multi_kwargs() -> dict[str, Any]:
    """Return static options that enable safe partition lookahead."""
    return {
        "input_format": "jsonl",
        "input_mode": "single_file",
        "multi_threading": True,
        "memory_limit_bytes": 64 << 20,
    }


def test_operation_context_forks_share_resources_but_not_metadata(monkeypatch) -> None:
    """Forked partitions share one budget/coordinator while owning timestamps."""
    from schema_sanitizer.api_impl import operation_context as context_module

    timestamps = iter(
        [
            context_module.OperationTimestamps(100, "1970-01-01T00:00:00.000100Z"),
            context_module.OperationTimestamps(200, "1970-01-01T00:00:00.000200Z"),
        ]
    )
    monkeypatch.setattr(context_module, "capture_operation_timestamps", lambda: next(timestamps))

    root = OperationExecutionContext(threading_mode="multi", memory_limit_bytes=64 << 20)
    child = root.fork()
    try:
        assert root.temporary_storage is child.temporary_storage
        assert root.ingestion_timestamp_micros == 100
        assert child.ingestion_timestamp_micros == 200
        lease = child.temporary_storage.acquire(1024, label="forked partition")
        root.close()
        assert child.temporary_storage.snapshot().reserved_bytes == 1024
        lease.release()
    finally:
        child.close()
        root.close()

    assert child.temporary_storage.snapshot().active_leases == 0


def test_multi_pipeline_prepares_next_partition_before_current_callback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Static multi pipelines overlap exactly the next immutable source."""
    from schema_sanitizer.pipeline import partition_lookahead as lookahead_module

    plans = [_jsonl_plan(tmp_path, ordinal) for ordinal in range(2)]
    original_prepare = lookahead_module.prepare_public_input
    second_started = threading.Event()
    release_second = threading.Event()
    preparation_threads: list[str] = []

    def delayed_prepare(path: str, **kwargs: Any) -> PreparedPublicInput:
        """Pause only speculative preparation of the second source."""
        preparation_threads.append(threading.current_thread().name)
        if path == plans[1].source_uri:
            second_started.set()
            assert release_second.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
        return original_prepare(path, **kwargs)

    monkeypatch.setattr(lookahead_module, "prepare_public_input", delayed_prepare)
    callbacks: list[int] = []

    def after_partition(index: int, *_args: Any) -> None:
        """Confirm the next source started before partition one was reported."""
        if index == 1:
            assert second_started.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
            release_second.set()
        callbacks.append(index)

    result = run_partitioned_to_parquet(
        plans,
        initial_schema_registry={},
        to_parquet_kwargs=_multi_kwargs(),
        after_partition=after_partition,
    )

    assert callbacks == [1, 2]
    assert len(result.completed_runs) == 2
    assert preparation_threads[0] == threading.current_thread().name
    assert any(
        name.startswith("schema-sanitizer-partition-lookahead") for name in preparation_threads
    )
    assert all(Path(plan.output_uri).is_file() for plan in plans)


def test_lookahead_error_is_retained_until_its_partition_ordinal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A future source failure cannot suppress the current partition commit."""
    from schema_sanitizer.pipeline import partition_lookahead as lookahead_module

    plans = [_jsonl_plan(tmp_path, ordinal) for ordinal in range(2)]
    original_prepare = lookahead_module.prepare_public_input
    failure_ready = threading.Event()
    callbacks: list[int] = []

    def failing_prepare(path: str, **kwargs: Any) -> PreparedPublicInput:
        """Fail the speculative second source on its worker thread."""
        if path == plans[1].source_uri:
            failure_ready.set()
            raise OSError("lookahead source unavailable")
        return original_prepare(path, **kwargs)

    monkeypatch.setattr(lookahead_module, "prepare_public_input", failing_prepare)

    def after_partition(index: int, *_args: Any) -> None:
        """Record the current commit before the next failure is published."""
        if index == 1:
            assert failure_ready.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
            assert not Path(plans[1].output_uri).exists()
        callbacks.append(index)

    with pytest.raises(OSError, match="lookahead source unavailable"):
        run_partitioned_to_parquet(
            plans,
            initial_schema_registry={},
            to_parquet_kwargs=_multi_kwargs(),
            after_partition=after_partition,
        )

    assert callbacks == [1]
    assert Path(plans[0].output_uri).is_file()
    assert not Path(plans[1].output_uri).exists()


def test_single_pipeline_prepares_partitions_on_the_caller(tmp_path: Path) -> None:
    """Strict single mode keeps partition preparation on the caller thread."""
    plans = [_jsonl_plan(tmp_path, ordinal) for ordinal in range(2)]
    result = run_partitioned_to_parquet(
        plans,
        initial_schema_registry={},
        to_parquet_kwargs={
            "input_format": "jsonl",
            "input_mode": "single_file",
            "multi_threading": False,
            "memory_limit_bytes": 64 << 20,
        },
    )

    assert len(result.completed_runs) == 2


def test_callable_partition_options_preserve_original_evaluation_order(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Dynamic option factories remain sequential and are never evaluated early."""
    from schema_sanitizer.pipeline import partition_execution as execution_module

    plans = [_jsonl_plan(tmp_path, ordinal) for ordinal in range(2)]
    calls: list[str] = []

    class ForbiddenLookahead:
        """Fail if a dynamic options pipeline attempts speculative preparation."""

        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            """Fail immediately if callable options enable speculative lookahead."""
            raise AssertionError("callable options unexpectedly enabled lookahead")

    monkeypatch.setattr(execution_module, "PartitionSourceLookahead", ForbiddenLookahead)

    def options(plan: PartitionRunPlan) -> dict[str, Any]:
        """Record the exact source-order evaluation of options."""
        calls.append(plan.source_uri)
        return _multi_kwargs()

    run_partitioned_to_parquet(
        plans,
        initial_schema_registry={},
        to_parquet_kwargs=options,
    )

    assert calls == [plan.source_uri for plan in plans]


def test_temporary_window_contention_is_retried_at_partition_ordinal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Speculative permit contention falls back without changing success semantics."""
    from schema_sanitizer.pipeline import partition_lookahead as lookahead_module

    plan = _jsonl_plan(tmp_path, 0)
    controller = PartitionSourceLookahead(_multi_kwargs())
    assert controller.enabled
    parent = OperationExecutionContext(threading_mode="multi", memory_limit_bytes=64 << 20)
    held = parent.temporary_storage.acquire(200 << 20, label="current partition")
    attempts = 0

    def prepare_after_capacity(path: str, **_kwargs: Any) -> PreparedPublicInput:
        """Model a source that can only stage after the current lease drains."""
        nonlocal attempts
        attempts += 1
        pool: TemporaryStoragePermitPool = _kwargs["operation_context"].temporary_storage
        lease = pool.try_acquire(100 << 20, label="next partition")
        if lease is None:
            snapshot = pool.snapshot()
            raise SchemaSanitizerResourceError(
                "temporary storage window exhausted",
                detail={
                    "stage": "temporary_storage",
                    "limit_name": "temporary_storage_bytes",
                    "limit_bytes": snapshot.limit_bytes,
                    "actual_bytes": snapshot.reserved_bytes + (100 << 20),
                },
            )
        lease.release()
        return PreparedPublicInput(path, "jsonl", "path")

    monkeypatch.setattr(lookahead_module, "prepare_public_input", prepare_after_capacity)
    try:
        deferred = controller._prepare(plan, parent.fork())
        assert type(deferred).__name__ == "_DeferredPartition"
        held.release()
        prepared = controller._materialize_deferred(deferred)
        prepared.close()
    finally:
        held.release()
        controller.close()
        parent.close()

    assert attempts == 2


def test_prefetched_remote_prefix_resumes_after_retained_file_count(monkeypatch) -> None:
    """A retained remote packet is consumed once and resume starts after it."""
    retained = object()
    later = object()
    starts: list[int] = []

    class Context:
        """Return one later staged packet for the remaining manifest."""

        def __enter__(self):
            """Return the managed context value from context entry."""
            return iter([later])

        def __exit__(self, *_exc: object) -> None:
            """Finalize the context context without suppressing exceptions."""
            return None

    def open_chunks(_manifest: object, *, start: int = 0) -> Context:
        """Record the resume ordinal and return the remaining packet."""
        starts.append(start)
        return Context()

    monkeypatch.setattr(remote_source_plan, "open_staged_remote_chunks", open_chunks)
    provider = remote_source_plan.RemotePathSourceChunkProvider(
        retained_chunks=[retained],
        remaining_manifest=SimpleNamespace(),
        remaining_start=3,
    )
    try:
        assert provider._next_staged_chunk() is retained
        assert provider._next_staged_chunk() is later
    finally:
        provider.close()

    assert starts == [3]


def test_one_slot_window_never_prepares_partition_n_plus_two_early(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The dedicated worker never advances beyond exactly one partition."""
    from schema_sanitizer.pipeline import partition_lookahead as lookahead_module

    plans = [_jsonl_plan(tmp_path, ordinal) for ordinal in range(3)]
    original_prepare = lookahead_module.prepare_public_input
    second_started = threading.Event()
    release_second = threading.Event()
    third_started = threading.Event()

    def ordered_prepare(path: str, **kwargs: Any) -> PreparedPublicInput:
        """Hold N+1 and prove N+2 is not submitted into the one-slot window."""
        if path == plans[1].source_uri:
            second_started.set()
            assert release_second.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
        elif path == plans[2].source_uri:
            third_started.set()
        return original_prepare(path, **kwargs)

    monkeypatch.setattr(lookahead_module, "prepare_public_input", ordered_prepare)

    def after_partition(index: int, *_args: Any) -> None:
        """Release each next source only after proving the one-slot bound."""
        if index == 1:
            assert second_started.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
            assert not third_started.is_set()
            release_second.set()
        elif index == 2:
            assert third_started.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)

    result = run_partitioned_to_parquet(
        plans,
        initial_schema_registry={},
        to_parquet_kwargs=_multi_kwargs(),
        after_partition=after_partition,
    )

    assert len(result.completed_runs) == 3


def test_fixed_partition_timestamps_are_identical_across_modes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Lookahead scheduling cannot change per-partition generated UTC metadata."""
    import json

    pq = pytest.importorskip("pyarrow.parquet")

    from schema_sanitizer.api_impl import operation_context as context_module

    plans = [_jsonl_plan(tmp_path, ordinal) for ordinal in range(2)]

    def run(mode: str) -> tuple[list[list[dict[str, Any]]], list[str | None]]:
        """Run one mode with the same explicit timestamp sequence."""
        values = iter(
            [
                context_module.OperationTimestamps(
                    1_700_000_000_000_000,
                    "2023-11-14T22:13:20.000000Z",
                ),
                context_module.OperationTimestamps(
                    1_700_000_000_000_001,
                    "2023-11-14T22:13:20.000001Z",
                ),
            ]
        )
        lock = threading.Lock()

        def capture() -> Any:
            """Return the next deterministic per-partition timestamp."""
            with lock:
                return next(values)

        monkeypatch.setattr(context_module, "capture_operation_timestamps", capture)
        mode_plans = [
            PartitionRunPlan(
                plan.logical_date,
                plan.source_uri,
                str(tmp_path / f"{mode}-{index}.parquet"),
            )
            for index, plan in enumerate(plans)
        ]
        result = run_partitioned_to_parquet(
            mode_plans,
            initial_schema_registry={},
            to_parquet_kwargs={
                **_multi_kwargs(),
                "multi_threading": mode == "multi",
            },
        )
        rows = [pq.read_table(plan.output_uri).to_pylist() for plan in mode_plans]
        drifts = [run.schema_drifts_json for run in result.completed_runs]
        return rows, [json.dumps(json.loads(value or "[]"), sort_keys=True) for value in drifts]

    assert run("single") == run("multi")


def test_worker_creation_failure_falls_back_to_ordered_preparation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """An unavailable optional helper thread cannot make a valid pipeline fail."""
    from schema_sanitizer.pipeline import partition_lookahead as lookahead_module

    plans = [_jsonl_plan(tmp_path, ordinal) for ordinal in range(2)]

    monkeypatch.setattr(
        lookahead_module._DaemonThreadPoolExecutor,
        "submit",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("worker creation unavailable")
        ),
    )
    result = run_partitioned_to_parquet(
        plans,
        initial_schema_registry={},
        to_parquet_kwargs=_multi_kwargs(),
    )

    assert len(result.completed_runs) == 2
    assert all(Path(plan.output_uri).is_file() for plan in plans)


def test_live_static_options_discard_stale_speculative_input(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A live mapping mutation invalidates and re-prepares the retained source."""
    from schema_sanitizer.pipeline import partition_lookahead as lookahead_module

    plans = [_jsonl_plan(tmp_path, ordinal) for ordinal in range(2)]
    kwargs = _multi_kwargs()
    seen_encodings: list[str] = []
    original_prepare = lookahead_module.prepare_public_input

    def capture_prepare(path: str, **options: Any) -> PreparedPublicInput:
        """Record the encoding snapshot used for each physical preparation."""
        seen_encodings.append(options["input_text_encoding"])
        return original_prepare(path, **options)

    monkeypatch.setattr(lookahead_module, "prepare_public_input", capture_prepare)
    controller = PartitionSourceLookahead(kwargs)
    first = controller.prepare_first(plans[0])
    try:
        controller.arm(plans[1], first.operation_context)
        controller.trigger()
        kwargs["input_text_encoding"] = "latin-1"
        second = controller.take_next(plans[1])
        second.close()
    finally:
        first.close()
        controller.close()

    assert seen_encodings == ["utf-8", "utf-8", "latin-1"]


def test_manifest_carrier_closes_retained_remote_lookahead() -> None:
    """Abandoned prepared inputs release an attached prefetched manifest."""
    from schema_sanitizer.input_impl.prepared import NativeDirectoryManifestCarrier

    closed: list[str] = []

    class Manifest:
        """Track release of retained remote lookahead state."""

        def close(self) -> None:
            """Close the manifest and release its retained resources."""
            closed.append("remote")

    carrier = NativeDirectoryManifestCarrier()
    carrier.remote_native_multisource_manifest = Manifest()
    carrier.close()
    carrier.close()

    assert closed == ["remote"]
    assert not hasattr(carrier, "remote_native_multisource_manifest")
