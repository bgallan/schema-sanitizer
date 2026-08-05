"""Contracts for deterministic single and bounded multi execution modes."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.core_impl.execution_policy import (
    execution_policy,
    normalize_threading_mode,
    threading_mode_from_multi_threading,
)


def _logical_jsonl_rows(path: Path) -> list[dict[str, object]]:
    """Return output rows without operation-generated timestamp metadata."""
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    for row in rows:
        row.pop("ingestion_timestamp", None)
        if row.get("schema_drifts") is not None:
            drifts = json.loads(str(row["schema_drifts"]))
            for drift in drifts:
                drift.pop("detected_at", None)
            row["schema_drifts"] = drifts
    return rows


def test_single_policy_forces_every_project_owned_parallel_limit_to_one() -> None:
    """Single mode is the inline oracle regardless of host capacity or budget."""
    require_native()
    policy = execution_policy("single", 512 * 1024 * 1024, available_cpus=128)

    assert policy.requested_mode == "single"
    assert policy.available_cpus == 128
    assert policy.effective_workers == 1
    assert policy.task_queue_capacity == 1
    assert policy.reorder_capacity == 1
    assert policy.worker_arena_bytes > 0
    assert policy.materialization_packet_target_bytes == 1
    assert policy.materialization_packet_max_rows == 1
    assert policy.async_concurrency == 1
    assert policy.async_prefetch_files == 1
    assert policy.remote_chunk_prefetch == 1
    assert policy.source_discovery_concurrency == 1
    assert policy.temporary_storage_limit_bytes == 2 * 1024 * 1024 * 1024
    assert policy.pyarrow_use_threads is False
    assert policy.fallback_to_one_worker_reason == "single_requested"


def test_multi_policy_is_bounded_and_can_fall_back_to_one_worker() -> None:
    """Multi derives safe limits rather than exposing a worker-count knob."""
    require_native()
    parallel = execution_policy("multi", 256 * 1024 * 1024, available_cpus=8)
    constrained = execution_policy("multi", 1, available_cpus=8)

    assert 1 < parallel.effective_workers <= 8
    assert parallel.task_queue_capacity <= 64
    assert parallel.reorder_capacity == parallel.task_queue_capacity
    assert parallel.worker_arena_bytes > 0
    assert 1 <= parallel.materialization_packet_target_bytes <= 1024 * 1024
    assert parallel.materialization_packet_max_rows == 5120
    assert (
        parallel.materialization_packet_target_bytes * parallel.reorder_capacity
        <= 256 * 1024 * 1024 // 8
    )
    assert parallel.pyarrow_use_threads is True
    assert parallel.temporary_storage_limit_bytes == 1024 * 1024 * 1024
    assert parallel.fallback_to_one_worker_reason is None

    assert constrained.effective_workers == 1
    assert constrained.task_queue_capacity == constrained.reorder_capacity == 1
    assert constrained.materialization_packet_target_bytes == 1
    assert constrained.materialization_packet_max_rows == 1
    assert constrained.async_concurrency == constrained.async_prefetch_files == 1
    assert constrained.remote_chunk_prefetch == constrained.source_discovery_concurrency == 1
    assert constrained.pyarrow_use_threads is False
    assert constrained.fallback_to_one_worker_reason == "memory_limited"


def test_multi_policy_has_no_32_worker_ceiling() -> None:
    """CPU capacity above 32 remains usable when memory permits it."""
    policy_64 = execution_policy(
        "multi",
        1024 * 1024 * 1024,
        available_cpus=64,
    )
    policy_128 = execution_policy(
        "multi",
        2 * 1024 * 1024 * 1024,
        available_cpus=128,
    )

    assert policy_64.effective_workers == 64
    assert policy_64.task_queue_capacity >= 128
    assert policy_128.effective_workers == 128
    assert policy_128.task_queue_capacity >= 256
    assert policy_128.async_concurrency == 128
    assert policy_128.remote_chunk_prefetch == 64
    assert policy_128.source_discovery_concurrency == 256

    memory_limited = execution_policy(
        "multi",
        256 * 1024 * 1024,
        available_cpus=128,
    )
    assert memory_limited.effective_workers == 21
    assert memory_limited.worker_arena_bytes >= 8 * 1024 * 1024


def test_internal_threading_mode_normalization_is_strict() -> None:
    """Only the two canonical internal values are accepted."""
    assert normalize_threading_mode(" single ") == "single"
    assert normalize_threading_mode("MULTI") == "multi"
    with pytest.raises(ValueError, match="threading_mode"):
        normalize_threading_mode("auto")
    with pytest.raises(ValueError, match="threading_mode"):
        normalize_threading_mode(True)


def test_public_multi_threading_translation_is_strict() -> None:
    """The public concurrency switch accepts actual booleans only."""
    assert threading_mode_from_multi_threading(False) == "single"
    assert threading_mode_from_multi_threading(True) == "multi"
    for value in ("multi", 1, None):
        with pytest.raises(TypeError, match="multi_threading"):
            threading_mode_from_multi_threading(value)


def test_public_entry_points_default_to_single() -> None:
    """The first certified release keeps single as its public default."""
    functions = (
        ss.to_pyarrow,
        ss.to_pandas,
        ss.to_polars,
        ss.to_duckdb,
        ss.to_jsonl,
        ss.to_csv,
        ss.to_parquet,
    )
    for function in functions:
        parameters = inspect.signature(function).parameters
        assert "threading_mode" not in parameters
        assert parameters["multi_threading"].default is False
        assert parameters["multi_threading"].annotation in {bool, "bool"}


def test_public_entry_point_rejects_non_boolean_multi_threading(tmp_path: Path) -> None:
    """Public converters validate the switch before creating an output."""
    output = tmp_path / "invalid.jsonl"
    with pytest.raises(TypeError, match="multi_threading"):
        ss.to_jsonl(
            [],
            output,
            input_format="python",
            multi_threading="multi",  # type: ignore[arg-type]
        )
    assert not output.exists()


def test_run_sync_single_refuses_to_create_async_helper_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An active event loop cannot silently force a helper host thread in single mode."""
    from schema_sanitizer.remote_impl import async_bridge, transport

    class ForbiddenThread:
        """Fail if the extracted async bridge constructs a host thread."""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            """Reject construction of the forbidden helper thread."""
            raise AssertionError("single mode constructed async helper thread")

    monkeypatch.setattr(async_bridge, "Thread", ForbiddenThread)

    async def run() -> None:
        """Exercise the synchronous bridge from an active event loop."""

        async def value() -> int:
            """Return a trivial coroutine value."""
            return 1

        with pytest.raises(RuntimeError, match="helper host thread"):
            transport.run_sync(value(), threading_mode="single")

    asyncio.run(run())


def test_remote_prefetch_single_stages_inline_without_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The remote source-plan prefetcher owns no pool in single mode."""
    from schema_sanitizer.api_impl.source_plan import remote

    class ForbiddenCoordinator:
        """Fail if the multi-only remote coordinator is constructed."""

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            """Reject construction of the forbidden coordinator."""
            raise AssertionError("single mode constructed RemoteIoCoordinator")

    staged = SimpleNamespace(close=lambda: None)

    class Manifest:
        """Provide one inline-stage manifest for the prefetch test."""

        files = ("a",)
        chunk_size = 1
        memory_limit_bytes = 512 * 1024 * 1024
        threading_mode = "single"

        def stage_chunk(self, start: int) -> object:
            """Return the one staged test chunk."""
            assert start == 0
            return staged

    monkeypatch.setattr(remote, "RemoteIoCoordinator", ForbiddenCoordinator)
    iterator = remote.RemoteChunkPrefetchIterator(Manifest())
    assert next(iterator) is staged
    iterator.close()
    assert iterator._coordinator is None


def test_single_and_multi_produce_same_logical_jsonl_output(tmp_path: Path) -> None:
    """The initial executors preserve ordered logical output and registry state."""
    require_native()
    source = tmp_path / "input.jsonl"
    source.write_text(
        '{"id":1,"nested":{"name":"a"}}\n{"id":2,"nested":{"name":"b"}}\n',
        encoding="utf-8",
    )
    outputs: dict[str, Path] = {}
    results = {}
    for mode in ("single", "multi"):
        output = tmp_path / f"{mode}.jsonl"
        outputs[mode] = output
        results[mode] = ss.to_jsonl(
            source,
            output,
            input_format="jsonl",
            multi_threading=mode == "multi",
            memory_limit_bytes=256 * 1024 * 1024,
        )

    assert _logical_jsonl_rows(outputs["single"]) == _logical_jsonl_rows(outputs["multi"])
    assert results["single"].schema_registry_json == results["multi"].schema_registry_json
    assert results["single"].execution_policy["requested_mode"] == "single"
    assert results["multi"].execution_policy["requested_mode"] == "multi"


def test_single_local_conversion_does_not_add_host_threads_or_processes(
    tmp_path: Path,
) -> None:
    """A local single run preserves its thread baseline and creates no child process."""
    require_native()
    proc_root = Path("/proc")
    if not sys.platform.startswith("linux") or not proc_root.is_dir():
        pytest.skip("host thread/process accounting requires Linux /proc")

    source = tmp_path / "host-thread-input.jsonl"
    source.write_text(
        "".join(f'{{"id":{index},"value":"x"}}\n' for index in range(50_000)),
        encoding="utf-8",
    )
    ready = tmp_path / "child-ready"
    go = tmp_path / "child-go"
    script = r"""
from pathlib import Path
import sys
import time

source = Path(sys.argv[1])
root = Path(sys.argv[2])
ready = root / "child-ready"
go = root / "child-go"
ready.write_text("ready", encoding="utf-8")
while not go.exists():
    time.sleep(0.001)
sys.path.insert(0, str(Path.cwd() / "src"))
import schema_sanitizer as ss
for index in range(2):
    ss.to_jsonl(
        source,
        root / f"single-{index}.jsonl",
        input_format="jsonl",
        multi_threading=False,
        memory_limit_bytes=64 * 1024 * 1024,
    )
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(source), str(tmp_path)],
        cwd=Path.cwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    child_proc = proc_root / str(process.pid)
    deadline = time.monotonic() + 10.0
    while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.001)
    if not ready.exists():
        process.terminate()
        stdout, stderr = process.communicate(timeout=5)
        pytest.fail(f"child did not initialize: stdout={stdout!r} stderr={stderr!r}")

    baseline_threads = len(tuple((child_proc / "task").iterdir()))
    maximum_threads = baseline_threads
    child_process_ids: set[str] = set()
    go.write_text("go", encoding="utf-8")
    while process.poll() is None:
        try:
            maximum_threads = max(
                maximum_threads,
                len(tuple((child_proc / "task").iterdir())),
            )
            children = (child_proc / "task" / str(process.pid) / "children").read_text(
                encoding="utf-8"
            )
            child_process_ids.update(children.split())
        except FileNotFoundError:
            pass
        time.sleep(0.0005)
    stdout, stderr = process.communicate()

    assert process.returncode == 0, f"stdout={stdout!r} stderr={stderr!r}"
    assert maximum_threads == baseline_threads
    assert child_process_ids == set()


def test_detected_cpu_capacity_respects_process_affinity() -> None:
    """Automatic worker sizing must honor Linux CPU affinity without configuration."""
    if not hasattr(os, "sched_getaffinity"):
        pytest.skip("process CPU affinity is unavailable")
    script = """
import json
import os
available = sorted(os.sched_getaffinity(0))
os.sched_setaffinity(0, {available[0]})
from schema_sanitizer.core_impl.execution_policy import execution_policy
policy = execution_policy('multi', 512 * 1024 * 1024)
print(json.dumps({'available': policy.available_cpus, 'workers': policy.effective_workers}))
"""
    source_root = str(Path(__file__).resolve().parents[1] / "src")
    env = {"PYTHONPATH": source_root}
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    payload = json.loads(completed.stdout)
    assert payload == {"available": 1, "workers": 1}


def test_low_memory_multi_request_matches_single_output(tmp_path: Path) -> None:
    """A constrained multi request must fall back safely without changing data."""
    import schema_sanitizer as ss

    source = tmp_path / "low-memory.jsonl"
    source.write_text(
        "\n".join(
            json.dumps({"id": index, "payload": {"value": f"x{index}"}}) for index in range(500)
        )
        + "\n",
        encoding="utf-8",
    )
    single = tmp_path / "single.jsonl"
    multi = tmp_path / "multi.jsonl"
    ss.to_jsonl(source, single, input_format="jsonl", memory_limit_bytes=1 << 20)
    ss.to_jsonl(
        source,
        multi,
        input_format="jsonl",
        memory_limit_bytes=1 << 20,
        multi_threading=True,
    )
    assert _logical_jsonl_rows(multi) == _logical_jsonl_rows(single)
