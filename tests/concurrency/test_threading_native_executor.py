"""Native ordinal executor contracts for inline and bounded pool execution."""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pytest
from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_native_inline_executor_preserves_order_without_worker_threads() -> None:
    """The inline executor runs every packet on one calling host thread."""
    require_native()
    ordinals, values, thread_count, failure, inline, workers, status = (
        native_core.ordered_executor_probe(0, 8, 24, -1)
    )

    assert ordinals == tuple(range(24))
    assert values == tuple(index * 10 for index in range(24))
    assert thread_count == 1
    assert failure == -1
    assert inline is True
    assert workers == 1
    assert status == "OK"


def test_native_pool_hides_forced_out_of_order_completion() -> None:
    """Pool completion timing never changes coordinator-visible ordinal order."""
    require_native()
    ordinals, values, thread_count, failure, inline, workers, status = (
        native_core.ordered_executor_probe(1, 4, 48, -1)
    )

    assert ordinals == tuple(range(48))
    assert values == tuple(index * 10 for index in range(48))
    assert 2 <= thread_count <= 4
    assert failure == -1
    assert inline is False
    assert workers == 4
    assert status == "OK"


def test_native_pool_reports_earliest_source_order_failure() -> None:
    """A later fast failure cannot overtake the lowest failing ordinal."""
    require_native()
    ordinals, values, _threads, failure, inline, workers, status = (
        native_core.ordered_executor_probe(1, 4, 32, 3)
    )

    assert ordinals == (0, 1, 2)
    assert values == (0, 10, 20)
    assert failure == 3
    assert inline is False
    assert workers == 4
    assert "probe failure at ordinal 3" in status


def test_native_executor_probe_validates_limits() -> None:
    """The internal probe rejects invalid modes, worker counts, and ordinals."""
    require_native()
    with pytest.raises(ValueError, match="mode"):
        native_core.ordered_executor_probe(2, 1, 1, -1)
    with pytest.raises(ValueError, match="workers"):
        native_core.ordered_executor_probe(1, 0, 1, -1)
    with pytest.raises(ValueError, match="task_count"):
        native_core.ordered_executor_probe(1, 1, -1, -1)
    with pytest.raises(ValueError, match="fail_ordinal"):
        native_core.ordered_executor_probe(1, 1, 1, 1)


def test_native_inline_probe_does_not_add_host_threads(tmp_path: Path) -> None:
    """The native inline primitive itself creates no temporary host thread."""
    require_native()
    proc_root = Path("/proc")
    if not sys.platform.startswith("linux") or not proc_root.is_dir():
        pytest.skip("host thread accounting requires Linux /proc")

    ready = tmp_path / "ready"
    go = tmp_path / "go"
    script = r"""
from pathlib import Path
import sys
import time

root = Path(sys.argv[1])
ready = root / "ready"
go = root / "go"
sys.path.insert(0, str(Path.cwd() / "src"))
from schema_sanitizer.core_impl.native_runtime import native_core
ready.write_text("ready", encoding="utf-8")
while not go.exists():
    time.sleep(0.001)
result = native_core.ordered_executor_probe(0, 32, 800, -1)
assert result[2] == 1
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(tmp_path)],
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
    go.write_text("go", encoding="utf-8")
    while process.poll() is None:
        try:
            maximum_threads = max(
                maximum_threads,
                len(tuple((child_proc / "task").iterdir())),
            )
        except FileNotFoundError:
            pass
        time.sleep(0.0005)
    stdout, stderr = process.communicate()

    assert process.returncode == 0, f"stdout={stdout!r} stderr={stderr!r}"
    assert maximum_threads == baseline_threads


def test_operation_task_arena_reuses_exact_worker_budget_across_stages() -> None:
    """Complementary stages share N physical workers without oversubscription."""
    require_native()
    workers, peak, total_threads, overlap, upstream, output, submitted = (
        native_core.operation_task_arena_probe(8, 4, 4, 32)
    )

    assert workers == 8
    assert peak == 8
    assert total_threads == 8
    assert overlap == 0
    assert upstream == 4
    assert output == 4
    assert submitted == 64


def test_operation_task_arena_executes_beyond_32_workers() -> None:
    """A 64-worker arena retains its lanes while sharing process CPU capacity."""
    require_native()
    workers, peak, total_threads, overlap, upstream, output, submitted = (
        native_core.operation_task_arena_probe(64, 32, 32, 128)
    )

    assert workers == 64
    assert 1 <= peak <= workers
    assert total_threads == 64
    assert overlap == 0
    assert upstream == 32
    assert output == 32
    assert submitted == 256


def test_operation_task_arena_single_mode_is_strictly_inline() -> None:
    """An arena with one worker does not create a native helper thread."""
    require_native()
    workers, peak, total_threads, overlap, upstream, output, submitted = (
        native_core.operation_task_arena_probe(1, 1, 1, 4)
    )

    assert workers == 1
    assert peak <= 1
    assert total_threads == 1
    assert overlap == 1
    assert upstream == 1
    assert output == 1
    assert submitted == 0


def test_operation_task_arena_starts_only_workers_used_by_stage_lanes() -> None:
    """N remains available while narrow stages avoid starting idle helpers."""
    require_native()
    workers, peak, total_threads, overlap, upstream, output, submitted = (
        native_core.operation_task_arena_probe(8, 2, 2, 16)
    )

    assert workers == 8
    assert peak == 4
    assert total_threads == 4
    assert overlap == 0
    assert upstream == 2
    assert output == 2
    assert submitted == 32


def test_operation_task_arena_steals_lane_compatible_backlog() -> None:
    """An idle compatible worker drains work queued behind a slow packet."""
    require_native()
    stolen, displaced_worker, completed, queued, peak = (
        native_core.operation_task_arena_stealing_probe()
    )

    assert stolen >= 1
    assert displaced_worker in {1, 2, 3}
    assert completed == 8
    assert queued == 0
    assert 2 <= peak <= 4


def test_operation_task_arena_cancels_active_stage_work_promptly() -> None:
    """Cancelling one ordered stage propagates to its active arena packets."""
    require_native()
    elapsed_us, active, observed_stop, queued = (
        native_core.operation_task_arena_cancellation_probe()
    )

    assert elapsed_us < 250_000
    assert active == 0
    assert observed_stop >= 1
    assert queued == 0
