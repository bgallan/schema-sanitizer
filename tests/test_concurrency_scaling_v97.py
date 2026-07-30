"""Regression coverage for v97 shutdown-only completion notifications."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import concurrency_pair_guarantees
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
STAGE = "shutdown_waiter_bit_external_completion_notification_elision"


def test_v97_completion_counter_embeds_shutdown_waiter_bit() -> None:
    """Verify the named concurrency regression contract."""
    source = EXECUTOR.read_text(encoding="utf-8")
    assert "completed_and_waiter" in source
    assert "kExternalCompletionWaiterBit" in source
    assert "kExternalCompletionCountMask" in source
    assert "counter.fetch_or(kExternalCompletionWaiterBit" in source


def test_v97_normal_completion_notifies_only_an_active_drain_waiter() -> None:
    """Verify the named concurrency regression contract."""
    source = EXECUTOR.read_text(encoding="utf-8")
    finish = source[source.index("void finish_external_task") : source.index("void worker_loop")]
    assert "const auto previous = counter.fetch_add(1" in finish
    assert "previous & kExternalCompletionWaiterBit" in finish
    assert finish.count("counter.notify_all()") == 1
    assert finish.index("previous & kExternalCompletionWaiterBit") < finish.index(
        "counter.notify_all()"
    )


def test_v97_all_56_pairs_inherit_shutdown_notification_elision() -> None:
    """Verify the named concurrency regression contract."""
    pairs = concurrency_pair_guarantees()
    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert STAGE in guarantee["shared_parallel_stages"]
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v97_native_completion_and_shutdown_drain_remain_exact() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4, 5, 8, 16):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 20_000, 0)
        )
        assert elapsed > 0
        assert completed == 20_000
        assert checksum >= 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 20_000
