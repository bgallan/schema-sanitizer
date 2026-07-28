"""Regression coverage for v89 external-task lifetime accounting."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import concurrency_pair_guarantees
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
LEASE = ROOT / "cpp/src/internal/runtime/external_task_lease.hh"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"
DOC = ROOT / "CONCURRENCY_SCALING_V89.md"


def test_v89_admission_total_remains_mutex_owned_after_v91_sharding() -> None:
    source = EXECUTOR.read_text(encoding="utf-8")
    helper = SUBMISSION.read_text(encoding="utf-8")
    combined = source + helper

    assert "scheduled_tasks_.fetch_add" not in combined
    assert combined.count("++scheduled_external_tasks_[completion_shard];") == 2
    assert "std::array<std::size_t, kMaxExternalCompletionShards>" in source


def test_v89_single_completion_rmw_is_superseded_by_v91_shards() -> None:
    source = EXECUTOR.read_text(encoding="utf-8")
    assert "completed_external_tasks_.fetch_add" not in source
    assert "completed_external_tasks_[shard].completed" in source
    assert "counter.fetch_add(1, std::memory_order_release)" in source
    assert "counter.notify_all()" in source
    assert "counter.wait(completed" in source or "counter.wait(waiting_value" in source
    assert "completed != scheduled[shard]" in source


def test_v89_submit_failure_remains_lease_accounted() -> None:
    source = EXECUTOR.read_text(encoding="utf-8")
    helper = SUBMISSION.read_text(encoding="utf-8")
    for text in (source, helper):
        assert "ExternalLease(" in text or "ExternalTaskLease(" in text
        assert "completion_shard" in text
        assert "lease.Complete();" in text
    lease = LEASE.read_text(encoding="utf-8")
    assert "&OrderedExecutor::abandon_external_task" in source
    assert "(owner_->*Abandon)(shard_);" in lease


def test_v89_all_56_pairs_inherit_single_rmw_lifetime_accounting() -> None:
    pairs = concurrency_pair_guarantees()
    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert (
                "single_rmw_external_task_lifetime_accounting"
                in guarantee["shared_parallel_stages"]
            )
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v89_documentation_records_safety_and_ab() -> None:
    text = DOC.read_text(encoding="utf-8")
    assert "two global RMW operations" in text
    assert "one global RMW operation" in text
    assert "56 supported input/output pairs" in text
    assert "shutdown" in text.lower()
    assert "100,000" in text
    assert "37.46%" in text
    assert "21.97%" in text


def test_v89_native_ordered_completion_still_drains_exactly() -> None:
    require_native()
    for workers in (2, 4):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 20_000, 0)
        )
        assert elapsed > 0
        assert completed == 20_000
        assert checksum >= 0
        assert started == workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 20_000
