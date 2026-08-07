"""Regression coverage for v102 typed-owner external task leases."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import concurrency_pair_guarantees
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
LEASE = ROOT / "cpp/src/internal/runtime/external_task_lease.hh"
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
STAGE = "typed_owner_member_abandonment_lease"


def test_v102_lease_has_typed_owner_and_member_policy():
    """Verify the named concurrency regression contract."""
    s = LEASE.read_text()
    assert "template <class Owner, void (Owner::*Abandon)(std::size_t) noexcept>" in s
    assert "Owner *owner_" in s
    assert "(owner_->*Abandon)(shard_);" in s
    assert "void *owner_" not in s


def test_v102_all_56_pairs_inherit_stage():
    """Verify the named concurrency regression contract."""
    pairs = concurrency_pair_guarantees()
    assert len(pairs) == 8 and sum(map(len, pairs.values())) == 56 and "python" in pairs
    for outputs in pairs.values():
        assert len(outputs) == 7
        for g in outputs.values():
            assert STAGE in g["shared_parallel_stages"]


def test_v102_native_order_cancel_drain():
    """Verify the named concurrency regression contract."""
    require_native()
    for w in (2, 4, 5, 8, 16):
        e, c, _, started, peak, q, submitted = native_core.ordered_executor_arena_completion_probe(
            w, 4000, 0
        )
        assert (
            e > 0
            and c == 4000
            and 1 <= started <= w
            and 1 <= peak <= w
            and q == 0
            and submitted == 4000
        )
    _, active, observed, q = native_core.operation_task_arena_cancellation_probe()
    assert active == 0 and observed >= 1 and q == 0
