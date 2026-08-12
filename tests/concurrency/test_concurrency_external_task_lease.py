"""Regression coverage for the external task lease lifecycle contract."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
LEASE = ROOT / "cpp/src/internal/runtime/external_task_lease.hh"
STATIC_EVIDENCE = ROOT / "benchmarks/evidence/concurrency/lifecycle/static-external-task-lease.json"
SENTINEL_EVIDENCE = ROOT / (
    "benchmarks/evidence/concurrency/lifecycle/external-task-lease-sentinel.json"
)


def test_abandonment_policy_is_compile_time_and_lease_is_two_words() -> None:
    """Verify the named concurrency regression contract."""
    source = LEASE.read_text(encoding="utf-8")
    assert (
        "template <void (*Abandon)(void *, std::size_t) noexcept>" in source
        or "template <class Owner, void (Owner::*Abandon)(std::size_t) noexcept>" in source
    )
    assert "static_assert(Abandon != nullptr)" in source
    assert "Abandon(owner_, shard_);" in source or "(owner_->*Abandon)(shard_);" in source
    assert "Abandon abandon_" not in source
    assert "void *owner_" in source or "Owner *owner_" in source
    assert "std::size_t shard_" in source
    assert "shard() const noexcept" in source
    assert "other.owner_ = nullptr" in source
    assert "void Complete() noexcept { owner_ = nullptr; }" in source


def test_documentation_and_benchmark_record_scope_and_limits() -> None:
    """Verify the named concurrency regression contract."""
    benchmark = STATIC_EVIDENCE.read_text(encoding="utf-8")
    assert '"pairs": 21' in benchmark
    assert '"iterations": 30000000' in benchmark
    assert '"baseline_lease_bytes": 24' in benchmark
    assert '"candidate_lease_bytes": 16' in benchmark


def test_owner_is_the_only_mutable_completion_sentinel() -> None:
    """Verify the named concurrency regression contract."""
    source = LEASE.read_text(encoding="utf-8")
    move_start = source.index("ExternalTaskLease(ExternalTaskLease &&other)")
    move = source[move_start : source.index("ExternalTaskLease &operator=", move_start)]
    complete = source[source.index("void Complete()") : source.index("private:")]
    assert "other.owner_ = nullptr" in move
    assert "other.Complete()" not in move
    assert "owner_ = nullptr" in complete
    assert "abandon_ = nullptr" not in complete
    assert "if (owner_)" in source or "if (owner_ && abandon_)" in source


def test_documentation_and_benchmark_cover_matrix_and_limits() -> None:
    """Verify the named concurrency regression contract."""
    benchmark = SENTINEL_EVIDENCE.read_text(encoding="utf-8")
    assert '"pairs": 21' in benchmark
    assert '"iterations": 30000000' in benchmark


def test_lease_has_typed_owner_and_member_policy():
    """Verify the named concurrency regression contract."""
    s = LEASE.read_text()
    assert "template <class Owner, void (Owner::*Abandon)(std::size_t) noexcept>" in s
    assert "Owner *owner_" in s
    assert "(owner_->*Abandon)(shard_);" in s
    assert "void *owner_" not in s


def test_native_order_cancel_drain():
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
