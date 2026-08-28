"""Protect structural, atomic, and cancellation contracts of the shared operation arena.

Bounded high-core batching, snapshot ownership, lane cursors, admission rollback, publication
orders, completion-ring wrapping, reserved slots, aggregate layouts, external leases, stop paths,
and native cancellation are checked against the exact low- and high-core strategies they require.
"""

from __future__ import annotations

import re

from _support.source_contracts import SourceContract, assert_source_contract, source_text

from benchmarks.concurrency.assets import load_evidence
from schema_sanitizer.core_impl.native_runtime import native_core

EXECUTOR = "cpp/src/internal/runtime/ordered_executor.hh"
SUBMISSION = "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"
COMPLETION = "cpp/src/internal/runtime/ordered_executor_arena_completion.cc.inc"
RUNTIME = "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
TELEMETRY = "cpp/src/internal/runtime/performance_telemetry.cc"
TASK_TELEMETRY = "cpp/src/internal/runtime/operation_task_telemetry.cc.inc"
RING = "cpp/src/internal/runtime/ordered_executor_completion_ring.hh"


def test_high_core_batching_remains_bounded() -> None:
    """High-core task telemetry retains its bounded 32-task publication batch."""
    for contract in (
        SourceContract(
            "task-telemetry-include",
            RUNTIME,
            contains=('#include "internal/runtime/operation_task_telemetry.cc.inc"',),
        ),
        SourceContract(
            "bounded-task-batch",
            TASK_TELEMETRY,
            contains=(
                "class TaskTelemetryBatch final",
                "worker_count > 8U ? 32U : 8U",
                "telemetry_->RecordWorkerTaskBatch(",
            ),
        ),
        SourceContract(
            "batched-task-counters",
            TELEMETRY,
            contains=(
                "RecordTaskBatch(",
                "task_started_[index].fetch_add(task_count",
                "task_finished_[index].fetch_add(task_count",
            ),
        ),
    ):
        assert_source_contract(contract)


def test_idle_selection_uses_initialized_snapshot_without_started_reload() -> None:
    """Idle selection consumes the initialized snapshot without a second load."""
    source = source_text(RUNTIME)
    helper = source[source.index("idle_started_worker(") : source.index("void mark_nonempty")]
    assert "std::uint64_t initialized_snapshot" in helper
    assert "initialized_snapshot & allowed" in helper
    assert "started_mask.load" not in helper
    assert "initialized implies started" in helper


def test_initialized_snapshot_evidence_covers_boundaries() -> None:
    """Retained evidence covers low, threshold, and high worker counts."""
    evidence = load_evidence("initialized-worker-admission-snapshot")
    assert {"2", "4", "5", "8", "16"} <= set(evidence)


def test_internal_in_flight_reads_are_mutex_owned_and_relaxed() -> None:
    """Internal in-flight decisions use relaxed snapshots only under the mutex."""
    header = source_text(EXECUTOR)
    submission = source_text(SUBMISSION)
    helper = header.split("std::size_t in_flight_locked() const noexcept", 1)[1]
    helper = helper.split("}\n", 1)[0]
    assert "in_flight_.load(std::memory_order_relaxed)" in helper
    assert len(re.findall(r"(?<![A-Za-z0-9_])in_flight_locked\(\)", header)) == 4
    assert len(re.findall(r"(?<![A-Za-z0-9_])in_flight_locked\(\)", submission)) == 1
    assert "return in_flight_.load(std::memory_order_acquire);" in header


def test_shared_lane_cursor_is_touched_once_per_executor() -> None:
    """The shared lane cursor advances during reservation, not per packet."""
    arena = source_text("cpp/src/internal/runtime/operation_task_arena.cc")
    reserve_start = arena.index("OperationTaskArena::ReserveSubmissionTicket")
    reserve_end = arena.index("sanitize::Status OperationTaskArena::Submit", reserve_start)
    reserve = arena[reserve_start:reserve_end]
    ticket_submit = arena[arena.index("std::size_t submission_ticket") :]
    assert reserve.count("cursor->fetch_add") == 1
    assert "cursor->fetch_add" not in ticket_submit

    executor = source_text(EXECUTOR)
    constructor = executor[executor.index("OrderedExecutor(std::size_t") :]
    assert "worker_count_ > 8U" in constructor
    assert "arena_->ReserveSubmissionTicket(arena_submission_plan_)" in constructor
    assert executor.count("ReserveSubmissionTicket(arena_submission_plan_)") == 1


def test_ticket_skip_on_failed_admission_needs_no_shared_rollback() -> None:
    """Failed admission rewinds ordinals without rewinding the shared ticket."""
    for source in (source_text(EXECUTOR), source_text(SUBMISSION)):
        failure = source[source.index("if (!submit_status.ok())") :]
        assert "--next_high_core_arena_ticket_" not in failure
        assert "--next_submit_ordinal_" in failure
        assert "completion_ring_.RollbackSubmit();" in failure


def test_arena_wait_has_no_preliminary_executor_mutex() -> None:
    """The slot wait occurs before the one authoritative executor lock."""
    source = source_text(COMPLETION)
    start = source.index("sanitize::Result<Outcome> take_next_arena()")
    take = source[start : source.index("void close_empty_arena_slot", start)]
    wait = take.index("WaitOnAtomic(slot.state")
    first_executor_lock = take.index("std::lock_guard lock(mutex_)")
    assert "const auto expected_ordinal = next_take_ordinal_;" in take[:wait]
    assert "std::lock_guard lock(mutex_)" not in take[:wait]
    assert first_executor_lock > wait
    assert take.count("std::lock_guard lock(mutex_)") == 1


def test_local_pool_keeps_ring_outcome_validation() -> None:
    """Local and inline fallback retain their ordinal validation."""
    assert_source_contract(
        SourceContract(
            "local-ring-outcome-validation",
            EXECUTOR,
            contains=(
                "std::vector<std::optional<Outcome>> completed_;",
                "slot->ordinal == next_take_ordinal_",
                "store_outcome_locked",
            ),
        )
    )


def test_high_core_submission_uses_single_writer_publication() -> None:
    """The high-core increment is a mutex-owned load/store publication."""
    header = source_text(EXECUTOR)
    submission = source_text(SUBMISSION)
    helper = header.split("void increment_high_core_in_flight_locked", 1)[1].split("}\n", 1)[0]
    assert "in_flight_.load(std::memory_order_relaxed)" in helper
    assert "in_flight_.store(current + 1U, std::memory_order_release)" in helper
    assert "increment_high_core_in_flight_locked();" in submission
    assert "in_flight_.fetch_add" not in submission


def test_low_core_and_consumption_paths_retain_atomic_rmw() -> None:
    """Inline, regular arena, and local-pool paths retain atomic RMWs."""
    header = source_text(EXECUTOR)
    completion = source_text(COMPLETION)
    assert header.count("in_flight_.fetch_add(1, std::memory_order_release);") == 3
    assert "in_flight_.fetch_sub(1, std::memory_order_release);" in header
    assert "in_flight_.fetch_sub(1, std::memory_order_release);" in completion
    assert "decrement_arena_in_flight_locked" not in header + completion


def test_high_core_decrement_is_single_writer_publication() -> None:
    """The high-core decrement is a mutex-owned load/store publication."""
    header = source_text(EXECUTOR)
    helper = header.split("void decrement_high_core_in_flight_locked() noexcept", 1)[1]
    helper = helper.split("}\n", 1)[0]
    assert "in_flight_.load(std::memory_order_relaxed)" in helper
    assert "in_flight_.store(current - 1U, std::memory_order_release)" in helper
    assert "fetch_sub" not in helper


def test_only_high_core_arena_consumption_uses_decrement_helper() -> None:
    """Successful low-core consumption keeps the atomic RMW path."""
    header = source_text(EXECUTOR)
    completion = source_text(COMPLETION)
    dispatch = header.split("sanitize::Result<Outcome> TakeNext()", 1)[1]
    dispatch = dispatch.split("std::unique_lock lock(mutex_);", 1)[0]
    assert "if (worker_count_ > 8U)" in dispatch
    assert "take_next_arena<true>()" in dispatch
    assert "take_next_arena<false>()" in dispatch
    assert "template <bool HighCore>" in completion
    assert "if constexpr (HighCore)" in completion
    assert "decrement_high_core_in_flight_locked();" in completion
    assert "in_flight_.fetch_sub(1, std::memory_order_release);" in completion


def test_high_core_submission_rollback_matches_publication_strategy() -> None:
    """Rejected high-core work rolls back under the same mutex."""
    rollback = source_text(SUBMISSION).split("if (!submit_status.ok())", 1)[1]
    assert "std::lock_guard lock(mutex_);" in rollback
    assert "completion_ring_.RollbackSubmit();" in rollback
    assert "decrement_high_core_in_flight_locked();" in rollback


def test_high_core_consumption_evidence_is_recorded() -> None:
    """The retained benchmark proves the scoped publication improvement."""
    evidence = load_evidence("high-core-inflight-consumption")
    assert evidence["pair_count"] == 15
    assert evidence["candidate_wins"] == 15
    assert evidence["paired_median_reduction_percent"] > 80.0


def test_completion_ring_uses_branch_wrap_without_runtime_modulo() -> None:
    """Submission and take cursors wrap without integer division."""
    assert_source_contract(
        SourceContract(
            "branch-wrapped-completion-ring",
            RING,
            contains=(
                "class CompletionRingCursor final",
                "ReserveSubmit",
                "RollbackSubmit",
                "NextTake",
                "AdvanceTake",
                "if (++next_submit_ == capacity_)",
                "if (++next_take_ == capacity_)",
            ),
            excludes=("%",),
        )
    )


def test_reserved_slot_crosses_all_executor_submission_paths() -> None:
    """Every executor path reserves once and can roll back its slot."""
    executor = source_text(EXECUTOR)
    submission = source_text(SUBMISSION)
    assert "ScheduledOrdinalPacket<Packet>" in executor
    assert executor.count("completion_ring_.ReserveSubmit()") == 3
    assert "completion_ring_.ReserveSubmit()" in submission
    assert "completion_ring_.RollbackSubmit()" in executor
    assert "completion_ring_.RollbackSubmit()" in submission
    assert "std::deque<ScheduledPacket> tasks_;" in executor


def test_packet_and_outcome_aggregate_layouts_remain_unchanged() -> None:
    """Completion metadata remains internal to the executor."""
    executor = source_text(EXECUTOR)
    packet = executor[executor.index("template <class Payload> struct OrdinalPacket") :]
    packet = packet[: packet.index("template <class Value>")]
    outcome = executor[executor.index("template <class Value> struct OrdinalOutcome") :]
    outcome = outcome[: outcome.index("template <class Input")]
    assert "completion_slot" not in packet
    assert "completion_slot" not in outcome
    assert "Payload payload;" in packet
    assert "sanitize::Result<Value> result;" in outcome


def test_external_lease_keeps_its_arena_alive_and_finishes_exactly_once() -> None:
    """The live lease owns the arena until completion transfers or releases it."""
    source = source_text(EXECUTOR)
    start = source.index("class ExternalLease final")
    lease = source[start : source.index("\n  };", start)]
    assert "std::shared_ptr<ArenaSharedState> owner_" in lease
    assert "owner_(std::move(other.owner_))" in lease
    assert "~ExternalLease() { Complete(); }" in lease
    assert "owner_->Finish(shard_);" in lease
    assert "owner_.reset();" in lease
    assert "ExternalLease &operator=(ExternalLease &&) = delete;" in lease
    assert "Abandon" not in lease


def test_worker_loop_compiles_distinct_low_and_parallel_stop_paths() -> None:
    """Low-core and parallel workers retain their distinct stop-check policies."""
    source = source_text(RUNTIME)
    loop = source[
        source.index(
            "template <bool PreferDedicatedOutput, bool CheckGlobalStopping>"
        ) : source.index("[[nodiscard]] bool worker_already_started_fast_path")
    ]
    startup = source[source.index("ensure_worker_started(") :]

    assert "if constexpr (CheckGlobalStopping)" in loop
    assert "state->stopping.load(std::memory_order_acquire)" in loop
    assert "worker_loop<false, true>" in startup
    assert "worker_loop<false, false>" in startup
    assert "worker_loop<true, false>" in startup
    assert startup.index("state->worker_count > 8U") < startup.index("state->worker_count >= 4U")


def test_four_plus_hot_path_has_no_dynamic_global_stop_reload() -> None:
    """The four-plus hot path reads global stop state only when specialized to."""
    source = source_text(RUNTIME)
    loop = source[
        source.index("while (!stop.stop_requested())") : source.index(
            "OperationTaskArena::State::QueuedTask queued"
        )
    ]
    assert "if constexpr (CheckGlobalStopping)" in loop
    assert loop.count("state->stopping.load") == 1
    assert "StopToken" in source
    assert "admission and park wakeup" in source


def test_stop_token_worker_loop_evidence_covers_threshold_matrix() -> None:
    """Retained evidence spans the four-worker threshold and high-core paths."""
    evidence = load_evidence("stop-token-worker-loop")
    assert {"4", "5", "8", "16"} <= set(evidence)


def test_native_cancellation_preserves_exact_terminal_state(require_native: None) -> None:
    """Cancellation leaves no active or queued arena work."""
    drained, active, observed_stop, queued = native_core.operation_task_arena_cancellation_probe()
    assert drained is True
    assert active == 0
    assert 1 <= observed_stop <= 4
    assert queued == 0
