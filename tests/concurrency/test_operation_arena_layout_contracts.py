"""Verify cache-line layout, visibility sharding, and task selection in the operation arena.

Native probes exercise peak bookkeeping, physical domains, writer isolation, wake and running
publication, compact lane bounds, ordered bit selection, one-shot initialization, and only the
relevant visibility shards across repeated real park-and-drain cycles.
"""

from __future__ import annotations

from _support.source_contracts import source_text

from benchmarks.concurrency.assets import load_evidence, load_probe

ARENA = "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
SELECTION = "cpp/src/internal/runtime/operation_task_arena_selection.hh"


def _compact_probe(path: str) -> tuple[str, str]:
    """Run the compact native arena probe and return its counters."""
    source = load_probe(path)
    return source, source.replace(" ", "").replace("\n", "")


def test_cache_preserves_exact_peak_proof() -> None:
    """A worker-local cache cannot hide an unpublished active-task peak."""
    source = source_text(RUNTIME)
    assert "worker is the sole writer" in source
    assert "was already offered to update_peak()" in source
    assert "covered by the global" in source
    assert "std::memory_order_relaxed" in source


def test_peak_cache_probe_exercises_repeated_real_arena_streaks() -> None:
    """The standalone peak probe covers repeated park/wake waves."""
    source = load_probe("telemetry/worker-local-peak-cache-tsan.cc")
    assert "OperationTaskArena::Make(workers)" in source
    assert "for (const auto workers : {4U, 8U, 16U})" in source
    assert "arena->active_tasks() != workers" in source
    assert "arena->peak_active_tasks() != workers" in source
    assert "constexpr std::size_t kWaves = 128U" in source
    assert "arena->queued_tasks() == 0U" in source


def test_peak_cache_evidence_is_scoped_to_bookkeeping() -> None:
    """Peak-cache evidence stays scoped to bookkeeping."""
    evidence = load_evidence("worker-local-peak-cache")
    assert evidence["pair_count"] == 15
    assert "peak-active bookkeeping" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {4, 8, 16}
    for item in scenarios.values():
        assert item["candidate_wins"] >= 10
        assert item["paired_median_reduction_percent"] > 15.0


def test_high_core_snapshot_uses_fixed_physical_domains() -> None:
    """High-core stealing reuses fixed shard geometry."""
    block = source_text(RUNTIME).split("template <bool Sharded>", 1)[1]
    block = block.split("idle_started_worker(", 1)[0]
    assert "if constexpr (!Sharded)" in block
    for boundary in (8, 16, 24):
        assert f"state->worker_count > {boundary}U" in block
    for shard in range(3):
        assert f"queue_visibility[{shard}].nonempty_mask.load" in block
    assert "std::countr_zero" not in block
    assert "while (remaining" not in block
    assert "return snapshot & allowed;" in block


def test_low_core_path_remains_one_visibility_load() -> None:
    """Arenas with at most eight workers keep one visibility load."""
    block = source_text(RUNTIME).split("template <bool Sharded>", 1)[1]
    block = block.split("idle_started_worker(", 1)[0]
    low_core = block.split("if constexpr (!Sharded)", 1)[1].split("// High-core workers", 1)[0]
    assert low_core.count("primary_queue_visibility.nonempty_mask.load") == 1
    assert "queue_visibility[" not in low_core
    assert "allowed" in low_core


def test_fixed_visibility_probe_covers_all_shard_boundaries() -> None:
    """The real arena probe forces steals at every physical boundary."""
    source, compact = _compact_probe("scheduler/fixed-visibility-snapshot-tsan.cc")
    assert "{9U,16U,17U,24U,25U,32U}" in compact
    assert "TaskArenaLane::kAll" in source
    assert "kQuickTasks=4096U" in compact
    assert "plan,0U" in compact
    assert "arena->stolen_tasks()>0U" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_fixed_visibility_evidence_is_positive_and_scoped() -> None:
    """Visibility evidence covers every two-, three-, and four-shard boundary."""
    evidence = load_evidence("fixed-visibility-snapshot")
    assert evidence["pair_count"] == 15
    assert evidence["iterations_per_process"] == 20_000_000
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {9, 16, 17, 24, 25, 32}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 40.0


def test_independent_arena_writer_domains_are_cacheline_isolated() -> None:
    """Independent producer cursors and activity domains start aligned."""
    source = source_text(ARENA)
    state = source[source.index("struct OperationTaskArena::State") :]
    state = state[: state.index("OperationTaskArena::OperationTaskArena")]
    for field in ("upstream_cursor", "output_cursor", "all_cursor"):
        assert f"alignas(64) std::atomic<std::size_t> {field}" in state
    assert "alignas(64) std::atomic<bool> stopping" in state
    assert "alignas(64) std::atomic<std::size_t> active" in state
    assert state.index("upstream_cursor") < state.index("output_cursor")
    assert state.index("output_cursor") < state.index("all_cursor")
    assert state.index("all_cursor") < state.index("stopping")
    assert state.index("initialized_mask") < state.index(
        "alignas(64) std::atomic<std::size_t> active"
    )


def test_writer_domain_probe_stresses_all_cursors_and_exact_drain() -> None:
    """The TSan probe submits concurrently through every lane cursor."""
    source, compact = _compact_probe("layout/arena-writer-domain-cacheline-tsan.cc")
    assert "std::barrier" in source
    for lane in ("kUpstream", "kOutput", "kAll"):
        assert f"TaskArenaLane::{lane}" in source
    assert "{2U,4U,8U,16U,32U}" in compact
    assert "kProducerCount=3U" in compact
    assert "arena->submitted_tasks()==kTasks" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_writer_domain_evidence_is_positive_and_scoped() -> None:
    """Writer-domain evidence avoids end-to-end throughput claims."""
    evidence = load_evidence("arena-writer-domain-cacheline")
    assert evidence["pair_count"] == 15
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {item["scenario"]: item for item in evidence["scenarios"]}
    assert set(scenarios) == {
        "cursor_activity",
        "two_cursors_activity",
        "three_cursors_two_activity",
    }
    assert {item["writer_threads"] for item in scenarios.values()} == {2, 3, 5}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 45.0


def test_wake_epoch_and_queue_are_separately_aligned() -> None:
    """The queue control block follows the aligned wake-publication line."""
    source = source_text(ARENA)
    slot = source[source.index("struct WorkerSlot final") : source.index("explicit State")]
    wake = "alignas(64) std::atomic<std::uint64_t> wake_epoch{0};"
    queue = "alignas(64) std::pmr::deque<QueuedTask> tasks;"
    assert wake in slot
    assert queue in slot
    assert slot.index(wake) < slot.index(queue)
    assert "unused tail of the epoch line" in slot


def test_wake_layout_preserves_protocol_operations() -> None:
    """The layout change retains wake RMWs, loads, and notifications."""
    arena = source_text(ARENA)
    runtime = source_text(RUNTIME)
    assert arena.count("wake_epoch.fetch_add(1, std::memory_order_release)") >= 4
    assert "helper_slot.wake_epoch.fetch_add(1, std::memory_order_release)" in arena
    assert "slot.wake_epoch.load(std::memory_order_acquire)" in runtime
    assert "WaitWithStop(slot.ready, lock, stop" in runtime
    assert "slot.ready.notify_one()" in arena
    assert "slot->ready.notify_all()" in arena


def test_wake_layout_probe_repeats_real_park_wake_and_exact_drain() -> None:
    """The wake-layout probe repeatedly stresses queue ownership."""
    _source, compact = _compact_probe("layout/wake-epoch-cacheline-tsan.cc")
    assert "kWaves=96U" in compact
    assert "{2U,4U,8U,16U,32U}" in compact
    assert "arena->wake_epoch_publishes()>=workers" in compact
    assert "arena->submitted_tasks()==workers*kWaves" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_wake_layout_evidence_is_positive_and_scoped() -> None:
    """Wake-layout evidence makes no parsing-throughput claim."""
    evidence = load_evidence("wake-epoch-cacheline")
    assert evidence["pair_count"] == 15
    assert evidence["iterations_per_thread"] == 5_000_000
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {item["scenario"]: item for item in evidence["scenarios"]}
    assert set(scenarios) == {"wake_queue", "wake_queue_observer"}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 60.0


def test_queue_packet_uses_unbounded_lane_bounds() -> None:
    """Queued lane metadata remains lossless beyond 32 workers."""
    arena = source_text(ARENA)
    packet = arena.split("struct QueuedTask final", 1)[1].split("};", 1)[0]
    assert "std::size_t lane_begin = 0;" in packet
    assert "std::size_t lane_end = 1;" in packet
    assert "std::uint8_t lane_begin" not in packet
    assert "std::uint8_t lane_end" not in packet
    assert ".lane_begin = lane_begin" in arena
    assert ".lane_end = lane_end" in arena
    assert "scalable_scan(count > 32U)" in arena
    assert "worker count exceeds 32" not in arena


def test_workers_use_native_size_bounds_for_arithmetic() -> None:
    """Compatibility and relative worker arithmetic remain size-exact."""
    source = source_text(RUNTIME)
    assert "index >= queued.lane_begin && index < queued.lane_end" in source
    assert "index - static_cast<std::size_t>(queued.lane_begin)" in source
    assert "static_cast<std::size_t>(queued.lane_end)" not in source
    assert "dedicated_high_output" in source
    assert "compatible(" in source


def test_compact_packet_probe_covers_every_lane_and_boundary_width() -> None:
    """The packet probe covers local and stolen work for every lane kind."""
    source, compact = _compact_probe("layout/compact-queued-task-tsan.cc")
    assert "{2U,3U,5U,8U,16U,32U}" in compact
    for lane in ("kUpstream", "kOutputCompact", "kOutput", "kAll"):
        assert f"TaskArenaLane::{lane}" in source
    assert "relative>=widths[producer]" in compact
    assert "arena->stolen_tasks()>0U" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_compact_packet_evidence_is_positive_and_scoped() -> None:
    """Packet-density evidence remains narrow and positive."""
    evidence = load_evidence("compact-queued-task")
    assert evidence["pair_count"] == 15
    assert evidence["baseline_packet_bytes"] == 72
    assert evidence["candidate_packet_bytes"] == 56
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {item["queue_packets"]: item for item in evidence["scenarios"]}
    assert set(scenarios) == {64, 256, 1024}
    for item in scenarios.values():
        assert item["candidate_wins"] >= 13
        assert item["paired_median_reduction_percent"] > 5.0


def test_running_publication_has_a_dedicated_cacheline() -> None:
    """Worker activity publication is separated from queue accounting."""
    source = source_text(ARENA)
    fragment = source.split("std::size_t stolen_local", 1)[1]
    fragment = fragment.split("std::atomic<bool> first_task_pending", 1)[0]
    assert "std::atomic<std::size_t> stolen" in fragment
    assert "alignas(64) std::atomic<bool> running" in fragment
    assert "independently contended publication off the queue snapshot" in " ".join(
        fragment.split()
    )


def test_running_layout_preserves_memory_orders_and_wake_logic() -> None:
    """The layout optimization retains synchronization semantics."""
    runtime = source_text(RUNTIME)
    arena = source_text(ARENA)
    assert "running.store(true, std::memory_order_release)" in runtime
    assert "running.store(false, std::memory_order_release)" in runtime
    assert "slot.running.load(std::memory_order_acquire)" in arena
    assert "const auto wake_target = !target_running;" in arena


def test_running_publication_probe_covers_multiple_worker_counts() -> None:
    """The running-publication probe validates exact completion and drain."""
    _source, compact = _compact_probe("layout/running-publication-cacheline-tsan.cc")
    assert "{2U,4U,8U,16U}" in compact
    assert "arena->submitted_tasks()==kTasks" in compact
    assert "arena->active_tasks()!=0U||arena->queued_tasks()!=0U" in compact
    assert "arena->peak_active_tasks()>0U" in compact


def test_running_publication_evidence_is_positive_and_scoped() -> None:
    """Running-publication evidence avoids throughput claims."""
    evidence = load_evidence("running-publication-cacheline")
    assert evidence["pair_count"] == 15
    assert "cache-line ownership" in evidence["scope"]
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {int(item["worker_pairs"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {2, 4, 8}
    for item in scenarios.values():
        assert item["candidate_wins"] >= 14
        assert item["paired_median_reduction_percent"] > 20.0


def test_selection_visits_only_ordered_set_bits() -> None:
    """Compact admission stays ordered and wide admission uses word shards."""
    helper = source_text(SELECTION)
    runtime = source_text(RUNTIME)
    for fragment in (
        "struct OrderedLaneCandidates final",
        ".first = relative & ~before_start",
        ".wrapped = relative & before_start",
        "std::countr_zero(ordered.first)",
        "std::countr_zero(ordered.wrapped)",
        "relative == width_mask",
    ):
        assert fragment in helper
    assert runtime.count("task_arena_detail::ordered_lane_candidates(") == 2
    assert "first_ordered_lane_index(" in runtime
    reservation = runtime.split("reserve_unstarted_worker(", 1)[1]
    reservation = reservation.split("queue_visibility_snapshot(", 1)[0]
    assert "if (state->scalable_scan)" in reservation
    assert "admitted_dynamic.TrySetFirstClear(begin, end, lane_origin)" in reservation
    assert "for (std::size_t offset = 0; offset < width; ++offset)" not in reservation
    assert reservation.index("if (state->scalable_scan)") < reservation.index(
        "task_arena_detail::ordered_lane_candidates("
    )


def test_selection_preserves_startup_cas_and_running_acquire() -> None:
    """Candidate enumeration retains authoritative synchronization."""
    runtime = source_text(RUNTIME)
    for fragment in (
        "admitted_mask.compare_exchange_weak(",
        "std::memory_order_acq_rel",
        "ordered.full_lane",
        "running.load(\n            std::memory_order_acquire)",
        "ordered.first &= ordered.first - 1U",
        "ordered.wrapped &= ordered.wrapped - 1U",
    ):
        assert fragment in runtime


def test_selection_probe_checks_equivalence_and_real_arena() -> None:
    """Selection is exhaustively equivalent and live-scheduler exact."""
    source, compact = _compact_probe("scheduler/sparse-round-robin-selection-tsan.cc")
    assert "verify_exhaustive_round_robin_equivalence" in source
    assert "verify_wide_random_round_robin_equivalence" in source
    assert "{16U,24U,32U}" in compact
    assert "{2U,4U,8U,16U,32U}" in compact
    for lane in ("kUpstream", "kOutput", "kAll"):
        assert f"TaskArenaLane::{lane}" in source
    assert "arena->submitted_tasks()==kTasks" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_selection_evidence_is_positive_and_scoped() -> None:
    """Selection evidence covers three widths without throughput claims."""
    evidence = load_evidence("sparse-round-robin-selection")
    assert evidence["pair_count"] == 15
    assert "round-robin worker selection" in evidence["scope"]
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {int(item["lane_width"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {8, 16, 32}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 70.0


def test_startup_flag_is_reloaded_only_while_it_can_change() -> None:
    """A cached false one-shot startup flag suppresses later loads."""
    park = source_text(RUNTIME).split("if (!found) {", 1)[1]
    park = park.split("activity.Start();", 1)[0]
    assert "first_task_pending is monotonic" in park
    assert park.count("if (first_task_pending) {") == 2
    assert park.count("slot.first_task_pending.load") == 2
    for fragment in park.split("slot.first_task_pending.load")[:-1]:
        assert fragment.rfind("if (first_task_pending) {") > fragment.rfind("}")


def test_local_recheck_and_wait_capture_remove_epoch_reloads() -> None:
    """Only a real park samples the epoch; the predicate retains its wake."""
    park = source_text(RUNTIME).split("if (!found) {", 1)[1]
    park = park.split("activity.Start();", 1)[0]
    local_recheck = park.split("if (!slot.tasks.empty()) {", 1)[1].split("}", 1)[0]
    wait = park.split("WaitWithStop(slot.ready", 1)[1]
    assert "wake_epoch.load" not in local_recheck
    assert "const auto current_epoch" in wait
    assert "observed_epoch = current_epoch;" in wait
    assert "observed_epoch = slot.wake_epoch.load" not in wait.split("});", 1)[1]


def test_initialized_worker_probe_exercises_repeated_park_wake_waves() -> None:
    """The initialized-worker probe requires exact zero-drain waves."""
    source = load_probe("scheduler/initialized-worker-park-snapshot-tsan.cc")
    assert "OperationTaskArena::Make(workers)" in source
    assert "for (const auto workers : {2U, 4U, 8U, 16U})" in source
    assert "constexpr std::size_t kWaves = 128U" in source
    assert "arena->active_tasks() == workers" in source
    assert "arena->active_tasks() == 0U" in source
    assert "arena->queued_tasks() == 0U" in source
    assert "arena->wake_epoch_publishes() >= workers" in source


def test_initialized_worker_evidence_is_positive_and_scoped() -> None:
    """Initialized-worker evidence remains scoped to atomic bookkeeping."""
    evidence = load_evidence("initialized-worker-park-snapshot")
    assert evidence["pair_count"] == 15
    assert "park/wake atomic snapshot bookkeeping" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {2, 4, 8, 16}
    for item in scenarios.values():
        assert item["candidate_wins"] >= 13
        assert item["paired_median_reduction_percent"] > 25.0


def test_visibility_shards_are_bounded_and_low_core_stays_single() -> None:
    """Only arenas wider than eight activate multiple aligned shards."""
    source = source_text(ARENA)
    assert "struct alignas(64) QueueVisibilityShard" in source
    assert "std::array<QueueVisibilityShard, 3> queue_visibility" in source
    assert "std::atomic<std::uint64_t> nonempty_mask{0}" in source


def test_publication_and_snapshots_use_only_relevant_shards() -> None:
    """Transitions publish locally and narrow admission avoids unrelated loads."""
    source = source_text(RUNTIME)
    snapshot = source.split("queue_visibility_snapshot(", 1)[1]
    snapshot = snapshot.split("idle_started_worker(", 1)[0]
    for fragment in (
        "plan.visibility_shard_begin",
        "plan.visibility_shard_end",
        "for (auto shard",
        "state->queue_visibility[shard - 1U]",
        "if constexpr (!Sharded)",
        "return snapshot & allowed;",
    ):
        assert fragment in snapshot
    for fragment in (
        "visibility->nonempty_mask.fetch_or(",
        "visibility->nonempty_mask.fetch_and(",
        "queue_visibility_snapshot<PreferDedicatedOutput>",
        "queue_visibility_snapshot(state, plan)",
        "initialized_snapshot) &",
    ):
        assert fragment in source


def test_visibility_probe_exercises_disjoint_domains() -> None:
    """The probe publishes low and high lane transitions concurrently."""
    source, compact = _compact_probe("scheduler/sharded-queue-visibility-tsan.cc")
    assert "for(constautoworkers:{9U,16U,32U})" in compact
    assert "TaskArenaLane::kUpstream" in source
    assert "TaskArenaLane::kOutput" in source
    assert "std::jthread low_producer" in source
    assert "std::jthread high_producer" in source
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_visibility_evidence_is_positive_and_scoped() -> None:
    """Visibility evidence avoids end-to-end throughput claims."""
    evidence = load_evidence("sharded-queue-visibility")
    assert evidence["pair_count"] == 15
    assert "queue-visibility publication" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {12, 16, 32}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 40.0
