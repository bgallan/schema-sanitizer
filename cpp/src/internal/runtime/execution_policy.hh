// Derives one immutable execution policy from mode, memory, and host CPUs.
#pragma once

#include "internal/memory/memory_budget.hh"
#include "internal/runtime/cpu_capacity.hh"
#include "sanitize/options/options.hh"

#include <algorithm>
#include <cstdint>
#include <limits>

namespace sanitize::internal {

inline constexpr std::int64_t kMinimumWorkerArenaBytes = 8LL * 1024LL * 1024LL;
// Native thread stacks are outside the PMR allocation graph. Reserve a
// conservative resident-stack/runtime allowance per helper before deciding
// how many helpers a memory budget can safely sustain.
inline constexpr std::int64_t kWorkerRuntimeReserveBytes =
    1LL * 1024LL * 1024LL;
inline constexpr std::int64_t kMinimumWorkerMemoryBytes =
    kMinimumWorkerArenaBytes + kWorkerRuntimeReserveBytes;
inline constexpr std::int64_t kMaxMaterializationPacketRows = 5120;
inline constexpr std::int64_t kMaxMaterializationPacketBytes =
    1LL * 1024LL * 1024LL;

enum class ExecutionFallbackReason : std::uint8_t {
  kNone = 0,
  kSingleRequested = 1,
  kCpuLimited = 2,
  kMemoryLimited = 3,
};

struct ExecutionPolicy {
  sanitize::ThreadingMode requested_mode = sanitize::ThreadingMode::kSingle;
  std::int64_t available_cpus = 1;
  std::int64_t effective_workers = 1;
  std::int64_t task_queue_capacity = 1;
  std::int64_t reorder_capacity = 1;
  std::int64_t worker_arena_bytes = 1;
  std::int64_t materialization_packet_target_bytes = 1;
  std::int64_t materialization_packet_max_rows = 1;
  std::int64_t async_concurrency = 1;
  std::int64_t async_prefetch_files = 1;
  std::int64_t remote_chunk_prefetch = 1;
  std::int64_t source_discovery_concurrency = 1;
  bool pyarrow_use_threads = false;
  ExecutionFallbackReason fallback_reason =
      ExecutionFallbackReason::kSingleRequested;
};

[[nodiscard]] inline std::int64_t available_cpu_count() noexcept {
  return available_cpu_capacity();
}

[[nodiscard]] constexpr ExecutionPolicy
execution_policy_from(sanitize::ThreadingMode mode,
                      std::int64_t requested_memory_limit,
                      std::int64_t available_cpus) noexcept {
  const auto budget = memory_budget_from_limit(requested_memory_limit);
  ExecutionPolicy out;
  out.requested_mode = mode;
  out.available_cpus = std::max<std::int64_t>(1, available_cpus);
  out.worker_arena_bytes = std::max<std::int64_t>(1, budget.total_bytes);

  if (mode == sanitize::ThreadingMode::kSingle) {
    return out;
  }

  const auto cpu_workers = out.available_cpus;
  // Reserve one quarter of the operation budget for the ordered reader,
  // reducer, writer, and reorder ownership. Workers share the remainder.
  const auto worker_pool_bytes =
      std::max<std::int64_t>(1, budget.total_bytes - budget.total_bytes / 4);
  const auto memory_workers =
      std::max<std::int64_t>(1, worker_pool_bytes / kMinimumWorkerMemoryBytes);
  out.effective_workers =
      std::max<std::int64_t>(1, std::min(cpu_workers, memory_workers));
  out.worker_arena_bytes =
      std::max<std::int64_t>(1, worker_pool_bytes / out.effective_workers -
                                    kWorkerRuntimeReserveBytes);
  if (out.effective_workers == 1) {
    out.fallback_reason = cpu_workers <= 1
                              ? ExecutionFallbackReason::kCpuLimited
                              : ExecutionFallbackReason::kMemoryLimited;
    return out;
  }
  const auto max = std::numeric_limits<std::int64_t>::max();
  out.task_queue_capacity =
      out.effective_workers > max / 2
          ? max
          : std::max<std::int64_t>(1, out.effective_workers * 2);
  out.reorder_capacity = out.task_queue_capacity;

  // Packet outputs live in the ordered reorder window until the coordinator
  // commits them. Derive a target from both the coordinator reserve and each
  // worker arena so queue growth remains bounded by the operation budget. One
  // unusually large source row may exceed this target, but it is isolated in a
  // one-row packet by the packet builder.
  const auto reorder_reserve_bytes =
      std::max<std::int64_t>(1, budget.total_bytes / 8);
  const auto per_reorder_slot_bytes =
      std::max<std::int64_t>(1, reorder_reserve_bytes / out.reorder_capacity);
  const auto per_worker_packet_bytes =
      std::max<std::int64_t>(1, out.worker_arena_bytes / 8);
  out.materialization_packet_target_bytes = std::max<std::int64_t>(
      1, std::min({kMaxMaterializationPacketBytes, per_reorder_slot_bytes,
                   per_worker_packet_bytes}));
  out.materialization_packet_max_rows = kMaxMaterializationPacketRows;

  out.async_concurrency = std::min<std::int64_t>(
      budget.async_concurrency,
      std::max<std::int64_t>(1, out.effective_workers * 2));
  out.async_prefetch_files = std::min<std::int64_t>(
      budget.async_prefetch_files,
      std::max<std::int64_t>(1, out.task_queue_capacity * 2));
  out.remote_chunk_prefetch = std::min<std::int64_t>(
      budget.remote_chunk_prefetch, out.task_queue_capacity);
  out.source_discovery_concurrency = std::min<std::int64_t>(
      budget.source_discovery_concurrency,
      std::max<std::int64_t>(1, out.task_queue_capacity * 2));
  out.pyarrow_use_threads = true;
  out.fallback_reason = ExecutionFallbackReason::kNone;
  return out;
}

[[nodiscard]] inline ExecutionPolicy
execution_policy_from(sanitize::ThreadingMode mode,
                      std::int64_t requested_memory_limit) noexcept {
  return execution_policy_from(mode, requested_memory_limit,
                               available_cpu_count());
}

// Narrows one stage without changing its aggregate worker-arena budget. This
// keeps stage-specific worker ceilings from accidentally increasing memory
// while allowing fewer workers to retain the per-worker headroom that was
// already reserved by the host-wide policy.
[[nodiscard]] constexpr ExecutionPolicy execution_policy_with_worker_ceiling(
    const ExecutionPolicy &policy, std::int64_t worker_ceiling,
    std::int64_t queue_slots_per_worker = 2) noexcept {
  if (policy.effective_workers <= 1) {
    return policy;
  }

  ExecutionPolicy out = policy;
  const auto ceiling = std::max<std::int64_t>(1, worker_ceiling);
  out.effective_workers =
      std::min<std::int64_t>(policy.effective_workers, ceiling);

  const auto slots_per_worker =
      std::max<std::int64_t>(1, queue_slots_per_worker);
  const auto max = std::numeric_limits<std::int64_t>::max();
  const auto desired_queue = out.effective_workers > max / slots_per_worker
                                 ? max
                                 : out.effective_workers * slots_per_worker;
  out.task_queue_capacity = std::max<std::int64_t>(
      1, std::min(policy.task_queue_capacity, desired_queue));
  out.reorder_capacity = out.task_queue_capacity;

  const auto worker_pool_bytes =
      policy.worker_arena_bytes > max / policy.effective_workers
          ? max
          : policy.worker_arena_bytes * policy.effective_workers;
  out.worker_arena_bytes =
      std::max<std::int64_t>(1, worker_pool_bytes / out.effective_workers);
  return out;
}

// Narrows a stage to the amount of independent work currently available. A
// one-item stage remains strictly inline even when the operation arena exposes
// more CPUs. Aggregate worker-arena memory is preserved while queue/reorder
// windows shrink with the useful worker count.
[[nodiscard]] constexpr ExecutionPolicy execution_policy_for_work_items(
    const ExecutionPolicy &policy, std::int64_t work_items,
    std::int64_t minimum_items_per_worker = 1,
    std::int64_t queue_slots_per_worker = 1) noexcept {
  if (policy.effective_workers <= 1) {
    return policy;
  }
  const auto items = std::max<std::int64_t>(1, work_items);
  const auto per_worker = std::max<std::int64_t>(1, minimum_items_per_worker);
  const auto useful_workers = 1 + (items - 1) / per_worker;
  return execution_policy_with_worker_ceiling(
      policy, std::min(policy.effective_workers, useful_workers),
      queue_slots_per_worker);
}

} // namespace sanitize::internal
