// Implements operation-local phase, queue, worker, and memory telemetry.
#include "internal/runtime/performance_telemetry.hh"

#include "internal/json_encoding/token_writer.hh"
#include "internal/memory/memory_pool.hh"

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <locale>
#include <sstream>
#include <string>
#include <string_view>

namespace sanitize::internal {
namespace {

using sanitize::internal::json_encoding::append_int_field;
using sanitize::internal::json_encoding::append_key;
using sanitize::internal::json_encoding::append_string_field;

constexpr std::array<std::string_view,
                     static_cast<std::size_t>(PerformancePhase::kCount)>
    kPhaseNames = {"prepare",
                   "inference",
                   "plan_compile",
                   "stream_get_next",
                   "frontend_read",
                   "json_validation",
                   "coordinator_work",
                   "coordinator_wait",
                   "arrow_merge",
                   "arrow_finalize",
                   "output"};

constexpr std::array<std::string_view,
                     static_cast<std::size_t>(TaskTelemetryKind::kCount)>
    kTaskKindNames = {"input",           "inference", "materialization",
                      "json_validation", "output",    "other"};

constexpr std::array<std::string_view,
                     static_cast<std::size_t>(PerformanceCounter::kCount)>
    kCounterNames = {"frontend_batches",
                     "source_rows",
                     "output_batches",
                     "packets_submitted",
                     "packets_completed",
                     "column_logical_packets_submitted",
                     "jsonl_row_packets_submitted",
                     "jsonl_validation_packets_submitted",
                     "jsonl_validation_packets_completed",
                     "jsonl_token_rows_indexed",
                     "jsonl_token_fields_indexed",
                     "jsonl_plan_ordered_rows",
                     "jsonl_token_rows_fallback",
                     "column_groups_submitted",
                     "column_groups_merged",
                     "column_slots_initialized",
                     "column_slot_reuses",
                     "peak_outstanding_packets",
                     "csv_fixed_plan_fixed_fields",
                     "csv_fixed_plan_dynamic_fields",
                     "csv_output_worker_ceiling",
                     "low_core_task_telemetry_batches",
                     "worker_active_streaks",
                     "process_cpu_governor_waits",
                     "process_cpu_governor_wait_ns",
                     "numa_local_steals",
                     "numa_remote_steals",
                     "output_pressure_serializations",
                     "output_estimate_expansion_bytes"};

void update_maximum(std::atomic<std::int64_t> *target,
                    std::int64_t value) noexcept {
  auto observed = target->load(std::memory_order_relaxed);
  while (value > observed && !target->compare_exchange_weak(
                                 observed, value, std::memory_order_relaxed,
                                 std::memory_order_relaxed)) {
  }
}

void append_bool_field(std::string &out, bool &first, std::string_view key,
                       bool value) {
  append_key(out, first, key);
  out += value ? "true" : "false";
}

void append_double_field(std::string &out, bool &first, std::string_view key,
                         double value) {
  append_key(out, first, key);
  if (!std::isfinite(value)) {
    out += "null";
    return;
  }
  // Floating-point to_chars is unavailable below macOS 13.3 even when the
  // overload is declared. Telemetry is emitted once per operation, so a
  // locale-stable stream preserves the macOS 11 baseline at negligible cost.
  std::ostringstream stream;
  stream.imbue(std::locale::classic());
  stream.precision(8);
  stream << value;
  out += stream.str();
}

std::int64_t nonnegative(std::int64_t value) noexcept {
  return std::max<std::int64_t>(0, value);
}

std::int64_t signed_snapshot(std::uint64_t value) noexcept {
  return std::bit_cast<std::int64_t>(value);
}

double ratio(std::int64_t numerator, std::int64_t denominator) noexcept {
  if (numerator <= 0 || denominator <= 0) {
    return 0.0;
  }
  return static_cast<double>(numerator) / static_cast<double>(denominator);
}

struct Diagnosis final {
  std::string_view primary = "mixed_or_unresolved";
  std::string_view confidence = "low";
  bool memory_capacity_pressure = false;
  bool memory_bandwidth_proven = false;
};

Diagnosis classify(std::int64_t stream_ns, std::int64_t frontend_ns,
                   std::int64_t coordinator_wait_ns,
                   std::int64_t arrow_terminal_ns, std::int64_t worker_run_ns,
                   std::int64_t peak_memory, std::int64_t memory_limit,
                   std::int64_t effective_workers,
                   std::int64_t peak_active_tasks,
                   std::int64_t worker_tasks) noexcept {
  Diagnosis diagnosis;
  const auto memory_pressure = ratio(peak_memory, memory_limit);
  const auto stream_denominator = std::max<std::int64_t>(1, stream_ns);
  const auto wait_share = ratio(coordinator_wait_ns, stream_denominator);
  const auto frontend_share = ratio(frontend_ns, stream_denominator);
  const auto arrow_share = ratio(arrow_terminal_ns, stream_denominator);
  const auto worker_parallelism =
      ratio(worker_run_ns,
            stream_denominator * std::max<std::int64_t>(1, effective_workers));

  if (memory_limit > 0 && memory_pressure >= 0.90) {
    diagnosis.primary = "memory_capacity_pressure";
    diagnosis.confidence = memory_pressure >= 0.98 ? "high" : "medium";
    diagnosis.memory_capacity_pressure = true;
  } else if (effective_workers > 1 && peak_active_tasks <= 1 &&
             worker_tasks <= 2 && wait_share >= 0.20) {
    diagnosis.primary = "insufficient_parallel_granularity";
    diagnosis.confidence = "high";
  } else if (frontend_share >= 0.35) {
    diagnosis.primary = "frontend_or_input";
    diagnosis.confidence = frontend_share >= 0.55 ? "high" : "medium";
  } else if (arrow_share >= 0.25) {
    diagnosis.primary = "arrow_finalize_or_merge";
    diagnosis.confidence = arrow_share >= 0.45 ? "high" : "medium";
  } else if (worker_parallelism >= 0.55) {
    diagnosis.primary = "worker_compute_or_memory_hierarchy";
    diagnosis.confidence = "medium";
  } else if (wait_share >= 0.25) {
    diagnosis.primary = "reorder_or_worker_imbalance";
    diagnosis.confidence = wait_share >= 0.45 ? "high" : "medium";
  }
  return diagnosis;
}

} // namespace

PerformanceTelemetry::PerformanceTelemetry(
    std::uint64_t operation_id, std::shared_ptr<MemoryPool> operation_pool,
    std::int64_t memory_limit_bytes, std::int64_t effective_workers,
    bool multi_mode)
    : operation_id_(operation_id), operation_pool_(std::move(operation_pool)),
      memory_limit_bytes_(memory_limit_bytes),
      effective_workers_(std::max<std::int64_t>(1, effective_workers)),
      multi_mode_(multi_mode), started_ns_(NowNs()) {}

std::int64_t PerformanceTelemetry::NowNs() noexcept {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

void PerformanceTelemetry::RecordPhase(PerformancePhase phase,
                                       std::int64_t elapsed_ns) noexcept {
  const auto index = static_cast<std::size_t>(phase);
  if (index >= kPhaseCount) {
    return;
  }
  phase_ns_[index].fetch_add(nonnegative(elapsed_ns),
                             std::memory_order_relaxed);
  phase_calls_[index].fetch_add(1, std::memory_order_relaxed);
}

void PerformanceTelemetry::AddCounter(PerformanceCounter counter,
                                      std::int64_t amount) noexcept {
  const auto index = static_cast<std::size_t>(counter);
  if (index < kCounterCount) {
    counters_[index].fetch_add(amount, std::memory_order_relaxed);
  }
}

void PerformanceTelemetry::ObserveCounterMaximum(PerformanceCounter counter,
                                                 std::int64_t value) noexcept {
  const auto index = static_cast<std::size_t>(counter);
  if (index < kCounterCount) {
    update_maximum(&counters_[index], nonnegative(value));
  }
}

void PerformanceTelemetry::RecordTaskSubmitted(
    TaskTelemetryKind kind, std::size_t queue_depth) noexcept {
  const auto index = static_cast<std::size_t>(kind);
  if (index >= kTaskKindCount) {
    return;
  }
  task_submitted_[index].fetch_add(1, std::memory_order_relaxed);
  update_maximum(&peak_queue_depth_, static_cast<std::int64_t>(queue_depth));
}

void PerformanceTelemetry::RecordWorkerTaskSubmitted(
    std::size_t worker_index, TaskTelemetryKind kind,
    std::size_t queue_depth) noexcept {
  const auto kind_index = static_cast<std::size_t>(kind);
  if (kind_index >= kTaskKindCount) {
    return;
  }
  if (worker_index >= worker_submission_shards_.size()) {
    RecordTaskSubmitted(kind, queue_depth);
    return;
  }
  auto &shard = worker_submission_shards_[worker_index];
  auto &submitted = shard.submitted_local[kind_index];
  ++submitted;
  shard.submitted[kind_index].store(signed_snapshot(submitted),
                                    std::memory_order_relaxed);

  const auto depth = static_cast<std::int64_t>(queue_depth);
  if (depth > shard.peak_queue_depth_local) {
    shard.peak_queue_depth_local = depth;
    shard.peak_queue_depth.store(depth, std::memory_order_relaxed);
  }
}

void PerformanceTelemetry::RecordTaskStarted(
    TaskTelemetryKind kind, std::int64_t queue_wait_ns) noexcept {
  const auto index = static_cast<std::size_t>(kind);
  if (index >= kTaskKindCount) {
    return;
  }
  const auto elapsed = nonnegative(queue_wait_ns);
  task_started_[index].fetch_add(1, std::memory_order_relaxed);
  task_queue_wait_ns_[index].fetch_add(elapsed, std::memory_order_relaxed);
  update_maximum(&task_max_queue_wait_ns_[index], elapsed);
}

void PerformanceTelemetry::RecordTaskFinished(TaskTelemetryKind kind,
                                              std::int64_t run_ns) noexcept {
  const auto index = static_cast<std::size_t>(kind);
  if (index >= kTaskKindCount) {
    return;
  }
  const auto elapsed = nonnegative(run_ns);
  task_finished_[index].fetch_add(1, std::memory_order_relaxed);
  task_run_ns_[index].fetch_add(elapsed, std::memory_order_relaxed);
  update_maximum(&task_max_run_ns_[index], elapsed);
}

void PerformanceTelemetry::RecordTaskBatch(TaskTelemetryKind kind,
                                           std::int64_t task_count,
                                           std::int64_t queue_wait_ns,
                                           std::int64_t run_ns,
                                           std::int64_t max_queue_wait_ns,
                                           std::int64_t max_run_ns) noexcept {
  const auto index = static_cast<std::size_t>(kind);
  if (index >= kTaskKindCount || task_count <= 0) {
    return;
  }
  task_started_[index].fetch_add(task_count, std::memory_order_relaxed);
  task_finished_[index].fetch_add(task_count, std::memory_order_relaxed);
  task_queue_wait_ns_[index].fetch_add(nonnegative(queue_wait_ns),
                                       std::memory_order_relaxed);
  task_run_ns_[index].fetch_add(nonnegative(run_ns), std::memory_order_relaxed);
  update_maximum(&task_max_queue_wait_ns_[index],
                 nonnegative(max_queue_wait_ns));
  update_maximum(&task_max_run_ns_[index], nonnegative(max_run_ns));
}

void PerformanceTelemetry::RecordWorkerTaskBatch(
    std::size_t worker_index, TaskTelemetryKind kind, std::int64_t task_count,
    std::int64_t queue_wait_ns, std::int64_t run_ns,
    std::int64_t max_queue_wait_ns, std::int64_t max_run_ns) noexcept {
  const auto kind_index = static_cast<std::size_t>(kind);
  if (kind_index >= kTaskKindCount || task_count <= 0) {
    return;
  }
  if (worker_index >= worker_task_shards_.size()) {
    RecordTaskBatch(kind, task_count, queue_wait_ns, run_ns, max_queue_wait_ns,
                    max_run_ns);
    return;
  }
  auto &shard = worker_task_shards_[worker_index];
  auto &completed = shard.completed_local[kind_index];
  completed += static_cast<std::uint64_t>(task_count);
  const auto completed_snapshot = signed_snapshot(completed);
  shard.started[kind_index].store(completed_snapshot,
                                  std::memory_order_relaxed);
  shard.finished[kind_index].store(completed_snapshot,
                                   std::memory_order_relaxed);

  auto &queue_wait_total = shard.queue_wait_ns_local[kind_index];
  queue_wait_total += static_cast<std::uint64_t>(nonnegative(queue_wait_ns));
  shard.queue_wait_ns[kind_index].store(signed_snapshot(queue_wait_total),
                                        std::memory_order_relaxed);

  auto &run_total = shard.run_ns_local[kind_index];
  run_total += static_cast<std::uint64_t>(nonnegative(run_ns));
  shard.run_ns[kind_index].store(signed_snapshot(run_total),
                                 std::memory_order_relaxed);

  const auto queue_wait_max = nonnegative(max_queue_wait_ns);
  auto &published_queue_wait_max = shard.max_queue_wait_ns_local[kind_index];
  if (queue_wait_max > published_queue_wait_max) {
    published_queue_wait_max = queue_wait_max;
    shard.max_queue_wait_ns[kind_index].store(queue_wait_max,
                                              std::memory_order_relaxed);
  }
  const auto run_max = nonnegative(max_run_ns);
  auto &published_run_max = shard.max_run_ns_local[kind_index];
  if (run_max > published_run_max) {
    published_run_max = run_max;
    shard.max_run_ns[kind_index].store(run_max, std::memory_order_relaxed);
  }

  ++shard.batches_local;
  shard.batches.store(signed_snapshot(shard.batches_local),
                      std::memory_order_relaxed);
}

void PerformanceTelemetry::RecordTaskStolen() noexcept {
  stolen_tasks_.fetch_add(1, std::memory_order_relaxed);
}

void PerformanceTelemetry::RecordWorkerTaskStolen(
    std::size_t worker_index) noexcept {
  if (worker_index >= worker_task_shards_.size()) {
    RecordTaskStolen();
    return;
  }
  auto &shard = worker_task_shards_[worker_index];
  ++shard.stolen_local;
  shard.stolen.store(shard.stolen_local, std::memory_order_relaxed);
}

void PerformanceTelemetry::RecordWorkerActiveStreak(
    std::size_t worker_index) noexcept {
  if (worker_index >= worker_task_shards_.size()) {
    AddCounter(PerformanceCounter::kWorkerActiveStreaks);
    return;
  }
  auto &shard = worker_task_shards_[worker_index];
  ++shard.active_streaks_local;
  shard.active_streaks.store(signed_snapshot(shard.active_streaks_local),
                             std::memory_order_relaxed);
}

void PerformanceTelemetry::RecordWorkerStarted() noexcept {
  started_workers_.fetch_add(1, std::memory_order_relaxed);
}

void PerformanceTelemetry::ObserveActiveTasks(std::size_t active) noexcept {
  update_maximum(&peak_active_tasks_, static_cast<std::int64_t>(active));
}

void PerformanceTelemetry::Finish() noexcept {
  auto expected = std::int64_t{0};
  const auto now = NowNs();
  if (finished_ns_.compare_exchange_strong(expected, now,
                                           std::memory_order_release,
                                           std::memory_order_relaxed) &&
      operation_pool_) {
    operation_pool_->ReleaseOperationLease();
  }
}

bool PerformanceTelemetry::finished() const noexcept {
  return finished_ns_.load(std::memory_order_acquire) != 0;
}

#include "internal/runtime/performance_telemetry_json.cc.inc"

PerformancePhaseScope::PerformancePhaseScope(
    std::shared_ptr<PerformanceTelemetry> telemetry,
    PerformancePhase phase) noexcept
    : telemetry_(std::move(telemetry)), phase_(phase),
      started_ns_(telemetry_ ? PerformanceTelemetry::NowNs() : 0) {}

PerformancePhaseScope::~PerformancePhaseScope() {
  if (telemetry_) {
    telemetry_->RecordPhase(phase_,
                            PerformanceTelemetry::NowNs() - started_ns_);
  }
}

PerformanceCompletionScope::~PerformanceCompletionScope() {
  if (telemetry_) {
    telemetry_->Finish();
  }
}

} // namespace sanitize::internal
