// Records bounded, operation-local performance telemetry without changing
// results.
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

namespace sanitize::internal {
class MemoryPool;

enum class PerformancePhase : std::uint8_t {
  kPrepare,
  kInference,
  kPlanCompile,
  kStreamGetNext,
  kFrontendRead,
  kJsonValidation,
  kCoordinatorWork,
  kCoordinatorWait,
  kArrowMerge,
  kArrowFinalize,
  kOutput,
  kCount,
};

enum class TaskTelemetryKind : std::uint8_t {
  kInput,
  kInference,
  kMaterialization,
  kJsonValidation,
  kOutput,
  kOther,
  kCount,
};

enum class PerformanceCounter : std::uint8_t {
  kFrontendBatches,
  kSourceRows,
  kOutputBatches,
  kPacketsSubmitted,
  kPacketsCompleted,
  kColumnLogicalPacketsSubmitted,
  kJsonlRowPacketsSubmitted,
  kJsonlValidationPacketsSubmitted,
  kJsonlValidationPacketsCompleted,
  kJsonlTokenRowsIndexed,
  kJsonlTokenFieldsIndexed,
  kJsonlPlanOrderedRows,
  kJsonlTokenRowsFallback,
  kColumnGroupsSubmitted,
  kColumnGroupsMerged,
  kColumnSlotsInitialized,
  kColumnSlotReuses,
  kPeakOutstandingPackets,
  kCsvFixedPlanFixedFields,
  kCsvFixedPlanDynamicFields,
  kCsvOutputWorkerCeiling,
  kLowCoreTaskTelemetryBatches,
  kWorkerActiveStreaks,
  kProcessCpuGovernorWaits,
  kProcessCpuGovernorWaitNs,
  kNumaLocalSteals,
  kNumaRemoteSteals,
  kOutputPressureSerializations,
  kOutputEstimateExpansionBytes,
  kCount,
};

class PerformanceTelemetry final {
public:
  PerformanceTelemetry(std::uint64_t operation_id,
                       std::shared_ptr<MemoryPool> operation_pool,
                       std::int64_t memory_limit_bytes,
                       std::int64_t effective_workers, bool multi_mode);

  PerformanceTelemetry(const PerformanceTelemetry &) = delete;
  PerformanceTelemetry &operator=(const PerformanceTelemetry &) = delete;

  [[nodiscard]] static std::int64_t NowNs() noexcept;

  void RecordPhase(PerformancePhase phase, std::int64_t elapsed_ns) noexcept;
  void AddCounter(PerformanceCounter counter, std::int64_t amount = 1) noexcept;
  void ObserveCounterMaximum(PerformanceCounter counter,
                             std::int64_t value) noexcept;

  void RecordTaskSubmitted(TaskTelemetryKind kind,
                           std::size_t queue_depth) noexcept;
  void RecordWorkerTaskSubmitted(std::size_t worker_index,
                                 TaskTelemetryKind kind,
                                 std::size_t queue_depth) noexcept;
  void RecordTaskStarted(TaskTelemetryKind kind,
                         std::int64_t queue_wait_ns) noexcept;
  void RecordTaskFinished(TaskTelemetryKind kind, std::int64_t run_ns) noexcept;
  void RecordTaskBatch(TaskTelemetryKind kind, std::int64_t task_count,
                       std::int64_t queue_wait_ns, std::int64_t run_ns,
                       std::int64_t max_queue_wait_ns,
                       std::int64_t max_run_ns) noexcept;
  void RecordWorkerTaskBatch(std::size_t worker_index, TaskTelemetryKind kind,
                             std::int64_t task_count,
                             std::int64_t queue_wait_ns, std::int64_t run_ns,
                             std::int64_t max_queue_wait_ns,
                             std::int64_t max_run_ns) noexcept;
  void RecordTaskStolen() noexcept;
  void RecordWorkerTaskStolen(std::size_t worker_index) noexcept;
  void RecordWorkerActiveStreak(std::size_t worker_index) noexcept;
  void RecordWorkerStarted() noexcept;
  void ObserveActiveTasks(std::size_t active) noexcept;

  void Finish() noexcept;
  [[nodiscard]] bool finished() const noexcept;
  [[nodiscard]] std::string ToJson() const;
  [[nodiscard]] std::shared_ptr<MemoryPool> memory_pool() const noexcept {
    return operation_pool_;
  }
  [[nodiscard]] std::int64_t memory_limit_bytes() const noexcept {
    return memory_limit_bytes_;
  }

private:
  static constexpr std::size_t kPhaseCount =
      static_cast<std::size_t>(PerformancePhase::kCount);
  static constexpr std::size_t kTaskKindCount =
      static_cast<std::size_t>(TaskTelemetryKind::kCount);
  static constexpr std::size_t kCounterCount =
      static_cast<std::size_t>(PerformanceCounter::kCount);

  std::uint64_t operation_id_ = 0;
  std::shared_ptr<MemoryPool> operation_pool_;
  std::int64_t memory_limit_bytes_ = -1;
  std::int64_t effective_workers_ = 1;
  bool multi_mode_ = false;
  std::int64_t started_ns_ = 0;
  std::atomic<std::int64_t> finished_ns_{0};

  std::array<std::atomic<std::int64_t>, kPhaseCount> phase_ns_{};
  std::array<std::atomic<std::int64_t>, kPhaseCount> phase_calls_{};
  std::array<std::atomic<std::int64_t>, kCounterCount> counters_{};

  std::array<std::atomic<std::int64_t>, kTaskKindCount> task_submitted_{};
  std::array<std::atomic<std::int64_t>, kTaskKindCount> task_started_{};
  std::array<std::atomic<std::int64_t>, kTaskKindCount> task_finished_{};
  std::array<std::atomic<std::int64_t>, kTaskKindCount> task_queue_wait_ns_{};
  std::array<std::atomic<std::int64_t>, kTaskKindCount> task_run_ns_{};
  std::array<std::atomic<std::int64_t>, kTaskKindCount>
      task_max_queue_wait_ns_{};
  std::array<std::atomic<std::int64_t>, kTaskKindCount> task_max_run_ns_{};

  struct alignas(64) WorkerSubmissionTelemetryShard final {
    std::array<std::atomic<std::int64_t>, kTaskKindCount> submitted{};
    std::atomic<std::int64_t> peak_queue_depth{0};
    // Producers targeting one physical queue are serialized by that queue's
    // mutex. Keep exact admission totals private to that serialization domain
    // and publish lock-free snapshots without a shared RMW or CAS loop.
    std::array<std::uint64_t, kTaskKindCount> submitted_local{};
    std::int64_t peak_queue_depth_local = 0;
  };
  static constexpr std::size_t kWorkerSubmissionShardCount = 32;
  std::array<WorkerSubmissionTelemetryShard, kWorkerSubmissionShardCount>
      worker_submission_shards_{};

  struct alignas(64) WorkerTaskTelemetryShard final {
    std::array<std::atomic<std::int64_t>, kTaskKindCount> started{};
    std::array<std::atomic<std::int64_t>, kTaskKindCount> finished{};
    std::array<std::atomic<std::int64_t>, kTaskKindCount> queue_wait_ns{};
    std::array<std::atomic<std::int64_t>, kTaskKindCount> run_ns{};
    std::array<std::atomic<std::int64_t>, kTaskKindCount> max_queue_wait_ns{};
    std::array<std::atomic<std::int64_t>, kTaskKindCount> max_run_ns{};
    std::atomic<std::int64_t> batches{0};
    // RecordWorkerTaskBatch is called only by the matching physical worker
    // on the compact telemetry path. Wider arenas use aggregate atomics.
    // Keep exact writer-local totals and publish lock-free snapshots with one
    // relaxed store per field instead of shared-style RMW/CAS operations.
    std::array<std::uint64_t, kTaskKindCount> completed_local{};
    std::array<std::uint64_t, kTaskKindCount> queue_wait_ns_local{};
    std::array<std::uint64_t, kTaskKindCount> run_ns_local{};
    std::array<std::int64_t, kTaskKindCount> max_queue_wait_ns_local{};
    std::array<std::int64_t, kTaskKindCount> max_run_ns_local{};
    std::uint64_t batches_local = 0;
    // This shard has exactly one arena-worker writer. Keep the authoritative
    // writer-local value plain and publish one atomic snapshot for readers.
    std::int64_t stolen_local = 0;
    std::atomic<std::int64_t> stolen{0};
    // The matching arena worker is the only writer. Publish the exact active
    // streak total without contending on the operation-global counter array.
    std::uint64_t active_streaks_local = 0;
    std::atomic<std::int64_t> active_streaks{0};
  };
  static constexpr std::size_t kWorkerTaskShardCount = 32;
  std::array<WorkerTaskTelemetryShard, kWorkerTaskShardCount>
      worker_task_shards_{};

  std::atomic<std::int64_t> peak_queue_depth_{0};
  std::atomic<std::int64_t> peak_active_tasks_{0};
  std::atomic<std::int64_t> stolen_tasks_{0};
  std::atomic<std::int64_t> started_workers_{0};
};

class PerformancePhaseScope final {
public:
  PerformancePhaseScope(std::shared_ptr<PerformanceTelemetry> telemetry,
                        PerformancePhase phase) noexcept;
  ~PerformancePhaseScope();

  PerformancePhaseScope(const PerformancePhaseScope &) = delete;
  PerformancePhaseScope &operator=(const PerformancePhaseScope &) = delete;

private:
  std::shared_ptr<PerformanceTelemetry> telemetry_;
  PerformancePhase phase_ = PerformancePhase::kPrepare;
  std::int64_t started_ns_ = 0;
};

class PerformanceCompletionScope final {
public:
  explicit PerformanceCompletionScope(
      std::shared_ptr<PerformanceTelemetry> telemetry) noexcept
      : telemetry_(std::move(telemetry)) {}
  ~PerformanceCompletionScope();
  void Dismiss() noexcept { telemetry_.reset(); }

  PerformanceCompletionScope(const PerformanceCompletionScope &) = delete;
  PerformanceCompletionScope &
  operator=(const PerformanceCompletionScope &) = delete;

private:
  std::shared_ptr<PerformanceTelemetry> telemetry_;
};

} // namespace sanitize::internal
