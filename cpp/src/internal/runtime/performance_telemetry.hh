// Declares bounded operation-local performance telemetry that cannot change
// results. Scoped helpers time phases and finalize one completion snapshot
// when their retained collector leaves scope.

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
  /// Captures immutable operation identity, memory, worker, and mode context.
  PerformanceTelemetry(std::uint64_t operation_id,
                       std::shared_ptr<MemoryPool> operation_pool,
                       std::int64_t memory_limit_bytes,
                       std::int64_t effective_workers, bool multi_mode);

  /// Disables copying the operation performance telemetry collector.
  PerformanceTelemetry(const PerformanceTelemetry &) = delete;
  /// Disables copy assignment for the operation performance
  /// telemetry collector.
  PerformanceTelemetry &operator=(const PerformanceTelemetry &) = delete;

  /// Returns a steady-clock timestamp for operation telemetry intervals.
  [[nodiscard]] static std::int64_t NowNs() noexcept;

  /// Adds elapsed time to one operation phase counter.
  void RecordPhase(PerformancePhase phase, std::int64_t elapsed_ns) noexcept;
  /// Adds a signed amount to one aggregate telemetry counter.
  void AddCounter(PerformanceCounter counter, std::int64_t amount = 1) noexcept;
  /// Raises one telemetry counter's observed maximum.
  void ObserveCounterMaximum(PerformanceCounter counter,
                             std::int64_t value) noexcept;

  /// Records a task submission and its aggregate queue depth.
  void RecordTaskSubmitted(TaskTelemetryKind kind,
                           std::size_t queue_depth) noexcept;
  /// Records a submission in its worker-local telemetry shard.
  void RecordWorkerTaskSubmitted(std::size_t worker_index,
                                 TaskTelemetryKind kind,
                                 std::size_t queue_depth) noexcept;
  /// Records queue wait when one task begins execution.
  void RecordTaskStarted(TaskTelemetryKind kind,
                         std::int64_t queue_wait_ns) noexcept;
  /// Records run time when one task finishes execution.
  void RecordTaskFinished(TaskTelemetryKind kind, std::int64_t run_ns) noexcept;
  /// Adds batched task counts, durations, and maxima to aggregate telemetry.
  void RecordTaskBatch(TaskTelemetryKind kind, std::int64_t task_count,
                       std::int64_t queue_wait_ns, std::int64_t run_ns,
                       std::int64_t max_queue_wait_ns,
                       std::int64_t max_run_ns) noexcept;
  /// Adds batched task measurements to one worker-local telemetry shard.
  void RecordWorkerTaskBatch(std::size_t worker_index, TaskTelemetryKind kind,
                             std::int64_t task_count,
                             std::int64_t queue_wait_ns, std::int64_t run_ns,
                             std::int64_t max_queue_wait_ns,
                             std::int64_t max_run_ns) noexcept;
  /// Admits one worker task whose batched measurements must precede finality.
  [[nodiscard]] bool BeginWorkerTaskPublication() noexcept;
  /// Releases worker-task publication ownership after its shard is visible.
  void CompleteWorkerTaskPublications(std::size_t task_count) noexcept;
  /// Increments the aggregate count of work-stealing executions.
  void RecordTaskStolen() noexcept;
  /// Increments one worker shard's stolen-task count.
  void RecordWorkerTaskStolen(std::size_t worker_index) noexcept;
  /// Increments one worker shard's active-streak count.
  void RecordWorkerActiveStreak(std::size_t worker_index) noexcept;
  /// Increments the number of native workers that actually started.
  void RecordWorkerStarted() noexcept;
  /// Updates the peak concurrently active task count.
  void ObserveActiveTasks(std::size_t active) noexcept;

  /// Finalizes immutable duration and bottleneck fields once per operation.
  void Finish() noexcept;
  /// Reports whether operation telemetry has already been finalized.
  [[nodiscard]] bool finished() const noexcept;
  /// Serializes a coherent aggregate and worker-shard telemetry snapshot.
  [[nodiscard]] std::string ToJson() const;
  /// Returns the operation memory pool sampled by this collector.
  [[nodiscard]] std::shared_ptr<MemoryPool> memory_pool() const noexcept {
    return operation_pool_;
  }
  /// Returns the operation memory limit captured by this collector.
  [[nodiscard]] std::int64_t memory_limit_bytes() const noexcept {
    return memory_limit_bytes_;
  }

private:
  /// Publishes the final timestamp and releases the operation memory lease
  /// once.
  void Finalize() noexcept;

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
  static constexpr std::uint64_t kFinishRequested = std::uint64_t{1} << 63U;
  static constexpr std::uint64_t kTaskPublicationCountMask =
      kFinishRequested - 1U;
  // The high bit closes admission when Finish() is requested; the remaining
  // bits count worker tasks whose completion batch is not yet public. Keeping
  // both states in one atomic prevents Finish() from racing a worker between
  // its admission and publication.
  std::atomic<std::uint64_t> task_publication_state_{0};

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
  /// Starts timing the selected operation phase for the supplied collector.
  PerformancePhaseScope(std::shared_ptr<PerformanceTelemetry> telemetry,
                        PerformancePhase phase) noexcept;
  /// Records elapsed phase time when the timing scope exits.
  ~PerformancePhaseScope();

  /// Disables copying the timed performance-phase scope.
  PerformancePhaseScope(const PerformancePhaseScope &) = delete;
  /// Disables copy assignment for the timed performance-phase scope.
  PerformancePhaseScope &operator=(const PerformancePhaseScope &) = delete;

private:
  std::shared_ptr<PerformanceTelemetry> telemetry_;
  PerformancePhase phase_ = PerformancePhase::kPrepare;
  std::int64_t started_ns_ = 0;
};

class PerformanceCompletionScope final {
public:
  /// Retains a collector that will be finalized when this scope exits.
  explicit PerformanceCompletionScope(
      std::shared_ptr<PerformanceTelemetry> telemetry) noexcept
      : telemetry_(std::move(telemetry)) {}
  /// Finalizes retained operation telemetry when the completion scope exits.
  ~PerformanceCompletionScope();
  /// Prevents scope destruction from finalizing the telemetry collector.
  void Dismiss() noexcept { telemetry_.reset(); }

  /// Disables copying the operation-completion telemetry scope.
  PerformanceCompletionScope(const PerformanceCompletionScope &) = delete;
  /// Disables copy assignment for the operation-completion telemetry scope.
  PerformanceCompletionScope &
  operator=(const PerformanceCompletionScope &) = delete;

private:
  std::shared_ptr<PerformanceTelemetry> telemetry_;
};

} // namespace sanitize::internal
