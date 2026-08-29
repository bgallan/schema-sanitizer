// Declares the bounded native worker set shared by one public operation.
// Permit and memory leases make task admission, completion, and shutdown
// ownership explicit.

#pragma once

#include "internal/runtime/performance_telemetry.hh"
#include "sanitize/core/status.hh"

#include "internal/runtime/thread_compat.hh"
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <memory_resource>
#include <optional>
#include <utility>
#if defined(__APPLE__)
#include <mutex>
#endif

namespace sanitize::internal {

enum class TaskArenaLane : unsigned char {
  kUpstream,
  kOutputCompact,
  kOutput,
  kAll,
};

/// Returns the process's observed native thread count.
[[nodiscard]] std::optional<std::size_t>
process_physical_thread_count() noexcept;
/// Acquires a physical-thread permit range meeting the requested minimum.
[[nodiscard]] std::size_t
acquire_process_physical_thread_permits(std::size_t desired,
                                        std::size_t minimum) noexcept;
/// Returns physical-thread permits to process-wide capacity.
void release_process_physical_thread_permits(std::size_t amount) noexcept;

class ProcessPhysicalThreadPermitLease final {
public:
  /// Creates an empty physical-thread permit lease.
  ProcessPhysicalThreadPermitLease() noexcept = default;
  /// Acquires the requested number of process physical-thread permits.
  explicit ProcessPhysicalThreadPermitLease(std::size_t amount) noexcept
      : amount_(acquire_process_physical_thread_permits(amount, amount)) {}
  /// Disables copying the physical-thread permit lease.
  ProcessPhysicalThreadPermitLease(const ProcessPhysicalThreadPermitLease &) =
      delete;
  /// Disables copy assignment for the physical-thread permit lease.
  ProcessPhysicalThreadPermitLease &
  operator=(const ProcessPhysicalThreadPermitLease &) = delete;
  /// Transfers ownership from another physical-thread permit lease.
  ProcessPhysicalThreadPermitLease(
      ProcessPhysicalThreadPermitLease &&other) noexcept
      : amount_(std::exchange(other.amount_, 0U)) {}
  /// Transfers owned state from another physical-thread permit lease.
  ProcessPhysicalThreadPermitLease &
  operator=(ProcessPhysicalThreadPermitLease &&other) noexcept {
    if (this != &other) {
      reset();
      amount_ = std::exchange(other.amount_, 0U);
    }
    return *this;
  }
  /// Returns any remaining physical-thread permits to process capacity.
  ~ProcessPhysicalThreadPermitLease() noexcept { reset(); }

  /// Reports whether this lease currently owns a nonzero reservation.
  [[nodiscard]] explicit operator bool() const noexcept {
    return amount_ != 0U;
  }
  /// Returns physical-thread permits currently owned by this lease.
  [[nodiscard]] std::size_t amount() const noexcept { return amount_; }
  /// Returns all owned physical-thread permits to process capacity.
  void reset() noexcept {
    if (amount_ != 0U) {
      const auto amount = std::exchange(amount_, 0U);
      release_process_physical_thread_permits(amount);
    }
  }

private:
  std::size_t amount_ = 0U;
};
/// Acquires active capacity for process-global external runtime pools from a
/// distinct subledger. Reservations are neither pending managed thread starts
/// nor OS-thread identity evidence.
[[nodiscard]] std::size_t
acquire_process_external_runtime_thread_permits(std::size_t desired,
                                                std::size_t minimum) noexcept;
/// Returns external-runtime thread permits to process-wide capacity.
void release_process_external_runtime_thread_permits(
    std::size_t amount) noexcept;

class ProcessExternalRuntimeThreadPermitLease final {
public:
  /// Creates an empty external-runtime thread permit lease.
  ProcessExternalRuntimeThreadPermitLease() noexcept = default;
  /// Acquires an external-runtime permit range meeting the
  /// requested minimum.
  ProcessExternalRuntimeThreadPermitLease(std::size_t desired,
                                          std::size_t minimum) noexcept
      : amount_(acquire_process_external_runtime_thread_permits(desired,
                                                                minimum)) {}
  /// Disables copying the external-runtime thread permit lease.
  ProcessExternalRuntimeThreadPermitLease(
      const ProcessExternalRuntimeThreadPermitLease &) = delete;
  /// Disables copy assignment for the external-runtime thread permit lease.
  ProcessExternalRuntimeThreadPermitLease &
  operator=(const ProcessExternalRuntimeThreadPermitLease &) = delete;
  /// Transfers ownership from another external-runtime thread permit lease.
  ProcessExternalRuntimeThreadPermitLease(
      ProcessExternalRuntimeThreadPermitLease &&other) noexcept
      : amount_(std::exchange(other.amount_, 0U)) {}
  /// Transfers owned state from another external-runtime thread
  /// permit lease.
  ProcessExternalRuntimeThreadPermitLease &
  operator=(ProcessExternalRuntimeThreadPermitLease &&other) noexcept {
    if (this != &other) {
      reset();
      amount_ = std::exchange(other.amount_, 0U);
    }
    return *this;
  }
  /// Returns any remaining external-runtime permits to process capacity.
  ~ProcessExternalRuntimeThreadPermitLease() noexcept { reset(); }

  /// Reports whether this lease currently owns a nonzero reservation.
  [[nodiscard]] explicit operator bool() const noexcept {
    return amount_ != 0U;
  }
  /// Returns external-runtime thread permits currently owned by this lease.
  [[nodiscard]] std::size_t amount() const noexcept { return amount_; }
  /// Returns permits above the requested retained lease size.
  [[nodiscard]] bool shrink(std::size_t target) noexcept {
    if (target > amount_) {
      return false;
    }
    if (target < amount_) {
      const auto returned = amount_ - target;
      // Publish reduced owner authority before returning aggregate capacity.
      amount_ = target;
      release_process_external_runtime_thread_permits(returned);
    }
    return true;
  }
  /// Returns all external-runtime thread permits owned by this lease.
  void reset() noexcept {
    const auto amount = std::exchange(amount_, 0U);
    if (amount != 0U) {
      release_process_external_runtime_thread_permits(amount);
    }
  }

private:
  std::size_t amount_ = 0U;
};
/// Records runtime-observed resident pool width as identity evidence without
/// reserving active capacity, keeping it separate from operation claims.
void add_process_external_runtime_resident_threads(std::size_t amount) noexcept;
/// Removes externally resident threads from process accounting.
void release_process_external_runtime_resident_threads(
    std::size_t amount) noexcept;
/// Charges stack reservations for unpermitted external-runtime threads.
void add_process_external_runtime_stack_debt_threads(
    std::size_t amount) noexcept;
/// Retires stack-debt charges for departed external-runtime threads.
void release_process_external_runtime_stack_debt_threads(
    std::size_t amount) noexcept;
/// Applies coordinated external-runtime identity and stack-debt deltas.
void update_process_external_runtime_residency(
    std::int64_t identity_delta, std::int64_t stack_debt_delta) noexcept;
/// Returns the stack reservation charged for each governed native thread.
[[nodiscard]] std::uint64_t process_thread_stack_reservation_bytes() noexcept;
/// Records that a governed physical thread has begun executing.
void mark_process_physical_thread_running() noexcept;
/// Records that a governed physical thread has stopped executing.
void mark_process_physical_thread_stopped() noexcept;

struct OperationTaskArenaRuntimeSnapshot final {
  std::size_t live_arenas = 0U;
  std::size_t detached_workers = 0U;
  std::size_t reaper_workers = 0U;
  std::size_t reaper_queued_states = 0U;
  std::size_t reaper_active_states = 0U;
  std::size_t reaper_reserved_states = 0U;
  std::size_t reaper_parked_states = 0U;
  std::size_t counter_underflows = 0U;
  std::size_t reaper_queued_bytes = 0U;
  std::size_t reaper_active_bytes = 0U;
  std::size_t reaper_reserved_bytes = 0U;
  std::size_t reaper_parked_bytes = 0U;
  std::int64_t oldest_parked_since_ns = 0;
  std::size_t reaper_thread_permits = 0U;
  std::size_t reaper_thread_start_failures = 0U;
  std::size_t native_physical_threads = 0U;
  std::size_t native_physical_thread_capacity = 0U;
  std::size_t native_physical_thread_rejections = 0U;
  std::size_t external_runtime_thread_permits = 0U;
  std::size_t completion_memory_protocol_violations = 0U;
  std::size_t total_physical_thread_permits = 0U;
  std::size_t external_runtime_resident_threads = 0U;
  std::size_t external_runtime_stack_debt_threads = 0U;
  bool thread_permit_snapshot_stable = false;
  std::size_t external_runtime_resident_protocol_violations = 0U;
  std::size_t reaper_over_capacity = 0U;
  std::size_t reaper_terminal_states = 0U;
  std::size_t reaper_terminal_bytes = 0U;
  std::int64_t oldest_terminal_since_ns = 0;
  std::size_t reaper_stopping_lanes = 0U;
};

// Queue-retention accounting only.  This charge limits how much ownership the
// arena may pin while queued/active; it never reserves bytes from the operation
// memory ledger and therefore may safely describe buffers already charged by a
// PMR resource or OperationMemoryLease without double physical accounting.
struct TaskMemoryCharge final {
  /// Creates an empty task retained-memory charge.
  constexpr TaskMemoryCharge() noexcept = default;
  /// Marks a positive retained-byte estimate as an explicit task charge.
  explicit constexpr TaskMemoryCharge(std::size_t bytes) noexcept
      : retained_bytes(std::max<std::size_t>(1U, bytes)),
        explicit_charge(true) {}

  std::size_t retained_bytes = 256U;
  bool explicit_charge = false;
};

// libc++ versions shipped by supported macOS runners do not provide the
// C++20 std::atomic<std::shared_ptr<T>> specialization. Keep its small API
// behind a mutex there. Other platforms use the native specialization; the
// C++11 shared_ptr atomic free functions are deprecated in C++20.
#if defined(__APPLE__)
template <typename T> class AtomicSharedPtr final {
public:
  /// Creates an empty mutex-backed shared pointer.
  AtomicSharedPtr() noexcept = default;
  /// Initializes the mutex-backed pointer with shared ownership of `value`.
  explicit AtomicSharedPtr(std::shared_ptr<T> value) noexcept
      : value_(std::move(value)) {}

  /// Disables copying the mutex-backed atomic shared pointer.
  AtomicSharedPtr(const AtomicSharedPtr &) = delete;
  /// Disables copy assignment for the mutex-backed atomic shared pointer.
  AtomicSharedPtr &operator=(const AtomicSharedPtr &) = delete;

  /// Loads a coherent shared pointer snapshot under the compatibility mutex.
  [[nodiscard]] std::shared_ptr<T>
  load(std::memory_order order = std::memory_order_seq_cst) const noexcept {
    static_cast<void>(order);
    const std::lock_guard<std::mutex> lock(mutex_);
    return value_;
  }

  /// Replaces the stored shared pointer and returns its previous ownership.
  std::shared_ptr<T>
  exchange(std::shared_ptr<T> desired,
           std::memory_order order = std::memory_order_seq_cst) noexcept {
    static_cast<void>(order);
    const std::lock_guard<std::mutex> lock(mutex_);
    auto previous = std::move(value_);
    value_ = std::move(desired);
    return previous;
  }

private:
  mutable std::mutex mutex_;
  std::shared_ptr<T> value_;
};
#else
template <typename T> using AtomicSharedPtr = std::atomic<std::shared_ptr<T>>;
#endif

class TaskMemoryLease final {
public:
  /// Creates an empty task retained-memory lease.
  TaskMemoryLease() noexcept = default;
  /// Couples retained task bytes to the shared owner that reserved them.
  TaskMemoryLease(std::shared_ptr<void> owner, std::size_t bytes) noexcept
      : owner_(std::move(owner)),
        retained_bytes_(std::max<std::size_t>(1U, bytes)) {}

  /// Disables copying the task retained-memory lease.
  TaskMemoryLease(const TaskMemoryLease &) = delete;
  /// Disables copy assignment for the task retained-memory lease.
  TaskMemoryLease &operator=(const TaskMemoryLease &) = delete;
  /// Transfers ownership from another task retained-memory lease.
  TaskMemoryLease(TaskMemoryLease &&) noexcept = default;
  /// Transfers owned state from another task retained-memory lease.
  TaskMemoryLease &operator=(TaskMemoryLease &&) noexcept = default;

  /// Reports whether this lease currently owns a nonzero reservation.
  [[nodiscard]] explicit operator bool() const noexcept {
    return static_cast<bool>(owner_);
  }
  /// Returns task bytes reserved by this retained-memory lease.
  [[nodiscard]] std::size_t retained_bytes() const noexcept {
    return retained_bytes_;
  }

private:
  friend class OperationTaskArena;
  std::shared_ptr<void> owner_;
  std::size_t retained_bytes_ = 0U;
};

class TaskArenaSubmissionPlan final {
public:
  /// Returns the arena generation for which this plan remains valid.
  [[nodiscard]] std::uint64_t generation() const noexcept {
    return generation_;
  }
  /// Returns the first eligible worker index in the primary lane.
  [[nodiscard]] std::size_t lane_begin() const noexcept { return lane_begin_; }
  /// Returns the exclusive end of the primary worker lane.
  [[nodiscard]] std::size_t lane_end() const noexcept { return lane_end_; }
  /// Returns the primary worker lane width.
  [[nodiscard]] std::size_t width() const noexcept { return width_; }
  /// Returns the fair offset used to search alternative workers.
  [[nodiscard]] std::size_t alternative_offset() const noexcept {
    return alternative_offset_;
  }
  /// Returns the compact worker mask eligible for this submission.
  [[nodiscard]] std::uint64_t allowed_mask() const noexcept {
    return allowed_mask_;
  }
  /// Reports whether worker selection uses dynamic wide-arena bitmaps.
  [[nodiscard]] bool scalable_scan() const noexcept { return scalable_scan_; }
  /// Returns the first queue-visibility shard searched by the plan.
  [[nodiscard]] std::uint8_t visibility_shard_begin() const noexcept {
    return visibility_shard_begin_;
  }
  /// Returns the exclusive queue-visibility shard search boundary.
  [[nodiscard]] std::uint8_t visibility_shard_end() const noexcept {
    return visibility_shard_end_;
  }
  /// Returns the scheduling cursor domain advanced by this submission.
  [[nodiscard]] TaskArenaLane cursor_lane() const noexcept {
    return cursor_lane_;
  }

private:
  friend class OperationTaskArena;
  std::uint64_t generation_ = 0;
  std::size_t lane_begin_ = 0;
  std::size_t lane_end_ = 1;
  std::size_t width_ = 1;
  std::size_t alternative_offset_ = 1;
  std::uint64_t allowed_mask_ = 1;
  bool scalable_scan_ = false;
  std::uint8_t visibility_shard_begin_ = 0;
  std::uint8_t visibility_shard_end_ = 1;
  TaskArenaLane cursor_lane_ = TaskArenaLane::kAll;
};

class CompletionMemoryLease;

class OperationTaskArena final {
public:
  using Task = sanitize::internal::MoveOnlyFunction<void(
      std::size_t, sanitize::internal::StopToken)>;

  /// Creates a bounded operation arena and reserves required cleanup capacity.
  static sanitize::Result<std::shared_ptr<OperationTaskArena>>
  Make(std::size_t worker_count,
       std::shared_ptr<PerformanceTelemetry> telemetry = nullptr);

  /// Disables copying the bounded operation task arena.
  OperationTaskArena(const OperationTaskArena &) = delete;
  /// Disables copy assignment for the bounded operation task arena.
  OperationTaskArena &operator=(const OperationTaskArena &) = delete;
  /// Runs bounded shutdown before releasing shared scheduling state.
  ~OperationTaskArena() noexcept;

  /// Builds a lane plan and submits with estimated retained-memory accounting.
  sanitize::Status
  Submit(Task task, std::size_t lane_width, TaskArenaLane lane,
         TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);
  /// Builds a lane plan and submits with an explicit retained-memory charge.
  sanitize::Status
  SubmitCharged(Task task, std::size_t lane_width, TaskArenaLane lane,
                TaskMemoryCharge charge,
                TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);
  /// Builds a lane plan and transfers an existing retained-memory lease.
  sanitize::Status
  SubmitLeased(Task task, std::size_t lane_width, TaskArenaLane lane,
               TaskMemoryLease lease,
               TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);

  /// Precomputes eligible worker lanes and a fair cursor for a submission.
  [[nodiscard]] TaskArenaSubmissionPlan
  PrepareSubmissionPlan(std::size_t lane_width, TaskArenaLane lane) noexcept;

  /// Reserves a fair ticket from a prepared plan before estimated charging.
  sanitize::Status
  Submit(Task task, const TaskArenaSubmissionPlan &plan,
         TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);
  /// Uses a prepared plan and submits with an explicit retained-memory charge.
  sanitize::Status
  SubmitCharged(Task task, const TaskArenaSubmissionPlan &plan,
                TaskMemoryCharge charge,
                TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);
  /// Wraps a task with its retained-memory owner before planned submission.
  sanitize::Status
  SubmitLeased(Task task, const TaskArenaSubmissionPlan &plan,
               TaskMemoryLease lease,
               TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);

  /// Reserves the fair lane-selection ticket consumed by a planned submission.
  [[nodiscard]] std::size_t
  ReserveSubmissionTicket(const TaskArenaSubmissionPlan &plan) noexcept;

  /// Consumes a caller-reserved ticket and submits with estimated
  /// retained bytes.
  sanitize::Status
  Submit(Task task, const TaskArenaSubmissionPlan &plan,
         std::size_t submission_ticket,
         TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);
  /// Performs admission with a prepared plan, ticket, and explicit charge.
  sanitize::Status
  SubmitCharged(Task task, const TaskArenaSubmissionPlan &plan,
                std::size_t submission_ticket, TaskMemoryCharge charge,
                TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);

  /// Returns the arena's configured logical worker count.
  [[nodiscard]] std::size_t worker_count() const noexcept;
  /// Reports whether tasks execute synchronously on the submitting caller.
  [[nodiscard]] bool inline_mode() const noexcept;
  /// Returns the highest number of concurrently executing tasks observed.
  [[nodiscard]] std::size_t peak_active_tasks() const noexcept;
  /// Returns the number of tasks currently executing.
  [[nodiscard]] std::size_t active_tasks() const noexcept;
  /// Returns the cumulative number of inline and queued task submissions.
  [[nodiscard]] std::size_t submitted_tasks() const noexcept;
  /// Returns tasks executed by a worker other than their queue owner.
  [[nodiscard]] std::size_t stolen_tasks() const noexcept;
  /// Returns output preferences bypassed to preserve scheduling fairness.
  [[nodiscard]] std::size_t output_preference_bypasses() const noexcept;
  /// Returns the number of tasks currently waiting in worker queues.
  [[nodiscard]] std::size_t queued_tasks() const noexcept;
  /// Returns the highest aggregate queued-task count observed.
  [[nodiscard]] std::size_t peak_queued_tasks() const noexcept;
  /// Returns the arena's aggregate queued-task admission capacity.
  [[nodiscard]] std::size_t queue_capacity() const noexcept;
  /// Returns retained bytes currently charged to queued tasks.
  [[nodiscard]] std::size_t queued_retained_bytes() const noexcept;
  /// Returns retained bytes currently charged to executing tasks.
  [[nodiscard]] std::size_t active_retained_bytes() const noexcept;
  /// Returns queued, active, and post-shutdown retained task bytes.
  [[nodiscard]] std::size_t retained_bytes() const noexcept;
  /// Returns the queued retained-byte high-water mark.
  [[nodiscard]] std::size_t peak_queued_retained_bytes() const noexcept;
  /// Returns the active retained-byte high-water mark.
  [[nodiscard]] std::size_t peak_active_retained_bytes() const noexcept;
  /// Returns the arena's aggregate retained-byte high-water mark.
  [[nodiscard]] std::size_t peak_retained_bytes() const noexcept;
  /// Returns the retained-byte ceiling for queued and active tasks.
  [[nodiscard]] std::size_t queue_byte_capacity() const noexcept;
  /// Atomically transfers active task credit into longer-lived completion
  /// ownership without double-charging or splitting capacity validation.
  [[nodiscard]] bool TryTransferActiveToCompletion(
      std::size_t active_credit, std::size_t completion_bytes,
      CompletionMemoryLease *completion_lease) noexcept;
  /// Returns task submissions rejected because bounded queue admission failed.
  [[nodiscard]] std::size_t rejected_submissions() const noexcept;
  /// Returns submissions rejected by retained-byte admission.
  [[nodiscard]] std::size_t rejected_byte_submissions() const noexcept;
  /// Returns retained-memory waits that reached their hard timeout.
  [[nodiscard]] std::size_t backpressure_timeouts() const noexcept;
  /// Returns retained-memory waits stopped by the operation deadline.
  [[nodiscard]] std::size_t logical_backpressure_timeouts() const noexcept;
  /// Returns producers currently waiting for retained-memory admission.
  [[nodiscard]] std::size_t backpressure_waiters() const noexcept;
  /// Returns the fixed capacity of the producer backpressure ticket queue.
  [[nodiscard]] std::size_t producer_waiter_capacity() const noexcept;
  /// Returns the highest concurrent producer-waiter count observed.
  [[nodiscard]] std::size_t peak_backpressure_waiters() const noexcept;
  /// Returns producers rejected because the waiter ticket queue was full.
  [[nodiscard]] std::size_t rejected_backpressure_waiters() const noexcept;
  /// Returns smaller requests admitted ahead of blocked oversized requests.
  [[nodiscard]] std::size_t backpressure_bypasses() const noexcept;
  /// Returns admissions delayed to protect an older request from starvation.
  [[nodiscard]] std::size_t starvation_preventions() const noexcept;
  /// Returns the age of the oldest active retained-memory waiter.
  [[nodiscard]] std::uint64_t
  oldest_backpressure_waiter_age_millis() const noexcept;
  /// Returns submissions whose retained-memory estimate was unavailable.
  [[nodiscard]] std::size_t unknown_charge_submissions() const noexcept;
  /// Returns arena workers detached and still running.
  [[nodiscard]] std::size_t detached_workers() const noexcept;
  /// Returns the cumulative number of workers detached by this arena.
  [[nodiscard]] std::size_t total_detached_workers() const noexcept;
  /// Returns the age of the arena's oldest still-running detached worker.
  [[nodiscard]] std::uint64_t detached_worker_age_millis() const noexcept;
  /// Returns bounded arena shutdown waits that expired.
  [[nodiscard]] std::size_t shutdown_timeouts() const noexcept;
  /// Returns queued tasks transferred to cleanup after shutdown timeout.
  [[nodiscard]] std::size_t abandoned_queued_tasks() const noexcept;
  /// Returns retained bytes transferred with abandoned queued tasks.
  [[nodiscard]] std::size_t abandoned_queued_bytes() const noexcept;
  /// Returns arena states queued for asynchronous cleanup.
  [[nodiscard]] std::size_t reaper_queued_states() const noexcept;
  /// Returns arena states currently being drained by cleanup workers.
  [[nodiscard]] std::size_t reaper_active_states() const noexcept;
  /// Returns retained bytes waiting in cleanup queues.
  [[nodiscard]] std::size_t reaper_queued_bytes() const noexcept;
  /// Returns retained bytes held by active cleanup work.
  [[nodiscard]] std::size_t reaper_active_bytes() const noexcept;
  /// Returns retained bytes that outlive synchronous arena shutdown.
  [[nodiscard]] std::size_t post_shutdown_retained_bytes() const noexcept;
  /// Returns logical workers whose native execution threads have started.
  [[nodiscard]] std::size_t started_workers() const noexcept;
  /// Returns worker wake-epoch publications across all arena slots.
  [[nodiscard]] std::uint64_t wake_epoch_publishes() const noexcept;
  /// Returns the telemetry collector shared by this arena.
  [[nodiscard]] std::shared_ptr<PerformanceTelemetry>
  telemetry() const noexcept;
  /// Returns the operation-backed PMR resource used by arena state.
  [[nodiscard]] std::shared_ptr<std::pmr::memory_resource>
  memory_resource() const noexcept;

  /// Rejects new submissions, wakes backpressure, and requests cooperative
  /// stop while preserving bounded cleanup ownership.
  void RequestCancellation() noexcept;
  /// Sets the relative wait timeout measured from each saturation episode.
  void SetBackpressureTimeoutMillis(std::uint64_t timeout_millis) noexcept;
  /// Sets the optional absolute producer-backpressure operation deadline.
  void SetBackpressureDeadlineMillis(std::uint64_t timeout_millis) noexcept;
  /// Stops admission, drains workers within bounds, and transfers cleanup.
  void Shutdown() noexcept;
  /// Drains and stops the process cleanup reaper within the requested timeout.
  [[nodiscard]] static bool
  ShutdownCleanupReaper(std::uint64_t timeout_millis) noexcept;
  /// Returns process-wide arena, thread-permit, and reaper diagnostics.
  [[nodiscard]] static OperationTaskArenaRuntimeSnapshot
  RuntimeSnapshot() noexcept;

public:
  struct State;
  struct DetachedMetrics;

private:
  friend class CompletionMemoryLease;
  /// Adopts initialized shared arena state and detached-worker metrics.
  explicit OperationTaskArena(
      std::shared_ptr<State> state,
      std::shared_ptr<DetachedMetrics> metrics) noexcept;
  /// Validates that a prepared plan belongs to the current arena generation.
  [[nodiscard]] static bool
  ValidPlan(const State &state, const TaskArenaSubmissionPlan &plan) noexcept;

  AtomicSharedPtr<State> state_;
  std::shared_ptr<DetachedMetrics> detached_metrics_;
  std::atomic<std::size_t> shutdown_timeouts_{0};
  std::atomic<std::size_t> abandoned_queued_tasks_{0};
  std::atomic<std::size_t> abandoned_queued_bytes_{0};
};

class CompletionMemoryLease final {
public:
  /// Creates an empty completion retained-memory lease.
  CompletionMemoryLease() noexcept = default;
  /// Disables copying the completion retained-memory lease.
  CompletionMemoryLease(const CompletionMemoryLease &) = delete;
  /// Disables copy assignment for the completion retained-memory lease.
  CompletionMemoryLease &operator=(const CompletionMemoryLease &) = delete;
  /// Transfers ownership from another completion retained-memory lease.
  CompletionMemoryLease(CompletionMemoryLease &&other) noexcept;
  /// Transfers owned state from another completion retained-memory lease.
  CompletionMemoryLease &operator=(CompletionMemoryLease &&other) noexcept;
  /// Releases retained completion bytes exactly once.
  ~CompletionMemoryLease() noexcept;

  /// Reports whether this lease currently owns a nonzero reservation.
  [[nodiscard]] explicit operator bool() const noexcept {
    return static_cast<bool>(state_) && retained_bytes_ != 0U;
  }
  /// Returns bytes retained for the pending ordered completion.
  [[nodiscard]] std::size_t retained_bytes() const noexcept {
    return retained_bytes_;
  }
  /// Releases the completion's retained-memory charge exactly once.
  void reset() noexcept;

private:
  friend class OperationTaskArena;
  /// Couples completion bytes to the arena state that reserved them.
  CompletionMemoryLease(std::shared_ptr<OperationTaskArena::State> state,
                        std::size_t retained_bytes) noexcept
      : state_(std::move(state)), retained_bytes_(retained_bytes) {}

  std::shared_ptr<OperationTaskArena::State> state_;
  std::size_t retained_bytes_ = 0U;
};

} // namespace sanitize::internal
