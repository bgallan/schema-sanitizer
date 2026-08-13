// Owns the bounded native worker set shared by one public operation.
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

[[nodiscard]] std::optional<std::size_t>
process_physical_thread_count() noexcept;
[[nodiscard]] std::size_t
acquire_process_physical_thread_permits(std::size_t desired,
                                        std::size_t minimum) noexcept;
void release_process_physical_thread_permits(std::size_t amount) noexcept;

class ProcessPhysicalThreadPermitLease final {
public:
  ProcessPhysicalThreadPermitLease() noexcept = default;
  explicit ProcessPhysicalThreadPermitLease(std::size_t amount) noexcept
      : amount_(acquire_process_physical_thread_permits(amount, amount)) {}
  ProcessPhysicalThreadPermitLease(const ProcessPhysicalThreadPermitLease &) =
      delete;
  ProcessPhysicalThreadPermitLease &
  operator=(const ProcessPhysicalThreadPermitLease &) = delete;
  ProcessPhysicalThreadPermitLease(
      ProcessPhysicalThreadPermitLease &&other) noexcept
      : amount_(std::exchange(other.amount_, 0U)) {}
  ProcessPhysicalThreadPermitLease &
  operator=(ProcessPhysicalThreadPermitLease &&other) noexcept {
    if (this != &other) {
      reset();
      amount_ = std::exchange(other.amount_, 0U);
    }
    return *this;
  }
  ~ProcessPhysicalThreadPermitLease() noexcept { reset(); }

  [[nodiscard]] explicit operator bool() const noexcept {
    return amount_ != 0U;
  }
  [[nodiscard]] std::size_t amount() const noexcept { return amount_; }
  void reset() noexcept {
    if (amount_ != 0U) {
      const auto amount = std::exchange(amount_, 0U);
      release_process_physical_thread_permits(amount);
    }
  }

private:
  std::size_t amount_ = 0U;
};
// External runtime pools (PyArrow/Polars/etc.) are process-global resources,
// not pending managed thread starts. Active reservations remain a distinct
// ownership subledger; they are never treated as OS-thread identity evidence.
[[nodiscard]] std::size_t
acquire_process_external_runtime_thread_permits(std::size_t desired,
                                                std::size_t minimum) noexcept;
void release_process_external_runtime_thread_permits(
    std::size_t amount) noexcept;

class ProcessExternalRuntimeThreadPermitLease final {
public:
  ProcessExternalRuntimeThreadPermitLease() noexcept = default;
  ProcessExternalRuntimeThreadPermitLease(std::size_t desired,
                                          std::size_t minimum) noexcept
      : amount_(acquire_process_external_runtime_thread_permits(desired,
                                                                minimum)) {}
  ProcessExternalRuntimeThreadPermitLease(
      const ProcessExternalRuntimeThreadPermitLease &) = delete;
  ProcessExternalRuntimeThreadPermitLease &
  operator=(const ProcessExternalRuntimeThreadPermitLease &) = delete;
  ProcessExternalRuntimeThreadPermitLease(
      ProcessExternalRuntimeThreadPermitLease &&other) noexcept
      : amount_(std::exchange(other.amount_, 0U)) {}
  ProcessExternalRuntimeThreadPermitLease &
  operator=(ProcessExternalRuntimeThreadPermitLease &&other) noexcept {
    if (this != &other) {
      reset();
      amount_ = std::exchange(other.amount_, 0U);
    }
    return *this;
  }
  ~ProcessExternalRuntimeThreadPermitLease() noexcept { reset(); }

  [[nodiscard]] explicit operator bool() const noexcept {
    return amount_ != 0U;
  }
  [[nodiscard]] std::size_t amount() const noexcept { return amount_; }
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
  void reset() noexcept {
    const auto amount = std::exchange(amount_, 0U);
    if (amount != 0U) {
      release_process_external_runtime_thread_permits(amount);
    }
  }

private:
  std::size_t amount_ = 0U;
};
// Runtime-observed resident pool width is identity evidence only; it does not
// reserve active execution capacity. Keep it separate from operation claims.
void add_process_external_runtime_resident_threads(std::size_t amount) noexcept;
void release_process_external_runtime_resident_threads(
    std::size_t amount) noexcept;
void add_process_external_runtime_stack_debt_threads(
    std::size_t amount) noexcept;
void release_process_external_runtime_stack_debt_threads(
    std::size_t amount) noexcept;
void update_process_external_runtime_residency(
    std::int64_t identity_delta, std::int64_t stack_debt_delta) noexcept;
[[nodiscard]] std::uint64_t process_thread_stack_reservation_bytes() noexcept;
void mark_process_physical_thread_running() noexcept;
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
  constexpr TaskMemoryCharge() noexcept = default;
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
  AtomicSharedPtr() noexcept = default;
  explicit AtomicSharedPtr(std::shared_ptr<T> value) noexcept
      : value_(std::move(value)) {}

  AtomicSharedPtr(const AtomicSharedPtr &) = delete;
  AtomicSharedPtr &operator=(const AtomicSharedPtr &) = delete;

  [[nodiscard]] std::shared_ptr<T>
  load(std::memory_order order = std::memory_order_seq_cst) const noexcept {
    static_cast<void>(order);
    const std::lock_guard<std::mutex> lock(mutex_);
    return value_;
  }

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
  TaskMemoryLease() noexcept = default;
  TaskMemoryLease(std::shared_ptr<void> owner, std::size_t bytes) noexcept
      : owner_(std::move(owner)),
        retained_bytes_(std::max<std::size_t>(1U, bytes)) {}

  TaskMemoryLease(const TaskMemoryLease &) = delete;
  TaskMemoryLease &operator=(const TaskMemoryLease &) = delete;
  TaskMemoryLease(TaskMemoryLease &&) noexcept = default;
  TaskMemoryLease &operator=(TaskMemoryLease &&) noexcept = default;

  [[nodiscard]] explicit operator bool() const noexcept {
    return static_cast<bool>(owner_);
  }
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
  [[nodiscard]] std::uint64_t generation() const noexcept {
    return generation_;
  }
  [[nodiscard]] std::size_t lane_begin() const noexcept { return lane_begin_; }
  [[nodiscard]] std::size_t lane_end() const noexcept { return lane_end_; }
  [[nodiscard]] std::size_t width() const noexcept { return width_; }
  [[nodiscard]] std::size_t alternative_offset() const noexcept {
    return alternative_offset_;
  }
  [[nodiscard]] std::uint64_t allowed_mask() const noexcept {
    return allowed_mask_;
  }
  [[nodiscard]] bool scalable_scan() const noexcept { return scalable_scan_; }
  [[nodiscard]] std::uint8_t visibility_shard_begin() const noexcept {
    return visibility_shard_begin_;
  }
  [[nodiscard]] std::uint8_t visibility_shard_end() const noexcept {
    return visibility_shard_end_;
  }
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

  static sanitize::Result<std::shared_ptr<OperationTaskArena>>
  Make(std::size_t worker_count,
       std::shared_ptr<PerformanceTelemetry> telemetry = nullptr);

  OperationTaskArena(const OperationTaskArena &) = delete;
  OperationTaskArena &operator=(const OperationTaskArena &) = delete;
  ~OperationTaskArena() noexcept;

  sanitize::Status
  Submit(Task task, std::size_t lane_width, TaskArenaLane lane,
         TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);
  sanitize::Status
  SubmitCharged(Task task, std::size_t lane_width, TaskArenaLane lane,
                TaskMemoryCharge charge,
                TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);
  sanitize::Status
  SubmitLeased(Task task, std::size_t lane_width, TaskArenaLane lane,
               TaskMemoryLease lease,
               TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);

  [[nodiscard]] TaskArenaSubmissionPlan
  PrepareSubmissionPlan(std::size_t lane_width, TaskArenaLane lane) noexcept;

  sanitize::Status
  Submit(Task task, const TaskArenaSubmissionPlan &plan,
         TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);
  sanitize::Status
  SubmitCharged(Task task, const TaskArenaSubmissionPlan &plan,
                TaskMemoryCharge charge,
                TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);
  sanitize::Status
  SubmitLeased(Task task, const TaskArenaSubmissionPlan &plan,
               TaskMemoryLease lease,
               TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);

  [[nodiscard]] std::size_t
  ReserveSubmissionTicket(const TaskArenaSubmissionPlan &plan) noexcept;

  sanitize::Status
  Submit(Task task, const TaskArenaSubmissionPlan &plan,
         std::size_t submission_ticket,
         TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);
  sanitize::Status
  SubmitCharged(Task task, const TaskArenaSubmissionPlan &plan,
                std::size_t submission_ticket, TaskMemoryCharge charge,
                TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);

  [[nodiscard]] std::size_t worker_count() const noexcept;
  [[nodiscard]] bool inline_mode() const noexcept;
  [[nodiscard]] std::size_t peak_active_tasks() const noexcept;
  [[nodiscard]] std::size_t active_tasks() const noexcept;
  [[nodiscard]] std::size_t submitted_tasks() const noexcept;
  [[nodiscard]] std::size_t stolen_tasks() const noexcept;
  [[nodiscard]] std::size_t output_preference_bypasses() const noexcept;
  [[nodiscard]] std::size_t queued_tasks() const noexcept;
  [[nodiscard]] std::size_t peak_queued_tasks() const noexcept;
  [[nodiscard]] std::size_t queue_capacity() const noexcept;
  [[nodiscard]] std::size_t queued_retained_bytes() const noexcept;
  [[nodiscard]] std::size_t active_retained_bytes() const noexcept;
  [[nodiscard]] std::size_t retained_bytes() const noexcept;
  [[nodiscard]] std::size_t peak_queued_retained_bytes() const noexcept;
  [[nodiscard]] std::size_t peak_active_retained_bytes() const noexcept;
  [[nodiscard]] std::size_t peak_retained_bytes() const noexcept;
  [[nodiscard]] std::size_t queue_byte_capacity() const noexcept;
  // Completion/result ownership outlives the worker callback. These methods
  // transfer retained-byte backpressure from an active task into an external
  // completion slot without double-charging the operation memory ledger.
  // Atomically replace one currently-active task credit with completion/result
  // ownership. This is the authoritative check+commit; callers must never split
  // the capacity test from the retained-total update.
  [[nodiscard]] bool TryTransferActiveToCompletion(
      std::size_t active_credit, std::size_t completion_bytes,
      CompletionMemoryLease *completion_lease) noexcept;
  [[nodiscard]] std::size_t rejected_submissions() const noexcept;
  [[nodiscard]] std::size_t rejected_byte_submissions() const noexcept;
  [[nodiscard]] std::size_t backpressure_timeouts() const noexcept;
  [[nodiscard]] std::size_t logical_backpressure_timeouts() const noexcept;
  [[nodiscard]] std::size_t backpressure_waiters() const noexcept;
  [[nodiscard]] std::size_t producer_waiter_capacity() const noexcept;
  [[nodiscard]] std::size_t peak_backpressure_waiters() const noexcept;
  [[nodiscard]] std::size_t rejected_backpressure_waiters() const noexcept;
  [[nodiscard]] std::size_t backpressure_bypasses() const noexcept;
  [[nodiscard]] std::size_t starvation_preventions() const noexcept;
  [[nodiscard]] std::uint64_t
  oldest_backpressure_waiter_age_millis() const noexcept;
  [[nodiscard]] std::size_t unknown_charge_submissions() const noexcept;
  [[nodiscard]] std::size_t detached_workers() const noexcept;
  [[nodiscard]] std::size_t total_detached_workers() const noexcept;
  [[nodiscard]] std::uint64_t detached_worker_age_millis() const noexcept;
  [[nodiscard]] std::size_t shutdown_timeouts() const noexcept;
  [[nodiscard]] std::size_t abandoned_queued_tasks() const noexcept;
  [[nodiscard]] std::size_t abandoned_queued_bytes() const noexcept;
  [[nodiscard]] std::size_t reaper_queued_states() const noexcept;
  [[nodiscard]] std::size_t reaper_active_states() const noexcept;
  [[nodiscard]] std::size_t reaper_queued_bytes() const noexcept;
  [[nodiscard]] std::size_t reaper_active_bytes() const noexcept;
  [[nodiscard]] std::size_t post_shutdown_retained_bytes() const noexcept;
  [[nodiscard]] std::size_t started_workers() const noexcept;
  [[nodiscard]] std::uint64_t wake_epoch_publishes() const noexcept;
  [[nodiscard]] std::shared_ptr<PerformanceTelemetry>
  telemetry() const noexcept;
  [[nodiscard]] std::shared_ptr<std::pmr::memory_resource>
  memory_resource() const noexcept;

  // Cooperative operation cancellation is distinct from destructive shutdown:
  // it rejects new submissions, wakes retained-byte backpressure and requests
  // stop on active worker tokens while preserving bounded cleanup ownership.
  void RequestCancellation() noexcept;
  // Relative producer-wait timeout. This is safe for long-lived arenas because
  // it is measured from the start of each saturation episode.
  void SetBackpressureTimeoutMillis(std::uint64_t timeout_millis) noexcept;
  // Optional absolute operation deadline published by an external owner.
  void SetBackpressureDeadlineMillis(std::uint64_t timeout_millis) noexcept;
  void Shutdown() noexcept;
  [[nodiscard]] static bool
  ShutdownCleanupReaper(std::uint64_t timeout_millis) noexcept;
  [[nodiscard]] static OperationTaskArenaRuntimeSnapshot
  RuntimeSnapshot() noexcept;

public:
  struct State;
  struct DetachedMetrics;

private:
  friend class CompletionMemoryLease;
  explicit OperationTaskArena(
      std::shared_ptr<State> state,
      std::shared_ptr<DetachedMetrics> metrics) noexcept;
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
  CompletionMemoryLease() noexcept = default;
  CompletionMemoryLease(const CompletionMemoryLease &) = delete;
  CompletionMemoryLease &operator=(const CompletionMemoryLease &) = delete;
  CompletionMemoryLease(CompletionMemoryLease &&other) noexcept;
  CompletionMemoryLease &operator=(CompletionMemoryLease &&other) noexcept;
  ~CompletionMemoryLease() noexcept;

  [[nodiscard]] explicit operator bool() const noexcept {
    return static_cast<bool>(state_) && retained_bytes_ != 0U;
  }
  [[nodiscard]] std::size_t retained_bytes() const noexcept {
    return retained_bytes_;
  }
  void reset() noexcept;

private:
  friend class OperationTaskArena;
  CompletionMemoryLease(std::shared_ptr<OperationTaskArena::State> state,
                        std::size_t retained_bytes) noexcept
      : state_(std::move(state)), retained_bytes_(retained_bytes) {}

  std::shared_ptr<OperationTaskArena::State> state_;
  std::size_t retained_bytes_ = 0U;
};

} // namespace sanitize::internal
