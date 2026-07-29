// Provides bounded inline and worker-pool execution with ordinal commit order.
#pragma once
#include "internal/runtime/external_task_lease.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/ordered_executor_completion_ring.hh"
#include "internal/runtime/thread_compat.hh"
#include "sanitize/core/status.hh"
#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>
namespace sanitize::internal {

// Owns one immutable work item and its canonical source-order ordinal.
template <class Payload> struct OrdinalPacket {
  std::uint64_t ordinal = 0;
  Payload payload;
};

// Owns one completed work result tagged with its original ordinal.
template <class Value> struct OrdinalOutcome {
  std::uint64_t ordinal = 0;
  sanitize::Result<Value> result;
};

// Executes ordinal packets inline or on a bounded pool and exposes them only in
// source order. The worker callable must not mutate coordinator-owned state.
template <class Input, class Output> class OrderedExecutor final {
public:
  using Packet = OrdinalPacket<Input>;
  using Outcome = OrdinalOutcome<Output>;
  using ScheduledPacket = ScheduledOrdinalPacket<Packet>;
  using Worker = std::function<sanitize::Result<Output>(
      Input &&, std::size_t, sanitize::internal::StopToken)>;

private:
  void abandon_external_task(std::size_t shard) noexcept {
    finish_external_task(shard);
  }
  using ExternalLease =
      ExternalTaskLease<OrderedExecutor,
                        &OrderedExecutor::abandon_external_task>;

  static constexpr std::size_t kMaxExternalCompletionShards = 32U;
  struct alignas(64) ExternalCompletionShard final {
    // The high bit announces that shutdown is waiting on this shard. Keeping
    // the waiter flag in the same atomic as the completed count gives one total
    // modification order: either shutdown observes a preceding completion, or
    // the completing task observes the waiter bit and performs the wake.
    std::atomic<std::size_t> completed_and_waiter{0};
  };
  static constexpr std::size_t kExternalCompletionWaiterBit =
      std::size_t{1} << (sizeof(std::size_t) * 8U - 1U);
  static constexpr std::size_t kExternalCompletionCountMask =
      ~kExternalCompletionWaiterBit;
  static constexpr std::uint8_t kArenaTerminalCancelledBit = 1U << 0U;
  static constexpr std::uint8_t kArenaTerminalFatalBit = 1U << 1U;

public:
  // One worker is strictly inline and creates no helper thread or process.
  static sanitize::Result<std::unique_ptr<OrderedExecutor>>
  Make(std::size_t worker_count, std::size_t task_queue_capacity,
       std::size_t reorder_capacity, Worker worker,
       std::shared_ptr<OperationTaskArena> arena = nullptr,
       TaskArenaLane lane = TaskArenaLane::kAll,
       TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther) {
    if (!worker) {
      return sanitize::Status::Invalid(
          "OrderedExecutor::Make: worker is empty");
    }
    const auto normalized_workers = std::max<std::size_t>(1, worker_count);
    auto executor =
        std::unique_ptr<OrderedExecutor>(new (std::nothrow) OrderedExecutor(
            normalized_workers, task_queue_capacity, reorder_capacity,
            std::move(worker), std::move(arena), lane, telemetry_kind));
    if (!executor) {
      return sanitize::Status::OutOfMemory(
          "OrderedExecutor::Make: allocation failed");
    }
    const auto status = executor->start_workers();
    if (!status.ok()) {
      return status;
    }
    return executor;
  }

  OrderedExecutor(const OrderedExecutor &) = delete;
  OrderedExecutor &operator=(const OrderedExecutor &) = delete;

  ~OrderedExecutor() { shutdown(); }
  // Submit contiguous ordinals; consume once the dispatch window is full.
  sanitize::Status Submit(Packet packet) {
    // Above eight arena workers, reserve directly at the authoritative
    // submission point. The legacy preliminary check would acquire the same
    // executor mutex twice per packet and becomes visible under high-core
    // submission pressure. One-through-eight workers keep the v47 path.
    if (worker_count_ > 8 && uses_arena_completion_slots()) {
      return submit_high_core_arena(std::move(packet));
    }
    {
      std::lock_guard lock(mutex_);
      if (packet.ordinal != next_submit_ordinal_) {
        return sanitize::Status::Invalid(
            "OrderedExecutor::Submit: expected ordinal ", next_submit_ordinal_,
            ", received ", packet.ordinal);
      }
      if (fatal_) {
        return sanitize::Status::OutOfMemory(
            "OrderedExecutor::Submit: executor result buffer failed");
      }
      if (cancelled_ || !accepting_) {
        return sanitize::Status::Cancelled(
            "OrderedExecutor::Submit: executor is closed");
      }
      if (in_flight_locked() >= dispatch_window()) {
        return sanitize::Status::Invalid(
            "OrderedExecutor::Submit: dispatch window is full; consume the "
            "next ordinal before submitting more work");
      }
    }

    if (inline_mode()) {
      std::size_t completion_slot = 0;
      {
        std::lock_guard lock(mutex_);
        if (fatal_) {
          return sanitize::Status::OutOfMemory(
              "OrderedExecutor::Submit: executor result buffer failed");
        }
        if (cancelled_ || !accepting_) {
          return sanitize::Status::Cancelled(
              "OrderedExecutor::Submit: executor is closed");
        }
        if (packet.ordinal != next_submit_ordinal_) {
          return sanitize::Status::Invalid(
              "OrderedExecutor::Submit: expected ordinal ",
              next_submit_ordinal_, ", received ", packet.ordinal);
        }
        completion_slot = completion_ring_.ReserveSubmit();
        ++next_submit_ordinal_;
        in_flight_.fetch_add(1, std::memory_order_release);
      }
      auto outcome = execute_packet(std::move(packet), 0, {});
      std::lock_guard lock(mutex_);
      if (cancelled_) {
        return sanitize::Status::Cancelled(
            "OrderedExecutor::Submit: executor was cancelled");
      }
      if (!store_outcome_locked(std::move(outcome), completion_slot)) {
        fatal_ = true;
        accepting_ = false;
        return sanitize::Status::OutOfMemory(
            "OrderedExecutor::Submit: result buffer failed");
      }
      result_ready_.notify_all();
      return sanitize::Status::OK();
    }

    if (arena_) {
      std::size_t completion_slot = 0;
      std::size_t completion_shard = 0;
      {
        std::lock_guard lock(mutex_);
        if (fatal_) {
          return sanitize::Status::OutOfMemory(
              "OrderedExecutor::Submit: executor result buffer failed");
        }
        if (cancelled_ || !accepting_) {
          return sanitize::Status::Cancelled(
              "OrderedExecutor::Submit: executor is closed");
        }
        if (packet.ordinal != next_submit_ordinal_) {
          return sanitize::Status::Invalid(
              "OrderedExecutor::Submit: expected ordinal ",
              next_submit_ordinal_, ", received ", packet.ordinal);
        }
        completion_slot = completion_ring_.ReserveSubmit();
        completion_shard = reserve_external_completion_shard_locked();
        ++next_submit_ordinal_;
        in_flight_.fetch_add(1, std::memory_order_release);
        ++scheduled_external_tasks_[completion_shard];
      }
      const auto submit_status = arena_->Submit(
          [this,
           scheduled = ScheduledPacket{.packet = std::move(packet),
                                       .completion_slot = completion_slot},
           lease = ExternalLease(this, completion_shard)](
              std::size_t worker_index,
              sanitize::internal::StopToken stop) mutable {
            execute_external(std::move(scheduled), worker_index, stop,
                             lease.shard());
            lease.Complete();
          },
          arena_submission_plan_, telemetry_kind_);
      if (!submit_status.ok()) {
        std::lock_guard lock(mutex_);
        --next_submit_ordinal_;
        completion_ring_.RollbackSubmit();
        in_flight_.fetch_sub(1, std::memory_order_release);
        return submit_status;
      }
      return sanitize::Status::OK();
    }

    std::unique_lock lock(mutex_);
    task_space_.wait(lock, [&] {
      return cancelled_ || fatal_ || !accepting_ ||
             tasks_.size() < task_queue_capacity_;
    });
    if (fatal_) {
      return sanitize::Status::OutOfMemory(
          "OrderedExecutor::Submit: executor result buffer failed");
    }
    if (cancelled_ || !accepting_) {
      return sanitize::Status::Cancelled(
          "OrderedExecutor::Submit: executor is closed");
    }
    if (packet.ordinal != next_submit_ordinal_) {
      return sanitize::Status::Invalid(
          "OrderedExecutor::Submit: expected ordinal ", next_submit_ordinal_,
          ", received ", packet.ordinal);
    }
    ScheduledPacket scheduled{.packet = std::move(packet),
                              .completion_slot =
                                  completion_ring_.ReserveSubmit()};
    try {
      tasks_.push_back(std::move(scheduled));
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "OrderedExecutor::Submit: task queue allocation failed");
    }
    ++next_submit_ordinal_;
    in_flight_.fetch_add(1, std::memory_order_release);
    lock.unlock();
    task_ready_.notify_one();
    return sanitize::Status::OK();
  }
  // Stop accepting after all work has been queued.
  sanitize::Status FinishSubmission() {
    bool close_waiting_arena_slot = false;
    std::uint64_t close_ordinal = 0;
    {
      std::lock_guard lock(mutex_);
      if (fatal_) {
        return sanitize::Status::OutOfMemory(
            "OrderedExecutor::FinishSubmission: executor result buffer "
            "failed");
      }
      if (cancelled_) {
        return sanitize::Status::Cancelled(
            "OrderedExecutor::FinishSubmission: executor was cancelled");
      }
      accepting_ = false;
      close_waiting_arena_slot =
          uses_arena_completion_slots() && in_flight_locked() == 0U;
      close_ordinal = next_take_ordinal_;
    }
    if (close_waiting_arena_slot) {
      close_empty_arena_slot(close_ordinal);
    }
    task_ready_.notify_all();
    task_space_.notify_all();
    result_ready_.notify_all();
    return sanitize::Status::OK();
  }
  // Return the next ordinal; failures remain ordered behind earlier results.
  sanitize::Result<Outcome> TakeNext() {
    if (uses_arena_completion_slots()) {
      if (worker_count_ > 8U) {
        return take_next_arena<true>();
      }
      return take_next_arena<false>();
    }
    std::unique_lock lock(mutex_);
    result_ready_.wait(lock, [&] {
      return fatal_ || cancelled_ || next_outcome_ready_locked() ||
             (!accepting_ && in_flight_locked() == 0);
    });
    if (fatal_) {
      return sanitize::Status::OutOfMemory(
          "OrderedExecutor::TakeNext: executor result buffer failed");
    }
    if (cancelled_) {
      return sanitize::Status::Cancelled(
          "OrderedExecutor::TakeNext: executor was cancelled");
    }
    if (!next_outcome_ready_locked()) {
      return sanitize::Status::Invalid(
          "OrderedExecutor::TakeNext: no completed packets remain");
    }

    auto &slot = completed_[completion_ring_.NextTake()];
    Outcome outcome = std::move(*slot);
    slot.reset();
    ++next_take_ordinal_;
    completion_ring_.AdvanceTake();
    in_flight_.fetch_sub(1, std::memory_order_release);
    lock.unlock();
    return outcome;
  }
  // Cancels queued/later work and requests cooperative stop from active
  // workers.
  void Cancel() noexcept {
    stage_stop_source_.request_stop();
    {
      std::lock_guard lock(mutex_);
      if (cancelled_) {
        return;
      }
      cancelled_ = true;
      accepting_ = false;
      arena_terminal_flags_.fetch_or(kArenaTerminalCancelledBit,
                                     std::memory_order_release);
      tasks_.clear();
      for (auto &slot : completed_) {
        slot.reset();
      }
      cancel_arena_slots_locked();
      in_flight_.store(0, std::memory_order_release);
    }
    for (auto &thread : workers_) {
      thread.request_stop();
    }
    notify_all();
  }
  // True only for the strict caller-thread path.
  [[nodiscard]] bool inline_mode() const noexcept {
    return worker_count_ == 1 && (!arena_ || arena_->inline_mode());
  }

  [[nodiscard]] std::size_t worker_count() const noexcept {
    return worker_count_;
  }

  [[nodiscard]] std::size_t dispatch_window() const noexcept {
    return reorder_capacity_;
  }

  [[nodiscard]] std::size_t in_flight() const noexcept {
    return in_flight_.load(std::memory_order_acquire);
  }

private:
  [[nodiscard]] std::size_t in_flight_locked() const noexcept {
    // All internal decisions using this helper own mutex_. That mutex orders
    // every in-flight writer, so an acquire barrier is redundant. Public
    // observers keep the acquire snapshot in in_flight(), paired with release
    // publication by admission and consumption.
    return in_flight_.load(std::memory_order_relaxed);
  }

  // Above eight workers, Submit already owns the executor mutex in one
  // authoritative coordinator transaction. All in-flight writers are therefore
  // serialized, so publishing the increment with load+store avoids a locked
  // atomic RMW while preserving the lock-free acquire snapshot used by callers.
  void increment_high_core_in_flight_locked() noexcept {
    const auto current = in_flight_.load(std::memory_order_relaxed);
    in_flight_.store(current + 1U, std::memory_order_release);
  }

  // The high-core arena consumer also owns mutex_ while advancing the ordered
  // completion cursor. Every writer of in_flight_ is serialized by that mutex,
  // so the >8-worker path can publish the decrement with a single store instead
  // of a locked atomic read-modify-write. Smaller executors retain the
  // historical fetch_sub path because their short stages did not amortize the
  // extra branch in earlier probes.
  void decrement_high_core_in_flight_locked() noexcept {
    const auto current = in_flight_.load(std::memory_order_relaxed);
    in_flight_.store(current - 1U, std::memory_order_release);
  }

#include "internal/runtime/ordered_executor_arena_completion.cc.inc"
  OrderedExecutor(std::size_t worker_count, std::size_t task_queue_capacity,
                  std::size_t reorder_capacity, Worker worker,
                  std::shared_ptr<OperationTaskArena> arena, TaskArenaLane lane,
                  TaskTelemetryKind telemetry_kind)
      : worker_count_(worker_count),
        task_queue_capacity_(std::max<std::size_t>(1, task_queue_capacity)),
        reorder_capacity_(std::max<std::size_t>(1, reorder_capacity)),
        completion_ring_(reorder_capacity_), worker_(std::move(worker)),
        completed_((!arena || arena->inline_mode()) ? reorder_capacity_ : 0U),
        arena_(std::move(arena)),
        arena_completed_(uses_arena_completion_slots() ? reorder_capacity_
                                                       : 0U),
        external_completion_shard_count_(
            uses_arena_completion_slots() && worker_count_ >= 4U
                ? std::min(worker_count_, kMaxExternalCompletionShards)
                : 1U),
        lane_(lane), telemetry_kind_(telemetry_kind) {
    if (arena_) {
      arena_submission_plan_ =
          arena_->PrepareSubmissionPlan(worker_count_, lane_);
      // The >8-worker submission helper already owns a single coordinator
      // transaction. Seed its local lane ticket once instead of touching the
      // arena-global cursor for every high-core packet.
      if (!arena_->inline_mode() && worker_count_ > 8U) {
        next_high_core_arena_ticket_ =
            arena_->ReserveSubmissionTicket(arena_submission_plan_);
      }
    }
  }
#include "internal/runtime/ordered_executor_workers.cc.inc"

#include "internal/runtime/ordered_executor_execution.cc.inc"
#include "internal/runtime/ordered_executor_submission.cc.inc"
  [[nodiscard]] std::size_t
  reserve_external_completion_shard_locked() noexcept {
    const auto shard = next_external_completion_shard_;
    ++next_external_completion_shard_;
    if (next_external_completion_shard_ == external_completion_shard_count_) {
      next_external_completion_shard_ = 0U;
    }
    return shard;
  }

  void finish_external_task(std::size_t shard) noexcept {
    auto &counter = completed_external_tasks_[shard].completed_and_waiter;
    const auto previous = counter.fetch_add(1, std::memory_order_release);
    // Normal execution has no waiter: shutdown is the only consumer of these
    // lifetime counters. Avoid a futile atomic notify for every external task,
    // but preserve a lost-wakeup-proof drain by testing the waiter bit returned
    // by the same RMW that publishes completion.
    if ((previous & kExternalCompletionWaiterBit) != 0U) {
      counter.notify_all();
    }
  }

  void worker_loop(std::size_t worker_index,
                   sanitize::internal::StopToken stop) noexcept {
    try {
      while (!stop.stop_requested()) {
        std::optional<ScheduledPacket> scheduled;
        {
          std::unique_lock lock(mutex_);
          task_ready_.wait(lock, [&] {
            return stop.stop_requested() || cancelled_ || fatal_ ||
                   !tasks_.empty() || !accepting_;
          });
          if (stop.stop_requested() || cancelled_ || fatal_) {
            return;
          }
          if (tasks_.empty()) {
            if (!accepting_) {
              return;
            }
            continue;
          }
          scheduled.emplace(std::move(tasks_.front()));
          tasks_.pop_front();
        }
        task_space_.notify_all();

        auto outcome =
            execute_packet(std::move(scheduled->packet), worker_index, stop);
        std::unique_lock lock(mutex_);
        if (stop.stop_requested() || cancelled_ || fatal_) {
          return;
        }
        if (!store_outcome_locked(std::move(outcome),
                                  scheduled->completion_slot)) {
          fatal_ = true;
          accepting_ = false;
          tasks_.clear();
          lock.unlock();
          notify_all();
          return;
        }
        const auto next_result_ready = next_outcome_ready_locked();
        lock.unlock();
        if (next_result_ready) {
          result_ready_.notify_one();
        }
      }
    } catch (...) {
      {
        std::lock_guard lock(mutex_);
        fatal_ = true;
        accepting_ = false;
        tasks_.clear();
      }
      notify_all();
    }
  }
  [[nodiscard]] std::size_t slot_index(std::uint64_t ordinal) const noexcept {
    return static_cast<std::size_t>(ordinal % reorder_capacity_);
  }
  [[nodiscard]] bool uses_arena_completion_slots() const noexcept {
    return arena_ && !arena_->inline_mode();
  }
  [[nodiscard]] bool next_outcome_ready_locked() const noexcept {
    const auto &slot = completed_[completion_ring_.NextTake()];
    return slot && slot->ordinal == next_take_ordinal_;
  }
  bool store_outcome_locked(Outcome outcome,
                            std::size_t completion_slot) noexcept {
    auto &slot = completed_[completion_slot];
    if (slot) {
      return false;
    }
    try {
      slot.emplace(std::move(outcome));
      return true;
    } catch (...) {
      return false;
    }
  }
  void notify_all() noexcept {
    task_ready_.notify_all();
    task_space_.notify_all();
    result_ready_.notify_all();
  }
  void shutdown() noexcept {
    Cancel();
    if (arena_) {
      std::array<std::size_t, kMaxExternalCompletionShards> scheduled{};
      {
        std::lock_guard lock(mutex_);
        scheduled = scheduled_external_tasks_;
      }
      for (std::size_t shard = 0; shard < external_completion_shard_count_;
           ++shard) {
        auto &counter = completed_external_tasks_[shard].completed_and_waiter;
        auto observed = counter.fetch_or(kExternalCompletionWaiterBit,
                                         std::memory_order_acq_rel);
        auto completed = observed & kExternalCompletionCountMask;
        while (completed != scheduled[shard]) {
          const auto waiting_value = completed | kExternalCompletionWaiterBit;
          sanitize::internal::WaitOnAtomic(counter, waiting_value,
                                           std::memory_order_acquire);
          observed = counter.load(std::memory_order_acquire);
          completed = observed & kExternalCompletionCountMask;
        }
      }
    }
    workers_.clear();
  }
  const std::size_t worker_count_;
  const std::size_t task_queue_capacity_;
  const std::size_t reorder_capacity_;
  CompletionRingCursor completion_ring_;
  Worker worker_;
  mutable std::mutex mutex_;
  std::condition_variable task_ready_;
  std::condition_variable task_space_;
  std::condition_variable result_ready_;
  std::mutex take_mutex_;
  std::deque<ScheduledPacket> tasks_;
  std::vector<std::optional<Outcome>> completed_;
  std::vector<sanitize::internal::JThread> workers_;
  std::shared_ptr<OperationTaskArena> arena_;
  std::vector<ArenaOutcomeSlot> arena_completed_;
  const std::size_t external_completion_shard_count_;
  std::array<ExternalCompletionShard, kMaxExternalCompletionShards>
      completed_external_tasks_{};
  std::array<std::size_t, kMaxExternalCompletionShards>
      scheduled_external_tasks_{};
  sanitize::internal::StopSource stage_stop_source_;
  TaskArenaLane lane_ = TaskArenaLane::kAll;
  TaskTelemetryKind telemetry_kind_ = TaskTelemetryKind::kOther;
  TaskArenaSubmissionPlan arena_submission_plan_;
  std::size_t next_high_core_arena_ticket_ = 0;
  std::uint64_t next_submit_ordinal_ = 0;
  std::uint64_t next_take_ordinal_ = 0;
  std::size_t next_external_completion_shard_ = 0;
  std::atomic<std::size_t> in_flight_{0};
  // Monotonic cancellation/fatality bits share one cache location so normal
  // result publication needs one acquire snapshot rather than two loads.
  std::atomic<std::uint8_t> arena_terminal_flags_{0};
  bool accepting_ = true;
  bool cancelled_ = false;
  bool fatal_ = false;
};

} // namespace sanitize::internal
