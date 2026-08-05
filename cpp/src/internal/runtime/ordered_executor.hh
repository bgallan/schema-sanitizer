// Provides bounded inline and worker-pool execution with ordinal commit order.
#pragma once
#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/ordered_executor_completion_ring.hh"
#include "internal/runtime/thread_compat.hh"
#include "sanitize/core/status.hh"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
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
  static constexpr std::size_t kMaxExternalCompletionShards = 32U;
  static constexpr std::uint8_t kArenaTerminalCancelledBit = 1U << 0U;
  static constexpr std::uint8_t kArenaTerminalFatalBit = 1U << 1U;

  enum class ArenaSlotState : unsigned char {
    kEmpty,
    kPublishing,
    kReady,
    kCancelled,
    kFatal,
    kClosed,
  };

  struct ArenaOutcomeSlot final {
    std::atomic<ArenaSlotState> state{ArenaSlotState::kEmpty};
    std::optional<Outcome> outcome;
  };

  struct ArenaSharedState final {
    explicit ArenaSharedState(std::size_t capacity, std::size_t shards,
                              Worker worker_fn)
        : slots(capacity), worker(std::move(worker_fn)),
          shard_count(std::max<std::size_t>(1U, shards)) {}

    std::vector<ArenaOutcomeSlot> slots;
    std::mutex slots_mutex;
    Worker worker;
    sanitize::internal::StopSource stop_source;
    std::atomic<std::uint8_t> terminal_flags{0};
    const std::size_t shard_count;
    std::array<std::atomic<std::size_t>, kMaxExternalCompletionShards>
        scheduled{};
    std::array<std::atomic<std::size_t>, kMaxExternalCompletionShards>
        completed{};
    std::mutex completion_mutex;
    std::condition_variable completion_ready;
    std::atomic<bool> completion_waiter{false};
    std::atomic<bool> drain_timed_out{false};

    void Schedule(std::size_t shard) noexcept {
      scheduled[shard].fetch_add(1U, std::memory_order_relaxed);
    }

    void Finish(std::size_t shard) noexcept {
      completed[shard].fetch_add(1U, std::memory_order_release);
    }

    [[nodiscard]] bool AllScheduledFinished() const noexcept {
      std::size_t scheduled_total = 0U;
      std::size_t completed_total = 0U;
      for (std::size_t shard = 0; shard < shard_count; ++shard) {
        scheduled_total += scheduled[shard].load(std::memory_order_acquire);
        completed_total += completed[shard].load(std::memory_order_acquire);
      }
      // Shards distribute completion writes for cache locality; draining is a
      // stage-wide property. Comparing totals avoids an inconsistent
      // cross-shard snapshot while completions are being published.
      return completed_total >= scheduled_total;
    }

    [[nodiscard]] bool
    WaitUntil(std::chrono::steady_clock::time_point deadline) noexcept {
      // Completion counters are deliberately sharded and lock-free on the hot
      // path. A condition variable would require every completion to take one
      // shared mutex to prevent a lost wake-up. Poll only on bounded shutdown
      // instead, keeping normal worker publication contention-free.
      while (!AllScheduledFinished() &&
             std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
      const auto finished = AllScheduledFinished();
      if (!finished) {
        drain_timed_out.store(true, std::memory_order_release);
      }
      return finished;
    }

    Outcome ExecutePacket(Packet packet, std::size_t worker_index,
                          sanitize::internal::StopToken stop) noexcept {
      const auto ordinal = packet.ordinal;
      try {
        if (stop.stop_requested()) {
          return Outcome{.ordinal = ordinal,
                         .result = sanitize::Status::Cancelled(
                             "ordered worker stop requested")};
        }
        return Outcome{
            .ordinal = ordinal,
            .result = worker(std::move(packet.payload), worker_index, stop)};
      } catch (const std::bad_alloc &) {
        return Outcome{.ordinal = ordinal,
                       .result = sanitize::Status::OutOfMemory(
                           "ordered worker allocation failed")};
      } catch (const std::exception &error) {
        return Outcome{.ordinal = ordinal,
                       .result = sanitize::Status::Invalid(
                           "ordered worker raised: ", error.what())};
      } catch (...) {
        return Outcome{.ordinal = ordinal,
                       .result = sanitize::Status::Invalid(
                           "ordered worker raised an unknown exception")};
      }
    }

    void Terminalize(ArenaSlotState terminal) noexcept {
      std::lock_guard outcomes_lock(slots_mutex);
      for (auto &slot : slots) {
        auto state = slot.state.load(std::memory_order_acquire);
        for (;;) {
          if (state == ArenaSlotState::kPublishing ||
              state == ArenaSlotState::kCancelled ||
              state == ArenaSlotState::kFatal) {
            break;
          }
          if (state == ArenaSlotState::kReady) {
            slot.outcome.reset();
            slot.state.store(terminal, std::memory_order_release);
            slot.state.notify_all();
            break;
          }
          if (slot.state.compare_exchange_weak(state, terminal,
                                               std::memory_order_release,
                                               std::memory_order_acquire)) {
            slot.state.notify_all();
            break;
          }
        }
      }
    }

    void Fail() noexcept {
      terminal_flags.fetch_or(kArenaTerminalFatalBit,
                              std::memory_order_release);
      Terminalize(ArenaSlotState::kFatal);
    }

    void Cancel() noexcept {
      stop_source.request_stop();
      terminal_flags.fetch_or(kArenaTerminalCancelledBit,
                              std::memory_order_release);
      Terminalize(ArenaSlotState::kCancelled);
    }

    void Publish(Outcome outcome, std::size_t completion_slot) noexcept {
      auto &slot = slots[completion_slot];
      auto expected = ArenaSlotState::kEmpty;
      if (!slot.state.compare_exchange_strong(
              expected, ArenaSlotState::kPublishing, std::memory_order_acquire,
              std::memory_order_acquire)) {
        if (expected == ArenaSlotState::kCancelled ||
            expected == ArenaSlotState::kFatal ||
            expected == ArenaSlotState::kClosed) {
          return;
        }
        Fail();
        return;
      }

      auto published_state = ArenaSlotState::kReady;
      bool publication_failed = false;
      {
        std::lock_guard outcomes_lock(slots_mutex);
        try {
          slot.outcome.emplace(std::move(outcome));
        } catch (...) {
          publication_failed = true;
        }
        if (!publication_failed) {
          const auto flags = terminal_flags.load(std::memory_order_acquire);
          if ((flags & kArenaTerminalCancelledBit) != 0U) {
            slot.outcome.reset();
            published_state = ArenaSlotState::kCancelled;
          } else if ((flags & kArenaTerminalFatalBit) != 0U) {
            slot.outcome.reset();
            published_state = ArenaSlotState::kFatal;
          }
        }
      }
      if (publication_failed) {
        slot.state.store(ArenaSlotState::kFatal, std::memory_order_release);
        slot.state.notify_all();
        Fail();
        return;
      }
      slot.state.store(published_state, std::memory_order_release);
      slot.state.notify_one();
    }

    void ExecuteExternal(ScheduledPacket scheduled_packet,
                         std::size_t worker_index,
                         sanitize::internal::StopToken arena_stop) noexcept {
      Outcome outcome{.ordinal = scheduled_packet.packet.ordinal,
                      .result = sanitize::Status::Cancelled(
                          "ordered worker stop requested")};
      {
        auto propagate_stop = [this] { stop_source.request_stop(); };
        StopCallback<decltype(propagate_stop)> propagate_arena_stop(
            arena_stop, std::move(propagate_stop));
        outcome = ExecutePacket(std::move(scheduled_packet.packet),
                                worker_index, stop_source.get_token());
      }
      Publish(std::move(outcome), scheduled_packet.completion_slot);
    }
  };

  class ExternalLease final {
  public:
    ExternalLease(std::shared_ptr<ArenaSharedState> owner,
                  std::size_t shard) noexcept
        : owner_(std::move(owner)), shard_(shard) {}
    ExternalLease(const ExternalLease &) = delete;
    ExternalLease &operator=(const ExternalLease &) = delete;
    ExternalLease(ExternalLease &&other) noexcept
        : owner_(std::move(other.owner_)), shard_(other.shard_) {}
    ExternalLease &operator=(ExternalLease &&) = delete;
    ~ExternalLease() { Complete(); }

    [[nodiscard]] std::size_t shard() const noexcept { return shard_; }
    void Complete() noexcept {
      if (owner_) {
        owner_->Finish(shard_);
        owner_.reset();
      }
    }

  private:
    std::shared_ptr<ArenaSharedState> owner_;
    std::size_t shard_ = 0;
  };

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
    return SubmitCharged(std::move(packet),
                         std::max<std::size_t>(256U, sizeof(Packet)));
  }

  sanitize::Status SubmitCharged(Packet packet, std::size_t retained_bytes) {
    retained_bytes = std::max<std::size_t>(1U, retained_bytes);
    // Above eight arena workers, reserve directly at the authoritative
    // submission point. The legacy preliminary check would acquire the same
    // executor mutex twice per packet and becomes visible under high-core
    // submission pressure. One-through-eight workers keep the v47 path.
    if (worker_count_ > 8 && uses_arena_completion_slots()) {
      return submit_high_core_arena(std::move(packet), retained_bytes);
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
        arena_shared_->Schedule(completion_shard);
      }
      auto shared = arena_shared_;
      const auto submit_status = arena_->SubmitCharged(
          [shared,
           scheduled = ScheduledPacket{.packet = std::move(packet),
                                       .completion_slot = completion_slot},
           lease = ExternalLease(shared, completion_shard)](
              std::size_t worker_index,
              sanitize::internal::StopToken stop) mutable {
            shared->ExecuteExternal(std::move(scheduled), worker_index, stop);
            lease.Complete();
          },
          arena_submission_plan_, TaskMemoryCharge{retained_bytes},
          telemetry_kind_);
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
    if (arena_shared_) {
      arena_shared_->stop_source.request_stop();
    }
    {
      std::lock_guard lock(mutex_);
      if (cancelled_) {
        return;
      }
      cancelled_ = true;
      accepting_ = false;
      if (arena_shared_) {
        arena_shared_->terminal_flags.fetch_or(kArenaTerminalCancelledBit,
                                               std::memory_order_release);
      }
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

  [[nodiscard]] bool external_drain_timed_out() const noexcept {
    return arena_shared_ &&
           arena_shared_->drain_timed_out.load(std::memory_order_acquire);
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
      if (!arena_->inline_mode()) {
        arena_shared_ = std::make_shared<ArenaSharedState>(
            reorder_capacity_, external_completion_shard_count_, worker_);
      }
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
    if (arena_shared_) {
      // External arena tasks own only this shared control block. A non-
      // cooperative task can no longer pin or dereference the executor object
      // after the bounded drain deadline.
      (void)arena_shared_->WaitUntil(std::chrono::steady_clock::now() +
                                     std::chrono::seconds(2));
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
  std::shared_ptr<ArenaSharedState> arena_shared_;
  const std::size_t external_completion_shard_count_;
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
  bool accepting_ = true;
  bool cancelled_ = false;
  bool fatal_ = false;
};

} // namespace sanitize::internal
