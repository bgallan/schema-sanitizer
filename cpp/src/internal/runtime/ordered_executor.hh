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
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
namespace sanitize::internal {

// Estimate queue-retained ownership without touching the operation memory
// ledger.  These bytes are a backpressure signal only: buffers may already be
// physically charged by an OperationMemoryLedger/PMR resource and charging
// them again here would double-account resident memory.
template <class Value>
[[nodiscard]] constexpr std::size_t
KnownRetainedByteValue(const Value &value) noexcept {
  using T = std::remove_cvref_t<Value>;
  if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
    if constexpr (std::is_signed_v<T>) {
      if (value <= 0) {
        return 0U;
      }
    }
    return static_cast<std::size_t>(value);
  } else {
    return 0U;
  }
}

template <class Value>
[[nodiscard]] std::size_t
EstimateQueueRetainedBytes(const Value &value) noexcept;

[[nodiscard]] constexpr std::size_t
SaturatingRetainedAdd(std::size_t left, std::size_t right) noexcept {
  return right > std::numeric_limits<std::size_t>::max() - left
             ? std::numeric_limits<std::size_t>::max()
             : left + right;
}

template <class Value>
[[nodiscard]] std::size_t
AdditionalInlineOwnedBytes(const Value &value) noexcept {
  const auto total = EstimateQueueRetainedBytes(value);
  return total > sizeof(Value) ? total - sizeof(Value) : 0U;
}

template <class Value>
[[nodiscard]] std::size_t
EstimateQueueRetainedBytes(const Value &value) noexcept {
  // ``estimated_retained_bytes`` is an optional whole-graph estimate. Source
  // and output hints represent simultaneously-owned regions, so combine those
  // additively (with reserved/estimated output treated as alternatives). This
  // reserves output headroom before worker execution rather than waiting until
  // a completion has already materialized.
  std::size_t whole_graph_hint = sizeof(Value);
  if constexpr (requires { value.estimated_retained_bytes; }) {
    whole_graph_hint =
        std::max(whole_graph_hint,
                 KnownRetainedByteValue(value.estimated_retained_bytes));
  }
  std::size_t source_hint = 0U;
  if constexpr (requires { value.estimated_source_bytes; }) {
    source_hint = KnownRetainedByteValue(value.estimated_source_bytes);
  }
  std::size_t output_hint = 0U;
  if constexpr (requires { value.estimated_output_bytes; }) {
    output_hint = KnownRetainedByteValue(value.estimated_output_bytes);
  }
  if constexpr (requires { value.reserved_output_bytes; }) {
    output_hint = std::max(output_hint,
                           KnownRetainedByteValue(value.reserved_output_bytes));
  }
  const auto hinted = std::max(whole_graph_hint,
                               SaturatingRetainedAdd(source_hint, output_hint));

  std::size_t structural = sizeof(Value);
  if constexpr (requires { value.owned; }) {
    structural = SaturatingRetainedAdd(structural,
                                       AdditionalInlineOwnedBytes(value.owned));
  }
  if constexpr (requires { value.payload; }) {
    structural = SaturatingRetainedAdd(
        structural, AdditionalInlineOwnedBytes(value.payload));
  }
  if constexpr (requires {
                  value.result.ok();
                  *value.result;
                }) {
    if (value.result.ok()) {
      structural = SaturatingRetainedAdd(
          structural, EstimateQueueRetainedBytes(*value.result));
    }
  }
  if constexpr (requires {
                  value.get();
                  *value;
                }) {
    if (value) {
      // A pointer-like owner keeps the pointed object outside its own inline
      // representation, so add the complete pointee estimate.
      structural =
          SaturatingRetainedAdd(structural, EstimateQueueRetainedBytes(*value));
    }
  }
  if constexpr (requires {
                  value.capacity();
                  value.size();
                  typename std::remove_cvref_t<Value>::value_type;
                }) {
    using Element = typename std::remove_cvref_t<Value>::value_type;
    const auto capacity = static_cast<std::size_t>(value.capacity());
    const auto element_size = std::max<std::size_t>(1U, sizeof(Element));
    const auto storage =
        capacity > std::numeric_limits<std::size_t>::max() / element_size
            ? std::numeric_limits<std::size_t>::max()
            : capacity * element_size;
    structural = SaturatingRetainedAdd(structural, storage);
    if constexpr (!std::is_trivially_destructible_v<Element>) {
      for (const auto &element : value) {
        structural = SaturatingRetainedAdd(structural,
                                           AdditionalInlineOwnedBytes(element));
      }
    }
  }
  if constexpr (requires { value.cells; }) {
    structural = SaturatingRetainedAdd(structural,
                                       AdditionalInlineOwnedBytes(value.cells));
  }
  if constexpr (requires { value.rows; }) {
    structural = SaturatingRetainedAdd(structural,
                                       AdditionalInlineOwnedBytes(value.rows));
  }
  if constexpr (requires { value.nodes; }) {
    structural = SaturatingRetainedAdd(structural,
                                       AdditionalInlineOwnedBytes(value.nodes));
  }
  if constexpr (requires {
                  static_cast<bool>(value.partitioned);
                  *value.partitioned;
                }) {
    if (value.partitioned) {
      structural = SaturatingRetainedAdd(
          structural, EstimateQueueRetainedBytes(*value.partitioned));
    }
  }
  return std::max<std::size_t>(256U, std::max(hinted, structural));
}

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
    CompletionMemoryLease retained_lease;
  };

  struct ArenaSharedState final {
    explicit ArenaSharedState(std::size_t capacity, std::size_t shards,
                              Worker worker_fn,
                              std::shared_ptr<OperationTaskArena> arena_owner)
        : slots(capacity), worker(std::move(worker_fn)),
          arena(std::move(arena_owner)),
          shard_count(std::max<std::size_t>(1U, shards)) {}

    std::vector<ArenaOutcomeSlot> slots;
    std::mutex slots_mutex;
    Worker worker;
    std::shared_ptr<OperationTaskArena> arena;
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
      // Shutdown is rare. Keep the normal completion path lock-free unless a
      // bounded drain waiter has explicitly armed itself. Taking the mutex only
      // in that state closes the condition-variable lost-wakeup window without
      // adding shared-lock contention to steady-state publication.
      if (completion_waiter.load(std::memory_order_acquire)) {
        std::lock_guard lock(completion_mutex);
        completion_ready.notify_all();
      }
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
      if (AllScheduledFinished()) {
        return true;
      }
      std::unique_lock lock(completion_mutex);
      completion_waiter.store(true, std::memory_order_release);
      while (!AllScheduledFinished()) {
        if (completion_ready.wait_until(lock, deadline) ==
            std::cv_status::timeout) {
          break;
        }
      }
      const auto finished = AllScheduledFinished();
      completion_waiter.store(false, std::memory_order_release);
      lock.unlock();
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
            slot.retained_lease.reset();
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

    void Publish(Outcome outcome, std::size_t completion_slot,
                 std::size_t input_retained_bytes) noexcept {
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

      const auto retained = EstimateQueueRetainedBytes(outcome.result);
      CompletionMemoryLease completion_lease;
      if (arena && retained != 0U &&
          !arena->TryTransferActiveToCompletion(input_retained_bytes, retained,
                                                &completion_lease)) {
        slot.state.store(ArenaSlotState::kFatal, std::memory_order_release);
        slot.state.notify_all();
        Fail();
        return;
      }
      auto published_state = ArenaSlotState::kReady;
      bool publication_failed = false;
      {
        std::lock_guard outcomes_lock(slots_mutex);
        try {
          slot.outcome.emplace(std::move(outcome));
          slot.retained_lease = std::move(completion_lease);
        } catch (...) {
          publication_failed = true;
        }
        if (!publication_failed) {
          const auto flags = terminal_flags.load(std::memory_order_acquire);
          if ((flags & kArenaTerminalCancelledBit) != 0U) {
            slot.outcome.reset();
            slot.retained_lease.reset();
            published_state = ArenaSlotState::kCancelled;
          } else if ((flags & kArenaTerminalFatalBit) != 0U) {
            slot.outcome.reset();
            slot.retained_lease.reset();
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
      Publish(std::move(outcome), scheduled_packet.completion_slot,
              scheduled_packet.retained_bytes);
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
       TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther,
       std::size_t retained_byte_capacity = 0U) {
    if (!worker) {
      return sanitize::Status::Invalid(
          "OrderedExecutor::Make: worker is empty");
    }
    const auto normalized_workers = std::max<std::size_t>(1, worker_count);
    auto executor =
        std::unique_ptr<OrderedExecutor>(new (std::nothrow) OrderedExecutor(
            normalized_workers, task_queue_capacity, reorder_capacity,
            std::move(worker), std::move(arena), lane, telemetry_kind,
            retained_byte_capacity));
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

  ~OrderedExecutor() { (void)shutdown(); }
  // Submit contiguous ordinals; consume once the dispatch window is full.
  sanitize::Status Submit(Packet packet) {
    const auto retained_bytes = EstimateQueueRetainedBytes(packet.payload);
    return SubmitCharged(std::move(packet), retained_bytes);
  }

  sanitize::Status SubmitCharged(Packet packet, std::size_t retained_bytes) {
    retained_bytes = std::max<std::size_t>(1U, retained_bytes);
    // Above eight arena workers, reserve directly at the authoritative
    // submission point so each packet acquires the executor mutex only once.
    // One-through-eight workers use the compact single-domain path below.
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
      const auto outcome_retained = EstimateQueueRetainedBytes(outcome.result);
      std::lock_guard lock(mutex_);
      if (cancelled_) {
        return sanitize::Status::Cancelled(
            "OrderedExecutor::Submit: executor was cancelled");
      }
      if (!store_outcome_locked(std::move(outcome), completion_slot,
                                outcome_retained)) {
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
      // Schedule() was already published under mutex_.  Construct the lease
      // before any throwing packet/callback construction so every exceptional
      // path also retires the external completion shard.
      ExternalLease external_lease(shared, completion_shard);
      sanitize::Status submit_status = sanitize::Status::OK();
      try {
        ScheduledPacket scheduled{.packet = std::move(packet),
                                  .completion_slot = completion_slot,
                                  .retained_bytes = retained_bytes};
        submit_status = arena_->SubmitCharged(
            [shared, scheduled = std::move(scheduled),
             lease = std::move(external_lease)](
                std::size_t worker_index,
                sanitize::internal::StopToken stop) mutable {
              shared->ExecuteExternal(std::move(scheduled), worker_index, stop);
              lease.Complete();
            },
            arena_submission_plan_, TaskMemoryCharge{retained_bytes},
            telemetry_kind_);
      } catch (const std::bad_alloc &) {
        std::lock_guard lock(mutex_);
        --next_submit_ordinal_;
        completion_ring_.RollbackSubmit();
        in_flight_.fetch_sub(1, std::memory_order_release);
        return sanitize::Status::OutOfMemory(
            "OrderedExecutor::Submit: arena publication allocation failed");
      } catch (...) {
        std::lock_guard lock(mutex_);
        --next_submit_ordinal_;
        completion_ring_.RollbackSubmit();
        in_flight_.fetch_sub(1, std::memory_order_release);
        return sanitize::Status::Invalid(
            "OrderedExecutor::Submit: arena publication failed");
      }
      if (!submit_status.ok()) {
        std::lock_guard lock(mutex_);
        --next_submit_ordinal_;
        completion_ring_.RollbackSubmit();
        in_flight_.fetch_sub(1, std::memory_order_release);
        return submit_status;
      }
      return sanitize::Status::OK();
    }

    if (retained_bytes > retained_byte_capacity_) {
      return sanitize::Status::OutOfMemory(
          "OrderedExecutor::Submit: retained-byte charge exceeds private "
          "executor capacity");
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
    if (private_retained_bytes_ > retained_byte_capacity_ - retained_bytes) {
      return sanitize::Status::Invalid(
          "OrderedExecutor::Submit: retained-byte window is full; consume the "
          "next ordinal before submitting more work");
    }
    const auto completion_slot = completion_ring_.ReserveSubmit();
    try {
      // Construction itself can allocate or invoke a throwing move. Keep the
      // completion cursor transactional across both packet construction and
      // deque growth, not just push_back().
      ScheduledPacket scheduled{.packet = std::move(packet),
                                .completion_slot = completion_slot,
                                .retained_bytes = retained_bytes};
      tasks_.push_back(std::move(scheduled));
    } catch (const std::bad_alloc &) {
      completion_ring_.RollbackSubmit();
      return sanitize::Status::OutOfMemory(
          "OrderedExecutor::Submit: task queue allocation failed");
    } catch (...) {
      completion_ring_.RollbackSubmit();
      return sanitize::Status::Invalid(
          "OrderedExecutor::Submit: task queue publication failed");
    }
    private_retained_bytes_ += retained_bytes;
    peak_private_retained_bytes_ =
        std::max(peak_private_retained_bytes_, private_retained_bytes_);
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

    const auto completion_slot = completion_ring_.NextTake();
    auto &slot = completed_[completion_slot];
    Outcome outcome = std::move(*slot);
    slot.reset();
    const auto retained =
        std::exchange(completed_retained_bytes_[completion_slot], 0U);
    private_retained_bytes_ = retained >= private_retained_bytes_
                                  ? 0U
                                  : private_retained_bytes_ - retained;
    ++next_take_ordinal_;
    completion_ring_.AdvanceTake();
    in_flight_.fetch_sub(1, std::memory_order_release);
    lock.unlock();
    task_space_.notify_all();
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
      std::size_t dropped_retained = 0U;
      const auto add_dropped_retained = [&dropped_retained](std::size_t bytes) {
        if (bytes >
            std::numeric_limits<std::size_t>::max() - dropped_retained) {
          dropped_retained = std::numeric_limits<std::size_t>::max();
        } else {
          dropped_retained += bytes;
        }
      };
      for (const auto &task : tasks_) {
        add_dropped_retained(task.retained_bytes);
      }
      tasks_.clear();
      for (std::size_t index = 0; index < completed_.size(); ++index) {
        completed_[index].reset();
        add_dropped_retained(completed_retained_bytes_[index]);
        completed_retained_bytes_[index] = 0U;
      }
      private_retained_bytes_ =
          dropped_retained >= private_retained_bytes_
              ? 0U
              : private_retained_bytes_ - dropped_retained;
      cancel_arena_slots_locked();
      in_flight_.store(0, std::memory_order_release);
    }
    for (auto &thread : workers_) {
      thread.request_stop();
    }
    notify_all();
  }
  // Cancel and drain external arena tasks, reporting whether every scheduled
  // completion lease retired before the bounded safety deadline. This is an
  // internal lifecycle primitive; repeated calls preserve the first result.
  [[nodiscard]] bool Shutdown() noexcept { return shutdown(); }
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
  // of a locked atomic read-modify-write. Smaller executors use fetch_sub to
  // avoid the extra branch on their short stages.
  void decrement_high_core_in_flight_locked() noexcept {
    const auto current = in_flight_.load(std::memory_order_relaxed);
    in_flight_.store(current - 1U, std::memory_order_release);
  }

#include "internal/runtime/ordered_executor_arena_completion.cc.inc"
  OrderedExecutor(std::size_t worker_count, std::size_t task_queue_capacity,
                  std::size_t reorder_capacity, Worker worker,
                  std::shared_ptr<OperationTaskArena> arena, TaskArenaLane lane,
                  TaskTelemetryKind telemetry_kind,
                  std::size_t retained_byte_capacity)
      : worker_count_(worker_count),
        task_queue_capacity_(std::max<std::size_t>(1, task_queue_capacity)),
        reorder_capacity_(std::max<std::size_t>(1, reorder_capacity)),
        retained_byte_capacity_(
            retained_byte_capacity != 0U
                ? retained_byte_capacity
                : (arena && arena->queue_byte_capacity() != 0U
                       ? arena->queue_byte_capacity()
                       : std::max<std::size_t>(
                             16U * 1024U * 1024U,
                             std::min<std::size_t>(
                                 std::numeric_limits<std::size_t>::max() / 2U,
                                 std::max<std::size_t>(1U, reorder_capacity_) *
                                     (16U * 1024U * 1024U))))),
        completion_ring_(reorder_capacity_), worker_(std::move(worker)),
        completed_((!arena || arena->inline_mode()) ? reorder_capacity_ : 0U),
        completed_retained_bytes_(
            (!arena || arena->inline_mode()) ? reorder_capacity_ : 0U, 0U),
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
            reorder_capacity_, external_completion_shard_count_, worker_,
            arena_);
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
        const auto outcome_retained =
            EstimateQueueRetainedBytes(outcome.result);
        std::unique_lock lock(mutex_);
        if (stop.stop_requested() || cancelled_ || fatal_) {
          private_retained_bytes_ =
              scheduled->retained_bytes >= private_retained_bytes_
                  ? 0U
                  : private_retained_bytes_ - scheduled->retained_bytes;
          lock.unlock();
          task_space_.notify_all();
          return;
        }
        if (!store_outcome_locked(std::move(outcome),
                                  scheduled->completion_slot, outcome_retained,
                                  scheduled->retained_bytes)) {
          fatal_ = true;
          accepting_ = false;
          // The failed active packet still owns its input charge because the
          // completion transfer did not commit. Drop it and every queued/result
          // owner exactly; no future admission is allowed after fatality.
          private_retained_bytes_ =
              scheduled->retained_bytes >= private_retained_bytes_
                  ? 0U
                  : private_retained_bytes_ - scheduled->retained_bytes;
          for (const auto &pending : tasks_) {
            private_retained_bytes_ =
                pending.retained_bytes >= private_retained_bytes_
                    ? 0U
                    : private_retained_bytes_ - pending.retained_bytes;
          }
          tasks_.clear();
          for (std::size_t index = 0; index < completed_retained_bytes_.size();
               ++index) {
            const auto bytes = completed_retained_bytes_[index];
            private_retained_bytes_ = bytes >= private_retained_bytes_
                                          ? 0U
                                          : private_retained_bytes_ - bytes;
            completed_retained_bytes_[index] = 0U;
            if (index < completed_.size()) {
              completed_[index].reset();
            }
          }
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
        for (std::size_t index = 0; index < completed_.size(); ++index) {
          completed_[index].reset();
          completed_retained_bytes_[index] = 0U;
        }
        private_retained_bytes_ = 0U;
        in_flight_.store(0, std::memory_order_release);
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
  bool store_outcome_locked(Outcome outcome, std::size_t completion_slot,
                            std::size_t output_retained,
                            std::size_t input_retained_bytes = 0U) noexcept {
    auto &slot = completed_[completion_slot];
    if (slot) {
      return false;
    }
    const auto retained_after_input =
        input_retained_bytes >= private_retained_bytes_
            ? 0U
            : private_retained_bytes_ - input_retained_bytes;
    if (output_retained > retained_byte_capacity_ ||
        retained_after_input > retained_byte_capacity_ - output_retained) {
      return false;
    }
    try {
      slot.emplace(std::move(outcome));
    } catch (...) {
      return false;
    }
    if (input_retained_bytes >= private_retained_bytes_) {
      private_retained_bytes_ = 0U;
    } else {
      private_retained_bytes_ -= input_retained_bytes;
    }
    if (output_retained >
        std::numeric_limits<std::size_t>::max() - private_retained_bytes_) {
      private_retained_bytes_ = std::numeric_limits<std::size_t>::max();
    } else {
      private_retained_bytes_ += output_retained;
    }
    completed_retained_bytes_[completion_slot] = output_retained;
    peak_private_retained_bytes_ =
        std::max(peak_private_retained_bytes_, private_retained_bytes_);
    return true;
  }
  void notify_all() noexcept {
    task_ready_.notify_all();
    task_space_.notify_all();
    result_ready_.notify_all();
  }
  [[nodiscard]] bool shutdown() noexcept {
    if (shutdown_complete_) {
      return shutdown_drained_;
    }
    Cancel();
    auto drained = true;
    if (arena_shared_) {
      // External arena tasks own only this shared control block. A non-
      // cooperative task can no longer pin or dereference the executor object
      // after the bounded drain deadline.
      drained = arena_shared_->WaitUntil(std::chrono::steady_clock::now() +
                                         std::chrono::seconds(2));
    }
    workers_.clear();
    shutdown_drained_ = drained;
    shutdown_complete_ = true;
    return shutdown_drained_;
  }
  const std::size_t worker_count_;
  const std::size_t task_queue_capacity_;
  const std::size_t reorder_capacity_;
  const std::size_t retained_byte_capacity_;
  CompletionRingCursor completion_ring_;
  Worker worker_;
  mutable std::mutex mutex_;
  std::condition_variable task_ready_;
  std::condition_variable task_space_;
  std::condition_variable result_ready_;
  std::mutex take_mutex_;
  std::deque<ScheduledPacket> tasks_;
  std::vector<std::optional<Outcome>> completed_;
  std::vector<std::size_t> completed_retained_bytes_;
  std::size_t private_retained_bytes_ = 0U;
  std::size_t peak_private_retained_bytes_ = 0U;
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
  bool shutdown_complete_ = false;
  bool shutdown_drained_ = true;
  // Monotonic cancellation/fatality bits share one cache location so normal
  // result publication needs one acquire snapshot rather than two loads.
  bool accepting_ = true;
  bool cancelled_ = false;
  bool fatal_ = false;
};

} // namespace sanitize::internal
