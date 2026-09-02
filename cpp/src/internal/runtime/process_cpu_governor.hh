// Coordinates native task execution across concurrent public operations.
// Fair registrations share visible CPU capacity while stop-aware leases
// bound activity.

#pragma once

#include "internal/runtime/cpu_capacity.hh"

#include "internal/runtime/thread_compat.hh"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <utility>

namespace sanitize::internal {

class ProcessCpuGovernor final {
public:
  class TaskLease final {
  public:
    /// Creates an empty process CPU task lease.
    TaskLease() = default;
    /// Owns one admitted CPU task slot and its measured wait state.
    TaskLease(ProcessCpuGovernor *owner, std::int64_t wait_ns,
              bool waited) noexcept
        : owner_(owner), wait_ns_(wait_ns), waited_(waited) {}
    /// Disables copying the process CPU task lease.
    TaskLease(const TaskLease &) = delete;
    /// Disables copy assignment for the process CPU task lease.
    TaskLease &operator=(const TaskLease &) = delete;
    /// Transfers ownership from another process CPU task lease.
    TaskLease(TaskLease &&other) noexcept
        : owner_(other.owner_), wait_ns_(other.wait_ns_),
          waited_(other.waited_) {
      other.owner_ = nullptr;
      other.wait_ns_ = 0;
      other.waited_ = false;
    }
    /// Transfers owned state from another process CPU task lease.
    TaskLease &operator=(TaskLease &&other) noexcept {
      if (this != &other) {
        Release();
        owner_ = other.owner_;
        wait_ns_ = other.wait_ns_;
        waited_ = other.waited_;
        other.owner_ = nullptr;
        other.wait_ns_ = 0;
        other.waited_ = false;
      }
      return *this;
    }
    /// Returns the admitted task slot to the process CPU governor.
    ~TaskLease() { Release(); }

    /// Returns nanoseconds spent waiting for process CPU admission.
    [[nodiscard]] std::int64_t wait_ns() const noexcept { return wait_ns_; }
    /// Reports whether CPU admission required blocking.
    [[nodiscard]] bool waited() const noexcept { return waited_; }

  private:
    /// Returns this lease's admitted task slot when one is owned.
    void Release() noexcept {
      if (owner_) {
        owner_->ReleaseTask();
      }
      owner_ = nullptr;
    }

    ProcessCpuGovernor *owner_ = nullptr;
    std::int64_t wait_ns_ = 0;
    bool waited_ = false;
  };

  class Registration final {
  public:
    /// Creates an empty operation registration with the process
    /// CPU governor.
    Registration() = default;
    /// Registers multi-worker operation demand with the process
    /// CPU governor.
    Registration(ProcessCpuGovernor *owner, std::size_t worker_count) noexcept
        : owner_(worker_count > 1U ? owner : nullptr), width_(worker_count) {
      if (owner_) {
        owner_->Register();
      }
    }
    /// Disables copying the operation registration with the process
    /// CPU governor.
    Registration(const Registration &) = delete;
    /// Disables copy assignment for the operation registration with the
    /// process CPU governor.
    Registration &operator=(const Registration &) = delete;
    /// Transfers ownership from another operation registration with the
    /// process CPU governor.
    Registration(Registration &&other) noexcept
        : owner_(other.owner_), width_(other.width_) {
      other.owner_ = nullptr;
      other.width_ = 0U;
    }
    /// Transfers owned state from another operation registration with the
    /// process CPU governor.
    Registration &operator=(Registration &&other) noexcept {
      if (this != &other) {
        Release();
        owner_ = other.owner_;
        width_ = other.width_;
        other.owner_ = nullptr;
        other.width_ = 0U;
      }
      return *this;
    }
    /// Removes this operation from process CPU demand accounting.
    ~Registration() { Release(); }

    /// Waits cooperatively for one process CPU task slot.
    [[nodiscard]] TaskLease
    Acquire(sanitize::internal::StopToken stop) noexcept {
      return owner_ ? owner_->AcquireTask(stop, width_) : TaskLease{};
    }

  private:
    /// Removes this operation from process CPU demand accounting once.
    void Release() noexcept {
      if (owner_) {
        owner_->Unregister();
      }
      owner_ = nullptr;
    }

    ProcessCpuGovernor *owner_ = nullptr;
    std::size_t width_ = 0U;
  };

  /// Registers an operation's logical worker demand with the CPU governor.
  [[nodiscard]] Registration
  MakeRegistration(std::size_t worker_count) noexcept {
    return Registration(this, worker_count);
  }
  /// Returns the process CPU task capacity currently enforced.
  [[nodiscard]] std::int64_t capacity() noexcept { return RefreshCapacity(); }

private:
  struct Waiter final {
    Waiter *next = nullptr;
  };

  /// Initializes an empty process-wide CPU admission queue.
  ProcessCpuGovernor() = default;

  static constexpr auto kCapacityRefreshPeriod = std::chrono::milliseconds(250);

  /// Returns the last published positive process CPU capacity.
  [[nodiscard]] std::int64_t CachedCapacity() const noexcept {
    return std::max<std::int64_t>(
        1, cached_capacity_.load(std::memory_order_acquire));
  }

  /// Detects and publishes current CPU capacity, waking waiters on changes.
  [[nodiscard]] std::int64_t RefreshCapacity() noexcept {
    // available_cpu_capacity() may inspect affinity and, periodically, the
    // cgroup hierarchy. Always sample before acquiring mutex_ so OS I/O cannot
    // stall task release or FIFO admission. Wait predicates consume only the
    // published atomic value.
    const auto detected = std::max<std::int64_t>(1, available_cpu_capacity());
    const auto previous =
        cached_capacity_.exchange(detected, std::memory_order_acq_rel);
    if (previous != detected) {
      ready_.notify_all();
    }
    return detected;
  }

  /// Adds one multi-worker operation to process CPU demand accounting.
  void Register() noexcept {
    registered_arenas_.fetch_add(1, std::memory_order_acq_rel);
    ready_.notify_all();
  }

  /// Removes one multi-worker operation from process CPU demand accounting.
  void Unregister() noexcept {
    registered_arenas_.fetch_sub(1, std::memory_order_acq_rel);
    ready_.notify_all();
  }

  /// Appends a waiter to the process CPU admission FIFO.
  void Enqueue(Waiter *waiter) noexcept {
    if (tail_) {
      tail_->next = waiter;
    } else {
      head_ = waiter;
    }
    tail_ = waiter;
  }

  /// Removes a waiter from the process CPU admission FIFO if still present.
  void Remove(Waiter *waiter) noexcept {
    Waiter *previous = nullptr;
    auto *current = head_;
    while (current && current != waiter) {
      previous = current;
      current = current->next;
    }
    if (!current) {
      return;
    }
    if (previous) {
      previous->next = current->next;
    } else {
      head_ = current->next;
    }
    if (tail_ == current) {
      tail_ = previous;
    }
    current->next = nullptr;
  }

  /// Acquires one stop-aware CPU task lease using fair process-wide admission.
  [[nodiscard]] TaskLease AcquireTask(sanitize::internal::StopToken stop,
                                      std::size_t arena_width) noexcept {
    const auto current_capacity = RefreshCapacity();
    if (registered_arenas_.load(std::memory_order_acquire) <= 1 &&
        static_cast<std::int64_t>(arena_width) <= current_capacity) {
      return {};
    }
    const auto started = std::chrono::steady_clock::now();
    Waiter waiter;
    std::unique_lock lock(mutex_);
    const auto can_bypass = [&] {
      return registered_arenas_.load(std::memory_order_acquire) <= 1 &&
             static_cast<std::int64_t>(arena_width) <= CachedCapacity();
    };
    if (can_bypass()) {
      return {};
    }
    Enqueue(&waiter);
    const bool contended =
        head_ != &waiter || active_tasks_ >= CachedCapacity();
    const auto can_admit = [&] {
      return can_bypass() ||
             (head_ == &waiter && active_tasks_ < CachedCapacity());
    };
    bool admitted = false;
    auto wake_waiter = [this] { ready_.notify_all(); };
    {
      StopCallback<decltype(wake_waiter)> stop_callback(stop,
                                                        std::move(wake_waiter));
      auto refresh_at =
          std::chrono::steady_clock::now() + kCapacityRefreshPeriod;
      for (;;) {
        if (stop.stop_requested()) {
          break;
        }
        if (can_admit()) {
          admitted = true;
          break;
        }
        if (ready_.wait_until(lock, refresh_at) == std::cv_status::timeout) {
          // Refresh affinity and the TTL-bound cgroup cache without mutex_. A
          // quota increase therefore wakes an otherwise idle FIFO, while a
          // decrease prevents further admission after existing leases drain.
          lock.unlock();
          (void)RefreshCapacity();
          lock.lock();
          refresh_at =
              std::chrono::steady_clock::now() + kCapacityRefreshPeriod;
        }
      }
    }
    const bool bypass = can_bypass();
    Remove(&waiter);
    if (!admitted || bypass) {
      lock.unlock();
      ready_.notify_all();
      return {};
    }
    ++active_tasks_;
    lock.unlock();
    ready_.notify_all();
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    return TaskLease(this, std::max<std::int64_t>(0, elapsed), contended);
  }

  /// Returns one active task slot and wakes queued CPU waiters.
  void ReleaseTask() noexcept {
    {
      std::lock_guard lock(mutex_);
      active_tasks_ = std::max<std::int64_t>(0, active_tasks_ - 1);
    }
    ready_.notify_all();
  }

  std::atomic<std::int64_t> cached_capacity_{1};
  std::atomic<std::int64_t> registered_arenas_{0};
  std::mutex mutex_;
  std::condition_variable_any ready_;
  Waiter *head_ = nullptr;
  Waiter *tail_ = nullptr;
  std::int64_t active_tasks_ = 0;

  friend ProcessCpuGovernor &process_cpu_governor() noexcept;
};

/// Returns the singleton coordinating CPU capacity across operations.
[[nodiscard]] inline ProcessCpuGovernor &process_cpu_governor() noexcept {
  static auto *governor = new ProcessCpuGovernor();
  return *governor;
}

} // namespace sanitize::internal
