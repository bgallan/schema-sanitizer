// Coordinates native task execution across concurrent public operations.
#pragma once

#include "internal/runtime/cpu_capacity.hh"

#include "internal/runtime/thread_compat.hh"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>

namespace sanitize::internal {

class ProcessCpuGovernor final {
public:
  class TaskLease final {
  public:
    TaskLease() = default;
    TaskLease(ProcessCpuGovernor *owner, std::int64_t wait_ns,
              bool waited) noexcept
        : owner_(owner), wait_ns_(wait_ns), waited_(waited) {}
    TaskLease(const TaskLease &) = delete;
    TaskLease &operator=(const TaskLease &) = delete;
    TaskLease(TaskLease &&other) noexcept
        : owner_(other.owner_), wait_ns_(other.wait_ns_),
          waited_(other.waited_) {
      other.owner_ = nullptr;
      other.wait_ns_ = 0;
      other.waited_ = false;
    }
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
    ~TaskLease() { Release(); }

    [[nodiscard]] std::int64_t wait_ns() const noexcept { return wait_ns_; }
    [[nodiscard]] bool waited() const noexcept { return waited_; }

  private:
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
    Registration() = default;
    Registration(ProcessCpuGovernor *owner, std::size_t worker_count) noexcept
        : owner_(worker_count > 1U ? owner : nullptr), width_(worker_count) {
      if (owner_) {
        owner_->Register();
      }
    }
    Registration(const Registration &) = delete;
    Registration &operator=(const Registration &) = delete;
    Registration(Registration &&other) noexcept
        : owner_(other.owner_), width_(other.width_) {
      other.owner_ = nullptr;
      other.width_ = 0U;
    }
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
    ~Registration() { Release(); }

    [[nodiscard]] TaskLease
    Acquire(sanitize::internal::StopToken stop) noexcept {
      return owner_ ? owner_->AcquireTask(stop, width_) : TaskLease{};
    }

  private:
    void Release() noexcept {
      if (owner_) {
        owner_->Unregister();
      }
      owner_ = nullptr;
    }

    ProcessCpuGovernor *owner_ = nullptr;
    std::size_t width_ = 0U;
  };

  [[nodiscard]] Registration
  MakeRegistration(std::size_t worker_count) noexcept {
    return Registration(this, worker_count);
  }
  [[nodiscard]] std::int64_t capacity() const noexcept {
    return std::max<std::int64_t>(1, available_cpu_capacity());
  }

private:
  struct Waiter final {
    Waiter *next = nullptr;
  };

  ProcessCpuGovernor() = default;

  void Register() noexcept {
    registered_arenas_.fetch_add(1, std::memory_order_acq_rel);
    ready_.notify_all();
  }

  void Unregister() noexcept {
    registered_arenas_.fetch_sub(1, std::memory_order_acq_rel);
    ready_.notify_all();
  }

  void Enqueue(Waiter *waiter) noexcept {
    if (tail_) {
      tail_->next = waiter;
    } else {
      head_ = waiter;
    }
    tail_ = waiter;
  }

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

  [[nodiscard]] TaskLease AcquireTask(sanitize::internal::StopToken stop,
                                      std::size_t arena_width) noexcept {
    const auto current_capacity = capacity();
    if (registered_arenas_.load(std::memory_order_acquire) <= 1 &&
        static_cast<std::int64_t>(arena_width) <= current_capacity) {
      return {};
    }
    const auto started = std::chrono::steady_clock::now();
    Waiter waiter;
    std::unique_lock lock(mutex_);
    const auto can_bypass = [&] {
      return registered_arenas_.load(std::memory_order_acquire) <= 1 &&
             static_cast<std::int64_t>(arena_width) <= capacity();
    };
    if (can_bypass()) {
      return {};
    }
    Enqueue(&waiter);
    const bool contended = head_ != &waiter || active_tasks_ >= capacity();
    const auto admitted = WaitWithStop(ready_, lock, stop, [&] {
      return can_bypass() || (head_ == &waiter && active_tasks_ < capacity());
    });
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

  void ReleaseTask() noexcept {
    {
      std::lock_guard lock(mutex_);
      active_tasks_ = std::max<std::int64_t>(0, active_tasks_ - 1);
    }
    ready_.notify_all();
  }

  std::atomic<std::int64_t> registered_arenas_{0};
  std::mutex mutex_;
  std::condition_variable_any ready_;
  Waiter *head_ = nullptr;
  Waiter *tail_ = nullptr;
  std::int64_t active_tasks_ = 0;

  friend ProcessCpuGovernor &process_cpu_governor() noexcept;
};

[[nodiscard]] inline ProcessCpuGovernor &process_cpu_governor() noexcept {
  static auto *governor = new ProcessCpuGovernor();
  return *governor;
}

} // namespace sanitize::internal
