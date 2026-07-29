// Coordinates native task execution across concurrent public operations.
#pragma once

#include "internal/runtime/cpu_capacity.hh"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <stop_token>

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
    Registration(ProcessCpuGovernor *owner, bool active) noexcept
        : owner_(active ? owner : nullptr) {
      if (owner_) {
        owner_->Register();
      }
    }
    Registration(const Registration &) = delete;
    Registration &operator=(const Registration &) = delete;
    Registration(Registration &&other) noexcept : owner_(other.owner_) {
      other.owner_ = nullptr;
    }
    Registration &operator=(Registration &&other) noexcept {
      if (this != &other) {
        Release();
        owner_ = other.owner_;
        other.owner_ = nullptr;
      }
      return *this;
    }
    ~Registration() { Release(); }

    [[nodiscard]] TaskLease Acquire(std::stop_token stop) noexcept {
      return owner_ ? owner_->AcquireTask(stop) : TaskLease{};
    }

  private:
    void Release() noexcept {
      if (owner_) {
        owner_->Unregister();
      }
      owner_ = nullptr;
    }

    ProcessCpuGovernor *owner_ = nullptr;
  };

  [[nodiscard]] Registration MakeRegistration(bool multi_worker) noexcept {
    return Registration(this, multi_worker);
  }
  [[nodiscard]] std::int64_t capacity() const noexcept { return capacity_; }

private:
  struct Waiter final {
    Waiter *next = nullptr;
  };

  ProcessCpuGovernor()
      : capacity_(std::max<std::int64_t>(1, available_cpu_capacity())) {}

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

  [[nodiscard]] TaskLease AcquireTask(std::stop_token stop) noexcept {
    if (registered_arenas_.load(std::memory_order_acquire) <= 1) {
      return {};
    }
    const auto started = std::chrono::steady_clock::now();
    Waiter waiter;
    std::unique_lock lock(mutex_);
    if (registered_arenas_.load(std::memory_order_acquire) <= 1) {
      return {};
    }
    Enqueue(&waiter);
    const bool contended = head_ != &waiter || active_tasks_ >= capacity_;
    const auto admitted = ready_.wait(lock, stop, [&] {
      return registered_arenas_.load(std::memory_order_acquire) <= 1 ||
             (head_ == &waiter && active_tasks_ < capacity_);
    });
    const auto single_operation =
        registered_arenas_.load(std::memory_order_acquire) <= 1;
    Remove(&waiter);
    if (!admitted || single_operation) {
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

  const std::int64_t capacity_;
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
