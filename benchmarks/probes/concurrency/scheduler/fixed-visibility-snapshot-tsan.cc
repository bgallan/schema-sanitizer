#include "internal/runtime/operation_task_arena.hh"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <stop_token>
#include <thread>

namespace {

bool WaitFor(const auto &predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (!predicate() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return predicate();
}

bool Run(std::size_t workers) {
  auto made = sanitize::internal::OperationTaskArena::Make(workers);
  if (!made.ok()) {
    return false;
  }
  auto arena = std::move(made).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(
      workers, sanitize::internal::TaskArenaLane::kAll);

  std::atomic<std::size_t> blockers_entered{0};
  std::atomic<std::size_t> completed{0};
  std::atomic<bool> release{false};

  for (std::size_t index = 0; index < workers; ++index) {
    const auto status = arena->Submit(
        [&blockers_entered, &completed, &release](
            std::size_t, std::stop_token stop) {
          blockers_entered.fetch_add(1, std::memory_order_release);
          while (!release.load(std::memory_order_acquire) &&
                 !stop.stop_requested()) {
            std::this_thread::yield();
          }
          completed.fetch_add(1, std::memory_order_release);
        },
        plan, index);
    if (!status.ok()) {
      return false;
    }
  }
  if (!WaitFor([&] {
        return blockers_entered.load(std::memory_order_acquire) == workers &&
               arena->active_tasks() == workers;
      })) {
    return false;
  }

  // With every worker active, repeating one explicit ticket concentrates this
  // broad-lane backlog on the preferred and precompiled alternative queues.
  // Once blockers are released, idle workers must take the high-core stealing
  // snapshot and drain work across every physical visibility-shard geometry.
  constexpr std::size_t kQuickTasks = 4096U;
  for (std::size_t task = 0; task < kQuickTasks; ++task) {
    const auto status = arena->Submit(
        [&completed](std::size_t, std::stop_token) {
          completed.fetch_add(1, std::memory_order_release);
        },
        plan, 0U);
    if (!status.ok()) {
      return false;
    }
  }

  release.store(true, std::memory_order_release);
  const auto expected = workers + kQuickTasks;
  if (!WaitFor([&] {
        return completed.load(std::memory_order_acquire) == expected &&
               arena->active_tasks() == 0U && arena->queued_tasks() == 0U;
      })) {
    return false;
  }

  const bool exact = arena->submitted_tasks() == expected &&
                     arena->started_workers() == workers &&
                     arena->peak_active_tasks() == workers &&
                     arena->stolen_tasks() > 0U;
  arena->Shutdown();
  return exact;
}

} // namespace

int main() {
  for (const auto workers : {9U, 16U, 17U, 24U, 25U, 32U}) {
    if (!Run(workers)) {
      std::cerr << "fixed visibility snapshot probe failed at "
                << workers << " workers\n";
      return 1;
    }
  }
  std::cout << "fixed visibility snapshot probe passed\n";
  return 0;
}
