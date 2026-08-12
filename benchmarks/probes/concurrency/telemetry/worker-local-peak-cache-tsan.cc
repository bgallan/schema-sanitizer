#include "internal/runtime/operation_task_arena.hh"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <stop_token>
#include <thread>

namespace {

bool wait_for(const auto &predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (!predicate() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return predicate();
}

bool run(std::size_t workers) {
  auto made = sanitize::internal::OperationTaskArena::Make(workers);
  if (!made.ok()) {
    return false;
  }
  auto arena = std::move(made).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(
      workers, sanitize::internal::TaskArenaLane::kAll);

  std::atomic<std::size_t> entered{0};
  std::atomic<std::size_t> completed{0};
  std::atomic<bool> release{false};
  for (std::size_t index = 0; index < workers; ++index) {
    const auto status = arena->Submit(
        [&entered, &completed, &release](std::size_t,
                                        std::stop_token stop) {
          entered.fetch_add(1, std::memory_order_release);
          while (!release.load(std::memory_order_acquire) &&
                 !stop.stop_requested()) {
            std::this_thread::yield();
          }
          completed.fetch_add(1, std::memory_order_release);
        },
        plan);
    if (!status.ok()) {
      return false;
    }
  }

  if (!wait_for([&] {
        return entered.load(std::memory_order_acquire) == workers;
      })) {
    return false;
  }
  if (arena->active_tasks() != workers ||
      arena->peak_active_tasks() != workers) {
    return false;
  }
  release.store(true, std::memory_order_release);
  if (!wait_for([&] {
        return completed.load(std::memory_order_acquire) == workers &&
               arena->active_tasks() == 0U && arena->queued_tasks() == 0U;
      })) {
    return false;
  }

  constexpr std::size_t kWaves = 128U;
  for (std::size_t wave = 0; wave < kWaves; ++wave) {
    const auto target = completed.load(std::memory_order_relaxed) + workers;
    for (std::size_t index = 0; index < workers; ++index) {
      const auto status = arena->Submit(
          [&completed](std::size_t, std::stop_token stop) {
            if (!stop.stop_requested()) {
              completed.fetch_add(1, std::memory_order_release);
            }
          },
          plan);
      if (!status.ok()) {
        return false;
      }
    }
    if (!wait_for([&] {
          return completed.load(std::memory_order_acquire) == target &&
                 arena->active_tasks() == 0U && arena->queued_tasks() == 0U;
        })) {
      return false;
    }
  }

  const bool exact = arena->peak_active_tasks() == workers &&
                     arena->started_workers() == workers &&
                     arena->submitted_tasks() == workers * (kWaves + 1U);
  arena->Shutdown();
  return exact;
}

} // namespace

int main() {
  for (const auto workers : {4U, 8U, 16U}) {
    if (!run(workers)) {
      std::cerr << "worker-local peak cache probe failed at " << workers
                << " workers\n";
      return 1;
    }
  }
  std::cout << "worker-local peak cache probe passed\n";
  return 0;
}
