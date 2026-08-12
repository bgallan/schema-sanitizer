#include "internal/runtime/operation_task_arena.hh"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <stop_token>
#include <thread>

namespace {

[[nodiscard]] bool wait_for(const auto &predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (!predicate() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return predicate();
}

[[nodiscard]] bool verify_wake_queue_isolation(std::size_t workers) {
  auto made = sanitize::internal::OperationTaskArena::Make(workers);
  if (!made.ok()) {
    return false;
  }
  auto arena = std::move(made).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(
      workers, sanitize::internal::TaskArenaLane::kAll);

  constexpr std::size_t kWaves = 96U;
  std::atomic<std::size_t> entered{0};
  std::atomic<std::size_t> completed{0};
  std::atomic<std::size_t> release_wave{0};

  for (std::size_t wave = 0; wave < kWaves; ++wave) {
    const auto expected = (wave + 1U) * workers;
    for (std::size_t index = 0; index < workers; ++index) {
      const auto status = arena->Submit(
          [&entered, &completed, &release_wave, wave](
              std::size_t, std::stop_token stop) {
            entered.fetch_add(1U, std::memory_order_release);
            while (release_wave.load(std::memory_order_acquire) <= wave &&
                   !stop.stop_requested()) {
              std::this_thread::yield();
            }
            completed.fetch_add(1U, std::memory_order_release);
          },
          plan);
      if (!status.ok()) {
        return false;
      }
    }
    if (!wait_for([&] {
          return entered.load(std::memory_order_acquire) == expected &&
                 arena->active_tasks() == workers;
        })) {
      return false;
    }
    release_wave.store(wave + 1U, std::memory_order_release);
    if (!wait_for([&] {
          return completed.load(std::memory_order_acquire) == expected &&
                 arena->active_tasks() == 0U &&
                 arena->queued_tasks() == 0U;
        })) {
      return false;
    }
  }

  const auto exact = arena->submitted_tasks() == workers * kWaves &&
                     arena->started_workers() == workers &&
                     arena->peak_active_tasks() == workers &&
                     arena->wake_epoch_publishes() >= workers;
  arena->Shutdown();
  return exact;
}

} // namespace

int main() {
  for (const auto workers : {2U, 4U, 8U, 16U, 32U}) {
    if (!verify_wake_queue_isolation(workers)) {
      std::cerr << "wake epoch cache-line probe failed at " << workers
                << " workers\n";
      return 1;
    }
  }
  std::cout << "wake epoch cache-line probe passed\n";
  return 0;
}
