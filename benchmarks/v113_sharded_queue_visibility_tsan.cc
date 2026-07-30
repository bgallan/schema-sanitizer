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
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
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
  const auto low_width = workers / 2U;
  const auto high_width = workers - low_width;
  const auto low = arena->PrepareSubmissionPlan(
      low_width, sanitize::internal::TaskArenaLane::kUpstream);
  const auto high = arena->PrepareSubmissionPlan(
      high_width, sanitize::internal::TaskArenaLane::kOutput);

  constexpr std::size_t kWaves = 64U;
  std::atomic<std::size_t> entered{0};
  std::atomic<std::size_t> completed{0};
  std::atomic<std::size_t> release_wave{0};
  std::atomic<bool> failed{false};

  for (std::size_t wave = 0; wave < kWaves; ++wave) {
    const auto submit_lane = [&](const auto &plan, std::size_t count) {
      for (std::size_t index = 0; index < count; ++index) {
        const auto status = arena->Submit(
            [&entered, &completed, &release_wave, wave](
                std::size_t, std::stop_token stop) {
              entered.fetch_add(1, std::memory_order_release);
              while (release_wave.load(std::memory_order_acquire) <= wave &&
                     !stop.stop_requested()) {
                std::this_thread::yield();
              }
              completed.fetch_add(1, std::memory_order_release);
            },
            plan);
        if (!status.ok()) {
          failed.store(true, std::memory_order_release);
        }
      }
    };

    std::jthread low_producer([&] { submit_lane(low, low_width); });
    std::jthread high_producer([&] { submit_lane(high, high_width); });
    low_producer.join();
    high_producer.join();
    if (failed.load(std::memory_order_acquire)) {
      return false;
    }

    const auto target = (wave + 1U) * workers;
    if (!wait_for([&] {
          return entered.load(std::memory_order_acquire) == target &&
                 arena->active_tasks() == workers;
        })) {
      return false;
    }
    release_wave.store(wave + 1U, std::memory_order_release);
    if (!wait_for([&] {
          return completed.load(std::memory_order_acquire) == target &&
                 arena->active_tasks() == 0U && arena->queued_tasks() == 0U;
        })) {
      return false;
    }
  }

  const bool exact = arena->submitted_tasks() == workers * kWaves &&
                     arena->peak_active_tasks() == workers &&
                     arena->started_workers() == workers;
  arena->Shutdown();
  return exact;
}

} // namespace

int main() {
  for (const auto workers : {9U, 16U, 32U}) {
    if (!run(workers)) {
      std::cerr << "v113 sharded queue visibility probe failed at " << workers
                << " workers\n";
      return 1;
    }
  }
  std::cout << "v113 sharded queue visibility probe passed\n";
  return 0;
}
