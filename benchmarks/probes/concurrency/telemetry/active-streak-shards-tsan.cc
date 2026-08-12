#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/performance_telemetry.hh"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <stop_token>
#include <string>
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
  auto telemetry =
      std::make_shared<sanitize::internal::PerformanceTelemetry>(
          workers, nullptr, -1, static_cast<std::int64_t>(workers), true);
  auto made = sanitize::internal::OperationTaskArena::Make(workers, telemetry);
  if (!made.ok()) {
    return false;
  }
  auto arena = std::move(made).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(
      workers, sanitize::internal::TaskArenaLane::kAll);

  constexpr std::size_t kWaves = 64U;
  std::atomic<std::size_t> entered{0};
  std::atomic<std::size_t> completed{0};
  std::atomic<std::size_t> release_wave{0};

  for (std::size_t wave = 0; wave < kWaves; ++wave) {
    const auto entered_target = (wave + 1U) * workers;
    const auto completed_target = entered_target;
    for (std::size_t task_index = 0; task_index < workers; ++task_index) {
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
        return false;
      }
    }
    if (!wait_for([&] {
          return entered.load(std::memory_order_acquire) == entered_target &&
                 arena->active_tasks() == workers;
        })) {
      return false;
    }
    release_wave.store(wave + 1U, std::memory_order_release);
    if (!wait_for([&] {
          return completed.load(std::memory_order_acquire) ==
                     completed_target &&
                 arena->active_tasks() == 0U &&
                 arena->queued_tasks() == 0U;
        })) {
      return false;
    }
  }

  telemetry->Finish();
  const auto json = telemetry->ToJson();
  const auto expected = std::string{"\"worker_active_streaks\":"} +
                        std::to_string(workers * kWaves);
  const bool exact = json.find(expected) != std::string::npos &&
                     arena->peak_active_tasks() == workers &&
                     arena->submitted_tasks() == workers * kWaves;
  arena->Shutdown();
  return exact;
}

} // namespace

int main() {
  for (const auto workers : {2U, 4U, 8U, 16U}) {
    if (!run(workers)) {
      std::cerr << "active-streak shard probe failed at " << workers
                << " workers\n";
      return 1;
    }
  }
  std::cout << "active-streak shard probe passed\n";
  return 0;
}
