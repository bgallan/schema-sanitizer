#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/performance_telemetry.hh"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

namespace {

bool run(std::size_t workers, std::size_t task_count) {
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
  std::atomic<std::size_t> completed{0};
  for (std::size_t ordinal = 0; ordinal < task_count; ++ordinal) {
    const auto kind = static_cast<sanitize::internal::TaskTelemetryKind>(
        ordinal % static_cast<std::size_t>(
                      sanitize::internal::TaskTelemetryKind::kCount));
    const auto status = arena->Submit(
        [&completed](std::size_t, std::stop_token stop) {
          if (!stop.stop_requested()) {
            completed.fetch_add(1, std::memory_order_release);
          }
        },
        plan, kind);
    if (!status.ok()) {
      return false;
    }
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while ((completed.load(std::memory_order_acquire) != task_count ||
          arena->active_tasks() != 0U || arena->queued_tasks() != 0U) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  telemetry->Finish();
  const auto json = telemetry->ToJson();
  const auto expected_per_kind = task_count / 6U;
  const auto exact = std::string{"\"input\":{\"submitted\":"} +
                     std::to_string(expected_per_kind) +
                     ",\"started\":" + std::to_string(expected_per_kind) +
                     ",\"finished\":" + std::to_string(expected_per_kind);
  const bool valid = completed.load(std::memory_order_acquire) == task_count &&
                     arena->active_tasks() == 0U &&
                     arena->queued_tasks() == 0U &&
                     json.find(exact) != std::string::npos;
  arena->Shutdown();
  return valid;
}

} // namespace

int main() {
  for (const auto workers : {5U, 8U, 16U}) {
    if (!run(workers, 12000U)) {
      std::cerr << "worker-completion telemetry TSan probe failed at " << workers
                << " workers\n";
      return 1;
    }
  }
  std::cout << "worker-completion telemetry TSan probe passed\n";
  return 0;
}
