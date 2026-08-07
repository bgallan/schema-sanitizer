#include "internal/runtime/operation_task_arena.hh"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <stop_token>
#include <thread>

namespace {

[[nodiscard]] bool WaitFor(const auto &predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (!predicate() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return predicate();
}

[[nodiscard]] bool Run(std::size_t workers) {
  auto made = sanitize::internal::OperationTaskArena::Make(workers);
  if (!made.ok()) {
    return false;
  }
  auto arena = std::move(made).ValueOrDie();
  const auto all = arena->PrepareSubmissionPlan(
      workers, sanitize::internal::TaskArenaLane::kAll);
  const auto upstream_width = std::max<std::size_t>(1U, workers / 2U);
  const auto output_width = std::max<std::size_t>(1U, workers - workers / 2U);
  const auto compact_width = std::max<std::size_t>(1U, workers / 3U);
  const auto upstream = arena->PrepareSubmissionPlan(
      upstream_width, sanitize::internal::TaskArenaLane::kUpstream);
  const auto output = arena->PrepareSubmissionPlan(
      output_width, sanitize::internal::TaskArenaLane::kOutput);
  const auto compact = arena->PrepareSubmissionPlan(
      compact_width, sanitize::internal::TaskArenaLane::kOutputCompact);

  std::atomic<std::size_t> blockers_entered{0};
  std::atomic<std::size_t> completed{0};
  std::atomic<bool> release{false};
  std::atomic<bool> failed{false};

  for (std::size_t index = 0; index < workers; ++index) {
    const auto status = arena->Submit(
        [&blockers_entered, &completed, &release](
            std::size_t relative, std::stop_token stop) {
          if (relative >= 32U) {
            return;
          }
          blockers_entered.fetch_add(1U, std::memory_order_release);
          while (!release.load(std::memory_order_acquire) &&
                 !stop.stop_requested()) {
            std::this_thread::yield();
          }
          completed.fetch_add(1U, std::memory_order_release);
        },
        all, index);
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

  const std::array<const sanitize::internal::TaskArenaSubmissionPlan *, 4>
      plans{&upstream, &compact, &output, &all};
  // Keep the saturation probe inside the arena's explicit admission bound.
  // All blockers are already active, so the entire queue budget is available
  // to these four producers without relying on rejected submissions.
  const auto tasks_per_plan = std::max<std::size_t>(
      16U, arena->queue_capacity() / plans.size());
  const std::array<std::size_t, 4> widths{
      upstream_width, compact_width, output_width, workers};
  std::array<std::jthread, 4> producers;
  for (std::size_t producer = 0; producer < producers.size(); ++producer) {
    producers[producer] = std::jthread([&, producer] {
      for (std::size_t ordinal = 0; ordinal < tasks_per_plan; ++ordinal) {
        const auto status = arena->Submit(
            [&, producer](std::size_t relative, std::stop_token) {
              if (relative >= widths[producer]) {
                failed.store(true, std::memory_order_release);
              }
              completed.fetch_add(1U, std::memory_order_release);
            },
            *plans[producer], 0U);
        if (!status.ok()) {
          failed.store(true, std::memory_order_release);
          return;
        }
      }
    });
  }
  for (auto &producer : producers) {
    if (producer.joinable()) {
      producer.join();
    }
  }

  release.store(true, std::memory_order_release);
  const auto queued_tasks = tasks_per_plan * plans.size();
  const auto expected = workers + queued_tasks;
  if (!WaitFor([&] {
        return completed.load(std::memory_order_acquire) == expected &&
               arena->active_tasks() == 0U && arena->queued_tasks() == 0U;
      })) {
    return false;
  }

  const bool exact = !failed.load(std::memory_order_acquire) &&
                     arena->submitted_tasks() == expected &&
                     arena->started_workers() == workers &&
                     arena->peak_active_tasks() == workers &&
                     (workers <= 2U || arena->stolen_tasks() > 0U);
  arena->Shutdown();
  return exact;
}

} // namespace

int main() {
  for (const auto workers : {2U, 3U, 5U, 8U, 16U, 32U}) {
    if (!Run(workers)) {
      return 1;
    }
  }
  return 0;
}
