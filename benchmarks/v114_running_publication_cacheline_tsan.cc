#include "internal/runtime/operation_task_arena.hh"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <thread>

int main() {
  for (const auto workers : {2U, 4U, 8U, 16U}) {
    auto made = sanitize::internal::OperationTaskArena::Make(workers);
    assert(made.ok());
    auto arena = std::move(made).ValueOrDie();
    const auto plan = arena->PrepareSubmissionPlan(
        workers, sanitize::internal::TaskArenaLane::kAll);
    std::atomic<std::size_t> completed{0};
    constexpr std::size_t kTasks = 4096;
    for (std::size_t ordinal = 0; ordinal < kTasks; ++ordinal) {
      const auto status = arena->Submit(
          [&completed](std::size_t, std::stop_token) {
            completed.fetch_add(1, std::memory_order_relaxed);
          },
          plan);
      assert(status.ok());
    }
    while (completed.load(std::memory_order_acquire) != kTasks) {
      std::this_thread::yield();
    }
    while (arena->active_tasks() != 0U || arena->queued_tasks() != 0U) {
      std::this_thread::yield();
    }
    assert(arena->submitted_tasks() == kTasks);
    assert(arena->peak_active_tasks() > 0U);
    arena->Shutdown();
  }
}
