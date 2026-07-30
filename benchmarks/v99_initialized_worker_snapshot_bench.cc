#include "internal/runtime/operation_task_arena.hh"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <thread>

using sanitize::internal::OperationTaskArena;
using sanitize::internal::TaskArenaLane;

int main(int argc, char** argv) {
  const std::size_t workers = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 5;
  const std::size_t tasks = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 50000;
  auto made = OperationTaskArena::Make(workers);
  if (!made.ok()) { std::cerr << made.status().message() << '\n'; return 2; }
  auto arena = *std::move(made);
  auto plan = arena->PrepareSubmissionPlan(workers, TaskArenaLane::kAll);
  std::atomic<std::size_t> arrived{0};
  std::atomic<bool> release{false};
  for (std::size_t i=0; i<workers; ++i) {
    auto st = arena->Submit([&](std::size_t, std::stop_token stop) {
      arrived.fetch_add(1, std::memory_order_release);
      arrived.notify_one();
      if (!stop.stop_requested()) {
        release.wait(false, std::memory_order_acquire);
      }
    }, plan);
    if (!st.ok()) { std::cerr << st.message() << '\n'; return 3; }
    auto seen = arrived.load(std::memory_order_acquire);
    while (seen != i + 1U) {
      arrived.wait(seen, std::memory_order_acquire);
      seen = arrived.load(std::memory_order_acquire);
    }
  }

  const auto begin = std::chrono::steady_clock::now();
  for (std::size_t i=0; i<tasks; ++i) {
    auto st = arena->Submit([](std::size_t, std::stop_token) {}, plan);
    if (!st.ok()) { std::cerr << st.message() << '\n'; return 4; }
  }
  const auto end = std::chrono::steady_clock::now();
  release.store(true, std::memory_order_release);
  release.notify_all();
  while (arena->queued_tasks() != 0 || arena->active_tasks() != 0) {
    std::this_thread::yield();
  }
  const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count();
  std::cout << ns << ' ' << arena->submitted_tasks() << ' ' << arena->started_workers() << ' '
            << arena->queued_tasks() << '\n';
  arena->Shutdown();
}
