#include "internal/runtime/operation_task_arena.hh"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <memory>
#include <stop_token>
#include <thread>
#include <vector>

namespace {

using sanitize::internal::OperationTaskArena;
using sanitize::internal::TaskArenaLane;

[[nodiscard]] bool WaitFor(const auto &predicate,
                           std::chrono::seconds timeout =
                               std::chrono::seconds(10)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!predicate() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return predicate();
}

void VerifyQueuedClosuresAreReleasedBeforeWorkerDetach() {
  auto made = OperationTaskArena::Make(2U);
  assert(made.ok());
  auto arena = std::move(made).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(2U, TaskArenaLane::kAll);

  std::atomic<std::size_t> blockers{0U};
  std::atomic<std::size_t> terminals{0U};
  std::atomic<bool> release{false};
  for (std::size_t ticket = 0; ticket < 2U; ++ticket) {
    const auto status = arena->Submit(
        [&blockers, &terminals, &release](std::size_t, std::stop_token) {
          blockers.fetch_add(1U, std::memory_order_release);
          while (!release.load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
          terminals.fetch_add(1U, std::memory_order_release);
        },
        plan, ticket);
    assert(status.ok());
  }
  assert(WaitFor([&] {
    return blockers.load(std::memory_order_acquire) == 2U;
  }));

  std::vector<std::weak_ptr<int>> retained;
  retained.reserve(64U);
  for (std::size_t index = 0; index < 64U; ++index) {
    auto payload = std::make_shared<int>(static_cast<int>(index));
    retained.emplace_back(payload);
    const auto status = arena->Submit(
        [payload = std::move(payload)](std::size_t, std::stop_token) {
          (void)payload;
        },
        plan, index + 2U);
    assert(status.ok());
  }
  assert(arena->queued_tasks() == retained.size());

  arena->Shutdown();
  assert(arena->abandoned_queued_tasks() == retained.size());
  assert(arena->abandoned_queued_bytes() >= retained.size() * 256U);
  assert(arena->detached_workers() == 2U);
  assert(arena->shutdown_timeouts() >= 1U);
  for (const auto &payload : retained) {
    assert(payload.expired());
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2));
  assert(arena->detached_worker_age_millis() >= 1U);

  release.store(true, std::memory_order_release);
  assert(WaitFor([&] {
    return terminals.load(std::memory_order_acquire) == 2U;
  }));
}

void VerifyConcurrentPublicCallsAgainstShutdown() {
  auto made = OperationTaskArena::Make(4U);
  assert(made.ok());
  auto arena = std::move(made).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(4U, TaskArenaLane::kAll);
  std::atomic<bool> stop{false};
  std::jthread producer([&](std::stop_token) {
    std::size_t ticket = 0U;
    while (!stop.load(std::memory_order_acquire)) {
      (void)arena->Submit([](std::size_t, std::stop_token) {}, plan, ticket++);
    }
  });
  std::jthread observer([&](std::stop_token) {
    while (!stop.load(std::memory_order_acquire)) {
      (void)arena->worker_count();
      (void)arena->queued_tasks();
      (void)arena->submitted_tasks();
      (void)arena->memory_resource();
    }
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  arena->Shutdown();
  stop.store(true, std::memory_order_release);
  producer.join();
  observer.join();
  const auto stale = arena->Submit(
      [](std::size_t, std::stop_token) {}, plan, 0U);
  assert(!stale.ok());
}

void VerifyInlineAdmissionOutlivesBoundedShutdownSafely() {
  auto made = OperationTaskArena::Make(1U);
  assert(made.ok());
  auto arena = std::move(made).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(1U, TaskArenaLane::kAll);
  std::atomic<bool> entered{false};
  std::atomic<bool> release{false};
  std::jthread caller([&](std::stop_token) {
    const auto status = arena->Submit(
        [&entered, &release](std::size_t, std::stop_token) {
          entered.store(true, std::memory_order_release);
          while (!release.load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
        },
        plan, 0U);
    assert(status.ok());
  });
  assert(WaitFor([&] { return entered.load(std::memory_order_acquire); }));
  arena->Shutdown();
  assert(arena->shutdown_timeouts() >= 1U);
  release.store(true, std::memory_order_release);
  caller.join();
}

} // namespace

int main() {
  VerifyQueuedClosuresAreReleasedBeforeWorkerDetach();
  VerifyConcurrentPublicCallsAgainstShutdown();
  VerifyInlineAdmissionOutlivesBoundedShutdownSafely();
  return 0;
}
