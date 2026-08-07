#include "internal/runtime/operation_task_arena.hh"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <memory>
#include <stop_token>
#include <thread>

namespace {

using sanitize::internal::OperationTaskArena;
using sanitize::internal::TaskArenaLane;
using sanitize::internal::TaskMemoryCharge;
using sanitize::internal::TaskMemoryLease;

[[nodiscard]] bool WaitFor(const auto &predicate,
                           std::chrono::seconds timeout =
                               std::chrono::seconds(10)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!predicate() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return predicate();
}

struct BlockingPayload final {
  std::atomic<bool> *entered;
  std::atomic<bool> *release;

  BlockingPayload(std::atomic<bool> *entered_value,
                  std::atomic<bool> *release_value) noexcept
      : entered(entered_value), release(release_value) {}

  ~BlockingPayload() {
    entered->store(true, std::memory_order_release);
    while (!release->load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }
};

void VerifyLeasedPayloadLifetime() {
  auto made = OperationTaskArena::Make(2U);
  assert(made.ok());
  auto arena = std::move(made).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(2U, TaskArenaLane::kAll);
  std::atomic<std::size_t> blockers{0U};
  std::atomic<bool> release{false};
  for (std::size_t ticket = 0; ticket < 2U; ++ticket) {
    assert(arena
               ->SubmitCharged(
                   [&blockers, &release](std::size_t, std::stop_token) {
                     blockers.fetch_add(1U, std::memory_order_release);
                     while (!release.load(std::memory_order_acquire)) {
                       std::this_thread::yield();
                     }
                   },
                   plan, ticket, TaskMemoryCharge{1024U})
               .ok());
  }
  assert(WaitFor([&] {
    return blockers.load(std::memory_order_acquire) == 2U;
  }));

  auto owner = std::make_shared<int>(7);
  const auto weak = std::weak_ptr<int>(owner);
  assert(arena
             ->SubmitLeased(
                 [](std::size_t, std::stop_token) {}, plan,
                 TaskMemoryLease(std::move(owner), 4096U))
             .ok());
  assert(!weak.expired());
  release.store(true, std::memory_order_release);
  assert(WaitFor([&] { return weak.expired(); }));
  arena->Shutdown();
}

void VerifyPostShutdownReaperMetrics() {
  auto made = OperationTaskArena::Make(2U);
  assert(made.ok());
  auto arena = std::move(made).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(2U, TaskArenaLane::kAll);
  std::atomic<std::size_t> blockers{0U};
  std::atomic<bool> release_workers{false};
  for (std::size_t ticket = 0; ticket < 2U; ++ticket) {
    assert(arena
               ->SubmitCharged(
                   [&blockers, &release_workers](std::size_t,
                                                 std::stop_token) {
                     blockers.fetch_add(1U, std::memory_order_release);
                     while (!release_workers.load(std::memory_order_acquire)) {
                       std::this_thread::yield();
                     }
                   },
                   plan, ticket, TaskMemoryCharge{1024U})
               .ok());
  }
  assert(WaitFor([&] {
    return blockers.load(std::memory_order_acquire) == 2U;
  }));

  std::atomic<bool> destructor_entered{false};
  std::atomic<bool> release_destructor{false};
  auto payload = std::make_shared<BlockingPayload>(
      &destructor_entered, &release_destructor);
  assert(arena
             ->SubmitCharged(
                 [payload = std::move(payload)](std::size_t, std::stop_token) {
                   (void)payload;
                 },
                 plan, 2U, TaskMemoryCharge{8192U})
             .ok());
  arena->Shutdown();
  assert(WaitFor([&] {
    return destructor_entered.load(std::memory_order_acquire);
  }));
  assert(arena->reaper_active_states() >= 1U);
  assert(arena->reaper_active_bytes() >= 8192U);
  assert(arena->post_shutdown_retained_bytes() >= 8192U);
  release_destructor.store(true, std::memory_order_release);
  release_workers.store(true, std::memory_order_release);
  assert(WaitFor([&] {
    return arena->reaper_active_states() == 0U &&
           arena->post_shutdown_retained_bytes() == 0U;
  }));
}

} // namespace

int main() {
  VerifyLeasedPayloadLifetime();
  VerifyPostShutdownReaperMetrics();
  return 0;
}
