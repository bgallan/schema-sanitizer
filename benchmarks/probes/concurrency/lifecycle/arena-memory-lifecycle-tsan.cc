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

[[nodiscard]] bool WaitFor(const auto &predicate,
                           std::chrono::seconds timeout =
                               std::chrono::seconds(10)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!predicate() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return predicate();
}

void VerifyByteCapacityAndReentrantQueueDestruction() {
  auto made = OperationTaskArena::Make(2U);
  assert(made.ok());
  auto arena = std::move(made).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(2U, TaskArenaLane::kAll);

  std::atomic<std::size_t> entered{0U};
  std::atomic<bool> release{false};
  for (std::size_t ticket = 0; ticket < 2U; ++ticket) {
    const auto status = arena->Submit(
        [&entered, &release](std::size_t, std::stop_token) {
          entered.fetch_add(1U, std::memory_order_release);
          while (!release.load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
        },
        plan, ticket);
    assert(status.ok());
  }
  assert(WaitFor([&] { return entered.load(std::memory_order_acquire) == 2U; }));

  struct ReentrantPayload final {
    std::weak_ptr<OperationTaskArena> arena;
    std::atomic<bool> *destroyed;
    ~ReentrantPayload() {
      if (const auto owner = arena.lock()) {
        (void)owner->queued_tasks();
        (void)owner->detached_workers();
      }
      destroyed->store(true, std::memory_order_release);
    }
  };

  std::atomic<bool> destroyed{false};
  auto payload = std::make_shared<ReentrantPayload>(
      ReentrantPayload{.arena = arena, .destroyed = &destroyed});
  const auto weak_payload = std::weak_ptr<ReentrantPayload>(payload);
  const auto charge = arena->queue_byte_capacity() / 2U + 1U;
  auto first = arena->SubmitCharged(
      [payload = std::move(payload)](std::size_t, std::stop_token) {
        (void)payload;
      },
      plan, 2U, TaskMemoryCharge{charge});
  assert(first.ok());
  auto rejected = arena->SubmitCharged(
      [](std::size_t, std::stop_token) {}, plan, 3U,
      TaskMemoryCharge{charge});
  assert(!rejected.ok());
  assert(arena->rejected_byte_submissions() >= 1U);

  arena->Shutdown();
  assert(destroyed.load(std::memory_order_acquire));
  assert(weak_payload.expired());
  assert(arena->abandoned_queued_bytes() >= charge);
  assert(arena->total_detached_workers() == 2U);
  assert(arena->detached_workers() == 2U);

  release.store(true, std::memory_order_release);
  assert(WaitFor([&] { return arena->detached_workers() == 0U; }));
  assert(arena->detached_worker_age_millis() == 0U);
}

} // namespace

int main() {
  VerifyByteCapacityAndReentrantQueueDestruction();
  return 0;
}
