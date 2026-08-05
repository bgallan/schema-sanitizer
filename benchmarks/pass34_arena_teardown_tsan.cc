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

[[nodiscard]] bool WaitFor(const auto &predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!predicate() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return predicate();
}

void VerifyActiveAndQueuedBytesShareOneCapacity() {
  auto made = OperationTaskArena::Make(2U);
  assert(made.ok());
  auto arena = std::move(made).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(2U, TaskArenaLane::kAll);
  std::atomic<std::size_t> entered{0U};
  std::atomic<bool> release{false};
  constexpr std::size_t active_charge = 4096U;
  for (std::size_t ticket = 0U; ticket < 2U; ++ticket) {
    const auto status = arena->SubmitCharged(
        [&entered, &release](std::size_t, std::stop_token) {
          entered.fetch_add(1U, std::memory_order_release);
          while (!release.load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
        },
        plan, ticket, TaskMemoryCharge{active_charge});
    assert(status.ok());
  }
  assert(WaitFor([&] {
    return entered.load(std::memory_order_acquire) == 2U;
  }));
  assert(arena->active_retained_bytes() >= 2U * active_charge);
  assert(arena->retained_bytes() >= arena->active_retained_bytes());

  auto payload = std::make_shared<int>(7);
  const auto weak_payload = std::weak_ptr<int>(payload);
  constexpr std::size_t queued_charge = 2048U;
  const auto queued = arena->SubmitCharged(
      [payload = std::move(payload)](std::size_t, std::stop_token) {
        (void)payload;
      },
      plan, 2U, TaskMemoryCharge{queued_charge});
  assert(queued.ok());
  assert(arena->queued_retained_bytes() >= queued_charge);
  assert(arena->retained_bytes() >= 2U * active_charge + queued_charge);

  arena->Shutdown();
  assert(WaitFor([&] { return weak_payload.expired(); }));
  // Shutdown retires the arena's public State snapshot immediately.  The
  // detached workers retain their private control block until the active Tasks
  // return, so the public byte gauges intentionally read as zero here.
  assert(arena->queued_retained_bytes() == 0U);
  assert(arena->active_retained_bytes() == 0U);
  assert(arena->retained_bytes() == 0U);

  release.store(true, std::memory_order_release);
  assert(WaitFor([&] { return arena->detached_workers() == 0U; }));
}

} // namespace

int main() {
  VerifyActiveAndQueuedBytesShareOneCapacity();
  return 0;
}
