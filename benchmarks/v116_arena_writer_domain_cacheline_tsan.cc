#include "internal/runtime/operation_task_arena.hh"

#include <array>
#include <atomic>
#include <barrier>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <stop_token>
#include <thread>

namespace {

[[nodiscard]] bool wait_for(const auto &predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (!predicate() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return predicate();
}

void verify_concurrent_writer_domains(std::size_t workers) {
  auto made = sanitize::internal::OperationTaskArena::Make(workers);
  assert(made.ok());
  auto arena = std::move(made).ValueOrDie();

  const auto upstream = arena->PrepareSubmissionPlan(
      std::max<std::size_t>(1U, workers / 2U),
      sanitize::internal::TaskArenaLane::kUpstream);
  const auto output = arena->PrepareSubmissionPlan(
      std::max<std::size_t>(1U, workers - workers / 2U),
      sanitize::internal::TaskArenaLane::kOutput);
  const auto all = arena->PrepareSubmissionPlan(
      workers, sanitize::internal::TaskArenaLane::kAll);
  const std::array<const sanitize::internal::TaskArenaSubmissionPlan *, 3>
      plans{&upstream, &output, &all};

  constexpr std::size_t kTasksPerProducer = 768U;
  constexpr std::size_t kProducerCount = 3U;
  constexpr std::size_t kTasks = kTasksPerProducer * kProducerCount;
  std::atomic<std::size_t> completed{0};
  std::barrier start_gate(static_cast<std::ptrdiff_t>(kProducerCount + 1U));
  std::array<std::jthread, kProducerCount> producers;

  for (std::size_t producer = 0; producer < producers.size(); ++producer) {
    producers[producer] = std::jthread(
        [&, producer](std::stop_token) {
          start_gate.arrive_and_wait();
          for (std::size_t ordinal = 0; ordinal < kTasksPerProducer;
               ++ordinal) {
            const auto status = arena->Submit(
                [&completed](std::size_t, std::stop_token) {
                  for (std::size_t spin = 0; spin < 32U; ++spin) {
                    std::atomic_signal_fence(std::memory_order_seq_cst);
                  }
                  completed.fetch_add(1U, std::memory_order_release);
                },
                *plans[producer],
                producer * kTasksPerProducer + ordinal);
            assert(status.ok());
          }
        });
  }

  start_gate.arrive_and_wait();
  for (auto &producer : producers) {
    producer.join();
  }
  assert(wait_for([&] {
    return completed.load(std::memory_order_acquire) == kTasks;
  }));
  assert(wait_for([&] {
    return arena->active_tasks() == 0U && arena->queued_tasks() == 0U;
  }));
  assert(arena->submitted_tasks() == kTasks);
  assert(arena->peak_active_tasks() > 0U);
  assert(arena->started_workers() <= workers);
  assert(arena->wake_epoch_publishes() > 0U);
  arena->Shutdown();
}

} // namespace

int main() {
  for (const auto workers : {2U, 4U, 8U, 16U, 32U}) {
    verify_concurrent_writer_domains(workers);
  }
}
