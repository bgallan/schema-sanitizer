#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/operation_task_arena_selection.hh"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <stop_token>
#include <thread>

namespace {
using sanitize::internal::task_arena_detail::advance_normalized_lane_origin;
using sanitize::internal::task_arena_detail::kNormalizedLaneOrigin;
using sanitize::internal::task_arena_detail::ordered_lane_candidates;

void verify_lane_origin_equivalence() {
  std::mt19937_64 random(0x1180A11ULL);
  for (std::size_t width = 1U; width <= 32U; ++width) {
    const auto alternative = std::max<std::size_t>(1U, width / 2U);
    const std::array<std::size_t, 9> edge_tickets{
        0U,
        1U,
        width - 1U,
        width,
        width + 1U,
        std::numeric_limits<std::size_t>::max() - alternative,
        std::numeric_limits<std::size_t>::max() - 1U,
        std::numeric_limits<std::size_t>::max(),
        static_cast<std::size_t>(random()),
    };
    for (const auto ticket : edge_tickets) {
      const auto origin = ticket % width;
      assert(advance_normalized_lane_origin(ticket, origin, alternative,
                                            width) ==
             (ticket + alternative) % width);
      assert(advance_normalized_lane_origin(ticket, origin, 1U, width) ==
             (ticket + 1U) % width);
      const auto candidates = (std::uint64_t{1} << width) - 1U;
      const auto baseline =
          ordered_lane_candidates(candidates, 0U, width, ticket);
      const auto candidate = ordered_lane_candidates(
          candidates, 0U, width, origin, kNormalizedLaneOrigin);
      assert(baseline.first == candidate.first);
      assert(baseline.wrapped == candidate.wrapped);
      assert(baseline.preferred == candidate.preferred);
      assert(baseline.full_lane == candidate.full_lane);
    }
    for (std::size_t iteration = 0; iteration < 100'000U; ++iteration) {
      const auto ticket = static_cast<std::size_t>(random());
      const auto origin = ticket % width;
      assert(advance_normalized_lane_origin(ticket, origin, alternative,
                                            width) ==
             (ticket + alternative) % width);
      assert(advance_normalized_lane_origin(ticket, origin, 1U, width) ==
             (ticket + 1U) % width);
    }
  }
}

[[nodiscard]] bool wait_for(const auto &predicate) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (!predicate() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return predicate();
}

[[nodiscard]] bool verify_real_arena(std::size_t workers) {
  auto made = sanitize::internal::OperationTaskArena::Make(workers);
  if (!made.ok()) {
    return false;
  }
  auto arena = std::move(made).ValueOrDie();
  const auto upstream = arena->PrepareSubmissionPlan(
      std::max<std::size_t>(1U, workers / 2U),
      sanitize::internal::TaskArenaLane::kUpstream);
  const auto output = arena->PrepareSubmissionPlan(
      std::max<std::size_t>(1U, workers - workers / 2U),
      sanitize::internal::TaskArenaLane::kOutput);
  const auto all = arena->PrepareSubmissionPlan(
      workers, sanitize::internal::TaskArenaLane::kAll);

  constexpr std::size_t kTasksPerProducer = 512U;
  constexpr std::size_t kProducerCount = 3U;
  std::atomic<std::size_t> completed{0};
  std::atomic<bool> release{false};
  std::atomic<bool> failed{false};
  std::array<std::jthread, kProducerCount> producers;
  const std::array<const sanitize::internal::TaskArenaSubmissionPlan *,
                   kProducerCount>
      plans{&upstream, &output, &all};

  for (std::size_t producer = 0; producer < kProducerCount; ++producer) {
    producers[producer] = std::jthread(
        [&, producer] {
          auto ticket = std::numeric_limits<std::size_t>::max() - 170U +
                        producer * 17U;
          for (std::size_t ordinal = 0; ordinal < kTasksPerProducer;
               ++ordinal, ++ticket) {
            const auto status = arena->Submit(
                [&completed, &release](std::size_t, std::stop_token stop) {
                  while (!release.load(std::memory_order_acquire) &&
                         !stop.stop_requested()) {
                    std::this_thread::yield();
                  }
                  completed.fetch_add(1U, std::memory_order_release);
                },
                *plans[producer], ticket);
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
  if (failed.load(std::memory_order_acquire)) {
    return false;
  }
  release.store(true, std::memory_order_release);
  constexpr auto kTasks = kTasksPerProducer * kProducerCount;
  if (!wait_for([&] {
        return completed.load(std::memory_order_acquire) == kTasks &&
               arena->active_tasks() == 0U && arena->queued_tasks() == 0U;
      })) {
    return false;
  }
  const auto exact = arena->submitted_tasks() == kTasks &&
                     arena->started_workers() >= 1U &&
                     arena->started_workers() <= workers &&
                     arena->peak_active_tasks() >= 1U &&
                     arena->peak_active_tasks() <= workers;
  arena->Shutdown();
  return exact;
}
} // namespace

int main() {
  verify_lane_origin_equivalence();
  for (const auto workers : {2U, 3U, 4U, 5U, 8U, 16U, 32U}) {
    if (!verify_real_arena(workers)) {
      return 1;
    }
  }
  return 0;
}
