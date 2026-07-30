#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/operation_task_arena_selection.hh"

#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <random>
#include <stop_token>
#include <thread>

namespace {

[[nodiscard]] constexpr std::uint64_t worker_bit(
    std::size_t index) noexcept {
  return std::uint64_t{1} << index;
}

[[nodiscard]] std::size_t reference_select(std::uint64_t candidates,
                                           std::uint64_t running,
                                           std::size_t begin,
                                           std::size_t width,
                                           std::size_t ticket) noexcept {
  const auto end = begin + width;
  for (std::size_t offset = 0; offset < width; ++offset) {
    const auto index = begin + ((ticket + offset) % width);
    if ((candidates & worker_bit(index)) != 0U &&
        (running & worker_bit(index)) == 0U) {
      return index;
    }
  }
  return end;
}

[[nodiscard]] std::size_t candidate_select(std::uint64_t candidates,
                                           std::uint64_t running,
                                           std::size_t begin,
                                           std::size_t width,
                                           std::size_t ticket) noexcept {
  const auto end = begin + width;
  auto ordered = sanitize::internal::task_arena_detail::
      ordered_lane_candidates(candidates, begin, width, ticket);
  if (ordered.full_lane) {
    if ((running & worker_bit(ordered.preferred)) == 0U) {
      return ordered.preferred;
    }
    ordered.first &= ~worker_bit(ordered.preferred - begin);
  }
  while (ordered.first != 0U) {
    const auto index = begin + static_cast<std::size_t>(
                                   std::countr_zero(ordered.first));
    if ((running & worker_bit(index)) == 0U) {
      return index;
    }
    ordered.first &= ordered.first - 1U;
  }
  while (ordered.wrapped != 0U) {
    const auto index = begin + static_cast<std::size_t>(
                                   std::countr_zero(ordered.wrapped));
    if ((running & worker_bit(index)) == 0U) {
      return index;
    }
    ordered.wrapped &= ordered.wrapped - 1U;
  }
  return end;
}

void verify_exhaustive_round_robin_equivalence() {
  for (std::size_t width = 1; width <= 8U; ++width) {
    const auto combinations = std::uint64_t{1} << width;
    for (const auto begin : {0U, 8U}) {
      for (std::uint64_t candidates = 0; candidates < combinations;
           ++candidates) {
        const auto physical_candidates = candidates << begin;
        for (std::uint64_t running = 0; running < combinations; ++running) {
          const auto physical_running = running << begin;
          for (std::size_t ticket = 0; ticket < width * 2U; ++ticket) {
            assert(candidate_select(physical_candidates, physical_running,
                                    begin, width, ticket) ==
                   reference_select(physical_candidates, physical_running,
                                    begin, width, ticket));
          }
        }
      }
    }
  }
}

void verify_wide_random_round_robin_equivalence() {
  std::mt19937_64 random(0x515E1EC7ULL);
  for (const auto width : {16U, 24U, 32U}) {
    const auto begin = width == 16U ? 8U : 0U;
    const auto width_mask = (std::uint64_t{1} << width) - 1U;
    for (std::size_t iteration = 0; iteration < 100'000U; ++iteration) {
      const auto candidates = (random() & width_mask) << begin;
      const auto running = (random() & width_mask) << begin;
      const auto ticket = static_cast<std::size_t>(random());
      assert(candidate_select(candidates, running, begin, width, ticket) ==
             reference_select(candidates, running, begin, width, ticket));
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

void verify_real_arena(std::size_t workers) {
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

  constexpr std::size_t kTasks = 4096U;
  std::atomic<std::size_t> completed{0};
  for (std::size_t ordinal = 0; ordinal < kTasks; ++ordinal) {
    const auto &plan = ordinal % 3U == 0U   ? upstream
                       : ordinal % 3U == 1U ? output
                                            : all;
    const auto status = arena->Submit(
        [&completed](std::size_t, std::stop_token) {
          completed.fetch_add(1U, std::memory_order_release);
        },
        plan, ordinal);
    assert(status.ok());
  }
  assert(wait_for([&] {
    return completed.load(std::memory_order_acquire) == kTasks;
  }));
  assert(wait_for([&] {
    return arena->active_tasks() == 0U && arena->queued_tasks() == 0U;
  }));
  assert(arena->submitted_tasks() == kTasks);
  assert(arena->peak_active_tasks() > 0U);
  arena->Shutdown();
}

} // namespace

int main() {
  verify_exhaustive_round_robin_equivalence();
  verify_wide_random_round_robin_equivalence();
  for (const auto workers : {2U, 4U, 8U, 16U, 32U}) {
    verify_real_arena(workers);
  }
}
