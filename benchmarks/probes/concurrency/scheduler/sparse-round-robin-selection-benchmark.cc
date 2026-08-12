#include "internal/runtime/operation_task_arena_selection.hh"

#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <random>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
constexpr std::size_t kIterations = 8'000'000;
constexpr std::size_t kMaskCount = 4096;

[[nodiscard]] constexpr std::uint64_t worker_bit(
    std::size_t index) noexcept {
  return std::uint64_t{1} << index;
}

[[nodiscard]] std::size_t baseline_select(
    std::uint64_t candidates, std::size_t begin, std::size_t width,
    std::size_t ticket,
    const std::array<std::atomic<bool>, 32> &running) noexcept {
  const auto end = begin + width;
  for (std::size_t offset = 0; offset < width; ++offset) {
    const auto index = begin + ((ticket + offset) % width);
    if ((candidates & worker_bit(index)) != 0U &&
        !running[index].load(std::memory_order_acquire)) {
      return index;
    }
  }
  return end;
}

[[nodiscard]] std::size_t candidate_select(
    std::uint64_t candidates, std::size_t begin, std::size_t width,
    std::size_t ticket,
    const std::array<std::atomic<bool>, 32> &running) noexcept {
  const auto end = begin + width;
  auto ordered = sanitize::internal::task_arena_detail::
      ordered_lane_candidates(candidates, begin, width, ticket);
  if (ordered.full_lane) {
    if (!running[ordered.preferred].load(std::memory_order_acquire)) {
      return ordered.preferred;
    }
    ordered.first &= ~worker_bit(ordered.preferred - begin);
  }
  while (ordered.first != 0U) {
    const auto relative =
        static_cast<std::size_t>(std::countr_zero(ordered.first));
    const auto index = begin + relative;
    if (!running[index].load(std::memory_order_acquire)) {
      return index;
    }
    ordered.first &= ordered.first - 1U;
  }
  while (ordered.wrapped != 0U) {
    const auto relative =
        static_cast<std::size_t>(std::countr_zero(ordered.wrapped));
    const auto index = begin + relative;
    if (!running[index].load(std::memory_order_acquire)) {
      return index;
    }
    ordered.wrapped &= ordered.wrapped - 1U;
  }
  return end;
}

using Selector = std::size_t (*)(
    std::uint64_t, std::size_t, std::size_t, std::size_t,
    const std::array<std::atomic<bool>, 32> &) noexcept;

[[nodiscard]] std::vector<std::uint64_t> make_masks(
    std::size_t begin, std::size_t width, std::size_t candidates_per_mask) {
  std::mt19937_64 random(0xC011AB1EULL + width + candidates_per_mask);
  std::vector<std::uint64_t> masks(kMaskCount, 0U);
  for (auto &mask : masks) {
    while (static_cast<std::size_t>(std::popcount(mask)) <
           candidates_per_mask) {
      mask |= worker_bit(begin + (random() % width));
    }
  }
  return masks;
}

[[nodiscard]] std::uint64_t run(Selector selector, std::size_t begin,
                                std::size_t width,
                                std::size_t candidates_per_mask) {
  std::array<std::atomic<bool>, 32> running{};
  for (auto &value : running) {
    value.store(false, std::memory_order_relaxed);
  }
  const auto masks = make_masks(begin, width, candidates_per_mask);
  std::size_t checksum = 0;
  const auto start = Clock::now();
  for (std::size_t iteration = 0; iteration < kIterations; ++iteration) {
    checksum += selector(masks[iteration & (kMaskCount - 1U)], begin, width,
                         iteration, running);
  }
  const auto elapsed = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start)
          .count());
  if (checksum == 0U) {
    std::cerr << "unreachable checksum\n";
  }
  return elapsed;
}
} // namespace

int main(int argc, char **argv) {
  const auto width =
      argc > 1 ? static_cast<std::size_t>(std::stoul(argv[1])) : 16U;
  const auto candidates =
      argc > 2 ? static_cast<std::size_t>(std::stoul(argv[2])) : 4U;
  const auto begin = width == 16U ? 8U : 0U;
  std::cout << run(&baseline_select, begin, width, candidates) << ' '
            << run(&candidate_select, begin, width, candidates) << '\n';
}
