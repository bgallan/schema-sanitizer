#include "internal/runtime/operation_task_arena_selection.hh"

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string_view>

namespace {
using Clock = std::chrono::steady_clock;
constexpr std::size_t kIterations = 20'000'000;

[[nodiscard]] std::uint64_t baseline(std::size_t width) noexcept {
  const auto alternative_offset = std::max<std::size_t>(1U, width / 2U);
  const auto candidates = (std::uint64_t{1} << width) - 1U;
  std::uint64_t checksum = 0;
  for (std::size_t ticket = 0; ticket < kIterations; ++ticket) {
    const auto ordered =
        sanitize::internal::task_arena_detail::ordered_lane_candidates(
            candidates, 0U, width, ticket);
    const auto preferred = ticket % width;
    const auto alternative = (ticket + alternative_offset) % width;
    const auto helper = (ticket + 1U) % width;
    checksum += ordered.preferred + preferred + alternative + helper;
  }
  return checksum;
}

[[nodiscard]] std::uint64_t candidate(std::size_t width) noexcept {
  using sanitize::internal::task_arena_detail::advance_normalized_lane_origin;
  using sanitize::internal::task_arena_detail::kNormalizedLaneOrigin;
  using sanitize::internal::task_arena_detail::ordered_lane_candidates;

  const auto alternative_offset = std::max<std::size_t>(1U, width / 2U);
  const auto candidates = (std::uint64_t{1} << width) - 1U;
  std::uint64_t checksum = 0;
  for (std::size_t ticket = 0; ticket < kIterations; ++ticket) {
    const auto origin = ticket % width;
    const auto ordered = ordered_lane_candidates(
        candidates, 0U, width, origin, kNormalizedLaneOrigin);
    const auto alternative = advance_normalized_lane_origin(
        ticket, origin, alternative_offset, width);
    const auto helper =
        advance_normalized_lane_origin(ticket, origin, 1U, width);
    checksum += ordered.preferred + origin + alternative + helper;
  }
  return checksum;
}
} // namespace

int main(int argc, char **argv) {
  if (argc != 3) {
    return 2;
  }
  const std::string_view variant = argv[1];
  const auto width = static_cast<std::size_t>(std::stoull(argv[2]));
  if (width == 0U || width > 32U) {
    return 3;
  }

  const auto started = Clock::now();
  const auto checksum = variant == "baseline" ? baseline(width)
                        : variant == "candidate" ? candidate(width)
                                                   : 0U;
  if (variant != "baseline" && variant != "candidate") {
    return 4;
  }
  const auto elapsed = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                           started)
          .count());
  std::cout << elapsed << ' ' << checksum << '\n';
  return 0;
}
