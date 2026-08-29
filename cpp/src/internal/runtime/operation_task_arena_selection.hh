// Implements round-robin worker candidate ordering for operation task
// arenas. Normalized origins keep compact and wide lane selection
// deterministic and fair.

#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace sanitize::internal::task_arena_detail {

struct OrderedLaneCandidates final {
  std::uint64_t first = 0;
  std::uint64_t wrapped = 0;
  std::size_t preferred = 0;
  bool full_lane = false;
};

struct NormalizedLaneOriginTag final {};
inline constexpr NormalizedLaneOriginTag kNormalizedLaneOrigin{};

/// Splits eligible lane bits at an already-normalized lane-relative origin.
[[nodiscard]] constexpr OrderedLaneCandidates
ordered_lane_candidates(std::uint64_t candidates, std::size_t begin,
                        std::size_t width, std::size_t start,
                        NormalizedLaneOriginTag) noexcept {
  const auto width_mask = (std::uint64_t{1} << width) - 1U;
  const auto relative = (candidates >> begin) & width_mask;
  const auto before_start = (std::uint64_t{1} << start) - 1U;
  // Keep lane-relative bit positions intact and split only at the round-robin
  // origin. Callers visit set bits at/after the ticket first, then the wrapped
  // prefix. Sparse candidate sets therefore avoid scanning empty slots and
  // evaluating modulo for every offset, while a fully eligible lane can use
  // the direct preferred index.
  return {
      .first = relative & ~before_start,
      .wrapped = relative & before_start,
      .preferred = begin + start,
      .full_lane = relative == width_mask,
  };
}

/// Normalizes a submission ticket before ordering eligible worker lanes.
[[nodiscard]] constexpr OrderedLaneCandidates
ordered_lane_candidates(std::uint64_t candidates, std::size_t begin,
                        std::size_t width, std::size_t ticket) noexcept {
  const auto start = ticket % width;
  return ordered_lane_candidates(candidates, begin, width, start,
                                 kNormalizedLaneOrigin);
}

/// Advances a normalized lane origin by a delta that callers bound to `width`
/// (one or the precompiled half-lane offset). On overflow, the fallback
/// preserves unsigned `(ticket + delta) % width` semantics exactly.
[[nodiscard]] constexpr std::size_t
advance_normalized_lane_origin(std::size_t ticket, std::size_t origin,
                               std::size_t delta, std::size_t width) noexcept {
  if (ticket > std::numeric_limits<std::size_t>::max() - delta) {
    return (ticket + delta) % width;
  }
  const auto advanced = origin + delta;
  return advanced >= width ? advanced - width : advanced;
}

/// Returns the first in-range worker index from an ordered candidate set.
[[nodiscard]] constexpr std::size_t
first_ordered_lane_index(const OrderedLaneCandidates &ordered,
                         std::size_t begin, std::size_t end) noexcept {
  if (ordered.first != 0U) {
    return begin + static_cast<std::size_t>(std::countr_zero(ordered.first));
  }
  if (ordered.wrapped != 0U) {
    return begin + static_cast<std::size_t>(std::countr_zero(ordered.wrapped));
  }
  return end;
}

} // namespace sanitize::internal::task_arena_detail
