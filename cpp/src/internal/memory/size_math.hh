// Provides overflow-safe retained-memory accounting helpers.

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

namespace sanitize::internal {

// Converts a size_t byte count to int64_t, saturating instead of wrapping.
[[nodiscard]] constexpr std::int64_t
saturating_size_to_i64(std::size_t value) noexcept {
  constexpr auto kMax = std::numeric_limits<std::int64_t>::max();
  if (value > static_cast<std::size_t>(kMax)) {
    return kMax;
  }
  return static_cast<std::int64_t>(value);
}

// Adds non-negative byte counts, saturating at INT64_MAX.
[[nodiscard]] constexpr std::int64_t
saturating_add_i64(std::int64_t left, std::int64_t right) noexcept {
  constexpr auto kMax = std::numeric_limits<std::int64_t>::max();
  if (left < 0 || right < 0 || left > kMax - right) {
    return kMax;
  }
  return left + right;
}

// Converts an element capacity to bytes without overflowing size_t or int64_t.
[[nodiscard]] constexpr std::int64_t
saturating_capacity_bytes(std::size_t count, std::size_t width) noexcept {
  constexpr auto kMax = std::numeric_limits<std::int64_t>::max();
  if (width != 0 && count > static_cast<std::size_t>(kMax) / width) {
    return kMax;
  }
  return static_cast<std::int64_t>(count * width);
}

} // namespace sanitize::internal
