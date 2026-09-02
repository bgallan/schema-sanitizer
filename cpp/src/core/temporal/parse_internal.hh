// Provides shared fixed-width digit parsing for temporal primitives.
// The inline helpers validate input bounds and ASCII digits before committing
// two- or four-digit numeric fields to caller-owned output.

#pragma once

#include <cstddef>
#include <string_view>

namespace sanitize::temporal_internal {

/// Returns whether one byte is an ASCII decimal digit.
inline bool is_digit(char c) noexcept { return c >= '0' && c <= '9'; }

/// Parses exactly two decimal digits at the requested input offset.
inline bool parse_2d(std::string_view s, std::size_t pos, int *out) {
  if (pos + 1 >= s.size())
    return false;
  const char a = s[pos];
  const char b = s[pos + 1];
  if (!is_digit(a) || !is_digit(b))
    return false;
  *out = (a - '0') * 10 + (b - '0');
  return true;
}

/// Parses exactly four decimal digits at the requested input offset.
inline bool parse_4d(std::string_view s, std::size_t pos, int *out) {
  if (pos + 3 >= s.size())
    return false;
  int value = 0;
  for (int i = 0; i < 4; ++i) {
    const char c = s[pos + static_cast<std::size_t>(i)];
    if (!is_digit(c))
      return false;
    value = value * 10 + (c - '0');
  }
  *out = value;
  return true;
}

} // namespace sanitize::temporal_internal
