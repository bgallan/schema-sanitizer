// Parses strict integer scalar text values with complete-consumption checks.
// The parser accepts an optional sign, rejects non-ASCII digits, and reports
// overflow without modifying the caller's output.

#include "sanitize/core/primitives.hh"

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <system_error>

namespace sanitize {
namespace {

/// Returns whether one byte is an ASCII decimal digit.
bool is_digit(char c) noexcept { return c >= '0' && c <= '9'; }

} // namespace

bool parse_int64_strict(std::string_view s, int64_t *out) {
  if (!out || s.empty()) {
    return false;
  }

  std::size_t pos = 0;
  if (s[pos] == '+' || s[pos] == '-') {
    ++pos;
    if (pos == s.size()) {
      return false;
    }
  }
  for (std::size_t i = pos; i < s.size(); ++i) {
    if (!is_digit(s[i])) {
      return false;
    }
  }

  int64_t value = 0;
  const auto result = std::from_chars(s.data(), s.data() + s.size(), value, 10);
  if (result.ec != std::errc{} || result.ptr != s.data() + s.size()) {
    return false;
  }
  *out = value;
  return true;
}

} // namespace sanitize
