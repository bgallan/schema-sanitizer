// Parses user-supplied temporal regex capture groups into bounded fields.
// Digit, sign, timezone, and width validation is kept independent from the
// higher-level mapping of configured captures onto temporal parts.

#include "internal/planning/temporal/parts.hh"

#include <cstddef>
#include <limits>

namespace sanitize::internal {
namespace {

/// Returns whether one byte is an ASCII decimal digit.
bool is_digit(char c) noexcept { return c >= '0' && c <= '9'; }

/// Parses an exact number of decimal digits and advances the input position.
bool parse_n_digits(std::string_view value, std::size_t *position, int count,
                    int *out) {
  if (!position || !out || count < 0 || *position > value.size() ||
      static_cast<std::size_t>(count) > value.size() - *position) {
    return false;
  }
  int parsed = 0;
  for (int index = 0; index < count; ++index) {
    const char current = value[*position + static_cast<std::size_t>(index)];
    if (!is_digit(current)) {
      return false;
    }
    parsed = parsed * 10 + (current - '0');
  }
  *position += static_cast<std::size_t>(count);
  *out = parsed;
  return true;
}

/// Parses a UTC marker or signed timezone offset into total seconds.
bool parse_tz_offset(std::string_view timezone, int *out_seconds) {
  if (!out_seconds || timezone.empty()) {
    return false;
  }
  if (timezone == "Z" || timezone == "z") {
    *out_seconds = 0;
    return true;
  }
  if (timezone[0] != '+' && timezone[0] != '-') {
    return false;
  }
  if (timezone.size() != 5 && timezone.size() != 6) {
    return false;
  }

  const int sign = timezone[0] == '-' ? -1 : 1;
  int hours = 0;
  int minutes = 0;
  std::size_t position = 1;
  if (!parse_n_digits(timezone, &position, 2, &hours)) {
    return false;
  }
  if (timezone.size() == 6) {
    if (timezone[position] != ':') {
      return false;
    }
    ++position;
  }
  if (!parse_n_digits(timezone, &position, 2, &minutes)) {
    return false;
  }
  if (hours > 23 || minutes > 59) {
    return false;
  }
  *out_seconds = sign * (hours * 3600 + minutes * 60);
  return true;
}

} // namespace

bool parse_int_captured(std::string_view value, int *out) {
  if (!out || value.empty()) {
    return false;
  }

  std::size_t position = 0;
  int sign = 1;
  if (value[position] == '+' || value[position] == '-') {
    sign = value[position] == '-' ? -1 : 1;
    ++position;
    if (position == value.size()) {
      return false;
    }
  }

  int64_t parsed = 0;
  constexpr int64_t kLimit =
      static_cast<int64_t>(std::numeric_limits<int>::max()) + 1;
  for (; position < value.size(); ++position) {
    const char current = value[position];
    if (!is_digit(current)) {
      return false;
    }
    parsed = parsed * 10 + static_cast<int64_t>(current - '0');
    if (parsed > kLimit) {
      return false;
    }
  }

  parsed *= sign;
  if (parsed < std::numeric_limits<int>::min() ||
      parsed > std::numeric_limits<int>::max()) {
    return false;
  }
  *out = static_cast<int>(parsed);
  return true;
}

bool parse_fraction_capture(
    const std::match_results<std::string_view::const_iterator> &matches,
    TemporalParts *parts) {
  if (matches.size() < 8 || !matches[7].matched) {
    return true;
  }
  const std::string_view fraction{matches[7].first, matches[7].second};
  if (fraction.empty() || fraction.size() > 9) {
    return false;
  }
  for (char current : fraction) {
    if (!is_digit(current)) {
      return false;
    }
    parts->frac_ns = parts->frac_ns * 10 + static_cast<int64_t>(current - '0');
  }
  for (std::size_t index = fraction.size(); index < 9; ++index) {
    parts->frac_ns *= 10;
  }
  return true;
}

bool parse_timezone_capture(
    const std::match_results<std::string_view::const_iterator> &matches,
    TemporalParts *parts) {
  if (matches.size() < 9 || !matches[8].matched) {
    return true;
  }
  const std::string_view timezone{matches[8].first, matches[8].second};
  if (!parse_tz_offset(timezone, &parts->tz_offset_seconds)) {
    return false;
  }
  parts->has_tz = true;
  return true;
}

} // namespace sanitize::internal
