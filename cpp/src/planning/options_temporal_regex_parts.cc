// Implements helper parsers for user-supplied temporal regex captures.

#include "internal/planning/options_temporal_regex_parts.hh"

#include <cstddef>
#include <limits>

#include "sanitize/core/primitives.hh"

namespace sanitize::internal {
namespace {

// Returns whether a character is an ASCII decimal digit.
bool is_digit(char c) noexcept { return c >= '0' && c <= '9'; }

// Parses exactly n decimal digits from a string cursor.
bool parse_n_digits(std::string_view s, std::size_t *pos, int n, int *out) {
  if (!pos || !out || n < 0 || *pos > s.size() ||
      static_cast<std::size_t>(n) > s.size() - *pos) {
    return false;
  }
  int value = 0;
  for (int i = 0; i < n; ++i) {
    char c = s[*pos + static_cast<std::size_t>(i)];
    if (!is_digit(c)) {
      return false;
    }
    value = value * 10 + (c - '0');
  }
  *pos += static_cast<std::size_t>(n);
  *out = value;
  return true;
}

// Parses an ISO-8601 timezone offset into signed seconds.
bool parse_tz_offset(std::string_view tz, int *out_seconds) {
  if (!out_seconds || tz.empty()) {
    return false;
  }
  if (tz == "Z" || tz == "z") {
    *out_seconds = 0;
    return true;
  }
  if (tz[0] != '+' && tz[0] != '-') {
    return false;
  }
  if (tz.size() != 5 && tz.size() != 6) {
    return false;
  }

  int sign = (tz[0] == '-') ? -1 : 1;
  int hours = 0;
  int minutes = 0;
  if (tz.size() == 6) {
    if (tz[3] != ':') {
      return false;
    }
    std::size_t pos = 1;
    if (!parse_n_digits(tz, &pos, 2, &hours)) {
      return false;
    }
    ++pos;
    if (!parse_n_digits(tz, &pos, 2, &minutes)) {
      return false;
    }
  } else {
    std::size_t pos = 1;
    if (!parse_n_digits(tz, &pos, 2, &hours)) {
      return false;
    }
    if (!parse_n_digits(tz, &pos, 2, &minutes)) {
      return false;
    }
  }
  if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59) {
    return false;
  }
  *out_seconds = sign * (hours * 3600 + minutes * 60);
  return true;
}

// Returns whether a Gregorian year is a leap year.
bool is_leap_year(int year) {
  return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
}

// Returns the number of days in a Gregorian month.
int days_in_month(int year, int month) {
  switch (month) {
  case 1:
  case 3:
  case 5:
  case 7:
  case 8:
  case 10:
  case 12:
    return 31;
  case 4:
  case 6:
  case 9:
  case 11:
    return 30;
  case 2:
    return is_leap_year(year) ? 29 : 28;
  default:
    return 0;
  }
}

} // namespace

bool ymd_to_days(int year, int month, int day, int32_t *out_days) {
  if (!out_days) {
    return false;
  }
  const int max_day = days_in_month(year, month);
  if (max_day == 0 || day < 1 || day > max_day) {
    return false;
  }
  int64_t days = days_from_civil_epoch(year, static_cast<unsigned>(month),
                                       static_cast<unsigned>(day));
  if (days < std::numeric_limits<int32_t>::min() ||
      days > std::numeric_limits<int32_t>::max()) {
    return false;
  }
  *out_days = static_cast<int32_t>(days);
  return true;
}

bool hms_to_seconds(int hour, int minute, int second, int32_t *out_sec) {
  if (!out_sec) {
    return false;
  }
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 ||
      second > 59) {
    return false;
  }
  *out_sec = hour * 3600 + minute * 60 + second;
  return true;
}

bool parse_temporal_parts_to_timestamp(const TemporalParts &parts,
                                       int64_t *out_ns) {
  if (!out_ns) {
    return false;
  }
  int32_t days = 0;
  if (!ymd_to_days(parts.year, parts.month, parts.day, &days)) {
    return false;
  }
  int32_t seconds = 0;
  if (!hms_to_seconds(parts.hour, parts.minute, parts.second, &seconds)) {
    return false;
  }
  auto total_seconds = static_cast<int64_t>(days) * int64_t{86400} + seconds;
  if (parts.has_tz) {
    total_seconds -= static_cast<int64_t>(parts.tz_offset_seconds);
  }
  constexpr int64_t kNanosPerSecond = 1000000000LL;
  constexpr int64_t kMaxSeconds =
      std::numeric_limits<int64_t>::max() / kNanosPerSecond;
  constexpr int64_t kMaxRemainder =
      std::numeric_limits<int64_t>::max() % kNanosPerSecond;
  constexpr int64_t kMinSeconds =
      std::numeric_limits<int64_t>::min() / kNanosPerSecond;
  if (total_seconds > kMaxSeconds ||
      (total_seconds == kMaxSeconds && parts.frac_ns > kMaxRemainder) ||
      total_seconds < kMinSeconds) {
    return false;
  }
  *out_ns = total_seconds * kNanosPerSecond + parts.frac_ns;
  return true;
}

bool parse_int_captured(std::string_view value, int *out) {
  if (!out || value.empty()) {
    return false;
  }

  std::size_t pos = 0;
  int sign = 1;
  if (value[pos] == '+' || value[pos] == '-') {
    sign = value[pos] == '-' ? -1 : 1;
    ++pos;
    if (pos == value.size()) {
      return false;
    }
  }

  int64_t parsed = 0;
  constexpr int64_t kLimit =
      static_cast<int64_t>(std::numeric_limits<int>::max()) + 1;
  for (; pos < value.size(); ++pos) {
    const char c = value[pos];
    if (!is_digit(c)) {
      return false;
    }
    parsed = parsed * 10 + static_cast<int64_t>(c - '0');
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
  std::string_view fraction{matches[7].first, matches[7].second};
  if (fraction.empty() || fraction.size() > 9) {
    return false;
  }
  for (char c : fraction) {
    if (!is_digit(c)) {
      return false;
    }
    parts->frac_ns = parts->frac_ns * 10 + static_cast<int64_t>(c - '0');
  }
  for (std::size_t i = fraction.size(); i < 9; ++i) {
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
  std::string_view tz{matches[8].first, matches[8].second};
  if (!parse_tz_offset(tz, &parts->tz_offset_seconds)) {
    return false;
  }
  parts->has_tz = true;
  return true;
}

} // namespace sanitize::internal
