// Parses ISO temporal primitives into Arrow-compatible scalar values.

#include "sanitize/core/primitives.hh"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>

namespace sanitize {

namespace {

// Returns whether a character is an ASCII decimal digit.
bool is_digit(char c) noexcept { return c >= '0' && c <= '9'; }

// Parses two decimal digits at a fixed position.
bool parse_2d(std::string_view s, std::size_t pos, int *out) {
  if (pos + 1 >= s.size())
    return false;
  char a = s[pos], b = s[pos + 1];
  if (!is_digit(a) || !is_digit(b))
    return false;
  *out = (a - '0') * 10 + (b - '0');
  return true;
}

// Parses four decimal digits at a fixed position.
bool parse_4d(std::string_view s, std::size_t pos, int *out) {
  if (pos + 3 >= s.size())
    return false;
  int v = 0;
  for (int i = 0; i < 4; ++i) {
    char c = s[pos + i];
    if (!is_digit(c))
      return false;
    v = v * 10 + (c - '0');
  }
  *out = v;
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

// Parses an optional fractional second and normalizes it to nanoseconds.
bool parse_fractional_nanoseconds(std::string_view s, std::size_t *pos,
                                  int64_t *out_ns) {
  *out_ns = 0;
  if (*pos >= s.size() || s[*pos] != '.') {
    return true;
  }

  ++*pos;
  int digits = 0;
  int64_t value = 0;
  while (*pos < s.size() && is_digit(s[*pos])) {
    if (digits < 9) {
      value = value * 10 + (s[*pos] - '0');
      ++digits;
    }
    ++*pos;
  }
  if (digits == 0) {
    return false;
  }
  while (digits < 9) {
    value *= 10;
    ++digits;
  }
  *out_ns = value;
  return true;
}

// Parses an optional ISO timezone suffix into signed offset seconds.
bool parse_timezone_offset(std::string_view s, std::size_t *pos,
                           int64_t *out_seconds) {
  *out_seconds = 0;
  if (*pos >= s.size()) {
    return true;
  }
  if (s[*pos] == 'Z') {
    ++*pos;
    return true;
  }
  if (s[*pos] != '+' && s[*pos] != '-') {
    return true;
  }

  const int sign = s[*pos] == '+' ? 1 : -1;
  ++*pos;
  if (*pos + 4 >= s.size()) {
    return false;
  }

  int hours = 0;
  int minutes = 0;
  if (!parse_2d(s, *pos, &hours) || s[*pos + 2] != ':' ||
      !parse_2d(s, *pos + 3, &minutes) || hours > 23 || minutes > 59) {
    return false;
  }
  *pos += 5;
  *out_seconds = sign * (static_cast<int64_t>(hours) * 3600 +
                         static_cast<int64_t>(minutes) * 60);
  return true;
}

// Combines timestamp components while rejecting int64 nanosecond overflow.
bool combine_timestamp_nanoseconds(int32_t days, int32_t seconds,
                                   int64_t fraction_ns,
                                   int64_t timezone_offset_seconds,
                                   int64_t *out_ns) {
  const int64_t total_seconds =
      static_cast<int64_t>(days) * 86400 + seconds - timezone_offset_seconds;
  constexpr int64_t kNanosPerSecond = 1000000000LL;
  constexpr int64_t kMaxSeconds =
      std::numeric_limits<int64_t>::max() / kNanosPerSecond;
  constexpr int64_t kMaxRemainder =
      std::numeric_limits<int64_t>::max() % kNanosPerSecond;
  constexpr int64_t kMinSeconds =
      std::numeric_limits<int64_t>::min() / kNanosPerSecond;
  if (total_seconds > kMaxSeconds ||
      (total_seconds == kMaxSeconds && fraction_ns > kMaxRemainder) ||
      total_seconds < kMinSeconds) {
    return false;
  }
  *out_ns = total_seconds * kNanosPerSecond + fraction_ns;
  return true;
}

} // namespace

// Howard Hinnant's date algorithms (public domain).
int64_t days_from_civil_epoch(int y, unsigned m, unsigned d) {
  y -= m <= 2;
  const int era = (y >= 0 ? y : y - 399) / 400;
  const auto yoe = static_cast<unsigned>(y - era * 400);
  const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return static_cast<int64_t>(era) * 146097 + static_cast<int64_t>(doe) -
         719468;
}

bool parse_iso_date_to_days(std::string_view s, int32_t *out_days) {
  if (!out_days)
    return false;
  if (s.size() != 10)
    return false;
  if (s[4] != '-' || s[7] != '-')
    return false;
  int y = 0, mo = 0, d = 0;
  if (!parse_4d(s, 0, &y))
    return false;
  if (!parse_2d(s, 5, &mo))
    return false;
  if (!parse_2d(s, 8, &d))
    return false;
  const int max_day = days_in_month(y, mo);
  if (max_day == 0 || d < 1 || d > max_day)
    return false;
  int64_t days = days_from_civil_epoch(y, static_cast<unsigned>(mo),
                                       static_cast<unsigned>(d));
  if (days < std::numeric_limits<int32_t>::min() ||
      days > std::numeric_limits<int32_t>::max())
    return false;
  *out_days = static_cast<int32_t>(days);
  return true;
}

bool parse_iso_time_to_seconds(std::string_view s, int32_t *out_seconds) {
  if (!out_seconds)
    return false;
  if (s.size() != 8)
    return false;
  if (s[2] != ':' || s[5] != ':')
    return false;
  int hh = 0, mm = 0, ss = 0;
  if (!parse_2d(s, 0, &hh))
    return false;
  if (!parse_2d(s, 3, &mm))
    return false;
  if (!parse_2d(s, 6, &ss))
    return false;
  if (hh < 0 || hh > 23)
    return false;
  if (mm < 0 || mm > 59)
    return false;
  if (ss < 0 || ss > 59)
    return false;
  *out_seconds = hh * 3600 + mm * 60 + ss;
  return true;
}

bool parse_iso_timestamp_to_ns(std::string_view s, int64_t *out_ns) {
  if (!out_ns)
    return false;

  // Allow a single space instead of 'T'.
  if (s.size() < 19)
    return false;
  if (s[10] != 'T' && s[10] != ' ')
    return false;

  int32_t days = 0;
  if (!parse_iso_date_to_days(s.substr(0, 10), &days))
    return false;

  int32_t sec = 0;
  if (!parse_iso_time_to_seconds(s.substr(11, 8), &sec))
    return false;

  std::size_t pos = 19;
  int64_t frac_ns = 0;
  if (!parse_fractional_nanoseconds(s, &pos, &frac_ns))
    return false;

  int64_t timezone_offset_seconds = 0;
  if (!parse_timezone_offset(s, &pos, &timezone_offset_seconds) ||
      pos != s.size()) {
    return false;
  }
  return combine_timestamp_nanoseconds(days, sec, frac_ns,
                                       timezone_offset_seconds, out_ns);
}

} // namespace sanitize
