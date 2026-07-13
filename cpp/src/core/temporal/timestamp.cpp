// Parses ISO timestamps into nanoseconds since the Unix epoch.

#include "sanitize/core/primitives.hh"

#include "core/temporal/parse_internal.hh"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>

namespace sanitize {

namespace {

bool parse_fractional_nanoseconds(std::string_view s, std::size_t *pos,
                                  int64_t *out_ns) {
  *out_ns = 0;
  if (*pos >= s.size() || s[*pos] != '.')
    return true;

  ++*pos;
  int digits = 0;
  int64_t value = 0;
  while (*pos < s.size() && temporal_internal::is_digit(s[*pos])) {
    if (digits < 9) {
      value = value * 10 + (s[*pos] - '0');
      ++digits;
    }
    ++*pos;
  }
  if (digits == 0)
    return false;
  while (digits < 9) {
    value *= 10;
    ++digits;
  }
  *out_ns = value;
  return true;
}

bool parse_timezone_offset(std::string_view s, std::size_t *pos,
                           int64_t *out_seconds) {
  *out_seconds = 0;
  if (*pos >= s.size())
    return true;
  if (s[*pos] == 'Z') {
    ++*pos;
    return true;
  }
  if (s[*pos] != '+' && s[*pos] != '-')
    return true;

  const int sign = s[*pos] == '+' ? 1 : -1;
  ++*pos;
  if (*pos + 4 >= s.size())
    return false;

  int hours = 0;
  int minutes = 0;
  if (!temporal_internal::parse_2d(s, *pos, &hours) || s[*pos + 2] != ':' ||
      !temporal_internal::parse_2d(s, *pos + 3, &minutes) || hours > 23 ||
      minutes > 59) {
    return false;
  }
  *pos += 5;
  *out_seconds = sign * (static_cast<int64_t>(hours) * 3600 +
                         static_cast<int64_t>(minutes) * 60);
  return true;
}

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

bool parse_iso_timestamp_to_ns(std::string_view s, int64_t *out_ns) {
  if (!out_ns || s.size() < 19 || (s[10] != 'T' && s[10] != ' '))
    return false;

  int32_t days = 0;
  if (!parse_iso_date_to_days(s.substr(0, 10), &days))
    return false;
  int32_t seconds = 0;
  if (!parse_iso_time_to_seconds(s.substr(11, 8), &seconds))
    return false;

  std::size_t pos = 19;
  int64_t fraction_ns = 0;
  if (!parse_fractional_nanoseconds(s, &pos, &fraction_ns))
    return false;
  int64_t timezone_offset_seconds = 0;
  if (!parse_timezone_offset(s, &pos, &timezone_offset_seconds) ||
      pos != s.size()) {
    return false;
  }
  return combine_timestamp_nanoseconds(days, seconds, fraction_ns,
                                       timezone_offset_seconds, out_ns);
}

} // namespace sanitize
