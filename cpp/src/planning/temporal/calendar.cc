// Converts validated temporal fields into epoch-based values.

#include "internal/planning/temporal/parts.hh"

#include <limits>

#include "sanitize/core/primitives.hh"

namespace sanitize::internal {
namespace {

bool is_leap_year(int year) {
  return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
}

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
  const int64_t days = days_from_civil_epoch(year, static_cast<unsigned>(month),
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
  int64_t total_seconds = static_cast<int64_t>(days) * int64_t{86400} + seconds;
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

} // namespace sanitize::internal
