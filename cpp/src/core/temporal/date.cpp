// Parses ISO dates into Arrow date32-compatible day counts.

#include "sanitize/core/primitives.hh"

#include "core/temporal/parse_internal.hh"

#include <cstdint>
#include <limits>
#include <string_view>

namespace sanitize {

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
  if (!out_days || s.size() != 10 || s[4] != '-' || s[7] != '-')
    return false;
  int year = 0;
  int month = 0;
  int day = 0;
  if (!temporal_internal::parse_4d(s, 0, &year) ||
      !temporal_internal::parse_2d(s, 5, &month) ||
      !temporal_internal::parse_2d(s, 8, &day)) {
    return false;
  }
  const int max_day = days_in_month(year, month);
  if (max_day == 0 || day < 1 || day > max_day)
    return false;
  const int64_t days = days_from_civil_epoch(year, static_cast<unsigned>(month),
                                             static_cast<unsigned>(day));
  if (days < std::numeric_limits<int32_t>::min() ||
      days > std::numeric_limits<int32_t>::max()) {
    return false;
  }
  *out_days = static_cast<int32_t>(days);
  return true;
}

} // namespace sanitize
