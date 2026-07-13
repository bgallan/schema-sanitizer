// Implements Arrow temporal and interval text formatting helpers.

#include "internal/arrow_text/formatters.hh"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace sanitize::internal::arrow_format {
namespace {

/// Multiplies an int64 timestamp by a positive factor with saturation.
int64_t saturating_multiply(int64_t value, int64_t factor) noexcept {
  if (factor <= 1) {
    return value;
  }
  const int64_t max_value = std::numeric_limits<int64_t>::max();
  const int64_t min_value = std::numeric_limits<int64_t>::min();
  if (value > max_value / factor) {
    return max_value;
  }
  if (value < min_value / factor) {
    return min_value;
  }
  return value * factor;
}

} // namespace

int64_t timestamp_target_units_per_second(std::string_view precision) {
  if (precision == "TIMESTAMP_MILLIS") {
    return 1000LL;
  }
  if (precision == "TIMESTAMP_NANOS") {
    return 1000000000LL;
  }
  return 1000000LL;
}

int64_t timestamp_source_units_per_second(std::string_view format) {
  if (format.starts_with("tsm")) {
    return 1000LL;
  }
  if (format.starts_with("tsu")) {
    return 1000000LL;
  }
  return 1000000000LL;
}

int64_t scale_timestamp_value(int64_t value, int64_t source_units,
                              int64_t target_units) {
  if (source_units == target_units) {
    return value;
  }
  if (target_units > source_units) {
    return saturating_multiply(value, target_units / source_units);
  }
  return value / (source_units / target_units);
}

std::string format_time_fraction(int64_t value, int64_t units_per_second) {
  const bool negative = value < 0;
  const auto magnitude = negative ? static_cast<uint64_t>(-(value + 1)) + 1U
                                  : static_cast<uint64_t>(value);
  const uint64_t units = static_cast<uint64_t>(units_per_second);
  const uint64_t seconds_total = magnitude / units;
  const uint64_t fraction = magnitude % units;
  const uint64_t hours = seconds_total / 3600U;
  const uint64_t minutes = (seconds_total / 60U) % 60U;
  const uint64_t seconds = seconds_total % 60U;

  std::string out;
  if (negative) {
    out.push_back('-');
  }
  auto append_two = [&out](uint64_t v) {
    out.push_back(static_cast<char>('0' + ((v / 10U) % 10U)));
    out.push_back(static_cast<char>('0' + (v % 10U)));
  };
  append_two(hours);
  out.push_back(':');
  append_two(minutes);
  out.push_back(':');
  append_two(seconds);
  if (fraction == 0) {
    return out;
  }
  out.push_back('.');
  uint64_t divisor = units / 10U;
  uint64_t remaining = fraction;
  while (divisor > 0) {
    out.push_back(static_cast<char>('0' + (remaining / divisor)));
    remaining %= divisor;
    divisor /= 10U;
  }
  while (!out.empty() && out.back() == '0') {
    out.pop_back();
  }
  return out;
}

std::string duration_to_string(int64_t value, std::string_view format) {
  std::string out = std::to_string(value);
  if (format == "tDs") {
    out += "s";
  } else if (format == "tDm") {
    out += "ms";
  } else if (format == "tDu") {
    out += "us";
  } else {
    out += "ns";
  }
  return out;
}

std::string month_interval_to_string(int32_t months) {
  return "months=" + std::to_string(months);
}

std::string day_time_interval_to_string(int32_t days, int32_t milliseconds) {
  return "days=" + std::to_string(days) +
         ",milliseconds=" + std::to_string(milliseconds);
}

std::string month_day_nano_interval_to_string(int32_t months, int32_t days,
                                              int64_t nanoseconds) {
  return "months=" + std::to_string(months) + ",days=" + std::to_string(days) +
         ",nanoseconds=" + std::to_string(nanoseconds);
}

} // namespace sanitize::internal::arrow_format
