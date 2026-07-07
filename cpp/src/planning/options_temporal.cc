// Implements prepared temporal parsing option methods.
//
// Enabled built-in ISO parsing is handled by core primitives; user-supplied
// patterns are delegated to the private planning temporal parser helpers.

#include "sanitize/options/options.hh"

#include <cstdint>
#include <string_view>

#include "internal/planning/options_temporal_regex.hh"
#include "internal/planning/options_temporal_simple.hh"
#include "sanitize/core/primitives.hh"

namespace sanitize {

bool PreparedOptions::parse_timestamp_ns(std::string_view s,
                                         int64_t *out_ns) const {
  if (!out_ns) {
    return false;
  }

  if (spec.parse_iso_timestamps && parse_iso_timestamp_to_ns(s, out_ns)) {
    return true;
  }

  for (const auto &pattern : simple_timestamp_patterns) {
    if (internal::parse_simple_timestamp(s, pattern, out_ns)) {
      return true;
    }
  }

  for (const auto &re : compiled_timestamp_regexps) {
    if (internal::parse_timestamp_from_regex(s, re, out_ns)) {
      return true;
    }
  }
  return false;
}

bool PreparedOptions::parse_date_days(std::string_view s,
                                      int32_t *out_days) const {
  if (!out_days) {
    return false;
  }

  if (spec.parse_iso_dates && parse_iso_date_to_days(s, out_days)) {
    return true;
  }

  for (const auto &pattern : simple_date_patterns) {
    if (internal::parse_simple_date(s, pattern, out_days)) {
      return true;
    }
  }

  for (const auto &re : compiled_date_regexps) {
    if (internal::parse_date_from_regex(s, re, out_days)) {
      return true;
    }
  }
  return false;
}

bool PreparedOptions::parse_time_seconds(std::string_view s,
                                         int32_t *out_seconds) const {
  if (!out_seconds) {
    return false;
  }

  if (spec.parse_iso_times && parse_iso_time_to_seconds(s, out_seconds)) {
    return true;
  }

  for (const auto &pattern : simple_time_patterns) {
    if (internal::parse_simple_time(s, pattern, out_seconds)) {
      return true;
    }
  }

  for (const auto &re : compiled_time_regexps) {
    if (internal::parse_time_from_regex(s, re, out_seconds)) {
      return true;
    }
  }
  return false;
}

bool PreparedOptions::match_timestamp(std::string_view s) const {
  int64_t tmp = 0;
  return parse_timestamp_ns(s, &tmp);
}

bool PreparedOptions::match_date(std::string_view s) const {
  int32_t tmp = 0;
  return parse_date_days(s, &tmp);
}

bool PreparedOptions::match_time(std::string_view s) const {
  int32_t tmp = 0;
  return parse_time_seconds(s, &tmp);
}

} // namespace sanitize
