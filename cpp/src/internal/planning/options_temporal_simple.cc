// Implements simple temporal regex fast paths for prepared options.
// The helpers normalize private planning state without leaking wire or layout
// details into public APIs.

#include "internal/planning/options_temporal_simple.hh"

#include <cstddef>

#include "internal/planning/options_temporal_simple_parts.hh"
#include "internal/planning/temporal/parts.hh"
#include "sanitize/core/primitives.hh"

namespace sanitize::internal {
namespace {

using options_temporal_simple_parts::consume_capture;
using options_temporal_simple_parts::consume_char;
using options_temporal_simple_parts::consume_literal_separator;
using options_temporal_simple_parts::consume_z_timezone_pattern;
using options_temporal_simple_parts::parse_fraction_ns;
using options_temporal_simple_parts::parse_n_digits;
using options_temporal_simple_parts::parse_numeric_timezone;

} // namespace

bool detect_simple_date_pattern(std::string_view pattern,
                                sanitize::SimpleTemporalPattern *out) {
  if (!out) {
    return false;
  }
  std::size_t pos = 0;
  sanitize::SimpleTemporalPattern p;
  if (!consume_capture(pattern, &pos, R"((\d{4}))") ||
      !consume_literal_separator(pattern, &pos, &p.date_sep1) ||
      !consume_capture(pattern, &pos, R"((\d{2}))") ||
      !consume_literal_separator(pattern, &pos, &p.date_sep2) ||
      !consume_capture(pattern, &pos, R"((\d{2}))") || pos != pattern.size()) {
    return false;
  }
  *out = p;
  return true;
}

bool detect_simple_time_pattern(std::string_view pattern,
                                sanitize::SimpleTemporalPattern *out) {
  if (!out) {
    return false;
  }
  std::size_t pos = 0;
  sanitize::SimpleTemporalPattern p;
  if (!consume_capture(pattern, &pos, R"((\d{2}))") ||
      !consume_literal_separator(pattern, &pos, &p.time_sep1) ||
      !consume_capture(pattern, &pos, R"((\d{2}))") ||
      !consume_literal_separator(pattern, &pos, &p.time_sep2) ||
      !consume_capture(pattern, &pos, R"((\d{2}))") || pos != pattern.size()) {
    return false;
  }
  *out = p;
  return true;
}

bool detect_simple_timestamp_pattern(std::string_view pattern,
                                     sanitize::SimpleTemporalPattern *out) {
  if (!out) {
    return false;
  }
  std::size_t pos = 0;
  sanitize::SimpleTemporalPattern p;
  if (!consume_capture(pattern, &pos, R"((\d{4}))") ||
      !consume_literal_separator(pattern, &pos, &p.date_sep1) ||
      !consume_capture(pattern, &pos, R"((\d{2}))") ||
      !consume_literal_separator(pattern, &pos, &p.date_sep2) ||
      !consume_capture(pattern, &pos, R"((\d{2}))") ||
      !consume_literal_separator(pattern, &pos, &p.datetime_sep) ||
      !consume_capture(pattern, &pos, R"((\d{2}))") ||
      !consume_literal_separator(pattern, &pos, &p.time_sep1) ||
      !consume_capture(pattern, &pos, R"((\d{2}))") ||
      !consume_literal_separator(pattern, &pos, &p.time_sep2) ||
      !consume_capture(pattern, &pos, R"((\d{2}))")) {
    return false;
  }
  if (pos == pattern.size()) {
    *out = p;
    return true;
  }
  if (consume_z_timezone_pattern(pattern, &pos, &p)) {
    *out = p;
    return true;
  }
  char fraction_sep = '\0';
  if (!consume_literal_separator(pattern, &pos, &fraction_sep) ||
      fraction_sep != '.' || !consume_capture(pattern, &pos, R"((\d{1,9}))")) {
    return false;
  }
  p.has_fraction = true;
  if (pos == pattern.size()) {
    *out = p;
    return true;
  }
  if (consume_z_timezone_pattern(pattern, &pos, &p)) {
    *out = p;
    return true;
  }
  if (pattern.substr(pos) == R"(([+-]\d{4}))" ||
      pattern.substr(pos) == R"(([+-]\d{2}:?\d{2}))") {
    p.has_timezone = true;
    *out = p;
    return true;
  }
  return false;
}

bool parse_simple_date(std::string_view s,
                       const sanitize::SimpleTemporalPattern &pattern,
                       int32_t *out_days) {
  std::size_t pos = 0;
  int year = 0;
  int month = 0;
  int day = 0;
  return parse_n_digits(s, &pos, 4, &year) &&
         consume_char(s, &pos, pattern.date_sep1) &&
         parse_n_digits(s, &pos, 2, &month) &&
         consume_char(s, &pos, pattern.date_sep2) &&
         parse_n_digits(s, &pos, 2, &day) && pos == s.size() &&
         ymd_to_days(year, month, day, out_days);
}

bool parse_simple_time(std::string_view s,
                       const sanitize::SimpleTemporalPattern &pattern,
                       int32_t *out_seconds) {
  std::size_t pos = 0;
  int hour = 0;
  int minute = 0;
  int second = 0;
  return parse_n_digits(s, &pos, 2, &hour) &&
         consume_char(s, &pos, pattern.time_sep1) &&
         parse_n_digits(s, &pos, 2, &minute) &&
         consume_char(s, &pos, pattern.time_sep2) &&
         parse_n_digits(s, &pos, 2, &second) && pos == s.size() &&
         hms_to_seconds(hour, minute, second, out_seconds);
}

bool parse_simple_timestamp(std::string_view s,
                            const sanitize::SimpleTemporalPattern &pattern,
                            int64_t *out_ns) {
  std::size_t pos = 0;
  TemporalParts parts;
  if (!parse_n_digits(s, &pos, 4, &parts.year) ||
      !consume_char(s, &pos, pattern.date_sep1) ||
      !parse_n_digits(s, &pos, 2, &parts.month) ||
      !consume_char(s, &pos, pattern.date_sep2) ||
      !parse_n_digits(s, &pos, 2, &parts.day) ||
      !consume_char(s, &pos, pattern.datetime_sep) ||
      !parse_n_digits(s, &pos, 2, &parts.hour) ||
      !consume_char(s, &pos, pattern.time_sep1) ||
      !parse_n_digits(s, &pos, 2, &parts.minute) ||
      !consume_char(s, &pos, pattern.time_sep2) ||
      !parse_n_digits(s, &pos, 2, &parts.second)) {
    return false;
  }
  if (pattern.has_fraction) {
    if (!consume_char(s, &pos, '.') ||
        !parse_fraction_ns(s, &pos, &parts.frac_ns)) {
      return false;
    }
  }
  if (pattern.has_timezone) {
    if (pattern.timezone_z) {
      if (!consume_char(s, &pos, 'Z')) {
        return false;
      }
      parts.tz_offset_seconds = 0;
    } else if (!parse_numeric_timezone(s, &pos, &parts.tz_offset_seconds)) {
      return false;
    }
    parts.has_tz = true;
  }
  return pos == s.size() && parse_temporal_parts_to_timestamp(parts, out_ns);
}

} // namespace sanitize::internal
