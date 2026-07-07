// Declares helper parsers for user-supplied temporal regex captures.

#pragma once

#include <cstdint>
#include <regex>
#include <string_view>

namespace sanitize::internal {

struct TemporalParts {
  int year = -1;
  int month = -1;
  int day = -1;
  int hour = -1;
  int minute = -1;
  int second = -1;
  int64_t frac_ns = 0;
  bool has_tz = false;
  int tz_offset_seconds = 0;
};

// Parses a bounded integer regex capture.
bool parse_int_captured(std::string_view value, int *out);

// Parses optional fractional nanoseconds from regex match group 7.
bool parse_fraction_capture(
    const std::match_results<std::string_view::const_iterator> &matches,
    TemporalParts *parts);

// Parses optional timezone offset from regex match group 8.
bool parse_timezone_capture(
    const std::match_results<std::string_view::const_iterator> &matches,
    TemporalParts *parts);

// Converts parsed temporal fields to Unix epoch nanoseconds.
bool parse_temporal_parts_to_timestamp(const TemporalParts &parts,
                                       int64_t *out_ns);

// Converts a Gregorian date to days since the Unix epoch.
bool ymd_to_days(int year, int month, int day, int32_t *out_days);

// Converts a wall-clock time to seconds since midnight.
bool hms_to_seconds(int hour, int minute, int second, int32_t *out_sec);

} // namespace sanitize::internal
