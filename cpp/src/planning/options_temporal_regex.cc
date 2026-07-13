// Parses user-supplied temporal regex capture groups.
//
// Converts configured regex captures into bounded timestamp, date, and time
// scalar values while reusing the project's civil-date primitives.

#include "internal/planning/options_temporal_regex.hh"
#include "internal/planning/temporal/parts.hh"

#include <cstdint>
#include <regex>
#include <string_view>

namespace sanitize::internal {

bool parse_timestamp_from_regex(std::string_view s, const std::regex &re,
                                int64_t *out_ns) {
  if (!out_ns) {
    return false;
  }
  std::match_results<std::string_view::const_iterator> matches;
  if (!std::regex_match(s.begin(), s.end(), matches, re)) {
    return false;
  }
  if (matches.size() < 7) {
    return false;
  }

  TemporalParts parts;
  if (!parse_int_captured({matches[1].first, matches[1].second}, &parts.year) ||
      !parse_int_captured({matches[2].first, matches[2].second},
                          &parts.month) ||
      !parse_int_captured({matches[3].first, matches[3].second}, &parts.day) ||
      !parse_int_captured({matches[4].first, matches[4].second}, &parts.hour) ||
      !parse_int_captured({matches[5].first, matches[5].second},
                          &parts.minute) ||
      !parse_int_captured({matches[6].first, matches[6].second},
                          &parts.second)) {
    return false;
  }

  if (!parse_fraction_capture(matches, &parts) ||
      !parse_timezone_capture(matches, &parts)) {
    return false;
  }

  return parse_temporal_parts_to_timestamp(parts, out_ns);
}

bool parse_date_from_regex(std::string_view s, const std::regex &re,
                           int32_t *out_days) {
  if (!out_days) {
    return false;
  }
  std::match_results<std::string_view::const_iterator> matches;
  if (!std::regex_match(s.begin(), s.end(), matches, re)) {
    return false;
  }
  if (matches.size() < 4) {
    return false;
  }
  TemporalParts parts;
  if (!parse_int_captured({matches[1].first, matches[1].second}, &parts.year) ||
      !parse_int_captured({matches[2].first, matches[2].second},
                          &parts.month) ||
      !parse_int_captured({matches[3].first, matches[3].second}, &parts.day)) {
    return false;
  }
  return ymd_to_days(parts.year, parts.month, parts.day, out_days);
}

bool parse_time_from_regex(std::string_view s, const std::regex &re,
                           int32_t *out_seconds) {
  if (!out_seconds) {
    return false;
  }
  std::match_results<std::string_view::const_iterator> matches;
  if (!std::regex_match(s.begin(), s.end(), matches, re)) {
    return false;
  }
  if (matches.size() < 4) {
    return false;
  }
  TemporalParts parts;
  if (!parse_int_captured({matches[1].first, matches[1].second}, &parts.hour) ||
      !parse_int_captured({matches[2].first, matches[2].second},
                          &parts.minute) ||
      !parse_int_captured({matches[3].first, matches[3].second},
                          &parts.second)) {
    return false;
  }
  return hms_to_seconds(parts.hour, parts.minute, parts.second, out_seconds);
}

} // namespace sanitize::internal
