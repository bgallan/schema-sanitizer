// Declares fast-path temporal pattern detection and parsing helpers.
//
// These helpers recognize common fixed-capture regexes and parse them without
// invoking std::regex for every value.

#pragma once

#include "sanitize/options/options.hh"

#include <cstdint>
#include <string_view>

namespace sanitize::internal {

/// Detects fixed-capture timestamp regexes supported by the simple parser.
[[nodiscard]] bool
detect_simple_timestamp_pattern(std::string_view pattern,
                                sanitize::SimpleTemporalPattern *out);

/// Detects fixed-capture date regexes supported by the simple parser.
[[nodiscard]] bool
detect_simple_date_pattern(std::string_view pattern,
                           sanitize::SimpleTemporalPattern *out);

/// Detects fixed-capture time regexes supported by the simple parser.
[[nodiscard]] bool
detect_simple_time_pattern(std::string_view pattern,
                           sanitize::SimpleTemporalPattern *out);

/// Parses a timestamp value using a detected simple timestamp pattern.
[[nodiscard]] bool
parse_simple_timestamp(std::string_view s,
                       const sanitize::SimpleTemporalPattern &pattern,
                       int64_t *out_ns);

/// Parses a date value using a detected simple date pattern.
[[nodiscard]] bool
parse_simple_date(std::string_view s,
                  const sanitize::SimpleTemporalPattern &pattern,
                  int32_t *out_days);

/// Parses a time value using a detected simple time pattern.
[[nodiscard]] bool
parse_simple_time(std::string_view s,
                  const sanitize::SimpleTemporalPattern &pattern,
                  int32_t *out_seconds);

} // namespace sanitize::internal
