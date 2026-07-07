// Declares private parsers for user-supplied temporal regex patterns.
//
// The functions interpret regex capture groups into timestamp, date, and time
// scalar values used by PreparedOptions.

#pragma once

#include <cstdint>
#include <regex>
#include <string_view>

namespace sanitize::internal {

// Parses timestamp capture groups into Unix epoch nanoseconds.
bool parse_timestamp_from_regex(std::string_view s, const std::regex &re,
                                int64_t *out_ns);

// Parses date capture groups into Arrow date32 days.
bool parse_date_from_regex(std::string_view s, const std::regex &re,
                           int32_t *out_days);

// Parses time capture groups into seconds since midnight.
bool parse_time_from_regex(std::string_view s, const std::regex &re,
                           int32_t *out_seconds);

} // namespace sanitize::internal
