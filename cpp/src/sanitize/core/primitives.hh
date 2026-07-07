// Declares strict scalar text parsers for numeric and temporal values.

#pragma once

#include <cstdint>
#include <string_view>

namespace sanitize {

// Howard Hinnant civil date algorithms (public domain).
// Days are relative to 1970-01-01 (Unix epoch), consistent with Arrow date32.
int64_t days_from_civil_epoch(int y, unsigned m, unsigned d);

// Parses an integer without accepting trailing characters.
bool parse_int64_strict(std::string_view s, int64_t *out);
// Parses one canonical ASCII float using a locale-independent implementation.
bool parse_ascii_float64_strict(std::string_view s, double *out);
// Parses a localized float without accepting malformed grouping or trailing
// characters.
bool parse_float64_strict(std::string_view s, char decimal_separator,
                          char thousands_separator, double *out);

// ISO parsing. For timestamp, accepts
// "YYYY-MM-DDTHH:MM:SS[.frac][Z|+HH:MM|-HH:MM]" and also date-time with a space
// instead of 'T'.
bool parse_iso_date_to_days(std::string_view s, int32_t *out_days);
// Parses an ISO time into seconds since midnight.
bool parse_iso_time_to_seconds(std::string_view s, int32_t *out_seconds);
// Parses an ISO timestamp into nanoseconds since the Unix epoch.
bool parse_iso_timestamp_to_ns(std::string_view s, int64_t *out_ns);

// NOTE: token matching is performed via PreparedOptions without allocating.

} // namespace sanitize
