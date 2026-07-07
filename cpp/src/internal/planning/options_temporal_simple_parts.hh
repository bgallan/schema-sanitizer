// Declares cursor helpers for simple temporal regex fast paths.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "sanitize/options/options.hh"

namespace sanitize::internal::options_temporal_simple_parts {

// Consumes one capture token such as (\d{4}) from a pattern cursor.
bool consume_capture(std::string_view pattern, std::size_t *pos,
                     std::string_view token);

// Consumes one literal separator, allowing regex-escaped punctuation.
bool consume_literal_separator(std::string_view pattern, std::size_t *pos,
                               char *out);

// Parses exactly n ASCII digits from a string cursor.
bool parse_n_digits(std::string_view s, std::size_t *pos, std::size_t n,
                    int *out);

// Consumes one expected byte from a string cursor.
bool consume_char(std::string_view s, std::size_t *pos, char expected);

// Parses a 1..9 digit fractional second and normalizes it to nanoseconds.
bool parse_fraction_ns(std::string_view s, std::size_t *pos, int64_t *out);

// Parses a numeric timezone offset accepted by the simple pattern detector.
bool parse_numeric_timezone(std::string_view s, std::size_t *pos,
                            int *out_seconds);

// Consumes a literal UTC Z suffix from a regex pattern cursor.
bool consume_z_timezone_pattern(std::string_view pattern, std::size_t *pos,
                                sanitize::SimpleTemporalPattern *p);

} // namespace sanitize::internal::options_temporal_simple_parts
