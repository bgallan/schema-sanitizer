// Declares scalar format helpers used by the Arrow direct frontend.

#pragma once

#include "api/python_abi3/_core_abi3_arrow_direct_model.hh"

#include <cstdint>
#include <string>
#include <string_view>

namespace core_abi3_internal {

// Parses Arrow decimal format metadata into a direct input node.
bool parse_decimal_format(std::string_view format, ArrowInputNode *node);

// Returns timestamp units per second for an output precision token.
int64_t timestamp_target_units_per_second(std::string_view precision);

// Returns timestamp units per second for an Arrow timestamp format string.
int64_t timestamp_source_units_per_second(std::string_view format);

// Scales a timestamp integer between Arrow timestamp units with saturation.
int64_t scale_timestamp_value(int64_t value, int64_t source_units,
                              int64_t target_units);

// Formats an unsigned 64-bit integer as decimal text.
std::string uint64_to_string(uint64_t value);

// Formats Arrow decimal128/decimal256 bytes as lossless decimal text.
std::string decimal_to_string(const uint8_t *bytes, int32_t byte_width,
                              int32_t scale);

// Formats sub-second Arrow time values as HH:MM:SS[.fraction] text.
std::string format_time_fraction(int64_t value, int64_t units_per_second);

// Formats Arrow duration values as lossless unit-suffixed text.
std::string duration_to_string(int64_t value, std::string_view format);

// Formats Arrow interval values as explicit component text.
std::string month_interval_to_string(int32_t months);
std::string day_time_interval_to_string(int32_t days, int32_t milliseconds);
std::string month_day_nano_interval_to_string(int32_t months, int32_t days,
                                              int64_t nanoseconds);

// Encodes binary values as base64 text.
std::string base64_encode(std::string_view value);

} // namespace core_abi3_internal
