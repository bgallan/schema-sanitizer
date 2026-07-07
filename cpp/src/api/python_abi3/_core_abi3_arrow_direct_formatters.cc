// Implements direct-Arrow formatter wrappers over shared Arrow helpers.

#include "api/python_abi3/_core_abi3_arrow_direct_formatters.hh"

#include "internal/arrow/arrow_formatters.hh"

namespace core_abi3_internal {
namespace arrow_format = sanitize::internal::arrow_format;

bool parse_decimal_format(std::string_view format, ArrowInputNode *node) {
  arrow_format::DecimalFormat parsed;
  if (!arrow_format::parse_decimal_format(format, &parsed) || !node) {
    return false;
  }
  node->decimal_scale = parsed.scale;
  node->decimal_byte_width = parsed.byte_width;
  return true;
}

int64_t timestamp_target_units_per_second(std::string_view precision) {
  return arrow_format::timestamp_target_units_per_second(precision);
}

int64_t timestamp_source_units_per_second(std::string_view format) {
  return arrow_format::timestamp_source_units_per_second(format);
}

int64_t scale_timestamp_value(int64_t value, int64_t source_units,
                              int64_t target_units) {
  return arrow_format::scale_timestamp_value(value, source_units, target_units);
}

std::string uint64_to_string(uint64_t value) {
  return arrow_format::uint64_to_string(value);
}

std::string decimal_to_string(const uint8_t *bytes, int32_t byte_width,
                              int32_t scale) {
  return arrow_format::decimal_to_string(bytes, byte_width, scale);
}

std::string format_time_fraction(int64_t value, int64_t units_per_second) {
  return arrow_format::format_time_fraction(value, units_per_second);
}

std::string duration_to_string(int64_t value, std::string_view format) {
  return arrow_format::duration_to_string(value, format);
}

std::string month_interval_to_string(int32_t months) {
  return arrow_format::month_interval_to_string(months);
}

std::string day_time_interval_to_string(int32_t days, int32_t milliseconds) {
  return arrow_format::day_time_interval_to_string(days, milliseconds);
}

std::string month_day_nano_interval_to_string(int32_t months, int32_t days,
                                              int64_t nanoseconds) {
  return arrow_format::month_day_nano_interval_to_string(months, days,
                                                         nanoseconds);
}

std::string base64_encode(std::string_view value) {
  return arrow_format::base64_encode(value);
}

} // namespace core_abi3_internal
