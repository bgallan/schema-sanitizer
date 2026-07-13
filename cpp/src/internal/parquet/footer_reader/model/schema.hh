// Parquet schema metadata used by the bounded footer reader.

#pragma once

#include <cstdint>
#include <string>

namespace sanitize::internal::parquet_footer_reader {

struct SchemaElementInfo {
  std::string name;
  bool has_physical_type = false;
  std::int32_t physical_type = 0;
  bool has_type_length = false;
  std::int32_t type_length = 0;
  bool has_repetition_type = false;
  std::int32_t repetition_type = 0;
  bool has_num_children = false;
  std::int32_t num_children = 0;
  bool has_converted_type = false;
  std::int32_t converted_type = 0;
  bool has_decimal_scale = false;
  std::int32_t decimal_scale = 0;
  bool has_decimal_precision = false;
  std::int32_t decimal_precision = 0;
  std::string logical_type;
  std::string logical_type_time_unit;
  bool has_logical_type_is_adjusted_to_utc = false;
  bool logical_type_is_adjusted_to_utc = false;
  bool has_logical_type_integer_bit_width = false;
  std::int32_t logical_type_integer_bit_width = 0;
  bool has_logical_type_integer_is_signed = false;
  bool logical_type_integer_is_signed = true;
};

} // namespace sanitize::internal::parquet_footer_reader
