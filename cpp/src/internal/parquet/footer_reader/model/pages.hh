// Parquet page and index metadata used by the bounded footer reader.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace sanitize::internal::parquet_footer_reader {

enum class NativeValueBufferKind : std::uint8_t {
  none = 0,
  fixed_width,
  bit_packed_boolean,
  plain_byte_array,
  dictionary_byte_array,
  dictionary_fixed_width,
  delta_binary_packed,
  delta_length_byte_array,
  byte_stream_split,
};

constexpr std::string_view
native_value_buffer_kind_name(NativeValueBufferKind kind) noexcept {
  switch (kind) {
  case NativeValueBufferKind::none:
    return {};
  case NativeValueBufferKind::fixed_width:
    return "fixed_width";
  case NativeValueBufferKind::bit_packed_boolean:
    return "bit_packed_boolean";
  case NativeValueBufferKind::plain_byte_array:
    return "plain_byte_array";
  case NativeValueBufferKind::dictionary_byte_array:
    return "dictionary_byte_array";
  case NativeValueBufferKind::dictionary_fixed_width:
    return "dictionary_fixed_width";
  case NativeValueBufferKind::delta_binary_packed:
    return "delta_binary_packed";
  case NativeValueBufferKind::delta_length_byte_array:
    return "delta_length_byte_array";
  case NativeValueBufferKind::byte_stream_split:
    return "byte_stream_split";
  }
  return {};
}

struct PageHeaderInfo {
  bool has_type = false;
  std::int32_t type = 0;
  bool has_uncompressed_page_size = false;
  std::int32_t uncompressed_page_size = 0;
  bool has_compressed_page_size = false;
  std::int32_t compressed_page_size = 0;
  bool has_num_values = false;
  std::int32_t num_values = 0;
  bool has_value_encoding = false;
  std::int32_t value_encoding = 0;
  bool has_definition_level_encoding = false;
  std::int32_t definition_level_encoding = 0;
  bool has_repetition_level_encoding = false;
  std::int32_t repetition_level_encoding = 0;
  bool is_dictionary_page = false;
  bool dictionary_is_sorted = false;
  std::int64_t header_offset = 0;
  std::int32_t header_size = 0;
  std::int64_t compressed_payload_offset = 0;
  bool has_decompressed_page_size = false;
  std::int32_t decompressed_page_size = 0;
  bool payload_verified = false;
  bool payload_verification_skipped = false;
  bool levels_decoded = false;
  std::int32_t decoded_definition_levels = 0;
  std::int32_t decoded_repetition_levels = 0;
  std::vector<std::int16_t> decoded_repetition_level_values;
  std::vector<std::int16_t> decoded_definition_level_values;
  std::int32_t value_payload_offset = 0;
  std::int32_t decoded_non_null_values = 0;
  std::int32_t decoded_null_values = 0;
  bool validity_bitmap_decoded = false;
  std::int32_t decoded_validity_bytes = 0;
  std::vector<std::uint8_t> decoded_validity_bitmap;
  bool values_decoded = false;
  bool values_decode_skipped = false;
  std::int32_t decoded_value_bytes = 0;
  std::int32_t materialized_value_bytes = 0;
  std::int32_t materialized_offset_bytes = 0;
  std::int32_t dictionary_index_bit_width = 0;
  std::vector<std::string> decoded_value_preview;
  std::vector<std::string> decoded_byte_array_values;
  std::vector<std::uint8_t> decoded_fixed_width_values;
};

struct PageLocationInfo {
  std::int64_t offset = 0;
  std::int32_t compressed_page_size = 0;
  std::int64_t first_row_index = 0;
};

struct ColumnIndexInfo {
  bool decoded = false;
  std::vector<bool> null_pages;
  std::vector<std::string> min_values;
  std::vector<std::string> max_values;
  bool has_boundary_order = false;
  std::int32_t boundary_order = 0;
  std::vector<std::int64_t> null_counts;
};

struct OffsetIndexInfo {
  bool decoded = false;
  std::vector<PageLocationInfo> locations;
};

struct NativeReadPageSpanInfo {
  std::int32_t page_index = 0;
  std::int64_t first_row_index = 0;
  std::int32_t row_count = 0;
  std::int32_t non_null_count = 0;
  std::int32_t null_count = 0;
  std::int32_t value_encoding = 0;
  std::int64_t payload_offset = 0;
  std::int32_t payload_size = 0;
  std::int32_t validity_bitmap_bytes = 0;
  std::int32_t value_payload_offset = 0;
  std::int32_t value_payload_bytes = 0;
  std::int32_t value_width_bytes = 0;
  std::int32_t materialized_value_bytes = 0;
  std::int32_t materialized_offset_bytes = 0;
  std::int32_t dictionary_index_bit_width = 0;
  NativeValueBufferKind value_buffer_kind = NativeValueBufferKind::none;
};

struct RepeatedLevelLayoutInfo {
  bool decoded = false;
  std::int64_t row_count = 0;
  std::int64_t null_count = 0;
  std::int64_t element_count = 0;
  std::int64_t non_null_value_count = 0;
  std::vector<std::int32_t> offsets;
  std::vector<std::uint8_t> validity_bitmap;
};

} // namespace sanitize::internal::parquet_footer_reader
