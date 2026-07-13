// Parquet column-chunk metadata and native-read state.

#pragma once

#include "internal/parquet/footer_reader/model/pages.hh"

#include <cstdint>
#include <string>
#include <vector>

namespace sanitize::internal::parquet_footer_reader {

struct ColumnChunkInfo {
  std::vector<std::string> path_in_schema;
  bool has_physical_type = false;
  std::int32_t physical_type = 0;
  std::vector<std::int32_t> encodings;
  bool has_codec = false;
  std::int32_t codec = 0;
  bool has_num_values = false;
  std::int64_t num_values = 0;
  bool has_total_uncompressed_size = false;
  std::int64_t total_uncompressed_size = 0;
  bool has_total_compressed_size = false;
  std::int64_t total_compressed_size = 0;
  bool has_file_offset = false;
  std::int64_t file_offset = 0;
  bool has_data_page_offset = false;
  std::int64_t data_page_offset = 0;
  bool has_dictionary_page_offset = false;
  std::int64_t dictionary_page_offset = 0;
  bool has_column_index_offset = false;
  std::int64_t column_index_offset = 0;
  bool has_column_index_length = false;
  std::int32_t column_index_length = 0;
  bool has_offset_index_offset = false;
  std::int64_t offset_index_offset = 0;
  bool has_offset_index_length = false;
  std::int32_t offset_index_length = 0;
  std::int16_t max_definition_level = 0;
  std::int16_t max_repetition_level = 0;
  std::vector<std::int16_t> path_definition_levels;
  std::vector<std::int16_t> path_repetition_levels;
  bool top_level_required = true;
  std::int32_t fixed_type_length = 0;
  std::string native_arrow_format;
  std::vector<PageHeaderInfo> pages;
  ColumnIndexInfo column_index;
  OffsetIndexInfo offset_index;
  bool native_read_plan_decoded = false;
  std::int32_t native_read_data_page_count = 0;
  std::int64_t native_read_total_rows = 0;
  std::int64_t native_read_total_non_nulls = 0;
  std::int64_t native_read_total_nulls = 0;
  std::int64_t native_read_validity_bitmap_bytes = 0;
  std::int64_t native_read_value_payload_bytes = 0;
  std::int64_t native_read_materialized_value_bytes = 0;
  std::int64_t native_read_materialized_offset_bytes = 0;
  std::int32_t native_read_value_width_bytes = 0;
  std::int32_t native_read_dictionary_index_bit_width = 0;
  NativeValueBufferKind native_read_value_buffer_kind =
      NativeValueBufferKind::none;
  std::int64_t native_read_arrow_length = 0;
  std::int64_t native_read_arrow_null_count = 0;
  std::int32_t native_read_arrow_n_buffers = 0;
  std::int32_t native_read_arrow_n_children = 0;
  std::int32_t native_read_has_validity_buffer = 0;
  std::int32_t native_read_has_offsets_buffer = 0;
  std::int32_t native_read_has_values_buffer = 0;
  std::vector<std::string> decoded_dictionary_values;
  std::vector<std::uint8_t> decoded_dictionary_fixed_width_values;
  std::vector<NativeReadPageSpanInfo> native_read_page_spans;
  std::vector<RepeatedLevelLayoutInfo> repeated_level_layouts;
};

} // namespace sanitize::internal::parquet_footer_reader
