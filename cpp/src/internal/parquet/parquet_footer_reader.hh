// Declares bounded native Parquet footer/page parsing helpers.

#pragma once

#include "sanitize/core/status.hh"

#include <cstdint>
#include <string>
#include <vector>

struct ArrowArrayStream;

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
  std::string value_buffer_kind;
};

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
  std::string native_read_value_buffer_kind;
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
  bool repeated_level_layout_decoded = false;
  std::int64_t repeated_level_row_count = 0;
  std::int64_t repeated_level_null_count = 0;
  std::int64_t repeated_level_element_count = 0;
  std::int64_t repeated_level_non_null_value_count = 0;
  std::vector<std::int32_t> repeated_level_offsets;
  std::vector<std::uint8_t> repeated_level_validity_bitmap;
};

struct RowGroupInfo {
  bool has_total_byte_size = false;
  std::int64_t total_byte_size = 0;
  bool has_num_rows = false;
  std::int64_t num_rows = 0;
  std::vector<ColumnChunkInfo> columns;
};

struct FooterInfo {
  std::int32_t version = 0;
  std::int64_t num_rows = 0;
  std::int32_t schema_element_count = 0;
  std::int32_t row_group_count = 0;
  std::string created_by;
  std::vector<SchemaElementInfo> schema_elements;
  std::vector<RowGroupInfo> row_groups;
};

// Reads bounded Parquet footer metadata from a local file path.
sanitize::Result<FooterInfo> read_footer_info(const std::string &path);

// Reads footer metadata and returns a compact JSON diagnostic payload.
sanitize::Result<std::string> read_footer_info_json(const std::string &path);

// Opens a bounded native Arrow C stream for supported local Parquet files.
sanitize::Result<ArrowArrayStream *>
make_arrow_stream(const std::string &path,
                  const std::vector<std::string> &projected_columns = {});

} // namespace sanitize::internal::parquet_footer_reader
