// Native Parquet decode owner: dictionary binary columns.

// ---- native_stream_decode_dictionary_binary ----
sanitize::Status materialize_simple_list_dictionary_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out ||
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::dictionary_byte_array ||
      column.physical_type != kPhysicalByteArray ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list dictionary byte-array layout");
  }
  if (column.decoded_dictionary_values.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: missing list dictionary byte-array values");
  }
  const auto element_count = list_leaf_value_count(column);
  out->offsets.assign(static_cast<std::size_t>(element_count + 1), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((element_count + 7) / 8), 0);
  }
  if (column.native_read_materialized_value_bytes < 0 ||
      static_cast<std::uint64_t>(column.native_read_materialized_value_bytes) >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary byte-array buffer is too "
        "large");
  }
  out->values.reserve(
      static_cast<std::size_t>(column.native_read_materialized_value_bytes));

  DictionaryPageState dictionary;
  dictionary.decoded = true;
  dictionary.value_count =
      static_cast<std::int32_t>(column.decoded_dictionary_values.size());
  dictionary.byte_array_values = column.decoded_dictionary_values;

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  std::int32_t current_offset = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingRleDictionary ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list dictionary byte-array page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list dictionary byte-array offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    auto &indices = scratch->dictionary_indices;
    std::int32_t index_bit_width = 0;
    SAN_ASSIGN_OR_RAISE(const auto decoded_indices,
                        decode_rle_dictionary_indices(
                            values, dictionary, page.decoded_non_null_values,
                            nullptr, &indices, &index_bit_width));
    if (decoded_indices != page.decoded_non_null_values ||
        static_cast<std::size_t>(decoded_indices) != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list dictionary byte-array index count "
          "mismatch");
    }
    std::size_t index_offset = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list dictionary byte-array element span "
            "exceeds row group");
      }
      if (definition == column.max_definition_level) {
        if (index_offset >= indices.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing list dictionary byte-array "
              "index");
        }
        const auto dictionary_index = indices[index_offset++];
        if (dictionary_index >= column.decoded_dictionary_values.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: list dictionary byte-array index out of "
              "range");
        }
        const auto &value =
            column.decoded_dictionary_values[static_cast<std::size_t>(
                dictionary_index)];
        if (value.size() > static_cast<std::size_t>(
                               std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(value.size())) {
          return sanitize::Status::Invalid(
              "native Parquet reader: list dictionary byte-array offsets "
              "exceed int32");
        }
        out->values.insert(out->values.end(), value.begin(), value.end());
        current_offset += static_cast<std::int32_t>(value.size());
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, element_index);
        }
      }
      out->offsets[static_cast<std::size_t>(element_index + 1)] =
          current_offset;
      ++element_index;
    }
    if (index_offset != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list dictionary byte-array indices");
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary byte-array element count "
        "mismatch");
  }
  return {};
}

// ---- native_stream_decode_dictionary_byte_array ----
sanitize::Status materialize_dictionary_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid dictionary byte-array row count");
  }
  if (column.decoded_dictionary_values.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: missing decoded dictionary values");
  }
  out->offsets.assign(static_cast<std::size_t>(row_count + 1), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }
  if (column.native_read_materialized_value_bytes < 0 ||
      static_cast<std::uint64_t>(column.native_read_materialized_value_bytes) >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: dictionary byte-array buffer is too large");
  }
  out->values.reserve(
      static_cast<std::size_t>(column.native_read_materialized_value_bytes));

  DictionaryPageState dictionary;
  dictionary.decoded = true;
  dictionary.value_count =
      static_cast<std::int32_t>(column.decoded_dictionary_values.size());
  dictionary.byte_array_values = column.decoded_dictionary_values;

  std::int32_t current_offset = 0;
  std::int64_t next_expected_row = 0;
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid dictionary page span index");
    }
    if (span.first_row_index != next_expected_row) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: non-contiguous dictionary page spans");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingRleDictionary) {
      return sanitize::Status::Invalid(
          "native Parquet reader: expected RLE dictionary data page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid dictionary value payload offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    auto &indices = scratch->dictionary_indices;
    std::int32_t index_bit_width = 0;
    SAN_ASSIGN_OR_RAISE(
        const auto decoded_indices,
        decode_rle_dictionary_indices(values, dictionary, span.non_null_count,
                                      nullptr, &indices, &index_bit_width));
    if (decoded_indices != span.non_null_count ||
        static_cast<std::size_t>(decoded_indices) != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: dictionary index count mismatch");
    }
    std::size_t index_offset = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid("native Parquet reader: dictionary "
                                         "page row span exceeds row group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (index_offset >= indices.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing dictionary index");
        }
        const auto dictionary_index = indices[index_offset++];
        if (dictionary_index >= column.decoded_dictionary_values.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: dictionary index out of range");
        }
        const auto &value =
            column.decoded_dictionary_values[static_cast<std::size_t>(
                dictionary_index)];
        if (value.size() > static_cast<std::size_t>(
                               std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(value.size())) {
          return sanitize::Status::Invalid("native Parquet reader: dictionary "
                                           "byte-array offsets exceed int32");
        }
        out->values.insert(out->values.end(), value.begin(), value.end());
        current_offset += static_cast<std::int32_t>(value.size());
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
      out->offsets[static_cast<std::size_t>(global_row + 1)] = current_offset;
    }
    if (index_offset != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing dictionary indices");
    }
    next_expected_row += span.row_count;
  }
  return {};
}
