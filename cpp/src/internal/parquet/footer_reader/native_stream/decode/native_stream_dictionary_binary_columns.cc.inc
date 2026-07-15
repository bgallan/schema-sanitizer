// Native Parquet decode owner: dictionary binary columns.

// ---- native_stream_decode_dictionary_binary ----
sanitize::Status materialize_simple_list_dictionary_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    const NativeRepeatedLeafWindow &window, NativeParquetPageScratch *scratch,
    NativeParquetChildArray *out) {
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
  const auto element_count = window.elements.count;
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
  if (window.total_elements > 0 && element_count > 0) {
    const auto scaled =
        static_cast<long double>(column.native_read_materialized_value_bytes) *
        static_cast<long double>(element_count) /
        static_cast<long double>(window.total_elements);
    const auto reserve_bytes = static_cast<std::size_t>(std::min<long double>(
        scaled + 1.0L,
        static_cast<long double>(std::numeric_limits<std::size_t>::max())));
    out->values.reserve(reserve_bytes);
  }

  DictionaryPageState dictionary;
  dictionary.decoded = true;
  dictionary.value_count =
      static_cast<std::int32_t>(column.decoded_dictionary_values.size());

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
      if (element_index >= window.total_elements) {
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
        if (window.elements.contains(element_index)) {
          const auto output_index = window.elements.local(element_index);
          out->values.insert(out->values.end(), value.begin(), value.end());
          current_offset += static_cast<std::int32_t>(value.size());
          if (!out->validity.empty()) {
            set_output_validity_bit(&out->validity, output_index);
          }
        }
      }
      if (window.elements.contains(element_index)) {
        const auto output_index = window.elements.local(element_index);
        out->offsets[static_cast<std::size_t>(output_index + 1)] =
            current_offset;
      }
      ++element_index;
    }
    if (index_offset != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list dictionary byte-array indices");
    }
  }
  if (element_index != window.total_elements) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary byte-array element count "
        "mismatch");
  }
  return {};
}

// ---- native_stream_decode_dictionary_byte_array ----
sanitize::Status materialize_dictionary_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    const NativeFlatRowWindow &window, NativeParquetPageScratch *scratch,
    NativeParquetChildArray *out) {
  SAN_RETURN_NOT_OK(validate_native_flat_row_window(window));
  if (!out) {
    return sanitize::Status::Invalid(
        "native Parquet reader: dictionary byte-array output is null");
  }
  if (column.decoded_dictionary_values.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: missing decoded dictionary values");
  }
  if (column.decoded_dictionary_values.size() >
      static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: dictionary value count exceeds int32");
  }
  out->offsets.assign(static_cast<std::size_t>(window.row_count + 1), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((window.row_count + 7) / 8),
                         0);
  }
  DictionaryPageState dictionary;
  dictionary.decoded = true;
  dictionary.value_count =
      static_cast<std::int32_t>(column.decoded_dictionary_values.size());
  std::int32_t current_offset = 0;
  std::int64_t next_expected_row = 0;
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size() ||
        span.row_count < 0 || span.first_row_index != next_expected_row ||
        span.first_row_index > window.row_group_rows - span.row_count) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid dictionary page span");
    }
    next_expected_row += span.row_count;
    if (!window.overlaps(span.first_row_index, span.row_count)) {
      continue;
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
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      std::uint32_t dictionary_index = 0;
      if (valid) {
        if (index_offset >= indices.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing dictionary index");
        }
        dictionary_index = indices[index_offset++];
        if (dictionary_index >= column.decoded_dictionary_values.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: dictionary index out of range");
        }
      }
      if (!window.contains(global_row)) {
        continue;
      }
      const auto output_row = window.local_row(global_row);
      if (valid) {
        const auto &value = column.decoded_dictionary_values[
            static_cast<std::size_t>(dictionary_index)];
        if (value.size() > static_cast<std::size_t>(
                               std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(value.size())) {
          return sanitize::Status::Invalid(
              "native Parquet reader: dictionary offsets exceed int32");
        }
        out->values.insert(out->values.end(), value.begin(), value.end());
        current_offset += static_cast<std::int32_t>(value.size());
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, output_row);
        }
      }
      out->offsets[static_cast<std::size_t>(output_row + 1)] = current_offset;
    }
    if (index_offset != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing dictionary indices");
    }
  }
  if (next_expected_row != window.row_group_rows) {
    return sanitize::Status::Invalid(
        "native Parquet reader: incomplete dictionary page spans");
  }
  return {};
}
