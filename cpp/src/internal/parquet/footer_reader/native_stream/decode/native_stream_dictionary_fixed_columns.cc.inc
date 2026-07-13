// Native Parquet decode owner: dictionary fixed columns.

// ---- native_stream_decode_dictionary_fixed_width ----
sanitize::Status materialize_dictionary_fixed_width_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid fixed-width dictionary row count");
  }
  const auto arrow_width = column.native_read_value_width_bytes;
  const auto physical_width = fixed_width_for_plain_values(column);
  if (!physical_width || *physical_width <= 0 || arrow_width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width dictionary width is invalid");
  }
  if (column.decoded_dictionary_fixed_width_values.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: missing fixed-width dictionary values");
  }
  if (column.decoded_dictionary_fixed_width_values.size() %
          static_cast<std::size_t>(*physical_width) !=
      0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width dictionary payload is misaligned");
  }
  const auto dictionary_value_count =
      column.decoded_dictionary_fixed_width_values.size() /
      static_cast<std::size_t>(*physical_width);
  if (dictionary_value_count >
      static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width dictionary is too large");
  }
  const auto value_bytes = static_cast<std::uint64_t>(row_count) *
                           static_cast<std::uint64_t>(arrow_width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width dictionary value buffer is too "
        "large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }

  DictionaryPageState dictionary;
  dictionary.decoded = true;
  dictionary.value_count = static_cast<std::int32_t>(dictionary_value_count);
  dictionary.fixed_width_values = column.decoded_dictionary_fixed_width_values;

  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid fixed-width dictionary page span "
          "index");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingRleDictionary) {
      return sanitize::Status::Invalid(
          "native Parquet reader: expected RLE fixed-width dictionary data "
          "page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid fixed-width dictionary value payload "
          "offset");
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
          "native Parquet reader: fixed-width dictionary index count mismatch");
    }
    std::size_t index_offset = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: fixed-width dictionary page row span "
            "exceeds row group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (index_offset >= indices.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing fixed-width dictionary index");
        }
        const auto dictionary_index = indices[index_offset++];
        if (dictionary_index >= dictionary_value_count) {
          return sanitize::Status::Invalid(
              "native Parquet reader: fixed-width dictionary index out of "
              "range");
        }
        const auto source_offset = static_cast<std::size_t>(dictionary_index) *
                                   static_cast<std::size_t>(*physical_width);
        auto *target =
            out->values.data() + static_cast<std::size_t>(global_row) *
                                     static_cast<std::size_t>(arrow_width);
        SAN_RETURN_NOT_OK(copy_fixed_width_physical_to_arrow(
            target,
            reinterpret_cast<const char *>(
                column.decoded_dictionary_fixed_width_values.data() +
                source_offset),
            column, *physical_width, arrow_width));
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
    }
    if (index_offset != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing fixed-width dictionary indices");
    }
  }
  return {};
}

// ---- native_stream_decode_simple_list_dictionary_fixed_width ----
sanitize::Status materialize_simple_list_dictionary_fixed_width_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out ||
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::dictionary_fixed_width ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list dictionary fixed-width layout");
  }
  const auto element_count = list_leaf_value_count(column);
  const auto arrow_width = column.native_read_value_width_bytes;
  const auto physical_width = fixed_width_for_plain_values(column);
  if (!physical_width || *physical_width <= 0 || arrow_width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary fixed-width width is invalid");
  }
  if (column.decoded_dictionary_fixed_width_values.empty() ||
      column.decoded_dictionary_fixed_width_values.size() %
              static_cast<std::size_t>(*physical_width) !=
          0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary fixed-width payload is "
        "invalid");
  }
  const auto dictionary_value_count =
      column.decoded_dictionary_fixed_width_values.size() /
      static_cast<std::size_t>(*physical_width);
  const auto value_bytes = static_cast<std::uint64_t>(element_count) *
                           static_cast<std::uint64_t>(arrow_width);
  if (dictionary_value_count >
          static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max()) ||
      value_bytes >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary fixed-width buffer is too "
        "large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((element_count + 7) / 8), 0);
  }

  DictionaryPageState dictionary;
  dictionary.decoded = true;
  dictionary.value_count = static_cast<std::int32_t>(dictionary_value_count);
  dictionary.fixed_width_values = column.decoded_dictionary_fixed_width_values;

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingRleDictionary ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list dictionary fixed-width page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list dictionary fixed-width offset");
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
          "native Parquet reader: list dictionary fixed-width index count "
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
            "native Parquet reader: list dictionary fixed-width element span "
            "exceeds row group");
      }
      if (definition == column.max_definition_level) {
        if (index_offset >= indices.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing list dictionary fixed-width "
              "index");
        }
        const auto dictionary_index = indices[index_offset++];
        if (dictionary_index >= dictionary_value_count) {
          return sanitize::Status::Invalid(
              "native Parquet reader: list dictionary fixed-width index out "
              "of range");
        }
        const auto source_offset = static_cast<std::size_t>(dictionary_index) *
                                   static_cast<std::size_t>(*physical_width);
        auto *target =
            out->values.data() + static_cast<std::size_t>(element_index) *
                                     static_cast<std::size_t>(arrow_width);
        SAN_RETURN_NOT_OK(copy_fixed_width_physical_to_arrow(
            target,
            reinterpret_cast<const char *>(
                column.decoded_dictionary_fixed_width_values.data() +
                source_offset),
            column, *physical_width, arrow_width));
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, element_index);
        }
      }
      ++element_index;
    }
    if (index_offset != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list dictionary fixed-width "
          "indices");
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary fixed-width element count "
        "mismatch");
  }
  return {};
}
