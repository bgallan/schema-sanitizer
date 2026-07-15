// Native Parquet decode owner: binary columns.

// ---- native_stream_decode_delta_length_binary ----
sanitize::Status materialize_simple_list_delta_length_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    const NativeRepeatedLeafWindow &window, NativeParquetPageScratch *scratch,
    NativeParquetChildArray *out) {
  if (!out ||
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::delta_length_byte_array ||
      column.physical_type != kPhysicalByteArray ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list DELTA_LENGTH_BYTE_ARRAY layout");
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
        "native Parquet reader: list DELTA_LENGTH_BYTE_ARRAY buffer is too "
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

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  std::int32_t current_offset = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingDeltaLengthByteArray ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list DELTA_LENGTH_BYTE_ARRAY page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list DELTA_LENGTH_BYTE_ARRAY offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    auto &lengths = scratch->delta_i32_lengths;
    lengths.clear();
    lengths.reserve(static_cast<std::size_t>(page.decoded_non_null_values));
    std::uint64_t page_value_bytes = 0;
    SAN_ASSIGN_OR_RAISE(
        const auto lengths_bytes,
        decode_delta_binary_packed_stream(
            values, page.decoded_non_null_values,
            [&](std::int64_t length) -> sanitize::Status {
              if (length < 0 ||
                  length > std::numeric_limits<std::int32_t>::max()) {
                return sanitize::Status::Invalid(
                    "native Parquet reader: list DELTA_LENGTH_BYTE_ARRAY "
                    "invalid length");
              }
              const auto size = static_cast<std::uint64_t>(length);
              if (page_value_bytes >
                  std::numeric_limits<std::uint64_t>::max() - size) {
                return sanitize::Status::Invalid(
                    "native Parquet reader: list DELTA_LENGTH_BYTE_ARRAY "
                    "length overflow");
              }
              page_value_bytes += size;
              lengths.push_back(static_cast<std::int32_t>(length));
              return {};
            }));
    if (lengths.size() !=
            static_cast<std::size_t>(page.decoded_non_null_values) ||
        lengths_bytes > values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list DELTA_LENGTH_BYTE_ARRAY length count "
          "mismatch");
    }
    const auto bytes = values.substr(lengths_bytes);
    if (page_value_bytes != static_cast<std::uint64_t>(bytes.size())) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list DELTA_LENGTH_BYTE_ARRAY payload "
          "mismatch");
    }

    std::size_t length_offset = 0;
    std::size_t byte_offset = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= window.total_elements) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list byte-array element span exceeds row "
            "group");
      }
      if (definition == column.max_definition_level) {
        if (length_offset >= lengths.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing list DELTA_LENGTH_BYTE_ARRAY "
              "length");
        }
        const auto size = static_cast<std::size_t>(lengths[length_offset++]);
        if (bytes.size() - byte_offset < size) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated list DELTA_LENGTH_BYTE_ARRAY "
              "payload");
        }
        if (size > static_cast<std::size_t>(
                       std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(size)) {
          return sanitize::Status::Invalid(
              "native Parquet reader: list DELTA_LENGTH_BYTE_ARRAY offsets "
              "exceed int32");
        }
        if (window.elements.contains(element_index)) {
          const auto output_index = window.elements.local(element_index);
          out->values.insert(out->values.end(), bytes.data() + byte_offset,
                             bytes.data() + byte_offset + size);
          current_offset += static_cast<std::int32_t>(size);
          if (!out->validity.empty()) {
            set_output_validity_bit(&out->validity, output_index);
          }
        }
        byte_offset += size;
      }
      if (window.elements.contains(element_index)) {
        const auto output_index = window.elements.local(element_index);
        out->offsets[static_cast<std::size_t>(output_index + 1)] =
            current_offset;
      }
      ++element_index;
    }
    if (length_offset != lengths.size() || byte_offset != bytes.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list DELTA_LENGTH_BYTE_ARRAY "
          "values");
    }
  }
  if (element_index != window.total_elements) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list byte-array element count mismatch");
  }
  return {};
}

// ---- native_stream_decode_plain_binary ----
sanitize::Status materialize_simple_list_plain_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    const NativeRepeatedLeafWindow &window, NativeParquetPageScratch *scratch,
    NativeParquetChildArray *out) {
  if (!out ||
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::plain_byte_array ||
      column.physical_type != kPhysicalByteArray ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list PLAIN BYTE_ARRAY layout");
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
        "native Parquet reader: list PLAIN BYTE_ARRAY buffer is too large");
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

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  std::int32_t current_offset = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding || page.value_encoding != kEncodingPlain ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list PLAIN BYTE_ARRAY page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list PLAIN BYTE_ARRAY offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::size_t value_offset = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= window.total_elements) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list PLAIN BYTE_ARRAY element span "
            "exceeds row group");
      }
      if (definition == column.max_definition_level) {
        if (values.size() - value_offset < 4) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated list PLAIN BYTE_ARRAY length");
        }
        const auto size =
            static_cast<std::size_t>(read_u32_le(values.data() + value_offset));
        value_offset += 4;
        if (values.size() - value_offset < size) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated list PLAIN BYTE_ARRAY "
              "payload");
        }
        if (size > static_cast<std::size_t>(
                       std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(size)) {
          return sanitize::Status::Invalid(
              "native Parquet reader: list PLAIN BYTE_ARRAY offsets exceed "
              "int32");
        }
        if (window.elements.contains(element_index)) {
          const auto output_index = window.elements.local(element_index);
          out->values.insert(out->values.end(), values.data() + value_offset,
                             values.data() + value_offset + size);
          current_offset += static_cast<std::int32_t>(size);
          if (!out->validity.empty()) {
            set_output_validity_bit(&out->validity, output_index);
          }
        }
        value_offset += size;
      }
      if (window.elements.contains(element_index)) {
        const auto output_index = window.elements.local(element_index);
        out->offsets[static_cast<std::size_t>(output_index + 1)] =
            current_offset;
      }
      ++element_index;
    }
    if (value_offset != values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list PLAIN BYTE_ARRAY values");
    }
  }
  if (element_index != window.total_elements) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list PLAIN BYTE_ARRAY element count mismatch");
  }
  return {};
}
