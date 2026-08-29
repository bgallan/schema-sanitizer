// Materializes simple repeated fixed-width columns as Arrow lists with offsets
// and validity. This fragment is included by the consolidated footer reader
// inside its private implementation namespace.

// ---- native_stream_decode_simple_list_fixed_width ----
/// Materializes repeated fixed-width values into Arrow list-element buffers.
sanitize::Status materialize_simple_list_fixed_width_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    const NativeRepeatedLeafWindow &window, NativeParquetPageScratch *scratch,
    NativeParquetChildArray *out) {
  if (!out ||
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::fixed_width ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list fixed-width layout");
  }
  const auto element_count = window.elements.count;
  const auto arrow_width = column.native_read_value_width_bytes;
  const auto physical_width = fixed_width_for_plain_values(column);
  if (!physical_width || *physical_width <= 0 || arrow_width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list fixed-width value width is invalid");
  }
  const auto value_bytes = static_cast<std::uint64_t>(element_count) *
                           static_cast<std::uint64_t>(arrow_width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list fixed-width value buffer is too large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((element_count + 7) / 8), 0);
  }

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding || page.value_encoding != kEncodingPlain ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list fixed-width page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list fixed-width payload offset");
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
            "native Parquet reader: list fixed-width element span exceeds row "
            "group");
      }
      if (definition == column.max_definition_level) {
        if (values.size() - value_offset <
            static_cast<std::size_t>(*physical_width)) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated list fixed-width payload");
        }
        if (window.elements.contains(element_index)) {
          const auto output_index = window.elements.local(element_index);
          auto *target =
              out->values.data() + static_cast<std::size_t>(output_index) *
                                       static_cast<std::size_t>(arrow_width);
          SAN_RETURN_NOT_OK(copy_fixed_width_physical_to_arrow(
              target, values.data() + value_offset, column, *physical_width,
              arrow_width));
          if (!out->validity.empty()) {
            set_output_validity_bit(&out->validity, output_index);
          }
        }
        value_offset += static_cast<std::size_t>(*physical_width);
      }
      ++element_index;
    }
    if (value_offset != values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list fixed-width payload bytes");
    }
  }
  if (element_index != window.total_elements) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list fixed-width element count mismatch");
  }
  return {};
}

// ---- native_stream_decode_simple_list_boolean ----
/// Materializes repeated bit-packed Boolean values into an Arrow element
/// bitmap.
sanitize::Status materialize_simple_list_boolean_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    const NativeRepeatedLeafWindow &window, NativeParquetPageScratch *scratch,
    NativeParquetChildArray *out) {
  if (!out ||
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::bit_packed_boolean ||
      column.physical_type != kPhysicalBoolean ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list boolean layout");
  }
  const auto element_count = window.elements.count;
  SAN_ASSIGN_OR_RAISE(const auto value_buffer_bytes,
                      arrow_boolean_value_buffer_bytes(
                          static_cast<std::int32_t>(element_count)));
  out->values.assign(static_cast<std::size_t>(value_buffer_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((element_count + 7) / 8), 0);
  }

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding || page.value_encoding != kEncodingPlain ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list boolean page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list boolean payload offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    const auto expected_bytes =
        static_cast<std::size_t>((page.decoded_non_null_values + 7) / 8);
    if (values.size() != expected_bytes) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list boolean payload size mismatch");
    }
    std::int32_t value_index = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= window.total_elements) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list boolean element span exceeds row "
            "group");
      }
      if (definition == column.max_definition_level) {
        if (value_index >= page.decoded_non_null_values) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing list boolean value");
        }
        if (window.elements.contains(element_index)) {
          const auto output_index = window.elements.local(element_index);
          if (bit_stream_value_is_set(values, value_index)) {
            set_output_validity_bit(&out->values, output_index);
          }
          if (!out->validity.empty()) {
            set_output_validity_bit(&out->validity, output_index);
          }
        }
        ++value_index;
      }
      ++element_index;
    }
    if (value_index != page.decoded_non_null_values) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list boolean values");
    }
  }
  if (element_index != window.total_elements) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list boolean element count mismatch");
  }
  return {};
}

// ---- native_stream_decode_list_byte_stream_split ----
/// Materializes repeated BYTE_STREAM_SPLIT values into fixed-width Arrow
/// element buffers.
sanitize::Status materialize_simple_list_byte_stream_split_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    const NativeRepeatedLeafWindow &window, NativeParquetPageScratch *scratch,
    NativeParquetChildArray *out) {
  if (!out ||
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::byte_stream_split ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list BYTE_STREAM_SPLIT layout");
  }
  const auto element_count = window.elements.count;
  const auto width = column.native_read_value_width_bytes;
  if ((column.physical_type != kPhysicalFloat &&
       column.physical_type != kPhysicalDouble) ||
      (width != 4 && width != 8)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: unsupported list BYTE_STREAM_SPLIT type");
  }
  const auto value_bytes = static_cast<std::uint64_t>(element_count) *
                           static_cast<std::uint64_t>(width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list BYTE_STREAM_SPLIT buffer is too large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((element_count + 7) / 8), 0);
  }

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingByteStreamSplit ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list BYTE_STREAM_SPLIT page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list BYTE_STREAM_SPLIT offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    const auto expected_bytes =
        static_cast<std::uint64_t>(page.decoded_non_null_values) *
        static_cast<std::uint64_t>(width);
    if (expected_bytes > static_cast<std::uint64_t>(
                             std::numeric_limits<std::size_t>::max()) ||
        values.size() != static_cast<std::size_t>(expected_bytes)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list BYTE_STREAM_SPLIT payload size "
          "mismatch");
    }
    std::int32_t value_index = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= window.total_elements) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list BYTE_STREAM_SPLIT element span "
            "exceeds row group");
      }
      if (definition == column.max_definition_level) {
        if (value_index >= page.decoded_non_null_values) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing list BYTE_STREAM_SPLIT value");
        }
        if (window.elements.contains(element_index)) {
          const auto output_index = window.elements.local(element_index);
          auto *target =
              out->values.data() + static_cast<std::size_t>(output_index) *
                                       static_cast<std::size_t>(width);
          for (std::int32_t byte_index = 0; byte_index < width; ++byte_index) {
            const auto source_offset =
                static_cast<std::size_t>(byte_index) *
                    static_cast<std::size_t>(page.decoded_non_null_values) +
                static_cast<std::size_t>(value_index);
            target[static_cast<std::size_t>(byte_index)] =
                static_cast<std::uint8_t>(values[source_offset]);
          }
          if (!out->validity.empty()) {
            set_output_validity_bit(&out->validity, output_index);
          }
        }
        ++value_index;
      }
      ++element_index;
    }
    if (value_index != page.decoded_non_null_values) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list BYTE_STREAM_SPLIT values");
    }
  }
  if (element_index != window.total_elements) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list BYTE_STREAM_SPLIT element count mismatch");
  }
  return {};
}

// ---- native_stream_decode_list_delta_binary ----
/// Materializes repeated DELTA_BINARY_PACKED integers into fixed-width Arrow
/// element buffers.
sanitize::Status materialize_simple_list_delta_binary_packed_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    const NativeRepeatedLeafWindow &window, NativeParquetPageScratch *scratch,
    NativeParquetChildArray *out) {
  if (!out ||
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::delta_binary_packed ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list DELTA_BINARY_PACKED layout");
  }
  const auto row_count = window.elements.count;
  const auto width = column.native_read_value_width_bytes;
  if ((column.physical_type != kPhysicalInt32 &&
       column.physical_type != kPhysicalInt64) ||
      width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: unsupported list DELTA_BINARY_PACKED type");
  }
  const auto value_bytes =
      static_cast<std::uint64_t>(row_count) * static_cast<std::uint64_t>(width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list DELTA_BINARY_PACKED buffer is too large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingDeltaBinaryPacked ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list DELTA_BINARY_PACKED page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list DELTA_BINARY_PACKED offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    auto &decoded_values = scratch->delta_i64_values;
    decoded_values.clear();
    decoded_values.reserve(
        static_cast<std::size_t>(page.decoded_non_null_values));
    SAN_ASSIGN_OR_RAISE(
        const auto consumed,
        decode_delta_binary_packed_stream(
            values, page.decoded_non_null_values,
            [&](std::int64_t value) -> sanitize::Status {
              if (column.physical_type == kPhysicalInt32 &&
                  (value < std::numeric_limits<std::int32_t>::min() ||
                   value > std::numeric_limits<std::int32_t>::max())) {
                return sanitize::Status::Invalid(
                    "native Parquet reader: list DELTA_BINARY_PACKED int32 out "
                    "of range");
              }
              decoded_values.push_back(value);
              return {};
            }));
    if (consumed != values.size() ||
        decoded_values.size() !=
            static_cast<std::size_t>(page.decoded_non_null_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list DELTA_BINARY_PACKED decoded count "
          "mismatch");
    }
    std::size_t value_index = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= window.total_elements) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list element span exceeds row group");
      }
      if (definition == column.max_definition_level) {
        if (value_index >= decoded_values.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing list DELTA_BINARY_PACKED value");
        }
        if (window.elements.contains(element_index)) {
          const auto output_index = window.elements.local(element_index);
          const auto target =
              out->values.data() + static_cast<std::size_t>(output_index) *
                                       static_cast<std::size_t>(width);
          SAN_RETURN_NOT_OK(write_arrow_integer_value(
              target, column, decoded_values[value_index]));
          if (!out->validity.empty()) {
            set_output_validity_bit(&out->validity, output_index);
          }
        }
        ++value_index;
      }
      ++element_index;
    }
    if (value_index != decoded_values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list DELTA_BINARY_PACKED values");
    }
  }
  if (element_index != window.total_elements) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list element count mismatch");
  }
  return {};
}
