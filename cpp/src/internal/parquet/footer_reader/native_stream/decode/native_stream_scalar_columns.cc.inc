// Native Parquet decode owner: scalar columns.

// ---- native_stream_column_validation ----
// Native stream flat/plain column validation and decoding helpers.
// Included directly by footer_reader.cc.

sanitize::Status validate_native_plain_column(const ColumnChunkInfo &column) {
  if (!native_plain_path_is_materializable(column.path_in_schema,
                                           column.max_repetition_level,
                                           column.top_level_required) ||
      !column.native_read_plan_decoded) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: column is not materializable");
  }
  if (!column.has_physical_type) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: unsupported physical type");
  }
  if (column.native_read_value_buffer_kind !=
          NativeValueBufferKind::fixed_width &&
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::plain_byte_array &&
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::dictionary_byte_array &&
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::dictionary_fixed_width &&
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::delta_binary_packed &&
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::delta_length_byte_array &&
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::byte_stream_split &&
      column.native_read_value_buffer_kind !=
          NativeValueBufferKind::bit_packed_boolean) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: unsupported value encoding");
  }
  if (column.native_read_value_buffer_kind ==
          NativeValueBufferKind::fixed_width &&
      column.native_read_value_width_bytes <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width value width is invalid");
  }
  if (column.native_read_value_buffer_kind ==
          NativeValueBufferKind::delta_binary_packed &&
      (column.native_read_value_width_bytes <= 0 ||
       (column.physical_type != kPhysicalInt32 &&
        column.physical_type != kPhysicalInt64))) {
    return sanitize::Status::Invalid(
        "native Parquet reader: DELTA_BINARY_PACKED integer width is invalid");
  }
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: missing page value encoding");
    }
    if (column.native_read_value_buffer_kind ==
        NativeValueBufferKind::dictionary_byte_array) {
      if (column.decoded_dictionary_values.empty() ||
          page.value_encoding != kEncodingRleDictionary) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported dictionary page");
      }
      continue;
    }
    if (column.native_read_value_buffer_kind ==
        NativeValueBufferKind::dictionary_fixed_width) {
      if (column.decoded_dictionary_fixed_width_values.empty() ||
          page.value_encoding != kEncodingRleDictionary ||
          column.native_read_value_width_bytes <= 0) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported fixed-width dictionary page");
      }
      continue;
    }
    if (column.native_read_value_buffer_kind ==
        NativeValueBufferKind::delta_binary_packed) {
      if (page.value_encoding != kEncodingDeltaBinaryPacked) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported DELTA_BINARY_PACKED page");
      }
      continue;
    }
    if (column.native_read_value_buffer_kind ==
        NativeValueBufferKind::delta_length_byte_array) {
      if (column.physical_type != kPhysicalByteArray ||
          page.value_encoding != kEncodingDeltaLengthByteArray) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported DELTA_LENGTH_BYTE_ARRAY page");
      }
      continue;
    }
    if (column.native_read_value_buffer_kind ==
        NativeValueBufferKind::byte_stream_split) {
      if ((column.physical_type != kPhysicalFloat &&
           column.physical_type != kPhysicalDouble) ||
          page.value_encoding != kEncodingByteStreamSplit ||
          column.native_read_value_width_bytes <= 0) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported BYTE_STREAM_SPLIT page");
      }
      continue;
    }
    if (column.native_read_value_buffer_kind ==
        NativeValueBufferKind::bit_packed_boolean) {
      if (column.physical_type != kPhysicalBoolean ||
          page.value_encoding != kEncodingPlain) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported boolean page");
      }
      continue;
    }
    if (page.value_encoding != kEncodingPlain) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: only PLAIN data pages are materialized");
    }
  }
  return {};
}

// ---- native_stream_decode_fixed_width ----
sanitize::Status materialize_fixed_width_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid fixed-width row count");
  }
  const auto arrow_width = column.native_read_value_width_bytes;
  const auto physical_width = fixed_width_for_plain_values(column);
  if (!physical_width || *physical_width <= 0 || arrow_width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width value width is invalid");
  }
  const auto value_bytes = static_cast<std::uint64_t>(row_count) *
                           static_cast<std::uint64_t>(arrow_width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width value buffer is too large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid page span index");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid value payload offset");
    }
    const std::string_view values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::size_t value_offset = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: page row span exceeds row group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (values.size() - value_offset <
            static_cast<std::size_t>(*physical_width)) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated fixed-width payload");
        }
        auto *target =
            out->values.data() + static_cast<std::size_t>(global_row) *
                                     static_cast<std::size_t>(arrow_width);
        SAN_RETURN_NOT_OK(copy_fixed_width_physical_to_arrow(
            target, values.data() + value_offset, column, *physical_width,
            arrow_width));
        value_offset += static_cast<std::size_t>(*physical_width);
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
    }
    if (value_offset != values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing fixed-width payload bytes");
    }
  }
  return {};
}

// ---- native_stream_decode_boolean ----
sanitize::Status materialize_boolean_column(std::ifstream &file,
                                            const ColumnChunkInfo &column,
                                            std::int64_t row_count,
                                            NativeParquetPageScratch *scratch,
                                            NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid boolean row count");
  }
  if (column.physical_type != kPhysicalBoolean) {
    return sanitize::Status::Invalid(
        "native Parquet reader: boolean materialization requires boolean type");
  }
  SAN_ASSIGN_OR_RAISE(
      const auto value_buffer_bytes,
      arrow_boolean_value_buffer_bytes(static_cast<std::int32_t>(row_count)));
  out->values.assign(static_cast<std::size_t>(value_buffer_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid boolean page span index");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (!page.has_value_encoding || page.value_encoding != kEncodingPlain) {
      return sanitize::Status::Invalid(
          "native Parquet reader: expected PLAIN boolean data page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid boolean payload offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    const auto expected_bytes =
        static_cast<std::size_t>((span.non_null_count + 7) / 8);
    if (values.size() != expected_bytes) {
      return sanitize::Status::Invalid(
          "native Parquet reader: boolean payload size mismatch");
    }
    std::int32_t value_index = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: boolean row span exceeds row group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (value_index >= span.non_null_count) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing boolean value");
        }
        if (bit_stream_value_is_set(values, value_index)) {
          set_output_validity_bit(&out->values, global_row);
        }
        ++value_index;
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
    }
    if (value_index != span.non_null_count) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing boolean values");
    }
  }
  return {};
}

// ---- native_stream_decode_delta_binary_column ----
sanitize::Status materialize_delta_binary_packed_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid DELTA_BINARY_PACKED row count");
  }
  const auto width = column.native_read_value_width_bytes;
  const auto physical_width = fixed_width_for_plain_values(column);
  if ((column.physical_type != kPhysicalInt32 &&
       column.physical_type != kPhysicalInt64) ||
      !physical_width || *physical_width <= 0 || width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: unsupported DELTA_BINARY_PACKED physical type");
  }
  const auto value_bytes =
      static_cast<std::uint64_t>(row_count) * static_cast<std::uint64_t>(width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: DELTA_BINARY_PACKED value buffer is too large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid DELTA_BINARY_PACKED page span index");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingDeltaBinaryPacked) {
      return sanitize::Status::Invalid(
          "native Parquet reader: expected DELTA_BINARY_PACKED data page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid DELTA_BINARY_PACKED payload offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    auto &decoded_values = scratch->delta_i64_values;
    decoded_values.clear();
    decoded_values.reserve(static_cast<std::size_t>(span.non_null_count));
    SAN_ASSIGN_OR_RAISE(
        const auto consumed,
        decode_delta_binary_packed_stream(
            values, span.non_null_count,
            [&](std::int64_t value) -> sanitize::Status {
              if (column.physical_type == kPhysicalInt32 &&
                  (value < std::numeric_limits<std::int32_t>::min() ||
                   value > std::numeric_limits<std::int32_t>::max())) {
                return sanitize::Status::Invalid(
                    "native Parquet reader: DELTA_BINARY_PACKED int32 out of "
                    "range");
              }
              decoded_values.push_back(value);
              return {};
            }));
    if (consumed != values.size() ||
        decoded_values.size() !=
            static_cast<std::size_t>(span.non_null_count)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: DELTA_BINARY_PACKED decoded count mismatch");
    }
    std::size_t value_offset = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: DELTA_BINARY_PACKED row span exceeds row "
            "group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (value_offset >= decoded_values.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing DELTA_BINARY_PACKED value");
        }
        const auto target =
            out->values.data() + static_cast<std::size_t>(global_row) *
                                     static_cast<std::size_t>(width);
        SAN_RETURN_NOT_OK(write_arrow_integer_value(
            target, column, decoded_values[value_offset]));
        ++value_offset;
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
    }
    if (value_offset != decoded_values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing DELTA_BINARY_PACKED values");
    }
  }
  return {};
}

// ---- native_stream_decode_byte_stream_split ----
sanitize::Status materialize_byte_stream_split_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid BYTE_STREAM_SPLIT row count");
  }
  const auto width = column.native_read_value_width_bytes;
  if ((column.physical_type != kPhysicalFloat &&
       column.physical_type != kPhysicalDouble) ||
      (width != 4 && width != 8)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: unsupported BYTE_STREAM_SPLIT physical type");
  }
  const auto value_bytes =
      static_cast<std::uint64_t>(row_count) * static_cast<std::uint64_t>(width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: BYTE_STREAM_SPLIT value buffer is too large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid BYTE_STREAM_SPLIT page span index");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingByteStreamSplit) {
      return sanitize::Status::Invalid(
          "native Parquet reader: expected BYTE_STREAM_SPLIT data page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid BYTE_STREAM_SPLIT payload offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    const auto expected_bytes =
        static_cast<std::uint64_t>(span.non_null_count) *
        static_cast<std::uint64_t>(width);
    if (expected_bytes > static_cast<std::uint64_t>(
                             std::numeric_limits<std::size_t>::max()) ||
        values.size() != static_cast<std::size_t>(expected_bytes)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: BYTE_STREAM_SPLIT payload size mismatch");
    }
    std::int32_t value_index = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid("native Parquet reader: "
                                         "BYTE_STREAM_SPLIT row span exceeds "
                                         "row group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (value_index >= span.non_null_count) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing BYTE_STREAM_SPLIT value");
        }
        auto *target =
            out->values.data() + static_cast<std::size_t>(global_row) *
                                     static_cast<std::size_t>(width);
        for (std::int32_t byte_index = 0; byte_index < width; ++byte_index) {
          const auto source_offset =
              static_cast<std::size_t>(byte_index) *
                  static_cast<std::size_t>(span.non_null_count) +
              static_cast<std::size_t>(value_index);
          target[static_cast<std::size_t>(byte_index)] =
              static_cast<std::uint8_t>(values[source_offset]);
        }
        ++value_index;
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
    }
    if (value_index != span.non_null_count) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing BYTE_STREAM_SPLIT values");
    }
  }
  return {};
}
