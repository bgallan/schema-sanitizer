// Materializes plain, delta-length, and delta-byte-array Parquet values into
// Arrow binary arrays. This fragment is included by the consolidated footer
// reader inside its private implementation namespace.

// ---- native_stream_decode_byte_array ----
/// Materializes a flat PLAIN byte-array column into Arrow offsets, data, and
/// validity buffers.
sanitize::Status materialize_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    const NativeFlatRowWindow &window, NativeParquetPageScratch *scratch,
    NativeParquetChildArray *out) {
  SAN_RETURN_NOT_OK(validate_native_flat_row_window(window));
  if (!out) {
    return sanitize::Status::Invalid(
        "native Parquet reader: byte-array output is null");
  }
  out->offsets.assign(static_cast<std::size_t>(window.row_count + 1), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((window.row_count + 7) / 8),
                         0);
  }
  std::int32_t current_offset = 0;
  std::int64_t next_expected_row = 0;
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size() ||
        span.row_count < 0 || span.first_row_index != next_expected_row ||
        span.first_row_index > window.row_group_rows - span.row_count) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid or non-contiguous byte-array span");
    }
    next_expected_row += span.row_count;
    if (!window.overlaps(span.first_row_index, span.row_count)) {
      continue;
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
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::size_t value_offset = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      std::size_t size = 0;
      const char *value = nullptr;
      if (valid) {
        if (values.size() - value_offset < 4) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated BYTE_ARRAY length");
        }
        size =
            static_cast<std::size_t>(read_u32_le(values.data() + value_offset));
        value_offset += 4;
        if (values.size() - value_offset < size) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated BYTE_ARRAY payload");
        }
        value = values.data() + value_offset;
        value_offset += size;
      }
      if (!window.contains(global_row)) {
        continue;
      }
      const auto output_row = window.local_row(global_row);
      if (valid) {
        if (size > static_cast<std::size_t>(
                       std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(size)) {
          return sanitize::Status::Invalid(
              "native Parquet reader: BYTE_ARRAY buffer exceeds int32 offsets");
        }
        out->values.insert(out->values.end(), value, value + size);
        current_offset += static_cast<std::int32_t>(size);
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, output_row);
        }
      }
      out->offsets[static_cast<std::size_t>(output_row + 1)] = current_offset;
    }
    if (value_offset != values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing BYTE_ARRAY payload bytes");
    }
  }
  if (next_expected_row != window.row_group_rows) {
    return sanitize::Status::Invalid(
        "native Parquet reader: incomplete byte-array row spans");
  }
  return {};
}

// ---- native_stream_decode_delta_length_byte_array ----
/// Materializes a flat DELTA_LENGTH_BYTE_ARRAY column into Arrow binary
/// buffers.
sanitize::Status materialize_delta_length_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    const NativeFlatRowWindow &window, NativeParquetPageScratch *scratch,
    NativeParquetChildArray *out) {
  SAN_RETURN_NOT_OK(validate_native_flat_row_window(window));
  if (!out) {
    return sanitize::Status::Invalid(
        "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY output is null");
  }
  if (column.physical_type != kPhysicalByteArray) {
    return sanitize::Status::Invalid(
        "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY requires BYTE_ARRAY");
  }
  out->offsets.assign(static_cast<std::size_t>(window.row_count + 1), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((window.row_count + 7) / 8),
                         0);
  }
  std::int32_t current_offset = 0;
  std::int64_t next_expected_row = 0;
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size() ||
        span.row_count < 0 || span.first_row_index != next_expected_row ||
        span.first_row_index > window.row_group_rows - span.row_count) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid DELTA_LENGTH_BYTE_ARRAY span");
    }
    next_expected_row += span.row_count;
    if (!window.overlaps(span.first_row_index, span.row_count)) {
      continue;
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingDeltaLengthByteArray) {
      return sanitize::Status::Invalid(
          "native Parquet reader: expected DELTA_LENGTH_BYTE_ARRAY data page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid DELTA_LENGTH_BYTE_ARRAY payload "
          "offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    auto &lengths = scratch->delta_i32_lengths;
    lengths.clear();
    lengths.reserve(static_cast<std::size_t>(span.non_null_count));
    std::uint64_t page_value_bytes = 0;
    SAN_ASSIGN_OR_RAISE(
        const auto lengths_bytes,
        decode_delta_binary_packed_stream(
            values, span.non_null_count,
            [&](std::int64_t length) -> sanitize::Status {
              if (length < 0 ||
                  length > std::numeric_limits<std::int32_t>::max()) {
                return sanitize::Status::Invalid(
                    "native Parquet reader: invalid DELTA_LENGTH_BYTE_ARRAY "
                    "length");
              }
              const auto size = static_cast<std::uint64_t>(length);
              if (page_value_bytes >
                  std::numeric_limits<std::uint64_t>::max() - size) {
                return sanitize::Status::Invalid(
                    "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY length "
                    "overflow");
              }
              page_value_bytes += size;
              lengths.push_back(static_cast<std::int32_t>(length));
              return {};
            }));
    if (lengths.size() != static_cast<std::size_t>(span.non_null_count) ||
        lengths_bytes > values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY length count "
          "mismatch");
    }
    const auto bytes = values.substr(lengths_bytes);
    if (page_value_bytes != static_cast<std::uint64_t>(bytes.size())) {
      return sanitize::Status::Invalid(
          "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY byte payload "
          "mismatch");
    }
    std::size_t length_offset = 0;
    std::size_t byte_offset = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      std::size_t size = 0;
      const char *value = nullptr;
      if (valid) {
        if (length_offset >= lengths.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing DELTA_LENGTH_BYTE_ARRAY length");
        }
        size = static_cast<std::size_t>(lengths[length_offset++]);
        if (bytes.size() - byte_offset < size) {
          return sanitize::Status::Invalid("native Parquet reader: truncated "
                                           "DELTA_LENGTH_BYTE_ARRAY payload");
        }
        value = bytes.data() + byte_offset;
        byte_offset += size;
      }
      if (!window.contains(global_row)) {
        continue;
      }
      const auto output_row = window.local_row(global_row);
      if (valid) {
        if (size > static_cast<std::size_t>(
                       std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(size)) {
          return sanitize::Status::Invalid(
              "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY offsets exceed "
              "int32");
        }
        out->values.insert(out->values.end(), value, value + size);
        current_offset += static_cast<std::int32_t>(size);
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, output_row);
        }
      }
      out->offsets[static_cast<std::size_t>(output_row + 1)] = current_offset;
    }
    if (length_offset != lengths.size() || byte_offset != bytes.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing DELTA_LENGTH_BYTE_ARRAY values");
    }
  }
  if (next_expected_row != window.row_group_rows) {
    return sanitize::Status::Invalid(
        "native Parquet reader: incomplete DELTA_LENGTH_BYTE_ARRAY spans");
  }
  return {};
}
