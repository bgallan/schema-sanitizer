// Decodes DELTA_LENGTH_BYTE_ARRAY data pages.

sanitize::Status
decode_delta_length_byte_array_page(std::string_view payload,
                                    const ColumnChunkInfo &column,
                                    PageHeaderInfo *page) {
  if (!page || page->value_payload_offset < 0 ||
      static_cast<std::size_t>(page->value_payload_offset) > payload.size()) {
    return sanitize::Status::Invalid(
        "Parquet values: invalid value payload offset");
  }
  if (!column.has_physical_type || column.physical_type != kPhysicalByteArray) {
    page->values_decode_skipped = true;
    return {};
  }
  const auto values =
      payload.substr(static_cast<std::size_t>(page->value_payload_offset));
  const auto expected_values = page->decoded_non_null_values;
  std::int32_t decoded = 0;
  std::uint64_t total_bytes = 0;
  std::vector<std::int32_t> preview_lengths;
  std::size_t lengths_bytes = 0;
  SAN_ASSIGN_OR_RAISE(
      lengths_bytes,
      decode_delta_binary_packed_stream(
          values, expected_values,
          [&](std::int64_t length) -> sanitize::Status {
            if (length < 0 ||
                length > std::numeric_limits<std::int32_t>::max()) {
              return sanitize::Status::Invalid(
                  "Parquet values: DELTA_LENGTH_BYTE_ARRAY invalid length");
            }
            if (total_bytes > std::numeric_limits<std::uint64_t>::max() -
                                  static_cast<std::uint64_t>(length)) {
              return sanitize::Status::Invalid(
                  "Parquet values: DELTA_LENGTH_BYTE_ARRAY length overflow");
            }
            total_bytes += static_cast<std::uint64_t>(length);
            if (preview_lengths.size() < kMaxValuePreviewItems) {
              preview_lengths.push_back(static_cast<std::int32_t>(length));
            }
            ++decoded;
            return {};
          }));
  if (decoded != expected_values) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_LENGTH_BYTE_ARRAY decoded count mismatch");
  }
  const auto remaining = values.size() - lengths_bytes;
  if (total_bytes != static_cast<std::uint64_t>(remaining)) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_LENGTH_BYTE_ARRAY byte payload mismatch");
  }
  const auto bytes = values.substr(lengths_bytes);
  std::size_t offset = 0;
  std::vector<std::string> preview;
  preview.reserve(preview_lengths.size());
  for (const auto length : preview_lengths) {
    const auto size = static_cast<std::size_t>(length);
    if (bytes.size() - offset < size) {
      return sanitize::Status::Invalid(
          "Parquet values: truncated DELTA_LENGTH_BYTE_ARRAY preview");
    }
    preview.push_back(preview_bytes(bytes.substr(offset, size)));
    offset += size;
  }
  if (values.size() >
      static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_LENGTH_BYTE_ARRAY payload too large");
  }
  page->decoded_value_bytes = static_cast<std::int32_t>(values.size());
  SAN_ASSIGN_OR_RAISE(
      page->materialized_value_bytes,
      checked_i32_byte_count(static_cast<std::int64_t>(total_bytes),
                             "DELTA_LENGTH_BYTE_ARRAY materialized value"));
  SAN_ASSIGN_OR_RAISE(page->materialized_offset_bytes,
                      arrow_i32_offset_buffer_bytes(page->num_values));
  page->decoded_value_preview = std::move(preview);
  page->values_decoded = true;
  page->values_decode_skipped = false;
  return {};
}
