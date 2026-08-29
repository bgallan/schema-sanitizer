// Reassembles BYTE_STREAM_SPLIT lanes into fixed-width values and bounded
// diagnostic previews. This fragment is included by the consolidated footer
// reader inside its private implementation namespace.

/// Returns the physical width supported by BYTE_STREAM_SPLIT, or no value for
/// other types.
std::optional<std::int32_t>
byte_stream_split_width(const ColumnChunkInfo &column) {
  if (!column.has_physical_type) {
    return std::nullopt;
  }
  if (column.physical_type == kPhysicalFloat) {
    return 4;
  }
  if (column.physical_type == kPhysicalDouble) {
    return 8;
  }
  return std::nullopt;
}

/// Builds a bounded diagnostic representation of byte stream split values
/// without retaining full page data.
std::vector<std::string> preview_byte_stream_split_values(
    std::string_view values, const ColumnChunkInfo &column,
    std::int32_t expected_values, std::int32_t width) {
  std::vector<std::string> out;
  const auto limit = std::min<std::int32_t>(
      expected_values, static_cast<std::int32_t>(kMaxValuePreviewItems));
  out.reserve(static_cast<std::size_t>(limit));
  std::array<char, 8> bytes{};
  for (std::int32_t value_index = 0; value_index < limit; ++value_index) {
    for (std::int32_t byte_index = 0; byte_index < width; ++byte_index) {
      const auto source_offset = static_cast<std::size_t>(byte_index) *
                                     static_cast<std::size_t>(expected_values) +
                                 static_cast<std::size_t>(value_index);
      bytes[static_cast<std::size_t>(byte_index)] = values[source_offset];
    }
    if (column.physical_type == kPhysicalFloat) {
      out.push_back(std::to_string(read_plain_value<float>(
          std::string_view(bytes.data(), static_cast<std::size_t>(width)), 0)));
    } else {
      out.push_back(std::to_string(read_plain_value<double>(
          std::string_view(bytes.data(), static_cast<std::size_t>(width)), 0)));
    }
  }
  return out;
}

/// Reassembles one BYTE_STREAM_SPLIT page and records its decoded values and
/// preview.
sanitize::Status decode_byte_stream_split_page(std::string_view payload,
                                               const ColumnChunkInfo &column,
                                               PageHeaderInfo *page) {
  if (!page || page->value_payload_offset < 0 ||
      static_cast<std::size_t>(page->value_payload_offset) > payload.size()) {
    return sanitize::Status::Invalid(
        "Parquet values: invalid value payload offset");
  }
  const auto values =
      payload.substr(static_cast<std::size_t>(page->value_payload_offset));
  const auto expected_values = page->decoded_non_null_values;
  auto width = byte_stream_split_width(column);
  if (!width) {
    page->values_decode_skipped = true;
    return {};
  }
  if (expected_values < 0) {
    return sanitize::Status::Invalid("Parquet values: negative value count");
  }
  const auto expected_bytes =
      static_cast<std::int64_t>(*width) * expected_values;
  if (expected_bytes < 0 ||
      expected_bytes > std::numeric_limits<std::int32_t>::max()) {
    return sanitize::Status::Invalid(
        "Parquet values: BYTE_STREAM_SPLIT payload too large");
  }
  if (values.size() != static_cast<std::size_t>(expected_bytes)) {
    return sanitize::Status::Invalid(
        "Parquet values: BYTE_STREAM_SPLIT payload size mismatch");
  }
  page->decoded_value_bytes = static_cast<std::int32_t>(values.size());
  SAN_ASSIGN_OR_RAISE(page->materialized_value_bytes,
                      arrow_fixed_width_value_buffer_bytes(
                          page->num_values, *width,
                          "BYTE_STREAM_SPLIT materialized value buffer"));
  page->decoded_value_preview =
      preview_byte_stream_split_values(values, column, expected_values, *width);
  page->values_decoded = true;
  page->values_decode_skipped = false;
  return {};
}
