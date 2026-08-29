// Decodes PLAIN byte-array and fixed-width page values with exact bounds and
// output accounting. This fragment is included by the consolidated footer
// reader inside its private implementation namespace.

/// Decodes length-prefixed PLAIN byte arrays into raw values and bounded
/// previews.
sanitize::Result<std::int32_t> decode_plain_byte_array_values(
    std::string_view values, std::int32_t expected_values,
    std::vector<std::string> *preview, std::vector<std::string> *raw_values,
    std::int32_t *data_bytes) {
  if (expected_values < 0) {
    return sanitize::Status::Invalid("Parquet values: negative value count");
  }
  if (preview) {
    preview->clear();
  }
  if (raw_values) {
    raw_values->clear();
    raw_values->reserve(static_cast<std::size_t>(expected_values));
  }
  std::int64_t total_data_bytes = 0;
  std::size_t offset = 0;
  std::int32_t decoded = 0;
  while (decoded < expected_values) {
    if (values.size() - offset < 4) {
      return sanitize::Status::Invalid(
          "Parquet values: truncated BYTE_ARRAY length");
    }
    const auto size =
        static_cast<std::size_t>(read_u32_le(values.data() + offset));
    offset += 4;
    if (values.size() - offset < size) {
      return sanitize::Status::Invalid(
          "Parquet values: truncated BYTE_ARRAY payload");
    }
    if (preview && preview->size() < kMaxValuePreviewItems) {
      preview->push_back(preview_bytes(values.substr(offset, size)));
    }
    if (raw_values) {
      raw_values->emplace_back(values.substr(offset, size));
    }
    if (size > static_cast<std::size_t>(
                   std::numeric_limits<std::int32_t>::max()) ||
        total_data_bytes > std::numeric_limits<std::int32_t>::max() -
                               static_cast<std::int64_t>(size)) {
      return sanitize::Status::Invalid(
          "Parquet values: BYTE_ARRAY data payload too large");
    }
    total_data_bytes += static_cast<std::int64_t>(size);
    offset += size;
    ++decoded;
  }
  if (offset != values.size()) {
    return sanitize::Status::Invalid(
        "Parquet values: trailing BYTE_ARRAY payload bytes");
  }
  if (data_bytes) {
    *data_bytes = static_cast<std::int32_t>(total_data_bytes);
  }
  return decoded;
}

/// Decodes the PLAIN value section according to the column's physical type.
sanitize::Result<PlainValueDecodeInfo>
decode_plain_value_payload(std::string_view values,
                           const ColumnChunkInfo &column,
                           std::int32_t expected_values) {
  if (expected_values < 0) {
    return sanitize::Status::Invalid("Parquet values: negative value count");
  }
  PlainValueDecodeInfo info;
  if (column.has_physical_type && column.physical_type == kPhysicalBoolean) {
    const auto expected_bytes =
        static_cast<std::int32_t>((expected_values + 7) / 8);
    if (values.size() != static_cast<std::size_t>(expected_bytes)) {
      return sanitize::Status::Invalid(
          "Parquet values: boolean payload size mismatch");
    }
    info.decoded_bytes = expected_bytes;
    info.materialized_value_bytes = expected_bytes;
    info.preview = preview_plain_boolean_values(values, expected_values);
    return info;
  }
  if (column.has_physical_type && column.physical_type == kPhysicalByteArray) {
    std::int32_t decoded_values = 0;
    std::int32_t data_bytes = 0;
    SAN_ASSIGN_OR_RAISE(
        decoded_values,
        decode_plain_byte_array_values(values, expected_values, &info.preview,
                                       &info.byte_array_values, &data_bytes));
    if (decoded_values != expected_values) {
      return sanitize::Status::Invalid(
          "Parquet values: BYTE_ARRAY count mismatch");
    }
    if (values.size() >
        static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
      return sanitize::Status::Invalid(
          "Parquet values: BYTE_ARRAY payload too large");
    }
    info.decoded_bytes = static_cast<std::int32_t>(values.size());
    info.materialized_value_bytes = data_bytes;
    SAN_ASSIGN_OR_RAISE(info.materialized_offset_bytes,
                        arrow_i32_offset_buffer_bytes(expected_values));
    return info;
  }

  auto width = fixed_width_for_plain_values(column);
  if (!width) {
    return sanitize::Status::NotImplemented(
        "Parquet values: unsupported PLAIN physical type");
  }
  const auto expected_bytes =
      static_cast<std::int64_t>(*width) * expected_values;
  if (expected_bytes < 0 ||
      expected_bytes > std::numeric_limits<std::int32_t>::max()) {
    return sanitize::Status::Invalid(
        "Parquet values: fixed-width payload too large");
  }
  if (values.size() != static_cast<std::size_t>(expected_bytes)) {
    return sanitize::Status::Invalid(
        "Parquet values: fixed-width payload size mismatch");
  }
  info.decoded_bytes = static_cast<std::int32_t>(expected_bytes);
  auto arrow_width = arrow_value_width_for_column(column);
  if (!arrow_width || *arrow_width <= 0) {
    return sanitize::Status::NotImplemented(
        "Parquet values: unsupported Arrow fixed-width type");
  }
  SAN_ASSIGN_OR_RAISE(info.materialized_value_bytes,
                      arrow_fixed_width_value_buffer_bytes(
                          expected_values, *arrow_width, "PLAIN value buffer"));
  info.preview = preview_plain_fixed_values(values, column, expected_values);
  info.fixed_width_values.assign(values.begin(), values.end());
  return info;
}

/// Locates and decodes the PLAIN value section of one data page.
sanitize::Status decode_plain_values(std::string_view payload,
                                     const ColumnChunkInfo &column,
                                     PageHeaderInfo *page) {
  if (!page || page->is_dictionary_page || !page->levels_decoded ||
      !page->has_value_encoding || page->value_encoding != kEncodingPlain) {
    if (page && !page->is_dictionary_page) {
      page->values_decode_skipped = true;
    }
    return {};
  }
  if (page->value_payload_offset < 0 ||
      static_cast<std::size_t>(page->value_payload_offset) > payload.size()) {
    return sanitize::Status::Invalid(
        "Parquet values: invalid value payload offset");
  }
  const auto values =
      payload.substr(static_cast<std::size_t>(page->value_payload_offset));
  const auto expected_values = page->decoded_non_null_values;
  PlainValueDecodeInfo decoded;
  auto decoded_result =
      decode_plain_value_payload(values, column, expected_values);
  if (!decoded_result.ok()) {
    if (decoded_result.status().code() ==
        sanitize::StatusCode::kNotImplemented) {
      page->values_decode_skipped = true;
      return {};
    }
    return decoded_result.status();
  }
  decoded = std::move(decoded_result).ValueOrDie();
  page->decoded_value_bytes = decoded.decoded_bytes;
  page->materialized_value_bytes = decoded.materialized_value_bytes;
  page->materialized_offset_bytes = decoded.materialized_offset_bytes;
  if (column.has_physical_type && column.physical_type == kPhysicalBoolean) {
    SAN_ASSIGN_OR_RAISE(page->materialized_value_bytes,
                        arrow_boolean_value_buffer_bytes(page->num_values));
  } else if (column.has_physical_type &&
             column.physical_type == kPhysicalByteArray) {
    SAN_ASSIGN_OR_RAISE(page->materialized_offset_bytes,
                        arrow_i32_offset_buffer_bytes(page->num_values));
  } else {
    auto width = arrow_value_width_for_column(column);
    if (width && *width > 0) {
      SAN_ASSIGN_OR_RAISE(page->materialized_value_bytes,
                          arrow_fixed_width_value_buffer_bytes(
                              page->num_values, *width, "PLAIN value buffer"));
    }
  }
  page->decoded_value_preview = std::move(decoded.preview);
  page->values_decoded = true;
  page->values_decode_skipped = false;
  return {};
}
