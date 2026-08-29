// Decodes dictionary-page values and records the typed dictionary state used by
// data pages. This fragment is included by the consolidated footer reader
// inside its private implementation namespace.

/// Resolves an RLE_DICTIONARY data page through the previously decoded
/// dictionary.
sanitize::Status decode_rle_dictionary_page(
    std::string_view payload, const ColumnChunkInfo &column,
    const DictionaryPageState *dictionary, PageHeaderInfo *page) {
  if (!page || page->value_payload_offset < 0 ||
      static_cast<std::size_t>(page->value_payload_offset) > payload.size()) {
    return sanitize::Status::Invalid(
        "Parquet values: invalid value payload offset");
  }
  if (!dictionary) {
    page->values_decode_skipped = true;
    return {};
  }
  const auto values =
      payload.substr(static_cast<std::size_t>(page->value_payload_offset));
  std::vector<std::string> preview;
  std::vector<std::uint32_t> indices;
  std::int32_t decoded_values = 0;
  std::int32_t index_bit_width = 0;
  SAN_ASSIGN_OR_RAISE(decoded_values,
                      decode_rle_dictionary_indices(
                          values, *dictionary, page->decoded_non_null_values,
                          &preview, &indices, &index_bit_width));
  if (decoded_values != page->decoded_non_null_values) {
    return sanitize::Status::Invalid(
        "Parquet values: dictionary decoded count mismatch");
  }
  if (values.size() >
      static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet values: dictionary index payload too large");
  }
  page->decoded_value_bytes = static_cast<std::int32_t>(values.size());
  page->dictionary_index_bit_width = index_bit_width;
  if (column.has_physical_type && column.physical_type == kPhysicalByteArray &&
      !dictionary->byte_array_values.empty()) {
    std::int64_t materialized_bytes = 0;
    for (const auto index : indices) {
      const auto &value =
          dictionary->byte_array_values[static_cast<std::size_t>(index)];
      if (value.size() > static_cast<std::size_t>(
                             std::numeric_limits<std::int32_t>::max()) ||
          materialized_bytes > std::numeric_limits<std::int32_t>::max() -
                                   static_cast<std::int64_t>(value.size())) {
        return sanitize::Status::Invalid(
            "Parquet values: dictionary materialized value buffer too large");
      }
      materialized_bytes += static_cast<std::int64_t>(value.size());
    }
    page->materialized_value_bytes =
        static_cast<std::int32_t>(materialized_bytes);
    SAN_ASSIGN_OR_RAISE(page->materialized_offset_bytes,
                        arrow_i32_offset_buffer_bytes(page->num_values));
  } else if (!dictionary->fixed_width_values.empty()) {
    auto physical_width = fixed_width_for_plain_values(column);
    if (!physical_width || *physical_width <= 0) {
      return sanitize::Status::Invalid(
          "Parquet values: dictionary fixed-width value width is invalid");
    }
    const auto expected_dictionary_bytes =
        static_cast<std::int64_t>(dictionary->value_count) *
        static_cast<std::int64_t>(*physical_width);
    if (expected_dictionary_bytes < 0 ||
        static_cast<std::uint64_t>(expected_dictionary_bytes) !=
            dictionary->fixed_width_values.size()) {
      return sanitize::Status::Invalid(
          "Parquet values: dictionary fixed-width payload size mismatch");
    }
    auto arrow_width = arrow_value_width_for_column(column);
    if (!arrow_width || *arrow_width <= 0) {
      return sanitize::Status::Invalid(
          "Parquet values: dictionary fixed-width Arrow width is invalid");
    }
    SAN_ASSIGN_OR_RAISE(
        page->materialized_value_bytes,
        arrow_fixed_width_value_buffer_bytes(
            page->num_values, *arrow_width,
            "dictionary fixed-width materialized value buffer"));
  } else {
    SAN_ASSIGN_OR_RAISE(page->materialized_value_bytes,
                        arrow_fixed_width_value_buffer_bytes(
                            page->num_values,
                            static_cast<std::int32_t>(sizeof(std::int32_t)),
                            "dictionary index value buffer"));
  }
  page->decoded_value_preview = std::move(preview);
  page->values_decoded = true;
  page->values_decode_skipped = false;
  return {};
}
