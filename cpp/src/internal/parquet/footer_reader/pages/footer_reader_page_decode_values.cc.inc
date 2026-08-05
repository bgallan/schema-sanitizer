bool page_values_physical_type_supported(const ColumnChunkInfo &column) {
  if (!column.has_physical_type) {
    return false;
  }
  switch (column.physical_type) {
  case kPhysicalBoolean:
  case kPhysicalInt32:
  case kPhysicalInt64:
  case kPhysicalFloat:
  case kPhysicalDouble:
  case kPhysicalByteArray:
  case kPhysicalFixedLenByteArray:
    return true;
  default:
    return false;
  }
}

sanitize::Status decode_page_values(std::string_view payload,
                                    const ColumnChunkInfo &column,
                                    PageHeaderInfo *page,
                                    const DictionaryPageState *dictionary) {
  if (!page || page->is_dictionary_page || !page->levels_decoded ||
      !page->has_value_encoding) {
    if (page && !page->is_dictionary_page) {
      page->values_decode_skipped = true;
    }
    return {};
  }
  if (!page_values_physical_type_supported(column)) {
    page->values_decode_skipped = true;
    return {};
  }
  if (page->value_encoding == kEncodingPlain) {
    return decode_plain_values(payload, column, page);
  }
  if (page->value_encoding == kEncodingDeltaBinaryPacked) {
    return decode_delta_binary_packed_page(payload, column, page);
  }
  if (page->value_encoding == kEncodingDeltaLengthByteArray) {
    return decode_delta_length_byte_array_page(payload, column, page);
  }
  if (page->value_encoding == kEncodingRleDictionary) {
    return decode_rle_dictionary_page(payload, column, dictionary, page);
  }
  if (page->value_encoding == kEncodingByteStreamSplit) {
    return decode_byte_stream_split_page(payload, column, page);
  }
  page->values_decode_skipped = true;
  return {};
}

sanitize::Status decode_page_levels(std::string_view payload,
                                    const ColumnChunkInfo &column,
                                    PageHeaderInfo *page,
                                    const DictionaryPageState *dictionary,
                                    std::uint64_t max_derived_bytes) {
  if (!page || page->is_dictionary_page || !page->has_num_values) {
    return {};
  }
  std::size_t offset = 0;
  std::uint64_t planned_level_bytes = 0;
  if (column.max_repetition_level > 0) {
    LevelDecodeInfo repetition;
    SAN_ASSIGN_OR_RAISE(
        repetition,
        decode_level_stream(payload, &offset, column.max_repetition_level,
                            page->num_values, false, true, max_derived_bytes,
                            &planned_level_bytes));
    page->decoded_repetition_levels = repetition.decoded_count;
    page->decoded_repetition_level_values = std::move(repetition.level_values);
  }
  LevelDecodeInfo definition;
  definition.max_level_count = page->num_values;
  const bool capture_definition_level_values =
      column.max_repetition_level > 0 ||
      (column.max_definition_level > 0 &&
       (column.path_in_schema.size() > 2 ||
        (column.path_in_schema.size() == 2 && !column.top_level_required)));
  if (column.max_definition_level > 0) {
    SAN_ASSIGN_OR_RAISE(
        definition, decode_level_stream(
                        payload, &offset, column.max_definition_level,
                        page->num_values, true, capture_definition_level_values,
                        max_derived_bytes, &planned_level_bytes));
    page->decoded_definition_levels = definition.decoded_count;
  } else {
    SAN_RETURN_NOT_OK(initialize_validity_bitmap(page->num_values, true,
                                                 &definition.validity_bitmap));
  }
  if (offset >
      static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet levels: value payload offset out of range");
  }
  page->value_payload_offset = static_cast<std::int32_t>(offset);
  page->decoded_non_null_values = definition.max_level_count;
  page->decoded_null_values = page->num_values - definition.max_level_count;
  if (definition.validity_bitmap.size() >
      static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet levels: validity bitmap too large");
  }
  page->decoded_validity_bitmap = std::move(definition.validity_bitmap);
  page->decoded_definition_level_values = std::move(definition.level_values);
  page->decoded_validity_bytes =
      static_cast<std::int32_t>(page->decoded_validity_bitmap.size());
  page->validity_bitmap_decoded = true;
  page->levels_decoded = true;
  SAN_RETURN_NOT_OK(decode_page_values(payload, column, page, dictionary));
  return {};
}

sanitize::Status decode_dictionary_page_values(std::string_view payload,
                                               const ColumnChunkInfo &column,
                                               PageHeaderInfo *page,
                                               DictionaryPageState *state) {
  if (!page || !page->is_dictionary_page || !page->has_num_values ||
      !page->has_value_encoding) {
    return {};
  }
  if (page->num_values < 0 || page->num_values > kMaxDictionaryEntries) {
    return sanitize::Status::Invalid(
        "Parquet dictionary: entry count exceeds security limit");
  }
  if (page->value_encoding != kEncodingPlain) {
    page->values_decode_skipped = true;
    return {};
  }
  PlainValueDecodeInfo decoded;
  auto decoded_result =
      decode_plain_value_payload(payload, column, page->num_values);
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
  page->decoded_value_preview = decoded.preview;
  page->decoded_byte_array_values = decoded.byte_array_values;
  page->decoded_fixed_width_values = decoded.fixed_width_values;
  page->values_decoded = true;
  page->values_decode_skipped = false;
  if (state) {
    state->decoded = true;
    state->value_count = page->num_values;
    state->preview = std::move(decoded.preview);
    state->byte_array_values = std::move(decoded.byte_array_values);
    state->fixed_width_values = std::move(decoded.fixed_width_values);
  }
  return {};
}

sanitize::Status decode_verified_page_payload(std::string_view payload,
                                              const ColumnChunkInfo &column,
                                              PageHeaderInfo *page,
                                              DictionaryPageState *dictionary,
                                              std::uint64_t max_derived_bytes) {
  if (page && page->is_dictionary_page) {
    return decode_dictionary_page_values(payload, column, page, dictionary);
  }
  return decode_page_levels(payload, column, page, dictionary,
                            max_derived_bytes);
}
