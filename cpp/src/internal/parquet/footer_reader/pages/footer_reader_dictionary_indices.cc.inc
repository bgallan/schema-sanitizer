// Decodes dictionary indices and materializes dictionary-backed data pages.

std::string dictionary_preview_value(const DictionaryPageState &dictionary,
                                     std::uint32_t index) {
  if (index < dictionary.preview.size()) {
    return dictionary.preview[static_cast<std::size_t>(index)];
  }
  return std::string("@dict[") + std::to_string(index) + "]";
}

sanitize::Result<std::uint32_t> read_little_u32_value(std::string_view data,
                                                      std::size_t *offset,
                                                      std::uint8_t bit_width) {
  if (!offset) {
    return sanitize::Status::Invalid("Parquet values: internal offset error");
  }
  if (bit_width > 32) {
    return sanitize::Status::Invalid(
        "Parquet values: dictionary index bit width too large");
  }
  const auto byte_width = static_cast<std::size_t>((bit_width + 7U) / 8U);
  if (data.size() - *offset < byte_width) {
    return sanitize::Status::Invalid(
        "Parquet values: truncated dictionary RLE value");
  }
  std::uint32_t value = 0;
  for (std::size_t i = 0; i < byte_width; ++i) {
    value |=
        static_cast<std::uint32_t>(static_cast<std::uint8_t>(data[*offset + i]))
        << (i * 8U);
  }
  *offset += byte_width;
  return value;
}

sanitize::Result<std::int32_t> decode_rle_dictionary_indices(
    std::string_view values, const DictionaryPageState &dictionary,
    std::int32_t expected_values, std::vector<std::string> *preview,
    std::vector<std::uint32_t> *indices, std::int32_t *index_bit_width) {
  if (!dictionary.decoded || dictionary.value_count <= 0) {
    return sanitize::Status::Invalid(
        "Parquet values: missing dictionary page before dictionary data page");
  }
  if (expected_values < 0) {
    return sanitize::Status::Invalid("Parquet values: negative value count");
  }
  if (values.empty()) {
    return sanitize::Status::Invalid(
        "Parquet values: missing dictionary index bit width");
  }
  if (preview) {
    preview->clear();
  }
  if (indices) {
    indices->clear();
    indices->reserve(static_cast<std::size_t>(expected_values));
  }
  std::size_t offset = 0;
  const auto bit_width = static_cast<std::uint8_t>(values[offset++]);
  if (bit_width == 0 || bit_width > 32) {
    return sanitize::Status::Invalid(
        "Parquet values: invalid dictionary index bit width");
  }
  if (index_bit_width) {
    *index_bit_width = static_cast<std::int32_t>(bit_width);
  }

  std::int32_t decoded = 0;
  const auto encoded = values.substr(offset);
  std::size_t encoded_offset = 0;
  while (encoded_offset < encoded.size() && decoded < expected_values) {
    std::uint64_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, read_varint_from(encoded, &encoded_offset));
    if ((header & 1U) == 0) {
      const auto run_length = static_cast<std::int64_t>(header >> 1U);
      if (run_length < 0 || run_length > expected_values - decoded) {
        return sanitize::Status::Invalid(
            "Parquet values: dictionary RLE run too long");
      }
      std::uint32_t index = 0;
      SAN_ASSIGN_OR_RAISE(
          index, read_little_u32_value(encoded, &encoded_offset, bit_width));
      if (index >= static_cast<std::uint32_t>(dictionary.value_count)) {
        return sanitize::Status::Invalid(
            "Parquet values: dictionary index out of range");
      }
      for (std::int64_t i = 0; i < run_length; ++i) {
        if (preview && preview->size() < kMaxValuePreviewItems) {
          preview->push_back(dictionary_preview_value(dictionary, index));
        }
        if (indices) {
          indices->push_back(index);
        }
      }
      decoded += static_cast<std::int32_t>(run_length);
      continue;
    }

    const auto groups = header >> 1U;
    const auto value_count = groups * 8U;
    if (value_count >
        static_cast<std::uint64_t>(std::numeric_limits<std::int32_t>::max())) {
      return sanitize::Status::Invalid(
          "Parquet values: dictionary bit-packed run too long");
    }
    const auto packed_bytes =
        static_cast<std::size_t>((value_count * bit_width + 7U) / 8U);
    if (encoded.size() - encoded_offset < packed_bytes) {
      return sanitize::Status::Invalid(
          "Parquet values: truncated dictionary bit-packed run");
    }
    const auto packed = encoded.substr(encoded_offset, packed_bytes);
    for (std::uint64_t i = 0; i < value_count && decoded < expected_values;
         ++i) {
      std::uint64_t index64 = 0;
      SAN_ASSIGN_OR_RAISE(
          index64,
          read_bit_packed_u64(packed, static_cast<std::size_t>(i * bit_width),
                              bit_width));
      if (index64 >= static_cast<std::uint64_t>(dictionary.value_count)) {
        return sanitize::Status::Invalid(
            "Parquet values: dictionary index out of range");
      }
      if (preview && preview->size() < kMaxValuePreviewItems) {
        preview->push_back(dictionary_preview_value(
            dictionary, static_cast<std::uint32_t>(index64)));
      }
      if (indices) {
        indices->push_back(static_cast<std::uint32_t>(index64));
      }
      ++decoded;
    }
    encoded_offset += packed_bytes;
  }
  if (decoded != expected_values) {
    return sanitize::Status::Invalid(
        "Parquet values: dictionary index count mismatch");
  }
  if (encoded_offset != encoded.size()) {
    return sanitize::Status::Invalid(
        "Parquet values: trailing dictionary index payload bytes");
  }
  return decoded;
}
