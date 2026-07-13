// Decodes and validates DELTA_BINARY_PACKED Parquet pages.

template <class Emit>
sanitize::Result<std::size_t>
decode_delta_binary_packed_stream(std::string_view values,
                                  std::int32_t expected_values, Emit emit) {
  if (expected_values < 0) {
    return sanitize::Status::Invalid("Parquet values: negative value count");
  }
  if (expected_values == 0) {
    return 0;
  }

  std::size_t offset = 0;
  std::uint64_t block_size = 0;
  std::uint64_t miniblock_count = 0;
  std::uint64_t value_count = 0;
  std::int64_t previous = 0;
  SAN_ASSIGN_OR_RAISE(block_size, read_varint_from(values, &offset));
  SAN_ASSIGN_OR_RAISE(miniblock_count, read_varint_from(values, &offset));
  SAN_ASSIGN_OR_RAISE(value_count, read_varint_from(values, &offset));
  SAN_ASSIGN_OR_RAISE(previous, read_zigzag_varint_from(values, &offset));

  if (block_size == 0 || miniblock_count == 0 ||
      block_size % miniblock_count != 0) {
    return sanitize::Status::Invalid(
        "Parquet values: invalid DELTA_BINARY_PACKED block header");
  }
  if (block_size > static_cast<std::uint64_t>(
                       std::numeric_limits<std::int32_t>::max()) ||
      miniblock_count > static_cast<std::uint64_t>(
                            std::numeric_limits<std::int32_t>::max()) ||
      value_count > static_cast<std::uint64_t>(
                        std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_BINARY_PACKED header count out of range");
  }
  if (value_count != static_cast<std::uint64_t>(expected_values)) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_BINARY_PACKED value count mismatch");
  }
  SAN_RETURN_NOT_OK(emit(previous));

  const auto miniblock_value_count = block_size / miniblock_count;
  std::uint64_t decoded = 1;
  while (decoded < value_count) {
    std::int64_t min_delta = 0;
    SAN_ASSIGN_OR_RAISE(min_delta, read_zigzag_varint_from(values, &offset));
    if (miniblock_count > static_cast<std::uint64_t>(
                              std::numeric_limits<std::size_t>::max()) ||
        values.size() - offset < static_cast<std::size_t>(miniblock_count)) {
      return sanitize::Status::Invalid(
          "Parquet values: truncated DELTA_BINARY_PACKED bit widths");
    }

    const auto bit_widths =
        values.substr(offset, static_cast<std::size_t>(miniblock_count));
    offset += static_cast<std::size_t>(miniblock_count);
    for (const char encoded_width : bit_widths) {
      if (static_cast<std::uint8_t>(encoded_width) > 64) {
        return sanitize::Status::Invalid(
            "Parquet values: DELTA_BINARY_PACKED bit width too large");
      }
    }

    for (std::uint64_t mini = 0; mini < miniblock_count; ++mini) {
      const auto bit_width =
          static_cast<std::uint8_t>(bit_widths[static_cast<std::size_t>(mini)]);
      if (miniblock_value_count > std::numeric_limits<std::uint64_t>::max() /
                                      std::max<std::uint8_t>(bit_width, 1)) {
        return sanitize::Status::Invalid(
            "Parquet values: DELTA_BINARY_PACKED miniblock too large");
      }
      const auto packed_bits = miniblock_value_count * bit_width;
      const auto packed_bytes =
          static_cast<std::size_t>((packed_bits + 7U) / 8U);
      if (values.size() - offset < packed_bytes) {
        return sanitize::Status::Invalid(
            "Parquet values: truncated DELTA_BINARY_PACKED miniblock");
      }
      const auto miniblock_payload = values.substr(offset, packed_bytes);
      const auto remaining = value_count - decoded;
      const auto values_to_read = std::min(miniblock_value_count, remaining);
      for (std::uint64_t i = 0; i < values_to_read; ++i) {
        std::uint64_t adjusted = 0;
        if (bit_width > 0) {
          SAN_ASSIGN_OR_RAISE(
              adjusted,
              read_bit_packed_u64(miniblock_payload,
                                  static_cast<std::size_t>(i * bit_width),
                                  bit_width));
        }
        if (adjusted > static_cast<std::uint64_t>(
                           std::numeric_limits<std::int64_t>::max())) {
          return sanitize::Status::Invalid(
              "Parquet values: DELTA_BINARY_PACKED adjusted value too large");
        }
        const auto adjusted_i64 = static_cast<std::int64_t>(adjusted);
        if ((min_delta > 0 &&
             previous > std::numeric_limits<std::int64_t>::max() - min_delta) ||
            (min_delta < 0 &&
             previous < std::numeric_limits<std::int64_t>::min() - min_delta)) {
          return sanitize::Status::Invalid(
              "Parquet values: DELTA_BINARY_PACKED value overflow");
        }
        const auto with_min_delta = previous + min_delta;
        if (adjusted_i64 > 0 &&
            with_min_delta >
                std::numeric_limits<std::int64_t>::max() - adjusted_i64) {
          return sanitize::Status::Invalid(
              "Parquet values: DELTA_BINARY_PACKED value overflow");
        }
        previous = with_min_delta + adjusted_i64;
        SAN_RETURN_NOT_OK(emit(previous));
        ++decoded;
      }
      offset += packed_bytes;
    }
  }
  if (decoded >
      static_cast<std::uint64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet values: decoded DELTA_BINARY_PACKED count out of range");
  }
  return offset;
}

sanitize::Result<std::int32_t> decode_delta_binary_packed_values(
    std::string_view values, const ColumnChunkInfo &column,
    std::int32_t expected_values, std::vector<std::string> *preview) {
  if (!column.has_physical_type || (column.physical_type != kPhysicalInt32 &&
                                    column.physical_type != kPhysicalInt64)) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_BINARY_PACKED requires integer physical type");
  }
  if (preview) {
    preview->clear();
  }
  std::int32_t decoded = 0;
  std::size_t consumed = 0;
  SAN_ASSIGN_OR_RAISE(
      consumed,
      decode_delta_binary_packed_stream(
          values, expected_values, [&](std::int64_t value) -> sanitize::Status {
            if (column.physical_type == kPhysicalInt32 &&
                (value < std::numeric_limits<std::int32_t>::min() ||
                 value > std::numeric_limits<std::int32_t>::max())) {
              return sanitize::Status::Invalid(
                  "Parquet values: DELTA_BINARY_PACKED int32 out of range");
            }
            if (preview && preview->size() < kMaxValuePreviewItems) {
              preview->push_back(std::to_string(value));
            }
            ++decoded;
            return {};
          }));
  if (consumed != values.size()) {
    return sanitize::Status::Invalid(
        "Parquet values: trailing DELTA_BINARY_PACKED payload bytes");
  }
  if (decoded != expected_values) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_BINARY_PACKED decoded count mismatch");
  }
  return static_cast<std::int32_t>(decoded);
}

sanitize::Status decode_delta_binary_packed_page(std::string_view payload,
                                                 const ColumnChunkInfo &column,
                                                 PageHeaderInfo *page) {
  if (!page || page->value_payload_offset < 0 ||
      static_cast<std::size_t>(page->value_payload_offset) > payload.size()) {
    return sanitize::Status::Invalid(
        "Parquet values: invalid value payload offset");
  }
  const auto values =
      payload.substr(static_cast<std::size_t>(page->value_payload_offset));
  std::vector<std::string> preview;
  std::int32_t decoded_values = 0;
  SAN_ASSIGN_OR_RAISE(
      decoded_values,
      decode_delta_binary_packed_values(
          values, column, page->decoded_non_null_values, &preview));
  if (decoded_values != page->decoded_non_null_values) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_BINARY_PACKED decoded count mismatch");
  }
  if (values.size() >
      static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_BINARY_PACKED payload too large");
  }
  page->decoded_value_bytes = static_cast<std::int32_t>(values.size());
  auto width = arrow_value_width_for_column(column);
  if (!width || *width <= 0) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_BINARY_PACKED materialized width missing");
  }
  SAN_ASSIGN_OR_RAISE(page->materialized_value_bytes,
                      arrow_fixed_width_value_buffer_bytes(
                          page->num_values, *width,
                          "DELTA_BINARY_PACKED materialized value buffer"));
  page->decoded_value_preview = std::move(preview);
  page->values_decoded = true;
  page->values_decode_skipped = false;
  return {};
}
