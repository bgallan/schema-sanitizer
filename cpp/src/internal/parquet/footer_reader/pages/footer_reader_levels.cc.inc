// Decodes Parquet definition and repetition levels into bounded validity and
// layout buffers. This fragment is included by the consolidated footer reader
// inside its private implementation namespace.

/// Initializes an all-valid or all-null bitmap for the expected value count.
sanitize::Status initialize_validity_bitmap(std::int32_t expected_count,
                                            bool all_valid,
                                            std::vector<std::uint8_t> *bitmap) {
  if (!bitmap) {
    return {};
  }
  if (expected_count < 0) {
    return sanitize::Status::Invalid("Parquet levels: negative expected count");
  }
  const auto bytes = (static_cast<std::size_t>(expected_count) + 7U) / 8U;
  if (bytes > kMaxValidityBitmapBytes) {
    return sanitize::Status::Invalid(
        "Parquet levels: validity bitmap exceeds memory limit");
  }
  bitmap->assign(bytes, all_valid ? static_cast<std::uint8_t>(0xFFU)
                                  : static_cast<std::uint8_t>(0));
  if (all_valid && expected_count % 8 != 0 && !bitmap->empty()) {
    const auto valid_bits = static_cast<std::uint8_t>(expected_count % 8);
    bitmap->back() = static_cast<std::uint8_t>((1U << valid_bits) - 1U);
  }
  return {};
}

/// Sets validity bit in a prevalidated Arrow bitmap without touching adjacent
/// bits.
void set_validity_bit(std::vector<std::uint8_t> *bitmap, std::int32_t index,
                      bool valid) {
  if (!bitmap || index < 0) {
    return;
  }
  const auto byte_index = static_cast<std::size_t>(index / 8);
  if (byte_index >= bitmap->size()) {
    return;
  }
  const auto mask = static_cast<std::uint8_t>(1U << (index % 8));
  if (valid) {
    (*bitmap)[byte_index] =
        static_cast<std::uint8_t>((*bitmap)[byte_index] | mask);
  } else {
    (*bitmap)[byte_index] =
        static_cast<std::uint8_t>((*bitmap)[byte_index] & ~mask);
  }
}
/// Reads one unsigned varint from the level stream and advances its bounded
/// offset.
sanitize::Result<std::uint64_t> read_varint_from(std::string_view data,
                                                 std::size_t *offset) {
  if (!offset) {
    return sanitize::Status::Invalid("Parquet levels: internal offset error");
  }
  std::uint64_t out = 0;
  int shift = 0;
  for (int i = 0; i < 10; ++i) {
    if (*offset >= data.size()) {
      return sanitize::Status::Invalid("Parquet levels: truncated varint");
    }
    const auto byte = static_cast<std::uint8_t>(data[(*offset)++]);
    out |= static_cast<std::uint64_t>(byte & 0x7FU) << shift;
    if ((byte & 0x80U) == 0) {
      return out;
    }
    shift += 7;
  }
  return sanitize::Status::Invalid("Parquet levels: invalid varint");
}

/// Reads one zigzag varint from the level stream and returns its signed value.
sanitize::Result<std::int64_t> read_zigzag_varint_from(std::string_view data,
                                                       std::size_t *offset) {
  std::uint64_t raw = 0;
  SAN_ASSIGN_OR_RAISE(raw, read_varint_from(data, offset));
  return unzigzag(raw);
}

/// Reads one little-endian bit-packed level value without crossing the supplied
/// input span.
sanitize::Result<std::uint64_t> read_bit_packed_u64(std::string_view data,
                                                    std::size_t bit_index,
                                                    std::uint8_t bit_width) {
  if (bit_width > 64) {
    return sanitize::Status::Invalid(
        "Parquet values: bit width exceeds uint64");
  }
  std::uint64_t out = 0;
  for (std::uint8_t bit = 0; bit < bit_width; ++bit) {
    const auto absolute_bit = bit_index + bit;
    const auto byte_index = absolute_bit / 8U;
    if (byte_index >= data.size()) {
      return sanitize::Status::Invalid(
          "Parquet values: truncated bit-packed value");
    }
    const auto byte = static_cast<std::uint8_t>(data[byte_index]);
    if ((byte & (std::uint8_t{1} << (absolute_bit % 8U))) != 0) {
      out |= std::uint64_t{1} << bit;
    }
  }
  return out;
}

/// Reads one little-endian bit-packed level without crossing the supplied byte
/// span.
sanitize::Result<std::int16_t> read_little_level_value(std::string_view data,
                                                       std::size_t *offset,
                                                       std::uint8_t bit_width) {
  if (!offset) {
    return sanitize::Status::Invalid("Parquet levels: internal offset error");
  }
  const auto byte_width = static_cast<std::size_t>((bit_width + 7U) / 8U);
  if (data.size() - *offset < byte_width) {
    return sanitize::Status::Invalid("Parquet levels: truncated RLE value");
  }
  std::uint32_t value = 0;
  for (std::size_t i = 0; i < byte_width; ++i) {
    value |=
        static_cast<std::uint32_t>(static_cast<std::uint8_t>(data[*offset + i]))
        << (i * 8U);
  }
  *offset += byte_width;
  if (value >
      static_cast<std::uint32_t>(std::numeric_limits<std::int16_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet levels: level value out of range");
  }
  return static_cast<std::int16_t>(value);
}
/// Charges level derived bytes to the active memory budget before allocating
/// derived storage.
sanitize::Status reserve_level_derived_bytes(
    std::int32_t expected_count, bool capture_validity_bitmap,
    bool capture_level_values, std::uint64_t max_derived_bytes,
    std::uint64_t *planned_derived_bytes) {
  if (!planned_derived_bytes || expected_count < 0) {
    return sanitize::Status::Invalid(
        "Parquet levels: invalid derived-budget state");
  }
  const auto count = static_cast<std::uint64_t>(expected_count);
  std::uint64_t required = 0;
  if (capture_level_values) {
    if (count >
        std::numeric_limits<std::uint64_t>::max() / sizeof(std::int16_t)) {
      return sanitize::Status::Invalid(
          "Parquet levels: level output size overflows");
    }
    required = count * sizeof(std::int16_t);
  }
  if (capture_validity_bitmap) {
    const auto validity = (count + 7U) / 8U;
    if (validity > std::numeric_limits<std::uint64_t>::max() - required) {
      return sanitize::Status::Invalid(
          "Parquet levels: validity output size overflows");
    }
    required += validity;
  }
  if (*planned_derived_bytes > max_derived_bytes ||
      required > max_derived_bytes - *planned_derived_bytes) {
    return sanitize::Status::Invalid(
        "Parquet levels: derived level output exceeds effective operation "
        "limit ",
        max_derived_bytes);
  }
  *planned_derived_bytes += required;
  return {};
}

/// Decodes an RLE/bit-packed level stream into bounded 16-bit values.
sanitize::Result<LevelDecodeInfo>
decode_level_stream(std::string_view payload, std::size_t *offset,
                    std::int16_t max_level, std::int32_t expected_count,
                    bool capture_validity_bitmap, bool capture_level_values,
                    std::uint64_t max_derived_bytes,
                    std::uint64_t *planned_derived_bytes) {
  if (!offset) {
    return sanitize::Status::Invalid("Parquet levels: internal offset error");
  }
  if (expected_count < 0) {
    return sanitize::Status::Invalid("Parquet levels: negative expected count");
  }
  SAN_RETURN_NOT_OK(reserve_level_derived_bytes(
      expected_count, capture_validity_bitmap, capture_level_values,
      max_derived_bytes, planned_derived_bytes));
  if (max_level <= 0) {
    LevelDecodeInfo info;
    info.max_level_count = expected_count;
    if (capture_level_values) {
      info.level_values.assign(static_cast<std::size_t>(expected_count), 0);
    }
    if (capture_validity_bitmap) {
      SAN_RETURN_NOT_OK(initialize_validity_bitmap(expected_count, true,
                                                   &info.validity_bitmap));
    }
    return info;
  }
  if (payload.size() - *offset < 4) {
    return sanitize::Status::Invalid("Parquet levels: missing length prefix");
  }
  const auto encoded_size =
      static_cast<std::size_t>(read_u32_le(payload.data() + *offset));
  *offset += 4;
  if (payload.size() - *offset < encoded_size) {
    return sanitize::Status::Invalid("Parquet levels: stream exceeds payload");
  }
  const auto encoded = payload.substr(*offset, encoded_size);
  *offset += encoded_size;

  const auto bit_width = level_bit_width(max_level);
  std::size_t encoded_offset = 0;
  LevelDecodeInfo info;
  if (capture_validity_bitmap) {
    SAN_RETURN_NOT_OK(initialize_validity_bitmap(expected_count, false,
                                                 &info.validity_bitmap));
  }
  if (capture_level_values) {
    info.level_values.reserve(static_cast<std::size_t>(expected_count));
  }
  while (encoded_offset < encoded.size() &&
         info.decoded_count < expected_count) {
    std::uint64_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, read_varint_from(encoded, &encoded_offset));
    if ((header & 1U) == 0) {
      const auto run_length = static_cast<std::int64_t>(header >> 1U);
      std::int16_t value = 0;
      SAN_ASSIGN_OR_RAISE(
          value, read_little_level_value(encoded, &encoded_offset, bit_width));
      if (value > max_level) {
        return sanitize::Status::Invalid(
            "Parquet levels: RLE level exceeds max");
      }
      if (run_length > expected_count - info.decoded_count) {
        return sanitize::Status::Invalid("Parquet levels: RLE run too long");
      }
      const auto run_start = info.decoded_count;
      info.decoded_count += static_cast<std::int32_t>(run_length);
      if (capture_level_values) {
        info.level_values.insert(info.level_values.end(),
                                 static_cast<std::size_t>(run_length), value);
      }
      if (value == max_level) {
        info.max_level_count += static_cast<std::int32_t>(run_length);
      }
      if (capture_validity_bitmap) {
        const bool valid = value == max_level;
        for (std::int64_t i = 0; i < run_length; ++i) {
          set_validity_bit(&info.validity_bitmap,
                           run_start + static_cast<std::int32_t>(i), valid);
        }
      }
      continue;
    }
    const auto groups = header >> 1U;
    const auto value_count = groups * 8U;
    for (std::uint64_t i = 0;
         i < value_count && info.decoded_count < expected_count; ++i) {
      const auto bit_index = static_cast<std::size_t>(i) * bit_width;
      const auto byte_index = bit_index / 8U;
      const auto bit_offset = bit_index % 8U;
      const auto needed = byte_index + ((bit_offset + bit_width + 7U) / 8U);
      if (needed > encoded.size() - encoded_offset) {
        return sanitize::Status::Invalid(
            "Parquet levels: truncated bit-packed run");
      }
      std::uint32_t scratch = 0;
      for (std::size_t b = 0;
           b < 4 && byte_index + b < encoded.size() - encoded_offset; ++b) {
        scratch |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(
                       encoded[encoded_offset + byte_index + b]))
                   << (b * 8U);
      }
      const auto mask = bit_width >= 32
                            ? std::numeric_limits<std::uint32_t>::max()
                            : ((std::uint32_t{1} << bit_width) - 1U);
      const auto value =
          static_cast<std::int16_t>((scratch >> bit_offset) & mask);
      if (value > max_level) {
        return sanitize::Status::Invalid(
            "Parquet levels: bit-packed level exceeds max");
      }
      if (capture_validity_bitmap) {
        set_validity_bit(&info.validity_bitmap, info.decoded_count,
                         value == max_level);
      }
      if (capture_level_values) {
        info.level_values.push_back(value);
      }
      ++info.decoded_count;
      if (value == max_level) {
        ++info.max_level_count;
      }
    }
    const auto packed_bytes =
        static_cast<std::size_t>((value_count * bit_width + 7U) / 8U);
    if (encoded.size() - encoded_offset < packed_bytes) {
      return sanitize::Status::Invalid(
          "Parquet levels: truncated packed bytes");
    }
    encoded_offset += packed_bytes;
  }
  if (info.decoded_count != expected_count) {
    return sanitize::Status::Invalid("Parquet levels: decoded count mismatch");
  }
  if (capture_level_values &&
      info.level_values.size() != static_cast<std::size_t>(expected_count)) {
    return sanitize::Status::Invalid(
        "Parquet levels: decoded level value count mismatch");
  }
  return info;
}
