#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
sanitize::Status gzip_decompress_payload_into(std::string_view payload,
                                              std::int32_t expected,
                                              std::string *out) {
  if (!out) {
    return sanitize::Status::Invalid(
        "Parquet page payload: decompression output is null");
  }
  if (expected < 0) {
    return sanitize::Status::Invalid(
        "Parquet page payload: negative uncompressed size");
  }
  if (static_cast<std::size_t>(expected) > kMaxPayloadVerificationBytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: uncompressed page exceeds verification limit");
  }
  bool decompressed = false;
  const auto expected_size = static_cast<std::size_t>(expected);
  out->resize_and_overwrite(
      expected_size, [&](char *data, std::size_t capacity) {
        if (capacity < expected_size) {
          return std::size_t{0};
        }
        z_stream stream{};
        stream.next_in =
            reinterpret_cast<Bytef *>(const_cast<char *>(payload.data()));
        stream.avail_in = static_cast<uInt>(payload.size());
        stream.next_out = reinterpret_cast<Bytef *>(data);
        stream.avail_out = static_cast<uInt>(expected_size);
        if (inflateInit2(&stream, 15 + 32) != Z_OK) {
          return std::size_t{0};
        }
        const int rc = inflate(&stream, Z_FINISH);
        const int end_rc = inflateEnd(&stream);
        decompressed = rc == Z_STREAM_END && end_rc == Z_OK &&
                       stream.total_out == static_cast<uLong>(expected_size);
        return decompressed ? expected_size : std::size_t{0};
      });
  if (!decompressed) {
    return sanitize::Status::Invalid(
        "Parquet page payload: gzip decompression failed");
  }
  return {};
}
#endif

sanitize::Result<std::uint32_t> read_snappy_varint(std::string_view payload,
                                                   std::size_t *offset) {
  if (!offset) {
    return sanitize::Status::Invalid(
        "Parquet page payload: snappy offset is null");
  }
  std::uint32_t out = 0;
  int shift = 0;
  for (int i = 0; i < 5; ++i) {
    if (*offset >= payload.size()) {
      return sanitize::Status::Invalid(
          "Parquet page payload: truncated snappy length");
    }
    const auto byte = static_cast<std::uint8_t>(payload[(*offset)++]);
    out |= static_cast<std::uint32_t>(byte & 0x7FU) << shift;
    if ((byte & 0x80U) == 0) {
      return out;
    }
    shift += 7;
  }
  return sanitize::Status::Invalid(
      "Parquet page payload: invalid snappy length");
}

sanitize::Result<std::uint32_t>
read_snappy_literal_length(std::uint8_t tag, std::string_view payload,
                           std::size_t *offset) {
  std::uint32_t literal_length = static_cast<std::uint32_t>(tag >> 2U);
  if (literal_length < 60U) {
    return literal_length + 1U;
  }
  const auto length_bytes = literal_length - 59U;
  if (length_bytes < 1U || length_bytes > 4U || !offset ||
      *offset > payload.size() || payload.size() - *offset < length_bytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: truncated snappy literal length");
  }
  literal_length = 0;
  for (std::uint32_t i = 0; i < length_bytes; ++i) {
    literal_length |= static_cast<std::uint32_t>(
                          static_cast<std::uint8_t>(payload[(*offset)++]))
                      << (8U * i);
  }
  if (literal_length == std::numeric_limits<std::uint32_t>::max()) {
    return sanitize::Status::Invalid(
        "Parquet page payload: invalid snappy literal length");
  }
  return literal_length + 1U;
}

sanitize::Status snappy_copy(std::string *out, std::uint32_t offset,
                             std::uint32_t length, std::size_t expected_size) {
  if (!out) {
    return sanitize::Status::Invalid(
        "Parquet page payload: snappy output is null");
  }
  if (offset == 0 || offset > out->size()) {
    return sanitize::Status::Invalid(
        "Parquet page payload: invalid snappy copy offset");
  }
  if (out->size() > expected_size || length > expected_size - out->size()) {
    return sanitize::Status::Invalid(
        "Parquet page payload: snappy copy exceeds expected size");
  }
  const auto source_offset = out->size() - static_cast<std::size_t>(offset);
  for (std::uint32_t i = 0; i < length; ++i) {
    out->push_back((*out)[source_offset + i]);
  }
  return {};
}

sanitize::Status snappy_decompress_payload_into(std::string_view payload,
                                                std::int32_t expected,
                                                std::string *out) {
  if (!out) {
    return sanitize::Status::Invalid(
        "Parquet page payload: decompression output is null");
  }
  if (expected < 0) {
    return sanitize::Status::Invalid(
        "Parquet page payload: negative uncompressed size");
  }
  if (static_cast<std::size_t>(expected) > kMaxPayloadVerificationBytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: uncompressed page exceeds verification limit");
  }
  std::size_t input_offset = 0;
  std::uint32_t declared_size = 0;
  SAN_ASSIGN_OR_RAISE(declared_size,
                      read_snappy_varint(payload, &input_offset));
  if (declared_size != static_cast<std::uint32_t>(expected)) {
    return sanitize::Status::Invalid(
        "Parquet page payload: snappy decompressed size mismatch");
  }
  out->clear();
  out->reserve(static_cast<std::size_t>(expected));
  while (input_offset < payload.size()) {
    const auto tag = static_cast<std::uint8_t>(payload[input_offset++]);
    switch (tag & 0x03U) {
    case 0: {
      std::uint32_t literal_length = 0;
      SAN_ASSIGN_OR_RAISE(literal_length, read_snappy_literal_length(
                                              tag, payload, &input_offset));
      if (literal_length > out->capacity() - out->size() ||
          input_offset > payload.size() ||
          payload.size() - input_offset < literal_length) {
        return sanitize::Status::Invalid(
            "Parquet page payload: truncated snappy literal");
      }
      out->append(payload.data() + input_offset,
                  static_cast<std::size_t>(literal_length));
      input_offset += literal_length;
      break;
    }
    case 1: {
      if (input_offset >= payload.size()) {
        return sanitize::Status::Invalid(
            "Parquet page payload: truncated snappy copy-1");
      }
      const auto length =
          static_cast<std::uint32_t>(((tag >> 2U) & 0x07U) + 4U);
      const auto offset = static_cast<std::uint32_t>(
          ((tag & 0xE0U) << 3U) |
          static_cast<std::uint8_t>(payload[input_offset++]));
      SAN_RETURN_NOT_OK(
          snappy_copy(out, offset, length, static_cast<std::size_t>(expected)));
      break;
    }
    case 2: {
      if (input_offset > payload.size() || payload.size() - input_offset < 2U) {
        return sanitize::Status::Invalid(
            "Parquet page payload: truncated snappy copy-2");
      }
      const auto length = static_cast<std::uint32_t>((tag >> 2U) + 1U);
      const auto offset =
          static_cast<std::uint32_t>(
              static_cast<std::uint8_t>(payload[input_offset])) |
          (static_cast<std::uint32_t>(
               static_cast<std::uint8_t>(payload[input_offset + 1U]))
           << 8U);
      input_offset += 2U;
      SAN_RETURN_NOT_OK(
          snappy_copy(out, offset, length, static_cast<std::size_t>(expected)));
      break;
    }
    case 3: {
      if (input_offset > payload.size() || payload.size() - input_offset < 4U) {
        return sanitize::Status::Invalid(
            "Parquet page payload: truncated snappy copy-4");
      }
      const auto length = static_cast<std::uint32_t>((tag >> 2U) + 1U);
      const auto offset =
          static_cast<std::uint32_t>(
              static_cast<std::uint8_t>(payload[input_offset])) |
          (static_cast<std::uint32_t>(
               static_cast<std::uint8_t>(payload[input_offset + 1U]))
           << 8U) |
          (static_cast<std::uint32_t>(
               static_cast<std::uint8_t>(payload[input_offset + 2U]))
           << 16U) |
          (static_cast<std::uint32_t>(
               static_cast<std::uint8_t>(payload[input_offset + 3U]))
           << 24U);
      input_offset += 4U;
      SAN_RETURN_NOT_OK(
          snappy_copy(out, offset, length, static_cast<std::size_t>(expected)));
      break;
    }
    }
  }
  if (out->size() != static_cast<std::size_t>(expected)) {
    return sanitize::Status::Invalid(
        "Parquet page payload: snappy decompressed size mismatch");
  }
  return {};
}
