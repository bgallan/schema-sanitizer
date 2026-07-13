void append_snappy_varint(std::string &out, std::uint32_t value) {
  while (value >= 0x80U) {
    out.push_back(static_cast<char>((value & 0x7FU) | 0x80U));
    value >>= 7U;
  }
  out.push_back(static_cast<char>(value));
}

void append_snappy_literal_tag(std::string &out, std::uint32_t length) {
  const auto literal_length = length - 1U;
  if (literal_length < 60U) {
    out.push_back(static_cast<char>(literal_length << 2U));
    return;
  }
  std::uint32_t tmp = literal_length;
  std::uint32_t length_bytes = 0;
  while (tmp != 0) {
    ++length_bytes;
    tmp >>= 8U;
  }
  out.push_back(static_cast<char>((59U + length_bytes) << 2U));
  tmp = literal_length;
  for (std::uint32_t i = 0; i < length_bytes; ++i) {
    out.push_back(static_cast<char>(tmp & 0xFFU));
    tmp >>= 8U;
  }
}

sanitize::Result<std::string>
snappy_encode_literal_payload(std::string_view payload) {
  if (payload.size() >
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet writer: snappy payload is too large");
  }
  std::string out;
  out.reserve(payload.size() + 16U + payload.size() / 65536U * 4U);
  append_snappy_varint(out, static_cast<std::uint32_t>(payload.size()));
  std::size_t offset = 0;
  while (offset < payload.size()) {
    const auto chunk_size = std::min<std::size_t>(
        payload.size() - offset,
        static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max()) +
            1U);
    append_snappy_literal_tag(out, static_cast<std::uint32_t>(chunk_size));
    out.append(payload.data() + offset, chunk_size);
    offset += chunk_size;
  }
  return out;
}

sanitize::Result<std::string> compress_payload(std::string_view payload,
                                               CompressionCodec codec) {
  if (codec == CompressionCodec::kUncompressed) {
    return std::string(payload);
  }
  if (codec == CompressionCodec::kSnappy) {
    return snappy_encode_literal_payload(payload);
  }
#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
  if (codec == CompressionCodec::kGzip) {
    SAN_ASSIGN_OR_RAISE(const int gzip_level, configured_gzip_level());
    z_stream stream{};
    int rc = deflateInit2(&stream, gzip_level, Z_DEFLATED, MAX_WBITS + 16, 8,
                          Z_DEFAULT_STRATEGY);
    if (rc != Z_OK) {
      return sanitize::Status::IOError(
          "native Parquet writer: failed to initialize gzip compression");
    }
    std::string out;
    const auto bound = deflateBound(
        &stream, static_cast<uLong>(std::min<std::size_t>(
                     payload.size(), std::numeric_limits<uLong>::max())));
    out.resize(static_cast<std::size_t>(bound));
    stream.next_in =
        reinterpret_cast<Bytef *>(const_cast<char *>(payload.data()));
    stream.avail_in = static_cast<uInt>(payload.size());
    stream.next_out = reinterpret_cast<Bytef *>(out.data());
    stream.avail_out = static_cast<uInt>(out.size());
    rc = deflate(&stream, Z_FINISH);
    if (rc != Z_STREAM_END) {
      deflateEnd(&stream);
      return sanitize::Status::IOError(
          "native Parquet writer: gzip compression failed");
    }
    out.resize(stream.total_out);
    deflateEnd(&stream);
    return out;
  }
#endif
  return sanitize::Status::Invalid(
      "native Parquet writer: unsupported compression codec");
}
