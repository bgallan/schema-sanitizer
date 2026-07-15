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

void append_snappy_literal(std::string &out, std::string_view payload,
                           std::size_t offset, std::size_t length) {
  while (length != 0) {
    const auto chunk_size = std::min<std::size_t>(
        length,
        static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max()) +
            1U);
    append_snappy_literal_tag(out, static_cast<std::uint32_t>(chunk_size));
    out.append(payload.data() + offset, chunk_size);
    offset += chunk_size;
    length -= chunk_size;
  }
}

void append_snappy_copy(std::string &out, std::uint32_t offset,
                        std::size_t length) {
  while (length != 0) {
    const auto chunk_size = std::min<std::size_t>(length, 64U);
    out.push_back(static_cast<char>(
        ((static_cast<std::uint32_t>(chunk_size) - 1U) << 2U) | 0x02U));
    out.push_back(static_cast<char>(offset & 0xFFU));
    out.push_back(static_cast<char>((offset >> 8U) & 0xFFU));
    length -= chunk_size;
  }
}

std::uint32_t load_snappy_word(std::string_view payload, std::size_t offset) {
  std::uint32_t word = 0;
  std::memcpy(&word, payload.data() + offset, sizeof(word));
  return word;
}

std::size_t snappy_hash_table_size(std::size_t payload_size) {
  std::size_t size = 256U;
  while (size < 16384U && size < payload_size) {
    size <<= 1U;
  }
  return size;
}

std::size_t snappy_hash(std::uint32_t word, std::size_t table_size) {
  const auto shift = static_cast<unsigned>(32U - std::countr_zero(table_size));
  return static_cast<std::size_t>((word * 0x1E35A7BDU) >> shift);
}

sanitize::Result<std::string> snappy_encode_payload(std::string_view payload) {
  if (payload.size() >
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet writer: snappy payload is too large");
  }

  std::string out;
  constexpr std::size_t kInitialReserve = 256U * 1024U;
  out.reserve(std::min(payload.size() + 32U, kInitialReserve));
  append_snappy_varint(out, static_cast<std::uint32_t>(payload.size()));
  if (payload.empty()) {
    return out;
  }
  if (payload.size() < sizeof(std::uint32_t)) {
    append_snappy_literal(out, payload, 0, payload.size());
    return out;
  }

  constexpr auto kNoCandidate = std::numeric_limits<std::uint32_t>::max();
  const auto table_size = snappy_hash_table_size(payload.size());
  std::vector<std::uint32_t> table(table_size, kNoCandidate);
  std::size_t next_emit = 0;
  std::size_t cursor = 0;
  const auto last_match_start = payload.size() - sizeof(std::uint32_t);

  while (cursor <= last_match_start) {
    const auto word = load_snappy_word(payload, cursor);
    const auto hash = snappy_hash(word, table_size);
    const auto candidate = table[hash];
    table[hash] = static_cast<std::uint32_t>(cursor);

    if (candidate == kNoCandidate || cursor <= candidate ||
        cursor - candidate > std::numeric_limits<std::uint16_t>::max() ||
        load_snappy_word(payload, candidate) != word) {
      ++cursor;
      continue;
    }

    append_snappy_literal(out, payload, next_emit, cursor - next_emit);
    std::size_t match_length = sizeof(std::uint32_t);
    while (cursor + match_length < payload.size() &&
           payload[candidate + match_length] ==
               payload[cursor + match_length]) {
      ++match_length;
    }
    append_snappy_copy(out, static_cast<std::uint32_t>(cursor - candidate),
                       match_length);
    cursor += match_length;
    next_emit = cursor;

    if (cursor <= last_match_start && cursor != 0) {
      const auto previous = cursor - 1U;
      table[snappy_hash(load_snappy_word(payload, previous), table_size)] =
          static_cast<std::uint32_t>(previous);
    }
  }

  append_snappy_literal(out, payload, next_emit, payload.size() - next_emit);
  return out;
}

sanitize::Result<std::string> compress_payload(std::string_view payload,
                                               CompressionCodec codec,
                                               int gzip_level) {
  if (codec == CompressionCodec::kUncompressed) {
    return std::string(payload);
  }
  if (codec == CompressionCodec::kSnappy) {
    return snappy_encode_payload(payload);
  }
#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
  if (codec == CompressionCodec::kGzip) {
    z_stream stream{};
    int rc = deflateInit2(&stream, gzip_level, Z_DEFLATED, MAX_WBITS + 16, 8,
                          Z_DEFAULT_STRATEGY);
    if (rc != Z_OK) {
      return sanitize::Status::IOError(
          "native Parquet writer: failed to initialize gzip compression");
    }
    // Stream compression into a bounded scratch block. deflateBound()
    // reserved roughly the entire input size even for highly compressible
    // pages, inflating peak RSS while alternate encodings were evaluated.
    constexpr std::size_t kInputChunkBytes = 1U << 20;
    constexpr std::size_t kOutputChunkBytes = 64U << 10;
    std::array<unsigned char, kOutputChunkBytes> output_chunk{};
    std::string out;
    out.reserve(std::min<std::size_t>(payload.size() / 4U + 64U,
                                      kInputChunkBytes));
    std::size_t input_offset = 0;
    do {
      if (stream.avail_in == 0 && input_offset < payload.size()) {
        const auto input_size = std::min<std::size_t>(
            payload.size() - input_offset, kInputChunkBytes);
        stream.next_in = reinterpret_cast<Bytef *>(
            const_cast<char *>(payload.data() + input_offset));
        stream.avail_in = static_cast<uInt>(input_size);
        input_offset += input_size;
      }
      const int flush =
          input_offset == payload.size() ? Z_FINISH : Z_NO_FLUSH;
      do {
        stream.next_out = output_chunk.data();
        stream.avail_out = static_cast<uInt>(output_chunk.size());
        rc = deflate(&stream, flush);
        if (rc != Z_OK && rc != Z_STREAM_END) {
          deflateEnd(&stream);
          return sanitize::Status::IOError(
              "native Parquet writer: gzip compression failed");
        }
        const auto produced = output_chunk.size() - stream.avail_out;
        out.append(reinterpret_cast<const char *>(output_chunk.data()),
                   produced);
      } while (stream.avail_out == 0);
    } while (rc != Z_STREAM_END);
    deflateEnd(&stream);
    return out;
  }
#endif
  return sanitize::Status::Invalid(
      "native Parquet writer: unsupported compression codec");
}
