
constexpr std::string_view kParquetMagic = "PAR1";
constexpr std::int32_t kCompressionUncompressed = 0;
constexpr std::int32_t kCompressionSnappy = 1;
constexpr std::int32_t kCompressionGzip = 2;
constexpr std::int32_t kEncodingPlain = 0;
constexpr std::int32_t kEncodingDeltaBinaryPacked = 5;
constexpr std::int32_t kEncodingDeltaLengthByteArray = 6;
constexpr std::int32_t kEncodingRleDictionary = 8;
constexpr std::int32_t kEncodingByteStreamSplit = 9;
constexpr std::int32_t kPhysicalBoolean = 0;
constexpr std::int32_t kPhysicalInt32 = 1;
constexpr std::int32_t kPhysicalInt64 = 2;
constexpr std::int32_t kPhysicalFloat = 4;
constexpr std::int32_t kPhysicalDouble = 5;
constexpr std::int32_t kPhysicalByteArray = 6;
constexpr std::int32_t kPhysicalFixedLenByteArray = 7;
constexpr std::int32_t kConvertedUtf8 = 0;
constexpr std::int32_t kConvertedDecimal = 5;
constexpr std::int32_t kConvertedDate = 6;
constexpr std::int32_t kConvertedTimeMillis = 7;
constexpr std::int32_t kConvertedUInt8 = 11;
constexpr std::int32_t kConvertedUInt16 = 12;
constexpr std::int32_t kConvertedUInt32 = 13;
constexpr std::int32_t kConvertedUInt64 = 14;
constexpr std::int32_t kConvertedInt8 = 15;
constexpr std::int32_t kConvertedInt16 = 16;
constexpr std::int32_t kConvertedInt32 = 17;
constexpr std::int32_t kConvertedInt64 = 18;
constexpr std::uint8_t kTypeStop = 0;
constexpr std::uint8_t kTypeBoolTrue = 1;
constexpr std::uint8_t kTypeBoolFalse = 2;
constexpr std::uint8_t kTypeByte = 3;
constexpr std::uint8_t kTypeI16 = 4;
constexpr std::uint8_t kTypeI32 = 5;
constexpr std::uint8_t kTypeI64 = 6;
constexpr std::uint8_t kTypeDouble = 7;
constexpr std::uint8_t kTypeBinary = 8;
constexpr std::uint8_t kTypeList = 9;
constexpr std::uint8_t kTypeSet = 10;
constexpr std::uint8_t kTypeMap = 11;
constexpr std::uint8_t kTypeStruct = 12;
constexpr std::uint8_t kTypeUuid = 13;
constexpr int kMaxSkipDepth = 64;
constexpr std::size_t kMaxValuePreviewItems = 8;
constexpr std::size_t kMaxValuePreviewBytes = 128;
constexpr std::size_t kMaxPageHeaderBytes = 1024 * 1024;
constexpr std::size_t kMaxPayloadVerificationBytes = 256ULL * 1024ULL * 1024ULL;
constexpr std::size_t kMaxValidityBitmapBytes = 64ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kMaxContainerElements = 100000000ULL;
constexpr std::int64_t kDefaultNativeReaderMaxBufferBytes =
    1024LL * 1024LL * 1024LL;

std::uint32_t read_u32_le(const char *ptr) {
  return static_cast<std::uint32_t>(static_cast<unsigned char>(ptr[0])) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(ptr[1]))
          << 8U) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(ptr[2]))
          << 16U) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(ptr[3]))
          << 24U);
}

std::int64_t unzigzag(std::uint64_t value) {
  return static_cast<std::int64_t>((value >> 1U) ^ (~(value & 1U) + 1U));
}

std::string preview_bytes(std::string_view value) {
  if (value.size() <= kMaxValuePreviewBytes) {
    return std::string(value);
  }
  std::string out(value.substr(0, kMaxValuePreviewBytes));
  out += "...";
  return out;
}

std::string hex_bytes(std::string_view value) {
  constexpr std::string_view digits = "0123456789abcdef";
  std::string out;
  out.reserve(value.size() * 2U);
  for (const char ch : value) {
    const auto byte = static_cast<std::uint8_t>(ch);
    out.push_back(digits[byte >> 4U]);
    out.push_back(digits[byte & 0x0FU]);
  }
  return out;
}

std::string hex_bytes_preview(const std::vector<std::uint8_t> &value) {
  constexpr std::size_t kMaxHexPreviewBytes = 32;
  const auto size = std::min(value.size(), kMaxHexPreviewBytes);
  std::string_view bytes(reinterpret_cast<const char *>(value.data()), size);
  std::string out = hex_bytes(bytes);
  if (value.size() > kMaxHexPreviewBytes) {
    out += "...";
  }
  return out;
}

template <class T>
T read_plain_value(std::string_view values, std::size_t offset) {
  T out{};
  std::memcpy(&out, values.data() + offset, sizeof(T));
  return out;
}

struct LevelDecodeInfo {
  std::int32_t decoded_count = 0;
  std::int32_t max_level_count = 0;
  std::vector<std::int16_t> level_values;
  std::vector<std::uint8_t> validity_bitmap;
};

struct PlainValueDecodeInfo {
  std::int32_t decoded_bytes = 0;
  std::int32_t materialized_value_bytes = 0;
  std::int32_t materialized_offset_bytes = 0;
  std::vector<std::string> preview;
  std::vector<std::string> byte_array_values;
  std::vector<std::uint8_t> fixed_width_values;
};

struct DictionaryPageState {
  bool decoded = false;
  std::int32_t value_count = 0;
  std::vector<std::string> preview;
  std::vector<std::string> byte_array_values;
  std::vector<std::uint8_t> fixed_width_values;
};

struct NativeReadinessInfo {
  bool ready = true;
  std::vector<std::string> blockers;
};

struct LogicalTypeReadInfo {
  std::string name;
  std::string time_unit;
  bool has_is_adjusted_to_utc = false;
  bool is_adjusted_to_utc = false;
  bool has_integer_bit_width = false;
  std::int32_t integer_bit_width = 0;
  bool has_integer_is_signed = false;
  bool integer_is_signed = true;
  bool has_decimal_scale = false;
  std::int32_t decimal_scale = 0;
  bool has_decimal_precision = false;
  std::int32_t decimal_precision = 0;
};

std::uint8_t level_bit_width(std::int16_t max_level) {
  std::uint8_t width = 0;
  while ((std::int16_t{1} << width) <= max_level) {
    ++width;
  }
  return width;
}

sanitize::Result<std::int32_t> checked_i32_byte_count(std::int64_t bytes,
                                                      std::string_view what) {
  if (bytes < 0 || bytes > std::numeric_limits<std::int32_t>::max()) {
    return sanitize::Status::Invalid("Parquet values: ", what,
                                     " byte count out of range");
  }
  return static_cast<std::int32_t>(bytes);
}

sanitize::Result<std::int32_t>
arrow_i32_offset_buffer_bytes(std::int32_t value_count) {
  if (value_count < 0) {
    return sanitize::Status::Invalid(
        "Parquet values: negative offset value count");
  }
  const auto bytes = (static_cast<std::int64_t>(value_count) + 1LL) *
                     static_cast<std::int64_t>(sizeof(std::int32_t));
  return checked_i32_byte_count(bytes, "offset buffer");
}

sanitize::Result<std::int32_t>
arrow_fixed_width_value_buffer_bytes(std::int32_t row_count, std::int32_t width,
                                     std::string_view what) {
  if (row_count < 0 || width < 0) {
    return sanitize::Status::Invalid("Parquet values: negative ", what,
                                     " buffer size input");
  }
  return checked_i32_byte_count(static_cast<std::int64_t>(row_count) *
                                    static_cast<std::int64_t>(width),
                                what);
}

sanitize::Result<std::int32_t>
arrow_boolean_value_buffer_bytes(std::int32_t row_count) {
  if (row_count < 0) {
    return sanitize::Status::Invalid(
        "Parquet values: negative boolean row count");
  }
  return checked_i32_byte_count((static_cast<std::int64_t>(row_count) + 7LL) /
                                    8LL,
                                "boolean value buffer");
}

// Included by footer_reader.cc inside its implementation namespace.

bool validity_bit_is_set(const std::vector<std::uint8_t> &bitmap,
                         std::int64_t index) {
  if (index < 0) {
    return false;
  }
  const auto byte_index = static_cast<std::size_t>(index / 8);
  if (byte_index >= bitmap.size()) {
    return true;
  }
  const auto mask = static_cast<std::uint8_t>(1U << (index % 8));
  return (bitmap[byte_index] & mask) != 0;
}

void set_output_validity_bit(std::vector<std::uint8_t> *bitmap,
                             std::int64_t index) {
  if (!bitmap || index < 0) {
    return;
  }
  const auto byte_index = static_cast<std::size_t>(index / 8);
  if (byte_index >= bitmap->size()) {
    return;
  }
  (*bitmap)[byte_index] = static_cast<std::uint8_t>(
      (*bitmap)[byte_index] | static_cast<std::uint8_t>(1U << (index % 8)));
}

bool bit_stream_value_is_set(std::string_view values, std::int32_t index) {
  if (index < 0) {
    return false;
  }
  const auto byte_index = static_cast<std::size_t>(index / 8);
  if (byte_index >= values.size()) {
    return false;
  }
  const auto mask = static_cast<std::uint8_t>(1U << (index % 8));
  return (static_cast<std::uint8_t>(values[byte_index]) & mask) != 0;
}
