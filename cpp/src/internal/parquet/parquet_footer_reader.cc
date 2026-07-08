// Implements bounded Parquet footer parsing for native reader dispatch.

#include "internal/parquet/parquet_footer_reader.hh"

#include "internal/json/json_write.hh"
#include "internal/pipeline/cdata_stream_utils.hh"

#include "nanoarrow/nanoarrow.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <new>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
#include <zlib.h>
#endif

namespace sanitize::internal::parquet_footer_reader {
namespace {

constexpr std::string_view kParquetMagic = "PAR1";
constexpr std::int32_t kCompressionUncompressed = 0;
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

sanitize::Result<std::int64_t> read_zigzag_varint_from(std::string_view data,
                                                       std::size_t *offset) {
  std::uint64_t raw = 0;
  SAN_ASSIGN_OR_RAISE(raw, read_varint_from(data, offset));
  return unzigzag(raw);
}

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

sanitize::Result<LevelDecodeInfo>
decode_level_stream(std::string_view payload, std::size_t *offset,
                    std::int16_t max_level, std::int32_t expected_count,
                    bool capture_validity_bitmap = false,
                    bool capture_level_values = false) {
  if (!offset) {
    return sanitize::Status::Invalid("Parquet levels: internal offset error");
  }
  if (expected_count < 0) {
    return sanitize::Status::Invalid("Parquet levels: negative expected count");
  }
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

class CompactReader {
public:
  explicit CompactReader(std::string_view data) : data_(data) {}

  sanitize::Result<std::uint8_t> read_byte() {
    if (pos_ >= data_.size()) {
      return sanitize::Status::Invalid(
          "Parquet footer: unexpected end of data");
    }
    return static_cast<std::uint8_t>(data_[pos_++]);
  }

  sanitize::Result<std::uint64_t> read_varint() {
    std::uint64_t out = 0;
    int shift = 0;
    for (int i = 0; i < 10; ++i) {
      std::uint8_t byte = 0;
      SAN_ASSIGN_OR_RAISE(byte, read_byte());
      out |= static_cast<std::uint64_t>(byte & 0x7FU) << shift;
      if ((byte & 0x80U) == 0) {
        return out;
      }
      shift += 7;
    }
    return sanitize::Status::Invalid("Parquet footer: invalid varint");
  }

  sanitize::Result<std::int32_t> read_i32() {
    std::uint64_t raw = 0;
    SAN_ASSIGN_OR_RAISE(raw, read_varint());
    const auto value = unzigzag(raw);
    if (value < std::numeric_limits<std::int32_t>::min() ||
        value > std::numeric_limits<std::int32_t>::max()) {
      return sanitize::Status::Invalid("Parquet footer: i32 out of range");
    }
    return static_cast<std::int32_t>(value);
  }

  sanitize::Result<std::int16_t> read_i16() {
    std::int32_t value = 0;
    SAN_ASSIGN_OR_RAISE(value, read_i32());
    if (value < std::numeric_limits<std::int16_t>::min() ||
        value > std::numeric_limits<std::int16_t>::max()) {
      return sanitize::Status::Invalid("Parquet footer: i16 out of range");
    }
    return static_cast<std::int16_t>(value);
  }

  sanitize::Result<std::int64_t> read_i64() {
    std::uint64_t raw = 0;
    SAN_ASSIGN_OR_RAISE(raw, read_varint());
    return unzigzag(raw);
  }

  sanitize::Result<std::string> read_binary() {
    std::uint64_t size = 0;
    SAN_ASSIGN_OR_RAISE(size, read_varint());
    if (size > data_.size() - pos_) {
      return sanitize::Status::Invalid(
          "Parquet footer: binary value exceeds footer");
    }
    std::string out(data_.data() + pos_, static_cast<std::size_t>(size));
    pos_ += static_cast<std::size_t>(size);
    return out;
  }

  sanitize::Status skip_type(std::uint8_t type, int depth) {
    if (depth > kMaxSkipDepth) {
      return sanitize::Status::Invalid(
          "Parquet footer: nested metadata too deep");
    }
    switch (type) {
    case kTypeStop:
    case kTypeBoolTrue:
    case kTypeBoolFalse:
      return {};
    case kTypeByte: {
      std::uint8_t ignored = 0;
      SAN_ASSIGN_OR_RAISE(ignored, read_byte());
      (void)ignored;
      return {};
    }
    case kTypeI16: {
      std::int16_t ignored = 0;
      SAN_ASSIGN_OR_RAISE(ignored, read_i16());
      (void)ignored;
      return {};
    }
    case kTypeI32: {
      std::int32_t ignored = 0;
      SAN_ASSIGN_OR_RAISE(ignored, read_i32());
      (void)ignored;
      return {};
    }
    case kTypeI64: {
      std::int64_t ignored = 0;
      SAN_ASSIGN_OR_RAISE(ignored, read_i64());
      (void)ignored;
      return {};
    }
    case kTypeDouble: {
      if (data_.size() - pos_ < 8) {
        return sanitize::Status::Invalid("Parquet footer: truncated double");
      }
      pos_ += 8;
      return {};
    }
    case kTypeBinary: {
      std::string ignored;
      SAN_ASSIGN_OR_RAISE(ignored, read_binary());
      return {};
    }
    case kTypeList:
    case kTypeSet:
      return skip_list(depth + 1);
    case kTypeMap:
      return skip_map(depth + 1);
    case kTypeStruct:
      return skip_struct(depth + 1);
    case kTypeUuid: {
      if (data_.size() - pos_ < 16) {
        return sanitize::Status::Invalid("Parquet footer: truncated uuid");
      }
      pos_ += 16;
      return {};
    }
    default:
      return sanitize::Status::Invalid(
          "Parquet footer: unsupported compact type ", static_cast<int>(type));
    }
  }

  sanitize::Status skip_container_value(std::uint8_t type, int depth) {
    if (type == kTypeBoolTrue || type == kTypeBoolFalse) {
      std::uint8_t ignored = 0;
      SAN_ASSIGN_OR_RAISE(ignored, read_byte());
      (void)ignored;
      return {};
    }
    return skip_type(type, depth);
  }

  sanitize::Status skip_struct(int depth) {
    std::int16_t last_field_id = 0;
    while (true) {
      std::uint8_t header = 0;
      SAN_ASSIGN_OR_RAISE(header, read_byte());
      const auto type = static_cast<std::uint8_t>(header & 0x0FU);
      if (type == kTypeStop) {
        return {};
      }
      const auto delta = static_cast<std::uint8_t>(header >> 4U);
      if (delta == 0) {
        SAN_ASSIGN_OR_RAISE(last_field_id, read_i16());
      } else {
        last_field_id = static_cast<std::int16_t>(last_field_id + delta);
      }
      SAN_RETURN_NOT_OK(skip_type(type, depth));
    }
  }

  sanitize::Status skip_list(int depth) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, read_byte());
    auto size = static_cast<std::uint64_t>(header >> 4U);
    const auto element_type = static_cast<std::uint8_t>(header & 0x0FU);
    if (size == 15) {
      SAN_ASSIGN_OR_RAISE(size, read_varint());
    }
    if (size > kMaxContainerElements) {
      return sanitize::Status::Invalid("Parquet footer: container too large");
    }
    for (std::uint64_t i = 0; i < size; ++i) {
      SAN_RETURN_NOT_OK(skip_container_value(element_type, depth));
    }
    return {};
  }

  sanitize::Status skip_map(int depth) {
    std::uint64_t size = 0;
    SAN_ASSIGN_OR_RAISE(size, read_varint());
    if (size == 0) {
      return {};
    }
    if (size > kMaxContainerElements) {
      return sanitize::Status::Invalid("Parquet footer: map too large");
    }
    std::uint8_t types = 0;
    SAN_ASSIGN_OR_RAISE(types, read_byte());
    const auto key_type = static_cast<std::uint8_t>((types >> 4U) & 0x0FU);
    const auto value_type = static_cast<std::uint8_t>(types & 0x0FU);
    for (std::uint64_t i = 0; i < size; ++i) {
      SAN_RETURN_NOT_OK(skip_container_value(key_type, depth));
      SAN_RETURN_NOT_OK(skip_container_value(value_type, depth));
    }
    return {};
  }

  sanitize::Status read_list_header(std::uint8_t *element_type,
                                    std::uint64_t *size) {
    if (!element_type || !size) {
      return sanitize::Status::Invalid("Parquet footer: internal list error");
    }
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, read_byte());
    *size = static_cast<std::uint64_t>(header >> 4U);
    *element_type = static_cast<std::uint8_t>(header & 0x0FU);
    if (*size == 15) {
      SAN_ASSIGN_OR_RAISE(*size, read_varint());
    }
    if (*size > kMaxContainerElements) {
      return sanitize::Status::Invalid("Parquet footer: container too large");
    }
    return {};
  }

  [[nodiscard]] std::size_t position() const noexcept { return pos_; }

private:
  std::string_view data_;
  std::size_t pos_ = 0;
};

std::string logical_type_name(std::int16_t field_id) {
  switch (field_id) {
  case 1:
    return "string";
  case 2:
    return "map";
  case 3:
    return "list";
  case 4:
    return "enum";
  case 5:
    return "decimal";
  case 6:
    return "date";
  case 7:
    return "time";
  case 8:
    return "timestamp";
  case 10:
    return "integer";
  case 11:
    return "null";
  case 12:
    return "json";
  case 13:
    return "bson";
  case 14:
    return "uuid";
  case 15:
    return "float16";
  default:
    return "unknown";
  }
}

sanitize::Result<std::string> read_time_unit(CompactReader &reader) {
  std::int16_t last_field_id = 0;
  std::string out;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return out;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    if (out.empty()) {
      switch (field_id) {
      case 1:
        out = "millis";
        break;
      case 2:
        out = "micros";
        break;
      case 3:
        out = "nanos";
        break;
      default:
        out = "unknown";
        break;
      }
    }
    SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
  }
}

sanitize::Result<LogicalTypeReadInfo>
read_decimal_logical_type(CompactReader &reader, LogicalTypeReadInfo out) {
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return out;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    if (field_id == 1 && type == kTypeI32) {
      SAN_ASSIGN_OR_RAISE(out.decimal_scale, reader.read_i32());
      out.has_decimal_scale = true;
    } else if (field_id == 2 && type == kTypeI32) {
      SAN_ASSIGN_OR_RAISE(out.decimal_precision, reader.read_i32());
      out.has_decimal_precision = true;
    } else {
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
    }
  }
}

sanitize::Result<LogicalTypeReadInfo>
read_temporal_logical_type(CompactReader &reader, LogicalTypeReadInfo out) {
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return out;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    if (field_id == 1 && (type == kTypeBoolTrue || type == kTypeBoolFalse)) {
      out.has_is_adjusted_to_utc = true;
      out.is_adjusted_to_utc = type == kTypeBoolTrue;
    } else if (field_id == 2 && type == kTypeStruct) {
      SAN_ASSIGN_OR_RAISE(out.time_unit, read_time_unit(reader));
    } else {
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
    }
  }
}

sanitize::Result<LogicalTypeReadInfo>
read_integer_logical_type(CompactReader &reader, LogicalTypeReadInfo out) {
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return out;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    if (field_id == 1 && type == kTypeByte) {
      std::uint8_t width = 0;
      SAN_ASSIGN_OR_RAISE(width, reader.read_byte());
      out.integer_bit_width = static_cast<std::int32_t>(width);
      out.has_integer_bit_width = true;
    } else if (field_id == 2 &&
               (type == kTypeBoolTrue || type == kTypeBoolFalse)) {
      out.has_integer_is_signed = true;
      out.integer_is_signed = type == kTypeBoolTrue;
    } else {
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
    }
  }
}

sanitize::Result<LogicalTypeReadInfo> read_logical_type(CompactReader &reader) {
  std::int16_t last_field_id = 0;
  LogicalTypeReadInfo out;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return out;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    if (out.name.empty()) {
      out.name = logical_type_name(field_id);
    }
    if (field_id == 5 && type == kTypeStruct) {
      SAN_ASSIGN_OR_RAISE(out, read_decimal_logical_type(reader, out));
    } else if ((field_id == 7 || field_id == 8) && type == kTypeStruct) {
      SAN_ASSIGN_OR_RAISE(out, read_temporal_logical_type(reader, out));
    } else if (field_id == 10 && type == kTypeStruct) {
      SAN_ASSIGN_OR_RAISE(out, read_integer_logical_type(reader, out));
    } else {
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
    }
  }
}

sanitize::Result<SchemaElementInfo> read_schema_element(CompactReader &reader) {
  SchemaElementInfo out;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return out;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;

    switch (field_id) {
    case 1:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.physical_type, reader.read_i32());
        out.has_physical_type = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.type_length, reader.read_i32());
        out.has_type_length = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.repetition_type, reader.read_i32());
        out.has_repetition_type = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeBinary) {
        SAN_ASSIGN_OR_RAISE(out.name, reader.read_binary());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 5:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.num_children, reader.read_i32());
        out.has_num_children = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 6:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.converted_type, reader.read_i32());
        out.has_converted_type = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 7:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.decimal_scale, reader.read_i32());
        out.has_decimal_scale = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 8:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.decimal_precision, reader.read_i32());
        out.has_decimal_precision = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 10:
      if (type == kTypeStruct) {
        LogicalTypeReadInfo logical;
        SAN_ASSIGN_OR_RAISE(logical, read_logical_type(reader));
        out.logical_type = std::move(logical.name);
        out.logical_type_time_unit = std::move(logical.time_unit);
        out.has_logical_type_is_adjusted_to_utc =
            logical.has_is_adjusted_to_utc;
        out.logical_type_is_adjusted_to_utc = logical.is_adjusted_to_utc;
        out.has_logical_type_integer_bit_width = logical.has_integer_bit_width;
        out.logical_type_integer_bit_width = logical.integer_bit_width;
        out.has_logical_type_integer_is_signed = logical.has_integer_is_signed;
        out.logical_type_integer_is_signed = logical.integer_is_signed;
        if (logical.has_decimal_scale) {
          out.decimal_scale = logical.decimal_scale;
          out.has_decimal_scale = true;
        }
        if (logical.has_decimal_precision) {
          out.decimal_precision = logical.decimal_precision;
          out.has_decimal_precision = true;
        }
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    default:
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      break;
    }
  }
}

sanitize::Status read_schema_elements(CompactReader &reader, FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid("Parquet footer: internal schema error");
  }
  std::uint8_t element_type = 0;
  std::uint64_t count = 0;
  SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
  if (count >
      static_cast<std::uint64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet footer: schema element count out of range");
  }
  info->schema_element_count = static_cast<std::int32_t>(count);
  info->schema_elements.clear();
  info->schema_elements.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t i = 0; i < count; ++i) {
    if (element_type != kTypeStruct) {
      SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
      continue;
    }
    SchemaElementInfo element;
    SAN_ASSIGN_OR_RAISE(element, read_schema_element(reader));
    info->schema_elements.push_back(std::move(element));
  }
  return {};
}

sanitize::Result<std::vector<std::int32_t>>
read_i32_list(CompactReader &reader) {
  std::uint8_t element_type = 0;
  std::uint64_t count = 0;
  SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
  std::vector<std::int32_t> out;
  out.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t i = 0; i < count; ++i) {
    if (element_type != kTypeI32) {
      SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
      continue;
    }
    std::int32_t value = 0;
    SAN_ASSIGN_OR_RAISE(value, reader.read_i32());
    out.push_back(value);
  }
  return out;
}

sanitize::Result<std::vector<std::int64_t>>
read_i64_list(CompactReader &reader) {
  std::uint8_t element_type = 0;
  std::uint64_t count = 0;
  SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
  std::vector<std::int64_t> out;
  out.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t i = 0; i < count; ++i) {
    if (element_type != kTypeI64) {
      SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
      continue;
    }
    std::int64_t value = 0;
    SAN_ASSIGN_OR_RAISE(value, reader.read_i64());
    out.push_back(value);
  }
  return out;
}

sanitize::Result<std::vector<bool>> read_bool_list(CompactReader &reader) {
  std::uint8_t element_type = 0;
  std::uint64_t count = 0;
  SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
  std::vector<bool> out;
  out.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t i = 0; i < count; ++i) {
    if (element_type != kTypeBoolTrue && element_type != kTypeBoolFalse) {
      SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
      continue;
    }
    std::uint8_t value = 0;
    SAN_ASSIGN_OR_RAISE(value, reader.read_byte());
    if (value == kTypeBoolTrue) {
      out.push_back(true);
    } else if (value == kTypeBoolFalse) {
      out.push_back(false);
    } else {
      return sanitize::Status::Invalid(
          "Parquet index: invalid bool list value");
    }
  }
  return out;
}

sanitize::Result<std::vector<std::string>>
read_string_list(CompactReader &reader) {
  std::uint8_t element_type = 0;
  std::uint64_t count = 0;
  SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
  std::vector<std::string> out;
  out.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t i = 0; i < count; ++i) {
    if (element_type != kTypeBinary) {
      SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
      continue;
    }
    std::string value;
    SAN_ASSIGN_OR_RAISE(value, reader.read_binary());
    out.push_back(std::move(value));
  }
  return out;
}

sanitize::Result<ColumnChunkInfo> read_column_metadata(CompactReader &reader,
                                                       ColumnChunkInfo column) {
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return column;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;

    switch (field_id) {
    case 1:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(column.physical_type, reader.read_i32());
        column.has_physical_type = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeList) {
        SAN_ASSIGN_OR_RAISE(column.encodings, read_i32_list(reader));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeList) {
        SAN_ASSIGN_OR_RAISE(column.path_in_schema, read_string_list(reader));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(column.codec, reader.read_i32());
        column.has_codec = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 5:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.num_values, reader.read_i64());
        column.has_num_values = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 6:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.total_uncompressed_size, reader.read_i64());
        column.has_total_uncompressed_size = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 7:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.total_compressed_size, reader.read_i64());
        column.has_total_compressed_size = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 9:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.data_page_offset, reader.read_i64());
        column.has_data_page_offset = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 11:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.dictionary_page_offset, reader.read_i64());
        column.has_dictionary_page_offset = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    default:
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      break;
    }
  }
}

sanitize::Result<ColumnChunkInfo> read_column_chunk(CompactReader &reader) {
  ColumnChunkInfo column;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return column;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;

    switch (field_id) {
    case 2:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.file_offset, reader.read_i64());
        column.has_file_offset = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeStruct) {
        SAN_ASSIGN_OR_RAISE(column, read_column_metadata(reader, column));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.offset_index_offset, reader.read_i64());
        column.has_offset_index_offset = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 5:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(column.offset_index_length, reader.read_i32());
        column.has_offset_index_length = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 6:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.column_index_offset, reader.read_i64());
        column.has_column_index_offset = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 7:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(column.column_index_length, reader.read_i32());
        column.has_column_index_length = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    default:
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      break;
    }
  }
}

sanitize::Result<RowGroupInfo> read_row_group(CompactReader &reader) {
  RowGroupInfo row_group;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return row_group;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;

    switch (field_id) {
    case 1:
      if (type == kTypeList) {
        std::uint8_t element_type = 0;
        std::uint64_t count = 0;
        SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
        row_group.columns.clear();
        row_group.columns.reserve(static_cast<std::size_t>(count));
        for (std::uint64_t i = 0; i < count; ++i) {
          if (element_type != kTypeStruct) {
            SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
            continue;
          }
          ColumnChunkInfo column;
          SAN_ASSIGN_OR_RAISE(column, read_column_chunk(reader));
          row_group.columns.push_back(std::move(column));
        }
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(row_group.total_byte_size, reader.read_i64());
        row_group.has_total_byte_size = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(row_group.num_rows, reader.read_i64());
        row_group.has_num_rows = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    default:
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      break;
    }
  }
}

sanitize::Status read_row_groups(CompactReader &reader, FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid(
        "Parquet footer: internal row group error");
  }
  std::uint8_t element_type = 0;
  std::uint64_t count = 0;
  SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
  if (count >
      static_cast<std::uint64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet footer: row group count out of range");
  }
  info->row_group_count = static_cast<std::int32_t>(count);
  info->row_groups.clear();
  info->row_groups.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t i = 0; i < count; ++i) {
    if (element_type != kTypeStruct) {
      SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
      continue;
    }
    RowGroupInfo row_group;
    SAN_ASSIGN_OR_RAISE(row_group, read_row_group(reader));
    info->row_groups.push_back(std::move(row_group));
  }
  return {};
}

std::string arrow_integer_format(std::int32_t bit_width, bool is_signed) {
  switch (bit_width) {
  case 8:
    return is_signed ? "c" : "C";
  case 16:
    return is_signed ? "s" : "S";
  case 32:
    return is_signed ? "i" : "I";
  case 64:
    return is_signed ? "l" : "L";
  default:
    return {};
  }
}

std::string arrow_integer_format_from_converted_type(std::int32_t converted) {
  switch (converted) {
  case kConvertedUInt8:
    return "C";
  case kConvertedUInt16:
    return "S";
  case kConvertedUInt32:
    return "I";
  case kConvertedUInt64:
    return "L";
  case kConvertedInt8:
    return "c";
  case kConvertedInt16:
    return "s";
  case kConvertedInt32:
    return "i";
  case kConvertedInt64:
    return "l";
  default:
    return {};
  }
}

std::string arrow_temporal_format(std::string_view logical_type,
                                  std::string_view unit,
                                  std::int32_t physical_type,
                                  bool is_adjusted_to_utc) {
  if (logical_type == "date") {
    return "tdD";
  }
  if (logical_type == "time") {
    if (unit == "millis") {
      return "ttm";
    }
    if (unit == "micros") {
      return "ttu";
    }
    if (unit == "nanos") {
      return "ttn";
    }
    if (physical_type == kPhysicalInt32) {
      return "ttm";
    }
    return {};
  }
  if (logical_type == "timestamp") {
    const auto timezone = is_adjusted_to_utc ? "UTC" : "";
    if (unit == "millis") {
      return std::string("tsm:") + timezone;
    }
    if (unit == "micros") {
      return std::string("tsu:") + timezone;
    }
    if (unit == "nanos") {
      return std::string("tsn:") + timezone;
    }
    return {};
  }
  return {};
}

std::string arrow_format_for_leaf(const SchemaElementInfo &element) {
  if (!element.has_physical_type) {
    return {};
  }
  if (element.logical_type == "null") {
    return "n";
  }
  if (element.logical_type == "string" ||
      (element.has_converted_type &&
       element.converted_type == kConvertedUtf8)) {
    return "u";
  }
  if (element.logical_type == "date" || element.logical_type == "time" ||
      element.logical_type == "timestamp") {
    return arrow_temporal_format(
        element.logical_type, element.logical_type_time_unit,
        element.physical_type, element.logical_type_is_adjusted_to_utc);
  }
  if (element.logical_type == "integer" &&
      element.has_logical_type_integer_bit_width &&
      element.has_logical_type_integer_is_signed) {
    return arrow_integer_format(element.logical_type_integer_bit_width,
                                element.logical_type_integer_is_signed);
  }
  if (element.has_converted_type) {
    const auto integer_format =
        arrow_integer_format_from_converted_type(element.converted_type);
    if (!integer_format.empty()) {
      return integer_format;
    }
    if (element.converted_type == kConvertedDate) {
      return "tdD";
    }
    if (element.converted_type == kConvertedTimeMillis) {
      return "ttm";
    }
  }
  if ((element.logical_type == "decimal" ||
       (element.has_converted_type &&
        element.converted_type == kConvertedDecimal)) &&
      element.has_decimal_precision && element.has_decimal_scale &&
      element.has_type_length && element.type_length > 0) {
    return "d:" + std::to_string(element.decimal_precision) + "," +
           std::to_string(element.decimal_scale) + "," +
           std::to_string(element.type_length * 8);
  }
  switch (element.physical_type) {
  case kPhysicalBoolean:
    return "b";
  case kPhysicalInt32:
    return "i";
  case kPhysicalInt64:
    return "l";
  case kPhysicalFloat:
    return "f";
  case kPhysicalDouble:
    return "g";
  case kPhysicalByteArray:
    return "z";
  case kPhysicalFixedLenByteArray:
    if (element.has_type_length && element.type_length > 0) {
      return "w:" + std::to_string(element.type_length);
    }
    return {};
  default:
    return {};
  }
}

struct LeafLevelInfo {
  std::vector<std::string> path;
  std::int16_t max_definition_level = 0;
  std::int16_t max_repetition_level = 0;
  bool top_level_required = true;
  std::int32_t fixed_type_length = 0;
  std::string native_arrow_format;
};

sanitize::Status
collect_leaf_levels(const std::vector<SchemaElementInfo> &schema,
                    std::size_t *index, std::vector<std::string> path,
                    std::int16_t definition_level,
                    std::int16_t repetition_level, bool is_root,
                    bool top_level_required, std::vector<LeafLevelInfo> *out) {
  if (!index || !out || *index >= schema.size()) {
    return sanitize::Status::Invalid("Parquet schema levels: invalid schema");
  }
  const auto &element = schema[(*index)++];
  const auto repetition =
      element.has_repetition_type ? element.repetition_type : 0;
  std::int16_t next_definition_level = definition_level;
  std::int16_t next_repetition_level = repetition_level;
  if (!is_root) {
    if (repetition == 1 || repetition == 2) {
      ++next_definition_level;
    }
    if (repetition == 2) {
      ++next_repetition_level;
    }
    path.push_back(element.name);
    if (path.size() == 1) {
      top_level_required = repetition == 0;
    }
  }

  const auto child_count = element.has_num_children
                               ? std::max<std::int32_t>(element.num_children, 0)
                               : 0;
  if (child_count == 0) {
    if (element.has_physical_type) {
      out->push_back(LeafLevelInfo{
          .path = std::move(path),
          .max_definition_level = next_definition_level,
          .max_repetition_level = next_repetition_level,
          .top_level_required = top_level_required,
          .fixed_type_length =
              element.has_type_length ? element.type_length : 0,
          .native_arrow_format = arrow_format_for_leaf(element),
      });
    }
    return {};
  }
  for (std::int32_t i = 0; i < child_count; ++i) {
    SAN_RETURN_NOT_OK(collect_leaf_levels(
        schema, index, path, next_definition_level, next_repetition_level,
        false, top_level_required, out));
  }
  return {};
}

sanitize::Result<std::vector<LeafLevelInfo>>
schema_leaf_levels(const std::vector<SchemaElementInfo> &schema) {
  std::vector<LeafLevelInfo> out;
  if (schema.empty()) {
    return out;
  }
  std::size_t index = 0;
  std::vector<std::string> path;
  SAN_RETURN_NOT_OK(collect_leaf_levels(schema, &index, std::move(path), 0, 0,
                                        true, true, &out));
  return out;
}

sanitize::Status
project_leaf_levels_for_columns(const std::vector<LeafLevelInfo> &leaves,
                                const std::vector<std::string> &columns,
                                std::vector<LeafLevelInfo> *out) {
  if (!out) {
    return sanitize::Status::Invalid(
        "native Parquet reader: projection output is null");
  }
  out->clear();
  if (columns.empty()) {
    *out = leaves;
    return {};
  }
  out->reserve(leaves.size());
  for (const auto &name : columns) {
    const auto before_count = out->size();
    for (const auto &leaf : leaves) {
      if (!leaf.path.empty() && leaf.path.front() == name) {
        out->push_back(leaf);
      }
    }
    if (out->size() == before_count) {
      return sanitize::Status::Invalid(
          "native Parquet reader: projection column not found: ", name);
    }
  }
  return {};
}

sanitize::Status assign_column_levels(FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid("Parquet schema levels: internal error");
  }
  std::vector<LeafLevelInfo> leaves;
  SAN_ASSIGN_OR_RAISE(leaves, schema_leaf_levels(info->schema_elements));
  for (auto &row_group : info->row_groups) {
    for (auto &column : row_group.columns) {
      auto match = std::find_if(leaves.begin(), leaves.end(),
                                [&](const LeafLevelInfo &leaf) {
                                  return leaf.path == column.path_in_schema;
                                });
      if (match == leaves.end()) {
        continue;
      }
      column.max_definition_level = match->max_definition_level;
      column.max_repetition_level = match->max_repetition_level;
      column.top_level_required = match->top_level_required;
      column.fixed_type_length = match->fixed_type_length;
      column.native_arrow_format = match->native_arrow_format;
    }
  }
  return {};
}

sanitize::Result<PageHeaderInfo> read_data_page_header(CompactReader &reader,
                                                       PageHeaderInfo page) {
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return page;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    switch (field_id) {
    case 1:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.num_values, reader.read_i32());
        page.has_num_values = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.value_encoding, reader.read_i32());
        page.has_value_encoding = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.definition_level_encoding, reader.read_i32());
        page.has_definition_level_encoding = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.repetition_level_encoding, reader.read_i32());
        page.has_repetition_level_encoding = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    default:
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      break;
    }
  }
}

sanitize::Result<PageHeaderInfo>
read_dictionary_page_header(CompactReader &reader, PageHeaderInfo page) {
  page.is_dictionary_page = true;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return page;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    switch (field_id) {
    case 1:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.num_values, reader.read_i32());
        page.has_num_values = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.value_encoding, reader.read_i32());
        page.has_value_encoding = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeBoolTrue || type == kTypeBoolFalse) {
        page.dictionary_is_sorted = type == kTypeBoolTrue;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    default:
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      break;
    }
  }
}

sanitize::Result<PageHeaderInfo> read_page_header(std::string_view bytes) {
  CompactReader reader(bytes);
  PageHeaderInfo page;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      if (!page.has_compressed_page_size) {
        return sanitize::Status::Invalid(
            "Parquet page header: missing compressed page size");
      }
      if (page.compressed_page_size < 0) {
        return sanitize::Status::Invalid(
            "Parquet page header: negative compressed page size");
      }
      if (reader.position() >
          static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
        return sanitize::Status::Invalid(
            "Parquet page header: header is too large");
      }
      page.header_size = static_cast<std::int32_t>(reader.position());
      return page;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;

    switch (field_id) {
    case 1:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.type, reader.read_i32());
        page.has_type = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.uncompressed_page_size, reader.read_i32());
        page.has_uncompressed_page_size = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.compressed_page_size, reader.read_i32());
        page.has_compressed_page_size = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 5:
      if (type == kTypeStruct) {
        SAN_ASSIGN_OR_RAISE(page, read_data_page_header(reader, page));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 7:
      if (type == kTypeStruct) {
        SAN_ASSIGN_OR_RAISE(page, read_dictionary_page_header(reader, page));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    default:
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      break;
    }
  }
}

sanitize::Result<PageHeaderInfo> read_page_header_at(std::ifstream &file,
                                                     std::uint64_t offset,
                                                     std::uint64_t limit) {
  if (offset >= limit) {
    return sanitize::Status::Invalid("Parquet page header: invalid offset");
  }
  const auto available = limit - offset;
  const auto window_size = static_cast<std::size_t>(std::min<std::uint64_t>(
      available, static_cast<std::uint64_t>(kMaxPageHeaderBytes)));
  std::string bytes(window_size, '\0');
  file.clear();
  file.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  file.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  if (!file) {
    return sanitize::Status::IOError("Parquet page header: failed reading");
  }
  PageHeaderInfo page;
  SAN_ASSIGN_OR_RAISE(page, read_page_header(bytes));
  page.header_offset = static_cast<std::int64_t>(offset);
  page.compressed_payload_offset = static_cast<std::int64_t>(
      offset + static_cast<std::uint64_t>(page.header_size));
  const auto page_end = offset + static_cast<std::uint64_t>(page.header_size) +
                        static_cast<std::uint64_t>(page.compressed_page_size);
  if (page_end > limit) {
    return sanitize::Status::Invalid("Parquet page header: page exceeds chunk");
  }
  return page;
}

sanitize::Result<std::string> read_exact_payload(std::ifstream &file,
                                                 std::int64_t offset,
                                                 std::int32_t size) {
  if (offset < 0 || size < 0) {
    return sanitize::Status::Invalid("Parquet page payload: invalid range");
  }
  if (static_cast<std::size_t>(size) > kMaxPayloadVerificationBytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: payload exceeds verification limit");
  }
  std::string payload(static_cast<std::size_t>(size), '\0');
  file.clear();
  file.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  file.read(payload.data(), static_cast<std::streamsize>(payload.size()));
  if (!file) {
    return sanitize::Status::IOError("Parquet page payload: failed reading");
  }
  return payload;
}

sanitize::Status read_exact_payload_into(std::ifstream &file,
                                         std::int64_t offset, std::int32_t size,
                                         std::string *out) {
  if (!out) {
    return sanitize::Status::Invalid("Parquet page payload: output is null");
  }
  if (offset < 0 || size < 0) {
    return sanitize::Status::Invalid("Parquet page payload: invalid range");
  }
  if (static_cast<std::size_t>(size) > kMaxPayloadVerificationBytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: payload exceeds verification limit");
  }
  out->assign(static_cast<std::size_t>(size), '\0');
  file.clear();
  file.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  file.read(out->data(), static_cast<std::streamsize>(out->size()));
  if (!file) {
    return sanitize::Status::IOError("Parquet page payload: failed reading");
  }
  return {};
}

#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
sanitize::Result<std::string> gzip_decompress_payload(std::string_view payload,
                                                      std::int32_t expected) {
  if (expected < 0) {
    return sanitize::Status::Invalid(
        "Parquet page payload: negative uncompressed size");
  }
  if (static_cast<std::size_t>(expected) > kMaxPayloadVerificationBytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: uncompressed page exceeds verification limit");
  }
  std::string out(static_cast<std::size_t>(expected), '\0');
  z_stream stream{};
  int rc = inflateInit2(&stream, MAX_WBITS + 16);
  if (rc != Z_OK) {
    return sanitize::Status::IOError(
        "Parquet page payload: failed to initialize gzip decompression");
  }
  stream.next_in =
      reinterpret_cast<Bytef *>(const_cast<char *>(payload.data()));
  stream.avail_in = static_cast<uInt>(payload.size());
  stream.next_out = reinterpret_cast<Bytef *>(out.data());
  stream.avail_out = static_cast<uInt>(out.size());
  rc = inflate(&stream, Z_FINISH);
  const auto total_out = static_cast<std::int64_t>(stream.total_out);
  inflateEnd(&stream);
  if (rc != Z_STREAM_END || total_out != expected) {
    return sanitize::Status::Invalid(
        "Parquet page payload: gzip decompressed size mismatch");
  }
  out.resize(static_cast<std::size_t>(total_out));
  return out;
}

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
  out->assign(static_cast<std::size_t>(expected), '\0');
  z_stream stream{};
  stream.next_in =
      reinterpret_cast<Bytef *>(const_cast<char *>(payload.data()));
  stream.avail_in = static_cast<uInt>(payload.size());
  stream.next_out = reinterpret_cast<Bytef *>(out->data());
  stream.avail_out = static_cast<uInt>(out->size());
  if (inflateInit2(&stream, 15 + 32) != Z_OK) {
    return sanitize::Status::Invalid("Parquet page payload: gzip init failed");
  }
  const int rc = inflate(&stream, Z_FINISH);
  const int end_rc = inflateEnd(&stream);
  if (rc != Z_STREAM_END || end_rc != Z_OK ||
      stream.total_out != static_cast<uLong>(out->size())) {
    return sanitize::Status::Invalid(
        "Parquet page payload: gzip decompression failed");
  }
  return {};
}
#endif

std::optional<std::int32_t>
fixed_width_for_plain_values(const ColumnChunkInfo &column) {
  if (!column.has_physical_type) {
    return std::nullopt;
  }
  switch (column.physical_type) {
  case kPhysicalBoolean:
    return 0;
  case kPhysicalInt32:
  case kPhysicalFloat:
    return 4;
  case kPhysicalInt64:
  case kPhysicalDouble:
    return 8;
  case kPhysicalFixedLenByteArray:
    if (column.fixed_type_length <= 0) {
      return std::nullopt;
    }
    return column.fixed_type_length;
  default:
    return std::nullopt;
  }
}

bool starts_with(std::string_view value, std::string_view prefix) {
  return value.size() >= prefix.size() &&
         value.substr(0, prefix.size()) == prefix;
}

std::optional<std::int32_t> parse_positive_i32(std::string_view value) {
  if (value.empty()) {
    return std::nullopt;
  }
  std::int64_t out = 0;
  for (const char ch : value) {
    if (ch < '0' || ch > '9') {
      return std::nullopt;
    }
    out = out * 10 + static_cast<std::int64_t>(ch - '0');
    if (out > std::numeric_limits<std::int32_t>::max()) {
      return std::nullopt;
    }
  }
  if (out <= 0) {
    return std::nullopt;
  }
  return static_cast<std::int32_t>(out);
}

bool is_decimal_arrow_format(std::string_view format) {
  return starts_with(format, "d:");
}

std::optional<std::int32_t> decimal_arrow_width_bytes(std::string_view format) {
  if (!is_decimal_arrow_format(format)) {
    return std::nullopt;
  }
  const auto last_comma = format.rfind(',');
  if (last_comma == std::string_view::npos || last_comma + 1 >= format.size()) {
    return std::nullopt;
  }
  auto bits = parse_positive_i32(format.substr(last_comma + 1));
  if (!bits || *bits % 8 != 0) {
    return std::nullopt;
  }
  return *bits / 8;
}

std::optional<std::int32_t>
arrow_value_width_for_column(const ColumnChunkInfo &column) {
  const auto format = std::string_view(column.native_arrow_format);
  if (format == "c" || format == "C") {
    return 1;
  }
  if (format == "s" || format == "S") {
    return 2;
  }
  if (format == "i" || format == "I" || format == "f" || format == "tdD" ||
      format == "ttm") {
    return 4;
  }
  if (format == "l" || format == "L" || format == "g" ||
      starts_with(format, "ts") || format == "ttu" || format == "ttn") {
    return 8;
  }
  if (auto decimal_width = decimal_arrow_width_bytes(format)) {
    return decimal_width;
  }
  if (starts_with(format, "w:")) {
    return parse_positive_i32(format.substr(2));
  }
  return fixed_width_for_plain_values(column);
}

template <class T> void copy_numeric_value(std::uint8_t *target, T value) {
  std::memcpy(target, &value, sizeof(T));
}

sanitize::Status write_arrow_integer_value(std::uint8_t *target,
                                           const ColumnChunkInfo &column,
                                           std::int64_t value) {
  if (!target) {
    return sanitize::Status::Invalid(
        "native Parquet reader: null fixed-width output buffer");
  }
  const auto format = std::string_view(column.native_arrow_format);
  if (format == "c") {
    if (value < std::numeric_limits<std::int8_t>::min() ||
        value > std::numeric_limits<std::int8_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: int8 value out of range");
    }
    copy_numeric_value(target, static_cast<std::int8_t>(value));
    return {};
  }
  if (format == "C") {
    if (value < 0 || value > std::numeric_limits<std::uint8_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: uint8 value out of range");
    }
    copy_numeric_value(target, static_cast<std::uint8_t>(value));
    return {};
  }
  if (format == "s") {
    if (value < std::numeric_limits<std::int16_t>::min() ||
        value > std::numeric_limits<std::int16_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: int16 value out of range");
    }
    copy_numeric_value(target, static_cast<std::int16_t>(value));
    return {};
  }
  if (format == "S") {
    if (value < 0 || value > std::numeric_limits<std::uint16_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: uint16 value out of range");
    }
    copy_numeric_value(target, static_cast<std::uint16_t>(value));
    return {};
  }
  if (format == "i" || format == "tdD" || format == "ttm") {
    if (value < std::numeric_limits<std::int32_t>::min() ||
        value > std::numeric_limits<std::int32_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: int32 value out of range");
    }
    copy_numeric_value(target, static_cast<std::int32_t>(value));
    return {};
  }
  if (format == "I") {
    if (value < 0 || static_cast<std::uint64_t>(value) >
                         std::numeric_limits<std::uint32_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: uint32 value out of range");
    }
    copy_numeric_value(target, static_cast<std::uint32_t>(value));
    return {};
  }
  if (format == "l" || starts_with(format, "ts") || format == "ttu" ||
      format == "ttn") {
    copy_numeric_value(target, value);
    return {};
  }
  if (format == "L") {
    copy_numeric_value(target, static_cast<std::uint64_t>(value));
    return {};
  }
  return sanitize::Status::Invalid(
      "native Parquet reader: unsupported integer Arrow format");
}

sanitize::Status copy_fixed_width_physical_to_arrow(
    std::uint8_t *target, const char *source, const ColumnChunkInfo &column,
    std::int32_t physical_width, std::int32_t arrow_width) {
  if (!target || !source || physical_width <= 0 || arrow_width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid fixed-width copy input");
  }
  if (column.physical_type == kPhysicalInt32) {
    if (physical_width != 4) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid INT32 physical width");
    }
    if (column.native_arrow_format == "I") {
      copy_numeric_value(target, read_plain_value<std::uint32_t>(
                                     std::string_view(source, 4), 0));
      return {};
    }
    return write_arrow_integer_value(
        target, column,
        static_cast<std::int64_t>(
            read_plain_value<std::int32_t>(std::string_view(source, 4), 0)));
  }
  if (column.physical_type == kPhysicalInt64) {
    if (physical_width != 8) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid INT64 physical width");
    }
    return write_arrow_integer_value(
        target, column,
        read_plain_value<std::int64_t>(std::string_view(source, 8), 0));
  }
  if (is_decimal_arrow_format(column.native_arrow_format)) {
    if (physical_width != arrow_width) {
      return sanitize::Status::Invalid(
          "native Parquet reader: decimal physical width mismatch");
    }
    for (std::int32_t i = 0; i < arrow_width; ++i) {
      target[static_cast<std::size_t>(i)] =
          static_cast<std::uint8_t>(source[physical_width - 1 - i]);
    }
    return {};
  }
  if (physical_width != arrow_width) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width Arrow width mismatch");
  }
  std::memcpy(target, source, static_cast<std::size_t>(arrow_width));
  return {};
}

std::vector<std::string> preview_plain_boolean_values(std::string_view values,
                                                      std::int32_t count) {
  std::vector<std::string> out;
  out.reserve(static_cast<std::size_t>(std::min<std::int32_t>(
      count, static_cast<std::int32_t>(kMaxValuePreviewItems))));
  for (std::int32_t i = 0; i < count && out.size() < kMaxValuePreviewItems;
       ++i) {
    const auto byte =
        static_cast<std::uint8_t>(values[static_cast<std::size_t>(i) / 8U]);
    const auto bit = static_cast<std::uint8_t>(i % 8);
    out.push_back((byte & (std::uint8_t{1} << bit)) != 0 ? "true" : "false");
  }
  return out;
}

std::vector<std::string>
preview_plain_fixed_values(std::string_view values,
                           const ColumnChunkInfo &column,
                           std::int32_t expected_values) {
  std::vector<std::string> out;
  out.reserve(static_cast<std::size_t>(std::min<std::int32_t>(
      expected_values, static_cast<std::int32_t>(kMaxValuePreviewItems))));
  const auto limit = std::min<std::int32_t>(
      expected_values, static_cast<std::int32_t>(kMaxValuePreviewItems));
  for (std::int32_t i = 0; i < limit; ++i) {
    const auto offset =
        static_cast<std::size_t>(i) *
        static_cast<std::size_t>(*fixed_width_for_plain_values(column));
    switch (column.physical_type) {
    case kPhysicalInt32:
      out.push_back(
          std::to_string(read_plain_value<std::int32_t>(values, offset)));
      break;
    case kPhysicalInt64:
      out.push_back(
          std::to_string(read_plain_value<std::int64_t>(values, offset)));
      break;
    case kPhysicalFloat:
      out.push_back(std::to_string(read_plain_value<float>(values, offset)));
      break;
    case kPhysicalDouble:
      out.push_back(std::to_string(read_plain_value<double>(values, offset)));
      break;
    case kPhysicalFixedLenByteArray:
      out.push_back(hex_bytes(values.substr(
          offset, static_cast<std::size_t>(column.fixed_type_length))));
      break;
    default:
      break;
    }
  }
  return out;
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

    std::vector<std::uint8_t> bit_widths;
    bit_widths.reserve(static_cast<std::size_t>(miniblock_count));
    for (std::uint64_t mini = 0; mini < miniblock_count; ++mini) {
      const auto bit_width = static_cast<std::uint8_t>(values[offset++]);
      if (bit_width > 64) {
        return sanitize::Status::Invalid(
            "Parquet values: DELTA_BINARY_PACKED bit width too large");
      }
      bit_widths.push_back(bit_width);
    }

    for (std::uint64_t mini = 0; mini < miniblock_count; ++mini) {
      const auto bit_width = bit_widths[static_cast<std::size_t>(mini)];
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

std::optional<std::int32_t>
byte_stream_split_width(const ColumnChunkInfo &column) {
  if (!column.has_physical_type) {
    return std::nullopt;
  }
  if (column.physical_type == kPhysicalFloat) {
    return 4;
  }
  if (column.physical_type == kPhysicalDouble) {
    return 8;
  }
  return std::nullopt;
}

std::vector<std::string> preview_byte_stream_split_values(
    std::string_view values, const ColumnChunkInfo &column,
    std::int32_t expected_values, std::int32_t width) {
  std::vector<std::string> out;
  const auto limit = std::min<std::int32_t>(
      expected_values, static_cast<std::int32_t>(kMaxValuePreviewItems));
  out.reserve(static_cast<std::size_t>(limit));
  std::array<char, 8> bytes{};
  for (std::int32_t value_index = 0; value_index < limit; ++value_index) {
    for (std::int32_t byte_index = 0; byte_index < width; ++byte_index) {
      const auto source_offset = static_cast<std::size_t>(byte_index) *
                                     static_cast<std::size_t>(expected_values) +
                                 static_cast<std::size_t>(value_index);
      bytes[static_cast<std::size_t>(byte_index)] = values[source_offset];
    }
    if (column.physical_type == kPhysicalFloat) {
      out.push_back(std::to_string(read_plain_value<float>(
          std::string_view(bytes.data(), static_cast<std::size_t>(width)), 0)));
    } else {
      out.push_back(std::to_string(read_plain_value<double>(
          std::string_view(bytes.data(), static_cast<std::size_t>(width)), 0)));
    }
  }
  return out;
}

sanitize::Status decode_byte_stream_split_page(std::string_view payload,
                                               const ColumnChunkInfo &column,
                                               PageHeaderInfo *page) {
  if (!page || page->value_payload_offset < 0 ||
      static_cast<std::size_t>(page->value_payload_offset) > payload.size()) {
    return sanitize::Status::Invalid(
        "Parquet values: invalid value payload offset");
  }
  const auto values =
      payload.substr(static_cast<std::size_t>(page->value_payload_offset));
  const auto expected_values = page->decoded_non_null_values;
  auto width = byte_stream_split_width(column);
  if (!width) {
    page->values_decode_skipped = true;
    return {};
  }
  if (expected_values < 0) {
    return sanitize::Status::Invalid("Parquet values: negative value count");
  }
  const auto expected_bytes =
      static_cast<std::int64_t>(*width) * expected_values;
  if (expected_bytes < 0 ||
      expected_bytes > std::numeric_limits<std::int32_t>::max()) {
    return sanitize::Status::Invalid(
        "Parquet values: BYTE_STREAM_SPLIT payload too large");
  }
  if (values.size() != static_cast<std::size_t>(expected_bytes)) {
    return sanitize::Status::Invalid(
        "Parquet values: BYTE_STREAM_SPLIT payload size mismatch");
  }
  page->decoded_value_bytes = static_cast<std::int32_t>(values.size());
  SAN_ASSIGN_OR_RAISE(page->materialized_value_bytes,
                      arrow_fixed_width_value_buffer_bytes(
                          page->num_values, *width,
                          "BYTE_STREAM_SPLIT materialized value buffer"));
  page->decoded_value_preview =
      preview_byte_stream_split_values(values, column, expected_values, *width);
  page->values_decoded = true;
  page->values_decode_skipped = false;
  return {};
}

sanitize::Status
decode_delta_length_byte_array_page(std::string_view payload,
                                    const ColumnChunkInfo &column,
                                    PageHeaderInfo *page) {
  if (!page || page->value_payload_offset < 0 ||
      static_cast<std::size_t>(page->value_payload_offset) > payload.size()) {
    return sanitize::Status::Invalid(
        "Parquet values: invalid value payload offset");
  }
  if (!column.has_physical_type || column.physical_type != kPhysicalByteArray) {
    page->values_decode_skipped = true;
    return {};
  }
  const auto values =
      payload.substr(static_cast<std::size_t>(page->value_payload_offset));
  const auto expected_values = page->decoded_non_null_values;
  std::int32_t decoded = 0;
  std::uint64_t total_bytes = 0;
  std::vector<std::int32_t> preview_lengths;
  std::size_t lengths_bytes = 0;
  SAN_ASSIGN_OR_RAISE(
      lengths_bytes,
      decode_delta_binary_packed_stream(
          values, expected_values,
          [&](std::int64_t length) -> sanitize::Status {
            if (length < 0 ||
                length > std::numeric_limits<std::int32_t>::max()) {
              return sanitize::Status::Invalid(
                  "Parquet values: DELTA_LENGTH_BYTE_ARRAY invalid length");
            }
            if (total_bytes > std::numeric_limits<std::uint64_t>::max() -
                                  static_cast<std::uint64_t>(length)) {
              return sanitize::Status::Invalid(
                  "Parquet values: DELTA_LENGTH_BYTE_ARRAY length overflow");
            }
            total_bytes += static_cast<std::uint64_t>(length);
            if (preview_lengths.size() < kMaxValuePreviewItems) {
              preview_lengths.push_back(static_cast<std::int32_t>(length));
            }
            ++decoded;
            return {};
          }));
  if (decoded != expected_values) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_LENGTH_BYTE_ARRAY decoded count mismatch");
  }
  const auto remaining = values.size() - lengths_bytes;
  if (total_bytes != static_cast<std::uint64_t>(remaining)) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_LENGTH_BYTE_ARRAY byte payload mismatch");
  }
  const auto bytes = values.substr(lengths_bytes);
  std::size_t offset = 0;
  std::vector<std::string> preview;
  preview.reserve(preview_lengths.size());
  for (const auto length : preview_lengths) {
    const auto size = static_cast<std::size_t>(length);
    if (bytes.size() - offset < size) {
      return sanitize::Status::Invalid(
          "Parquet values: truncated DELTA_LENGTH_BYTE_ARRAY preview");
    }
    preview.push_back(preview_bytes(bytes.substr(offset, size)));
    offset += size;
  }
  if (values.size() >
      static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet values: DELTA_LENGTH_BYTE_ARRAY payload too large");
  }
  page->decoded_value_bytes = static_cast<std::int32_t>(values.size());
  SAN_ASSIGN_OR_RAISE(
      page->materialized_value_bytes,
      checked_i32_byte_count(static_cast<std::int64_t>(total_bytes),
                             "DELTA_LENGTH_BYTE_ARRAY materialized value"));
  SAN_ASSIGN_OR_RAISE(page->materialized_offset_bytes,
                      arrow_i32_offset_buffer_bytes(page->num_values));
  page->decoded_value_preview = std::move(preview);
  page->values_decoded = true;
  page->values_decode_skipped = false;
  return {};
}

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
                                    const DictionaryPageState *dictionary) {
  if (!page || page->is_dictionary_page || !page->has_num_values) {
    return {};
  }
  std::size_t offset = 0;
  if (column.max_repetition_level > 0) {
    LevelDecodeInfo repetition;
    SAN_ASSIGN_OR_RAISE(repetition,
                        decode_level_stream(payload, &offset,
                                            column.max_repetition_level,
                                            page->num_values, false, true));
    page->decoded_repetition_levels = repetition.decoded_count;
    page->decoded_repetition_level_values = std::move(repetition.level_values);
  }
  LevelDecodeInfo definition;
  definition.max_level_count = page->num_values;
  const bool capture_definition_level_values =
      column.max_repetition_level > 0 ||
      (column.path_in_schema.size() == 2 && !column.top_level_required);
  if (column.max_definition_level > 0) {
    SAN_ASSIGN_OR_RAISE(definition,
                        decode_level_stream(payload, &offset,
                                            column.max_definition_level,
                                            page->num_values, true,
                                            capture_definition_level_values));
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
  if (page->value_encoding != kEncodingPlain) {
    page->values_decode_skipped = true;
    return {};
  }
  PlainValueDecodeInfo decoded;
  SAN_ASSIGN_OR_RAISE(
      decoded, decode_plain_value_payload(payload, column, page->num_values));
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
                                              DictionaryPageState *dictionary) {
  if (page && page->is_dictionary_page) {
    return decode_dictionary_page_values(payload, column, page, dictionary);
  }
  return decode_page_levels(payload, column, page, dictionary);
}

sanitize::Status verify_page_payload(std::ifstream &file,
                                     const ColumnChunkInfo &column,
                                     PageHeaderInfo *page,
                                     DictionaryPageState *dictionary) {
  if (!page || !column.has_codec || !page->has_compressed_page_size ||
      !page->has_uncompressed_page_size) {
    return {};
  }
  if (page->compressed_page_size < 0 || page->uncompressed_page_size < 0) {
    return sanitize::Status::Invalid(
        "Parquet page payload: negative page size");
  }
  const auto compressed_size =
      static_cast<std::size_t>(page->compressed_page_size);
  const auto uncompressed_size =
      static_cast<std::size_t>(page->uncompressed_page_size);
  if (compressed_size > kMaxPayloadVerificationBytes ||
      uncompressed_size > kMaxPayloadVerificationBytes) {
    page->payload_verification_skipped = true;
    return {};
  }
  if (column.codec == kCompressionUncompressed) {
    std::string payload;
    SAN_ASSIGN_OR_RAISE(
        payload, read_exact_payload(file, page->compressed_payload_offset,
                                    page->compressed_page_size));
    page->decompressed_page_size = page->compressed_page_size;
    page->has_decompressed_page_size = true;
    page->payload_verified =
        page->compressed_page_size == page->uncompressed_page_size;
    if (!page->payload_verified) {
      return sanitize::Status::Invalid(
          "Parquet page payload: uncompressed size mismatch");
    }
    SAN_RETURN_NOT_OK(
        decode_verified_page_payload(payload, column, page, dictionary));
    return {};
  }
  if (column.codec == kCompressionGzip) {
#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
    std::string payload;
    SAN_ASSIGN_OR_RAISE(
        payload, read_exact_payload(file, page->compressed_payload_offset,
                                    page->compressed_page_size));
    std::string decompressed;
    SAN_ASSIGN_OR_RAISE(
        decompressed,
        gzip_decompress_payload(payload, page->uncompressed_page_size));
    page->decompressed_page_size =
        static_cast<std::int32_t>(decompressed.size());
    page->has_decompressed_page_size = true;
    page->payload_verified = true;
    SAN_RETURN_NOT_OK(
        decode_verified_page_payload(decompressed, column, page, dictionary));
#else
    page->payload_verification_skipped = true;
#endif
    return {};
  }
  return {};
}

sanitize::Status read_page_headers_for_column(std::ifstream &file,
                                              ColumnChunkInfo *column) {
  if (!column || !column->has_total_compressed_size ||
      column->total_compressed_size <= 0) {
    return {};
  }
  if (column->has_num_values && column->num_values == 0) {
    column->pages.clear();
    column->decoded_dictionary_values.clear();
    column->decoded_dictionary_fixed_width_values.clear();
    return {};
  }
  const bool has_dictionary = column->has_dictionary_page_offset;
  if (!has_dictionary && !column->has_data_page_offset) {
    return {};
  }
  auto start =
      static_cast<std::uint64_t>(has_dictionary ? column->dictionary_page_offset
                                                : column->data_page_offset);
  const auto data_offset =
      column->has_data_page_offset
          ? static_cast<std::uint64_t>(column->data_page_offset)
          : start;
  if (has_dictionary) {
    start = std::min(start, data_offset);
  }
  const auto limit =
      start + static_cast<std::uint64_t>(column->total_compressed_size);
  column->pages.clear();
  std::uint64_t offset = start;
  std::int64_t data_values = 0;
  DictionaryPageState dictionary;
  while (offset < limit) {
    PageHeaderInfo page;
    SAN_ASSIGN_OR_RAISE(page, read_page_header_at(file, offset, limit));
    SAN_RETURN_NOT_OK(verify_page_payload(file, *column, &page, &dictionary));
    const auto next_offset =
        offset + static_cast<std::uint64_t>(page.header_size) +
        static_cast<std::uint64_t>(page.compressed_page_size);
    if (next_offset <= offset) {
      return sanitize::Status::Invalid(
          "Parquet page header: parser did not advance");
    }
    if (!page.is_dictionary_page && page.has_num_values) {
      data_values += page.num_values;
    }
    column->pages.push_back(page);
    offset = next_offset;
    if (column->has_num_values && data_values >= column->num_values) {
      break;
    }
  }
  column->decoded_dictionary_values = std::move(dictionary.byte_array_values);
  column->decoded_dictionary_fixed_width_values =
      std::move(dictionary.fixed_width_values);
  return {};
}

sanitize::Status read_page_headers(std::ifstream &file, FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid("Parquet page header: internal error");
  }
  for (auto &row_group : info->row_groups) {
    for (auto &column : row_group.columns) {
      SAN_RETURN_NOT_OK(read_page_headers_for_column(file, &column));
    }
  }
  return {};
}

sanitize::Result<ColumnIndexInfo> parse_column_index(std::string_view bytes) {
  CompactReader reader(bytes);
  ColumnIndexInfo index;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      index.decoded = true;
      return index;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    switch (field_id) {
    case 1:
      if (type == kTypeList) {
        SAN_ASSIGN_OR_RAISE(index.null_pages, read_bool_list(reader));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeList) {
        SAN_ASSIGN_OR_RAISE(index.min_values, read_string_list(reader));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeList) {
        SAN_ASSIGN_OR_RAISE(index.max_values, read_string_list(reader));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(index.boundary_order, reader.read_i32());
        index.has_boundary_order = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 5:
      if (type == kTypeList) {
        SAN_ASSIGN_OR_RAISE(index.null_counts, read_i64_list(reader));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    default:
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      break;
    }
  }
}

sanitize::Result<PageLocationInfo> read_page_location(CompactReader &reader) {
  PageLocationInfo location;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return location;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    switch (field_id) {
    case 1:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(location.offset, reader.read_i64());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(location.compressed_page_size, reader.read_i32());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(location.first_row_index, reader.read_i64());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    default:
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      break;
    }
  }
}

sanitize::Result<OffsetIndexInfo> parse_offset_index(std::string_view bytes) {
  CompactReader reader(bytes);
  OffsetIndexInfo index;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      index.decoded = true;
      return index;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    if (field_id == 1 && type == kTypeList) {
      std::uint8_t element_type = 0;
      std::uint64_t count = 0;
      SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
      index.locations.reserve(static_cast<std::size_t>(count));
      for (std::uint64_t i = 0; i < count; ++i) {
        if (element_type != kTypeStruct) {
          SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
          continue;
        }
        PageLocationInfo location;
        SAN_ASSIGN_OR_RAISE(location, read_page_location(reader));
        index.locations.push_back(location);
      }
    } else {
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
    }
  }
}

std::vector<const PageHeaderInfo *> data_pages(const ColumnChunkInfo &column) {
  std::vector<const PageHeaderInfo *> out;
  out.reserve(column.pages.size());
  for (const auto &page : column.pages) {
    if (!page.is_dictionary_page) {
      out.push_back(&page);
    }
  }
  return out;
}

sanitize::Status validate_column_index(const ColumnChunkInfo &column) {
  if (!column.column_index.decoded) {
    return {};
  }
  const auto pages = data_pages(column);
  const auto count = pages.size();
  if (column.column_index.null_pages.size() != count ||
      column.column_index.min_values.size() != count ||
      column.column_index.max_values.size() != count ||
      column.column_index.null_counts.size() != count) {
    return sanitize::Status::Invalid(
        "Parquet column index: page count mismatch");
  }
  for (std::size_t i = 0; i < count; ++i) {
    const auto *page = pages[i];
    if (page->has_num_values &&
        column.column_index.null_counts[i] > page->num_values) {
      return sanitize::Status::Invalid(
          "Parquet column index: null count exceeds page values");
    }
    const bool expected_null_page =
        page->has_num_values &&
        column.column_index.null_counts[i] == page->num_values &&
        page->num_values > 0;
    if (column.column_index.null_pages[i] != expected_null_page) {
      return sanitize::Status::Invalid(
          "Parquet column index: null page flag mismatch");
    }
  }
  return {};
}

sanitize::Status validate_offset_index(const ColumnChunkInfo &column) {
  if (!column.offset_index.decoded) {
    return {};
  }
  const auto pages = data_pages(column);
  if (column.offset_index.locations.size() != pages.size()) {
    return sanitize::Status::Invalid(
        "Parquet offset index: page count mismatch");
  }
  std::int64_t expected_first_row = 0;
  for (std::size_t i = 0; i < pages.size(); ++i) {
    const auto *page = pages[i];
    const auto &location = column.offset_index.locations[i];
    const auto expected_size = static_cast<std::int64_t>(page->header_size) +
                               page->compressed_page_size;
    if (location.offset != page->header_offset ||
        location.compressed_page_size != expected_size) {
      return sanitize::Status::Invalid(
          "Parquet offset index: page location mismatch");
    }
    if (location.first_row_index != expected_first_row) {
      return sanitize::Status::Invalid(
          "Parquet offset index: first row index mismatch");
    }
    if (page->has_num_values) {
      expected_first_row += page->num_values;
    }
  }
  return {};
}

sanitize::Status read_page_indexes(std::ifstream &file, FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid("Parquet page index: internal error");
  }
  for (auto &row_group : info->row_groups) {
    for (auto &column : row_group.columns) {
      if (column.has_column_index_offset && column.has_column_index_length &&
          column.column_index_length > 0) {
        std::string bytes;
        SAN_ASSIGN_OR_RAISE(bytes,
                            read_exact_payload(file, column.column_index_offset,
                                               column.column_index_length));
        SAN_ASSIGN_OR_RAISE(column.column_index, parse_column_index(bytes));
        SAN_RETURN_NOT_OK(validate_column_index(column));
      }
      if (column.has_offset_index_offset && column.has_offset_index_length &&
          column.offset_index_length > 0) {
        std::string bytes;
        SAN_ASSIGN_OR_RAISE(bytes,
                            read_exact_payload(file, column.offset_index_offset,
                                               column.offset_index_length));
        SAN_ASSIGN_OR_RAISE(column.offset_index, parse_offset_index(bytes));
        SAN_RETURN_NOT_OK(validate_offset_index(column));
      }
    }
  }
  return {};
}

std::string value_buffer_kind_for_page(const ColumnChunkInfo &column,
                                       const PageHeaderInfo &page) {
  if (!page.has_value_encoding || !column.has_physical_type) {
    return {};
  }
  if (page.value_encoding == kEncodingPlain) {
    if (column.physical_type == kPhysicalBoolean) {
      return "bit_packed_boolean";
    }
    if (column.physical_type == kPhysicalByteArray) {
      return "plain_byte_array";
    }
    if (column.physical_type == kPhysicalInt32 ||
        column.physical_type == kPhysicalInt64 ||
        column.physical_type == kPhysicalFloat ||
        column.physical_type == kPhysicalDouble ||
        column.physical_type == kPhysicalFixedLenByteArray) {
      return "fixed_width";
    }
    return {};
  }
  if (page.value_encoding == kEncodingDeltaBinaryPacked) {
    return "delta_binary_packed";
  }
  if (page.value_encoding == kEncodingDeltaLengthByteArray) {
    return "delta_length_byte_array";
  }
  if (page.value_encoding == kEncodingRleDictionary) {
    if (column.physical_type == kPhysicalByteArray &&
        !column.decoded_dictionary_values.empty()) {
      return "dictionary_byte_array";
    }
    if ((column.physical_type == kPhysicalInt32 ||
         column.physical_type == kPhysicalInt64 ||
         column.physical_type == kPhysicalFloat ||
         column.physical_type == kPhysicalDouble ||
         column.physical_type == kPhysicalFixedLenByteArray) &&
        !column.decoded_dictionary_fixed_width_values.empty()) {
      return "dictionary_fixed_width";
    }
    return "rle_dictionary_indices";
  }
  if (page.value_encoding == kEncodingByteStreamSplit) {
    return "byte_stream_split";
  }
  return {};
}

std::int32_t value_width_bytes_for_page(const ColumnChunkInfo &column,
                                        const PageHeaderInfo &page) {
  if (!page.has_value_encoding) {
    return 0;
  }
  if (page.value_encoding == kEncodingByteStreamSplit) {
    auto width = arrow_value_width_for_column(column);
    return width.value_or(0);
  }
  if (page.value_encoding == kEncodingDeltaBinaryPacked) {
    auto width = arrow_value_width_for_column(column);
    return width.value_or(0);
  }
  if (page.value_encoding == kEncodingRleDictionary) {
    auto width = arrow_value_width_for_column(column);
    return width.value_or(0);
  }
  if (page.value_encoding != kEncodingPlain) {
    return 0;
  }
  auto width = arrow_value_width_for_column(column);
  return width.value_or(0);
}

std::int32_t arrow_buffer_count_for_value_kind(std::string_view kind) {
  if (kind == "plain_byte_array" || kind == "dictionary_byte_array" ||
      kind == "delta_length_byte_array") {
    return 3;
  }
  if (kind == "fixed_width" || kind == "delta_binary_packed" ||
      kind == "rle_dictionary_indices" || kind == "dictionary_fixed_width" ||
      kind == "byte_stream_split" || kind == "bit_packed_boolean") {
    return 2;
  }
  return 0;
}

std::optional<std::string> env_value(const char *name) {
  if (!name || *name == '\0') {
    return std::nullopt;
  }
#if defined(_MSC_VER)
  char *raw = nullptr;
  std::size_t size = 0;
  if (_dupenv_s(&raw, &size, name) != 0 || !raw) {
    return std::nullopt;
  }
  std::string value(raw, size > 0 ? size - 1 : 0);
  std::free(raw);
  return value;
#else
  const char *raw = std::getenv(name);
  if (!raw) {
    return std::nullopt;
  }
  return std::string(raw);
#endif
}

std::int64_t configured_positive_i64_env(const char *name,
                                         std::int64_t default_value) {
  const auto raw = env_value(name);
  if (!raw || raw->empty()) {
    return default_value;
  }
  char *end = nullptr;
  const char *begin = raw->c_str();
  const auto parsed = std::strtoll(begin, &end, 10);
  if (end == begin || (end && *end != '\0') || parsed <= 0) {
    return default_value;
  }
  return parsed;
}

std::int64_t configured_native_reader_max_buffer_bytes() {
  return configured_positive_i64_env(
      "SCHEMA_SANITIZER_NATIVE_PARQUET_READER_MAX_BUFFER_BYTES",
      kDefaultNativeReaderMaxBufferBytes);
}

sanitize::Status add_i64_checked(std::int64_t *target, std::int64_t value,
                                 std::string_view what) {
  if (!target || value < 0 ||
      *target > std::numeric_limits<std::int64_t>::max() - value) {
    return sanitize::Status::Invalid("Parquet native read plan: ", what,
                                     " exceeds int64");
  }
  *target += value;
  return {};
}

sanitize::Result<std::int64_t>
native_reader_column_buffer_bytes(const ColumnChunkInfo &column,
                                  std::int64_t row_count) {
  if (row_count < 0) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: negative row count");
  }
  std::int64_t total = 0;
  if (column.repeated_level_layout_decoded) {
    SAN_RETURN_NOT_OK(add_i64_checked(&total,
                                      (column.repeated_level_row_count + 1) * 4,
                                      "native repeated offset buffer bytes"));
    if (column.repeated_level_null_count > 0) {
      SAN_RETURN_NOT_OK(
          add_i64_checked(&total, (column.repeated_level_row_count + 7) / 8,
                          "native repeated validity buffer bytes"));
    }
  }
  if (column.nested_repeated_level_layout_decoded) {
    SAN_RETURN_NOT_OK(add_i64_checked(
        &total, (column.nested_repeated_level_row_count + 1) * 4,
        "native nested repeated offset buffer bytes"));
    if (column.nested_repeated_level_null_count > 0) {
      SAN_RETURN_NOT_OK(add_i64_checked(
          &total, (column.nested_repeated_level_row_count + 7) / 8,
          "native nested repeated validity buffer bytes"));
    }
  }
  if (column.deep_repeated_level_layout_decoded) {
    SAN_RETURN_NOT_OK(
        add_i64_checked(&total, (column.deep_repeated_level_row_count + 1) * 4,
                        "native deep repeated offset buffer bytes"));
    if (column.deep_repeated_level_null_count > 0) {
      SAN_RETURN_NOT_OK(add_i64_checked(
          &total, (column.deep_repeated_level_row_count + 7) / 8,
          "native deep repeated validity buffer bytes"));
    }
  }
  for (std::size_t level = 3; level < column.repeated_level_layouts.size();
       ++level) {
    const auto &layout = column.repeated_level_layouts[level];
    if (!layout.decoded) {
      continue;
    }
    SAN_RETURN_NOT_OK(
        add_i64_checked(&total, (layout.row_count + 1) * 4,
                        "native generic repeated offset buffer bytes"));
    if (layout.null_count > 0) {
      SAN_RETURN_NOT_OK(
          add_i64_checked(&total, (layout.row_count + 7) / 8,
                          "native generic repeated validity buffer bytes"));
    }
  }
  const auto value_row_count = column.native_read_plan_decoded
                                   ? column.native_read_arrow_length
                                   : (column.repeated_level_layout_decoded
                                          ? column.repeated_level_element_count
                                          : row_count);
  if (value_row_count < 0) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: negative value row count");
  }
  if (column.native_read_total_nulls > 0) {
    SAN_RETURN_NOT_OK(add_i64_checked(&total, (value_row_count + 7) / 8,
                                      "native validity buffer bytes"));
  }
  SAN_RETURN_NOT_OK(
      add_i64_checked(&total, column.native_read_materialized_offset_bytes,
                      "native offset buffer bytes"));
  SAN_RETURN_NOT_OK(add_i64_checked(&total,
                                    column.native_read_materialized_value_bytes,
                                    "native value buffer bytes"));
  return total;
}

sanitize::Result<std::int64_t>
native_reader_row_group_buffer_bytes(const RowGroupInfo &row_group) {
  if (!row_group.has_num_rows) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: row group is missing row count");
  }
  std::int64_t total = 0;
  for (const auto &column : row_group.columns) {
    if (!column.native_read_plan_decoded) {
      continue;
    }
    SAN_ASSIGN_OR_RAISE(
        const auto column_bytes,
        native_reader_column_buffer_bytes(column, row_group.num_rows));
    SAN_RETURN_NOT_OK(
        add_i64_checked(&total, column_bytes, "native row group buffer bytes"));
  }
  return total;
}

bool is_simple_top_level_list_leaf(const ColumnChunkInfo &column) {
  return column.max_repetition_level == 1 &&
         column.path_in_schema.size() == 3 &&
         column.path_in_schema[1] == "list";
}

std::int16_t
top_level_list_chain_depth_path(const std::vector<std::string> &path,
                                std::int16_t max_repetition_level) {
  if (max_repetition_level <= 0 || path.size() < 3 ||
      path.size() != static_cast<std::size_t>(max_repetition_level) * 2U + 1U) {
    return 0;
  }
  for (std::int16_t level = 0; level < max_repetition_level; ++level) {
    const auto list_index = static_cast<std::size_t>(level) * 2U + 1U;
    const auto element_index = list_index + 1U;
    if (path[list_index] != "list" || path[element_index] != "element") {
      return 0;
    }
  }
  return max_repetition_level;
}

std::int16_t top_level_list_chain_depth(const ColumnChunkInfo &column) {
  return top_level_list_chain_depth_path(column.path_in_schema,
                                         column.max_repetition_level);
}

bool is_top_level_list_chain_leaf_path(const std::vector<std::string> &path,
                                       std::int16_t max_repetition_level) {
  return top_level_list_chain_depth_path(path, max_repetition_level) > 0;
}

bool is_top_level_list_chain_leaf(const ColumnChunkInfo &column) {
  return top_level_list_chain_depth(column) > 0;
}

bool is_top_level_list_struct_leaf_path(const std::vector<std::string> &path,
                                        std::int16_t max_repetition_level) {
  return max_repetition_level == 1 && path.size() == 4 && path[1] == "list" &&
         path[2] == "element";
}

bool is_top_level_list_struct_list_leaf_path(
    const std::vector<std::string> &path, std::int16_t max_repetition_level) {
  return max_repetition_level == 2 && path.size() == 6 && path[1] == "list" &&
         path[2] == "element" && path[4] == "list" && path[5] == "element";
}

bool is_top_level_list_struct_map_leaf_path(
    const std::vector<std::string> &path, std::int16_t max_repetition_level) {
  return max_repetition_level == 2 && path.size() == 6 && path[1] == "list" &&
         path[2] == "element" && path[4] == "key_value";
}

std::int16_t top_level_list_struct_map_list_chain_depth_path(
    const std::vector<std::string> &path, std::int16_t max_repetition_level) {
  if (max_repetition_level < 3 || path.size() < 8 || path[1] != "list" ||
      path[2] != "element" || path[4] != "key_value" || path[5] != "value") {
    return 0;
  }
  const auto depth = static_cast<std::int16_t>(max_repetition_level - 2);
  if (path.size() != 6U + static_cast<std::size_t>(depth) * 2U) {
    return 0;
  }
  for (std::int16_t level = 0; level < depth; ++level) {
    const auto list_index = 6U + static_cast<std::size_t>(level) * 2U;
    const auto element_index = list_index + 1U;
    if (path[list_index] != "list" || path[element_index] != "element") {
      return 0;
    }
  }
  return depth;
}

std::int16_t top_level_list_struct_list_chain_depth_path(
    const std::vector<std::string> &path, std::int16_t max_repetition_level) {
  if (max_repetition_level < 2 || path.size() < 6 || path[1] != "list" ||
      path[2] != "element") {
    return 0;
  }
  const auto depth = static_cast<std::int16_t>(max_repetition_level - 1);
  if (path.size() != 4U + static_cast<std::size_t>(depth) * 2U) {
    return 0;
  }
  for (std::int16_t level = 0; level < depth; ++level) {
    const auto list_index = 4U + static_cast<std::size_t>(level) * 2U;
    const auto element_index = list_index + 1U;
    if (path[list_index] != "list" || path[element_index] != "element") {
      return 0;
    }
  }
  return depth;
}

bool is_top_level_list_list_leaf_path(const std::vector<std::string> &path,
                                      std::int16_t max_repetition_level) {
  return max_repetition_level == 2 && path.size() == 5 && path[1] == "list" &&
         path[2] == "element" && path[3] == "list";
}

bool is_top_level_list_list_list_leaf_path(const std::vector<std::string> &path,
                                           std::int16_t max_repetition_level) {
  return max_repetition_level == 3 && path.size() == 7 && path[1] == "list" &&
         path[2] == "element" && path[3] == "list" && path[4] == "element" &&
         path[5] == "list";
}

bool is_top_level_list_map_leaf_path(const std::vector<std::string> &path,
                                     std::int16_t max_repetition_level) {
  return max_repetition_level == 2 && path.size() == 5 && path[1] == "list" &&
         path[2] == "element" && path[3] == "key_value";
}

bool is_top_level_list_map_struct_leaf_path(
    const std::vector<std::string> &path, std::int16_t max_repetition_level) {
  return max_repetition_level == 2 && path.size() == 6 && path[1] == "list" &&
         path[2] == "element" && path[3] == "key_value" && path[4] == "value";
}

std::int16_t top_level_list_map_struct_list_chain_depth_path(
    const std::vector<std::string> &path, std::int16_t max_repetition_level) {
  if (max_repetition_level < 3 || path.size() < 8 || path[1] != "list" ||
      path[2] != "element" || path[3] != "key_value" || path[4] != "value") {
    return 0;
  }
  const auto depth = static_cast<std::int16_t>(max_repetition_level - 2);
  if (path.size() != 6U + static_cast<std::size_t>(depth) * 2U) {
    return 0;
  }
  for (std::int16_t level = 0; level < depth; ++level) {
    const auto list_index = 6U + static_cast<std::size_t>(level) * 2U;
    const auto element_index = list_index + 1U;
    if (path[list_index] != "list" || path[element_index] != "element") {
      return 0;
    }
  }
  return depth;
}

bool is_top_level_map_leaf_path(const std::vector<std::string> &path,
                                std::int16_t max_repetition_level) {
  return max_repetition_level == 1 && path.size() == 3 &&
         path[1] == "key_value";
}

bool is_top_level_struct_map_leaf_path(const std::vector<std::string> &path,
                                       std::int16_t max_repetition_level) {
  return max_repetition_level == 1 && path.size() == 4 &&
         path[2] == "key_value";
}

std::int16_t
top_level_struct_map_list_chain_depth_path(const std::vector<std::string> &path,
                                           std::int16_t max_repetition_level) {
  if (max_repetition_level < 2 || path.size() < 6 || path[2] != "key_value" ||
      path[3] != "value") {
    return 0;
  }
  const auto depth = static_cast<std::int16_t>(max_repetition_level - 1);
  if (path.size() != 4U + static_cast<std::size_t>(depth) * 2U) {
    return 0;
  }
  for (std::int16_t level = 0; level < depth; ++level) {
    const auto list_index = 4U + static_cast<std::size_t>(level) * 2U;
    const auto element_index = list_index + 1U;
    if (path[list_index] != "list" || path[element_index] != "element") {
      return 0;
    }
  }
  return depth;
}

bool is_top_level_map_struct_leaf_path(const std::vector<std::string> &path,
                                       std::int16_t max_repetition_level) {
  return max_repetition_level == 1 && path.size() == 4 &&
         path[1] == "key_value" && path[2] == "value";
}

std::int16_t
top_level_map_struct_list_chain_depth_path(const std::vector<std::string> &path,
                                           std::int16_t max_repetition_level) {
  if (max_repetition_level < 2 || path.size() < 6 || path[1] != "key_value" ||
      path[2] != "value") {
    return 0;
  }
  const auto depth = static_cast<std::int16_t>(max_repetition_level - 1);
  if (path.size() != 4U + static_cast<std::size_t>(depth) * 2U) {
    return 0;
  }
  for (std::int16_t level = 0; level < depth; ++level) {
    const auto list_index = 4U + static_cast<std::size_t>(level) * 2U;
    const auto element_index = list_index + 1U;
    if (path[list_index] != "list" || path[element_index] != "element") {
      return 0;
    }
  }
  return depth;
}

bool is_top_level_map_list_leaf_path(const std::vector<std::string> &path,
                                     std::int16_t max_repetition_level) {
  return max_repetition_level == 2 && path.size() == 5 &&
         path[1] == "key_value" && path[3] == "list" && path[4] == "element";
}

std::int16_t
top_level_map_list_chain_depth_path(const std::vector<std::string> &path,
                                    std::int16_t max_repetition_level) {
  if (max_repetition_level < 2 || path.size() < 5 || path[1] != "key_value") {
    return 0;
  }
  const auto depth = static_cast<std::int16_t>(max_repetition_level - 1);
  if (path.size() != 3U + static_cast<std::size_t>(depth) * 2U) {
    return 0;
  }
  for (std::int16_t level = 0; level < depth; ++level) {
    const auto list_index = 3U + static_cast<std::size_t>(level) * 2U;
    const auto element_index = list_index + 1U;
    if (path[list_index] != "list" || path[element_index] != "element") {
      return 0;
    }
  }
  return depth;
}

bool is_top_level_list_struct_leaf(const ColumnChunkInfo &column) {
  return is_top_level_list_struct_leaf_path(column.path_in_schema,
                                            column.max_repetition_level);
}

bool is_top_level_list_struct_list_leaf(const ColumnChunkInfo &column) {
  return is_top_level_list_struct_list_leaf_path(column.path_in_schema,
                                                 column.max_repetition_level);
}

bool is_top_level_list_struct_map_leaf(const ColumnChunkInfo &column) {
  return is_top_level_list_struct_map_leaf_path(column.path_in_schema,
                                                column.max_repetition_level);
}

std::int16_t
top_level_list_struct_map_list_chain_depth(const ColumnChunkInfo &column) {
  return top_level_list_struct_map_list_chain_depth_path(
      column.path_in_schema, column.max_repetition_level);
}

std::int16_t
top_level_list_struct_list_chain_depth(const ColumnChunkInfo &column) {
  return top_level_list_struct_list_chain_depth_path(
      column.path_in_schema, column.max_repetition_level);
}

bool is_top_level_list_list_leaf(const ColumnChunkInfo &column) {
  return is_top_level_list_list_leaf_path(column.path_in_schema,
                                          column.max_repetition_level);
}

bool is_top_level_list_list_list_leaf(const ColumnChunkInfo &column) {
  return is_top_level_list_list_list_leaf_path(column.path_in_schema,
                                               column.max_repetition_level);
}

bool is_top_level_list_map_leaf(const ColumnChunkInfo &column) {
  return is_top_level_list_map_leaf_path(column.path_in_schema,
                                         column.max_repetition_level);
}

bool is_top_level_list_map_struct_leaf(const ColumnChunkInfo &column) {
  return is_top_level_list_map_struct_leaf_path(column.path_in_schema,
                                                column.max_repetition_level);
}

std::int16_t
top_level_list_map_struct_list_chain_depth(const ColumnChunkInfo &column) {
  return top_level_list_map_struct_list_chain_depth_path(
      column.path_in_schema, column.max_repetition_level);
}

bool is_top_level_map_leaf(const ColumnChunkInfo &column) {
  return is_top_level_map_leaf_path(column.path_in_schema,
                                    column.max_repetition_level);
}

bool is_top_level_struct_map_leaf(const ColumnChunkInfo &column) {
  return is_top_level_struct_map_leaf_path(column.path_in_schema,
                                           column.max_repetition_level);
}

std::int16_t
top_level_struct_map_list_chain_depth(const ColumnChunkInfo &column) {
  return top_level_struct_map_list_chain_depth_path(
      column.path_in_schema, column.max_repetition_level);
}

bool is_top_level_map_struct_leaf(const ColumnChunkInfo &column) {
  return is_top_level_map_struct_leaf_path(column.path_in_schema,
                                           column.max_repetition_level);
}

std::int16_t
top_level_map_struct_list_chain_depth(const ColumnChunkInfo &column) {
  return top_level_map_struct_list_chain_depth_path(
      column.path_in_schema, column.max_repetition_level);
}

bool is_top_level_map_list_leaf(const ColumnChunkInfo &column) {
  return is_top_level_map_list_leaf_path(column.path_in_schema,
                                         column.max_repetition_level);
}

std::int16_t top_level_map_list_chain_depth(const ColumnChunkInfo &column) {
  return top_level_map_list_chain_depth_path(column.path_in_schema,
                                             column.max_repetition_level);
}

bool is_supported_top_level_list_leaf(const ColumnChunkInfo &column) {
  return is_simple_top_level_list_leaf(column) ||
         is_top_level_list_struct_leaf(column) ||
         is_top_level_list_struct_list_leaf(column) ||
         is_top_level_list_struct_map_leaf(column) ||
         (top_level_list_struct_map_list_chain_depth(column) > 0) ||
         (top_level_list_struct_list_chain_depth(column) > 1) ||
         is_top_level_list_list_leaf(column) ||
         is_top_level_list_list_list_leaf(column) ||
         (is_top_level_list_chain_leaf(column) &&
          top_level_list_chain_depth(column) > 3) ||
         is_top_level_list_map_leaf(column) ||
         is_top_level_list_map_struct_leaf(column) ||
         (top_level_list_map_struct_list_chain_depth(column) > 0) ||
         is_top_level_map_leaf(column) ||
         is_top_level_struct_map_leaf(column) ||
         (top_level_struct_map_list_chain_depth(column) > 0) ||
         is_top_level_map_struct_leaf(column) ||
         (top_level_map_struct_list_chain_depth(column) > 0) ||
         is_top_level_map_list_leaf(column) ||
         (top_level_map_list_chain_depth(column) > 1);
}

std::int64_t list_leaf_value_count(const ColumnChunkInfo &column) {
  if ((top_level_list_struct_list_chain_depth(column) > 1 ||
       top_level_list_struct_map_list_chain_depth(column) > 0 ||
       top_level_list_map_struct_list_chain_depth(column) > 0 ||
       top_level_struct_map_list_chain_depth(column) > 0 ||
       top_level_map_struct_list_chain_depth(column) > 0 ||
       top_level_map_list_chain_depth(column) > 1) &&
      !column.repeated_level_layouts.empty()) {
    return column.repeated_level_layouts.back().element_count;
  }
  if (is_top_level_list_chain_leaf(column) &&
      top_level_list_chain_depth(column) > 3 &&
      !column.repeated_level_layouts.empty()) {
    return column.repeated_level_layouts.back().element_count;
  }
  if (is_top_level_list_struct_list_leaf(column)) {
    return column.nested_repeated_level_element_count;
  }
  if (is_top_level_list_struct_map_leaf(column)) {
    return column.nested_repeated_level_element_count;
  }
  if (is_top_level_list_map_struct_leaf(column)) {
    return column.nested_repeated_level_element_count;
  }
  if (is_top_level_map_list_leaf(column)) {
    return column.nested_repeated_level_element_count;
  }
  if (is_top_level_list_list_list_leaf(column)) {
    return column.deep_repeated_level_element_count;
  }
  return (is_top_level_list_list_leaf(column) ||
          is_top_level_list_map_leaf(column))
             ? column.nested_repeated_level_element_count
             : column.repeated_level_element_count;
}

bool list_leaf_value_count_is_materializable(const ColumnChunkInfo &column) {
  const auto element_count = list_leaf_value_count(column);
  return element_count >= 0 &&
         element_count <= static_cast<std::int64_t>(
                              std::numeric_limits<std::int32_t>::max());
}

std::int16_t
list_leaf_value_parent_defined_level(const ColumnChunkInfo &column) {
  const auto list_chain_depth = top_level_list_chain_depth(column);
  if (list_chain_depth > 3) {
    const auto list_defined_level =
        column.top_level_required ? std::int16_t{0} : std::int16_t{1};
    return static_cast<std::int16_t>(list_defined_level +
                                     (list_chain_depth - 1) * 2);
  }
  const auto list_struct_chain_depth =
      top_level_list_struct_list_chain_depth(column);
  if (list_struct_chain_depth > 1) {
    const auto list_defined_level =
        column.top_level_required ? std::int16_t{0} : std::int16_t{1};
    return static_cast<std::int16_t>(list_defined_level + 3 +
                                     (list_struct_chain_depth - 1) * 2);
  }
  const auto map_list_chain_depth = top_level_map_list_chain_depth(column);
  if (map_list_chain_depth > 1) {
    const auto list_defined_level =
        column.top_level_required ? std::int16_t{0} : std::int16_t{1};
    return static_cast<std::int16_t>(list_defined_level + 2 +
                                     (map_list_chain_depth - 1) * 2);
  }
  const auto map_struct_list_chain_depth =
      top_level_map_struct_list_chain_depth(column);
  if (map_struct_list_chain_depth > 0) {
    const auto list_defined_level =
        column.top_level_required ? std::int16_t{0} : std::int16_t{1};
    return static_cast<std::int16_t>(list_defined_level + 3 +
                                     (map_struct_list_chain_depth - 1) * 2);
  }
  const auto struct_map_list_chain_depth =
      top_level_struct_map_list_chain_depth(column);
  if (struct_map_list_chain_depth > 0) {
    return static_cast<std::int16_t>(2 + 2 +
                                     (struct_map_list_chain_depth - 1) * 2);
  }
  const auto list_struct_map_list_chain_depth =
      top_level_list_struct_map_list_chain_depth(column);
  if (list_struct_map_list_chain_depth > 0) {
    const auto list_defined_level =
        column.top_level_required ? std::int16_t{0} : std::int16_t{1};
    return static_cast<std::int16_t>(
        list_defined_level + 5 + (list_struct_map_list_chain_depth - 1) * 2);
  }
  const auto list_map_struct_list_chain_depth =
      top_level_list_map_struct_list_chain_depth(column);
  if (list_map_struct_list_chain_depth > 0) {
    const auto list_defined_level =
        column.top_level_required ? std::int16_t{0} : std::int16_t{1};
    return static_cast<std::int16_t>(
        list_defined_level + 5 + (list_map_struct_list_chain_depth - 1) * 2);
  }
  if (is_top_level_list_struct_list_leaf(column)) {
    const auto list_defined_level =
        column.top_level_required ? std::int16_t{0} : std::int16_t{1};
    return static_cast<std::int16_t>(list_defined_level + 3);
  }
  if (is_top_level_list_struct_map_leaf(column)) {
    const auto list_defined_level =
        column.top_level_required ? std::int16_t{0} : std::int16_t{1};
    return static_cast<std::int16_t>(list_defined_level + 3);
  }
  if (is_top_level_map_list_leaf(column)) {
    const auto list_defined_level =
        column.top_level_required ? std::int16_t{0} : std::int16_t{1};
    return static_cast<std::int16_t>(list_defined_level + 2);
  }
  if (is_top_level_list_list_list_leaf(column)) {
    const auto list_defined_level =
        column.top_level_required ? std::int16_t{0} : std::int16_t{1};
    return static_cast<std::int16_t>(list_defined_level + 4);
  }
  if (is_top_level_list_list_leaf(column) ||
      is_top_level_list_map_leaf(column) ||
      is_top_level_list_map_struct_leaf(column)) {
    const auto list_defined_level =
        column.top_level_required ? std::int16_t{0} : std::int16_t{1};
    return static_cast<std::int16_t>(list_defined_level + 2);
  }
  if (is_top_level_struct_map_leaf(column)) {
    return std::int16_t{2};
  }
  return column.top_level_required ? std::int16_t{0} : std::int16_t{1};
}

sanitize::Status assign_nested_list_level_layout(std::int64_t row_count,
                                                 ColumnChunkInfo *column) {
  if (!column || !(is_top_level_list_list_leaf(*column) ||
                   is_top_level_list_map_leaf(*column) ||
                   is_top_level_list_map_struct_leaf(*column) ||
                   is_top_level_list_struct_list_leaf(*column) ||
                   is_top_level_list_struct_map_leaf(*column) ||
                   is_top_level_map_list_leaf(*column))) {
    return {};
  }
  if (row_count < 0 ||
      row_count > std::numeric_limits<std::int32_t>::max() - 1) {
    return sanitize::Status::Invalid(
        "Parquet nested repeated levels: row count is not materializable");
  }
  const auto outer_validity_bytes = (row_count + 7) / 8;
  if (static_cast<std::uint64_t>(outer_validity_bytes) >
      kMaxValidityBitmapBytes) {
    return sanitize::Status::Invalid(
        "Parquet nested repeated levels: outer validity bitmap exceeds memory "
        "limit");
  }
  column->repeated_level_offsets.clear();
  column->repeated_level_validity_bitmap.assign(
      static_cast<std::size_t>(outer_validity_bytes), 0);
  column->repeated_level_offsets.reserve(static_cast<std::size_t>(row_count) +
                                         1U);
  column->repeated_level_offsets.push_back(0);
  column->repeated_level_row_count = row_count;
  column->repeated_level_null_count = 0;
  column->repeated_level_element_count = 0;
  column->repeated_level_non_null_value_count = 0;
  column->nested_repeated_level_offsets.clear();
  column->nested_repeated_level_validity_bitmap.clear();
  column->nested_repeated_level_offsets.push_back(0);
  column->nested_repeated_level_row_count = 0;
  column->nested_repeated_level_null_count = 0;
  column->nested_repeated_level_element_count = 0;
  column->nested_repeated_level_non_null_value_count = 0;
  column->deep_repeated_level_offsets.clear();
  column->deep_repeated_level_validity_bitmap.clear();
  column->deep_repeated_level_row_count = 0;
  column->deep_repeated_level_null_count = 0;
  column->deep_repeated_level_element_count = 0;
  column->deep_repeated_level_non_null_value_count = 0;

  bool native_plan_complete = true;
  bool saw_data_page = false;
  std::string native_value_kind;
  std::int32_t native_value_width = 0;
  std::int32_t native_dictionary_index_bit_width = 0;
  const auto list_defined_level = static_cast<std::int16_t>(
      (column->top_level_required ? std::int16_t{0} : std::int16_t{1}) +
      (is_top_level_struct_map_leaf(*column) ? std::int16_t{1}
                                             : std::int16_t{0}));
  const auto list_valid_for_definition = [&](std::int16_t definition) {
    if (is_top_level_struct_map_leaf(*column)) {
      return definition >= list_defined_level;
    }
    return column->top_level_required || definition >= list_defined_level;
  };
  const auto inner_list_defined_level = static_cast<std::int16_t>(
      list_defined_level + ((is_top_level_list_struct_list_leaf(*column) ||
                             is_top_level_list_struct_map_leaf(*column))
                                ? 3
                                : 2));
  std::int64_t current_row = -1;
  std::int32_t current_outer_element_count = 0;
  std::int32_t current_inner_element_count = 0;
  bool saw_level = false;
  bool saw_inner_list = false;
  for (const auto &page : column->pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    saw_data_page = true;
    if (!page.has_value_encoding || !page.values_decoded ||
        page.values_decode_skipped || !page.has_compressed_page_size) {
      native_plan_complete = false;
    }
    const auto page_value_kind = value_buffer_kind_for_page(*column, page);
    const auto page_value_width = value_width_bytes_for_page(*column, page);
    if (!((page_value_kind == "fixed_width" && page_value_width > 0) ||
          page_value_kind == "plain_byte_array" ||
          page_value_kind == "dictionary_byte_array" ||
          (page_value_kind == "dictionary_fixed_width" &&
           page_value_width > 0) ||
          (page_value_kind == "delta_binary_packed" && page_value_width > 0) ||
          page_value_kind == "delta_length_byte_array" ||
          (page_value_kind == "byte_stream_split" && page_value_width > 0) ||
          page_value_kind == "bit_packed_boolean")) {
      native_plan_complete = false;
    } else if (native_value_kind.empty()) {
      native_value_kind = page_value_kind;
      native_value_width = page_value_width;
    } else if (native_value_kind != page_value_kind ||
               native_value_width != page_value_width) {
      native_plan_complete = false;
    }
    if (page_value_kind == "dictionary_byte_array" ||
        page_value_kind == "dictionary_fixed_width") {
      if (native_dictionary_index_bit_width == 0) {
        native_dictionary_index_bit_width = page.dictionary_index_bit_width;
      } else if (page.dictionary_index_bit_width != 0 &&
                 native_dictionary_index_bit_width !=
                     page.dictionary_index_bit_width) {
        native_plan_complete = false;
      }
    }
    if (!page.levels_decoded || !page.has_num_values ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values) ||
        page.decoded_repetition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      column->repeated_level_offsets.clear();
      column->repeated_level_validity_bitmap.clear();
      column->nested_repeated_level_offsets.clear();
      column->nested_repeated_level_validity_bitmap.clear();
      return {};
    }
    std::int32_t page_non_null_values = 0;
    for (std::int32_t i = 0; i < page.num_values; ++i) {
      saw_level = true;
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(i)];
      const auto repetition =
          page.decoded_repetition_level_values[static_cast<std::size_t>(i)];
      if (definition < 0 || definition > column->max_definition_level ||
          repetition < 0 || repetition > column->max_repetition_level) {
        return sanitize::Status::Invalid(
            "Parquet nested repeated levels: level exceeds schema maximum");
      }
      if (repetition == 0) {
        if (current_row >= 0) {
          column->repeated_level_offsets.push_back(current_outer_element_count);
        }
        ++current_row;
        if (current_row >= row_count) {
          return sanitize::Status::Invalid(
              "Parquet nested repeated levels: row count exceeds row group");
        }
        const bool list_valid = list_valid_for_definition(definition);
        if (list_valid) {
          set_validity_bit(&column->repeated_level_validity_bitmap,
                           static_cast<std::int32_t>(current_row), true);
        } else {
          ++column->repeated_level_null_count;
        }
      } else if (current_row < 0) {
        return sanitize::Status::Invalid(
            "Parquet nested repeated levels: first value does not start a row");
      } else if (definition <= list_defined_level) {
        return sanitize::Status::Invalid(
            "Parquet nested repeated levels: repeated null or empty outer list "
            "marker");
      }

      const bool list_valid = list_valid_for_definition(definition);
      if (list_valid && definition > list_defined_level && repetition <= 1) {
        if (saw_inner_list) {
          column->nested_repeated_level_offsets.push_back(
              current_inner_element_count);
        }
        saw_inner_list = true;
        ++current_outer_element_count;
        ++column->repeated_level_element_count;
        ++column->nested_repeated_level_row_count;
        const bool inner_list_valid = definition >= inner_list_defined_level;
        if (inner_list_valid) {
          if (column->nested_repeated_level_validity_bitmap.empty()) {
            const auto validity_bytes =
                (column->nested_repeated_level_row_count + 7) / 8;
            column->nested_repeated_level_validity_bitmap.assign(
                static_cast<std::size_t>(validity_bytes), 0);
          } else if (column->nested_repeated_level_row_count >
                     static_cast<std::int64_t>(
                         column->nested_repeated_level_validity_bitmap.size() *
                         8ULL)) {
            column->nested_repeated_level_validity_bitmap.push_back(0);
          }
          set_validity_bit(&column->nested_repeated_level_validity_bitmap,
                           static_cast<std::int32_t>(
                               column->nested_repeated_level_row_count - 1),
                           true);
        } else {
          ++column->nested_repeated_level_null_count;
        }
      }
      if (list_valid && definition > inner_list_defined_level) {
        ++current_inner_element_count;
        ++column->nested_repeated_level_element_count;
        if (definition == column->max_definition_level) {
          ++page_non_null_values;
          ++column->nested_repeated_level_non_null_value_count;
        }
      }
    }
    if (page_non_null_values != page.decoded_non_null_values) {
      return sanitize::Status::Invalid(
          "Parquet nested repeated levels: non-null value count mismatch");
    }
  }
  if (row_count == 0) {
    column->repeated_level_layout_decoded = true;
    column->nested_repeated_level_layout_decoded = true;
    return {};
  }
  if (!saw_level || current_row + 1 != row_count) {
    return sanitize::Status::Invalid(
        "Parquet nested repeated levels: row count mismatch");
  }
  column->repeated_level_offsets.push_back(current_outer_element_count);
  if (saw_inner_list) {
    column->nested_repeated_level_offsets.push_back(
        current_inner_element_count);
  }
  if (column->repeated_level_offsets.size() !=
      static_cast<std::size_t>(row_count + 1)) {
    return sanitize::Status::Invalid(
        "Parquet nested repeated levels: outer offset count mismatch");
  }
  if (column->nested_repeated_level_offsets.size() !=
      static_cast<std::size_t>(column->nested_repeated_level_row_count + 1)) {
    return sanitize::Status::Invalid(
        "Parquet nested repeated levels: inner offset count mismatch");
  }
  if (column->nested_repeated_level_null_count == 0) {
    column->nested_repeated_level_validity_bitmap.clear();
  } else if (column->nested_repeated_level_validity_bitmap.empty()) {
    const auto validity_bytes =
        (column->nested_repeated_level_row_count + 7) / 8;
    column->nested_repeated_level_validity_bitmap.assign(
        static_cast<std::size_t>(validity_bytes), 0);
  }
  column->repeated_level_layout_decoded = true;
  column->nested_repeated_level_layout_decoded = true;
  if (native_plan_complete && saw_data_page &&
      (native_value_kind == "fixed_width" ||
       native_value_kind == "plain_byte_array" ||
       native_value_kind == "dictionary_byte_array" ||
       native_value_kind == "dictionary_fixed_width" ||
       native_value_kind == "delta_binary_packed" ||
       native_value_kind == "delta_length_byte_array" ||
       native_value_kind == "byte_stream_split" ||
       native_value_kind == "bit_packed_boolean")) {
    column->native_read_plan_decoded = true;
    column->native_read_data_page_count = 0;
    column->native_read_total_rows =
        column->nested_repeated_level_element_count;
    column->native_read_total_non_nulls =
        column->nested_repeated_level_non_null_value_count;
    column->native_read_total_nulls =
        column->nested_repeated_level_element_count -
        column->nested_repeated_level_non_null_value_count;
    column->native_read_validity_bitmap_bytes =
        column->native_read_total_nulls > 0
            ? (column->nested_repeated_level_element_count + 7) / 8
            : 0;
    column->native_read_value_payload_bytes = 0;
    for (const auto &page : column->pages) {
      if (page.is_dictionary_page) {
        continue;
      }
      ++column->native_read_data_page_count;
      column->native_read_value_payload_bytes += page.decoded_value_bytes;
    }
    column->native_read_materialized_value_bytes =
        (native_value_kind == "fixed_width" ||
         native_value_kind == "dictionary_fixed_width" ||
         native_value_kind == "byte_stream_split" ||
         native_value_kind == "delta_binary_packed")
            ? column->nested_repeated_level_element_count * native_value_width
            : std::int64_t{0};
    if (native_value_kind == "bit_packed_boolean") {
      SAN_ASSIGN_OR_RAISE(
          const auto boolean_value_bytes,
          arrow_boolean_value_buffer_bytes(static_cast<std::int32_t>(
              column->nested_repeated_level_element_count)));
      column->native_read_materialized_value_bytes = boolean_value_bytes;
    }
    column->native_read_materialized_offset_bytes =
        (native_value_kind == "plain_byte_array" ||
         native_value_kind == "delta_length_byte_array" ||
         native_value_kind == "dictionary_byte_array")
            ? (column->nested_repeated_level_element_count + 1) * 4
            : std::int64_t{0};
    if (native_value_kind == "plain_byte_array" ||
        native_value_kind == "delta_length_byte_array" ||
        native_value_kind == "dictionary_byte_array") {
      for (const auto &page : column->pages) {
        if (page.is_dictionary_page) {
          continue;
        }
        column->native_read_materialized_value_bytes +=
            page.materialized_value_bytes;
      }
    }
    column->native_read_value_width_bytes = native_value_width;
    column->native_read_dictionary_index_bit_width =
        native_dictionary_index_bit_width;
    column->native_read_value_buffer_kind = native_value_kind;
    column->native_read_arrow_length =
        column->nested_repeated_level_element_count;
    column->native_read_arrow_null_count = column->native_read_total_nulls;
    column->native_read_arrow_n_buffers = 2;
    if (native_value_kind == "plain_byte_array" ||
        native_value_kind == "delta_length_byte_array" ||
        native_value_kind == "dictionary_byte_array") {
      column->native_read_arrow_n_buffers = 3;
    }
    column->native_read_arrow_n_children = 0;
    column->native_read_has_validity_buffer =
        column->native_read_total_nulls > 0 ? 1 : 0;
    column->native_read_has_offsets_buffer = 0;
    column->native_read_has_values_buffer = 1;
  }
  return {};
}

sanitize::Status assign_deep_nested_list_level_layout(std::int64_t row_count,
                                                      ColumnChunkInfo *column) {
  if (!column || !is_top_level_list_list_list_leaf(*column)) {
    return {};
  }
  if (row_count < 0 ||
      row_count > std::numeric_limits<std::int32_t>::max() - 1) {
    return sanitize::Status::Invalid(
        "Parquet deep nested repeated levels: row count is not materializable");
  }
  const auto outer_validity_bytes = (row_count + 7) / 8;
  if (static_cast<std::uint64_t>(outer_validity_bytes) >
      kMaxValidityBitmapBytes) {
    return sanitize::Status::Invalid(
        "Parquet deep nested repeated levels: outer validity bitmap exceeds "
        "memory limit");
  }
  column->repeated_level_offsets.clear();
  column->repeated_level_validity_bitmap.assign(
      static_cast<std::size_t>(outer_validity_bytes), 0);
  column->repeated_level_offsets.reserve(static_cast<std::size_t>(row_count) +
                                         1U);
  column->repeated_level_offsets.push_back(0);
  column->repeated_level_row_count = row_count;
  column->repeated_level_null_count = 0;
  column->repeated_level_element_count = 0;
  column->repeated_level_non_null_value_count = 0;
  column->nested_repeated_level_offsets.clear();
  column->nested_repeated_level_validity_bitmap.clear();
  column->nested_repeated_level_offsets.push_back(0);
  column->nested_repeated_level_row_count = 0;
  column->nested_repeated_level_null_count = 0;
  column->nested_repeated_level_element_count = 0;
  column->nested_repeated_level_non_null_value_count = 0;
  column->deep_repeated_level_offsets.clear();
  column->deep_repeated_level_validity_bitmap.clear();
  column->deep_repeated_level_offsets.push_back(0);
  column->deep_repeated_level_row_count = 0;
  column->deep_repeated_level_null_count = 0;
  column->deep_repeated_level_element_count = 0;
  column->deep_repeated_level_non_null_value_count = 0;

  bool native_plan_complete = true;
  bool saw_data_page = false;
  std::string native_value_kind;
  std::int32_t native_value_width = 0;
  std::int32_t native_dictionary_index_bit_width = 0;
  const auto list_defined_level = static_cast<std::int16_t>(
      (column->top_level_required ? std::int16_t{0} : std::int16_t{1}) +
      (is_top_level_struct_map_leaf(*column) ? std::int16_t{1}
                                             : std::int16_t{0}));
  const auto list_valid_for_definition = [&](std::int16_t definition) {
    if (is_top_level_struct_map_leaf(*column)) {
      return definition >= list_defined_level;
    }
    return column->top_level_required || definition >= list_defined_level;
  };
  const auto middle_list_defined_level =
      static_cast<std::int16_t>(list_defined_level + 2);
  const auto inner_list_defined_level =
      static_cast<std::int16_t>(list_defined_level + 4);
  std::int64_t current_row = -1;
  std::int32_t current_middle_count = 0;
  std::int32_t current_inner_count = 0;
  std::int32_t current_scalar_count = 0;
  bool saw_level = false;
  bool saw_middle_list = false;
  bool saw_inner_list = false;
  for (const auto &page : column->pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    saw_data_page = true;
    if (!page.has_value_encoding || !page.values_decoded ||
        page.values_decode_skipped || !page.has_compressed_page_size) {
      native_plan_complete = false;
    }
    const auto page_value_kind = value_buffer_kind_for_page(*column, page);
    const auto page_value_width = value_width_bytes_for_page(*column, page);
    if (!((page_value_kind == "fixed_width" && page_value_width > 0) ||
          page_value_kind == "plain_byte_array" ||
          page_value_kind == "dictionary_byte_array" ||
          (page_value_kind == "dictionary_fixed_width" &&
           page_value_width > 0) ||
          (page_value_kind == "delta_binary_packed" && page_value_width > 0) ||
          page_value_kind == "delta_length_byte_array" ||
          (page_value_kind == "byte_stream_split" && page_value_width > 0) ||
          page_value_kind == "bit_packed_boolean")) {
      native_plan_complete = false;
    } else if (native_value_kind.empty()) {
      native_value_kind = page_value_kind;
      native_value_width = page_value_width;
    } else if (native_value_kind != page_value_kind ||
               native_value_width != page_value_width) {
      native_plan_complete = false;
    }
    if (page_value_kind == "dictionary_byte_array" ||
        page_value_kind == "dictionary_fixed_width") {
      if (native_dictionary_index_bit_width == 0) {
        native_dictionary_index_bit_width = page.dictionary_index_bit_width;
      } else if (page.dictionary_index_bit_width != 0 &&
                 native_dictionary_index_bit_width !=
                     page.dictionary_index_bit_width) {
        native_plan_complete = false;
      }
    }
    if (!page.levels_decoded || !page.has_num_values ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values) ||
        page.decoded_repetition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      column->repeated_level_offsets.clear();
      column->repeated_level_validity_bitmap.clear();
      column->nested_repeated_level_offsets.clear();
      column->nested_repeated_level_validity_bitmap.clear();
      column->deep_repeated_level_offsets.clear();
      column->deep_repeated_level_validity_bitmap.clear();
      return {};
    }
    std::int32_t page_non_null_values = 0;
    for (std::int32_t i = 0; i < page.num_values; ++i) {
      saw_level = true;
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(i)];
      const auto repetition =
          page.decoded_repetition_level_values[static_cast<std::size_t>(i)];
      if (definition < 0 || definition > column->max_definition_level ||
          repetition < 0 || repetition > column->max_repetition_level) {
        return sanitize::Status::Invalid(
            "Parquet deep nested repeated levels: level exceeds schema "
            "maximum");
      }
      if (repetition == 0) {
        if (current_row >= 0) {
          column->repeated_level_offsets.push_back(current_middle_count);
        }
        ++current_row;
        if (current_row >= row_count) {
          return sanitize::Status::Invalid(
              "Parquet deep nested repeated levels: row count exceeds row "
              "group");
        }
        const bool list_valid = list_valid_for_definition(definition);
        if (list_valid) {
          set_validity_bit(&column->repeated_level_validity_bitmap,
                           static_cast<std::int32_t>(current_row), true);
        } else {
          ++column->repeated_level_null_count;
        }
      } else if (current_row < 0) {
        return sanitize::Status::Invalid(
            "Parquet deep nested repeated levels: first value does not start a "
            "row");
      } else if (definition <= list_defined_level) {
        return sanitize::Status::Invalid(
            "Parquet deep nested repeated levels: repeated null or empty outer "
            "list marker");
      }

      const bool list_valid = list_valid_for_definition(definition);
      if (list_valid && definition > list_defined_level && repetition <= 1) {
        if (saw_middle_list) {
          column->nested_repeated_level_offsets.push_back(current_inner_count);
        }
        saw_middle_list = true;
        ++current_middle_count;
        ++column->repeated_level_element_count;
        ++column->nested_repeated_level_row_count;
        const bool middle_list_valid = definition >= middle_list_defined_level;
        if (middle_list_valid) {
          if (column->nested_repeated_level_validity_bitmap.empty()) {
            const auto validity_bytes =
                (column->nested_repeated_level_row_count + 7) / 8;
            column->nested_repeated_level_validity_bitmap.assign(
                static_cast<std::size_t>(validity_bytes), 0);
          } else if (column->nested_repeated_level_row_count >
                     static_cast<std::int64_t>(
                         column->nested_repeated_level_validity_bitmap.size() *
                         8ULL)) {
            column->nested_repeated_level_validity_bitmap.push_back(0);
          }
          set_validity_bit(&column->nested_repeated_level_validity_bitmap,
                           static_cast<std::int32_t>(
                               column->nested_repeated_level_row_count - 1),
                           true);
        } else {
          ++column->nested_repeated_level_null_count;
        }
      }
      if (list_valid && definition > middle_list_defined_level &&
          repetition <= 2) {
        if (saw_inner_list) {
          column->deep_repeated_level_offsets.push_back(current_scalar_count);
        }
        saw_inner_list = true;
        ++current_inner_count;
        ++column->nested_repeated_level_element_count;
        ++column->deep_repeated_level_row_count;
        const bool inner_list_valid = definition >= inner_list_defined_level;
        if (inner_list_valid) {
          if (column->deep_repeated_level_validity_bitmap.empty()) {
            const auto validity_bytes =
                (column->deep_repeated_level_row_count + 7) / 8;
            column->deep_repeated_level_validity_bitmap.assign(
                static_cast<std::size_t>(validity_bytes), 0);
          } else if (column->deep_repeated_level_row_count >
                     static_cast<std::int64_t>(
                         column->deep_repeated_level_validity_bitmap.size() *
                         8ULL)) {
            column->deep_repeated_level_validity_bitmap.push_back(0);
          }
          set_validity_bit(&column->deep_repeated_level_validity_bitmap,
                           static_cast<std::int32_t>(
                               column->deep_repeated_level_row_count - 1),
                           true);
        } else {
          ++column->deep_repeated_level_null_count;
        }
      }
      if (list_valid && definition > inner_list_defined_level) {
        ++current_scalar_count;
        ++column->deep_repeated_level_element_count;
        if (definition == column->max_definition_level) {
          ++page_non_null_values;
          ++column->deep_repeated_level_non_null_value_count;
        }
      }
    }
    if (page_non_null_values != page.decoded_non_null_values) {
      return sanitize::Status::Invalid(
          "Parquet deep nested repeated levels: non-null value count mismatch");
    }
  }
  if (row_count == 0) {
    column->repeated_level_layout_decoded = true;
    column->nested_repeated_level_layout_decoded = true;
    column->deep_repeated_level_layout_decoded = true;
    return {};
  }
  if (!saw_level || current_row + 1 != row_count) {
    return sanitize::Status::Invalid(
        "Parquet deep nested repeated levels: row count mismatch");
  }
  column->repeated_level_offsets.push_back(current_middle_count);
  if (saw_middle_list) {
    column->nested_repeated_level_offsets.push_back(current_inner_count);
  }
  if (saw_inner_list) {
    column->deep_repeated_level_offsets.push_back(current_scalar_count);
  }
  if (column->repeated_level_offsets.size() !=
      static_cast<std::size_t>(row_count + 1)) {
    return sanitize::Status::Invalid(
        "Parquet deep nested repeated levels: outer offset count mismatch");
  }
  if (column->nested_repeated_level_offsets.size() !=
      static_cast<std::size_t>(column->nested_repeated_level_row_count + 1)) {
    return sanitize::Status::Invalid(
        "Parquet deep nested repeated levels: middle offset count mismatch");
  }
  if (column->deep_repeated_level_offsets.size() !=
      static_cast<std::size_t>(column->deep_repeated_level_row_count + 1)) {
    return sanitize::Status::Invalid(
        "Parquet deep nested repeated levels: inner offset count mismatch");
  }
  if (column->nested_repeated_level_null_count == 0) {
    column->nested_repeated_level_validity_bitmap.clear();
  } else if (column->nested_repeated_level_validity_bitmap.empty()) {
    const auto validity_bytes =
        (column->nested_repeated_level_row_count + 7) / 8;
    column->nested_repeated_level_validity_bitmap.assign(
        static_cast<std::size_t>(validity_bytes), 0);
  }
  if (column->deep_repeated_level_null_count == 0) {
    column->deep_repeated_level_validity_bitmap.clear();
  } else if (column->deep_repeated_level_validity_bitmap.empty()) {
    const auto validity_bytes = (column->deep_repeated_level_row_count + 7) / 8;
    column->deep_repeated_level_validity_bitmap.assign(
        static_cast<std::size_t>(validity_bytes), 0);
  }
  column->repeated_level_layout_decoded = true;
  column->nested_repeated_level_layout_decoded = true;
  column->deep_repeated_level_layout_decoded = true;
  if (native_plan_complete && saw_data_page &&
      (native_value_kind == "fixed_width" ||
       native_value_kind == "plain_byte_array" ||
       native_value_kind == "dictionary_byte_array" ||
       native_value_kind == "dictionary_fixed_width" ||
       native_value_kind == "delta_binary_packed" ||
       native_value_kind == "delta_length_byte_array" ||
       native_value_kind == "byte_stream_split" ||
       native_value_kind == "bit_packed_boolean")) {
    column->native_read_plan_decoded = true;
    column->native_read_data_page_count = 0;
    column->native_read_total_rows = column->deep_repeated_level_element_count;
    column->native_read_total_non_nulls =
        column->deep_repeated_level_non_null_value_count;
    column->native_read_total_nulls =
        column->deep_repeated_level_element_count -
        column->deep_repeated_level_non_null_value_count;
    column->native_read_validity_bitmap_bytes =
        column->native_read_total_nulls > 0
            ? (column->deep_repeated_level_element_count + 7) / 8
            : 0;
    column->native_read_value_payload_bytes = 0;
    for (const auto &page : column->pages) {
      if (page.is_dictionary_page) {
        continue;
      }
      ++column->native_read_data_page_count;
      column->native_read_value_payload_bytes += page.decoded_value_bytes;
    }
    column->native_read_materialized_value_bytes =
        (native_value_kind == "fixed_width" ||
         native_value_kind == "dictionary_fixed_width" ||
         native_value_kind == "byte_stream_split" ||
         native_value_kind == "delta_binary_packed")
            ? column->deep_repeated_level_element_count * native_value_width
            : std::int64_t{0};
    if (native_value_kind == "bit_packed_boolean") {
      SAN_ASSIGN_OR_RAISE(
          const auto boolean_value_bytes,
          arrow_boolean_value_buffer_bytes(static_cast<std::int32_t>(
              column->deep_repeated_level_element_count)));
      column->native_read_materialized_value_bytes = boolean_value_bytes;
    }
    column->native_read_materialized_offset_bytes =
        (native_value_kind == "plain_byte_array" ||
         native_value_kind == "delta_length_byte_array" ||
         native_value_kind == "dictionary_byte_array")
            ? (column->deep_repeated_level_element_count + 1) * 4
            : std::int64_t{0};
    if (native_value_kind == "plain_byte_array" ||
        native_value_kind == "delta_length_byte_array" ||
        native_value_kind == "dictionary_byte_array") {
      for (const auto &page : column->pages) {
        if (page.is_dictionary_page) {
          continue;
        }
        column->native_read_materialized_value_bytes +=
            page.materialized_value_bytes;
      }
    }
    column->native_read_value_width_bytes = native_value_width;
    column->native_read_dictionary_index_bit_width =
        native_dictionary_index_bit_width;
    column->native_read_value_buffer_kind = native_value_kind;
    column->native_read_arrow_length =
        column->deep_repeated_level_element_count;
    column->native_read_arrow_null_count = column->native_read_total_nulls;
    column->native_read_arrow_n_buffers = 2;
    if (native_value_kind == "plain_byte_array" ||
        native_value_kind == "delta_length_byte_array" ||
        native_value_kind == "dictionary_byte_array") {
      column->native_read_arrow_n_buffers = 3;
    }
    column->native_read_arrow_n_children = 0;
    column->native_read_has_validity_buffer =
        column->native_read_total_nulls > 0 ? 1 : 0;
    column->native_read_has_offsets_buffer = 0;
    column->native_read_has_values_buffer = 1;
  }
  return {};
}

void clear_legacy_repeated_level_layouts(ColumnChunkInfo *column) {
  if (!column) {
    return;
  }
  column->repeated_level_layout_decoded = false;
  column->repeated_level_row_count = 0;
  column->repeated_level_null_count = 0;
  column->repeated_level_element_count = 0;
  column->repeated_level_non_null_value_count = 0;
  column->repeated_level_offsets.clear();
  column->repeated_level_validity_bitmap.clear();
  column->nested_repeated_level_layout_decoded = false;
  column->nested_repeated_level_row_count = 0;
  column->nested_repeated_level_null_count = 0;
  column->nested_repeated_level_element_count = 0;
  column->nested_repeated_level_non_null_value_count = 0;
  column->nested_repeated_level_offsets.clear();
  column->nested_repeated_level_validity_bitmap.clear();
  column->deep_repeated_level_layout_decoded = false;
  column->deep_repeated_level_row_count = 0;
  column->deep_repeated_level_null_count = 0;
  column->deep_repeated_level_element_count = 0;
  column->deep_repeated_level_non_null_value_count = 0;
  column->deep_repeated_level_offsets.clear();
  column->deep_repeated_level_validity_bitmap.clear();
}

void copy_repeated_layout_to_legacy(const RepeatedLevelLayoutInfo &layout,
                                    bool *decoded, std::int64_t *row_count,
                                    std::int64_t *null_count,
                                    std::int64_t *element_count,
                                    std::int64_t *non_null_value_count,
                                    std::vector<std::int32_t> *offsets,
                                    std::vector<std::uint8_t> *validity) {
  if (!decoded || !row_count || !null_count || !element_count ||
      !non_null_value_count || !offsets || !validity) {
    return;
  }
  *decoded = layout.decoded;
  *row_count = layout.row_count;
  *null_count = layout.null_count;
  *element_count = layout.element_count;
  *non_null_value_count = layout.non_null_value_count;
  *offsets = layout.offsets;
  *validity = layout.validity_bitmap;
}

void sync_legacy_repeated_level_layouts(ColumnChunkInfo *column) {
  if (!column) {
    return;
  }
  clear_legacy_repeated_level_layouts(column);
  if (!column->repeated_level_layouts.empty()) {
    copy_repeated_layout_to_legacy(column->repeated_level_layouts[0],
                                   &column->repeated_level_layout_decoded,
                                   &column->repeated_level_row_count,
                                   &column->repeated_level_null_count,
                                   &column->repeated_level_element_count,
                                   &column->repeated_level_non_null_value_count,
                                   &column->repeated_level_offsets,
                                   &column->repeated_level_validity_bitmap);
  }
  if (column->repeated_level_layouts.size() > 1) {
    copy_repeated_layout_to_legacy(
        column->repeated_level_layouts[1],
        &column->nested_repeated_level_layout_decoded,
        &column->nested_repeated_level_row_count,
        &column->nested_repeated_level_null_count,
        &column->nested_repeated_level_element_count,
        &column->nested_repeated_level_non_null_value_count,
        &column->nested_repeated_level_offsets,
        &column->nested_repeated_level_validity_bitmap);
  }
  if (column->repeated_level_layouts.size() > 2) {
    copy_repeated_layout_to_legacy(
        column->repeated_level_layouts[2],
        &column->deep_repeated_level_layout_decoded,
        &column->deep_repeated_level_row_count,
        &column->deep_repeated_level_null_count,
        &column->deep_repeated_level_element_count,
        &column->deep_repeated_level_non_null_value_count,
        &column->deep_repeated_level_offsets,
        &column->deep_repeated_level_validity_bitmap);
  }
}

sanitize::Status
ensure_repeated_layout_validity_capacity(RepeatedLevelLayoutInfo *layout) {
  if (!layout || layout->row_count <= 0) {
    return {};
  }
  const auto validity_bytes = (layout->row_count + 7) / 8;
  if (static_cast<std::uint64_t>(validity_bytes) > kMaxValidityBitmapBytes) {
    return sanitize::Status::Invalid(
        "Parquet generic repeated levels: validity bitmap exceeds memory "
        "limit");
  }
  if (layout->validity_bitmap.empty()) {
    layout->validity_bitmap.assign(static_cast<std::size_t>(validity_bytes), 0);
  } else if (layout->row_count >
             static_cast<std::int64_t>(layout->validity_bitmap.size() * 8ULL)) {
    layout->validity_bitmap.push_back(0);
  }
  return {};
}

sanitize::Status increment_i32_counter(std::int32_t *counter,
                                       std::string_view what) {
  if (!counter || *counter == std::numeric_limits<std::int32_t>::max()) {
    return sanitize::Status::Invalid("Parquet generic repeated levels: ", what,
                                     " exceeds int32");
  }
  ++(*counter);
  return {};
}

sanitize::Status
assign_generic_list_chain_level_layout(std::int64_t row_count,
                                       ColumnChunkInfo *column) {
  if (!column) {
    return {};
  }
  const auto list_defined_level =
      (is_top_level_struct_map_leaf(*column) ||
       top_level_struct_map_list_chain_depth(*column) > 0)
          ? std::int16_t{2}
          : (column->top_level_required ? std::int16_t{0} : std::int16_t{1});
  auto depth = top_level_list_chain_depth(*column);
  std::vector<std::int16_t> list_defined_levels;
  if (depth > 3) {
    list_defined_levels.resize(static_cast<std::size_t>(depth));
    for (std::int16_t level = 0; level < depth; ++level) {
      list_defined_levels[static_cast<std::size_t>(level)] =
          static_cast<std::int16_t>(list_defined_level + level * 2);
    }
  } else {
    const auto list_struct_chain_depth =
        top_level_list_struct_list_chain_depth(*column);
    const auto list_struct_map_list_chain_depth =
        top_level_list_struct_map_list_chain_depth(*column);
    const auto list_map_struct_list_chain_depth =
        top_level_list_map_struct_list_chain_depth(*column);
    const auto struct_map_list_chain_depth =
        top_level_struct_map_list_chain_depth(*column);
    const auto map_struct_list_chain_depth =
        top_level_map_struct_list_chain_depth(*column);
    const auto map_list_chain_depth = top_level_map_list_chain_depth(*column);
    if (list_struct_chain_depth > 1) {
      depth = static_cast<std::int16_t>(list_struct_chain_depth + 1);
      list_defined_levels.resize(static_cast<std::size_t>(depth));
      list_defined_levels[0] = list_defined_level;
      for (std::int16_t level = 1; level < depth; ++level) {
        list_defined_levels[static_cast<std::size_t>(level)] =
            static_cast<std::int16_t>(list_defined_level + 3 + (level - 1) * 2);
      }
    } else if (list_struct_map_list_chain_depth > 0) {
      depth = static_cast<std::int16_t>(list_struct_map_list_chain_depth + 2);
      list_defined_levels.resize(static_cast<std::size_t>(depth));
      list_defined_levels[0] = list_defined_level;
      list_defined_levels[1] =
          static_cast<std::int16_t>(list_defined_level + 3);
      for (std::int16_t level = 2; level < depth; ++level) {
        list_defined_levels[static_cast<std::size_t>(level)] =
            static_cast<std::int16_t>(list_defined_level + 5 + (level - 2) * 2);
      }
    } else if (list_map_struct_list_chain_depth > 0) {
      depth = static_cast<std::int16_t>(list_map_struct_list_chain_depth + 2);
      list_defined_levels.resize(static_cast<std::size_t>(depth));
      list_defined_levels[0] = list_defined_level;
      list_defined_levels[1] =
          static_cast<std::int16_t>(list_defined_level + 2);
      for (std::int16_t level = 2; level < depth; ++level) {
        list_defined_levels[static_cast<std::size_t>(level)] =
            static_cast<std::int16_t>(list_defined_level + 5 + (level - 2) * 2);
      }
    } else if (struct_map_list_chain_depth > 0) {
      depth = static_cast<std::int16_t>(struct_map_list_chain_depth + 1);
      list_defined_levels.resize(static_cast<std::size_t>(depth));
      list_defined_levels[0] = list_defined_level;
      for (std::int16_t level = 1; level < depth; ++level) {
        list_defined_levels[static_cast<std::size_t>(level)] =
            static_cast<std::int16_t>(list_defined_level + 2 + (level - 1) * 2);
      }
    } else if (map_struct_list_chain_depth > 0) {
      depth = static_cast<std::int16_t>(map_struct_list_chain_depth + 1);
      list_defined_levels.resize(static_cast<std::size_t>(depth));
      list_defined_levels[0] = list_defined_level;
      for (std::int16_t level = 1; level < depth; ++level) {
        list_defined_levels[static_cast<std::size_t>(level)] =
            static_cast<std::int16_t>(list_defined_level + 3 + (level - 1) * 2);
      }
    } else if (map_list_chain_depth > 1) {
      depth = static_cast<std::int16_t>(map_list_chain_depth + 1);
      list_defined_levels.resize(static_cast<std::size_t>(depth));
      list_defined_levels[0] = list_defined_level;
      for (std::int16_t level = 1; level < depth; ++level) {
        list_defined_levels[static_cast<std::size_t>(level)] =
            static_cast<std::int16_t>(list_defined_level + 2 + (level - 1) * 2);
      }
    }
  }
  if (depth <= 0 || list_defined_levels.empty()) {
    return {};
  }
  if (row_count < 0 ||
      row_count > std::numeric_limits<std::int32_t>::max() - 1) {
    return sanitize::Status::Invalid(
        "Parquet generic repeated levels: row count is not materializable");
  }

  clear_legacy_repeated_level_layouts(column);
  column->repeated_level_layouts.clear();
  column->repeated_level_layouts.resize(static_cast<std::size_t>(depth));
  auto &outer = column->repeated_level_layouts[0];
  outer.offsets.reserve(static_cast<std::size_t>(row_count) + 1U);
  outer.offsets.push_back(0);
  outer.row_count = row_count;
  outer.validity_bitmap.assign(static_cast<std::size_t>((row_count + 7) / 8),
                               0);
  for (std::size_t level = 1; level < column->repeated_level_layouts.size();
       ++level) {
    column->repeated_level_layouts[level].offsets.push_back(0);
  }

  bool native_plan_complete = true;
  bool saw_data_page = false;
  std::string native_value_kind;
  std::int32_t native_value_width = 0;
  std::int32_t native_dictionary_index_bit_width = 0;
  std::vector<std::int32_t> current_child_counts(
      static_cast<std::size_t>(depth), 0);
  std::vector<bool> saw_layout_row(static_cast<std::size_t>(depth), false);
  std::int64_t current_row = -1;
  bool saw_level = false;

  for (const auto &page : column->pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    saw_data_page = true;
    if (!page.has_value_encoding || !page.values_decoded ||
        page.values_decode_skipped || !page.has_compressed_page_size) {
      native_plan_complete = false;
    }
    const auto page_value_kind = value_buffer_kind_for_page(*column, page);
    const auto page_value_width = value_width_bytes_for_page(*column, page);
    if (!((page_value_kind == "fixed_width" && page_value_width > 0) ||
          page_value_kind == "plain_byte_array" ||
          page_value_kind == "dictionary_byte_array" ||
          (page_value_kind == "dictionary_fixed_width" &&
           page_value_width > 0) ||
          (page_value_kind == "delta_binary_packed" && page_value_width > 0) ||
          page_value_kind == "delta_length_byte_array" ||
          (page_value_kind == "byte_stream_split" && page_value_width > 0) ||
          page_value_kind == "bit_packed_boolean")) {
      native_plan_complete = false;
    } else if (native_value_kind.empty()) {
      native_value_kind = page_value_kind;
      native_value_width = page_value_width;
    } else if (native_value_kind != page_value_kind ||
               native_value_width != page_value_width) {
      native_plan_complete = false;
    }
    if (page_value_kind == "dictionary_byte_array" ||
        page_value_kind == "dictionary_fixed_width") {
      if (native_dictionary_index_bit_width == 0) {
        native_dictionary_index_bit_width = page.dictionary_index_bit_width;
      } else if (page.dictionary_index_bit_width != 0 &&
                 native_dictionary_index_bit_width !=
                     page.dictionary_index_bit_width) {
        native_plan_complete = false;
      }
    }
    if (!page.levels_decoded || !page.has_num_values ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values) ||
        page.decoded_repetition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      column->repeated_level_layouts.clear();
      return {};
    }
    std::int32_t page_non_null_values = 0;
    for (std::int32_t i = 0; i < page.num_values; ++i) {
      saw_level = true;
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(i)];
      const auto repetition =
          page.decoded_repetition_level_values[static_cast<std::size_t>(i)];
      if (definition < 0 || definition > column->max_definition_level ||
          repetition < 0 || repetition > column->max_repetition_level) {
        return sanitize::Status::Invalid(
            "Parquet generic repeated levels: level exceeds schema maximum");
      }
      if (repetition == 0) {
        if (current_row >= 0) {
          column->repeated_level_layouts[0].offsets.push_back(
              current_child_counts[0]);
        }
        ++current_row;
        if (current_row >= row_count) {
          return sanitize::Status::Invalid(
              "Parquet generic repeated levels: row count exceeds row group");
        }
        saw_layout_row[0] = true;
        const bool list_valid =
            column->top_level_required || definition >= list_defined_levels[0];
        if (list_valid) {
          set_validity_bit(&column->repeated_level_layouts[0].validity_bitmap,
                           static_cast<std::int32_t>(current_row), true);
        } else {
          ++column->repeated_level_layouts[0].null_count;
        }
      } else if (current_row < 0) {
        return sanitize::Status::Invalid(
            "Parquet generic repeated levels: first value does not start a "
            "row");
      } else if (definition <= list_defined_levels[0]) {
        return sanitize::Status::Invalid(
            "Parquet generic repeated levels: repeated null or empty outer "
            "list marker");
      }

      const bool outer_valid =
          column->top_level_required || definition >= list_defined_levels[0];
      if (!outer_valid) {
        continue;
      }
      for (std::int16_t level = 1; level < depth; ++level) {
        const auto parent_level = static_cast<std::size_t>(level - 1);
        const auto layout_level = static_cast<std::size_t>(level);
        if (definition <= list_defined_levels[parent_level] ||
            repetition > level) {
          continue;
        }
        if (saw_layout_row[layout_level]) {
          column->repeated_level_layouts[layout_level].offsets.push_back(
              current_child_counts[layout_level]);
        }
        saw_layout_row[layout_level] = true;
        SAN_RETURN_NOT_OK(increment_i32_counter(
            &current_child_counts[parent_level], "child offset"));
        ++column->repeated_level_layouts[parent_level].element_count;
        ++column->repeated_level_layouts[layout_level].row_count;
        const bool list_valid = definition >= list_defined_levels[layout_level];
        if (list_valid) {
          SAN_RETURN_NOT_OK(ensure_repeated_layout_validity_capacity(
              &column->repeated_level_layouts[layout_level]));
          set_validity_bit(
              &column->repeated_level_layouts[layout_level].validity_bitmap,
              static_cast<std::int32_t>(
                  column->repeated_level_layouts[layout_level].row_count - 1),
              true);
        } else {
          ++column->repeated_level_layouts[layout_level].null_count;
        }
      }
      const auto deepest_level = static_cast<std::size_t>(depth - 1);
      if (definition > list_defined_levels[deepest_level]) {
        SAN_RETURN_NOT_OK(increment_i32_counter(
            &current_child_counts[deepest_level], "leaf offset"));
        ++column->repeated_level_layouts[deepest_level].element_count;
        if (definition == column->max_definition_level) {
          ++page_non_null_values;
          ++column->repeated_level_layouts[deepest_level].non_null_value_count;
        }
      }
    }
    if (page_non_null_values != page.decoded_non_null_values) {
      return sanitize::Status::Invalid(
          "Parquet generic repeated levels: non-null value count mismatch");
    }
  }
  if (row_count == 0) {
    for (auto &layout : column->repeated_level_layouts) {
      layout.decoded = true;
    }
    sync_legacy_repeated_level_layouts(column);
    return {};
  }
  if (!saw_level || current_row + 1 != row_count) {
    return sanitize::Status::Invalid(
        "Parquet generic repeated levels: row count mismatch");
  }
  column->repeated_level_layouts[0].offsets.push_back(current_child_counts[0]);
  for (std::int16_t level = 1; level < depth; ++level) {
    const auto layout_level = static_cast<std::size_t>(level);
    if (saw_layout_row[layout_level]) {
      column->repeated_level_layouts[layout_level].offsets.push_back(
          current_child_counts[layout_level]);
    }
  }
  for (std::int16_t level = 0; level < depth; ++level) {
    auto &layout =
        column->repeated_level_layouts[static_cast<std::size_t>(level)];
    if (layout.offsets.size() !=
        static_cast<std::size_t>(layout.row_count + 1)) {
      return sanitize::Status::Invalid(
          "Parquet generic repeated levels: offset count mismatch");
    }
    if (layout.null_count == 0) {
      layout.validity_bitmap.clear();
    } else if (layout.validity_bitmap.empty()) {
      const auto validity_bytes = (layout.row_count + 7) / 8;
      if (static_cast<std::uint64_t>(validity_bytes) >
          kMaxValidityBitmapBytes) {
        return sanitize::Status::Invalid(
            "Parquet generic repeated levels: validity bitmap exceeds memory "
            "limit");
      }
      layout.validity_bitmap.assign(static_cast<std::size_t>(validity_bytes),
                                    0);
    }
    layout.decoded = true;
  }
  auto &leaf_layout = column->repeated_level_layouts.back();
  if (native_plan_complete && saw_data_page &&
      (native_value_kind == "fixed_width" ||
       native_value_kind == "plain_byte_array" ||
       native_value_kind == "dictionary_byte_array" ||
       native_value_kind == "dictionary_fixed_width" ||
       native_value_kind == "delta_binary_packed" ||
       native_value_kind == "delta_length_byte_array" ||
       native_value_kind == "byte_stream_split" ||
       native_value_kind == "bit_packed_boolean")) {
    column->native_read_plan_decoded = true;
    column->native_read_data_page_count = 0;
    column->native_read_total_rows = leaf_layout.element_count;
    column->native_read_total_non_nulls = leaf_layout.non_null_value_count;
    column->native_read_total_nulls =
        leaf_layout.element_count - leaf_layout.non_null_value_count;
    column->native_read_validity_bitmap_bytes =
        column->native_read_total_nulls > 0
            ? (leaf_layout.element_count + 7) / 8
            : 0;
    column->native_read_value_payload_bytes = 0;
    for (const auto &page : column->pages) {
      if (page.is_dictionary_page) {
        continue;
      }
      ++column->native_read_data_page_count;
      column->native_read_value_payload_bytes += page.decoded_value_bytes;
    }
    column->native_read_materialized_value_bytes =
        (native_value_kind == "fixed_width" ||
         native_value_kind == "dictionary_fixed_width" ||
         native_value_kind == "byte_stream_split" ||
         native_value_kind == "delta_binary_packed")
            ? leaf_layout.element_count * native_value_width
            : std::int64_t{0};
    if (native_value_kind == "bit_packed_boolean") {
      SAN_ASSIGN_OR_RAISE(
          const auto boolean_value_bytes,
          arrow_boolean_value_buffer_bytes(
              static_cast<std::int32_t>(leaf_layout.element_count)));
      column->native_read_materialized_value_bytes = boolean_value_bytes;
    }
    column->native_read_materialized_offset_bytes =
        (native_value_kind == "plain_byte_array" ||
         native_value_kind == "delta_length_byte_array" ||
         native_value_kind == "dictionary_byte_array")
            ? (leaf_layout.element_count + 1) * 4
            : std::int64_t{0};
    if (native_value_kind == "plain_byte_array" ||
        native_value_kind == "delta_length_byte_array" ||
        native_value_kind == "dictionary_byte_array") {
      for (const auto &page : column->pages) {
        if (page.is_dictionary_page) {
          continue;
        }
        column->native_read_materialized_value_bytes +=
            page.materialized_value_bytes;
      }
    }
    column->native_read_value_width_bytes = native_value_width;
    column->native_read_dictionary_index_bit_width =
        native_dictionary_index_bit_width;
    column->native_read_value_buffer_kind = native_value_kind;
    column->native_read_arrow_length = leaf_layout.element_count;
    column->native_read_arrow_null_count = column->native_read_total_nulls;
    column->native_read_arrow_n_buffers = 2;
    if (native_value_kind == "plain_byte_array" ||
        native_value_kind == "delta_length_byte_array" ||
        native_value_kind == "dictionary_byte_array") {
      column->native_read_arrow_n_buffers = 3;
    }
    column->native_read_arrow_n_children = 0;
    column->native_read_has_validity_buffer =
        column->native_read_total_nulls > 0 ? 1 : 0;
    column->native_read_has_offsets_buffer = 0;
    column->native_read_has_values_buffer = 1;
  }
  sync_legacy_repeated_level_layouts(column);
  return {};
}

sanitize::Status assign_simple_list_level_layout(std::int64_t row_count,
                                                 ColumnChunkInfo *column) {
  if (!column || !is_supported_top_level_list_leaf(*column)) {
    return {};
  }
  if (is_top_level_list_chain_leaf(*column) &&
      top_level_list_chain_depth(*column) > 3) {
    return assign_generic_list_chain_level_layout(row_count, column);
  }
  if (top_level_list_struct_list_chain_depth(*column) > 1 ||
      top_level_list_struct_map_list_chain_depth(*column) > 0 ||
      top_level_list_map_struct_list_chain_depth(*column) > 0 ||
      top_level_struct_map_list_chain_depth(*column) > 0 ||
      top_level_map_struct_list_chain_depth(*column) > 0 ||
      top_level_map_list_chain_depth(*column) > 1) {
    return assign_generic_list_chain_level_layout(row_count, column);
  }
  if (is_top_level_list_list_list_leaf(*column)) {
    return assign_deep_nested_list_level_layout(row_count, column);
  }
  if (is_top_level_list_list_leaf(*column) ||
      is_top_level_list_map_leaf(*column) ||
      is_top_level_list_map_struct_leaf(*column) ||
      is_top_level_list_struct_list_leaf(*column) ||
      is_top_level_list_struct_map_leaf(*column) ||
      is_top_level_map_list_leaf(*column)) {
    return assign_nested_list_level_layout(row_count, column);
  }
  if (row_count < 0 ||
      row_count > std::numeric_limits<std::int32_t>::max() - 1) {
    return sanitize::Status::Invalid(
        "Parquet repeated levels: row count is not materializable");
  }
  const auto validity_bytes = (row_count + 7) / 8;
  if (static_cast<std::uint64_t>(validity_bytes) > kMaxValidityBitmapBytes) {
    return sanitize::Status::Invalid(
        "Parquet repeated levels: validity bitmap exceeds memory limit");
  }
  column->repeated_level_offsets.clear();
  column->repeated_level_validity_bitmap.assign(
      static_cast<std::size_t>(validity_bytes), 0);
  column->repeated_level_offsets.reserve(static_cast<std::size_t>(row_count) +
                                         1U);
  column->repeated_level_offsets.push_back(0);
  column->repeated_level_row_count = row_count;
  column->repeated_level_null_count = 0;
  column->repeated_level_element_count = 0;
  column->repeated_level_non_null_value_count = 0;
  bool native_plan_complete = true;
  bool saw_data_page = false;
  std::string native_value_kind;
  std::int32_t native_value_width = 0;
  std::int32_t native_dictionary_index_bit_width = 0;

  const auto list_defined_level =
      (is_top_level_struct_map_leaf(*column) ||
       top_level_struct_map_list_chain_depth(*column) > 0)
          ? std::int16_t{2}
          : (column->top_level_required ? std::int16_t{0} : std::int16_t{1});
  const auto list_valid_for_definition = [&](std::int16_t definition) {
    if (is_top_level_struct_map_leaf(*column) ||
        top_level_struct_map_list_chain_depth(*column) > 0) {
      return definition >= list_defined_level;
    }
    return column->top_level_required || definition >= list_defined_level;
  };
  std::int64_t current_row = -1;
  std::int32_t current_element_count = 0;
  bool saw_level = false;
  for (const auto &page : column->pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    saw_data_page = true;
    if (!page.has_value_encoding || !page.values_decoded ||
        page.values_decode_skipped || !page.has_compressed_page_size) {
      native_plan_complete = false;
    }
    const auto page_value_kind = value_buffer_kind_for_page(*column, page);
    const auto page_value_width = value_width_bytes_for_page(*column, page);
    if (!((page_value_kind == "fixed_width" && page_value_width > 0) ||
          page_value_kind == "plain_byte_array" ||
          page_value_kind == "dictionary_byte_array" ||
          (page_value_kind == "dictionary_fixed_width" &&
           page_value_width > 0) ||
          (page_value_kind == "delta_binary_packed" && page_value_width > 0) ||
          page_value_kind == "delta_length_byte_array" ||
          (page_value_kind == "byte_stream_split" && page_value_width > 0) ||
          page_value_kind == "bit_packed_boolean")) {
      native_plan_complete = false;
    } else if (native_value_kind.empty()) {
      native_value_kind = page_value_kind;
      native_value_width = page_value_width;
    } else if (native_value_kind != page_value_kind ||
               native_value_width != page_value_width) {
      native_plan_complete = false;
    }
    if (page_value_kind == "dictionary_byte_array" ||
        page_value_kind == "dictionary_fixed_width") {
      if (native_dictionary_index_bit_width == 0) {
        native_dictionary_index_bit_width = page.dictionary_index_bit_width;
      } else if (page.dictionary_index_bit_width != 0 &&
                 native_dictionary_index_bit_width !=
                     page.dictionary_index_bit_width) {
        native_plan_complete = false;
      }
    }
    if (!page.levels_decoded || !page.has_num_values ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values) ||
        page.decoded_repetition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      column->repeated_level_offsets.clear();
      column->repeated_level_validity_bitmap.clear();
      return {};
    }
    std::int32_t page_non_null_values = 0;
    for (std::int32_t i = 0; i < page.num_values; ++i) {
      saw_level = true;
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(i)];
      const auto repetition =
          page.decoded_repetition_level_values[static_cast<std::size_t>(i)];
      if (definition < 0 || definition > column->max_definition_level ||
          repetition < 0 || repetition > column->max_repetition_level) {
        return sanitize::Status::Invalid(
            "Parquet repeated levels: level exceeds schema maximum");
      }
      if (repetition == 0) {
        if (current_row >= 0) {
          column->repeated_level_offsets.push_back(current_element_count);
        }
        ++current_row;
        if (current_row >= row_count) {
          return sanitize::Status::Invalid(
              "Parquet repeated levels: row count exceeds row group");
        }
        const bool list_valid = list_valid_for_definition(definition);
        if (list_valid) {
          set_validity_bit(&column->repeated_level_validity_bitmap,
                           static_cast<std::int32_t>(current_row), true);
        } else {
          ++column->repeated_level_null_count;
        }
      } else if (current_row < 0) {
        return sanitize::Status::Invalid(
            "Parquet repeated levels: first value does not start a row");
      } else if (definition <= list_defined_level) {
        return sanitize::Status::Invalid(
            "Parquet repeated levels: repeated null or empty list marker");
      }

      const bool list_valid = list_valid_for_definition(definition);
      if (list_valid && definition > list_defined_level) {
        ++current_element_count;
        ++column->repeated_level_element_count;
        if (definition == column->max_definition_level) {
          ++page_non_null_values;
          ++column->repeated_level_non_null_value_count;
        }
      }
    }
    if (page_non_null_values != page.decoded_non_null_values) {
      return sanitize::Status::Invalid(
          "Parquet repeated levels: non-null value count mismatch");
    }
  }
  if (row_count == 0) {
    column->repeated_level_layout_decoded = true;
    return {};
  }
  if (!saw_level || current_row + 1 != row_count) {
    return sanitize::Status::Invalid(
        "Parquet repeated levels: row count mismatch");
  }
  column->repeated_level_offsets.push_back(current_element_count);
  if (column->repeated_level_offsets.size() !=
      static_cast<std::size_t>(row_count + 1)) {
    return sanitize::Status::Invalid(
        "Parquet repeated levels: offset count mismatch");
  }
  column->repeated_level_layout_decoded = true;
  if (native_plan_complete && saw_data_page &&
      (native_value_kind == "fixed_width" ||
       native_value_kind == "plain_byte_array" ||
       native_value_kind == "dictionary_byte_array" ||
       native_value_kind == "dictionary_fixed_width" ||
       native_value_kind == "delta_binary_packed" ||
       native_value_kind == "delta_length_byte_array" ||
       native_value_kind == "byte_stream_split" ||
       native_value_kind == "bit_packed_boolean")) {
    column->native_read_plan_decoded = true;
    column->native_read_data_page_count = 0;
    column->native_read_total_rows = column->repeated_level_element_count;
    column->native_read_total_non_nulls =
        column->repeated_level_non_null_value_count;
    column->native_read_total_nulls =
        column->repeated_level_element_count -
        column->repeated_level_non_null_value_count;
    column->native_read_validity_bitmap_bytes =
        column->native_read_total_nulls > 0
            ? (column->repeated_level_element_count + 7) / 8
            : 0;
    column->native_read_value_payload_bytes = 0;
    for (const auto &page : column->pages) {
      if (page.is_dictionary_page) {
        continue;
      }
      ++column->native_read_data_page_count;
      column->native_read_value_payload_bytes += page.decoded_value_bytes;
    }
    column->native_read_materialized_value_bytes =
        (native_value_kind == "fixed_width" ||
         native_value_kind == "dictionary_fixed_width" ||
         native_value_kind == "byte_stream_split" ||
         native_value_kind == "delta_binary_packed")
            ? column->repeated_level_element_count * native_value_width
            : std::int64_t{0};
    if (native_value_kind == "bit_packed_boolean") {
      SAN_ASSIGN_OR_RAISE(
          const auto boolean_value_bytes,
          arrow_boolean_value_buffer_bytes(
              static_cast<std::int32_t>(column->repeated_level_element_count)));
      column->native_read_materialized_value_bytes = boolean_value_bytes;
    }
    column->native_read_materialized_offset_bytes =
        (native_value_kind == "plain_byte_array" ||
         native_value_kind == "delta_length_byte_array" ||
         native_value_kind == "dictionary_byte_array")
            ? (column->repeated_level_element_count + 1) * 4
            : std::int64_t{0};
    if (native_value_kind == "plain_byte_array" ||
        native_value_kind == "delta_length_byte_array" ||
        native_value_kind == "dictionary_byte_array") {
      for (const auto &page : column->pages) {
        if (page.is_dictionary_page) {
          continue;
        }
        column->native_read_materialized_value_bytes +=
            page.materialized_value_bytes;
      }
    }
    column->native_read_value_width_bytes = native_value_width;
    column->native_read_dictionary_index_bit_width =
        native_dictionary_index_bit_width;
    column->native_read_value_buffer_kind = native_value_kind;
    column->native_read_arrow_length = column->repeated_level_element_count;
    column->native_read_arrow_null_count = column->native_read_total_nulls;
    column->native_read_arrow_n_buffers = 2;
    if (native_value_kind == "plain_byte_array" ||
        native_value_kind == "delta_length_byte_array" ||
        native_value_kind == "dictionary_byte_array") {
      column->native_read_arrow_n_buffers = 3;
    }
    column->native_read_arrow_n_children = 0;
    column->native_read_has_validity_buffer =
        column->native_read_total_nulls > 0 ? 1 : 0;
    column->native_read_has_offsets_buffer = 0;
    column->native_read_has_values_buffer = 1;
  }
  return {};
}

sanitize::Status assign_native_read_page_spans(FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: internal error");
  }
  for (auto &row_group : info->row_groups) {
    for (auto &column : row_group.columns) {
      column.native_read_plan_decoded = false;
      column.native_read_data_page_count = 0;
      column.native_read_total_rows = 0;
      column.native_read_total_non_nulls = 0;
      column.native_read_total_nulls = 0;
      column.native_read_validity_bitmap_bytes = 0;
      column.native_read_value_payload_bytes = 0;
      column.native_read_materialized_value_bytes = 0;
      column.native_read_materialized_offset_bytes = 0;
      column.native_read_value_width_bytes = 0;
      column.native_read_dictionary_index_bit_width = 0;
      column.native_read_value_buffer_kind.clear();
      column.native_read_arrow_length = 0;
      column.native_read_arrow_null_count = 0;
      column.native_read_arrow_n_buffers = 0;
      column.native_read_arrow_n_children = 0;
      column.native_read_has_validity_buffer = 0;
      column.native_read_has_offsets_buffer = 0;
      column.native_read_has_values_buffer = 0;
      column.native_read_page_spans.clear();
      column.repeated_level_layout_decoded = false;
      column.repeated_level_row_count = 0;
      column.repeated_level_null_count = 0;
      column.repeated_level_element_count = 0;
      column.repeated_level_non_null_value_count = 0;
      column.repeated_level_offsets.clear();
      column.repeated_level_validity_bitmap.clear();
      column.nested_repeated_level_layout_decoded = false;
      column.nested_repeated_level_row_count = 0;
      column.nested_repeated_level_null_count = 0;
      column.nested_repeated_level_element_count = 0;
      column.nested_repeated_level_non_null_value_count = 0;
      column.nested_repeated_level_offsets.clear();
      column.nested_repeated_level_validity_bitmap.clear();
      column.deep_repeated_level_layout_decoded = false;
      column.deep_repeated_level_row_count = 0;
      column.deep_repeated_level_null_count = 0;
      column.deep_repeated_level_element_count = 0;
      column.deep_repeated_level_non_null_value_count = 0;
      column.deep_repeated_level_offsets.clear();
      column.deep_repeated_level_validity_bitmap.clear();
      column.repeated_level_layouts.clear();
      if (column.max_repetition_level != 0) {
        SAN_RETURN_NOT_OK(assign_simple_list_level_layout(
            row_group.has_num_rows ? row_group.num_rows : -1, &column));
        continue;
      }
      bool complete = true;
      bool saw_data_page = false;
      std::int64_t cumulative_rows = 0;
      std::size_t data_page_index = 0;
      for (std::size_t page_index = 0; page_index < column.pages.size();
           ++page_index) {
        const auto &page = column.pages[page_index];
        if (page.is_dictionary_page) {
          continue;
        }
        saw_data_page = true;
        if (!page.has_num_values || !page.levels_decoded ||
            !page.validity_bitmap_decoded || !page.has_value_encoding ||
            !page.has_compressed_page_size || !page.values_decoded ||
            page.values_decode_skipped) {
          complete = false;
          break;
        }
        const auto value_buffer_kind = value_buffer_kind_for_page(column, page);
        if (value_buffer_kind.empty()) {
          complete = false;
          break;
        }
        const auto value_width_bytes = value_width_bytes_for_page(column, page);
        if (column.native_read_value_buffer_kind.empty()) {
          column.native_read_value_buffer_kind = value_buffer_kind;
        } else if (column.native_read_value_buffer_kind != value_buffer_kind) {
          complete = false;
          break;
        }
        if (column.native_read_value_width_bytes == 0) {
          column.native_read_value_width_bytes = value_width_bytes;
        } else if (value_width_bytes != 0 &&
                   column.native_read_value_width_bytes != value_width_bytes) {
          complete = false;
          break;
        }
        if (column.native_read_dictionary_index_bit_width == 0) {
          column.native_read_dictionary_index_bit_width =
              page.dictionary_index_bit_width;
        } else if (page.dictionary_index_bit_width != 0 &&
                   column.native_read_dictionary_index_bit_width !=
                       page.dictionary_index_bit_width) {
          complete = false;
          break;
        }
        SAN_RETURN_NOT_OK(add_i64_checked(&column.native_read_total_rows,
                                          page.num_values, "row count"));
        SAN_RETURN_NOT_OK(add_i64_checked(&column.native_read_total_non_nulls,
                                          page.decoded_non_null_values,
                                          "non-null count"));
        SAN_RETURN_NOT_OK(add_i64_checked(&column.native_read_total_nulls,
                                          page.decoded_null_values,
                                          "null count"));
        SAN_RETURN_NOT_OK(add_i64_checked(
            &column.native_read_validity_bitmap_bytes,
            page.decoded_validity_bytes, "validity bitmap bytes"));
        SAN_RETURN_NOT_OK(
            add_i64_checked(&column.native_read_value_payload_bytes,
                            page.decoded_value_bytes, "value payload bytes"));
        SAN_RETURN_NOT_OK(add_i64_checked(
            &column.native_read_materialized_value_bytes,
            page.materialized_value_bytes, "materialized value bytes"));
        SAN_RETURN_NOT_OK(add_i64_checked(
            &column.native_read_materialized_offset_bytes,
            page.materialized_offset_bytes, "materialized offset bytes"));
        std::int64_t first_row_index = cumulative_rows;
        if (column.offset_index.decoded &&
            data_page_index < column.offset_index.locations.size()) {
          first_row_index =
              column.offset_index.locations[data_page_index].first_row_index;
        }
        column.native_read_page_spans.push_back(NativeReadPageSpanInfo{
            .page_index = static_cast<std::int32_t>(page_index),
            .first_row_index = first_row_index,
            .row_count = page.num_values,
            .non_null_count = page.decoded_non_null_values,
            .null_count = page.decoded_null_values,
            .value_encoding = page.value_encoding,
            .payload_offset = page.compressed_payload_offset,
            .payload_size = page.compressed_page_size,
            .validity_bitmap_bytes = page.decoded_validity_bytes,
            .value_payload_offset = page.value_payload_offset,
            .value_payload_bytes = page.decoded_value_bytes,
            .value_width_bytes = value_width_bytes,
            .materialized_value_bytes = page.materialized_value_bytes,
            .materialized_offset_bytes = page.materialized_offset_bytes,
            .dictionary_index_bit_width = page.dictionary_index_bit_width,
            .value_buffer_kind = value_buffer_kind,
        });
        cumulative_rows += page.num_values;
        ++column.native_read_data_page_count;
        ++data_page_index;
      }
      if (column.has_num_values && cumulative_rows != column.num_values) {
        complete = false;
      }
      if (!saw_data_page && column.has_num_values && column.num_values > 0) {
        complete = false;
      }
      if (complete) {
        const auto arrow_buffer_count = arrow_buffer_count_for_value_kind(
            column.native_read_value_buffer_kind);
        if (arrow_buffer_count == 0) {
          complete = false;
        } else {
          column.native_read_arrow_length = column.native_read_total_rows;
          column.native_read_arrow_null_count = column.native_read_total_nulls;
          column.native_read_arrow_n_buffers = arrow_buffer_count;
          column.native_read_arrow_n_children = 0;
          column.native_read_has_validity_buffer =
              column.native_read_total_nulls > 0 ? 1 : 0;
          column.native_read_has_offsets_buffer =
              column.native_read_materialized_offset_bytes > 0 ? 1 : 0;
          column.native_read_has_values_buffer = 1;
        }
      }
      column.native_read_plan_decoded = complete;
    }
  }
  return {};
}

std::string column_path_label(const ColumnChunkInfo &column) {
  if (column.path_in_schema.empty()) {
    return "<unknown>";
  }
  std::string out;
  for (std::size_t i = 0; i < column.path_in_schema.size(); ++i) {
    if (i > 0) {
      out.push_back('.');
    }
    out += column.path_in_schema[i];
  }
  return out;
}

bool supported_native_reader_physical_type(const ColumnChunkInfo &column) {
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

void add_readiness_blocker(NativeReadinessInfo *info, std::string blocker) {
  if (!info) {
    return;
  }
  info->ready = false;
  info->blockers.push_back(std::move(blocker));
}

bool native_plain_path_is_materializable(const std::vector<std::string> &path,
                                         std::int16_t max_repetition_level,
                                         bool top_level_required);
bool is_simple_top_level_list_path(const std::vector<std::string> &path,
                                   std::int16_t max_repetition_level);
bool is_top_level_list_struct_leaf_path(const std::vector<std::string> &path,
                                        std::int16_t max_repetition_level);
bool is_top_level_list_struct_list_leaf_path(
    const std::vector<std::string> &path, std::int16_t max_repetition_level);
std::int16_t top_level_list_struct_list_chain_depth_path(
    const std::vector<std::string> &path, std::int16_t max_repetition_level);
bool is_top_level_list_struct_map_leaf_path(
    const std::vector<std::string> &path, std::int16_t max_repetition_level);
std::int16_t top_level_list_struct_map_list_chain_depth_path(
    const std::vector<std::string> &path, std::int16_t max_repetition_level);
bool is_top_level_list_list_leaf_path(const std::vector<std::string> &path,
                                      std::int16_t max_repetition_level);
bool is_top_level_list_list_list_leaf_path(const std::vector<std::string> &path,
                                           std::int16_t max_repetition_level);
bool is_top_level_list_chain_leaf_path(const std::vector<std::string> &path,
                                       std::int16_t max_repetition_level);
bool is_top_level_list_map_leaf_path(const std::vector<std::string> &path,
                                     std::int16_t max_repetition_level);
bool is_top_level_list_map_struct_leaf_path(
    const std::vector<std::string> &path, std::int16_t max_repetition_level);
std::int16_t top_level_list_map_struct_list_chain_depth_path(
    const std::vector<std::string> &path, std::int16_t max_repetition_level);
bool is_top_level_map_leaf_path(const std::vector<std::string> &path,
                                std::int16_t max_repetition_level);
bool is_top_level_struct_map_leaf_path(const std::vector<std::string> &path,
                                       std::int16_t max_repetition_level);
std::int16_t
top_level_struct_map_list_chain_depth_path(const std::vector<std::string> &path,
                                           std::int16_t max_repetition_level);
bool is_top_level_map_struct_leaf_path(const std::vector<std::string> &path,
                                       std::int16_t max_repetition_level);
std::int16_t
top_level_map_struct_list_chain_depth_path(const std::vector<std::string> &path,
                                           std::int16_t max_repetition_level);
bool is_top_level_map_list_leaf_path(const std::vector<std::string> &path,
                                     std::int16_t max_repetition_level);
std::int16_t
top_level_map_list_chain_depth_path(const std::vector<std::string> &path,
                                    std::int16_t max_repetition_level);
bool is_supported_top_level_list_leaf(const ColumnChunkInfo &column);

NativeReadinessInfo native_reader_readiness(const FooterInfo &info) {
  NativeReadinessInfo readiness;
  const auto max_buffer_bytes = configured_native_reader_max_buffer_bytes();
  if (info.created_by != "schema-sanitizer native parquet writer") {
    add_readiness_blocker(&readiness,
                          "file was not written by schema-sanitizer native "
                          "parquet writer");
  }
  if (info.schema_elements.empty()) {
    add_readiness_blocker(&readiness, "missing schema elements");
  }
  if (info.row_groups.empty() && info.num_rows > 0) {
    add_readiness_blocker(&readiness, "non-empty file has no row groups");
  }
  if (info.row_groups.empty() && info.num_rows == 0 &&
      !info.schema_elements.empty()) {
    auto leaves = schema_leaf_levels(info.schema_elements);
    if (!leaves.ok()) {
      add_readiness_blocker(&readiness,
                            "empty file schema is not materializable yet");
    } else {
      std::vector<LeafLevelInfo> projected_leaves;
      const auto projection_status = project_leaf_levels_for_columns(
          leaves.ValueOrDie(), info.projected_columns, &projected_leaves);
      if (!projection_status.ok()) {
        add_readiness_blocker(&readiness,
                              "empty file schema projection failed");
      }
      for (const auto &leaf : projected_leaves) {
        std::string label;
        for (std::size_t i = 0; i < leaf.path.size(); ++i) {
          if (i > 0) {
            label.push_back('.');
          }
          label += leaf.path[i];
        }
        if (label.empty()) {
          label = "<unknown>";
        }
        if (!native_plain_path_is_materializable(leaf.path,
                                                 leaf.max_repetition_level,
                                                 leaf.top_level_required)) {
          add_readiness_blocker(
              &readiness, label + ": nested path is not materializable yet");
        }
        if (leaf.max_repetition_level != 0 &&
            !is_simple_top_level_list_path(leaf.path,
                                           leaf.max_repetition_level) &&
            !is_top_level_list_struct_leaf_path(leaf.path,
                                                leaf.max_repetition_level) &&
            !is_top_level_list_struct_list_leaf_path(
                leaf.path, leaf.max_repetition_level) &&
            !is_top_level_list_struct_map_leaf_path(
                leaf.path, leaf.max_repetition_level) &&
            top_level_list_struct_map_list_chain_depth_path(
                leaf.path, leaf.max_repetition_level) == 0 &&
            top_level_list_struct_list_chain_depth_path(
                leaf.path, leaf.max_repetition_level) == 0 &&
            !is_top_level_list_list_leaf_path(leaf.path,
                                              leaf.max_repetition_level) &&
            !is_top_level_list_list_list_leaf_path(leaf.path,
                                                   leaf.max_repetition_level) &&
            !is_top_level_list_chain_leaf_path(leaf.path,
                                               leaf.max_repetition_level) &&
            !is_top_level_list_map_leaf_path(leaf.path,
                                             leaf.max_repetition_level) &&
            !is_top_level_list_map_struct_leaf_path(
                leaf.path, leaf.max_repetition_level) &&
            top_level_list_map_struct_list_chain_depth_path(
                leaf.path, leaf.max_repetition_level) == 0 &&
            !is_top_level_map_leaf_path(leaf.path, leaf.max_repetition_level) &&
            !is_top_level_struct_map_leaf_path(leaf.path,
                                               leaf.max_repetition_level) &&
            top_level_struct_map_list_chain_depth_path(
                leaf.path, leaf.max_repetition_level) == 0 &&
            !is_top_level_map_struct_leaf_path(leaf.path,
                                               leaf.max_repetition_level) &&
            top_level_map_struct_list_chain_depth_path(
                leaf.path, leaf.max_repetition_level) == 0 &&
            !is_top_level_map_list_leaf_path(leaf.path,
                                             leaf.max_repetition_level) &&
            top_level_map_list_chain_depth_path(
                leaf.path, leaf.max_repetition_level) == 0) {
          add_readiness_blocker(&readiness, label + ": repeated levels are not "
                                                    "materializable yet");
        }
        if (leaf.native_arrow_format.empty()) {
          add_readiness_blocker(
              &readiness, label + ": native Arrow format was not planned");
        }
      }
    }
  }

  for (std::size_t row_group_index = 0;
       row_group_index < info.row_groups.size(); ++row_group_index) {
    const auto &row_group = info.row_groups[row_group_index];
    if (row_group.columns.empty() && row_group.has_num_rows &&
        row_group.num_rows > 0) {
      add_readiness_blocker(&readiness, "row group " +
                                            std::to_string(row_group_index) +
                                            " has rows but no columns");
    }
    for (const auto &column : row_group.columns) {
      const auto label = column_path_label(column);
      const bool supported_list_ready =
          is_supported_top_level_list_leaf(column) &&
          column.repeated_level_layout_decoded &&
          column.native_read_plan_decoded;
      if (!native_plain_path_is_materializable(column.path_in_schema,
                                               column.max_repetition_level,
                                               column.top_level_required)) {
        add_readiness_blocker(
            &readiness, label + ": nested path is not materializable yet");
      }
      if (column.max_repetition_level != 0 && !supported_list_ready) {
        add_readiness_blocker(&readiness, label + ": repeated levels are not "
                                                  "materializable yet");
      }
      if (!supported_native_reader_physical_type(column)) {
        add_readiness_blocker(&readiness,
                              label + ": unsupported physical type");
      }
      if (column.native_arrow_format.empty()) {
        add_readiness_blocker(&readiness,
                              label + ": native Arrow format was not planned");
      }
      if (!column.has_codec || (column.codec != kCompressionUncompressed &&
                                column.codec != kCompressionGzip)) {
        add_readiness_blocker(&readiness, label + ": unsupported compression");
      }
      if (!supported_list_ready && !column.offset_index.decoded) {
        add_readiness_blocker(&readiness,
                              label + ": offset index was not decoded");
      }
      if (!supported_list_ready && !column.column_index.decoded) {
        add_readiness_blocker(&readiness,
                              label + ": column index was not decoded");
      }
      if (!column.native_read_plan_decoded) {
        add_readiness_blocker(
            &readiness, label + ": native read page plan was not decoded");
      }

      std::int64_t data_values = 0;
      bool saw_data_page = false;
      for (const auto &page : column.pages) {
        if (page.payload_verification_skipped) {
          add_readiness_blocker(&readiness,
                                label + ": page payload verification skipped");
        }
        if (!page.payload_verified) {
          add_readiness_blocker(&readiness,
                                label + ": page payload was not verified");
        }
        if (!page.values_decoded || page.values_decode_skipped) {
          add_readiness_blocker(&readiness,
                                label + ": page values were not decoded");
        }
        if (page.is_dictionary_page) {
          continue;
        }
        saw_data_page = true;
        if (!page.levels_decoded) {
          add_readiness_blocker(&readiness,
                                label + ": page levels were not decoded");
        }
        if (!page.validity_bitmap_decoded) {
          add_readiness_blocker(
              &readiness, label + ": page validity bitmap was not decoded");
        }
        if (page.has_num_values) {
          data_values += page.num_values;
        } else {
          add_readiness_blocker(&readiness,
                                label + ": data page is missing value count");
        }
      }
      if (column.has_num_values && data_values != column.num_values) {
        add_readiness_blocker(&readiness,
                              label + ": decoded page values do not match "
                                      "column value count");
      }
      if (!saw_data_page && column.has_num_values && column.num_values > 0) {
        add_readiness_blocker(&readiness, label + ": column has no data pages");
      }
    }
    auto estimated = native_reader_row_group_buffer_bytes(row_group);
    if (!estimated.ok()) {
      add_readiness_blocker(&readiness,
                            "row group " + std::to_string(row_group_index) +
                                ": native buffer estimate failed: " +
                                estimated.status().message());
    } else if (*estimated > max_buffer_bytes) {
      add_readiness_blocker(
          &readiness,
          "row group " + std::to_string(row_group_index) +
              ": native buffer estimate " + std::to_string(*estimated) +
              " exceeds configured limit " + std::to_string(max_buffer_bytes));
    }
  }
  return readiness;
}

void append_int_array(std::string &out,
                      const std::vector<std::int32_t> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    out += std::to_string(items[i]);
  }
  out.push_back(']');
}

void append_int16_array(std::string &out,
                        const std::vector<std::int16_t> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    out += std::to_string(items[i]);
  }
  out.push_back(']');
}

void append_int64_array(std::string &out,
                        const std::vector<std::int64_t> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    out += std::to_string(items[i]);
  }
  out.push_back(']');
}

void append_bool_array(std::string &out, const std::vector<bool> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    out += items[i] ? "true" : "false";
  }
  out.push_back(']');
}

void append_string_array(std::string &out,
                         const std::vector<std::string> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    json_write::append_string(out, items[i]);
  }
  out.push_back(']');
}

void append_hex_string_array(std::string &out,
                             const std::vector<std::string> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    json_write::append_string(out, hex_bytes(items[i]));
  }
  out.push_back(']');
}

void append_page_locations(std::string &out,
                           const std::vector<PageLocationInfo> &locations) {
  out.push_back('[');
  for (std::size_t i = 0; i < locations.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    const auto &location = locations[i];
    out.push_back('{');
    bool first = true;
    json_write::append_int_field(out, first, "offset", location.offset);
    json_write::append_int_field(out, first, "compressed_page_size",
                                 location.compressed_page_size);
    json_write::append_int_field(out, first, "first_row_index",
                                 location.first_row_index);
    out.push_back('}');
  }
  out.push_back(']');
}

void append_native_read_page_spans(
    std::string &out, const std::vector<NativeReadPageSpanInfo> &spans) {
  out.push_back('[');
  for (std::size_t i = 0; i < spans.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    const auto &span = spans[i];
    out.push_back('{');
    bool first = true;
    json_write::append_int_field(out, first, "page_index", span.page_index);
    json_write::append_int_field(out, first, "first_row_index",
                                 span.first_row_index);
    json_write::append_int_field(out, first, "row_count", span.row_count);
    json_write::append_int_field(out, first, "non_null_count",
                                 span.non_null_count);
    json_write::append_int_field(out, first, "null_count", span.null_count);
    json_write::append_int_field(out, first, "value_encoding",
                                 span.value_encoding);
    json_write::append_int_field(out, first, "payload_offset",
                                 span.payload_offset);
    json_write::append_int_field(out, first, "payload_size", span.payload_size);
    json_write::append_int_field(out, first, "validity_bitmap_bytes",
                                 span.validity_bitmap_bytes);
    json_write::append_int_field(out, first, "value_payload_offset",
                                 span.value_payload_offset);
    json_write::append_int_field(out, first, "value_payload_bytes",
                                 span.value_payload_bytes);
    json_write::append_int_field(out, first, "value_width_bytes",
                                 span.value_width_bytes);
    json_write::append_int_field(out, first, "materialized_value_bytes",
                                 span.materialized_value_bytes);
    json_write::append_int_field(out, first, "materialized_offset_bytes",
                                 span.materialized_offset_bytes);
    json_write::append_int_field(out, first, "dictionary_index_bit_width",
                                 span.dictionary_index_bit_width);
    json_write::append_string_field(out, first, "value_buffer_kind",
                                    span.value_buffer_kind);
    out.push_back('}');
  }
  out.push_back(']');
}

sanitize::Result<FooterInfo> parse_footer(std::string_view footer) {
  CompactReader reader(footer);
  FooterInfo info;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return info;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;

    switch (field_id) {
    case 1:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(info.version, reader.read_i32());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeList) {
        SAN_RETURN_NOT_OK(read_schema_elements(reader, &info));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(info.num_rows, reader.read_i64());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeList) {
        SAN_RETURN_NOT_OK(read_row_groups(reader, &info));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 6:
      if (type == kTypeBinary) {
        SAN_ASSIGN_OR_RAISE(info.created_by, reader.read_binary());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    default:
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      break;
    }
  }
}

struct NativeParquetColumnSchema {
  ArrowSchema schema{};
  std::string name;
  std::string format;
};

struct NativeParquetInnerListSchema {
  ArrowSchema schema{};
  std::string name = "item";
  std::string format = "+l";
  NativeParquetColumnSchema child;
  std::array<ArrowSchema *, 1> child_ptrs{nullptr};
};

struct NativeParquetStructSchema {
  ArrowSchema schema{};
  std::string name;
  std::string format = "+s";
  std::vector<NativeParquetColumnSchema> children;
  std::vector<NativeParquetInnerListSchema> list_children;
  std::vector<NativeParquetStructSchema> struct_children;
  std::vector<ArrowSchema *> child_ptrs;
};

struct NativeParquetMapSchema {
  ArrowSchema schema{};
  std::string name;
  std::string format = "+m";
  NativeParquetStructSchema entries;
  std::array<ArrowSchema *, 1> child_ptrs{nullptr};
};

struct NativeParquetListSchema {
  ArrowSchema schema{};
  std::string name;
  std::string format = "+l";
  bool child_is_struct = false;
  bool child_is_list = false;
  bool child_is_deep_list = false;
  bool child_is_map = false;
  NativeParquetColumnSchema child;
  NativeParquetStructSchema struct_child;
  NativeParquetInnerListSchema list_child;
  NativeParquetInnerListSchema deep_list_child;
  std::vector<NativeParquetInnerListSchema> chain_list_children;
  NativeParquetMapSchema map_child;
  std::vector<NativeParquetMapSchema> struct_map_children;
  std::array<ArrowSchema *, 1> child_ptrs{nullptr};
};

struct NativeParquetTopLevelSchema {
  bool is_struct = false;
  bool is_list = false;
  bool is_map = false;
  NativeParquetColumnSchema leaf;
  NativeParquetStructSchema struct_node;
  NativeParquetListSchema list_node;
  NativeParquetMapSchema map_node;
  std::vector<NativeParquetMapSchema> struct_map_children;
};

struct NativeParquetSchemaState {
  std::string root_format = "+s";
  std::vector<NativeParquetTopLevelSchema> fields;
  std::vector<ArrowSchema *> children;
};

struct NativeParquetChildArray {
  ArrowArray array{};
  std::vector<std::uint8_t> validity;
  std::vector<std::int32_t> offsets;
  std::vector<std::uint8_t> values;
  std::array<const void *, 3> buffers{nullptr, nullptr, nullptr};
};

struct NativeParquetStructArray {
  ArrowArray array{};
  std::vector<std::uint8_t> validity;
  std::vector<ArrowArray *> children;
  std::array<const void *, 1> buffers{nullptr};
};

struct NativeParquetListArray {
  ArrowArray array{};
  std::vector<std::uint8_t> validity;
  std::vector<std::int32_t> offsets;
  std::array<ArrowArray *, 1> children{nullptr};
  std::array<const void *, 2> buffers{nullptr, nullptr};
};

struct NativeParquetArrayState {
  std::vector<NativeParquetChildArray> columns;
  std::vector<NativeParquetStructArray> structs;
  std::vector<NativeParquetListArray> lists;
  std::vector<ArrowArray *> children;
  std::array<const void *, 1> struct_buffers{nullptr};
};

struct NativeParquetPageScratch {
  std::string compressed_payload;
  std::string decompressed_payload;
};

struct NativeParquetStreamState {
  std::string path;
  std::ifstream file;
  NativeParquetPageScratch page_scratch;
  FooterInfo footer;
  std::size_t row_group_index = 0;
  std::string last_error;
};

void native_parquet_schema_child_release(ArrowSchema *schema) {
  if (!schema || !schema->release) {
    return;
  }
  sanitize::internal::cdata_stream::clear_schema(schema);
}

void native_parquet_schema_release(ArrowSchema *schema) {
  if (!schema || !schema->release) {
    return;
  }
  auto *state = static_cast<NativeParquetSchemaState *>(schema->private_data);
  delete state;
  sanitize::internal::cdata_stream::clear_schema(schema);
}

void native_parquet_array_child_release(ArrowArray *array) {
  if (!array || !array->release) {
    return;
  }
  sanitize::internal::cdata_stream::clear_array(array);
}

void native_parquet_array_release(ArrowArray *array) {
  if (!array || !array->release) {
    return;
  }
  auto *state = static_cast<NativeParquetArrayState *>(array->private_data);
  delete state;
  sanitize::internal::cdata_stream::clear_array(array);
}

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

struct NativeParquetOutputField {
  bool is_struct = false;
  bool is_list = false;
  bool is_list_struct = false;
  bool is_list_list = false;
  bool is_list_list_list = false;
  bool is_list_map = false;
  bool is_map = false;
  std::int16_t list_depth = 0;
  bool top_level_required = true;
  std::string name;
  std::vector<std::size_t> column_indices;
};

bool is_simple_top_level_list_path(const std::vector<std::string> &path,
                                   std::int16_t max_repetition_level) {
  return max_repetition_level == 1 && path.size() == 3 && path[1] == "list";
}

bool native_plain_path_is_materializable(const std::vector<std::string> &path,
                                         std::int16_t max_repetition_level,
                                         bool top_level_required) {
  if (is_simple_top_level_list_path(path, max_repetition_level) ||
      is_top_level_list_struct_leaf_path(path, max_repetition_level) ||
      is_top_level_list_struct_list_leaf_path(path, max_repetition_level) ||
      is_top_level_list_struct_map_leaf_path(path, max_repetition_level) ||
      top_level_list_struct_map_list_chain_depth_path(
          path, max_repetition_level) > 0 ||
      top_level_list_struct_list_chain_depth_path(path, max_repetition_level) >
          1 ||
      is_top_level_list_list_leaf_path(path, max_repetition_level) ||
      is_top_level_list_list_list_leaf_path(path, max_repetition_level) ||
      is_top_level_list_chain_leaf_path(path, max_repetition_level) ||
      is_top_level_list_map_leaf_path(path, max_repetition_level) ||
      is_top_level_list_map_struct_leaf_path(path, max_repetition_level) ||
      top_level_list_map_struct_list_chain_depth_path(
          path, max_repetition_level) > 0 ||
      is_top_level_map_leaf_path(path, max_repetition_level) ||
      is_top_level_struct_map_leaf_path(path, max_repetition_level) ||
      top_level_struct_map_list_chain_depth_path(path, max_repetition_level) >
          0 ||
      is_top_level_map_struct_leaf_path(path, max_repetition_level) ||
      top_level_map_struct_list_chain_depth_path(path, max_repetition_level) >
          0 ||
      is_top_level_map_list_leaf_path(path, max_repetition_level) ||
      top_level_map_list_chain_depth_path(path, max_repetition_level) > 1) {
    (void)top_level_required;
    return true;
  }
  if (max_repetition_level != 0) {
    return false;
  }
  if (path.size() == 1) {
    return true;
  }
  (void)top_level_required;
  return path.size() == 2;
}

sanitize::Status add_native_output_field(
    std::vector<NativeParquetOutputField> *fields,
    const std::vector<std::string> &path, std::size_t column_index,
    std::int16_t max_repetition_level, bool top_level_required) {
  if (!fields) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output layout is null");
  }
  if (!native_plain_path_is_materializable(path, max_repetition_level,
                                           top_level_required)) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: nested path is not materializable yet");
  }
  if (path.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: column path is empty");
  }
  const bool is_list =
      is_simple_top_level_list_path(path, max_repetition_level) ||
      is_top_level_list_struct_leaf_path(path, max_repetition_level) ||
      is_top_level_list_struct_list_leaf_path(path, max_repetition_level) ||
      is_top_level_list_struct_map_leaf_path(path, max_repetition_level) ||
      top_level_list_struct_map_list_chain_depth_path(
          path, max_repetition_level) > 0 ||
      top_level_list_struct_list_chain_depth_path(path, max_repetition_level) >
          1 ||
      is_top_level_list_list_leaf_path(path, max_repetition_level) ||
      is_top_level_list_list_list_leaf_path(path, max_repetition_level) ||
      is_top_level_list_chain_leaf_path(path, max_repetition_level) ||
      is_top_level_list_map_leaf_path(path, max_repetition_level) ||
      is_top_level_list_map_struct_leaf_path(path, max_repetition_level) ||
      top_level_list_map_struct_list_chain_depth_path(path,
                                                      max_repetition_level) > 0;
  const bool is_map =
      is_top_level_map_leaf_path(path, max_repetition_level) ||
      is_top_level_map_struct_leaf_path(path, max_repetition_level) ||
      top_level_map_struct_list_chain_depth_path(path, max_repetition_level) >
          0 ||
      is_top_level_map_list_leaf_path(path, max_repetition_level) ||
      top_level_map_list_chain_depth_path(path, max_repetition_level) > 1;
  const auto list_depth =
      top_level_list_chain_depth_path(path, max_repetition_level);
  const bool is_list_struct =
      is_top_level_list_struct_leaf_path(path, max_repetition_level) ||
      is_top_level_list_struct_list_leaf_path(path, max_repetition_level) ||
      is_top_level_list_struct_map_leaf_path(path, max_repetition_level) ||
      top_level_list_struct_map_list_chain_depth_path(
          path, max_repetition_level) > 0 ||
      top_level_list_struct_list_chain_depth_path(path, max_repetition_level) >
          1;
  const bool is_list_list =
      is_top_level_list_list_leaf_path(path, max_repetition_level);
  const bool is_list_list_list =
      is_top_level_list_list_list_leaf_path(path, max_repetition_level);
  const bool is_list_map =
      is_top_level_list_map_leaf_path(path, max_repetition_level) ||
      is_top_level_list_map_struct_leaf_path(path, max_repetition_level) ||
      top_level_list_map_struct_list_chain_depth_path(path,
                                                      max_repetition_level) > 0;
  const bool is_struct =
      !is_list && !is_map &&
      (path.size() == 2 ||
       is_top_level_struct_map_leaf_path(path, max_repetition_level) ||
       top_level_struct_map_list_chain_depth_path(path, max_repetition_level) >
           0);
  const auto &top_level_name = path[0];
  auto match = std::find_if(fields->begin(), fields->end(),
                            [&](const NativeParquetOutputField &field) {
                              return field.name == top_level_name;
                            });
  if (match == fields->end()) {
    NativeParquetOutputField field;
    field.is_struct = is_struct;
    field.is_list = is_list;
    field.is_list_struct = is_list_struct;
    field.is_list_list = is_list_list;
    field.is_list_list_list = is_list_list_list;
    field.is_list_map = is_list_map;
    field.is_map = is_map;
    field.list_depth = list_depth;
    field.top_level_required = top_level_required;
    field.name = top_level_name;
    field.column_indices.push_back(column_index);
    fields->push_back(std::move(field));
    return {};
  }
  if (match->is_struct != is_struct || match->is_list != is_list ||
      match->is_list_struct != is_list_struct ||
      match->is_list_list != is_list_list ||
      match->is_list_list_list != is_list_list_list ||
      match->is_list_map != is_list_map || match->is_map != is_map ||
      match->list_depth != list_depth) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: mixed scalar, struct, and list output path");
  }
  if (is_list && !is_list_struct && !is_list_list && !is_list_list_list &&
      !is_list_map && list_depth <= 1) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: multi-column list output path");
  }
  if (list_depth > 3) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: multi-column generic nested list output path");
  }
  if (is_list_list_list) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: multi-column deep nested list output path");
  }
  if (is_list_list) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: multi-column nested list output path");
  }
  const bool is_list_map_struct =
      is_top_level_list_map_struct_leaf_path(path, max_repetition_level) ||
      top_level_list_map_struct_list_chain_depth_path(path,
                                                      max_repetition_level) > 0;
  if (is_list_map && !is_list_map_struct && match->column_indices.size() >= 2) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: list map output has too many leaf columns");
  }
  const bool is_map_struct =
      is_top_level_map_struct_leaf_path(path, max_repetition_level) ||
      top_level_map_struct_list_chain_depth_path(path, max_repetition_level) >
          0;
  if (is_map && !is_map_struct && match->column_indices.size() >= 2) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: map output has too many leaf columns");
  }
  if (match->top_level_required != top_level_required) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: inconsistent struct output nullability");
  }
  match->column_indices.push_back(column_index);
  return {};
}

sanitize::Status
build_native_output_layout(const std::vector<ColumnChunkInfo> &columns,
                           std::vector<NativeParquetOutputField> *fields) {
  if (!fields) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output layout is null");
  }
  fields->clear();
  fields->reserve(columns.size());
  for (std::size_t i = 0; i < columns.size(); ++i) {
    const auto &column = columns[i];
    SAN_RETURN_NOT_OK(add_native_output_field(fields, column.path_in_schema, i,
                                              column.max_repetition_level,
                                              column.top_level_required));
  }
  return {};
}

sanitize::Status
build_native_output_layout(const std::vector<LeafLevelInfo> &leaves,
                           std::vector<NativeParquetOutputField> *fields) {
  if (!fields) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output layout is null");
  }
  fields->clear();
  fields->reserve(leaves.size());
  for (std::size_t i = 0; i < leaves.size(); ++i) {
    const auto &leaf = leaves[i];
    SAN_RETURN_NOT_OK(add_native_output_field(fields, leaf.path, i,
                                              leaf.max_repetition_level,
                                              leaf.top_level_required));
  }
  return {};
}

sanitize::Status materialization_payload(std::ifstream &file,
                                         const ColumnChunkInfo &column,
                                         const PageHeaderInfo &page,
                                         NativeParquetPageScratch *scratch,
                                         std::string_view *out) {
  if (!scratch || !out) {
    return sanitize::Status::Invalid(
        "native Parquet reader: page scratch is null");
  }
  if (!column.has_codec || !page.has_compressed_page_size ||
      !page.has_uncompressed_page_size) {
    return sanitize::Status::Invalid(
        "native Parquet reader: page payload sizes are incomplete");
  }
  SAN_RETURN_NOT_OK(read_exact_payload_into(
      file, page.compressed_payload_offset, page.compressed_page_size,
      &scratch->compressed_payload));
  if (column.codec == kCompressionUncompressed) {
    if (page.compressed_page_size != page.uncompressed_page_size) {
      return sanitize::Status::Invalid(
          "native Parquet reader: uncompressed page size mismatch");
    }
    *out = scratch->compressed_payload;
    return {};
  }
  if (column.codec == kCompressionGzip) {
#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
    SAN_RETURN_NOT_OK(gzip_decompress_payload_into(
        scratch->compressed_payload, page.uncompressed_page_size,
        &scratch->decompressed_payload));
    *out = scratch->decompressed_payload;
    return {};
#else
    return sanitize::Status::NotImplemented(
        "native Parquet reader: gzip support was not compiled in");
#endif
  }
  return sanitize::Status::NotImplemented(
      "native Parquet reader: unsupported compression");
}

sanitize::Result<std::int64_t>
materialize_optional_struct_validity(const ColumnChunkInfo &column,
                                     std::int64_t row_count,
                                     std::vector<std::uint8_t> *validity) {
  if (!validity) {
    return sanitize::Status::Invalid(
        "native Parquet reader: struct validity output is null");
  }
  if (row_count < 0 ||
      row_count > std::numeric_limits<std::int64_t>::max() - 7) {
    return sanitize::Status::Invalid(
        "native Parquet reader: struct row count is invalid");
  }
  const auto validity_bytes = (row_count + 7) / 8;
  validity->assign(static_cast<std::size_t>(validity_bytes), 0);
  std::int64_t null_count = row_count;
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: struct validity page span is invalid");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (span.row_count < 0 || page.decoded_definition_level_values.size() !=
                                  static_cast<std::size_t>(span.row_count)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: struct definition level count mismatch");
    }
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: struct row span exceeds row group");
      }
      if (page.decoded_definition_level_values[static_cast<std::size_t>(row)] >=
          1) {
        set_output_validity_bit(validity, global_row);
        --null_count;
      }
    }
  }
  return null_count;
}

sanitize::Status
validate_list_struct_repetition_layout(const RowGroupInfo &row_group,
                                       const NativeParquetOutputField &field) {
  if (!field.is_list || !field.is_list_struct || field.column_indices.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list struct output layout");
  }
  const auto first_index = field.column_indices.front();
  if (first_index >= row_group.columns.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list struct column index is invalid");
  }
  const auto &first = row_group.columns[first_index];
  if (!(is_top_level_list_struct_leaf(first) ||
        is_top_level_list_struct_list_leaf(first) ||
        is_top_level_list_struct_map_leaf(first) ||
        top_level_list_struct_map_list_chain_depth(first) > 0 ||
        top_level_list_struct_list_chain_depth(first) > 1) ||
      !first.repeated_level_layout_decoded) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: list struct layout was not decoded");
  }
  for (const auto column_index : field.column_indices) {
    if (column_index >= row_group.columns.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list struct column index is invalid");
    }
    const auto &column = row_group.columns[column_index];
    if (!(is_top_level_list_struct_leaf(column) ||
          is_top_level_list_struct_list_leaf(column) ||
          is_top_level_list_struct_map_leaf(column) ||
          top_level_list_struct_map_list_chain_depth(column) > 0 ||
          top_level_list_struct_list_chain_depth(column) > 1) ||
        !column.repeated_level_layout_decoded ||
        column.repeated_level_row_count != first.repeated_level_row_count ||
        column.repeated_level_null_count != first.repeated_level_null_count ||
        column.repeated_level_element_count !=
            first.repeated_level_element_count ||
        column.repeated_level_offsets != first.repeated_level_offsets ||
        column.repeated_level_validity_bitmap !=
            first.repeated_level_validity_bitmap) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: list struct leaf repetition layouts differ");
    }
    if ((is_top_level_list_struct_list_leaf(column) ||
         is_top_level_list_struct_map_leaf(column) ||
         top_level_list_struct_map_list_chain_depth(column) > 0 ||
         top_level_list_struct_list_chain_depth(column) > 1) &&
        !column.nested_repeated_level_layout_decoded) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: list struct nested list layout was not "
          "decoded");
    }
  }
  return {};
}

sanitize::Status
validate_map_repetition_layout(const RowGroupInfo &row_group,
                               const NativeParquetOutputField &field) {
  if (!field.is_map || field.column_indices.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid map output layout");
  }
  const auto first_index = field.column_indices.front();
  if (first_index >= row_group.columns.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: map column index is invalid");
  }
  const auto &first = row_group.columns[first_index];
  if (!(is_top_level_map_leaf(first) || is_top_level_map_struct_leaf(first) ||
        top_level_map_struct_list_chain_depth(first) > 0 ||
        is_top_level_map_list_leaf(first) ||
        top_level_map_list_chain_depth(first) > 1) ||
      !first.repeated_level_layout_decoded) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: map layout was not decoded");
  }
  for (const auto column_index : field.column_indices) {
    if (column_index >= row_group.columns.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: map column index is invalid");
    }
    const auto &column = row_group.columns[column_index];
    if (!(is_top_level_map_leaf(column) ||
          is_top_level_map_struct_leaf(column) ||
          top_level_map_struct_list_chain_depth(column) > 0 ||
          is_top_level_map_list_leaf(column) ||
          top_level_map_list_chain_depth(column) > 1) ||
        !column.repeated_level_layout_decoded ||
        column.repeated_level_row_count != first.repeated_level_row_count ||
        column.repeated_level_null_count != first.repeated_level_null_count ||
        column.repeated_level_element_count !=
            first.repeated_level_element_count ||
        column.repeated_level_offsets != first.repeated_level_offsets ||
        column.repeated_level_validity_bitmap !=
            first.repeated_level_validity_bitmap) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: map leaf repetition layouts differ");
    }
    if ((is_top_level_map_list_leaf(column) ||
         top_level_map_struct_list_chain_depth(column) > 0 ||
         top_level_map_list_chain_depth(column) > 1) &&
        !column.nested_repeated_level_layout_decoded) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: map nested list layout was not decoded");
    }
  }
  return {};
}

sanitize::Status
validate_list_map_repetition_layout(const RowGroupInfo &row_group,
                                    const NativeParquetOutputField &field) {
  if (!field.is_list || !field.is_list_map || field.column_indices.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list map output layout");
  }
  const auto first_index = field.column_indices.front();
  if (first_index >= row_group.columns.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list map column index is invalid");
  }
  const auto &first = row_group.columns[first_index];
  if (!(is_top_level_list_map_leaf(first) ||
        is_top_level_list_map_struct_leaf(first) ||
        top_level_list_map_struct_list_chain_depth(first) > 0) ||
      !first.repeated_level_layout_decoded ||
      !first.nested_repeated_level_layout_decoded) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: list map layout was not decoded");
  }
  for (const auto column_index : field.column_indices) {
    if (column_index >= row_group.columns.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list map column index is invalid");
    }
    const auto &column = row_group.columns[column_index];
    if (!(is_top_level_list_map_leaf(column) ||
          is_top_level_list_map_struct_leaf(column) ||
          top_level_list_map_struct_list_chain_depth(column) > 0) ||
        !column.repeated_level_layout_decoded ||
        !column.nested_repeated_level_layout_decoded ||
        column.repeated_level_row_count != first.repeated_level_row_count ||
        column.repeated_level_null_count != first.repeated_level_null_count ||
        column.repeated_level_element_count !=
            first.repeated_level_element_count ||
        column.repeated_level_offsets != first.repeated_level_offsets ||
        column.repeated_level_validity_bitmap !=
            first.repeated_level_validity_bitmap ||
        column.nested_repeated_level_row_count !=
            first.nested_repeated_level_row_count ||
        column.nested_repeated_level_null_count !=
            first.nested_repeated_level_null_count ||
        column.nested_repeated_level_element_count !=
            first.nested_repeated_level_element_count ||
        column.nested_repeated_level_offsets !=
            first.nested_repeated_level_offsets ||
        column.nested_repeated_level_validity_bitmap !=
            first.nested_repeated_level_validity_bitmap) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: list map leaf repetition layouts differ");
    }
  }
  return {};
}

sanitize::Result<std::int64_t>
materialize_list_struct_validity(const ColumnChunkInfo &column,
                                 std::vector<std::uint8_t> *validity) {
  if (!validity || !column.repeated_level_layout_decoded ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list struct validity layout");
  }
  const auto nested_list_chain_depth =
      top_level_list_struct_list_chain_depth(column);
  const bool use_outer_list_entry_records = nested_list_chain_depth > 1;
  const auto element_count = use_outer_list_entry_records
                                 ? column.repeated_level_element_count
                                 : list_leaf_value_count(column);
  const auto validity_bytes = (element_count + 7) / 8;
  if (static_cast<std::uint64_t>(validity_bytes) > kMaxValidityBitmapBytes) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list struct validity bitmap exceeds memory "
        "limit");
  }
  validity->assign(static_cast<std::size_t>(validity_bytes), 0);
  const auto list_defined_level =
      use_outer_list_entry_records
          ? (column.top_level_required ? std::int16_t{0} : std::int16_t{1})
          : list_leaf_value_parent_defined_level(column);
  const auto struct_defined_level =
      static_cast<std::int16_t>(list_defined_level + 1);
  std::int64_t null_count = 0;
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values) ||
        (use_outer_list_entry_records &&
         page.decoded_repetition_level_values.size() !=
             static_cast<std::size_t>(page.num_values))) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list struct definition level count "
          "mismatch");
    }
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (use_outer_list_entry_records) {
        const auto repetition =
            page.decoded_repetition_level_values[static_cast<std::size_t>(row)];
        if (repetition > 1) {
          continue;
        }
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list struct validity exceeds element "
            "count");
      }
      if (definition > struct_defined_level) {
        set_output_validity_bit(validity,
                                static_cast<std::int32_t>(element_index));
      } else {
        ++null_count;
      }
      ++element_index;
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list struct validity element count mismatch");
  }
  if (null_count == 0) {
    validity->clear();
  }
  return null_count;
}

sanitize::Result<std::int64_t>
materialize_map_value_struct_validity(const ColumnChunkInfo &column,
                                      std::vector<std::uint8_t> *validity) {
  if (!validity || !column.repeated_level_layout_decoded ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid map value struct validity layout");
  }
  const auto element_count = column.repeated_level_element_count;
  const auto validity_bytes = (element_count + 7) / 8;
  if (static_cast<std::uint64_t>(validity_bytes) > kMaxValidityBitmapBytes) {
    return sanitize::Status::Invalid(
        "native Parquet reader: map value struct validity bitmap exceeds "
        "memory limit");
  }
  validity->assign(static_cast<std::size_t>(validity_bytes), 0);
  const auto map_defined_level =
      column.top_level_required ? std::int16_t{0} : std::int16_t{1};
  const auto value_struct_defined_level =
      static_cast<std::int16_t>(map_defined_level + 1);
  std::int64_t null_count = 0;
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (page.decoded_definition_level_values.size() !=
        static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: map value struct definition level count "
          "mismatch");
    }
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= map_defined_level) {
        continue;
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: map value struct validity exceeds element "
            "count");
      }
      if (definition > value_struct_defined_level) {
        set_output_validity_bit(validity,
                                static_cast<std::int32_t>(element_index));
      } else {
        ++null_count;
      }
      ++element_index;
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: map value struct validity element count "
        "mismatch");
  }
  if (null_count == 0) {
    validity->clear();
  }
  return null_count;
}

sanitize::Result<std::int64_t> materialize_list_map_value_struct_validity(
    const ColumnChunkInfo &column, std::vector<std::uint8_t> *validity) {
  if (!validity || !column.nested_repeated_level_layout_decoded ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list map value struct validity layout");
  }
  const auto element_count = column.nested_repeated_level_element_count;
  const auto validity_bytes = (element_count + 7) / 8;
  if (static_cast<std::uint64_t>(validity_bytes) > kMaxValidityBitmapBytes) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list map value struct validity bitmap exceeds "
        "memory limit");
  }
  validity->assign(static_cast<std::size_t>(validity_bytes), 0);
  const auto list_defined_level =
      column.top_level_required ? std::int16_t{0} : std::int16_t{1};
  const auto entry_defined_level =
      static_cast<std::int16_t>(list_defined_level + 2);
  const auto value_struct_defined_level =
      static_cast<std::int16_t>(entry_defined_level + 1);
  std::int64_t null_count = 0;
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (page.decoded_definition_level_values.size() !=
        static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list map value struct definition level count "
          "mismatch");
    }
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= entry_defined_level) {
        continue;
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list map value struct validity exceeds "
            "element count");
      }
      if (definition > value_struct_defined_level) {
        set_output_validity_bit(validity,
                                static_cast<std::int32_t>(element_index));
      } else {
        ++null_count;
      }
      ++element_index;
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list map value struct validity element count "
        "mismatch");
  }
  if (null_count == 0) {
    validity->clear();
  }
  return null_count;
}

sanitize::Status validate_native_plain_column(const ColumnChunkInfo &column) {
  if (!native_plain_path_is_materializable(column.path_in_schema,
                                           column.max_repetition_level,
                                           column.top_level_required) ||
      !column.native_read_plan_decoded) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: column is not materializable");
  }
  if (!column.has_physical_type) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: unsupported physical type");
  }
  if (column.native_read_value_buffer_kind != "fixed_width" &&
      column.native_read_value_buffer_kind != "plain_byte_array" &&
      column.native_read_value_buffer_kind != "dictionary_byte_array" &&
      column.native_read_value_buffer_kind != "dictionary_fixed_width" &&
      column.native_read_value_buffer_kind != "delta_binary_packed" &&
      column.native_read_value_buffer_kind != "delta_length_byte_array" &&
      column.native_read_value_buffer_kind != "byte_stream_split" &&
      column.native_read_value_buffer_kind != "bit_packed_boolean") {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: unsupported value encoding");
  }
  if (column.native_read_value_buffer_kind == "fixed_width" &&
      column.native_read_value_width_bytes <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width value width is invalid");
  }
  if (column.native_read_value_buffer_kind == "delta_binary_packed" &&
      (column.native_read_value_width_bytes <= 0 ||
       (column.physical_type != kPhysicalInt32 &&
        column.physical_type != kPhysicalInt64))) {
    return sanitize::Status::Invalid(
        "native Parquet reader: DELTA_BINARY_PACKED integer width is invalid");
  }
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: missing page value encoding");
    }
    if (column.native_read_value_buffer_kind == "dictionary_byte_array") {
      if (column.decoded_dictionary_values.empty() ||
          page.value_encoding != kEncodingRleDictionary) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported dictionary page");
      }
      continue;
    }
    if (column.native_read_value_buffer_kind == "dictionary_fixed_width") {
      if (column.decoded_dictionary_fixed_width_values.empty() ||
          page.value_encoding != kEncodingRleDictionary ||
          column.native_read_value_width_bytes <= 0) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported fixed-width dictionary page");
      }
      continue;
    }
    if (column.native_read_value_buffer_kind == "delta_binary_packed") {
      if (page.value_encoding != kEncodingDeltaBinaryPacked) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported DELTA_BINARY_PACKED page");
      }
      continue;
    }
    if (column.native_read_value_buffer_kind == "delta_length_byte_array") {
      if (column.physical_type != kPhysicalByteArray ||
          page.value_encoding != kEncodingDeltaLengthByteArray) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported DELTA_LENGTH_BYTE_ARRAY page");
      }
      continue;
    }
    if (column.native_read_value_buffer_kind == "byte_stream_split") {
      if ((column.physical_type != kPhysicalFloat &&
           column.physical_type != kPhysicalDouble) ||
          page.value_encoding != kEncodingByteStreamSplit ||
          column.native_read_value_width_bytes <= 0) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported BYTE_STREAM_SPLIT page");
      }
      continue;
    }
    if (column.native_read_value_buffer_kind == "bit_packed_boolean") {
      if (column.physical_type != kPhysicalBoolean ||
          page.value_encoding != kEncodingPlain) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported boolean page");
      }
      continue;
    }
    if (page.value_encoding != kEncodingPlain) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: only PLAIN data pages are materialized");
    }
  }
  return {};
}

sanitize::Status materialize_fixed_width_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid fixed-width row count");
  }
  const auto arrow_width = column.native_read_value_width_bytes;
  const auto physical_width = fixed_width_for_plain_values(column);
  if (!physical_width || *physical_width <= 0 || arrow_width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width value width is invalid");
  }
  const auto value_bytes = static_cast<std::uint64_t>(row_count) *
                           static_cast<std::uint64_t>(arrow_width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width value buffer is too large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid page span index");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid value payload offset");
    }
    const std::string_view values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::size_t value_offset = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: page row span exceeds row group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (values.size() - value_offset <
            static_cast<std::size_t>(*physical_width)) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated fixed-width payload");
        }
        auto *target =
            out->values.data() + static_cast<std::size_t>(global_row) *
                                     static_cast<std::size_t>(arrow_width);
        SAN_RETURN_NOT_OK(copy_fixed_width_physical_to_arrow(
            target, values.data() + value_offset, column, *physical_width,
            arrow_width));
        value_offset += static_cast<std::size_t>(*physical_width);
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
    }
    if (value_offset != values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing fixed-width payload bytes");
    }
  }
  return {};
}

sanitize::Status materialize_boolean_column(std::ifstream &file,
                                            const ColumnChunkInfo &column,
                                            std::int64_t row_count,
                                            NativeParquetPageScratch *scratch,
                                            NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid boolean row count");
  }
  if (column.physical_type != kPhysicalBoolean) {
    return sanitize::Status::Invalid(
        "native Parquet reader: boolean materialization requires boolean type");
  }
  SAN_ASSIGN_OR_RAISE(
      const auto value_buffer_bytes,
      arrow_boolean_value_buffer_bytes(static_cast<std::int32_t>(row_count)));
  out->values.assign(static_cast<std::size_t>(value_buffer_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid boolean page span index");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (!page.has_value_encoding || page.value_encoding != kEncodingPlain) {
      return sanitize::Status::Invalid(
          "native Parquet reader: expected PLAIN boolean data page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid boolean payload offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    const auto expected_bytes =
        static_cast<std::size_t>((span.non_null_count + 7) / 8);
    if (values.size() != expected_bytes) {
      return sanitize::Status::Invalid(
          "native Parquet reader: boolean payload size mismatch");
    }
    std::int32_t value_index = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: boolean row span exceeds row group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (value_index >= span.non_null_count) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing boolean value");
        }
        if (bit_stream_value_is_set(values, value_index)) {
          set_output_validity_bit(&out->values, global_row);
        }
        ++value_index;
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
    }
    if (value_index != span.non_null_count) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing boolean values");
    }
  }
  return {};
}

sanitize::Status materialize_delta_binary_packed_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid DELTA_BINARY_PACKED row count");
  }
  const auto width = column.native_read_value_width_bytes;
  const auto physical_width = fixed_width_for_plain_values(column);
  if ((column.physical_type != kPhysicalInt32 &&
       column.physical_type != kPhysicalInt64) ||
      !physical_width || *physical_width <= 0 || width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: unsupported DELTA_BINARY_PACKED physical type");
  }
  const auto value_bytes =
      static_cast<std::uint64_t>(row_count) * static_cast<std::uint64_t>(width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: DELTA_BINARY_PACKED value buffer is too large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid DELTA_BINARY_PACKED page span index");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingDeltaBinaryPacked) {
      return sanitize::Status::Invalid(
          "native Parquet reader: expected DELTA_BINARY_PACKED data page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid DELTA_BINARY_PACKED payload offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::vector<std::int64_t> decoded_values;
    decoded_values.reserve(static_cast<std::size_t>(span.non_null_count));
    SAN_ASSIGN_OR_RAISE(
        const auto consumed,
        decode_delta_binary_packed_stream(
            values, span.non_null_count,
            [&](std::int64_t value) -> sanitize::Status {
              if (column.physical_type == kPhysicalInt32 &&
                  (value < std::numeric_limits<std::int32_t>::min() ||
                   value > std::numeric_limits<std::int32_t>::max())) {
                return sanitize::Status::Invalid(
                    "native Parquet reader: DELTA_BINARY_PACKED int32 out of "
                    "range");
              }
              decoded_values.push_back(value);
              return {};
            }));
    if (consumed != values.size() ||
        decoded_values.size() !=
            static_cast<std::size_t>(span.non_null_count)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: DELTA_BINARY_PACKED decoded count mismatch");
    }
    std::size_t value_offset = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: DELTA_BINARY_PACKED row span exceeds row "
            "group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (value_offset >= decoded_values.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing DELTA_BINARY_PACKED value");
        }
        const auto target =
            out->values.data() + static_cast<std::size_t>(global_row) *
                                     static_cast<std::size_t>(width);
        SAN_RETURN_NOT_OK(write_arrow_integer_value(
            target, column, decoded_values[value_offset]));
        ++value_offset;
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
    }
    if (value_offset != decoded_values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing DELTA_BINARY_PACKED values");
    }
  }
  return {};
}

sanitize::Status materialize_simple_list_fixed_width_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || !column.repeated_level_layout_decoded ||
      column.native_read_value_buffer_kind != "fixed_width" ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list fixed-width layout");
  }
  const auto element_count = list_leaf_value_count(column);
  const auto arrow_width = column.native_read_value_width_bytes;
  const auto physical_width = fixed_width_for_plain_values(column);
  if (!physical_width || *physical_width <= 0 || arrow_width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list fixed-width value width is invalid");
  }
  const auto value_bytes = static_cast<std::uint64_t>(element_count) *
                           static_cast<std::uint64_t>(arrow_width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list fixed-width value buffer is too large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((element_count + 7) / 8), 0);
  }

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding || page.value_encoding != kEncodingPlain ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list fixed-width page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list fixed-width payload offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::size_t value_offset = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list fixed-width element span exceeds row "
            "group");
      }
      if (definition == column.max_definition_level) {
        if (values.size() - value_offset <
            static_cast<std::size_t>(*physical_width)) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated list fixed-width payload");
        }
        auto *target =
            out->values.data() + static_cast<std::size_t>(element_index) *
                                     static_cast<std::size_t>(arrow_width);
        SAN_RETURN_NOT_OK(copy_fixed_width_physical_to_arrow(
            target, values.data() + value_offset, column, *physical_width,
            arrow_width));
        value_offset += static_cast<std::size_t>(*physical_width);
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, element_index);
        }
      }
      ++element_index;
    }
    if (value_offset != values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list fixed-width payload bytes");
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list fixed-width element count mismatch");
  }
  return {};
}

sanitize::Status materialize_simple_list_boolean_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || !column.repeated_level_layout_decoded ||
      column.native_read_value_buffer_kind != "bit_packed_boolean" ||
      column.physical_type != kPhysicalBoolean ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list boolean layout");
  }
  const auto element_count = list_leaf_value_count(column);
  SAN_ASSIGN_OR_RAISE(const auto value_buffer_bytes,
                      arrow_boolean_value_buffer_bytes(
                          static_cast<std::int32_t>(element_count)));
  out->values.assign(static_cast<std::size_t>(value_buffer_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((element_count + 7) / 8), 0);
  }

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding || page.value_encoding != kEncodingPlain ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list boolean page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list boolean payload offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    const auto expected_bytes =
        static_cast<std::size_t>((page.decoded_non_null_values + 7) / 8);
    if (values.size() != expected_bytes) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list boolean payload size mismatch");
    }
    std::int32_t value_index = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list boolean element span exceeds row "
            "group");
      }
      if (definition == column.max_definition_level) {
        if (value_index >= page.decoded_non_null_values) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing list boolean value");
        }
        if (bit_stream_value_is_set(values, value_index)) {
          set_output_validity_bit(&out->values, element_index);
        }
        ++value_index;
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, element_index);
        }
      }
      ++element_index;
    }
    if (value_index != page.decoded_non_null_values) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list boolean values");
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list boolean element count mismatch");
  }
  return {};
}

sanitize::Status materialize_simple_list_byte_stream_split_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || !column.repeated_level_layout_decoded ||
      column.native_read_value_buffer_kind != "byte_stream_split" ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list BYTE_STREAM_SPLIT layout");
  }
  const auto element_count = list_leaf_value_count(column);
  const auto width = column.native_read_value_width_bytes;
  if ((column.physical_type != kPhysicalFloat &&
       column.physical_type != kPhysicalDouble) ||
      (width != 4 && width != 8)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: unsupported list BYTE_STREAM_SPLIT type");
  }
  const auto value_bytes = static_cast<std::uint64_t>(element_count) *
                           static_cast<std::uint64_t>(width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list BYTE_STREAM_SPLIT buffer is too large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((element_count + 7) / 8), 0);
  }

  const auto list_defined_level =
      column.top_level_required ? std::int16_t{0} : std::int16_t{1};
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingByteStreamSplit ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list BYTE_STREAM_SPLIT page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list BYTE_STREAM_SPLIT offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    const auto expected_bytes =
        static_cast<std::uint64_t>(page.decoded_non_null_values) *
        static_cast<std::uint64_t>(width);
    if (expected_bytes > static_cast<std::uint64_t>(
                             std::numeric_limits<std::size_t>::max()) ||
        values.size() != static_cast<std::size_t>(expected_bytes)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list BYTE_STREAM_SPLIT payload size "
          "mismatch");
    }
    std::int32_t value_index = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list BYTE_STREAM_SPLIT element span "
            "exceeds row group");
      }
      if (definition == column.max_definition_level) {
        if (value_index >= page.decoded_non_null_values) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing list BYTE_STREAM_SPLIT value");
        }
        auto *target =
            out->values.data() + static_cast<std::size_t>(element_index) *
                                     static_cast<std::size_t>(width);
        for (std::int32_t byte_index = 0; byte_index < width; ++byte_index) {
          const auto source_offset =
              static_cast<std::size_t>(byte_index) *
                  static_cast<std::size_t>(page.decoded_non_null_values) +
              static_cast<std::size_t>(value_index);
          target[static_cast<std::size_t>(byte_index)] =
              static_cast<std::uint8_t>(values[source_offset]);
        }
        ++value_index;
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, element_index);
        }
      }
      ++element_index;
    }
    if (value_index != page.decoded_non_null_values) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list BYTE_STREAM_SPLIT values");
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list BYTE_STREAM_SPLIT element count mismatch");
  }
  return {};
}

sanitize::Status materialize_simple_list_delta_binary_packed_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || !column.repeated_level_layout_decoded ||
      column.native_read_value_buffer_kind != "delta_binary_packed" ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list DELTA_BINARY_PACKED layout");
  }
  const auto row_count = list_leaf_value_count(column);
  const auto width = column.native_read_value_width_bytes;
  if ((column.physical_type != kPhysicalInt32 &&
       column.physical_type != kPhysicalInt64) ||
      width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: unsupported list DELTA_BINARY_PACKED type");
  }
  const auto value_bytes =
      static_cast<std::uint64_t>(row_count) * static_cast<std::uint64_t>(width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list DELTA_BINARY_PACKED buffer is too large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingDeltaBinaryPacked ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list DELTA_BINARY_PACKED page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list DELTA_BINARY_PACKED offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::vector<std::int64_t> decoded_values;
    decoded_values.reserve(
        static_cast<std::size_t>(page.decoded_non_null_values));
    SAN_ASSIGN_OR_RAISE(
        const auto consumed,
        decode_delta_binary_packed_stream(
            values, page.decoded_non_null_values,
            [&](std::int64_t value) -> sanitize::Status {
              if (column.physical_type == kPhysicalInt32 &&
                  (value < std::numeric_limits<std::int32_t>::min() ||
                   value > std::numeric_limits<std::int32_t>::max())) {
                return sanitize::Status::Invalid(
                    "native Parquet reader: list DELTA_BINARY_PACKED int32 out "
                    "of range");
              }
              decoded_values.push_back(value);
              return {};
            }));
    if (consumed != values.size() ||
        decoded_values.size() !=
            static_cast<std::size_t>(page.decoded_non_null_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list DELTA_BINARY_PACKED decoded count "
          "mismatch");
    }
    std::size_t value_index = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= row_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list element span exceeds row group");
      }
      if (definition == column.max_definition_level) {
        if (value_index >= decoded_values.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing list DELTA_BINARY_PACKED value");
        }
        const auto target =
            out->values.data() + static_cast<std::size_t>(element_index) *
                                     static_cast<std::size_t>(width);
        SAN_RETURN_NOT_OK(write_arrow_integer_value(
            target, column, decoded_values[value_index]));
        ++value_index;
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, element_index);
        }
      }
      ++element_index;
    }
    if (value_index != decoded_values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list DELTA_BINARY_PACKED values");
    }
  }
  if (element_index != row_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list element count mismatch");
  }
  return {};
}

sanitize::Status materialize_simple_list_delta_length_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || !column.repeated_level_layout_decoded ||
      column.native_read_value_buffer_kind != "delta_length_byte_array" ||
      column.physical_type != kPhysicalByteArray ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list DELTA_LENGTH_BYTE_ARRAY layout");
  }
  const auto element_count = list_leaf_value_count(column);
  out->offsets.assign(static_cast<std::size_t>(element_count + 1), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((element_count + 7) / 8), 0);
  }
  if (column.native_read_materialized_value_bytes < 0 ||
      static_cast<std::uint64_t>(column.native_read_materialized_value_bytes) >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list DELTA_LENGTH_BYTE_ARRAY buffer is too "
        "large");
  }
  out->values.reserve(
      static_cast<std::size_t>(column.native_read_materialized_value_bytes));

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  std::int32_t current_offset = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingDeltaLengthByteArray ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list DELTA_LENGTH_BYTE_ARRAY page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list DELTA_LENGTH_BYTE_ARRAY offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::vector<std::int32_t> lengths;
    lengths.reserve(static_cast<std::size_t>(page.decoded_non_null_values));
    std::uint64_t page_value_bytes = 0;
    SAN_ASSIGN_OR_RAISE(
        const auto lengths_bytes,
        decode_delta_binary_packed_stream(
            values, page.decoded_non_null_values,
            [&](std::int64_t length) -> sanitize::Status {
              if (length < 0 ||
                  length > std::numeric_limits<std::int32_t>::max()) {
                return sanitize::Status::Invalid(
                    "native Parquet reader: list DELTA_LENGTH_BYTE_ARRAY "
                    "invalid length");
              }
              const auto size = static_cast<std::uint64_t>(length);
              if (page_value_bytes >
                  std::numeric_limits<std::uint64_t>::max() - size) {
                return sanitize::Status::Invalid(
                    "native Parquet reader: list DELTA_LENGTH_BYTE_ARRAY "
                    "length overflow");
              }
              page_value_bytes += size;
              lengths.push_back(static_cast<std::int32_t>(length));
              return {};
            }));
    if (lengths.size() !=
            static_cast<std::size_t>(page.decoded_non_null_values) ||
        lengths_bytes > values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list DELTA_LENGTH_BYTE_ARRAY length count "
          "mismatch");
    }
    const auto bytes = values.substr(lengths_bytes);
    if (page_value_bytes != static_cast<std::uint64_t>(bytes.size())) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list DELTA_LENGTH_BYTE_ARRAY payload "
          "mismatch");
    }

    std::size_t length_offset = 0;
    std::size_t byte_offset = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list byte-array element span exceeds row "
            "group");
      }
      if (definition == column.max_definition_level) {
        if (length_offset >= lengths.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing list DELTA_LENGTH_BYTE_ARRAY "
              "length");
        }
        const auto size = static_cast<std::size_t>(lengths[length_offset++]);
        if (bytes.size() - byte_offset < size) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated list DELTA_LENGTH_BYTE_ARRAY "
              "payload");
        }
        if (size > static_cast<std::size_t>(
                       std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(size)) {
          return sanitize::Status::Invalid(
              "native Parquet reader: list DELTA_LENGTH_BYTE_ARRAY offsets "
              "exceed int32");
        }
        out->values.insert(out->values.end(), bytes.data() + byte_offset,
                           bytes.data() + byte_offset + size);
        byte_offset += size;
        current_offset += static_cast<std::int32_t>(size);
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, element_index);
        }
      }
      out->offsets[static_cast<std::size_t>(element_index + 1)] =
          current_offset;
      ++element_index;
    }
    if (length_offset != lengths.size() || byte_offset != bytes.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list DELTA_LENGTH_BYTE_ARRAY "
          "values");
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list byte-array element count mismatch");
  }
  return {};
}

sanitize::Status materialize_simple_list_plain_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || !column.repeated_level_layout_decoded ||
      column.native_read_value_buffer_kind != "plain_byte_array" ||
      column.physical_type != kPhysicalByteArray ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list PLAIN BYTE_ARRAY layout");
  }
  const auto element_count = list_leaf_value_count(column);
  out->offsets.assign(static_cast<std::size_t>(element_count + 1), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((element_count + 7) / 8), 0);
  }
  if (column.native_read_materialized_value_bytes < 0 ||
      static_cast<std::uint64_t>(column.native_read_materialized_value_bytes) >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list PLAIN BYTE_ARRAY buffer is too large");
  }
  out->values.reserve(
      static_cast<std::size_t>(column.native_read_materialized_value_bytes));

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  std::int32_t current_offset = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding || page.value_encoding != kEncodingPlain ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list PLAIN BYTE_ARRAY page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list PLAIN BYTE_ARRAY offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::size_t value_offset = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list PLAIN BYTE_ARRAY element span "
            "exceeds row group");
      }
      if (definition == column.max_definition_level) {
        if (values.size() - value_offset < 4) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated list PLAIN BYTE_ARRAY length");
        }
        const auto size =
            static_cast<std::size_t>(read_u32_le(values.data() + value_offset));
        value_offset += 4;
        if (values.size() - value_offset < size) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated list PLAIN BYTE_ARRAY "
              "payload");
        }
        if (size > static_cast<std::size_t>(
                       std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(size)) {
          return sanitize::Status::Invalid(
              "native Parquet reader: list PLAIN BYTE_ARRAY offsets exceed "
              "int32");
        }
        out->values.insert(out->values.end(), values.data() + value_offset,
                           values.data() + value_offset + size);
        value_offset += size;
        current_offset += static_cast<std::int32_t>(size);
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, element_index);
        }
      }
      out->offsets[static_cast<std::size_t>(element_index + 1)] =
          current_offset;
      ++element_index;
    }
    if (value_offset != values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list PLAIN BYTE_ARRAY values");
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list PLAIN BYTE_ARRAY element count mismatch");
  }
  return {};
}

sanitize::Status materialize_simple_list_dictionary_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || !column.repeated_level_layout_decoded ||
      column.native_read_value_buffer_kind != "dictionary_byte_array" ||
      column.physical_type != kPhysicalByteArray ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list dictionary byte-array layout");
  }
  if (column.decoded_dictionary_values.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: missing list dictionary byte-array values");
  }
  const auto element_count = list_leaf_value_count(column);
  out->offsets.assign(static_cast<std::size_t>(element_count + 1), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((element_count + 7) / 8), 0);
  }
  if (column.native_read_materialized_value_bytes < 0 ||
      static_cast<std::uint64_t>(column.native_read_materialized_value_bytes) >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary byte-array buffer is too "
        "large");
  }
  out->values.reserve(
      static_cast<std::size_t>(column.native_read_materialized_value_bytes));

  DictionaryPageState dictionary;
  dictionary.decoded = true;
  dictionary.value_count =
      static_cast<std::int32_t>(column.decoded_dictionary_values.size());
  dictionary.byte_array_values = column.decoded_dictionary_values;

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  std::int32_t current_offset = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingRleDictionary ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list dictionary byte-array page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list dictionary byte-array offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::vector<std::uint32_t> indices;
    std::int32_t index_bit_width = 0;
    SAN_ASSIGN_OR_RAISE(const auto decoded_indices,
                        decode_rle_dictionary_indices(
                            values, dictionary, page.decoded_non_null_values,
                            nullptr, &indices, &index_bit_width));
    if (decoded_indices != page.decoded_non_null_values ||
        static_cast<std::size_t>(decoded_indices) != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list dictionary byte-array index count "
          "mismatch");
    }
    std::size_t index_offset = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list dictionary byte-array element span "
            "exceeds row group");
      }
      if (definition == column.max_definition_level) {
        if (index_offset >= indices.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing list dictionary byte-array "
              "index");
        }
        const auto dictionary_index = indices[index_offset++];
        if (dictionary_index >= column.decoded_dictionary_values.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: list dictionary byte-array index out of "
              "range");
        }
        const auto &value =
            column.decoded_dictionary_values[static_cast<std::size_t>(
                dictionary_index)];
        if (value.size() > static_cast<std::size_t>(
                               std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(value.size())) {
          return sanitize::Status::Invalid(
              "native Parquet reader: list dictionary byte-array offsets "
              "exceed int32");
        }
        out->values.insert(out->values.end(), value.begin(), value.end());
        current_offset += static_cast<std::int32_t>(value.size());
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, element_index);
        }
      }
      out->offsets[static_cast<std::size_t>(element_index + 1)] =
          current_offset;
      ++element_index;
    }
    if (index_offset != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list dictionary byte-array indices");
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary byte-array element count "
        "mismatch");
  }
  return {};
}

sanitize::Status materialize_simple_list_dictionary_fixed_width_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || !column.repeated_level_layout_decoded ||
      column.native_read_value_buffer_kind != "dictionary_fixed_width" ||
      !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list dictionary fixed-width layout");
  }
  const auto element_count = list_leaf_value_count(column);
  const auto arrow_width = column.native_read_value_width_bytes;
  const auto physical_width = fixed_width_for_plain_values(column);
  if (!physical_width || *physical_width <= 0 || arrow_width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary fixed-width width is invalid");
  }
  if (column.decoded_dictionary_fixed_width_values.empty() ||
      column.decoded_dictionary_fixed_width_values.size() %
              static_cast<std::size_t>(*physical_width) !=
          0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary fixed-width payload is "
        "invalid");
  }
  const auto dictionary_value_count =
      column.decoded_dictionary_fixed_width_values.size() /
      static_cast<std::size_t>(*physical_width);
  const auto value_bytes = static_cast<std::uint64_t>(element_count) *
                           static_cast<std::uint64_t>(arrow_width);
  if (dictionary_value_count >
          static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max()) ||
      value_bytes >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary fixed-width buffer is too "
        "large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((element_count + 7) / 8), 0);
  }

  DictionaryPageState dictionary;
  dictionary.decoded = true;
  dictionary.value_count = static_cast<std::int32_t>(dictionary_value_count);
  dictionary.fixed_width_values = column.decoded_dictionary_fixed_width_values;

  const auto list_defined_level = list_leaf_value_parent_defined_level(column);
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingRleDictionary ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list dictionary fixed-width page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid list dictionary fixed-width offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::vector<std::uint32_t> indices;
    std::int32_t index_bit_width = 0;
    SAN_ASSIGN_OR_RAISE(const auto decoded_indices,
                        decode_rle_dictionary_indices(
                            values, dictionary, page.decoded_non_null_values,
                            nullptr, &indices, &index_bit_width));
    if (decoded_indices != page.decoded_non_null_values ||
        static_cast<std::size_t>(decoded_indices) != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: list dictionary fixed-width index count "
          "mismatch");
    }
    std::size_t index_offset = 0;
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level) {
        continue;
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: list dictionary fixed-width element span "
            "exceeds row group");
      }
      if (definition == column.max_definition_level) {
        if (index_offset >= indices.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing list dictionary fixed-width "
              "index");
        }
        const auto dictionary_index = indices[index_offset++];
        if (dictionary_index >= dictionary_value_count) {
          return sanitize::Status::Invalid(
              "native Parquet reader: list dictionary fixed-width index out "
              "of range");
        }
        const auto source_offset = static_cast<std::size_t>(dictionary_index) *
                                   static_cast<std::size_t>(*physical_width);
        auto *target =
            out->values.data() + static_cast<std::size_t>(element_index) *
                                     static_cast<std::size_t>(arrow_width);
        SAN_RETURN_NOT_OK(copy_fixed_width_physical_to_arrow(
            target,
            reinterpret_cast<const char *>(
                column.decoded_dictionary_fixed_width_values.data() +
                source_offset),
            column, *physical_width, arrow_width));
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, element_index);
        }
      }
      ++element_index;
    }
    if (index_offset != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing list dictionary fixed-width "
          "indices");
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: list dictionary fixed-width element count "
        "mismatch");
  }
  return {};
}

sanitize::Status materialize_byte_stream_split_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid BYTE_STREAM_SPLIT row count");
  }
  const auto width = column.native_read_value_width_bytes;
  if ((column.physical_type != kPhysicalFloat &&
       column.physical_type != kPhysicalDouble) ||
      (width != 4 && width != 8)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: unsupported BYTE_STREAM_SPLIT physical type");
  }
  const auto value_bytes =
      static_cast<std::uint64_t>(row_count) * static_cast<std::uint64_t>(width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: BYTE_STREAM_SPLIT value buffer is too large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid BYTE_STREAM_SPLIT page span index");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingByteStreamSplit) {
      return sanitize::Status::Invalid(
          "native Parquet reader: expected BYTE_STREAM_SPLIT data page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid BYTE_STREAM_SPLIT payload offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    const auto expected_bytes =
        static_cast<std::uint64_t>(span.non_null_count) *
        static_cast<std::uint64_t>(width);
    if (expected_bytes > static_cast<std::uint64_t>(
                             std::numeric_limits<std::size_t>::max()) ||
        values.size() != static_cast<std::size_t>(expected_bytes)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: BYTE_STREAM_SPLIT payload size mismatch");
    }
    std::int32_t value_index = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid("native Parquet reader: "
                                         "BYTE_STREAM_SPLIT row span exceeds "
                                         "row group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (value_index >= span.non_null_count) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing BYTE_STREAM_SPLIT value");
        }
        auto *target =
            out->values.data() + static_cast<std::size_t>(global_row) *
                                     static_cast<std::size_t>(width);
        for (std::int32_t byte_index = 0; byte_index < width; ++byte_index) {
          const auto source_offset =
              static_cast<std::size_t>(byte_index) *
                  static_cast<std::size_t>(span.non_null_count) +
              static_cast<std::size_t>(value_index);
          target[static_cast<std::size_t>(byte_index)] =
              static_cast<std::uint8_t>(values[source_offset]);
        }
        ++value_index;
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
    }
    if (value_index != span.non_null_count) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing BYTE_STREAM_SPLIT values");
    }
  }
  return {};
}

sanitize::Status materialize_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid byte-array row count");
  }
  out->offsets.assign(static_cast<std::size_t>(row_count + 1), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }
  if (column.native_read_materialized_value_bytes < 0 ||
      static_cast<std::uint64_t>(column.native_read_materialized_value_bytes) >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: byte-array value buffer is too large");
  }
  out->values.reserve(
      static_cast<std::size_t>(column.native_read_materialized_value_bytes));
  std::int32_t current_offset = 0;
  std::int64_t next_expected_row = 0;
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid page span index");
    }
    if (span.first_row_index != next_expected_row) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: non-contiguous byte-array page spans");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid value payload offset");
    }
    const std::string_view values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::size_t value_offset = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (values.size() - value_offset < 4) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated BYTE_ARRAY length");
        }
        const auto size =
            static_cast<std::size_t>(read_u32_le(values.data() + value_offset));
        value_offset += 4;
        if (values.size() - value_offset < size) {
          return sanitize::Status::Invalid(
              "native Parquet reader: truncated BYTE_ARRAY payload");
        }
        if (size > static_cast<std::size_t>(
                       std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(size)) {
          return sanitize::Status::Invalid(
              "native Parquet reader: BYTE_ARRAY buffer exceeds int32 offsets");
        }
        out->values.insert(out->values.end(), values.data() + value_offset,
                           values.data() + value_offset + size);
        value_offset += size;
        current_offset += static_cast<std::int32_t>(size);
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
      out->offsets[static_cast<std::size_t>(global_row + 1)] = current_offset;
    }
    if (value_offset != values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing BYTE_ARRAY payload bytes");
    }
    next_expected_row += span.row_count;
  }
  return {};
}

sanitize::Status materialize_delta_length_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid DELTA_LENGTH_BYTE_ARRAY row count");
  }
  if (column.physical_type != kPhysicalByteArray) {
    return sanitize::Status::Invalid(
        "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY requires BYTE_ARRAY");
  }
  out->offsets.assign(static_cast<std::size_t>(row_count + 1), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }
  if (column.native_read_materialized_value_bytes < 0 ||
      static_cast<std::uint64_t>(column.native_read_materialized_value_bytes) >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY buffer is too large");
  }
  out->values.reserve(
      static_cast<std::size_t>(column.native_read_materialized_value_bytes));

  std::int32_t current_offset = 0;
  std::int64_t next_expected_row = 0;
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid DELTA_LENGTH_BYTE_ARRAY page span "
          "index");
    }
    if (span.first_row_index != next_expected_row) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: non-contiguous DELTA_LENGTH_BYTE_ARRAY page "
          "spans");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingDeltaLengthByteArray) {
      return sanitize::Status::Invalid(
          "native Parquet reader: expected DELTA_LENGTH_BYTE_ARRAY data page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid DELTA_LENGTH_BYTE_ARRAY payload "
          "offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::vector<std::int32_t> lengths;
    lengths.reserve(static_cast<std::size_t>(span.non_null_count));
    std::uint64_t page_value_bytes = 0;
    SAN_ASSIGN_OR_RAISE(
        const auto lengths_bytes,
        decode_delta_binary_packed_stream(
            values, span.non_null_count,
            [&](std::int64_t length) -> sanitize::Status {
              if (length < 0 ||
                  length > std::numeric_limits<std::int32_t>::max()) {
                return sanitize::Status::Invalid(
                    "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY invalid "
                    "length");
              }
              const auto size = static_cast<std::uint64_t>(length);
              if (page_value_bytes >
                  std::numeric_limits<std::uint64_t>::max() - size) {
                return sanitize::Status::Invalid(
                    "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY length "
                    "overflow");
              }
              page_value_bytes += size;
              lengths.push_back(static_cast<std::int32_t>(length));
              return {};
            }));
    if (lengths.size() != static_cast<std::size_t>(span.non_null_count) ||
        lengths_bytes > values.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY length count "
          "mismatch");
    }
    const auto bytes = values.substr(lengths_bytes);
    if (page_value_bytes != static_cast<std::uint64_t>(bytes.size())) {
      return sanitize::Status::Invalid(
          "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY byte payload "
          "mismatch");
    }

    std::size_t length_offset = 0;
    std::size_t byte_offset = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid("native Parquet reader: "
                                         "DELTA_LENGTH_BYTE_ARRAY row span "
                                         "exceeds row group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (length_offset >= lengths.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing DELTA_LENGTH_BYTE_ARRAY length");
        }
        const auto size = static_cast<std::size_t>(lengths[length_offset++]);
        if (bytes.size() - byte_offset < size) {
          return sanitize::Status::Invalid("native Parquet reader: truncated "
                                           "DELTA_LENGTH_BYTE_ARRAY payload");
        }
        if (size > static_cast<std::size_t>(
                       std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(size)) {
          return sanitize::Status::Invalid(
              "native Parquet reader: DELTA_LENGTH_BYTE_ARRAY offsets exceed "
              "int32");
        }
        out->values.insert(out->values.end(), bytes.data() + byte_offset,
                           bytes.data() + byte_offset + size);
        byte_offset += size;
        current_offset += static_cast<std::int32_t>(size);
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
      out->offsets[static_cast<std::size_t>(global_row + 1)] = current_offset;
    }
    if (length_offset != lengths.size() || byte_offset != bytes.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing DELTA_LENGTH_BYTE_ARRAY values");
    }
    next_expected_row += span.row_count;
  }
  return {};
}

sanitize::Status materialize_dictionary_byte_array_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid dictionary byte-array row count");
  }
  if (column.decoded_dictionary_values.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: missing decoded dictionary values");
  }
  out->offsets.assign(static_cast<std::size_t>(row_count + 1), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }
  if (column.native_read_materialized_value_bytes < 0 ||
      static_cast<std::uint64_t>(column.native_read_materialized_value_bytes) >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: dictionary byte-array buffer is too large");
  }
  out->values.reserve(
      static_cast<std::size_t>(column.native_read_materialized_value_bytes));

  DictionaryPageState dictionary;
  dictionary.decoded = true;
  dictionary.value_count =
      static_cast<std::int32_t>(column.decoded_dictionary_values.size());
  dictionary.byte_array_values = column.decoded_dictionary_values;

  std::int32_t current_offset = 0;
  std::int64_t next_expected_row = 0;
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid dictionary page span index");
    }
    if (span.first_row_index != next_expected_row) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: non-contiguous dictionary page spans");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingRleDictionary) {
      return sanitize::Status::Invalid(
          "native Parquet reader: expected RLE dictionary data page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid dictionary value payload offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::vector<std::uint32_t> indices;
    std::int32_t index_bit_width = 0;
    SAN_ASSIGN_OR_RAISE(
        const auto decoded_indices,
        decode_rle_dictionary_indices(values, dictionary, span.non_null_count,
                                      nullptr, &indices, &index_bit_width));
    if (decoded_indices != span.non_null_count ||
        static_cast<std::size_t>(decoded_indices) != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: dictionary index count mismatch");
    }
    std::size_t index_offset = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid("native Parquet reader: dictionary "
                                         "page row span exceeds row group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (index_offset >= indices.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing dictionary index");
        }
        const auto dictionary_index = indices[index_offset++];
        if (dictionary_index >= column.decoded_dictionary_values.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: dictionary index out of range");
        }
        const auto &value =
            column.decoded_dictionary_values[static_cast<std::size_t>(
                dictionary_index)];
        if (value.size() > static_cast<std::size_t>(
                               std::numeric_limits<std::int32_t>::max()) ||
            current_offset > std::numeric_limits<std::int32_t>::max() -
                                 static_cast<std::int32_t>(value.size())) {
          return sanitize::Status::Invalid("native Parquet reader: dictionary "
                                           "byte-array offsets exceed int32");
        }
        out->values.insert(out->values.end(), value.begin(), value.end());
        current_offset += static_cast<std::int32_t>(value.size());
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
      out->offsets[static_cast<std::size_t>(global_row + 1)] = current_offset;
    }
    if (index_offset != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing dictionary indices");
    }
    next_expected_row += span.row_count;
  }
  return {};
}

sanitize::Status materialize_dictionary_fixed_width_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *scratch, NativeParquetChildArray *out) {
  if (!out || row_count < 0 ||
      row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid fixed-width dictionary row count");
  }
  const auto arrow_width = column.native_read_value_width_bytes;
  const auto physical_width = fixed_width_for_plain_values(column);
  if (!physical_width || *physical_width <= 0 || arrow_width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width dictionary width is invalid");
  }
  if (column.decoded_dictionary_fixed_width_values.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: missing fixed-width dictionary values");
  }
  if (column.decoded_dictionary_fixed_width_values.size() %
          static_cast<std::size_t>(*physical_width) !=
      0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width dictionary payload is misaligned");
  }
  const auto dictionary_value_count =
      column.decoded_dictionary_fixed_width_values.size() /
      static_cast<std::size_t>(*physical_width);
  if (dictionary_value_count >
      static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width dictionary is too large");
  }
  const auto value_bytes = static_cast<std::uint64_t>(row_count) *
                           static_cast<std::uint64_t>(arrow_width);
  if (value_bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width dictionary value buffer is too "
        "large");
  }
  out->values.assign(static_cast<std::size_t>(value_bytes), 0);
  if (column.native_read_total_nulls > 0) {
    out->validity.assign(static_cast<std::size_t>((row_count + 7) / 8), 0);
  }

  DictionaryPageState dictionary;
  dictionary.decoded = true;
  dictionary.value_count = static_cast<std::int32_t>(dictionary_value_count);
  dictionary.fixed_width_values = column.decoded_dictionary_fixed_width_values;

  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid fixed-width dictionary page span "
          "index");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (!page.has_value_encoding ||
        page.value_encoding != kEncodingRleDictionary) {
      return sanitize::Status::Invalid(
          "native Parquet reader: expected RLE fixed-width dictionary data "
          "page");
    }
    std::string_view payload;
    SAN_RETURN_NOT_OK(
        materialization_payload(file, column, page, scratch, &payload));
    if (page.value_payload_offset < 0 ||
        static_cast<std::size_t>(page.value_payload_offset) > payload.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid fixed-width dictionary value payload "
          "offset");
    }
    const auto values = std::string_view(payload).substr(
        static_cast<std::size_t>(page.value_payload_offset));
    std::vector<std::uint32_t> indices;
    std::int32_t index_bit_width = 0;
    SAN_ASSIGN_OR_RAISE(
        const auto decoded_indices,
        decode_rle_dictionary_indices(values, dictionary, span.non_null_count,
                                      nullptr, &indices, &index_bit_width));
    if (decoded_indices != span.non_null_count ||
        static_cast<std::size_t>(decoded_indices) != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: fixed-width dictionary index count mismatch");
    }
    std::size_t index_offset = 0;
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: fixed-width dictionary page row span "
            "exceeds row group");
      }
      const bool valid = validity_bit_is_set(page.decoded_validity_bitmap, row);
      if (valid) {
        if (index_offset >= indices.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: missing fixed-width dictionary index");
        }
        const auto dictionary_index = indices[index_offset++];
        if (dictionary_index >= dictionary_value_count) {
          return sanitize::Status::Invalid(
              "native Parquet reader: fixed-width dictionary index out of "
              "range");
        }
        const auto source_offset = static_cast<std::size_t>(dictionary_index) *
                                   static_cast<std::size_t>(*physical_width);
        auto *target =
            out->values.data() + static_cast<std::size_t>(global_row) *
                                     static_cast<std::size_t>(arrow_width);
        SAN_RETURN_NOT_OK(copy_fixed_width_physical_to_arrow(
            target,
            reinterpret_cast<const char *>(
                column.decoded_dictionary_fixed_width_values.data() +
                source_offset),
            column, *physical_width, arrow_width));
        if (!out->validity.empty()) {
          set_output_validity_bit(&out->validity, global_row);
        }
      }
    }
    if (index_offset != indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: trailing fixed-width dictionary indices");
    }
  }
  return {};
}

sanitize::Status build_native_schema(const FooterInfo &footer,
                                     ArrowSchema *out) {
  if (!out) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output schema is null");
  }
  auto state = std::unique_ptr<NativeParquetSchemaState>(
      new (std::nothrow) NativeParquetSchemaState());
  if (!state) {
    return sanitize::Status::OutOfMemory("native Parquet reader schema OOM");
  }
  const RowGroupInfo *row_group =
      footer.row_groups.empty() ? nullptr : &footer.row_groups.front();

  std::vector<LeafLevelInfo> empty_file_leaves;
  if (!row_group) {
    SAN_ASSIGN_OR_RAISE(empty_file_leaves,
                        schema_leaf_levels(footer.schema_elements));
    std::vector<LeafLevelInfo> projected_empty_file_leaves;
    SAN_RETURN_NOT_OK(project_leaf_levels_for_columns(
        empty_file_leaves, footer.projected_columns,
        &projected_empty_file_leaves));
    empty_file_leaves = std::move(projected_empty_file_leaves);
  }

  std::vector<NativeParquetOutputField> layout;
  if (row_group) {
    for (const auto &column : row_group->columns) {
      SAN_RETURN_NOT_OK(validate_native_plain_column(column));
    }
    SAN_RETURN_NOT_OK(build_native_output_layout(row_group->columns, &layout));
  } else {
    for (const auto &leaf : empty_file_leaves) {
      if (!native_plain_path_is_materializable(
              leaf.path, leaf.max_repetition_level, leaf.top_level_required) ||
          leaf.native_arrow_format.empty()) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: empty file schema is not materializable");
      }
    }
    SAN_RETURN_NOT_OK(build_native_output_layout(empty_file_leaves, &layout));
  }

  state->fields.resize(layout.size());
  state->children.reserve(layout.size());
  for (std::size_t field_index = 0; field_index < layout.size();
       ++field_index) {
    const auto &field = layout[field_index];
    auto &top_level = state->fields[field_index];
    top_level.is_struct = field.is_struct;
    top_level.is_list = field.is_list;
    top_level.is_map = field.is_map;
    if (field.is_map) {
      auto &map_node = top_level.map_node;
      map_node.name = field.name;
      auto &entries = map_node.entries;
      entries.name = "entries";
      entries.children.resize(field.column_indices.size());
      const auto nested_list_child_count = std::accumulate(
          field.column_indices.begin(), field.column_indices.end(),
          std::int16_t{0}, [&](std::int16_t total, std::size_t column_index) {
            if (row_group) {
              return static_cast<std::int16_t>(
                  total + (column_index < row_group->columns.size()
                               ? top_level_map_list_chain_depth(
                                     row_group->columns[column_index])
                               : 0));
            }
            return static_cast<std::int16_t>(
                total +
                (column_index < empty_file_leaves.size()
                     ? top_level_map_list_chain_depth_path(
                           empty_file_leaves[column_index].path,
                           empty_file_leaves[column_index].max_repetition_level)
                     : 0));
          });
      entries.list_children.reserve(
          static_cast<std::size_t>(nested_list_child_count));
      entries.struct_children.reserve(1);
      entries.child_ptrs.reserve(field.column_indices.size());
      bool map_value_struct_added = false;
      for (std::size_t child_index = 0;
           child_index < field.column_indices.size(); ++child_index) {
        const auto column_index = field.column_indices[child_index];
        std::string name;
        std::string format;
        std::int16_t max_definition_level = 0;
        bool top_level_required = true;
        std::int16_t child_list_depth = 0;
        if (row_group) {
          const auto &column = row_group->columns[column_index];
          name = column.path_in_schema[2];
          format = column.native_arrow_format;
          max_definition_level = column.max_definition_level;
          top_level_required = column.top_level_required;
          child_list_depth = top_level_map_list_chain_depth(column);
        } else {
          const auto &leaf = empty_file_leaves[column_index];
          name = leaf.path[2];
          format = leaf.native_arrow_format;
          max_definition_level = leaf.max_definition_level;
          top_level_required = leaf.top_level_required;
          child_list_depth = top_level_map_list_chain_depth_path(
              leaf.path, leaf.max_repetition_level);
        }
        const auto &path = row_group
                               ? row_group->columns[column_index].path_in_schema
                               : empty_file_leaves[column_index].path;
        const auto max_repetition_level =
            row_group ? row_group->columns[column_index].max_repetition_level
                      : empty_file_leaves[column_index].max_repetition_level;
        const auto map_struct_list_depth =
            top_level_map_struct_list_chain_depth_path(path,
                                                       max_repetition_level);
        if (is_top_level_map_struct_leaf_path(path, max_repetition_level) ||
            map_struct_list_depth > 0) {
          if (map_value_struct_added) {
            continue;
          }
          map_value_struct_added = true;
          std::vector<std::size_t> struct_column_indices;
          for (const auto candidate_index : field.column_indices) {
            const auto &candidate_path =
                row_group ? row_group->columns[candidate_index].path_in_schema
                          : empty_file_leaves[candidate_index].path;
            const auto candidate_repetition_level =
                row_group
                    ? row_group->columns[candidate_index].max_repetition_level
                    : empty_file_leaves[candidate_index].max_repetition_level;
            if (is_top_level_map_struct_leaf_path(candidate_path,
                                                  candidate_repetition_level) ||
                top_level_map_struct_list_chain_depth_path(
                    candidate_path, candidate_repetition_level) > 0) {
              struct_column_indices.push_back(candidate_index);
            }
          }
          auto &value_struct = entries.struct_children.emplace_back();
          value_struct.name = "value";
          value_struct.children.resize(struct_column_indices.size());
          const auto nested_struct_list_child_count = std::accumulate(
              struct_column_indices.begin(), struct_column_indices.end(),
              std::int16_t{0},
              [&](std::int16_t total, std::size_t struct_column_index) {
                if (row_group) {
                  return static_cast<std::int16_t>(
                      total + top_level_map_struct_list_chain_depth(
                                  row_group->columns[struct_column_index]));
                }
                return static_cast<std::int16_t>(
                    total + top_level_map_struct_list_chain_depth_path(
                                empty_file_leaves[struct_column_index].path,
                                empty_file_leaves[struct_column_index]
                                    .max_repetition_level));
              });
          value_struct.list_children.reserve(
              static_cast<std::size_t>(nested_struct_list_child_count));
          value_struct.child_ptrs.reserve(struct_column_indices.size());
          for (std::size_t struct_child_index = 0;
               struct_child_index < struct_column_indices.size();
               ++struct_child_index) {
            const auto struct_column_index =
                struct_column_indices[struct_child_index];
            std::string struct_leaf_name;
            std::string struct_leaf_format;
            std::int16_t struct_leaf_max_definition_level = 0;
            bool struct_leaf_top_level_required = true;
            std::int16_t struct_leaf_list_depth = 0;
            if (row_group) {
              const auto &struct_column =
                  row_group->columns[struct_column_index];
              struct_leaf_name = struct_column.path_in_schema[3];
              struct_leaf_format = struct_column.native_arrow_format;
              struct_leaf_max_definition_level =
                  struct_column.max_definition_level;
              struct_leaf_top_level_required = struct_column.top_level_required;
              struct_leaf_list_depth =
                  top_level_map_struct_list_chain_depth(struct_column);
            } else {
              const auto &struct_leaf = empty_file_leaves[struct_column_index];
              struct_leaf_name = struct_leaf.path[3];
              struct_leaf_format = struct_leaf.native_arrow_format;
              struct_leaf_max_definition_level =
                  struct_leaf.max_definition_level;
              struct_leaf_top_level_required = struct_leaf.top_level_required;
              struct_leaf_list_depth =
                  top_level_map_struct_list_chain_depth_path(
                      struct_leaf.path, struct_leaf.max_repetition_level);
            }
            if (struct_leaf_list_depth > 0) {
              const auto first_list_index = value_struct.list_children.size();
              for (std::int16_t level = 0; level < struct_leaf_list_depth;
                   ++level) {
                auto &list_child = value_struct.list_children.emplace_back();
                list_child.name = level == 0 ? struct_leaf_name : "item";
              }
              auto &leaf_list = value_struct.list_children.back();
              leaf_list.child.name = "item";
              leaf_list.child.format = std::move(struct_leaf_format);
              sanitize::internal::cdata_stream::clear_schema(
                  &leaf_list.child.schema);
              leaf_list.child.schema.format = leaf_list.child.format.c_str();
              leaf_list.child.schema.name = leaf_list.child.name.c_str();
              leaf_list.child.schema.metadata = nullptr;
              const auto inner_list_defined_level = static_cast<std::int16_t>(
                  (struct_leaf_top_level_required ? std::int16_t{0}
                                                  : std::int16_t{1}) +
                  3 + (struct_leaf_list_depth - 1) * 2);
              leaf_list.child.schema.flags =
                  struct_leaf_max_definition_level >
                          inner_list_defined_level + 1
                      ? ARROW_FLAG_NULLABLE
                      : 0;
              leaf_list.child.schema.n_children = 0;
              leaf_list.child.schema.children = nullptr;
              leaf_list.child.schema.dictionary = nullptr;
              leaf_list.child.schema.private_data = nullptr;
              leaf_list.child.schema.release =
                  &native_parquet_schema_child_release;
              leaf_list.child_ptrs[0] = &leaf_list.child.schema;
              for (std::size_t reverse_index =
                       first_list_index +
                       static_cast<std::size_t>(struct_leaf_list_depth);
                   reverse_index > first_list_index; --reverse_index) {
                const auto list_index = reverse_index - 1;
                auto &list_child = value_struct.list_children[list_index];
                if (list_index + 1 <
                    first_list_index +
                        static_cast<std::size_t>(struct_leaf_list_depth)) {
                  list_child.child_ptrs[0] =
                      &value_struct.list_children[list_index + 1].schema;
                }
                sanitize::internal::cdata_stream::clear_schema(
                    &list_child.schema);
                list_child.schema.format = list_child.format.c_str();
                list_child.schema.name = list_child.name.c_str();
                list_child.schema.metadata = nullptr;
                list_child.schema.flags = ARROW_FLAG_NULLABLE;
                list_child.schema.n_children = 1;
                list_child.schema.children = list_child.child_ptrs.data();
                list_child.schema.dictionary = nullptr;
                list_child.schema.private_data = nullptr;
                list_child.schema.release =
                    &native_parquet_schema_child_release;
              }
              value_struct.child_ptrs.push_back(
                  &value_struct.list_children[first_list_index].schema);
              continue;
            }
            auto &struct_leaf_child = value_struct.children[struct_child_index];
            struct_leaf_child.name = std::move(struct_leaf_name);
            struct_leaf_child.format = std::move(struct_leaf_format);
            sanitize::internal::cdata_stream::clear_schema(
                &struct_leaf_child.schema);
            struct_leaf_child.schema.format = struct_leaf_child.format.c_str();
            struct_leaf_child.schema.name = struct_leaf_child.name.c_str();
            struct_leaf_child.schema.metadata = nullptr;
            const auto value_struct_defined_level =
                struct_leaf_top_level_required ? std::int16_t{2}
                                               : std::int16_t{3};
            struct_leaf_child.schema.flags =
                struct_leaf_max_definition_level > value_struct_defined_level
                    ? ARROW_FLAG_NULLABLE
                    : 0;
            struct_leaf_child.schema.n_children = 0;
            struct_leaf_child.schema.children = nullptr;
            struct_leaf_child.schema.dictionary = nullptr;
            struct_leaf_child.schema.private_data = nullptr;
            struct_leaf_child.schema.release =
                &native_parquet_schema_child_release;
            value_struct.child_ptrs.push_back(&struct_leaf_child.schema);
          }
          sanitize::internal::cdata_stream::clear_schema(&value_struct.schema);
          value_struct.schema.format = value_struct.format.c_str();
          value_struct.schema.name = value_struct.name.c_str();
          value_struct.schema.metadata = nullptr;
          value_struct.schema.flags = ARROW_FLAG_NULLABLE;
          value_struct.schema.n_children =
              static_cast<std::int64_t>(value_struct.child_ptrs.size());
          value_struct.schema.children = value_struct.child_ptrs.empty()
                                             ? nullptr
                                             : value_struct.child_ptrs.data();
          value_struct.schema.dictionary = nullptr;
          value_struct.schema.private_data = nullptr;
          value_struct.schema.release = &native_parquet_schema_child_release;
          entries.child_ptrs.push_back(&value_struct.schema);
          continue;
        }
        if (child_list_depth > 0) {
          const auto first_list_index = entries.list_children.size();
          for (std::int16_t level = 0; level < child_list_depth; ++level) {
            auto &list_child = entries.list_children.emplace_back();
            list_child.name = level == 0 ? name : "item";
          }
          auto &leaf_list = entries.list_children.back();
          leaf_list.child.name = "item";
          leaf_list.child.format = std::move(format);
          sanitize::internal::cdata_stream::clear_schema(
              &leaf_list.child.schema);
          leaf_list.child.schema.format = leaf_list.child.format.c_str();
          leaf_list.child.schema.name = leaf_list.child.name.c_str();
          leaf_list.child.schema.metadata = nullptr;
          const auto inner_list_defined_level = static_cast<std::int16_t>(
              (top_level_required ? std::int16_t{0} : std::int16_t{1}) + 2 +
              (child_list_depth - 1) * 2);
          leaf_list.child.schema.flags =
              max_definition_level > inner_list_defined_level + 1
                  ? ARROW_FLAG_NULLABLE
                  : 0;
          leaf_list.child.schema.n_children = 0;
          leaf_list.child.schema.children = nullptr;
          leaf_list.child.schema.dictionary = nullptr;
          leaf_list.child.schema.private_data = nullptr;
          leaf_list.child.schema.release = &native_parquet_schema_child_release;
          leaf_list.child_ptrs[0] = &leaf_list.child.schema;
          for (std::size_t reverse_index =
                   first_list_index +
                   static_cast<std::size_t>(child_list_depth);
               reverse_index > first_list_index; --reverse_index) {
            const auto list_index = reverse_index - 1;
            auto &list_child = entries.list_children[list_index];
            if (list_index + 1 <
                first_list_index + static_cast<std::size_t>(child_list_depth)) {
              list_child.child_ptrs[0] =
                  &entries.list_children[list_index + 1].schema;
            }
            sanitize::internal::cdata_stream::clear_schema(&list_child.schema);
            list_child.schema.format = list_child.format.c_str();
            list_child.schema.name = list_child.name.c_str();
            list_child.schema.metadata = nullptr;
            list_child.schema.flags = ARROW_FLAG_NULLABLE;
            list_child.schema.n_children = 1;
            list_child.schema.children = list_child.child_ptrs.data();
            list_child.schema.dictionary = nullptr;
            list_child.schema.private_data = nullptr;
            list_child.schema.release = &native_parquet_schema_child_release;
          }
          entries.child_ptrs.push_back(
              &entries.list_children[first_list_index].schema);
          continue;
        }
        auto &child = entries.children[child_index];
        child.name = std::move(name);
        child.format = std::move(format);
        sanitize::internal::cdata_stream::clear_schema(&child.schema);
        child.schema.format = child.format.c_str();
        child.schema.name = child.name.c_str();
        child.schema.metadata = nullptr;
        const auto entry_defined_level =
            top_level_required ? std::int16_t{1} : std::int16_t{2};
        child.schema.flags = max_definition_level > entry_defined_level
                                 ? ARROW_FLAG_NULLABLE
                                 : 0;
        child.schema.n_children = 0;
        child.schema.children = nullptr;
        child.schema.dictionary = nullptr;
        child.schema.private_data = nullptr;
        child.schema.release = &native_parquet_schema_child_release;
        entries.child_ptrs.push_back(&child.schema);
      }
      sanitize::internal::cdata_stream::clear_schema(&entries.schema);
      entries.schema.format = entries.format.c_str();
      entries.schema.name = entries.name.c_str();
      entries.schema.metadata = nullptr;
      entries.schema.flags = 0;
      entries.schema.n_children =
          static_cast<std::int64_t>(entries.child_ptrs.size());
      entries.schema.children =
          entries.child_ptrs.empty() ? nullptr : entries.child_ptrs.data();
      entries.schema.dictionary = nullptr;
      entries.schema.private_data = nullptr;
      entries.schema.release = &native_parquet_schema_child_release;
      map_node.child_ptrs[0] = &entries.schema;
      sanitize::internal::cdata_stream::clear_schema(&map_node.schema);
      map_node.schema.format = map_node.format.c_str();
      map_node.schema.name = map_node.name.c_str();
      map_node.schema.metadata = nullptr;
      map_node.schema.flags =
          field.top_level_required ? 0 : ARROW_FLAG_NULLABLE;
      map_node.schema.n_children = 1;
      map_node.schema.children = map_node.child_ptrs.data();
      map_node.schema.dictionary = nullptr;
      map_node.schema.private_data = nullptr;
      map_node.schema.release = &native_parquet_schema_child_release;
      state->children.push_back(&map_node.schema);
      continue;
    }
    if (!field.is_struct) {
      if (field.is_list) {
        auto &list_node = top_level.list_node;
        list_node.name = field.name;
        list_node.child_is_struct = field.is_list_struct;
        list_node.child_is_list = field.is_list_list;
        list_node.child_is_deep_list = field.is_list_list_list;
        list_node.child_is_map = field.is_list_map;
        if (field.is_list_struct) {
          auto &struct_child = list_node.struct_child;
          struct_child.name = "item";
          struct_child.children.resize(field.column_indices.size());
          const auto nested_list_child_count = std::accumulate(
              field.column_indices.begin(), field.column_indices.end(),
              std::int16_t{0},
              [&](std::int16_t total, std::size_t column_index) {
                if (row_group) {
                  return static_cast<std::int16_t>(
                      total + (column_index < row_group->columns.size()
                                   ? top_level_list_struct_list_chain_depth(
                                         row_group->columns[column_index])
                                   : 0));
                }
                return static_cast<std::int16_t>(
                    total + (column_index < empty_file_leaves.size()
                                 ? top_level_list_struct_list_chain_depth_path(
                                       empty_file_leaves[column_index].path,
                                       empty_file_leaves[column_index]
                                           .max_repetition_level)
                                 : 0));
              });
          struct_child.list_children.reserve(
              static_cast<std::size_t>(nested_list_child_count));
          list_node.struct_map_children.reserve(field.column_indices.size());
          struct_child.child_ptrs.reserve(field.column_indices.size());
          std::vector<std::string> map_child_names;
          for (std::size_t child_index = 0;
               child_index < field.column_indices.size(); ++child_index) {
            const auto column_index = field.column_indices[child_index];
            std::string name;
            std::string format;
            std::int16_t max_definition_level = 0;
            bool top_level_required = true;
            std::int16_t child_list_depth = 0;
            if (row_group) {
              const auto &column = row_group->columns[column_index];
              name = column.path_in_schema[3];
              format = column.native_arrow_format;
              max_definition_level = column.max_definition_level;
              top_level_required = column.top_level_required;
              child_list_depth = top_level_list_struct_list_chain_depth(column);
            } else {
              const auto &leaf = empty_file_leaves[column_index];
              name = leaf.path[3];
              format = leaf.native_arrow_format;
              max_definition_level = leaf.max_definition_level;
              top_level_required = leaf.top_level_required;
              child_list_depth = top_level_list_struct_list_chain_depth_path(
                  leaf.path, leaf.max_repetition_level);
            }
            const auto &path =
                row_group ? row_group->columns[column_index].path_in_schema
                          : empty_file_leaves[column_index].path;
            const auto max_repetition_level =
                row_group
                    ? row_group->columns[column_index].max_repetition_level
                    : empty_file_leaves[column_index].max_repetition_level;
            const auto map_value_list_depth =
                top_level_list_struct_map_list_chain_depth_path(
                    path, max_repetition_level);
            if (is_top_level_list_struct_map_leaf_path(path,
                                                       max_repetition_level) ||
                map_value_list_depth > 0) {
              if (std::find(map_child_names.begin(), map_child_names.end(),
                            name) != map_child_names.end()) {
                continue;
              }
              map_child_names.push_back(name);
              std::vector<std::size_t> map_column_indices;
              for (const auto candidate_index : field.column_indices) {
                const auto &candidate_path =
                    row_group
                        ? row_group->columns[candidate_index].path_in_schema
                        : empty_file_leaves[candidate_index].path;
                const auto candidate_repetition_level =
                    row_group ? row_group->columns[candidate_index]
                                    .max_repetition_level
                              : empty_file_leaves[candidate_index]
                                    .max_repetition_level;
                if ((is_top_level_list_struct_map_leaf_path(
                         candidate_path, candidate_repetition_level) ||
                     top_level_list_struct_map_list_chain_depth_path(
                         candidate_path, candidate_repetition_level) > 0) &&
                    candidate_path.size() > 5 && candidate_path[3] == name) {
                  map_column_indices.push_back(candidate_index);
                }
              }
              auto &map_child = list_node.struct_map_children.emplace_back();
              map_child.name = name;
              auto &entries = map_child.entries;
              entries.name = "entries";
              entries.children.resize(map_column_indices.size());
              const auto map_nested_list_child_count = std::accumulate(
                  map_column_indices.begin(), map_column_indices.end(),
                  std::int16_t{0},
                  [&](std::int16_t total, std::size_t map_column_index) {
                    if (row_group) {
                      return static_cast<std::int16_t>(
                          total + top_level_list_struct_map_list_chain_depth(
                                      row_group->columns[map_column_index]));
                    }
                    return static_cast<std::int16_t>(
                        total + top_level_list_struct_map_list_chain_depth_path(
                                    empty_file_leaves[map_column_index].path,
                                    empty_file_leaves[map_column_index]
                                        .max_repetition_level));
                  });
              entries.list_children.reserve(
                  static_cast<std::size_t>(map_nested_list_child_count));
              entries.child_ptrs.reserve(map_column_indices.size());
              for (std::size_t map_child_index = 0;
                   map_child_index < map_column_indices.size();
                   ++map_child_index) {
                const auto map_column_index =
                    map_column_indices[map_child_index];
                std::string map_leaf_name;
                std::string map_leaf_format;
                std::int16_t map_leaf_max_definition_level = 0;
                bool map_leaf_top_level_required = true;
                std::int16_t map_leaf_list_depth = 0;
                if (row_group) {
                  const auto &map_column = row_group->columns[map_column_index];
                  map_leaf_name = map_column.path_in_schema[5];
                  map_leaf_format = map_column.native_arrow_format;
                  map_leaf_max_definition_level =
                      map_column.max_definition_level;
                  map_leaf_top_level_required = map_column.top_level_required;
                  map_leaf_list_depth =
                      top_level_list_struct_map_list_chain_depth(map_column);
                } else {
                  const auto &map_leaf = empty_file_leaves[map_column_index];
                  map_leaf_name = map_leaf.path[5];
                  map_leaf_format = map_leaf.native_arrow_format;
                  map_leaf_max_definition_level = map_leaf.max_definition_level;
                  map_leaf_top_level_required = map_leaf.top_level_required;
                  map_leaf_list_depth =
                      top_level_list_struct_map_list_chain_depth_path(
                          map_leaf.path, map_leaf.max_repetition_level);
                }
                if (map_leaf_list_depth > 0) {
                  const auto first_list_index = entries.list_children.size();
                  for (std::int16_t level = 0; level < map_leaf_list_depth;
                       ++level) {
                    auto &list_child = entries.list_children.emplace_back();
                    list_child.name = level == 0 ? map_leaf_name : "item";
                  }
                  auto &leaf_list = entries.list_children.back();
                  leaf_list.child.name = "item";
                  leaf_list.child.format = std::move(map_leaf_format);
                  sanitize::internal::cdata_stream::clear_schema(
                      &leaf_list.child.schema);
                  leaf_list.child.schema.format =
                      leaf_list.child.format.c_str();
                  leaf_list.child.schema.name = leaf_list.child.name.c_str();
                  leaf_list.child.schema.metadata = nullptr;
                  const auto inner_list_defined_level =
                      static_cast<std::int16_t>(
                          (map_leaf_top_level_required ? std::int16_t{0}
                                                       : std::int16_t{1}) +
                          5 + (map_leaf_list_depth - 1) * 2);
                  leaf_list.child.schema.flags =
                      map_leaf_max_definition_level >
                              inner_list_defined_level + 1
                          ? ARROW_FLAG_NULLABLE
                          : 0;
                  leaf_list.child.schema.n_children = 0;
                  leaf_list.child.schema.children = nullptr;
                  leaf_list.child.schema.dictionary = nullptr;
                  leaf_list.child.schema.private_data = nullptr;
                  leaf_list.child.schema.release =
                      &native_parquet_schema_child_release;
                  leaf_list.child_ptrs[0] = &leaf_list.child.schema;
                  for (std::size_t reverse_index =
                           first_list_index +
                           static_cast<std::size_t>(map_leaf_list_depth);
                       reverse_index > first_list_index; --reverse_index) {
                    const auto list_index = reverse_index - 1;
                    auto &list_child = entries.list_children[list_index];
                    if (list_index + 1 <
                        first_list_index +
                            static_cast<std::size_t>(map_leaf_list_depth)) {
                      list_child.child_ptrs[0] =
                          &entries.list_children[list_index + 1].schema;
                    }
                    sanitize::internal::cdata_stream::clear_schema(
                        &list_child.schema);
                    list_child.schema.format = list_child.format.c_str();
                    list_child.schema.name = list_child.name.c_str();
                    list_child.schema.metadata = nullptr;
                    list_child.schema.flags = ARROW_FLAG_NULLABLE;
                    list_child.schema.n_children = 1;
                    list_child.schema.children = list_child.child_ptrs.data();
                    list_child.schema.dictionary = nullptr;
                    list_child.schema.private_data = nullptr;
                    list_child.schema.release =
                        &native_parquet_schema_child_release;
                  }
                  entries.child_ptrs.push_back(
                      &entries.list_children[first_list_index].schema);
                  continue;
                }
                auto &map_leaf_child = entries.children[map_child_index];
                map_leaf_child.name = std::move(map_leaf_name);
                map_leaf_child.format = std::move(map_leaf_format);
                sanitize::internal::cdata_stream::clear_schema(
                    &map_leaf_child.schema);
                map_leaf_child.schema.format = map_leaf_child.format.c_str();
                map_leaf_child.schema.name = map_leaf_child.name.c_str();
                map_leaf_child.schema.metadata = nullptr;
                const auto entry_defined_level = map_leaf_top_level_required
                                                     ? std::int16_t{4}
                                                     : std::int16_t{5};
                map_leaf_child.schema.flags =
                    map_leaf_max_definition_level > entry_defined_level
                        ? ARROW_FLAG_NULLABLE
                        : 0;
                map_leaf_child.schema.n_children = 0;
                map_leaf_child.schema.children = nullptr;
                map_leaf_child.schema.dictionary = nullptr;
                map_leaf_child.schema.private_data = nullptr;
                map_leaf_child.schema.release =
                    &native_parquet_schema_child_release;
                entries.child_ptrs.push_back(&map_leaf_child.schema);
              }
              sanitize::internal::cdata_stream::clear_schema(&entries.schema);
              entries.schema.format = entries.format.c_str();
              entries.schema.name = entries.name.c_str();
              entries.schema.metadata = nullptr;
              entries.schema.flags = 0;
              entries.schema.n_children =
                  static_cast<std::int64_t>(entries.child_ptrs.size());
              entries.schema.children = entries.child_ptrs.empty()
                                            ? nullptr
                                            : entries.child_ptrs.data();
              entries.schema.dictionary = nullptr;
              entries.schema.private_data = nullptr;
              entries.schema.release = &native_parquet_schema_child_release;
              map_child.child_ptrs[0] = &entries.schema;
              sanitize::internal::cdata_stream::clear_schema(&map_child.schema);
              map_child.schema.format = map_child.format.c_str();
              map_child.schema.name = map_child.name.c_str();
              map_child.schema.metadata = nullptr;
              map_child.schema.flags = ARROW_FLAG_NULLABLE;
              map_child.schema.n_children = 1;
              map_child.schema.children = map_child.child_ptrs.data();
              map_child.schema.dictionary = nullptr;
              map_child.schema.private_data = nullptr;
              map_child.schema.release = &native_parquet_schema_child_release;
              struct_child.child_ptrs.push_back(&map_child.schema);
              continue;
            }
            if (child_list_depth > 0) {
              const auto first_list_index = struct_child.list_children.size();
              for (std::int16_t level = 0; level < child_list_depth; ++level) {
                auto &list_child = struct_child.list_children.emplace_back();
                list_child.name = level == 0 ? name : "item";
              }
              auto &leaf_list = struct_child.list_children.back();
              leaf_list.child.name = "item";
              leaf_list.child.format = std::move(format);
              sanitize::internal::cdata_stream::clear_schema(
                  &leaf_list.child.schema);
              leaf_list.child.schema.format = leaf_list.child.format.c_str();
              leaf_list.child.schema.name = leaf_list.child.name.c_str();
              leaf_list.child.schema.metadata = nullptr;
              const auto inner_list_defined_level = static_cast<std::int16_t>(
                  (top_level_required ? std::int16_t{0} : std::int16_t{1}) + 3 +
                  (child_list_depth - 1) * 2);
              leaf_list.child.schema.flags =
                  max_definition_level > inner_list_defined_level + 1
                      ? ARROW_FLAG_NULLABLE
                      : 0;
              leaf_list.child.schema.n_children = 0;
              leaf_list.child.schema.children = nullptr;
              leaf_list.child.schema.dictionary = nullptr;
              leaf_list.child.schema.private_data = nullptr;
              leaf_list.child.schema.release =
                  &native_parquet_schema_child_release;
              leaf_list.child_ptrs[0] = &leaf_list.child.schema;
              for (std::size_t reverse_index =
                       first_list_index +
                       static_cast<std::size_t>(child_list_depth);
                   reverse_index > first_list_index; --reverse_index) {
                const auto list_index = reverse_index - 1;
                auto &list_child = struct_child.list_children[list_index];
                if (list_index + 1 <
                    first_list_index +
                        static_cast<std::size_t>(child_list_depth)) {
                  list_child.child_ptrs[0] =
                      &struct_child.list_children[list_index + 1].schema;
                }
                sanitize::internal::cdata_stream::clear_schema(
                    &list_child.schema);
                list_child.schema.format = list_child.format.c_str();
                list_child.schema.name = list_child.name.c_str();
                list_child.schema.metadata = nullptr;
                list_child.schema.flags = ARROW_FLAG_NULLABLE;
                list_child.schema.n_children = 1;
                list_child.schema.children = list_child.child_ptrs.data();
                list_child.schema.dictionary = nullptr;
                list_child.schema.private_data = nullptr;
                list_child.schema.release =
                    &native_parquet_schema_child_release;
              }
              struct_child.child_ptrs.push_back(
                  &struct_child.list_children[first_list_index].schema);
              continue;
            }
            auto &child = struct_child.children[child_index];
            child.name = std::move(name);
            child.format = std::move(format);
            sanitize::internal::cdata_stream::clear_schema(&child.schema);
            child.schema.format = child.format.c_str();
            child.schema.name = child.name.c_str();
            child.schema.metadata = nullptr;
            const auto struct_defined_level =
                top_level_required ? std::int16_t{2} : std::int16_t{3};
            child.schema.flags = max_definition_level > struct_defined_level
                                     ? ARROW_FLAG_NULLABLE
                                     : 0;
            child.schema.n_children = 0;
            child.schema.children = nullptr;
            child.schema.dictionary = nullptr;
            child.schema.private_data = nullptr;
            child.schema.release = &native_parquet_schema_child_release;
            struct_child.child_ptrs.push_back(&child.schema);
          }
          sanitize::internal::cdata_stream::clear_schema(&struct_child.schema);
          struct_child.schema.format = struct_child.format.c_str();
          struct_child.schema.name = struct_child.name.c_str();
          struct_child.schema.metadata = nullptr;
          struct_child.schema.flags = ARROW_FLAG_NULLABLE;
          struct_child.schema.n_children =
              static_cast<std::int64_t>(struct_child.child_ptrs.size());
          struct_child.schema.children = struct_child.child_ptrs.empty()
                                             ? nullptr
                                             : struct_child.child_ptrs.data();
          struct_child.schema.dictionary = nullptr;
          struct_child.schema.private_data = nullptr;
          struct_child.schema.release = &native_parquet_schema_child_release;
          list_node.child_ptrs[0] = &struct_child.schema;
        } else if (field.list_depth > 3) {
          const auto column_index = field.column_indices.front();
          std::string format;
          std::int16_t max_definition_level = 0;
          bool top_level_required = true;
          if (row_group) {
            const auto &column = row_group->columns[column_index];
            format = column.native_arrow_format;
            max_definition_level = column.max_definition_level;
            top_level_required = column.top_level_required;
          } else {
            const auto &leaf = empty_file_leaves[column_index];
            format = leaf.native_arrow_format;
            max_definition_level = leaf.max_definition_level;
            top_level_required = leaf.top_level_required;
          }
          list_node.chain_list_children.resize(
              static_cast<std::size_t>(field.list_depth - 1));
          auto &leaf_list = list_node.chain_list_children.back();
          leaf_list.child.name = "item";
          leaf_list.child.format = std::move(format);
          sanitize::internal::cdata_stream::clear_schema(
              &leaf_list.child.schema);
          leaf_list.child.schema.format = leaf_list.child.format.c_str();
          leaf_list.child.schema.name = leaf_list.child.name.c_str();
          leaf_list.child.schema.metadata = nullptr;
          const auto deepest_list_defined_level = static_cast<std::int16_t>(
              (top_level_required ? std::int16_t{0} : std::int16_t{1}) +
              (field.list_depth - 1) * 2);
          leaf_list.child.schema.flags =
              max_definition_level > deepest_list_defined_level + 1
                  ? ARROW_FLAG_NULLABLE
                  : 0;
          leaf_list.child.schema.n_children = 0;
          leaf_list.child.schema.children = nullptr;
          leaf_list.child.schema.dictionary = nullptr;
          leaf_list.child.schema.private_data = nullptr;
          leaf_list.child.schema.release = &native_parquet_schema_child_release;
          leaf_list.child_ptrs[0] = &leaf_list.child.schema;

          for (std::size_t reverse_index = list_node.chain_list_children.size();
               reverse_index > 0; --reverse_index) {
            const auto child_index = reverse_index - 1;
            auto &child_list = list_node.chain_list_children[child_index];
            if (child_index + 1 < list_node.chain_list_children.size()) {
              child_list.child_ptrs[0] =
                  &list_node.chain_list_children[child_index + 1].schema;
            }
            sanitize::internal::cdata_stream::clear_schema(&child_list.schema);
            child_list.schema.format = child_list.format.c_str();
            child_list.schema.name = child_list.name.c_str();
            child_list.schema.metadata = nullptr;
            child_list.schema.flags = ARROW_FLAG_NULLABLE;
            child_list.schema.n_children = 1;
            child_list.schema.children = child_list.child_ptrs.data();
            child_list.schema.dictionary = nullptr;
            child_list.schema.private_data = nullptr;
            child_list.schema.release = &native_parquet_schema_child_release;
          }
          list_node.child_ptrs[0] =
              &list_node.chain_list_children.front().schema;
        } else if (field.is_list_list_list) {
          const auto column_index = field.column_indices.front();
          std::string format;
          std::int16_t max_definition_level = 0;
          bool top_level_required = true;
          if (row_group) {
            const auto &column = row_group->columns[column_index];
            format = column.native_arrow_format;
            max_definition_level = column.max_definition_level;
            top_level_required = column.top_level_required;
          } else {
            const auto &leaf = empty_file_leaves[column_index];
            format = leaf.native_arrow_format;
            max_definition_level = leaf.max_definition_level;
            top_level_required = leaf.top_level_required;
          }
          auto &middle_list = list_node.list_child;
          auto &inner_list = list_node.deep_list_child;
          inner_list.child.name = "item";
          inner_list.child.format = std::move(format);
          sanitize::internal::cdata_stream::clear_schema(
              &inner_list.child.schema);
          inner_list.child.schema.format = inner_list.child.format.c_str();
          inner_list.child.schema.name = inner_list.child.name.c_str();
          inner_list.child.schema.metadata = nullptr;
          const auto deepest_list_defined_level =
              top_level_required ? std::int16_t{4} : std::int16_t{5};
          inner_list.child.schema.flags =
              max_definition_level > deepest_list_defined_level + 1
                  ? ARROW_FLAG_NULLABLE
                  : 0;
          inner_list.child.schema.n_children = 0;
          inner_list.child.schema.children = nullptr;
          inner_list.child.schema.dictionary = nullptr;
          inner_list.child.schema.private_data = nullptr;
          inner_list.child.schema.release =
              &native_parquet_schema_child_release;
          inner_list.child_ptrs[0] = &inner_list.child.schema;
          sanitize::internal::cdata_stream::clear_schema(&inner_list.schema);
          inner_list.schema.format = inner_list.format.c_str();
          inner_list.schema.name = inner_list.name.c_str();
          inner_list.schema.metadata = nullptr;
          inner_list.schema.flags = ARROW_FLAG_NULLABLE;
          inner_list.schema.n_children = 1;
          inner_list.schema.children = inner_list.child_ptrs.data();
          inner_list.schema.dictionary = nullptr;
          inner_list.schema.private_data = nullptr;
          inner_list.schema.release = &native_parquet_schema_child_release;

          middle_list.child_ptrs[0] = &inner_list.schema;
          sanitize::internal::cdata_stream::clear_schema(&middle_list.schema);
          middle_list.schema.format = middle_list.format.c_str();
          middle_list.schema.name = middle_list.name.c_str();
          middle_list.schema.metadata = nullptr;
          middle_list.schema.flags = ARROW_FLAG_NULLABLE;
          middle_list.schema.n_children = 1;
          middle_list.schema.children = middle_list.child_ptrs.data();
          middle_list.schema.dictionary = nullptr;
          middle_list.schema.private_data = nullptr;
          middle_list.schema.release = &native_parquet_schema_child_release;
          list_node.child_ptrs[0] = &middle_list.schema;
        } else if (field.is_list_list) {
          const auto column_index = field.column_indices.front();
          std::string format;
          std::int16_t max_definition_level = 0;
          bool top_level_required = true;
          if (row_group) {
            const auto &column = row_group->columns[column_index];
            format = column.native_arrow_format;
            max_definition_level = column.max_definition_level;
            top_level_required = column.top_level_required;
          } else {
            const auto &leaf = empty_file_leaves[column_index];
            format = leaf.native_arrow_format;
            max_definition_level = leaf.max_definition_level;
            top_level_required = leaf.top_level_required;
          }
          auto &inner_list = list_node.list_child;
          inner_list.child.name = "item";
          inner_list.child.format = std::move(format);
          sanitize::internal::cdata_stream::clear_schema(
              &inner_list.child.schema);
          inner_list.child.schema.format = inner_list.child.format.c_str();
          inner_list.child.schema.name = inner_list.child.name.c_str();
          inner_list.child.schema.metadata = nullptr;
          const auto inner_list_defined_level =
              top_level_required ? std::int16_t{2} : std::int16_t{3};
          inner_list.child.schema.flags =
              max_definition_level > inner_list_defined_level + 1
                  ? ARROW_FLAG_NULLABLE
                  : 0;
          inner_list.child.schema.n_children = 0;
          inner_list.child.schema.children = nullptr;
          inner_list.child.schema.dictionary = nullptr;
          inner_list.child.schema.private_data = nullptr;
          inner_list.child.schema.release =
              &native_parquet_schema_child_release;
          inner_list.child_ptrs[0] = &inner_list.child.schema;
          sanitize::internal::cdata_stream::clear_schema(&inner_list.schema);
          inner_list.schema.format = inner_list.format.c_str();
          inner_list.schema.name = inner_list.name.c_str();
          inner_list.schema.metadata = nullptr;
          inner_list.schema.flags = ARROW_FLAG_NULLABLE;
          inner_list.schema.n_children = 1;
          inner_list.schema.children = inner_list.child_ptrs.data();
          inner_list.schema.dictionary = nullptr;
          inner_list.schema.private_data = nullptr;
          inner_list.schema.release = &native_parquet_schema_child_release;
          list_node.child_ptrs[0] = &inner_list.schema;
        } else if (field.is_list_map) {
          auto &map_child = list_node.map_child;
          map_child.name = "item";
          auto &entries = map_child.entries;
          entries.name = "entries";
          entries.children.resize(field.column_indices.size());
          entries.struct_children.reserve(1);
          entries.child_ptrs.reserve(field.column_indices.size());
          bool map_value_struct_added = false;
          for (std::size_t child_index = 0;
               child_index < field.column_indices.size(); ++child_index) {
            const auto column_index = field.column_indices[child_index];
            std::string name;
            std::string format;
            std::int16_t max_definition_level = 0;
            bool top_level_required = true;
            if (row_group) {
              const auto &column = row_group->columns[column_index];
              name = column.path_in_schema[4];
              format = column.native_arrow_format;
              max_definition_level = column.max_definition_level;
              top_level_required = column.top_level_required;
            } else {
              const auto &leaf = empty_file_leaves[column_index];
              name = leaf.path[4];
              format = leaf.native_arrow_format;
              max_definition_level = leaf.max_definition_level;
              top_level_required = leaf.top_level_required;
            }
            const auto &path =
                row_group ? row_group->columns[column_index].path_in_schema
                          : empty_file_leaves[column_index].path;
            const auto max_repetition_level =
                row_group
                    ? row_group->columns[column_index].max_repetition_level
                    : empty_file_leaves[column_index].max_repetition_level;
            const auto list_map_struct_list_depth =
                top_level_list_map_struct_list_chain_depth_path(
                    path, max_repetition_level);
            if (is_top_level_list_map_struct_leaf_path(path,
                                                       max_repetition_level) ||
                list_map_struct_list_depth > 0) {
              if (map_value_struct_added) {
                continue;
              }
              map_value_struct_added = true;
              std::vector<std::size_t> struct_column_indices;
              for (const auto candidate_index : field.column_indices) {
                const auto &candidate_path =
                    row_group
                        ? row_group->columns[candidate_index].path_in_schema
                        : empty_file_leaves[candidate_index].path;
                const auto candidate_repetition_level =
                    row_group ? row_group->columns[candidate_index]
                                    .max_repetition_level
                              : empty_file_leaves[candidate_index]
                                    .max_repetition_level;
                if (is_top_level_list_map_struct_leaf_path(
                        candidate_path, candidate_repetition_level) ||
                    top_level_list_map_struct_list_chain_depth_path(
                        candidate_path, candidate_repetition_level) > 0) {
                  struct_column_indices.push_back(candidate_index);
                }
              }
              auto &value_struct = entries.struct_children.emplace_back();
              value_struct.name = "value";
              value_struct.children.resize(struct_column_indices.size());
              const auto nested_struct_list_child_count = std::accumulate(
                  struct_column_indices.begin(), struct_column_indices.end(),
                  std::int16_t{0},
                  [&](std::int16_t total, std::size_t struct_column_index) {
                    if (row_group) {
                      return static_cast<std::int16_t>(
                          total + top_level_list_map_struct_list_chain_depth(
                                      row_group->columns[struct_column_index]));
                    }
                    return static_cast<std::int16_t>(
                        total + top_level_list_map_struct_list_chain_depth_path(
                                    empty_file_leaves[struct_column_index].path,
                                    empty_file_leaves[struct_column_index]
                                        .max_repetition_level));
                  });
              value_struct.list_children.reserve(
                  static_cast<std::size_t>(nested_struct_list_child_count));
              value_struct.child_ptrs.reserve(struct_column_indices.size());
              for (std::size_t struct_child_index = 0;
                   struct_child_index < struct_column_indices.size();
                   ++struct_child_index) {
                const auto struct_column_index =
                    struct_column_indices[struct_child_index];
                std::string struct_leaf_name;
                std::string struct_leaf_format;
                std::int16_t struct_leaf_max_definition_level = 0;
                bool struct_leaf_top_level_required = true;
                std::int16_t struct_leaf_list_depth = 0;
                if (row_group) {
                  const auto &struct_column =
                      row_group->columns[struct_column_index];
                  struct_leaf_name = struct_column.path_in_schema[5];
                  struct_leaf_format = struct_column.native_arrow_format;
                  struct_leaf_max_definition_level =
                      struct_column.max_definition_level;
                  struct_leaf_top_level_required =
                      struct_column.top_level_required;
                  struct_leaf_list_depth =
                      top_level_list_map_struct_list_chain_depth(struct_column);
                } else {
                  const auto &struct_leaf =
                      empty_file_leaves[struct_column_index];
                  struct_leaf_name = struct_leaf.path[5];
                  struct_leaf_format = struct_leaf.native_arrow_format;
                  struct_leaf_max_definition_level =
                      struct_leaf.max_definition_level;
                  struct_leaf_top_level_required =
                      struct_leaf.top_level_required;
                  struct_leaf_list_depth =
                      top_level_list_map_struct_list_chain_depth_path(
                          struct_leaf.path, struct_leaf.max_repetition_level);
                }
                if (struct_leaf_list_depth > 0) {
                  const auto first_list_index =
                      value_struct.list_children.size();
                  for (std::int16_t level = 0; level < struct_leaf_list_depth;
                       ++level) {
                    auto &list_child =
                        value_struct.list_children.emplace_back();
                    list_child.name = level == 0 ? struct_leaf_name : "item";
                  }
                  auto &leaf_list = value_struct.list_children.back();
                  leaf_list.child.name = "item";
                  leaf_list.child.format = std::move(struct_leaf_format);
                  sanitize::internal::cdata_stream::clear_schema(
                      &leaf_list.child.schema);
                  leaf_list.child.schema.format =
                      leaf_list.child.format.c_str();
                  leaf_list.child.schema.name = leaf_list.child.name.c_str();
                  leaf_list.child.schema.metadata = nullptr;
                  const auto inner_list_defined_level =
                      static_cast<std::int16_t>(
                          (struct_leaf_top_level_required ? std::int16_t{0}
                                                          : std::int16_t{1}) +
                          5 + (struct_leaf_list_depth - 1) * 2);
                  leaf_list.child.schema.flags =
                      struct_leaf_max_definition_level >
                              inner_list_defined_level + 1
                          ? ARROW_FLAG_NULLABLE
                          : 0;
                  leaf_list.child.schema.n_children = 0;
                  leaf_list.child.schema.children = nullptr;
                  leaf_list.child.schema.dictionary = nullptr;
                  leaf_list.child.schema.private_data = nullptr;
                  leaf_list.child.schema.release =
                      &native_parquet_schema_child_release;
                  leaf_list.child_ptrs[0] = &leaf_list.child.schema;
                  for (std::size_t reverse_index =
                           first_list_index +
                           static_cast<std::size_t>(struct_leaf_list_depth);
                       reverse_index > first_list_index; --reverse_index) {
                    const auto list_index = reverse_index - 1;
                    auto &list_child = value_struct.list_children[list_index];
                    if (list_index + 1 <
                        first_list_index +
                            static_cast<std::size_t>(struct_leaf_list_depth)) {
                      list_child.child_ptrs[0] =
                          &value_struct.list_children[list_index + 1].schema;
                    }
                    sanitize::internal::cdata_stream::clear_schema(
                        &list_child.schema);
                    list_child.schema.format = list_child.format.c_str();
                    list_child.schema.name = list_child.name.c_str();
                    list_child.schema.metadata = nullptr;
                    list_child.schema.flags = ARROW_FLAG_NULLABLE;
                    list_child.schema.n_children = 1;
                    list_child.schema.children = list_child.child_ptrs.data();
                    list_child.schema.dictionary = nullptr;
                    list_child.schema.private_data = nullptr;
                    list_child.schema.release =
                        &native_parquet_schema_child_release;
                  }
                  value_struct.child_ptrs.push_back(
                      &value_struct.list_children[first_list_index].schema);
                  continue;
                }
                auto &struct_leaf_child =
                    value_struct.children[struct_child_index];
                struct_leaf_child.name = std::move(struct_leaf_name);
                struct_leaf_child.format = std::move(struct_leaf_format);
                sanitize::internal::cdata_stream::clear_schema(
                    &struct_leaf_child.schema);
                struct_leaf_child.schema.format =
                    struct_leaf_child.format.c_str();
                struct_leaf_child.schema.name = struct_leaf_child.name.c_str();
                struct_leaf_child.schema.metadata = nullptr;
                const auto value_struct_defined_level =
                    struct_leaf_top_level_required ? std::int16_t{4}
                                                   : std::int16_t{5};
                struct_leaf_child.schema.flags =
                    struct_leaf_max_definition_level >
                            value_struct_defined_level
                        ? ARROW_FLAG_NULLABLE
                        : 0;
                struct_leaf_child.schema.n_children = 0;
                struct_leaf_child.schema.children = nullptr;
                struct_leaf_child.schema.dictionary = nullptr;
                struct_leaf_child.schema.private_data = nullptr;
                struct_leaf_child.schema.release =
                    &native_parquet_schema_child_release;
                value_struct.child_ptrs.push_back(&struct_leaf_child.schema);
              }
              sanitize::internal::cdata_stream::clear_schema(
                  &value_struct.schema);
              value_struct.schema.format = value_struct.format.c_str();
              value_struct.schema.name = value_struct.name.c_str();
              value_struct.schema.metadata = nullptr;
              value_struct.schema.flags = ARROW_FLAG_NULLABLE;
              value_struct.schema.n_children =
                  static_cast<std::int64_t>(value_struct.child_ptrs.size());
              value_struct.schema.children =
                  value_struct.child_ptrs.empty()
                      ? nullptr
                      : value_struct.child_ptrs.data();
              value_struct.schema.dictionary = nullptr;
              value_struct.schema.private_data = nullptr;
              value_struct.schema.release =
                  &native_parquet_schema_child_release;
              entries.child_ptrs.push_back(&value_struct.schema);
              continue;
            }
            auto &child = entries.children[child_index];
            child.name = std::move(name);
            child.format = std::move(format);
            sanitize::internal::cdata_stream::clear_schema(&child.schema);
            child.schema.format = child.format.c_str();
            child.schema.name = child.name.c_str();
            child.schema.metadata = nullptr;
            const auto entry_defined_level =
                top_level_required ? std::int16_t{3} : std::int16_t{4};
            child.schema.flags = max_definition_level > entry_defined_level
                                     ? ARROW_FLAG_NULLABLE
                                     : 0;
            child.schema.n_children = 0;
            child.schema.children = nullptr;
            child.schema.dictionary = nullptr;
            child.schema.private_data = nullptr;
            child.schema.release = &native_parquet_schema_child_release;
            entries.child_ptrs.push_back(&child.schema);
          }
          sanitize::internal::cdata_stream::clear_schema(&entries.schema);
          entries.schema.format = entries.format.c_str();
          entries.schema.name = entries.name.c_str();
          entries.schema.metadata = nullptr;
          entries.schema.flags = 0;
          entries.schema.n_children =
              static_cast<std::int64_t>(entries.child_ptrs.size());
          entries.schema.children =
              entries.child_ptrs.empty() ? nullptr : entries.child_ptrs.data();
          entries.schema.dictionary = nullptr;
          entries.schema.private_data = nullptr;
          entries.schema.release = &native_parquet_schema_child_release;
          map_child.child_ptrs[0] = &entries.schema;
          sanitize::internal::cdata_stream::clear_schema(&map_child.schema);
          map_child.schema.format = map_child.format.c_str();
          map_child.schema.name = map_child.name.c_str();
          map_child.schema.metadata = nullptr;
          map_child.schema.flags = ARROW_FLAG_NULLABLE;
          map_child.schema.n_children = 1;
          map_child.schema.children = map_child.child_ptrs.data();
          map_child.schema.dictionary = nullptr;
          map_child.schema.private_data = nullptr;
          map_child.schema.release = &native_parquet_schema_child_release;
          list_node.child_ptrs[0] = &map_child.schema;
        } else {
          const auto column_index = field.column_indices.front();
          std::string format;
          std::int16_t max_definition_level = 0;
          bool top_level_required = true;
          if (row_group) {
            const auto &column = row_group->columns[column_index];
            format = column.native_arrow_format;
            max_definition_level = column.max_definition_level;
            top_level_required = column.top_level_required;
          } else {
            const auto &leaf = empty_file_leaves[column_index];
            format = leaf.native_arrow_format;
            max_definition_level = leaf.max_definition_level;
            top_level_required = leaf.top_level_required;
          }
          list_node.child.name = "item";
          list_node.child.format = std::move(format);
          sanitize::internal::cdata_stream::clear_schema(
              &list_node.child.schema);
          list_node.child.schema.format = list_node.child.format.c_str();
          list_node.child.schema.name = list_node.child.name.c_str();
          list_node.child.schema.metadata = nullptr;
          const auto list_defined_level =
              top_level_required ? std::int16_t{0} : std::int16_t{1};
          list_node.child.schema.flags =
              max_definition_level > list_defined_level + 1
                  ? ARROW_FLAG_NULLABLE
                  : 0;
          list_node.child.schema.n_children = 0;
          list_node.child.schema.children = nullptr;
          list_node.child.schema.dictionary = nullptr;
          list_node.child.schema.private_data = nullptr;
          list_node.child.schema.release = &native_parquet_schema_child_release;
          list_node.child_ptrs[0] = &list_node.child.schema;
        }
        sanitize::internal::cdata_stream::clear_schema(&list_node.schema);
        list_node.schema.format = list_node.format.c_str();
        list_node.schema.name = list_node.name.c_str();
        list_node.schema.metadata = nullptr;
        list_node.schema.flags =
            field.top_level_required ? 0 : ARROW_FLAG_NULLABLE;
        list_node.schema.n_children = 1;
        list_node.schema.children = list_node.child_ptrs.data();
        list_node.schema.dictionary = nullptr;
        list_node.schema.private_data = nullptr;
        list_node.schema.release = &native_parquet_schema_child_release;
        state->children.push_back(&list_node.schema);
        continue;
      }
      const auto column_index = field.column_indices.front();
      std::string format;
      std::int16_t max_definition_level = 0;
      if (row_group) {
        const auto &column = row_group->columns[column_index];
        format = column.native_arrow_format;
        max_definition_level = column.max_definition_level;
      } else {
        const auto &leaf = empty_file_leaves[column_index];
        format = leaf.native_arrow_format;
        max_definition_level = leaf.max_definition_level;
      }
      auto &child = top_level.leaf;
      child.name = field.name;
      child.format = std::move(format);
      sanitize::internal::cdata_stream::clear_schema(&child.schema);
      child.schema.format = child.format.c_str();
      child.schema.name = child.name.c_str();
      child.schema.metadata = nullptr;
      child.schema.flags = max_definition_level > 0 ? ARROW_FLAG_NULLABLE : 0;
      child.schema.n_children = 0;
      child.schema.children = nullptr;
      child.schema.dictionary = nullptr;
      child.schema.private_data = nullptr;
      child.schema.release = &native_parquet_schema_child_release;
      state->children.push_back(&child.schema);
      continue;
    }

    auto &struct_node = top_level.struct_node;
    struct_node.name = field.name;
    struct_node.children.resize(field.column_indices.size());
    top_level.struct_map_children.reserve(field.column_indices.size());
    struct_node.child_ptrs.reserve(field.column_indices.size());
    std::vector<std::string> map_child_names;
    for (std::size_t child_index = 0; child_index < field.column_indices.size();
         ++child_index) {
      const auto column_index = field.column_indices[child_index];
      const auto &path = row_group
                             ? row_group->columns[column_index].path_in_schema
                             : empty_file_leaves[column_index].path;
      const auto max_repetition_level =
          row_group ? row_group->columns[column_index].max_repetition_level
                    : empty_file_leaves[column_index].max_repetition_level;
      if (is_top_level_struct_map_leaf_path(path, max_repetition_level) ||
          top_level_struct_map_list_chain_depth_path(
              path, max_repetition_level) > 0) {
        const auto &map_name = path[1];
        if (std::find(map_child_names.begin(), map_child_names.end(),
                      map_name) != map_child_names.end()) {
          continue;
        }
        map_child_names.push_back(map_name);
        std::vector<std::size_t> map_column_indices;
        for (const auto candidate_index : field.column_indices) {
          const auto &candidate_path =
              row_group ? row_group->columns[candidate_index].path_in_schema
                        : empty_file_leaves[candidate_index].path;
          const auto candidate_repetition_level =
              row_group
                  ? row_group->columns[candidate_index].max_repetition_level
                  : empty_file_leaves[candidate_index].max_repetition_level;
          if ((is_top_level_struct_map_leaf_path(candidate_path,
                                                 candidate_repetition_level) ||
               top_level_struct_map_list_chain_depth_path(
                   candidate_path, candidate_repetition_level) > 0) &&
              candidate_path.size() > 1 && candidate_path[1] == map_name) {
            map_column_indices.push_back(candidate_index);
          }
        }
        auto &map_child = top_level.struct_map_children.emplace_back();
        map_child.name = map_name;
        auto &entries = map_child.entries;
        entries.name = "entries";
        entries.children.resize(map_column_indices.size());
        const auto nested_list_child_count = std::accumulate(
            map_column_indices.begin(), map_column_indices.end(),
            std::int16_t{0},
            [&](std::int16_t total, std::size_t map_column_index) {
              if (row_group) {
                return static_cast<std::int16_t>(
                    total + (map_column_index < row_group->columns.size()
                                 ? top_level_struct_map_list_chain_depth(
                                       row_group->columns[map_column_index])
                                 : 0));
              }
              return static_cast<std::int16_t>(
                  total + (map_column_index < empty_file_leaves.size()
                               ? top_level_struct_map_list_chain_depth_path(
                                     empty_file_leaves[map_column_index].path,
                                     empty_file_leaves[map_column_index]
                                         .max_repetition_level)
                               : 0));
            });
        entries.list_children.reserve(
            static_cast<std::size_t>(nested_list_child_count));
        entries.child_ptrs.reserve(map_column_indices.size());
        for (std::size_t map_child_index = 0;
             map_child_index < map_column_indices.size(); ++map_child_index) {
          const auto map_column_index = map_column_indices[map_child_index];
          const auto &map_path =
              row_group ? row_group->columns[map_column_index].path_in_schema
                        : empty_file_leaves[map_column_index].path;
          std::string child_name = map_path[3];
          std::string child_format;
          std::int16_t child_max_definition_level = 0;
          bool child_top_level_required = true;
          std::int16_t child_list_depth = 0;
          if (row_group) {
            const auto &map_column = row_group->columns[map_column_index];
            child_format = map_column.native_arrow_format;
            child_max_definition_level = map_column.max_definition_level;
            child_top_level_required = map_column.top_level_required;
            child_list_depth =
                top_level_struct_map_list_chain_depth(map_column);
          } else {
            const auto &map_leaf = empty_file_leaves[map_column_index];
            child_format = map_leaf.native_arrow_format;
            child_max_definition_level = map_leaf.max_definition_level;
            child_top_level_required = map_leaf.top_level_required;
            child_list_depth = top_level_struct_map_list_chain_depth_path(
                map_leaf.path, map_leaf.max_repetition_level);
          }
          if (child_list_depth > 0) {
            const auto first_list_index = entries.list_children.size();
            for (std::int16_t level = 0; level < child_list_depth; ++level) {
              auto &list_child = entries.list_children.emplace_back();
              list_child.name = level == 0 ? child_name : "item";
            }
            auto &leaf_list = entries.list_children.back();
            leaf_list.child.name = "item";
            leaf_list.child.format = std::move(child_format);
            sanitize::internal::cdata_stream::clear_schema(
                &leaf_list.child.schema);
            leaf_list.child.schema.format = leaf_list.child.format.c_str();
            leaf_list.child.schema.name = leaf_list.child.name.c_str();
            leaf_list.child.schema.metadata = nullptr;
            const auto inner_list_defined_level = static_cast<std::int16_t>(
                (child_top_level_required ? std::int16_t{1} : std::int16_t{2}) +
                2 + (child_list_depth - 1) * 2);
            leaf_list.child.schema.flags =
                child_max_definition_level > inner_list_defined_level + 1
                    ? ARROW_FLAG_NULLABLE
                    : 0;
            leaf_list.child.schema.n_children = 0;
            leaf_list.child.schema.children = nullptr;
            leaf_list.child.schema.dictionary = nullptr;
            leaf_list.child.schema.private_data = nullptr;
            leaf_list.child.schema.release =
                &native_parquet_schema_child_release;
            leaf_list.child_ptrs[0] = &leaf_list.child.schema;
            for (std::size_t reverse_index =
                     first_list_index +
                     static_cast<std::size_t>(child_list_depth);
                 reverse_index > first_list_index; --reverse_index) {
              const auto list_index = reverse_index - 1;
              auto &list_child = entries.list_children[list_index];
              if (list_index + 1 < first_list_index + static_cast<std::size_t>(
                                                          child_list_depth)) {
                list_child.child_ptrs[0] =
                    &entries.list_children[list_index + 1].schema;
              }
              sanitize::internal::cdata_stream::clear_schema(
                  &list_child.schema);
              list_child.schema.format = list_child.format.c_str();
              list_child.schema.name = list_child.name.c_str();
              list_child.schema.metadata = nullptr;
              list_child.schema.flags = ARROW_FLAG_NULLABLE;
              list_child.schema.n_children = 1;
              list_child.schema.children = list_child.child_ptrs.data();
              list_child.schema.dictionary = nullptr;
              list_child.schema.private_data = nullptr;
              list_child.schema.release = &native_parquet_schema_child_release;
            }
            entries.child_ptrs.push_back(
                &entries.list_children[first_list_index].schema);
            continue;
          }
          auto &entry_child = entries.children[map_child_index];
          entry_child.name = std::move(child_name);
          entry_child.format = std::move(child_format);
          sanitize::internal::cdata_stream::clear_schema(&entry_child.schema);
          entry_child.schema.format = entry_child.format.c_str();
          entry_child.schema.name = entry_child.name.c_str();
          entry_child.schema.metadata = nullptr;
          const auto entry_defined_level =
              child_top_level_required ? std::int16_t{2} : std::int16_t{3};
          entry_child.schema.flags =
              child_max_definition_level > entry_defined_level
                  ? ARROW_FLAG_NULLABLE
                  : 0;
          entry_child.schema.n_children = 0;
          entry_child.schema.children = nullptr;
          entry_child.schema.dictionary = nullptr;
          entry_child.schema.private_data = nullptr;
          entry_child.schema.release = &native_parquet_schema_child_release;
          entries.child_ptrs.push_back(&entry_child.schema);
        }
        sanitize::internal::cdata_stream::clear_schema(&entries.schema);
        entries.schema.format = entries.format.c_str();
        entries.schema.name = entries.name.c_str();
        entries.schema.metadata = nullptr;
        entries.schema.flags = 0;
        entries.schema.n_children =
            static_cast<std::int64_t>(entries.child_ptrs.size());
        entries.schema.children =
            entries.child_ptrs.empty() ? nullptr : entries.child_ptrs.data();
        entries.schema.dictionary = nullptr;
        entries.schema.private_data = nullptr;
        entries.schema.release = &native_parquet_schema_child_release;
        map_child.child_ptrs[0] = &entries.schema;
        sanitize::internal::cdata_stream::clear_schema(&map_child.schema);
        map_child.schema.format = map_child.format.c_str();
        map_child.schema.name = map_child.name.c_str();
        map_child.schema.metadata = nullptr;
        map_child.schema.flags = ARROW_FLAG_NULLABLE;
        map_child.schema.n_children = 1;
        map_child.schema.children = map_child.child_ptrs.data();
        map_child.schema.dictionary = nullptr;
        map_child.schema.private_data = nullptr;
        map_child.schema.release = &native_parquet_schema_child_release;
        struct_node.child_ptrs.push_back(&map_child.schema);
        continue;
      }
      std::string name;
      std::string format;
      std::int16_t max_definition_level = 0;
      if (row_group) {
        const auto &column = row_group->columns[column_index];
        name = column.path_in_schema[1];
        format = column.native_arrow_format;
        max_definition_level = column.max_definition_level;
      } else {
        const auto &leaf = empty_file_leaves[column_index];
        name = leaf.path[1];
        format = leaf.native_arrow_format;
        max_definition_level = leaf.max_definition_level;
      }
      auto &child = struct_node.children[child_index];
      child.name = std::move(name);
      child.format = std::move(format);
      sanitize::internal::cdata_stream::clear_schema(&child.schema);
      child.schema.format = child.format.c_str();
      child.schema.name = child.name.c_str();
      child.schema.metadata = nullptr;
      const auto parent_definition_level =
          field.top_level_required ? std::int16_t{0} : std::int16_t{1};
      child.schema.flags = max_definition_level > parent_definition_level
                               ? ARROW_FLAG_NULLABLE
                               : 0;
      child.schema.n_children = 0;
      child.schema.children = nullptr;
      child.schema.dictionary = nullptr;
      child.schema.private_data = nullptr;
      child.schema.release = &native_parquet_schema_child_release;
      struct_node.child_ptrs.push_back(&child.schema);
    }
    sanitize::internal::cdata_stream::clear_schema(&struct_node.schema);
    struct_node.schema.format = struct_node.format.c_str();
    struct_node.schema.name = struct_node.name.c_str();
    struct_node.schema.metadata = nullptr;
    struct_node.schema.flags =
        field.top_level_required ? 0 : ARROW_FLAG_NULLABLE;
    struct_node.schema.n_children =
        static_cast<std::int64_t>(struct_node.child_ptrs.size());
    struct_node.schema.children = struct_node.child_ptrs.empty()
                                      ? nullptr
                                      : struct_node.child_ptrs.data();
    struct_node.schema.dictionary = nullptr;
    struct_node.schema.private_data = nullptr;
    struct_node.schema.release = &native_parquet_schema_child_release;
    state->children.push_back(&struct_node.schema);
  }
  sanitize::internal::cdata_stream::clear_schema(out);
  out->format = state->root_format.c_str();
  out->name = nullptr;
  out->metadata = nullptr;
  out->flags = 0;
  out->n_children = static_cast<std::int64_t>(state->children.size());
  out->children = state->children.empty() ? nullptr : state->children.data();
  out->dictionary = nullptr;
  out->private_data = state.release();
  out->release = &native_parquet_schema_release;
  return {};
}

sanitize::Status build_native_row_group_array(NativeParquetStreamState *stream,
                                              ArrowArray *out) {
  if (!stream || !out) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output array is null");
  }
  if (stream->row_group_index >= stream->footer.row_groups.size()) {
    sanitize::internal::cdata_stream::clear_array(out);
    return {};
  }
  const auto &row_group = stream->footer.row_groups[stream->row_group_index++];
  if (!row_group.has_num_rows || row_group.num_rows < 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: row group is missing row count");
  }
  SAN_ASSIGN_OR_RAISE(const auto estimated_buffer_bytes,
                      native_reader_row_group_buffer_bytes(row_group));
  const auto max_buffer_bytes = configured_native_reader_max_buffer_bytes();
  if (estimated_buffer_bytes > max_buffer_bytes) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: row group buffer estimate ",
        estimated_buffer_bytes, " exceeds configured limit ", max_buffer_bytes);
  }
  auto state = std::unique_ptr<NativeParquetArrayState>(
      new (std::nothrow) NativeParquetArrayState());
  if (!state) {
    return sanitize::Status::OutOfMemory("native Parquet reader array OOM");
  }
  stream->file.clear();
  if (!stream->file) {
    return sanitize::Status::IOError(
        "native Parquet reader: input stream is not readable");
  }
  state->columns.resize(row_group.columns.size());
  for (std::size_t i = 0; i < row_group.columns.size(); ++i) {
    const auto &column = row_group.columns[i];
    SAN_RETURN_NOT_OK(validate_native_plain_column(column));
    auto &child = state->columns[i];
    std::int64_t arrow_length = row_group.num_rows;
    std::int64_t arrow_null_count = column.native_read_total_nulls;
    if (is_supported_top_level_list_leaf(column)) {
      if (column.native_read_value_buffer_kind == "fixed_width") {
        SAN_RETURN_NOT_OK(materialize_simple_list_fixed_width_column(
            stream->file, column, &stream->page_scratch, &child));
        child.buffers[0] =
            child.validity.empty() ? nullptr : child.validity.data();
        child.buffers[1] = child.values.empty() ? nullptr : child.values.data();
        child.array.n_buffers = 2;
      } else if (column.native_read_value_buffer_kind == "bit_packed_boolean") {
        SAN_RETURN_NOT_OK(materialize_simple_list_boolean_column(
            stream->file, column, &stream->page_scratch, &child));
        child.buffers[0] =
            child.validity.empty() ? nullptr : child.validity.data();
        child.buffers[1] = child.values.empty() ? nullptr : child.values.data();
        child.array.n_buffers = 2;
      } else if (column.native_read_value_buffer_kind == "byte_stream_split") {
        SAN_RETURN_NOT_OK(materialize_simple_list_byte_stream_split_column(
            stream->file, column, &stream->page_scratch, &child));
        child.buffers[0] =
            child.validity.empty() ? nullptr : child.validity.data();
        child.buffers[1] = child.values.empty() ? nullptr : child.values.data();
        child.array.n_buffers = 2;
      } else if (column.native_read_value_buffer_kind ==
                 "delta_binary_packed") {
        SAN_RETURN_NOT_OK(materialize_simple_list_delta_binary_packed_column(
            stream->file, column, &stream->page_scratch, &child));
        child.buffers[0] =
            child.validity.empty() ? nullptr : child.validity.data();
        child.buffers[1] = child.values.empty() ? nullptr : child.values.data();
        child.array.n_buffers = 2;
      } else if (column.native_read_value_buffer_kind ==
                 "delta_length_byte_array") {
        SAN_RETURN_NOT_OK(
            materialize_simple_list_delta_length_byte_array_column(
                stream->file, column, &stream->page_scratch, &child));
        child.buffers[0] =
            child.validity.empty() ? nullptr : child.validity.data();
        child.buffers[1] =
            child.offsets.empty() ? nullptr : child.offsets.data();
        child.buffers[2] = child.values.empty() ? nullptr : child.values.data();
        child.array.n_buffers = 3;
      } else if (column.native_read_value_buffer_kind == "plain_byte_array") {
        SAN_RETURN_NOT_OK(materialize_simple_list_plain_byte_array_column(
            stream->file, column, &stream->page_scratch, &child));
        child.buffers[0] =
            child.validity.empty() ? nullptr : child.validity.data();
        child.buffers[1] =
            child.offsets.empty() ? nullptr : child.offsets.data();
        child.buffers[2] = child.values.empty() ? nullptr : child.values.data();
        child.array.n_buffers = 3;
      } else if (column.native_read_value_buffer_kind ==
                 "dictionary_byte_array") {
        SAN_RETURN_NOT_OK(materialize_simple_list_dictionary_byte_array_column(
            stream->file, column, &stream->page_scratch, &child));
        child.buffers[0] =
            child.validity.empty() ? nullptr : child.validity.data();
        child.buffers[1] =
            child.offsets.empty() ? nullptr : child.offsets.data();
        child.buffers[2] = child.values.empty() ? nullptr : child.values.data();
        child.array.n_buffers = 3;
      } else if (column.native_read_value_buffer_kind ==
                 "dictionary_fixed_width") {
        SAN_RETURN_NOT_OK(materialize_simple_list_dictionary_fixed_width_column(
            stream->file, column, &stream->page_scratch, &child));
        child.buffers[0] =
            child.validity.empty() ? nullptr : child.validity.data();
        child.buffers[1] = child.values.empty() ? nullptr : child.values.data();
        child.array.n_buffers = 2;
      } else {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: unsupported list value buffer kind");
      }
      arrow_length = list_leaf_value_count(column);
      arrow_null_count = column.native_read_total_nulls;
    } else if (column.native_read_value_buffer_kind == "fixed_width") {
      SAN_RETURN_NOT_OK(materialize_fixed_width_column(
          stream->file, column, row_group.num_rows, &stream->page_scratch,
          &child));
      child.buffers[0] =
          child.validity.empty() ? nullptr : child.validity.data();
      child.buffers[1] = child.values.empty() ? nullptr : child.values.data();
      child.array.n_buffers = 2;
    } else if (column.native_read_value_buffer_kind == "bit_packed_boolean") {
      SAN_RETURN_NOT_OK(
          materialize_boolean_column(stream->file, column, row_group.num_rows,
                                     &stream->page_scratch, &child));
      child.buffers[0] =
          child.validity.empty() ? nullptr : child.validity.data();
      child.buffers[1] = child.values.empty() ? nullptr : child.values.data();
      child.array.n_buffers = 2;
    } else if (column.native_read_value_buffer_kind == "delta_binary_packed") {
      SAN_RETURN_NOT_OK(materialize_delta_binary_packed_column(
          stream->file, column, row_group.num_rows, &stream->page_scratch,
          &child));
      child.buffers[0] =
          child.validity.empty() ? nullptr : child.validity.data();
      child.buffers[1] = child.values.empty() ? nullptr : child.values.data();
      child.array.n_buffers = 2;
    } else if (column.native_read_value_buffer_kind == "byte_stream_split") {
      SAN_RETURN_NOT_OK(materialize_byte_stream_split_column(
          stream->file, column, row_group.num_rows, &stream->page_scratch,
          &child));
      child.buffers[0] =
          child.validity.empty() ? nullptr : child.validity.data();
      child.buffers[1] = child.values.empty() ? nullptr : child.values.data();
      child.array.n_buffers = 2;
    } else if (column.native_read_value_buffer_kind == "plain_byte_array") {
      SAN_RETURN_NOT_OK(materialize_byte_array_column(
          stream->file, column, row_group.num_rows, &stream->page_scratch,
          &child));
      child.buffers[0] =
          child.validity.empty() ? nullptr : child.validity.data();
      child.buffers[1] = child.offsets.empty() ? nullptr : child.offsets.data();
      child.buffers[2] = child.values.empty() ? nullptr : child.values.data();
      child.array.n_buffers = 3;
    } else if (column.native_read_value_buffer_kind ==
               "delta_length_byte_array") {
      SAN_RETURN_NOT_OK(materialize_delta_length_byte_array_column(
          stream->file, column, row_group.num_rows, &stream->page_scratch,
          &child));
      child.buffers[0] =
          child.validity.empty() ? nullptr : child.validity.data();
      child.buffers[1] = child.offsets.empty() ? nullptr : child.offsets.data();
      child.buffers[2] = child.values.empty() ? nullptr : child.values.data();
      child.array.n_buffers = 3;
    } else if (column.native_read_value_buffer_kind ==
               "dictionary_byte_array") {
      SAN_RETURN_NOT_OK(materialize_dictionary_byte_array_column(
          stream->file, column, row_group.num_rows, &stream->page_scratch,
          &child));
      child.buffers[0] =
          child.validity.empty() ? nullptr : child.validity.data();
      child.buffers[1] = child.offsets.empty() ? nullptr : child.offsets.data();
      child.buffers[2] = child.values.empty() ? nullptr : child.values.data();
      child.array.n_buffers = 3;
    } else if (column.native_read_value_buffer_kind ==
               "dictionary_fixed_width") {
      SAN_RETURN_NOT_OK(materialize_dictionary_fixed_width_column(
          stream->file, column, row_group.num_rows, &stream->page_scratch,
          &child));
      child.buffers[0] =
          child.validity.empty() ? nullptr : child.validity.data();
      child.buffers[1] = child.values.empty() ? nullptr : child.values.data();
      child.array.n_buffers = 2;
    } else {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: unsupported value buffer kind");
    }
    child.array.length = arrow_length;
    child.array.null_count = arrow_null_count;
    child.array.offset = 0;
    child.array.buffers = child.buffers.data();
    child.array.n_children = 0;
    child.array.children = nullptr;
    child.array.dictionary = nullptr;
    child.array.private_data = nullptr;
    child.array.release = &native_parquet_array_child_release;
  }
  std::vector<NativeParquetOutputField> layout;
  SAN_RETURN_NOT_OK(build_native_output_layout(row_group.columns, &layout));
  state->structs.resize(
      std::count_if(layout.begin(), layout.end(),
                    [](const NativeParquetOutputField &field) {
                      return field.is_struct || field.is_map ||
                             (field.is_list &&
                              (field.is_list_struct || field.is_list_map));
                    }) +
      std::accumulate(
          layout.begin(), layout.end(), std::size_t{0},
          [&](std::size_t total, const NativeParquetOutputField &field) {
            if (!field.is_struct) {
              return total;
            }
            std::vector<std::string> map_names;
            for (const auto column_index : field.column_indices) {
              if (column_index >= row_group.columns.size()) {
                continue;
              }
              const auto &column = row_group.columns[column_index];
              if ((is_top_level_struct_map_leaf(column) ||
                   top_level_struct_map_list_chain_depth(column) > 0) &&
                  column.path_in_schema.size() > 1 &&
                  std::find(map_names.begin(), map_names.end(),
                            column.path_in_schema[1]) == map_names.end()) {
                map_names.push_back(column.path_in_schema[1]);
              }
            }
            return total + map_names.size();
          }) +
      std::count_if(
          layout.begin(), layout.end(),
          [&](const NativeParquetOutputField &field) {
            return field.is_list_map &&
                   std::any_of(
                       field.column_indices.begin(), field.column_indices.end(),
                       [&](std::size_t column_index) {
                         return column_index < row_group.columns.size() &&
                                (is_top_level_list_map_struct_leaf(
                                     row_group.columns[column_index]) ||
                                 top_level_list_map_struct_list_chain_depth(
                                     row_group.columns[column_index]) > 0);
                       });
          }) +
      std::count_if(
          layout.begin(), layout.end(),
          [&](const NativeParquetOutputField &field) {
            return field.is_map &&
                   std::any_of(
                       field.column_indices.begin(), field.column_indices.end(),
                       [&](std::size_t column_index) {
                         return column_index < row_group.columns.size() &&
                                (is_top_level_map_struct_leaf(
                                     row_group.columns[column_index]) ||
                                 top_level_map_struct_list_chain_depth(
                                     row_group.columns[column_index]) > 0);
                       });
          }) +
      std::accumulate(
          layout.begin(), layout.end(), std::size_t{0},
          [&](std::size_t total, const NativeParquetOutputField &field) {
            if (!field.is_list_struct) {
              return total;
            }
            std::vector<std::string> names;
            for (const auto column_index : field.column_indices) {
              if (column_index >= row_group.columns.size()) {
                continue;
              }
              const auto &column = row_group.columns[column_index];
              if ((is_top_level_list_struct_map_leaf(column) ||
                   top_level_list_struct_map_list_chain_depth(column) > 0) &&
                  column.path_in_schema.size() > 3 &&
                  std::find(names.begin(), names.end(),
                            column.path_in_schema[3]) == names.end()) {
                names.push_back(column.path_in_schema[3]);
              }
            }
            return total + names.size();
          }));
  std::size_t list_array_count = 0;
  for (const auto &field : layout) {
    if (field.is_list) {
      if (field.list_depth > 3) {
        list_array_count += static_cast<std::size_t>(field.list_depth);
      } else if (field.is_list_list_list) {
        list_array_count += 3U;
      } else {
        list_array_count += (field.is_list_list || field.is_list_map) ? 2U : 1U;
      }
      if (field.is_list_struct) {
        list_array_count += static_cast<std::size_t>(std::accumulate(
            field.column_indices.begin(), field.column_indices.end(),
            std::int16_t{0}, [&](std::int16_t total, std::size_t column_index) {
              return static_cast<std::int16_t>(
                  total + (column_index < row_group.columns.size()
                               ? top_level_list_struct_list_chain_depth(
                                     row_group.columns[column_index])
                               : 0));
            }));
        std::vector<std::string> map_names;
        for (const auto column_index : field.column_indices) {
          if (column_index >= row_group.columns.size()) {
            continue;
          }
          const auto &column = row_group.columns[column_index];
          if ((is_top_level_list_struct_map_leaf(column) ||
               top_level_list_struct_map_list_chain_depth(column) > 0) &&
              column.path_in_schema.size() > 3 &&
              std::find(map_names.begin(), map_names.end(),
                        column.path_in_schema[3]) == map_names.end()) {
            map_names.push_back(column.path_in_schema[3]);
          }
        }
        list_array_count += map_names.size();
        list_array_count += static_cast<std::size_t>(std::accumulate(
            field.column_indices.begin(), field.column_indices.end(),
            std::int16_t{0}, [&](std::int16_t total, std::size_t column_index) {
              return static_cast<std::int16_t>(
                  total + (column_index < row_group.columns.size()
                               ? top_level_list_struct_map_list_chain_depth(
                                     row_group.columns[column_index])
                               : 0));
            }));
      }
      if (field.is_list_map) {
        list_array_count += static_cast<std::size_t>(std::accumulate(
            field.column_indices.begin(), field.column_indices.end(),
            std::int16_t{0}, [&](std::int16_t total, std::size_t column_index) {
              return static_cast<std::int16_t>(
                  total + (column_index < row_group.columns.size()
                               ? top_level_list_map_struct_list_chain_depth(
                                     row_group.columns[column_index])
                               : 0));
            }));
      }
    } else if (field.is_struct) {
      std::vector<std::string> map_names;
      for (const auto column_index : field.column_indices) {
        if (column_index >= row_group.columns.size()) {
          continue;
        }
        const auto &column = row_group.columns[column_index];
        if ((is_top_level_struct_map_leaf(column) ||
             top_level_struct_map_list_chain_depth(column) > 0) &&
            column.path_in_schema.size() > 1 &&
            std::find(map_names.begin(), map_names.end(),
                      column.path_in_schema[1]) == map_names.end()) {
          map_names.push_back(column.path_in_schema[1]);
        }
      }
      list_array_count += map_names.size();
      list_array_count += static_cast<std::size_t>(std::accumulate(
          field.column_indices.begin(), field.column_indices.end(),
          std::int16_t{0}, [&](std::int16_t total, std::size_t column_index) {
            return static_cast<std::int16_t>(
                total + (column_index < row_group.columns.size()
                             ? top_level_struct_map_list_chain_depth(
                                   row_group.columns[column_index])
                             : 0));
          }));
    } else if (field.is_map) {
      ++list_array_count;
      list_array_count += static_cast<std::size_t>(std::accumulate(
          field.column_indices.begin(), field.column_indices.end(),
          std::int16_t{0}, [&](std::int16_t total, std::size_t column_index) {
            return static_cast<std::int16_t>(
                total + (column_index < row_group.columns.size()
                             ? top_level_map_list_chain_depth(
                                   row_group.columns[column_index])
                             : 0));
          }));
      list_array_count += static_cast<std::size_t>(std::accumulate(
          field.column_indices.begin(), field.column_indices.end(),
          std::int16_t{0}, [&](std::int16_t total, std::size_t column_index) {
            return static_cast<std::int16_t>(
                total + (column_index < row_group.columns.size()
                             ? top_level_map_struct_list_chain_depth(
                                   row_group.columns[column_index])
                             : 0));
          }));
    }
  }
  state->lists.resize(list_array_count);
  state->children.reserve(layout.size());
  std::size_t struct_index = 0;
  std::size_t list_index = 0;
  for (const auto &field : layout) {
    if (field.is_list || field.is_map) {
      if (field.is_map) {
        const auto column_index = field.column_indices.front();
        const auto &column = row_group.columns[column_index];
        SAN_RETURN_NOT_OK(validate_map_repetition_layout(row_group, field));
        auto &map_array = state->lists[list_index++];
        auto &entries_array = state->structs[struct_index++];
        entries_array.children.reserve(field.column_indices.size());
        bool map_value_struct_added = false;
        for (const auto child_column_index : field.column_indices) {
          const auto &child_column = row_group.columns[child_column_index];
          const auto child_list_depth =
              top_level_map_list_chain_depth(child_column);
          if (is_top_level_map_struct_leaf(child_column) ||
              top_level_map_struct_list_chain_depth(child_column) > 0) {
            if (map_value_struct_added) {
              continue;
            }
            map_value_struct_added = true;
            std::vector<std::size_t> struct_column_indices;
            for (const auto candidate_index : field.column_indices) {
              if (candidate_index >= row_group.columns.size()) {
                continue;
              }
              const auto &candidate = row_group.columns[candidate_index];
              if (is_top_level_map_struct_leaf(candidate) ||
                  top_level_map_struct_list_chain_depth(candidate) > 0) {
                struct_column_indices.push_back(candidate_index);
              }
            }
            if (struct_column_indices.empty()) {
              return sanitize::Status::Invalid(
                  "native Parquet reader: map value struct has no leaves");
            }
            const ColumnChunkInfo *value_struct_layout_column = &child_column;
            for (const auto candidate_index : struct_column_indices) {
              if (candidate_index >= row_group.columns.size()) {
                continue;
              }
              const auto &candidate = row_group.columns[candidate_index];
              if (top_level_map_struct_list_chain_depth(candidate) == 0) {
                value_struct_layout_column = &candidate;
                break;
              }
            }
            auto &value_struct_array = state->structs[struct_index++];
            value_struct_array.children.reserve(struct_column_indices.size());
            for (const auto struct_column_index : struct_column_indices) {
              const auto &struct_column =
                  row_group.columns[struct_column_index];
              const auto struct_list_depth =
                  top_level_map_struct_list_chain_depth(struct_column);
              if (struct_list_depth > 0) {
                if (struct_column.repeated_level_layouts.size() !=
                    static_cast<std::size_t>(struct_list_depth + 1)) {
                  return sanitize::Status::NotImplemented(
                      "native Parquet reader: map value struct nested list "
                      "layout was not decoded");
                }
                std::vector<NativeParquetListArray *> chain_arrays;
                chain_arrays.reserve(
                    static_cast<std::size_t>(struct_list_depth));
                for (std::int16_t level = 0; level < struct_list_depth;
                     ++level) {
                  chain_arrays.push_back(&state->lists[list_index++]);
                }
                for (std::int16_t level = struct_list_depth; level >= 1;
                     --level) {
                  const auto layout_index = static_cast<std::size_t>(level);
                  const auto array_index = static_cast<std::size_t>(level - 1);
                  const auto &level_layout =
                      struct_column.repeated_level_layouts[layout_index];
                  if (!level_layout.decoded) {
                    return sanitize::Status::NotImplemented(
                        "native Parquet reader: map value struct nested list "
                        "level was not decoded");
                  }
                  auto &inner_array = *chain_arrays[array_index];
                  inner_array.validity = level_layout.validity_bitmap;
                  inner_array.offsets = level_layout.offsets;
                  inner_array.children[0] =
                      (level == struct_list_depth)
                          ? &state->columns[struct_column_index].array
                          : &chain_arrays[array_index + 1]->array;
                  inner_array.array.length = level_layout.row_count;
                  inner_array.array.null_count = level_layout.null_count;
                  inner_array.array.offset = 0;
                  inner_array.array.n_buffers = 2;
                  inner_array.buffers[0] = inner_array.validity.empty()
                                               ? nullptr
                                               : inner_array.validity.data();
                  inner_array.buffers[1] = inner_array.offsets.empty()
                                               ? nullptr
                                               : inner_array.offsets.data();
                  inner_array.array.buffers = inner_array.buffers.data();
                  inner_array.array.n_children = 1;
                  inner_array.array.children = inner_array.children.data();
                  inner_array.array.dictionary = nullptr;
                  inner_array.array.private_data = nullptr;
                  inner_array.array.release =
                      &native_parquet_array_child_release;
                }
                value_struct_array.children.push_back(
                    &chain_arrays.front()->array);
              } else {
                value_struct_array.children.push_back(
                    &state->columns[struct_column_index].array);
              }
            }
            value_struct_array.array.length =
                value_struct_layout_column->repeated_level_element_count;
            SAN_ASSIGN_OR_RAISE(
                const auto value_struct_null_count,
                materialize_map_value_struct_validity(
                    *value_struct_layout_column, &value_struct_array.validity));
            value_struct_array.array.null_count = value_struct_null_count;
            value_struct_array.array.offset = 0;
            value_struct_array.array.n_buffers = 1;
            value_struct_array.buffers[0] =
                value_struct_array.validity.empty()
                    ? nullptr
                    : value_struct_array.validity.data();
            value_struct_array.array.buffers =
                value_struct_array.buffers.data();
            value_struct_array.array.n_children =
                static_cast<std::int64_t>(value_struct_array.children.size());
            value_struct_array.array.children =
                value_struct_array.children.empty()
                    ? nullptr
                    : value_struct_array.children.data();
            value_struct_array.array.dictionary = nullptr;
            value_struct_array.array.private_data = nullptr;
            value_struct_array.array.release =
                &native_parquet_array_child_release;
            entries_array.children.push_back(&value_struct_array.array);
          } else if (child_list_depth > 1) {
            if (child_column.repeated_level_layouts.size() !=
                static_cast<std::size_t>(child_list_depth + 1)) {
              return sanitize::Status::NotImplemented(
                  "native Parquet reader: map nested list chain layout was not "
                  "decoded");
            }
            std::vector<NativeParquetListArray *> chain_arrays;
            chain_arrays.reserve(static_cast<std::size_t>(child_list_depth));
            for (std::int16_t level = 0; level < child_list_depth; ++level) {
              chain_arrays.push_back(&state->lists[list_index++]);
            }
            for (std::int16_t level = child_list_depth; level >= 1; --level) {
              const auto layout_index = static_cast<std::size_t>(level);
              const auto array_index = static_cast<std::size_t>(level - 1);
              const auto &level_layout =
                  child_column.repeated_level_layouts[layout_index];
              if (!level_layout.decoded) {
                return sanitize::Status::NotImplemented(
                    "native Parquet reader: map nested list chain level was "
                    "not decoded");
              }
              auto &inner_array = *chain_arrays[array_index];
              inner_array.validity = level_layout.validity_bitmap;
              inner_array.offsets = level_layout.offsets;
              inner_array.children[0] =
                  (level == child_list_depth)
                      ? &state->columns[child_column_index].array
                      : &chain_arrays[array_index + 1]->array;
              inner_array.array.length = level_layout.row_count;
              inner_array.array.null_count = level_layout.null_count;
              inner_array.array.offset = 0;
              inner_array.array.n_buffers = 2;
              inner_array.buffers[0] = inner_array.validity.empty()
                                           ? nullptr
                                           : inner_array.validity.data();
              inner_array.buffers[1] = inner_array.offsets.empty()
                                           ? nullptr
                                           : inner_array.offsets.data();
              inner_array.array.buffers = inner_array.buffers.data();
              inner_array.array.n_children = 1;
              inner_array.array.children = inner_array.children.data();
              inner_array.array.dictionary = nullptr;
              inner_array.array.private_data = nullptr;
              inner_array.array.release = &native_parquet_array_child_release;
            }
            entries_array.children.push_back(&chain_arrays.front()->array);
          } else if (child_list_depth == 1) {
            if (!child_column.nested_repeated_level_layout_decoded) {
              return sanitize::Status::NotImplemented(
                  "native Parquet reader: map nested list layout was not "
                  "decoded");
            }
            auto &inner_list_array = state->lists[list_index++];
            inner_list_array.validity =
                child_column.nested_repeated_level_validity_bitmap;
            inner_list_array.offsets =
                child_column.nested_repeated_level_offsets;
            inner_list_array.children[0] =
                &state->columns[child_column_index].array;
            inner_list_array.array.length =
                child_column.nested_repeated_level_row_count;
            inner_list_array.array.null_count =
                child_column.nested_repeated_level_null_count;
            inner_list_array.array.offset = 0;
            inner_list_array.array.n_buffers = 2;
            inner_list_array.buffers[0] =
                inner_list_array.validity.empty()
                    ? nullptr
                    : inner_list_array.validity.data();
            inner_list_array.buffers[1] = inner_list_array.offsets.empty()
                                              ? nullptr
                                              : inner_list_array.offsets.data();
            inner_list_array.array.buffers = inner_list_array.buffers.data();
            inner_list_array.array.n_children = 1;
            inner_list_array.array.children = inner_list_array.children.data();
            inner_list_array.array.dictionary = nullptr;
            inner_list_array.array.private_data = nullptr;
            inner_list_array.array.release =
                &native_parquet_array_child_release;
            entries_array.children.push_back(&inner_list_array.array);
          } else {
            entries_array.children.push_back(
                &state->columns[child_column_index].array);
          }
        }
        entries_array.array.length = column.repeated_level_element_count;
        entries_array.array.null_count = 0;
        entries_array.array.offset = 0;
        entries_array.array.n_buffers = 1;
        entries_array.buffers[0] = nullptr;
        entries_array.array.buffers = entries_array.buffers.data();
        entries_array.array.n_children =
            static_cast<std::int64_t>(entries_array.children.size());
        entries_array.array.children = entries_array.children.empty()
                                           ? nullptr
                                           : entries_array.children.data();
        entries_array.array.dictionary = nullptr;
        entries_array.array.private_data = nullptr;
        entries_array.array.release = &native_parquet_array_child_release;

        map_array.validity = column.repeated_level_validity_bitmap;
        map_array.offsets = column.repeated_level_offsets;
        map_array.children[0] = &entries_array.array;
        map_array.array.length = row_group.num_rows;
        map_array.array.null_count = column.repeated_level_null_count;
        map_array.array.offset = 0;
        map_array.array.n_buffers = 2;
        map_array.buffers[0] =
            map_array.validity.empty() ? nullptr : map_array.validity.data();
        map_array.buffers[1] =
            map_array.offsets.empty() ? nullptr : map_array.offsets.data();
        map_array.array.buffers = map_array.buffers.data();
        map_array.array.n_children = 1;
        map_array.array.children = map_array.children.data();
        map_array.array.dictionary = nullptr;
        map_array.array.private_data = nullptr;
        map_array.array.release = &native_parquet_array_child_release;
        state->children.push_back(&map_array.array);
        continue;
      }
      const auto column_index = field.column_indices.front();
      const auto &column = row_group.columns[column_index];
      auto &list_array = state->lists[list_index++];
      ArrowArray *list_child = &state->columns[column_index].array;
      if (field.is_list_map) {
        SAN_RETURN_NOT_OK(
            validate_list_map_repetition_layout(row_group, field));
        auto &map_array = state->lists[list_index++];
        auto &entries_array = state->structs[struct_index++];
        entries_array.children.reserve(field.column_indices.size());
        bool map_value_struct_added = false;
        for (const auto child_column_index : field.column_indices) {
          const auto &child_column = row_group.columns[child_column_index];
          if (is_top_level_list_map_struct_leaf(child_column) ||
              top_level_list_map_struct_list_chain_depth(child_column) > 0) {
            if (map_value_struct_added) {
              continue;
            }
            map_value_struct_added = true;
            std::vector<std::size_t> struct_column_indices;
            for (const auto candidate_index : field.column_indices) {
              if (candidate_index >= row_group.columns.size()) {
                continue;
              }
              const auto &candidate = row_group.columns[candidate_index];
              if (is_top_level_list_map_struct_leaf(candidate) ||
                  top_level_list_map_struct_list_chain_depth(candidate) > 0) {
                struct_column_indices.push_back(candidate_index);
              }
            }
            if (struct_column_indices.empty()) {
              return sanitize::Status::Invalid(
                  "native Parquet reader: list map value struct has no leaves");
            }
            const ColumnChunkInfo *value_struct_layout_column = &child_column;
            for (const auto candidate_index : struct_column_indices) {
              if (candidate_index >= row_group.columns.size()) {
                continue;
              }
              const auto &candidate = row_group.columns[candidate_index];
              if (top_level_list_map_struct_list_chain_depth(candidate) == 0) {
                value_struct_layout_column = &candidate;
                break;
              }
            }
            auto &value_struct_array = state->structs[struct_index++];
            value_struct_array.children.reserve(struct_column_indices.size());
            for (const auto struct_column_index : struct_column_indices) {
              const auto &struct_column =
                  row_group.columns[struct_column_index];
              const auto struct_list_depth =
                  top_level_list_map_struct_list_chain_depth(struct_column);
              if (struct_list_depth > 0) {
                if (struct_column.repeated_level_layouts.size() !=
                    static_cast<std::size_t>(struct_list_depth + 2)) {
                  return sanitize::Status::NotImplemented(
                      "native Parquet reader: list map value struct nested "
                      "list "
                      "layout was not decoded");
                }
                std::vector<NativeParquetListArray *> chain_arrays;
                chain_arrays.reserve(
                    static_cast<std::size_t>(struct_list_depth));
                for (std::int16_t level = 0; level < struct_list_depth;
                     ++level) {
                  chain_arrays.push_back(&state->lists[list_index++]);
                }
                for (std::int16_t level = struct_list_depth; level >= 1;
                     --level) {
                  const auto layout_index = static_cast<std::size_t>(level + 1);
                  const auto array_index = static_cast<std::size_t>(level - 1);
                  const auto &level_layout =
                      struct_column.repeated_level_layouts[layout_index];
                  if (!level_layout.decoded) {
                    return sanitize::Status::NotImplemented(
                        "native Parquet reader: list map value struct nested "
                        "list level was not decoded");
                  }
                  auto &inner_array = *chain_arrays[array_index];
                  inner_array.validity = level_layout.validity_bitmap;
                  inner_array.offsets = level_layout.offsets;
                  inner_array.children[0] =
                      (level == struct_list_depth)
                          ? &state->columns[struct_column_index].array
                          : &chain_arrays[array_index + 1]->array;
                  inner_array.array.length = level_layout.row_count;
                  inner_array.array.null_count = level_layout.null_count;
                  inner_array.array.offset = 0;
                  inner_array.array.n_buffers = 2;
                  inner_array.buffers[0] = inner_array.validity.empty()
                                               ? nullptr
                                               : inner_array.validity.data();
                  inner_array.buffers[1] = inner_array.offsets.empty()
                                               ? nullptr
                                               : inner_array.offsets.data();
                  inner_array.array.buffers = inner_array.buffers.data();
                  inner_array.array.n_children = 1;
                  inner_array.array.children = inner_array.children.data();
                  inner_array.array.dictionary = nullptr;
                  inner_array.array.private_data = nullptr;
                  inner_array.array.release =
                      &native_parquet_array_child_release;
                }
                value_struct_array.children.push_back(
                    &chain_arrays.front()->array);
              } else {
                value_struct_array.children.push_back(
                    &state->columns[struct_column_index].array);
              }
            }
            value_struct_array.array.length =
                value_struct_layout_column->nested_repeated_level_element_count;
            SAN_ASSIGN_OR_RAISE(
                const auto value_struct_null_count,
                materialize_list_map_value_struct_validity(
                    *value_struct_layout_column, &value_struct_array.validity));
            value_struct_array.array.null_count = value_struct_null_count;
            value_struct_array.array.offset = 0;
            value_struct_array.array.n_buffers = 1;
            value_struct_array.buffers[0] =
                value_struct_array.validity.empty()
                    ? nullptr
                    : value_struct_array.validity.data();
            value_struct_array.array.buffers =
                value_struct_array.buffers.data();
            value_struct_array.array.n_children =
                static_cast<std::int64_t>(value_struct_array.children.size());
            value_struct_array.array.children =
                value_struct_array.children.empty()
                    ? nullptr
                    : value_struct_array.children.data();
            value_struct_array.array.dictionary = nullptr;
            value_struct_array.array.private_data = nullptr;
            value_struct_array.array.release =
                &native_parquet_array_child_release;
            entries_array.children.push_back(&value_struct_array.array);
          } else {
            entries_array.children.push_back(
                &state->columns[child_column_index].array);
          }
        }
        entries_array.array.length = column.nested_repeated_level_element_count;
        entries_array.array.null_count = 0;
        entries_array.array.offset = 0;
        entries_array.array.n_buffers = 1;
        entries_array.buffers[0] = nullptr;
        entries_array.array.buffers = entries_array.buffers.data();
        entries_array.array.n_children =
            static_cast<std::int64_t>(entries_array.children.size());
        entries_array.array.children = entries_array.children.empty()
                                           ? nullptr
                                           : entries_array.children.data();
        entries_array.array.dictionary = nullptr;
        entries_array.array.private_data = nullptr;
        entries_array.array.release = &native_parquet_array_child_release;

        map_array.validity = column.nested_repeated_level_validity_bitmap;
        map_array.offsets = column.nested_repeated_level_offsets;
        map_array.children[0] = &entries_array.array;
        map_array.array.length = column.nested_repeated_level_row_count;
        map_array.array.null_count = column.nested_repeated_level_null_count;
        map_array.array.offset = 0;
        map_array.array.n_buffers = 2;
        map_array.buffers[0] =
            map_array.validity.empty() ? nullptr : map_array.validity.data();
        map_array.buffers[1] =
            map_array.offsets.empty() ? nullptr : map_array.offsets.data();
        map_array.array.buffers = map_array.buffers.data();
        map_array.array.n_children = 1;
        map_array.array.children = map_array.children.data();
        map_array.array.dictionary = nullptr;
        map_array.array.private_data = nullptr;
        map_array.array.release = &native_parquet_array_child_release;
        list_child = &map_array.array;
      } else if (field.list_depth > 3) {
        if (column.repeated_level_layouts.size() !=
            static_cast<std::size_t>(field.list_depth)) {
          return sanitize::Status::NotImplemented(
              "native Parquet reader: generic nested list layout was not "
              "decoded");
        }
        std::vector<NativeParquetListArray *> chain_arrays;
        chain_arrays.reserve(static_cast<std::size_t>(field.list_depth - 1));
        for (std::int16_t level = 1; level < field.list_depth; ++level) {
          chain_arrays.push_back(&state->lists[list_index++]);
        }
        for (std::int16_t level =
                 static_cast<std::int16_t>(field.list_depth - 1);
             level >= 1; --level) {
          const auto layout_index = static_cast<std::size_t>(level);
          const auto array_index = static_cast<std::size_t>(level - 1);
          const auto &level_layout =
              column.repeated_level_layouts[layout_index];
          if (!level_layout.decoded) {
            return sanitize::Status::NotImplemented(
                "native Parquet reader: generic nested list level was not "
                "decoded");
          }
          auto &inner_array = *chain_arrays[array_index];
          inner_array.validity = level_layout.validity_bitmap;
          inner_array.offsets = level_layout.offsets;
          inner_array.children[0] = (level == field.list_depth - 1)
                                        ? &state->columns[column_index].array
                                        : &chain_arrays[array_index + 1]->array;
          inner_array.array.length = level_layout.row_count;
          inner_array.array.null_count = level_layout.null_count;
          inner_array.array.offset = 0;
          inner_array.array.n_buffers = 2;
          inner_array.buffers[0] = inner_array.validity.empty()
                                       ? nullptr
                                       : inner_array.validity.data();
          inner_array.buffers[1] = inner_array.offsets.empty()
                                       ? nullptr
                                       : inner_array.offsets.data();
          inner_array.array.buffers = inner_array.buffers.data();
          inner_array.array.n_children = 1;
          inner_array.array.children = inner_array.children.data();
          inner_array.array.dictionary = nullptr;
          inner_array.array.private_data = nullptr;
          inner_array.array.release = &native_parquet_array_child_release;
        }
        list_child = &chain_arrays.front()->array;
      } else if (field.is_list_list_list) {
        if (!column.nested_repeated_level_layout_decoded ||
            !column.deep_repeated_level_layout_decoded) {
          return sanitize::Status::NotImplemented(
              "native Parquet reader: deep nested list layout was not "
              "decoded");
        }
        auto &middle_list_array = state->lists[list_index++];
        auto &inner_list_array = state->lists[list_index++];
        inner_list_array.validity = column.deep_repeated_level_validity_bitmap;
        inner_list_array.offsets = column.deep_repeated_level_offsets;
        inner_list_array.children[0] = &state->columns[column_index].array;
        inner_list_array.array.length = column.deep_repeated_level_row_count;
        inner_list_array.array.null_count =
            column.deep_repeated_level_null_count;
        inner_list_array.array.offset = 0;
        inner_list_array.array.n_buffers = 2;
        inner_list_array.buffers[0] = inner_list_array.validity.empty()
                                          ? nullptr
                                          : inner_list_array.validity.data();
        inner_list_array.buffers[1] = inner_list_array.offsets.empty()
                                          ? nullptr
                                          : inner_list_array.offsets.data();
        inner_list_array.array.buffers = inner_list_array.buffers.data();
        inner_list_array.array.n_children = 1;
        inner_list_array.array.children = inner_list_array.children.data();
        inner_list_array.array.dictionary = nullptr;
        inner_list_array.array.private_data = nullptr;
        inner_list_array.array.release = &native_parquet_array_child_release;

        middle_list_array.validity =
            column.nested_repeated_level_validity_bitmap;
        middle_list_array.offsets = column.nested_repeated_level_offsets;
        middle_list_array.children[0] = &inner_list_array.array;
        middle_list_array.array.length = column.nested_repeated_level_row_count;
        middle_list_array.array.null_count =
            column.nested_repeated_level_null_count;
        middle_list_array.array.offset = 0;
        middle_list_array.array.n_buffers = 2;
        middle_list_array.buffers[0] = middle_list_array.validity.empty()
                                           ? nullptr
                                           : middle_list_array.validity.data();
        middle_list_array.buffers[1] = middle_list_array.offsets.empty()
                                           ? nullptr
                                           : middle_list_array.offsets.data();
        middle_list_array.array.buffers = middle_list_array.buffers.data();
        middle_list_array.array.n_children = 1;
        middle_list_array.array.children = middle_list_array.children.data();
        middle_list_array.array.dictionary = nullptr;
        middle_list_array.array.private_data = nullptr;
        middle_list_array.array.release = &native_parquet_array_child_release;
        list_child = &middle_list_array.array;
      } else if (field.is_list_list) {
        if (!column.nested_repeated_level_layout_decoded) {
          return sanitize::Status::NotImplemented(
              "native Parquet reader: nested list layout was not decoded");
        }
        auto &inner_list_array = state->lists[list_index++];
        inner_list_array.validity =
            column.nested_repeated_level_validity_bitmap;
        inner_list_array.offsets = column.nested_repeated_level_offsets;
        inner_list_array.children[0] = &state->columns[column_index].array;
        inner_list_array.array.length = column.nested_repeated_level_row_count;
        inner_list_array.array.null_count =
            column.nested_repeated_level_null_count;
        inner_list_array.array.offset = 0;
        inner_list_array.array.n_buffers = 2;
        inner_list_array.buffers[0] = inner_list_array.validity.empty()
                                          ? nullptr
                                          : inner_list_array.validity.data();
        inner_list_array.buffers[1] = inner_list_array.offsets.empty()
                                          ? nullptr
                                          : inner_list_array.offsets.data();
        inner_list_array.array.buffers = inner_list_array.buffers.data();
        inner_list_array.array.n_children = 1;
        inner_list_array.array.children = inner_list_array.children.data();
        inner_list_array.array.dictionary = nullptr;
        inner_list_array.array.private_data = nullptr;
        inner_list_array.array.release = &native_parquet_array_child_release;
        list_child = &inner_list_array.array;
      } else if (field.is_list_struct) {
        SAN_RETURN_NOT_OK(
            validate_list_struct_repetition_layout(row_group, field));
        auto &struct_array = state->structs[struct_index++];
        struct_array.children.reserve(field.column_indices.size());
        const ColumnChunkInfo *struct_layout_column = &column;
        for (const auto candidate_index : field.column_indices) {
          if (candidate_index >= row_group.columns.size()) {
            continue;
          }
          const auto &candidate = row_group.columns[candidate_index];
          if (!is_top_level_list_struct_map_leaf(candidate) &&
              top_level_list_struct_map_list_chain_depth(candidate) == 0) {
            struct_layout_column = &candidate;
            break;
          }
        }
        std::vector<std::string> map_child_names;
        for (const auto child_column_index : field.column_indices) {
          const auto &child_column = row_group.columns[child_column_index];
          const auto child_list_depth =
              top_level_list_struct_list_chain_depth(child_column);
          if (is_top_level_list_struct_map_leaf(child_column) ||
              top_level_list_struct_map_list_chain_depth(child_column) > 0) {
            const auto &map_name = child_column.path_in_schema[3];
            if (std::find(map_child_names.begin(), map_child_names.end(),
                          map_name) != map_child_names.end()) {
              continue;
            }
            map_child_names.push_back(map_name);
            std::vector<std::size_t> map_column_indices;
            for (const auto candidate_index : field.column_indices) {
              if (candidate_index >= row_group.columns.size()) {
                continue;
              }
              const auto &candidate = row_group.columns[candidate_index];
              if ((is_top_level_list_struct_map_leaf(candidate) ||
                   top_level_list_struct_map_list_chain_depth(candidate) > 0) &&
                  candidate.path_in_schema.size() > 3 &&
                  candidate.path_in_schema[3] == map_name) {
                map_column_indices.push_back(candidate_index);
              }
            }
            auto &map_array = state->lists[list_index++];
            auto &entries_array = state->structs[struct_index++];
            entries_array.children.reserve(map_column_indices.size());
            for (const auto map_column_index : map_column_indices) {
              const auto &map_column = row_group.columns[map_column_index];
              const auto map_value_list_depth =
                  top_level_list_struct_map_list_chain_depth(map_column);
              if (map_value_list_depth > 0) {
                if (map_column.repeated_level_layouts.size() !=
                    static_cast<std::size_t>(map_value_list_depth + 2)) {
                  return sanitize::Status::NotImplemented(
                      "native Parquet reader: list struct map nested list "
                      "layout was not decoded");
                }
                std::vector<NativeParquetListArray *> chain_arrays;
                chain_arrays.reserve(
                    static_cast<std::size_t>(map_value_list_depth));
                for (std::int16_t level = 0; level < map_value_list_depth;
                     ++level) {
                  chain_arrays.push_back(&state->lists[list_index++]);
                }
                for (std::int16_t level = map_value_list_depth; level >= 1;
                     --level) {
                  const auto layout_index = static_cast<std::size_t>(level + 1);
                  const auto array_index = static_cast<std::size_t>(level - 1);
                  const auto &level_layout =
                      map_column.repeated_level_layouts[layout_index];
                  if (!level_layout.decoded) {
                    return sanitize::Status::NotImplemented(
                        "native Parquet reader: list struct map nested list "
                        "level was not decoded");
                  }
                  auto &inner_array = *chain_arrays[array_index];
                  inner_array.validity = level_layout.validity_bitmap;
                  inner_array.offsets = level_layout.offsets;
                  inner_array.children[0] =
                      (level == map_value_list_depth)
                          ? &state->columns[map_column_index].array
                          : &chain_arrays[array_index + 1]->array;
                  inner_array.array.length = level_layout.row_count;
                  inner_array.array.null_count = level_layout.null_count;
                  inner_array.array.offset = 0;
                  inner_array.array.n_buffers = 2;
                  inner_array.buffers[0] = inner_array.validity.empty()
                                               ? nullptr
                                               : inner_array.validity.data();
                  inner_array.buffers[1] = inner_array.offsets.empty()
                                               ? nullptr
                                               : inner_array.offsets.data();
                  inner_array.array.buffers = inner_array.buffers.data();
                  inner_array.array.n_children = 1;
                  inner_array.array.children = inner_array.children.data();
                  inner_array.array.dictionary = nullptr;
                  inner_array.array.private_data = nullptr;
                  inner_array.array.release =
                      &native_parquet_array_child_release;
                }
                entries_array.children.push_back(&chain_arrays.front()->array);
              } else {
                entries_array.children.push_back(
                    &state->columns[map_column_index].array);
              }
            }
            entries_array.array.length =
                child_column.nested_repeated_level_element_count;
            entries_array.array.null_count = 0;
            entries_array.array.offset = 0;
            entries_array.array.n_buffers = 1;
            entries_array.buffers[0] = nullptr;
            entries_array.array.buffers = entries_array.buffers.data();
            entries_array.array.n_children =
                static_cast<std::int64_t>(entries_array.children.size());
            entries_array.array.children = entries_array.children.empty()
                                               ? nullptr
                                               : entries_array.children.data();
            entries_array.array.dictionary = nullptr;
            entries_array.array.private_data = nullptr;
            entries_array.array.release = &native_parquet_array_child_release;

            map_array.validity =
                child_column.nested_repeated_level_validity_bitmap;
            map_array.offsets = child_column.nested_repeated_level_offsets;
            map_array.children[0] = &entries_array.array;
            map_array.array.length =
                child_column.nested_repeated_level_row_count;
            map_array.array.null_count =
                child_column.nested_repeated_level_null_count;
            map_array.array.offset = 0;
            map_array.array.n_buffers = 2;
            map_array.buffers[0] = map_array.validity.empty()
                                       ? nullptr
                                       : map_array.validity.data();
            map_array.buffers[1] =
                map_array.offsets.empty() ? nullptr : map_array.offsets.data();
            map_array.array.buffers = map_array.buffers.data();
            map_array.array.n_children = 1;
            map_array.array.children = map_array.children.data();
            map_array.array.dictionary = nullptr;
            map_array.array.private_data = nullptr;
            map_array.array.release = &native_parquet_array_child_release;
            struct_array.children.push_back(&map_array.array);
          } else if (child_list_depth > 1) {
            if (child_column.repeated_level_layouts.size() !=
                static_cast<std::size_t>(child_list_depth + 1)) {
              return sanitize::Status::NotImplemented(
                  "native Parquet reader: list struct nested list chain layout "
                  "was not decoded");
            }
            std::vector<NativeParquetListArray *> chain_arrays;
            chain_arrays.reserve(static_cast<std::size_t>(child_list_depth));
            for (std::int16_t level = 0; level < child_list_depth; ++level) {
              chain_arrays.push_back(&state->lists[list_index++]);
            }
            for (std::int16_t level = child_list_depth; level >= 1; --level) {
              const auto layout_index = static_cast<std::size_t>(level);
              const auto array_index = static_cast<std::size_t>(level - 1);
              const auto &level_layout =
                  child_column.repeated_level_layouts[layout_index];
              if (!level_layout.decoded) {
                return sanitize::Status::NotImplemented(
                    "native Parquet reader: list struct nested list chain "
                    "level was not decoded");
              }
              auto &inner_array = *chain_arrays[array_index];
              inner_array.validity = level_layout.validity_bitmap;
              inner_array.offsets = level_layout.offsets;
              inner_array.children[0] =
                  (level == child_list_depth)
                      ? &state->columns[child_column_index].array
                      : &chain_arrays[array_index + 1]->array;
              inner_array.array.length = level_layout.row_count;
              inner_array.array.null_count = level_layout.null_count;
              inner_array.array.offset = 0;
              inner_array.array.n_buffers = 2;
              inner_array.buffers[0] = inner_array.validity.empty()
                                           ? nullptr
                                           : inner_array.validity.data();
              inner_array.buffers[1] = inner_array.offsets.empty()
                                           ? nullptr
                                           : inner_array.offsets.data();
              inner_array.array.buffers = inner_array.buffers.data();
              inner_array.array.n_children = 1;
              inner_array.array.children = inner_array.children.data();
              inner_array.array.dictionary = nullptr;
              inner_array.array.private_data = nullptr;
              inner_array.array.release = &native_parquet_array_child_release;
            }
            struct_array.children.push_back(&chain_arrays.front()->array);
          } else if (child_list_depth == 1) {
            if (!child_column.nested_repeated_level_layout_decoded) {
              return sanitize::Status::NotImplemented(
                  "native Parquet reader: list struct nested list layout was "
                  "not decoded");
            }
            auto &inner_list_array = state->lists[list_index++];
            inner_list_array.validity =
                child_column.nested_repeated_level_validity_bitmap;
            inner_list_array.offsets =
                child_column.nested_repeated_level_offsets;
            inner_list_array.children[0] =
                &state->columns[child_column_index].array;
            inner_list_array.array.length =
                child_column.nested_repeated_level_row_count;
            inner_list_array.array.null_count =
                child_column.nested_repeated_level_null_count;
            inner_list_array.array.offset = 0;
            inner_list_array.array.n_buffers = 2;
            inner_list_array.buffers[0] =
                inner_list_array.validity.empty()
                    ? nullptr
                    : inner_list_array.validity.data();
            inner_list_array.buffers[1] = inner_list_array.offsets.empty()
                                              ? nullptr
                                              : inner_list_array.offsets.data();
            inner_list_array.array.buffers = inner_list_array.buffers.data();
            inner_list_array.array.n_children = 1;
            inner_list_array.array.children = inner_list_array.children.data();
            inner_list_array.array.dictionary = nullptr;
            inner_list_array.array.private_data = nullptr;
            inner_list_array.array.release =
                &native_parquet_array_child_release;
            struct_array.children.push_back(&inner_list_array.array);
          } else {
            struct_array.children.push_back(
                &state->columns[child_column_index].array);
          }
        }
        struct_array.array.length =
            struct_layout_column->repeated_level_element_count;
        SAN_ASSIGN_OR_RAISE(const auto struct_null_count,
                            materialize_list_struct_validity(
                                *struct_layout_column, &struct_array.validity));
        struct_array.array.null_count = struct_null_count;
        struct_array.array.offset = 0;
        struct_array.array.n_buffers = 1;
        struct_array.buffers[0] = struct_array.validity.empty()
                                      ? nullptr
                                      : struct_array.validity.data();
        struct_array.array.buffers = struct_array.buffers.data();
        struct_array.array.n_children =
            static_cast<std::int64_t>(struct_array.children.size());
        struct_array.array.children = struct_array.children.empty()
                                          ? nullptr
                                          : struct_array.children.data();
        struct_array.array.dictionary = nullptr;
        struct_array.array.private_data = nullptr;
        struct_array.array.release = &native_parquet_array_child_release;
        list_child = &struct_array.array;
      }
      list_array.validity = column.repeated_level_validity_bitmap;
      list_array.offsets = column.repeated_level_offsets;
      list_array.children[0] = list_child;
      list_array.array.length = row_group.num_rows;
      list_array.array.null_count = column.repeated_level_null_count;
      list_array.array.offset = 0;
      list_array.array.n_buffers = 2;
      list_array.buffers[0] =
          list_array.validity.empty() ? nullptr : list_array.validity.data();
      list_array.buffers[1] =
          list_array.offsets.empty() ? nullptr : list_array.offsets.data();
      list_array.array.buffers = list_array.buffers.data();
      list_array.array.n_children = 1;
      list_array.array.children = list_array.children.data();
      list_array.array.dictionary = nullptr;
      list_array.array.private_data = nullptr;
      list_array.array.release = &native_parquet_array_child_release;
      state->children.push_back(&list_array.array);
      continue;
    }
    if (!field.is_struct) {
      state->children.push_back(
          &state->columns[field.column_indices.front()].array);
      continue;
    }
    auto &struct_array = state->structs[struct_index++];
    struct_array.children.reserve(field.column_indices.size());
    const ColumnChunkInfo *struct_validity_column = nullptr;
    for (const auto candidate_index : field.column_indices) {
      if (candidate_index >= row_group.columns.size()) {
        continue;
      }
      const auto &candidate = row_group.columns[candidate_index];
      if (!is_top_level_struct_map_leaf(candidate) &&
          top_level_struct_map_list_chain_depth(candidate) == 0) {
        struct_validity_column = &candidate;
        break;
      }
    }
    if (!struct_validity_column && !field.column_indices.empty() &&
        field.column_indices.front() < row_group.columns.size()) {
      struct_validity_column = &row_group.columns[field.column_indices.front()];
    }
    std::vector<std::string> map_child_names;
    for (const auto column_index : field.column_indices) {
      if (column_index >= row_group.columns.size()) {
        continue;
      }
      const auto &child_column = row_group.columns[column_index];
      if (is_top_level_struct_map_leaf(child_column) ||
          top_level_struct_map_list_chain_depth(child_column) > 0) {
        const auto &map_name = child_column.path_in_schema[1];
        if (std::find(map_child_names.begin(), map_child_names.end(),
                      map_name) != map_child_names.end()) {
          continue;
        }
        map_child_names.push_back(map_name);
        std::vector<std::size_t> map_column_indices;
        for (const auto candidate_index : field.column_indices) {
          if (candidate_index >= row_group.columns.size()) {
            continue;
          }
          const auto &candidate = row_group.columns[candidate_index];
          if ((is_top_level_struct_map_leaf(candidate) ||
               top_level_struct_map_list_chain_depth(candidate) > 0) &&
              candidate.path_in_schema.size() > 1 &&
              candidate.path_in_schema[1] == map_name) {
            map_column_indices.push_back(candidate_index);
          }
        }
        auto &map_array = state->lists[list_index++];
        auto &entries_array = state->structs[struct_index++];
        entries_array.children.reserve(map_column_indices.size());
        for (const auto map_column_index : map_column_indices) {
          const auto &map_column = row_group.columns[map_column_index];
          const auto child_list_depth =
              top_level_struct_map_list_chain_depth(map_column);
          if (child_list_depth > 0) {
            if (map_column.repeated_level_layouts.size() !=
                static_cast<std::size_t>(child_list_depth + 1)) {
              return sanitize::Status::NotImplemented(
                  "native Parquet reader: struct map nested list chain layout "
                  "was not decoded");
            }
            std::vector<NativeParquetListArray *> chain_arrays;
            chain_arrays.reserve(static_cast<std::size_t>(child_list_depth));
            for (std::int16_t level = 0; level < child_list_depth; ++level) {
              chain_arrays.push_back(&state->lists[list_index++]);
            }
            for (std::int16_t level = child_list_depth; level >= 1; --level) {
              const auto layout_index = static_cast<std::size_t>(level);
              const auto array_index = static_cast<std::size_t>(level - 1);
              const auto &level_layout =
                  map_column.repeated_level_layouts[layout_index];
              if (!level_layout.decoded) {
                return sanitize::Status::NotImplemented(
                    "native Parquet reader: struct map nested list chain "
                    "level was not decoded");
              }
              auto &inner_array = *chain_arrays[array_index];
              inner_array.validity = level_layout.validity_bitmap;
              inner_array.offsets = level_layout.offsets;
              inner_array.children[0] =
                  (level == child_list_depth)
                      ? &state->columns[map_column_index].array
                      : &chain_arrays[array_index + 1]->array;
              inner_array.array.length = level_layout.row_count;
              inner_array.array.null_count = level_layout.null_count;
              inner_array.array.offset = 0;
              inner_array.array.n_buffers = 2;
              inner_array.buffers[0] = inner_array.validity.empty()
                                           ? nullptr
                                           : inner_array.validity.data();
              inner_array.buffers[1] = inner_array.offsets.empty()
                                           ? nullptr
                                           : inner_array.offsets.data();
              inner_array.array.buffers = inner_array.buffers.data();
              inner_array.array.n_children = 1;
              inner_array.array.children = inner_array.children.data();
              inner_array.array.dictionary = nullptr;
              inner_array.array.private_data = nullptr;
              inner_array.array.release = &native_parquet_array_child_release;
            }
            entries_array.children.push_back(&chain_arrays.front()->array);
          } else {
            entries_array.children.push_back(
                &state->columns[map_column_index].array);
          }
        }
        entries_array.array.length = child_column.repeated_level_element_count;
        entries_array.array.null_count = 0;
        entries_array.array.offset = 0;
        entries_array.array.n_buffers = 1;
        entries_array.buffers[0] = nullptr;
        entries_array.array.buffers = entries_array.buffers.data();
        entries_array.array.n_children =
            static_cast<std::int64_t>(entries_array.children.size());
        entries_array.array.children = entries_array.children.empty()
                                           ? nullptr
                                           : entries_array.children.data();
        entries_array.array.dictionary = nullptr;
        entries_array.array.private_data = nullptr;
        entries_array.array.release = &native_parquet_array_child_release;

        map_array.validity = child_column.repeated_level_validity_bitmap;
        map_array.offsets = child_column.repeated_level_offsets;
        map_array.children[0] = &entries_array.array;
        map_array.array.length = row_group.num_rows;
        map_array.array.null_count = child_column.repeated_level_null_count;
        map_array.array.offset = 0;
        map_array.array.n_buffers = 2;
        map_array.buffers[0] =
            map_array.validity.empty() ? nullptr : map_array.validity.data();
        map_array.buffers[1] =
            map_array.offsets.empty() ? nullptr : map_array.offsets.data();
        map_array.array.buffers = map_array.buffers.data();
        map_array.array.n_children = 1;
        map_array.array.children = map_array.children.data();
        map_array.array.dictionary = nullptr;
        map_array.array.private_data = nullptr;
        map_array.array.release = &native_parquet_array_child_release;
        struct_array.children.push_back(&map_array.array);
      } else {
        struct_array.children.push_back(&state->columns[column_index].array);
      }
    }
    struct_array.array.length = row_group.num_rows;
    if (field.top_level_required) {
      struct_array.array.null_count = 0;
      struct_array.buffers[0] = nullptr;
    } else {
      if (!struct_validity_column) {
        return sanitize::Status::Invalid(
            "native Parquet reader: struct validity column is missing");
      }
      SAN_ASSIGN_OR_RAISE(const auto null_count,
                          materialize_optional_struct_validity(
                              *struct_validity_column, row_group.num_rows,
                              &struct_array.validity));
      struct_array.array.null_count = null_count;
      struct_array.buffers[0] = struct_array.validity.empty()
                                    ? nullptr
                                    : struct_array.validity.data();
    }
    struct_array.array.offset = 0;
    struct_array.array.n_buffers = 1;
    struct_array.array.buffers = struct_array.buffers.data();
    struct_array.array.n_children =
        static_cast<std::int64_t>(struct_array.children.size());
    struct_array.array.children =
        struct_array.children.empty() ? nullptr : struct_array.children.data();
    struct_array.array.dictionary = nullptr;
    struct_array.array.private_data = nullptr;
    struct_array.array.release = &native_parquet_array_child_release;
    state->children.push_back(&struct_array.array);
  }
  sanitize::internal::cdata_stream::clear_array(out);
  out->length = row_group.num_rows;
  out->null_count = 0;
  out->offset = 0;
  out->n_buffers = 1;
  out->buffers = state->struct_buffers.data();
  out->n_children = static_cast<std::int64_t>(state->children.size());
  out->children = state->children.empty() ? nullptr : state->children.data();
  out->dictionary = nullptr;
  out->private_data = state.release();
  out->release = &native_parquet_array_release;
  return {};
}

const char *native_parquet_stream_last_error(ArrowArrayStream *stream) {
  if (!stream) {
    return "invalid native Parquet reader stream";
  }
  auto *state = static_cast<NativeParquetStreamState *>(stream->private_data);
  return state ? sanitize::internal::cdata_stream::last_error_ptr(
                     state->last_error)
               : nullptr;
}

void native_parquet_stream_release(ArrowArrayStream *stream) {
  if (!stream || !stream->release) {
    return;
  }
  auto *state = static_cast<NativeParquetStreamState *>(stream->private_data);
  delete state;
  sanitize::internal::cdata_stream::clear_stream(stream);
}

int native_parquet_stream_get_schema(ArrowArrayStream *stream,
                                     ArrowSchema *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *state = static_cast<NativeParquetStreamState *>(stream->private_data);
  if (!state) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_schema_callback(
      out, state->last_error, "native_parquet_stream.get_schema",
      [&](ArrowSchema *schema) {
        return build_native_schema(state->footer, schema);
      });
}

int native_parquet_stream_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *state = static_cast<NativeParquetStreamState *>(stream->private_data);
  if (!state) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_array_callback(
      out, state->last_error, "native_parquet_stream.get_next",
      [&](ArrowArray *array) {
        return build_native_row_group_array(state, array);
      });
}

} // namespace

sanitize::Result<FooterInfo> read_footer_info(const std::string &path) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    return sanitize::Status::IOError("Parquet footer: failed opening input");
  }
  file.seekg(0, std::ios::end);
  const auto end_pos = file.tellg();
  if (end_pos < static_cast<std::streampos>(8)) {
    return sanitize::Status::Invalid("Parquet footer: file is too small");
  }
  const auto file_size = static_cast<std::uint64_t>(end_pos);
  std::array<char, 8> trailer{};
  file.seekg(-8, std::ios::end);
  file.read(trailer.data(), static_cast<std::streamsize>(trailer.size()));
  if (!file) {
    return sanitize::Status::IOError("Parquet footer: failed reading trailer");
  }
  if (std::string_view(trailer.data() + 4, 4) != kParquetMagic) {
    return sanitize::Status::Invalid("Parquet footer: invalid trailing magic");
  }
  const auto footer_len =
      static_cast<std::uint64_t>(read_u32_le(trailer.data()));
  if (footer_len == 0 || footer_len > file_size - 8 ||
      file_size - 8 - footer_len < 4) {
    return sanitize::Status::Invalid("Parquet footer: invalid footer length");
  }
  std::string footer(static_cast<std::size_t>(footer_len), '\0');
  file.seekg(static_cast<std::streamoff>(file_size - 8 - footer_len),
             std::ios::beg);
  file.read(footer.data(), static_cast<std::streamsize>(footer.size()));
  if (!file) {
    return sanitize::Status::IOError("Parquet footer: failed reading footer");
  }
  FooterInfo info;
  SAN_ASSIGN_OR_RAISE(info, parse_footer(footer));
  SAN_RETURN_NOT_OK(assign_column_levels(&info));
  SAN_RETURN_NOT_OK(read_page_headers(file, &info));
  SAN_RETURN_NOT_OK(read_page_indexes(file, &info));
  SAN_RETURN_NOT_OK(assign_native_read_page_spans(&info));
  return info;
}

sanitize::Result<std::string> read_footer_info_json(const std::string &path) {
  FooterInfo info;
  SAN_ASSIGN_OR_RAISE(info, read_footer_info(path));
  std::string out;
  out.push_back('{');
  bool first = true;
  json_write::append_int_field(out, first, "version", info.version);
  json_write::append_int_field(out, first, "num_rows", info.num_rows);
  json_write::append_int_field(out, first, "schema_element_count",
                               info.schema_element_count);
  json_write::append_int_field(out, first, "row_group_count",
                               info.row_group_count);
  json_write::append_string_field(out, first, "created_by", info.created_by);
  const auto readiness = native_reader_readiness(info);
  json_write::append_int_field(out, first, "native_reader_ready",
                               readiness.ready ? 1 : 0);
  json_write::append_key(out, first, "native_reader_blockers");
  append_string_array(out, readiness.blockers);
  json_write::append_key(out, first, "schema_elements");
  out.push_back('[');
  for (std::size_t i = 0; i < info.schema_elements.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    const auto &element = info.schema_elements[i];
    out.push_back('{');
    bool element_first = true;
    json_write::append_string_field(out, element_first, "name", element.name);
    if (element.has_physical_type) {
      json_write::append_int_field(out, element_first, "physical_type",
                                   element.physical_type);
    }
    if (element.has_type_length) {
      json_write::append_int_field(out, element_first, "type_length",
                                   element.type_length);
    }
    if (element.has_repetition_type) {
      json_write::append_int_field(out, element_first, "repetition_type",
                                   element.repetition_type);
    }
    if (element.has_num_children) {
      json_write::append_int_field(out, element_first, "num_children",
                                   element.num_children);
    }
    if (element.has_converted_type) {
      json_write::append_int_field(out, element_first, "converted_type",
                                   element.converted_type);
    }
    if (element.has_decimal_scale) {
      json_write::append_int_field(out, element_first, "decimal_scale",
                                   element.decimal_scale);
    }
    if (element.has_decimal_precision) {
      json_write::append_int_field(out, element_first, "decimal_precision",
                                   element.decimal_precision);
    }
    if (!element.logical_type.empty()) {
      json_write::append_string_field(out, element_first, "logical_type",
                                      element.logical_type);
    }
    if (!element.logical_type_time_unit.empty()) {
      json_write::append_string_field(out, element_first,
                                      "logical_type_time_unit",
                                      element.logical_type_time_unit);
    }
    if (element.has_logical_type_is_adjusted_to_utc) {
      json_write::append_int_field(
          out, element_first, "logical_type_is_adjusted_to_utc",
          element.logical_type_is_adjusted_to_utc ? 1 : 0);
    }
    if (element.has_logical_type_integer_bit_width) {
      json_write::append_int_field(out, element_first,
                                   "logical_type_integer_bit_width",
                                   element.logical_type_integer_bit_width);
    }
    if (element.has_logical_type_integer_is_signed) {
      json_write::append_int_field(
          out, element_first, "logical_type_integer_is_signed",
          element.logical_type_integer_is_signed ? 1 : 0);
    }
    out.push_back('}');
  }
  out.push_back(']');
  json_write::append_key(out, first, "row_groups");
  out.push_back('[');
  for (std::size_t i = 0; i < info.row_groups.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    const auto &row_group = info.row_groups[i];
    out.push_back('{');
    bool row_group_first = true;
    if (row_group.has_num_rows) {
      json_write::append_int_field(out, row_group_first, "num_rows",
                                   row_group.num_rows);
    }
    if (row_group.has_total_byte_size) {
      json_write::append_int_field(out, row_group_first, "total_byte_size",
                                   row_group.total_byte_size);
    }
    json_write::append_key(out, row_group_first, "columns");
    out.push_back('[');
    for (std::size_t j = 0; j < row_group.columns.size(); ++j) {
      if (j > 0) {
        out.push_back(',');
      }
      const auto &column = row_group.columns[j];
      out.push_back('{');
      bool column_first = true;
      json_write::append_key(out, column_first, "path_in_schema");
      append_string_array(out, column.path_in_schema);
      if (column.has_physical_type) {
        json_write::append_int_field(out, column_first, "physical_type",
                                     column.physical_type);
      }
      json_write::append_key(out, column_first, "encodings");
      append_int_array(out, column.encodings);
      if (column.has_codec) {
        json_write::append_int_field(out, column_first, "codec", column.codec);
      }
      if (column.has_num_values) {
        json_write::append_int_field(out, column_first, "num_values",
                                     column.num_values);
      }
      if (column.has_total_uncompressed_size) {
        json_write::append_int_field(out, column_first,
                                     "total_uncompressed_size",
                                     column.total_uncompressed_size);
      }
      if (column.has_total_compressed_size) {
        json_write::append_int_field(out, column_first, "total_compressed_size",
                                     column.total_compressed_size);
      }
      if (column.has_file_offset) {
        json_write::append_int_field(out, column_first, "file_offset",
                                     column.file_offset);
      }
      if (column.has_data_page_offset) {
        json_write::append_int_field(out, column_first, "data_page_offset",
                                     column.data_page_offset);
      }
      if (column.has_dictionary_page_offset) {
        json_write::append_int_field(out, column_first,
                                     "dictionary_page_offset",
                                     column.dictionary_page_offset);
      }
      if (column.has_column_index_offset) {
        json_write::append_int_field(out, column_first, "column_index_offset",
                                     column.column_index_offset);
      }
      if (column.has_column_index_length) {
        json_write::append_int_field(out, column_first, "column_index_length",
                                     column.column_index_length);
      }
      if (column.has_offset_index_offset) {
        json_write::append_int_field(out, column_first, "offset_index_offset",
                                     column.offset_index_offset);
      }
      if (column.has_offset_index_length) {
        json_write::append_int_field(out, column_first, "offset_index_length",
                                     column.offset_index_length);
      }
      json_write::append_int_field(out, column_first, "max_definition_level",
                                   column.max_definition_level);
      json_write::append_int_field(out, column_first, "max_repetition_level",
                                   column.max_repetition_level);
      if (column.fixed_type_length > 0) {
        json_write::append_int_field(out, column_first, "fixed_type_length",
                                     column.fixed_type_length);
      }
      if (!column.native_arrow_format.empty()) {
        json_write::append_string_field(out, column_first,
                                        "native_arrow_format",
                                        column.native_arrow_format);
      }
      json_write::append_int_field(out, column_first, "column_index_decoded",
                                   column.column_index.decoded ? 1 : 0);
      if (column.column_index.decoded) {
        json_write::append_key(out, column_first, "column_index_null_pages");
        append_bool_array(out, column.column_index.null_pages);
        json_write::append_key(out, column_first, "column_index_min_hex");
        append_hex_string_array(out, column.column_index.min_values);
        json_write::append_key(out, column_first, "column_index_max_hex");
        append_hex_string_array(out, column.column_index.max_values);
        if (column.column_index.has_boundary_order) {
          json_write::append_int_field(out, column_first,
                                       "column_index_boundary_order",
                                       column.column_index.boundary_order);
        }
        json_write::append_key(out, column_first, "column_index_null_counts");
        append_int64_array(out, column.column_index.null_counts);
      }
      json_write::append_int_field(out, column_first, "offset_index_decoded",
                                   column.offset_index.decoded ? 1 : 0);
      if (column.offset_index.decoded) {
        json_write::append_key(out, column_first, "offset_index_locations");
        append_page_locations(out, column.offset_index.locations);
      }
      json_write::append_int_field(out, column_first,
                                   "native_read_plan_decoded",
                                   column.native_read_plan_decoded ? 1 : 0);
      if (column.native_read_plan_decoded) {
        json_write::append_int_field(out, column_first,
                                     "native_read_data_page_count",
                                     column.native_read_data_page_count);
        json_write::append_int_field(out, column_first,
                                     "native_read_total_rows",
                                     column.native_read_total_rows);
        json_write::append_int_field(out, column_first,
                                     "native_read_total_non_nulls",
                                     column.native_read_total_non_nulls);
        json_write::append_int_field(out, column_first,
                                     "native_read_total_nulls",
                                     column.native_read_total_nulls);
        json_write::append_int_field(out, column_first,
                                     "native_read_validity_bitmap_bytes",
                                     column.native_read_validity_bitmap_bytes);
        json_write::append_int_field(out, column_first,
                                     "native_read_value_payload_bytes",
                                     column.native_read_value_payload_bytes);
        json_write::append_int_field(
            out, column_first, "native_read_materialized_value_bytes",
            column.native_read_materialized_value_bytes);
        json_write::append_int_field(
            out, column_first, "native_read_materialized_offset_bytes",
            column.native_read_materialized_offset_bytes);
        json_write::append_int_field(out, column_first,
                                     "native_read_value_width_bytes",
                                     column.native_read_value_width_bytes);
        json_write::append_int_field(
            out, column_first, "native_read_dictionary_index_bit_width",
            column.native_read_dictionary_index_bit_width);
        json_write::append_string_field(out, column_first,
                                        "native_read_value_buffer_kind",
                                        column.native_read_value_buffer_kind);
        json_write::append_int_field(out, column_first,
                                     "native_read_arrow_length",
                                     column.native_read_arrow_length);
        json_write::append_int_field(out, column_first,
                                     "native_read_arrow_null_count",
                                     column.native_read_arrow_null_count);
        json_write::append_int_field(out, column_first,
                                     "native_read_arrow_n_buffers",
                                     column.native_read_arrow_n_buffers);
        json_write::append_int_field(out, column_first,
                                     "native_read_arrow_n_children",
                                     column.native_read_arrow_n_children);
        json_write::append_int_field(out, column_first,
                                     "native_read_has_validity_buffer",
                                     column.native_read_has_validity_buffer);
        json_write::append_int_field(out, column_first,
                                     "native_read_has_offsets_buffer",
                                     column.native_read_has_offsets_buffer);
        json_write::append_int_field(out, column_first,
                                     "native_read_has_values_buffer",
                                     column.native_read_has_values_buffer);
        json_write::append_key(out, column_first, "native_read_page_spans");
        append_native_read_page_spans(out, column.native_read_page_spans);
      }
      json_write::append_int_field(
          out, column_first, "repeated_level_layout_decoded",
          column.repeated_level_layout_decoded ? 1 : 0);
      if (column.repeated_level_layout_decoded) {
        json_write::append_int_field(out, column_first,
                                     "repeated_level_row_count",
                                     column.repeated_level_row_count);
        json_write::append_int_field(out, column_first,
                                     "repeated_level_null_count",
                                     column.repeated_level_null_count);
        json_write::append_int_field(out, column_first,
                                     "repeated_level_element_count",
                                     column.repeated_level_element_count);
        json_write::append_int_field(
            out, column_first, "repeated_level_non_null_value_count",
            column.repeated_level_non_null_value_count);
        json_write::append_key(out, column_first, "repeated_level_offsets");
        append_int_array(out, column.repeated_level_offsets);
        json_write::append_string_field(
            out, column_first, "repeated_level_validity_hex_preview",
            hex_bytes_preview(column.repeated_level_validity_bitmap));
      }
      json_write::append_int_field(
          out, column_first, "nested_repeated_level_layout_decoded",
          column.nested_repeated_level_layout_decoded ? 1 : 0);
      if (column.nested_repeated_level_layout_decoded) {
        json_write::append_int_field(out, column_first,
                                     "nested_repeated_level_row_count",
                                     column.nested_repeated_level_row_count);
        json_write::append_int_field(out, column_first,
                                     "nested_repeated_level_null_count",
                                     column.nested_repeated_level_null_count);
        json_write::append_int_field(
            out, column_first, "nested_repeated_level_element_count",
            column.nested_repeated_level_element_count);
        json_write::append_int_field(
            out, column_first, "nested_repeated_level_non_null_value_count",
            column.nested_repeated_level_non_null_value_count);
        json_write::append_key(out, column_first,
                               "nested_repeated_level_offsets");
        append_int_array(out, column.nested_repeated_level_offsets);
        json_write::append_string_field(
            out, column_first, "nested_repeated_level_validity_hex_preview",
            hex_bytes_preview(column.nested_repeated_level_validity_bitmap));
      }
      json_write::append_int_field(
          out, column_first, "deep_repeated_level_layout_decoded",
          column.deep_repeated_level_layout_decoded ? 1 : 0);
      if (column.deep_repeated_level_layout_decoded) {
        json_write::append_int_field(out, column_first,
                                     "deep_repeated_level_row_count",
                                     column.deep_repeated_level_row_count);
        json_write::append_int_field(out, column_first,
                                     "deep_repeated_level_null_count",
                                     column.deep_repeated_level_null_count);
        json_write::append_int_field(out, column_first,
                                     "deep_repeated_level_element_count",
                                     column.deep_repeated_level_element_count);
        json_write::append_int_field(
            out, column_first, "deep_repeated_level_non_null_value_count",
            column.deep_repeated_level_non_null_value_count);
        json_write::append_key(out, column_first,
                               "deep_repeated_level_offsets");
        append_int_array(out, column.deep_repeated_level_offsets);
        json_write::append_string_field(
            out, column_first, "deep_repeated_level_validity_hex_preview",
            hex_bytes_preview(column.deep_repeated_level_validity_bitmap));
      }
      json_write::append_key(out, column_first, "pages");
      out.push_back('[');
      for (std::size_t k = 0; k < column.pages.size(); ++k) {
        if (k > 0) {
          out.push_back(',');
        }
        const auto &page = column.pages[k];
        out.push_back('{');
        bool page_first = true;
        json_write::append_int_field(out, page_first, "header_offset",
                                     page.header_offset);
        json_write::append_int_field(out, page_first, "header_size",
                                     page.header_size);
        json_write::append_int_field(out, page_first,
                                     "compressed_payload_offset",
                                     page.compressed_payload_offset);
        if (page.has_type) {
          json_write::append_int_field(out, page_first, "type", page.type);
        }
        if (page.has_uncompressed_page_size) {
          json_write::append_int_field(out, page_first,
                                       "uncompressed_page_size",
                                       page.uncompressed_page_size);
        }
        if (page.has_compressed_page_size) {
          json_write::append_int_field(out, page_first, "compressed_page_size",
                                       page.compressed_page_size);
        }
        if (page.has_num_values) {
          json_write::append_int_field(out, page_first, "num_values",
                                       page.num_values);
        }
        if (page.has_value_encoding) {
          json_write::append_int_field(out, page_first, "value_encoding",
                                       page.value_encoding);
        }
        if (page.has_definition_level_encoding) {
          json_write::append_int_field(out, page_first,
                                       "definition_level_encoding",
                                       page.definition_level_encoding);
        }
        if (page.has_repetition_level_encoding) {
          json_write::append_int_field(out, page_first,
                                       "repetition_level_encoding",
                                       page.repetition_level_encoding);
        }
        if (page.has_decompressed_page_size) {
          json_write::append_int_field(out, page_first,
                                       "decompressed_page_size",
                                       page.decompressed_page_size);
        }
        json_write::append_int_field(out, page_first, "payload_verified",
                                     page.payload_verified ? 1 : 0);
        json_write::append_int_field(out, page_first,
                                     "payload_verification_skipped",
                                     page.payload_verification_skipped ? 1 : 0);
        json_write::append_int_field(out, page_first, "levels_decoded",
                                     page.levels_decoded ? 1 : 0);
        if (page.levels_decoded) {
          json_write::append_int_field(out, page_first,
                                       "decoded_definition_levels",
                                       page.decoded_definition_levels);
          if (!page.decoded_definition_level_values.empty()) {
            json_write::append_key(out, page_first,
                                   "decoded_definition_level_values");
            append_int16_array(out, page.decoded_definition_level_values);
          }
          json_write::append_int_field(out, page_first,
                                       "decoded_repetition_levels",
                                       page.decoded_repetition_levels);
          if (!page.decoded_repetition_level_values.empty()) {
            json_write::append_key(out, page_first,
                                   "decoded_repetition_level_values");
            append_int16_array(out, page.decoded_repetition_level_values);
          }
          json_write::append_int_field(out, page_first, "value_payload_offset",
                                       page.value_payload_offset);
          json_write::append_int_field(out, page_first,
                                       "decoded_non_null_values",
                                       page.decoded_non_null_values);
          json_write::append_int_field(out, page_first, "decoded_null_values",
                                       page.decoded_null_values);
          json_write::append_int_field(out, page_first,
                                       "validity_bitmap_decoded",
                                       page.validity_bitmap_decoded ? 1 : 0);
          if (page.validity_bitmap_decoded) {
            json_write::append_int_field(out, page_first,
                                         "decoded_validity_bytes",
                                         page.decoded_validity_bytes);
            json_write::append_string_field(
                out, page_first, "decoded_validity_hex_preview",
                hex_bytes_preview(page.decoded_validity_bitmap));
          }
        }
        json_write::append_int_field(out, page_first, "values_decoded",
                                     page.values_decoded ? 1 : 0);
        json_write::append_int_field(out, page_first, "values_decode_skipped",
                                     page.values_decode_skipped ? 1 : 0);
        if (page.values_decoded) {
          json_write::append_int_field(out, page_first, "decoded_value_bytes",
                                       page.decoded_value_bytes);
          json_write::append_int_field(out, page_first,
                                       "materialized_value_bytes",
                                       page.materialized_value_bytes);
          json_write::append_int_field(out, page_first,
                                       "materialized_offset_bytes",
                                       page.materialized_offset_bytes);
          json_write::append_int_field(out, page_first,
                                       "dictionary_index_bit_width",
                                       page.dictionary_index_bit_width);
          json_write::append_key(out, page_first, "decoded_value_preview");
          append_string_array(out, page.decoded_value_preview);
        }
        json_write::append_int_field(out, page_first, "is_dictionary_page",
                                     page.is_dictionary_page ? 1 : 0);
        if (page.is_dictionary_page) {
          json_write::append_int_field(out, page_first, "dictionary_is_sorted",
                                       page.dictionary_is_sorted ? 1 : 0);
        }
        out.push_back('}');
      }
      out.push_back(']');
      out.push_back('}');
    }
    out.push_back(']');
    out.push_back('}');
  }
  out.push_back(']');
  out.push_back('}');
  return out;
}

sanitize::Status project_footer_row_group_columns(
    FooterInfo *info, const std::vector<std::string> &projected_columns) {
  if (!info || projected_columns.empty()) {
    return {};
  }
  info->projected_columns = projected_columns;
  for (auto &row_group : info->row_groups) {
    std::vector<ColumnChunkInfo> selected;
    selected.reserve(row_group.columns.size());
    for (const auto &name : projected_columns) {
      auto before_count = selected.size();
      for (const auto &column : row_group.columns) {
        if (!column.path_in_schema.empty() &&
            column.path_in_schema.front() == name) {
          selected.push_back(column);
        }
      }
      if (selected.size() == before_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: projection column not found: ", name);
      }
    }
    row_group.columns = std::move(selected);
  }
  return {};
}

sanitize::Result<ArrowArrayStream *>
make_arrow_stream(const std::string &path,
                  const std::vector<std::string> &projected_columns) {
  FooterInfo info;
  SAN_ASSIGN_OR_RAISE(info, read_footer_info(path));
  SAN_RETURN_NOT_OK(project_footer_row_group_columns(&info, projected_columns));
  const auto readiness = native_reader_readiness(info);
  if (!readiness.ready) {
    std::string message = "native Parquet reader: file is not ready";
    if (!readiness.blockers.empty()) {
      message += ": ";
      message += readiness.blockers.front();
    }
    return sanitize::Status::NotImplemented(message);
  }
  for (const auto &row_group : info.row_groups) {
    for (const auto &column : row_group.columns) {
      SAN_RETURN_NOT_OK(validate_native_plain_column(column));
    }
  }
  auto state = std::unique_ptr<NativeParquetStreamState>(
      new (std::nothrow) NativeParquetStreamState());
  if (!state) {
    return sanitize::Status::OutOfMemory("native Parquet reader stream OOM");
  }
  state->path = path;
  state->file.open(path, std::ios::binary);
  if (!state->file) {
    return sanitize::Status::IOError(
        "native Parquet reader: failed opening input");
  }
  state->footer = std::move(info);

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    return sanitize::Status::OutOfMemory("native Parquet reader stream OOM");
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &native_parquet_stream_get_schema;
  stream->get_next = &native_parquet_stream_get_next;
  stream->get_last_error = &native_parquet_stream_last_error;
  stream->release = &native_parquet_stream_release;
  stream->private_data = state.release();
  return stream;
}

} // namespace sanitize::internal::parquet_footer_reader
