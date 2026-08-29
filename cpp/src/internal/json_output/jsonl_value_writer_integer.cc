// Implements integer Arrow value JSON serialization.
// The code validates Arrow layouts and emits deterministic JSON with correct
// null and logical-type semantics.

#include "internal/json_output/jsonl_value_writer_parts.hh"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>

namespace sanitize::internal::jsonl_stream_writer {
namespace {

/// Returns the typed Arrow values buffer after verifying that the buffer
/// pointer is available.
template <typename T> const T *data_buffer(const ArrowArray &array) {
  if (!array.buffers || !array.buffers[1]) {
    return nullptr;
  }
  return static_cast<const T *>(array.buffers[1]);
}

inline constexpr std::array<char, 200> kDecimalDigitPairs = [] {
  std::array<char, 200> digits{};
  for (std::size_t value = 0; value < 100; ++value) {
    digits[value * 2] = static_cast<char>('0' + value / 10);
    digits[value * 2 + 1] = static_cast<char>('0' + value % 10);
  }
  return digits;
}();

template <typename UInt>
/// Emits an unsigned integer with a two-digit lookup table and no temporary
/// string.
void append_unsigned_decimal(TextBuffer &out, UInt value) {
  static_assert(std::is_unsigned_v<UInt>);
  std::array<char, 32> buffer{};
  char *cursor = buffer.data() + buffer.size();
  while (value >= 100) {
    const auto quotient = value / 100;
    const auto remainder = static_cast<std::size_t>(value - quotient * 100);
    cursor -= 2;
    cursor[0] = kDecimalDigitPairs[remainder * 2];
    cursor[1] = kDecimalDigitPairs[remainder * 2 + 1];
    value = quotient;
  }
  if (value < 10) {
    *--cursor = static_cast<char>('0' + value);
  } else {
    cursor -= 2;
    const auto index = static_cast<std::size_t>(value) * 2;
    cursor[0] = kDecimalDigitPairs[index];
    cursor[1] = kDecimalDigitPairs[index + 1];
  }
  out.append(cursor, buffer.data() + buffer.size());
}

template <typename T>
/// Reads one signed Arrow integer and emits its decimal representation.
sanitize::Status append_signed(TextBuffer &out, const ArrowArray &array,
                               int64_t row) {
  const T *values = data_buffer<T>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing integer buffer");
  }
  const auto value = static_cast<int64_t>(values[array.offset + row]);
  if (value < 0) {
    out.push_back('-');
    const auto magnitude = static_cast<uint64_t>(-(value + 1)) + 1;
    append_unsigned_decimal(out, magnitude);
  } else {
    append_unsigned_decimal(out, static_cast<uint64_t>(value));
  }
  return sanitize::Status::OK();
}

template <typename T>
/// Reads one unsigned Arrow integer and emits its decimal representation.
sanitize::Status append_unsigned(TextBuffer &out, const ArrowArray &array,
                                 int64_t row) {
  const T *values = data_buffer<T>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing integer buffer");
  }
  append_unsigned_decimal(out,
                          static_cast<uint64_t>(values[array.offset + row]));
  return sanitize::Status::OK();
}

} // namespace

sanitize::Status append_int8_value(TextBuffer &out, const ArrowArray &array,
                                   int64_t row) {
  return append_signed<int8_t>(out, array, row);
}

/// Serializes one unsigned 8-bit Arrow value as a JSON integer.
sanitize::Status append_uint8_value(TextBuffer &out, const ArrowArray &array,
                                    int64_t row) {
  return append_unsigned<uint8_t>(out, array, row);
}

/// Serializes one signed 16-bit Arrow value as a JSON integer.
sanitize::Status append_int16_value(TextBuffer &out, const ArrowArray &array,
                                    int64_t row) {
  return append_signed<int16_t>(out, array, row);
}

/// Serializes one unsigned 16-bit Arrow value as a JSON integer.
sanitize::Status append_uint16_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row) {
  return append_unsigned<uint16_t>(out, array, row);
}

/// Serializes one signed 32-bit Arrow value as a JSON integer.
sanitize::Status append_int32_value(TextBuffer &out, const ArrowArray &array,
                                    int64_t row) {
  return append_signed<int32_t>(out, array, row);
}

/// Serializes one unsigned 32-bit Arrow value as a JSON integer.
sanitize::Status append_uint32_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row) {
  return append_unsigned<uint32_t>(out, array, row);
}

/// Serializes one signed 64-bit Arrow value as a JSON integer.
sanitize::Status append_int64_value(TextBuffer &out, const ArrowArray &array,
                                    int64_t row) {
  return append_signed<int64_t>(out, array, row);
}

/// Serializes one unsigned 64-bit Arrow value as a JSON integer.
sanitize::Status append_uint64_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row) {
  return append_unsigned<uint64_t>(out, array, row);
}

} // namespace sanitize::internal::jsonl_stream_writer
