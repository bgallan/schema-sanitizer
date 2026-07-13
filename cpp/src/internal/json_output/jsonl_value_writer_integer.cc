// Implements integer Arrow value JSON serialization.

#include "internal/json_output/jsonl_value_writer_parts.hh"

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <string>
#include <system_error>

namespace sanitize::internal::jsonl_stream_writer {
namespace {

template <typename T> const T *data_buffer(const ArrowArray &array) {
  if (!array.buffers || !array.buffers[1]) {
    return nullptr;
  }
  return static_cast<const T *>(array.buffers[1]);
}

template <typename T>
sanitize::Status append_signed(std::string &out, const ArrowArray &array,
                               int64_t row) {
  const T *values = data_buffer<T>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing integer buffer");
  }
  std::array<char, 32> buffer{};
  auto [ptr, ec] =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(),
                    static_cast<int64_t>(values[array.offset + row]));
  if (ec != std::errc()) {
    return sanitize::Status::Invalid("JSONL writer: integer formatting failed");
  }
  out.append(buffer.data(), static_cast<std::size_t>(ptr - buffer.data()));
  return sanitize::Status::OK();
}

template <typename T>
sanitize::Status append_unsigned(std::string &out, const ArrowArray &array,
                                 int64_t row) {
  const T *values = data_buffer<T>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing integer buffer");
  }
  std::array<char, 32> buffer{};
  auto [ptr, ec] =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(),
                    static_cast<uint64_t>(values[array.offset + row]));
  if (ec != std::errc()) {
    return sanitize::Status::Invalid("JSONL writer: integer formatting failed");
  }
  out.append(buffer.data(), static_cast<std::size_t>(ptr - buffer.data()));
  return sanitize::Status::OK();
}

} // namespace

sanitize::Status append_int8_value(std::string &out, const ArrowArray &array,
                                   int64_t row) {
  return append_signed<int8_t>(out, array, row);
}

sanitize::Status append_uint8_value(std::string &out, const ArrowArray &array,
                                    int64_t row) {
  return append_unsigned<uint8_t>(out, array, row);
}

sanitize::Status append_int16_value(std::string &out, const ArrowArray &array,
                                    int64_t row) {
  return append_signed<int16_t>(out, array, row);
}

sanitize::Status append_uint16_value(std::string &out, const ArrowArray &array,
                                     int64_t row) {
  return append_unsigned<uint16_t>(out, array, row);
}

sanitize::Status append_int32_value(std::string &out, const ArrowArray &array,
                                    int64_t row) {
  return append_signed<int32_t>(out, array, row);
}

sanitize::Status append_uint32_value(std::string &out, const ArrowArray &array,
                                     int64_t row) {
  return append_unsigned<uint32_t>(out, array, row);
}

sanitize::Status append_int64_value(std::string &out, const ArrowArray &array,
                                    int64_t row) {
  return append_signed<int64_t>(out, array, row);
}

sanitize::Status append_uint64_value(std::string &out, const ArrowArray &array,
                                     int64_t row) {
  return append_unsigned<uint64_t>(out, array, row);
}

} // namespace sanitize::internal::jsonl_stream_writer
