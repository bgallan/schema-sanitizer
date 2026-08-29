// Implements string and binary JSON serialization for Arrow values.
// The code validates Arrow layouts and emits deterministic JSON with correct
// null and logical-type semantics.

#include "internal/json_output/jsonl_value_writer_parts.hh"

#include "internal/json_encoding/token_writer.hh"

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace sanitize::internal::jsonl_stream_writer {
namespace {

template <typename OffsetT>
/// Reads one offset-delimited Arrow string and emits escaped JSON text.
sanitize::Status append_string_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row) {
  if (!array.buffers || !array.buffers[1] || !array.buffers[2]) {
    return sanitize::Status::Invalid("JSONL writer: missing string buffer");
  }
  const auto *offsets = static_cast<const OffsetT *>(array.buffers[1]);
  const auto *data = static_cast<const char *>(array.buffers[2]);
  const int64_t slot = array.offset + row;
  const auto begin = offsets[slot];
  const auto end = offsets[slot + 1];
  if (begin < 0 || end < begin) {
    return sanitize::Status::Invalid("JSONL writer: invalid string offsets");
  }
  sanitize::internal::json_encoding::append_string(
      out,
      std::string_view(data + begin, static_cast<std::size_t>(end - begin)));
  return sanitize::Status::OK();
}

/// Encodes arbitrary bytes as unquoted RFC 4648 base64 text.
void append_base64(TextBuffer &out, std::string_view value) {
  static constexpr char kAlphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::size_t i = 0;
  while (i + 3 <= value.size()) {
    const auto b0 = static_cast<unsigned char>(value[i]);
    const auto b1 = static_cast<unsigned char>(value[i + 1]);
    const auto b2 = static_cast<unsigned char>(value[i + 2]);
    out.push_back(kAlphabet[b0 >> 2]);
    out.push_back(kAlphabet[((b0 & 0x03u) << 4) | (b1 >> 4)]);
    out.push_back(kAlphabet[((b1 & 0x0Fu) << 2) | (b2 >> 6)]);
    out.push_back(kAlphabet[b2 & 0x3Fu]);
    i += 3;
  }
  const std::size_t remaining = value.size() - i;
  if (remaining == 1) {
    const auto b0 = static_cast<unsigned char>(value[i]);
    out.push_back(kAlphabet[b0 >> 2]);
    out.push_back(kAlphabet[(b0 & 0x03u) << 4]);
    out += "==";
  } else if (remaining == 2) {
    const auto b0 = static_cast<unsigned char>(value[i]);
    const auto b1 = static_cast<unsigned char>(value[i + 1]);
    out.push_back(kAlphabet[b0 >> 2]);
    out.push_back(kAlphabet[((b0 & 0x03u) << 4) | (b1 >> 4)]);
    out.push_back(kAlphabet[(b1 & 0x0Fu) << 2]);
    out.push_back('=');
  }
}

template <typename OffsetT>
/// Reads one offset-delimited Arrow binary value and emits optionally quoted
/// base64.
sanitize::Status append_binary_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row, bool quote) {
  if (!array.buffers || !array.buffers[1] || !array.buffers[2]) {
    return sanitize::Status::Invalid("JSONL writer: missing binary buffer");
  }
  const auto *offsets = static_cast<const OffsetT *>(array.buffers[1]);
  const auto *data = static_cast<const char *>(array.buffers[2]);
  const int64_t slot = array.offset + row;
  const auto begin = offsets[slot];
  const auto end = offsets[slot + 1];
  if (begin < 0 || end < begin) {
    return sanitize::Status::Invalid("JSONL writer: invalid binary offsets");
  }
  if (quote) {
    out.push_back('"');
  }
  append_base64(out, std::string_view(data + begin,
                                      static_cast<std::size_t>(end - begin)));
  if (quote) {
    out.push_back('"');
  }
  return sanitize::Status::OK();
}

} // namespace

sanitize::Status append_string32_value(TextBuffer &out, const ArrowArray &array,
                                       int64_t row) {
  return append_string_value<int32_t>(out, array, row);
}

/// Serializes one large-string value using 64-bit Arrow offsets.
sanitize::Status append_string64_value(TextBuffer &out, const ArrowArray &array,
                                       int64_t row) {
  return append_string_value<int64_t>(out, array, row);
}

/// Serializes one binary value using 32-bit Arrow offsets.
sanitize::Status append_binary32_value(TextBuffer &out, const ArrowArray &array,
                                       int64_t row, bool quote) {
  return append_binary_value<int32_t>(out, array, row, quote);
}

/// Serializes one large-binary value using 64-bit Arrow offsets.
sanitize::Status append_binary64_value(TextBuffer &out, const ArrowArray &array,
                                       int64_t row, bool quote) {
  return append_binary_value<int64_t>(out, array, row, quote);
}

/// Serializes one fixed-width binary slot as optionally quoted base64.
sanitize::Status append_fixed_size_binary_value(TextBuffer &out,
                                                const JsonlField &field,
                                                const ArrowArray &array,
                                                int64_t row, bool quote) {
  if (field.fixed_size_binary_size <= 0) {
    return sanitize::Status::Invalid(
        "JSONL writer: invalid fixed-size binary width");
  }
  if (!array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid(
        "JSONL writer: missing fixed-size binary buffer");
  }
  const auto *data = static_cast<const char *>(array.buffers[1]);
  const auto byte_width =
      static_cast<std::size_t>(field.fixed_size_binary_size);
  const auto offset = static_cast<std::size_t>(array.offset + row) * byte_width;
  if (quote) {
    out.push_back('"');
  }
  append_base64(out, std::string_view(data + offset, byte_width));
  if (quote) {
    out.push_back('"');
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal::jsonl_stream_writer
