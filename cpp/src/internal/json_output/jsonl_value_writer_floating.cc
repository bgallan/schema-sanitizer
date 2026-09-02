// Implements floating-point Arrow value JSON serialization.
// The code validates Arrow layouts and emits deterministic JSON with correct
// null and logical-type semantics.

#include "internal/json_output/jsonl_value_writer_parts.hh"

#include <array>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <locale>
#include <sstream>
#include <string>
#include <string_view>

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

/// Formats a double as a stable JSON numeric token, including non-finite
/// extensions.
void append_floating(TextBuffer &out, double value) {
  if (std::isnan(value)) {
    out += "NaN";
    return;
  }
  if (std::isinf(value)) {
    out += value < 0 ? "-Infinity" : "Infinity";
    return;
  }
  const std::size_t start = out.size();
#if !defined(__APPLE__)
  if constexpr (requires(char *first, char *last, double v) {
                  std::to_chars(first, last, v, std::chars_format::general,
                                std::numeric_limits<double>::max_digits10);
                }) {
    std::array<char, 64> buffer{};
    auto [ptr, ec] = std::to_chars(buffer.data(), buffer.data() + buffer.size(),
                                   value, std::chars_format::general,
                                   std::numeric_limits<double>::max_digits10);
    if (ec == std::errc()) {
      out.append(buffer.data(), static_cast<std::size_t>(ptr - buffer.data()));
    }
  }
#endif
  if (out.size() == start) {
    std::ostringstream oss;
    oss.imbue(std::locale::classic());
    oss.precision(std::numeric_limits<double>::max_digits10);
    oss << value;
    out.append(oss.str());
  }
  const std::string_view text(out.data() + start, out.size() - start);
  if (!text.contains('.') && !text.contains('e') && !text.contains('E')) {
    out += ".0";
  }
}

/// Expands an IEEE 754 binary16 bit pattern into its binary32 representation.
float half_to_float(uint16_t bits) noexcept {
  const uint32_t sign = static_cast<uint32_t>(bits & 0x8000u) << 16;
  uint32_t exponent = (bits >> 10) & 0x1fu;
  uint32_t mantissa = bits & 0x03ffu;
  uint32_t out_bits = 0;
  if (exponent == 0) {
    if (mantissa == 0) {
      out_bits = sign;
    } else {
      exponent = 1;
      while ((mantissa & 0x0400u) == 0) {
        mantissa <<= 1;
        --exponent;
      }
      mantissa &= 0x03ffu;
      out_bits = sign | ((exponent + 112u) << 23) | (mantissa << 13);
    }
  } else if (exponent == 0x1fu) {
    out_bits = sign | 0x7f800000u | (mantissa << 13);
  } else {
    out_bits = sign | ((exponent + 112u) << 23) | (mantissa << 13);
  }
  float value = 0.0f;
  std::memcpy(&value, &out_bits, sizeof(value));
  return value;
}

} // namespace

/// Reads and serializes one Arrow single-precision value.
sanitize::Status append_float32_value(TextBuffer &out, const ArrowArray &array,
                                      int64_t row) {
  const float *values = data_buffer<float>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing float buffer");
  }
  append_floating(out, static_cast<double>(values[array.offset + row]));
  return sanitize::Status::OK();
}

/// Expands and serializes one Arrow half-precision value.
sanitize::Status append_float16_value(TextBuffer &out, const ArrowArray &array,
                                      int64_t row) {
  const uint16_t *values = data_buffer<uint16_t>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing float buffer");
  }
  append_floating(
      out, static_cast<double>(half_to_float(values[array.offset + row])));
  return sanitize::Status::OK();
}

/// Reads and serializes one Arrow double-precision value.
sanitize::Status append_float64_value(TextBuffer &out, const ArrowArray &array,
                                      int64_t row) {
  const double *values = data_buffer<double>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing float buffer");
  }
  append_floating(out, values[array.offset + row]);
  return sanitize::Status::OK();
}

} // namespace sanitize::internal::jsonl_stream_writer
