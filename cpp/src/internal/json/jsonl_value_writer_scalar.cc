// Implements scalar Arrow value JSON serialization helpers.

#include "internal/json/jsonl_value_writer_parts.hh"

#include "internal/arrow/arrow_formatters.hh"
#include "internal/json/json_write.hh"
#include "internal/json/jsonl_stream_writer_schema.hh"

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

template <typename T> const T *data_buffer(const ArrowArray &array) {
  if (!array.buffers || !array.buffers[1]) {
    return nullptr;
  }
  return static_cast<const T *>(array.buffers[1]);
}

struct DayTimeInterval {
  int32_t days = 0;
  int32_t milliseconds = 0;
};

struct MonthDayNanoInterval {
  int32_t months = 0;
  int32_t days = 0;
  int64_t nanoseconds = 0;
};

sanitize::Status append_quoted_text(std::string &out, std::string_view value) {
  sanitize::internal::json_write::append_string(out, value);
  return sanitize::Status::OK();
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

void append_floating(std::string &out, double value) {
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
  if (text.find('.') == std::string_view::npos &&
      text.find('e') == std::string_view::npos &&
      text.find('E') == std::string_view::npos) {
    out += ".0";
  }
}

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

sanitize::Status append_float32_value(std::string &out, const ArrowArray &array,
                                      int64_t row) {
  const float *values = data_buffer<float>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing float buffer");
  }
  append_floating(out, static_cast<double>(values[array.offset + row]));
  return sanitize::Status::OK();
}

sanitize::Status append_float16_value(std::string &out, const ArrowArray &array,
                                      int64_t row) {
  const uint16_t *values = data_buffer<uint16_t>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing float buffer");
  }
  append_floating(
      out, static_cast<double>(half_to_float(values[array.offset + row])));
  return sanitize::Status::OK();
}

sanitize::Status append_float64_value(std::string &out, const ArrowArray &array,
                                      int64_t row) {
  const double *values = data_buffer<double>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing float buffer");
  }
  append_floating(out, values[array.offset + row]);
  return sanitize::Status::OK();
}

sanitize::Status append_decimal_value(std::string &out, const JsonlField &field,
                                      const ArrowArray &array, int64_t row) {
  if (!array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid("JSONL writer: missing decimal buffer");
  }
  const auto *data = static_cast<const uint8_t *>(array.buffers[1]);
  const auto offset =
      static_cast<std::size_t>((array.offset + row) * field.decimal_byte_width);
  return append_quoted_text(
      out, sanitize::internal::arrow_format::decimal_to_string(
               data + offset, field.decimal_byte_width, field.decimal_scale));
}

sanitize::Status append_duration_value(std::string &out,
                                       const JsonlField &field,
                                       const ArrowArray &array, int64_t row) {
  const int64_t *values = data_buffer<int64_t>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing duration buffer");
  }
  return append_quoted_text(
      out, sanitize::internal::arrow_format::duration_to_string(
               values[array.offset + row], field.format));
}

sanitize::Status append_interval_value(std::string &out,
                                       const JsonlField &field,
                                       const ArrowArray &array, int64_t row) {
  if (!array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid("JSONL writer: missing interval buffer");
  }
  std::string text;
  if (field.format == "tiM") {
    const int32_t *values = data_buffer<int32_t>(array);
    if (!values) {
      return sanitize::Status::Invalid("JSONL writer: missing interval buffer");
    }
    text = sanitize::internal::arrow_format::month_interval_to_string(
        values[array.offset + row]);
  } else if (field.format == "tiD") {
    const auto *values = static_cast<const DayTimeInterval *>(array.buffers[1]);
    const auto value = values[array.offset + row];
    text = sanitize::internal::arrow_format::day_time_interval_to_string(
        value.days, value.milliseconds);
  } else {
    const auto *values =
        static_cast<const MonthDayNanoInterval *>(array.buffers[1]);
    const auto value = values[array.offset + row];
    text = sanitize::internal::arrow_format::month_day_nano_interval_to_string(
        value.months, value.days, value.nanoseconds);
  }
  return append_quoted_text(out, text);
}

} // namespace sanitize::internal::jsonl_stream_writer
