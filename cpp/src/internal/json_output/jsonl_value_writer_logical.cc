// Implements decimal, duration, and interval Arrow value JSON serialization.

#include "internal/json_output/jsonl_value_writer_parts.hh"

#include "internal/arrow_text/formatters.hh"
#include "internal/json_encoding/token_writer.hh"

#include <cstdint>
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

sanitize::Status append_quoted_text(TextBuffer &out, std::string_view value) {
  sanitize::internal::json_encoding::append_string(out, value);
  return sanitize::Status::OK();
}

} // namespace

sanitize::Status append_decimal_value(TextBuffer &out, const JsonlField &field,
                                      const ArrowArray &array, int64_t row,
                                      bool quote) {
  if (!array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid("JSONL writer: missing decimal buffer");
  }
  const auto *data = static_cast<const uint8_t *>(array.buffers[1]);
  const auto offset =
      static_cast<std::size_t>((array.offset + row) * field.decimal_byte_width);
  const auto text = sanitize::internal::arrow_format::decimal_to_string(
      data + offset, field.decimal_byte_width, field.decimal_scale);
  if (quote) {
    return append_quoted_text(out, text);
  }
  out.append(text);
  return sanitize::Status::OK();
}

sanitize::Status append_duration_value(TextBuffer &out, const JsonlField &field,
                                       const ArrowArray &array, int64_t row,
                                       bool quote) {
  const int64_t *values = data_buffer<int64_t>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing duration buffer");
  }
  const auto text = sanitize::internal::arrow_format::duration_to_string(
      values[array.offset + row], field.format);
  if (quote) {
    return append_quoted_text(out, text);
  }
  out.append(text);
  return sanitize::Status::OK();
}

sanitize::Status append_interval_value(TextBuffer &out, const JsonlField &field,
                                       const ArrowArray &array, int64_t row) {
  if (!array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid("JSONL writer: missing interval buffer");
  }
  TextBuffer text(out.get_allocator().resource());
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
