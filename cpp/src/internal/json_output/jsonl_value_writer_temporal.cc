// Implements temporal JSON serialization for Arrow values.
// The code validates Arrow layouts and emits deterministic JSON with correct
// null and logical-type semantics.

#include "internal/json_output/jsonl_value_writer_parts.hh"

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>

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

/// Performs mathematical floor division so negative temporal offsets normalize
/// correctly.
int64_t floor_div(int64_t value, int64_t divisor) {
  int64_t q = value / divisor;
  int64_t r = value % divisor;
  if (r < 0) {
    --q;
  }
  return q;
}

/// Returns a nonnegative remainder paired with mathematical floor division.
int64_t floor_mod(int64_t value, int64_t divisor) {
  int64_t r = value % divisor;
  if (r < 0) {
    r += divisor;
  }
  return r;
}

/// Converts a Unix-epoch day offset to a proleptic Gregorian year, month, and
/// day.
void civil_from_days(int64_t z, int *year, unsigned *month, unsigned *day) {
  z += 719468;
  const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
  const unsigned doe = static_cast<unsigned>(z - era * 146097);
  const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  int y = static_cast<int>(yoe) + static_cast<int>(era) * 400;
  const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const unsigned mp = (5 * doy + 2) / 153;
  const unsigned d = doy - (153 * mp + 2) / 5 + 1;
  const unsigned m = mp < 10 ? mp + 3 : mp - 9;
  y += (m <= 2);
  *year = y;
  *month = m;
  *day = d;
}

/// Appends an integer padded with leading zeroes to the requested width.
void append_padded_int(TextBuffer &out, int value, int width) {
  std::array<char, 32> buffer{};
  auto [ptr, ec] =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
  if (ec != std::errc()) {
    out += std::to_string(value);
    return;
  }
  const int len = static_cast<int>(ptr - buffer.data());
  for (int i = len; i < width; ++i) {
    out.push_back('0');
  }
  out.append(buffer.data(), static_cast<std::size_t>(len));
}

/// Formats a Unix-epoch day offset as an ISO 8601 calendar date.
void append_iso_date(TextBuffer &out, int64_t days_since_epoch) {
  int year = 1970;
  unsigned month = 1;
  unsigned day = 1;
  civil_from_days(days_since_epoch, &year, &month, &day);
  append_padded_int(out, year, 4);
  out.push_back('-');
  append_padded_int(out, static_cast<int>(month), 2);
  out.push_back('-');
  append_padded_int(out, static_cast<int>(day), 2);
}

/// Formats seconds since midnight and optional nanoseconds as an ISO 8601 time.
void append_iso_time(TextBuffer &out, int64_t seconds_since_midnight,
                     int64_t nanos) {
  const int hour = static_cast<int>(seconds_since_midnight / 3600);
  const int minute = static_cast<int>((seconds_since_midnight % 3600) / 60);
  const int second = static_cast<int>(seconds_since_midnight % 60);
  append_padded_int(out, hour, 2);
  out.push_back(':');
  append_padded_int(out, minute, 2);
  out.push_back(':');
  append_padded_int(out, second, 2);
  if (nanos <= 0) {
    return;
  }
  std::array<char, 16> frac{};
  int64_t scale = 100000000;
  for (int i = 0; i < 9; ++i) {
    frac[static_cast<std::size_t>(i)] =
        static_cast<char>('0' + ((nanos / scale) % 10));
    scale /= 10;
  }
  int digits = 9;
  while (digits > 0 && frac[static_cast<std::size_t>(digits - 1)] == '0') {
    --digits;
  }
  if (digits > 0) {
    out.push_back('.');
    out.append(frac.data(), static_cast<std::size_t>(digits));
  }
}

} // namespace

sanitize::Status append_timestamp_value(TextBuffer &out,
                                        const ArrowArray &array, int64_t row,
                                        int64_t units_per_second, bool quote) {
  const int64_t *values = data_buffer<int64_t>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing timestamp buffer");
  }
  const int64_t value = values[array.offset + row];
  const int64_t seconds = floor_div(value, units_per_second);
  const int64_t unit_remainder = floor_mod(value, units_per_second);
  const int64_t nanos = unit_remainder * (1000000000 / units_per_second);
  const int64_t days = floor_div(seconds, 86400);
  const int64_t seconds_of_day = floor_mod(seconds, 86400);

  if (quote) {
    out.push_back('"');
  }
  append_iso_date(out, days);
  out.push_back('T');
  append_iso_time(out, seconds_of_day, nanos);
  if (quote) {
    out.push_back('"');
  }
  return sanitize::Status::OK();
}

/// Serializes one Arrow Date32 day count as quoted ISO 8601 text.
sanitize::Status append_date32_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row, bool quote) {
  const int32_t *values = data_buffer<int32_t>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing date32 buffer");
  }
  if (quote) {
    out.push_back('"');
  }
  append_iso_date(out, values[array.offset + row]);
  if (quote) {
    out.push_back('"');
  }
  return sanitize::Status::OK();
}

/// Serializes one Arrow Date64 millisecond count as quoted ISO 8601 text.
sanitize::Status append_date64_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row, bool quote) {
  const int64_t *values = data_buffer<int64_t>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing date64 buffer");
  }
  if (quote) {
    out.push_back('"');
  }
  append_iso_date(out, floor_div(values[array.offset + row], 86400000));
  if (quote) {
    out.push_back('"');
  }
  return sanitize::Status::OK();
}

/// Serializes one second-resolution Arrow Time32 value as quoted ISO 8601 text.
sanitize::Status append_time32s_value(TextBuffer &out, const ArrowArray &array,
                                      int64_t row, bool quote) {
  const int32_t *values = data_buffer<int32_t>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing time32 buffer");
  }
  if (quote) {
    out.push_back('"');
  }
  append_iso_time(out, values[array.offset + row], 0);
  if (quote) {
    out.push_back('"');
  }
  return sanitize::Status::OK();
}

/// Serializes one millisecond-resolution Arrow Time32 value as quoted ISO 8601
/// text.
sanitize::Status append_time32ms_value(TextBuffer &out, const ArrowArray &array,
                                       int64_t row, bool quote) {
  const int32_t *values = data_buffer<int32_t>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing time32 buffer");
  }
  const int64_t value = values[array.offset + row];
  const int64_t seconds = floor_div(value, 1000);
  const int64_t millis_remainder = floor_mod(value, 1000);
  if (quote) {
    out.push_back('"');
  }
  append_iso_time(out, seconds, millis_remainder * 1000000);
  if (quote) {
    out.push_back('"');
  }
  return sanitize::Status::OK();
}

/// Serializes one microsecond- or nanosecond-resolution Arrow Time64 value.
sanitize::Status append_time64_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row, int64_t units_per_second,
                                     bool quote) {
  const int64_t *values = data_buffer<int64_t>(array);
  if (!values) {
    return sanitize::Status::Invalid("JSONL writer: missing time64 buffer");
  }
  const int64_t value = values[array.offset + row];
  const int64_t seconds = floor_div(value, units_per_second);
  const int64_t unit_remainder = floor_mod(value, units_per_second);
  const int64_t nanos = unit_remainder * (1000000000 / units_per_second);
  if (quote) {
    out.push_back('"');
  }
  append_iso_time(out, seconds, nanos);
  if (quote) {
    out.push_back('"');
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal::jsonl_stream_writer
