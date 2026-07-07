/*
 * Arrow metadata stream column-array builders.
 *
 * This file builds generated metadata columns for one exported record batch.
 */
#include "api/python_abi3/_core_abi3_metadata_stream_builder_parts.hh"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>

namespace core_abi3_internal {
namespace {

void set_validity_bit(std::vector<uint8_t> *validity, int64_t index) {
  (*validity)[static_cast<std::size_t>(index >> 3)] |=
      static_cast<uint8_t>(1u << (index & 7));
}

void set_validity_range(std::vector<uint8_t> *validity, int64_t start,
                        int64_t count) {
  if (count <= 0) {
    return;
  }
  const int64_t end = start + count;
  const std::size_t first_byte = static_cast<std::size_t>(start >> 3);
  const std::size_t last_byte = static_cast<std::size_t>((end - 1) >> 3);
  const auto first_mask =
      static_cast<uint8_t>(0xFFu << static_cast<unsigned>(start & 7));
  const auto last_mask = static_cast<uint8_t>(
      (1u << (static_cast<unsigned>((end - 1) & 7) + 1u)) - 1u);
  if (first_byte == last_byte) {
    (*validity)[first_byte] |= static_cast<uint8_t>(first_mask & last_mask);
    return;
  }
  (*validity)[first_byte] |= first_mask;
  if (last_byte > first_byte + 1) {
    std::fill(validity->begin() + static_cast<std::ptrdiff_t>(first_byte + 1),
              validity->begin() + static_cast<std::ptrdiff_t>(last_byte),
              static_cast<uint8_t>(0xFFu));
  }
  (*validity)[last_byte] |= last_mask;
}

sanitize::Status ensure_utf8_capacity(std::string_view value, int64_t count) {
  if (count > 0 && value.size() > 0 &&
      static_cast<std::uint64_t>(count) >
          static_cast<std::uint64_t>(std::numeric_limits<int32_t>::max()) /
              value.size()) {
    return sanitize::Status::Invalid(
        "metadata value exceeds Arrow UTF-8 limit");
  }
  return sanitize::Status::OK();
}

sanitize::Status add_utf8_data_bytes(std::uint64_t *total,
                                     std::string_view value, int64_t count) {
  SAN_RETURN_NOT_OK(ensure_utf8_capacity(value, count));
  const auto bytes = static_cast<std::uint64_t>(value.size()) *
                     static_cast<std::uint64_t>(count);
  if (*total >
      static_cast<std::uint64_t>(std::numeric_limits<int32_t>::max()) - bytes) {
    return sanitize::Status::Invalid(
        "metadata value exceeds Arrow UTF-8 limit");
  }
  *total += bytes;
  return sanitize::Status::OK();
}

void append_utf8_run(Utf8ColumnData *out, std::string_view value,
                     int64_t row_offset, int64_t count) {
  if (count <= 0) {
    return;
  }
  const std::size_t value_size = value.size();
  const std::size_t old_data_size = out->data.size();
  out->data.resize(old_data_size +
                   value_size * static_cast<std::size_t>(count));
  char *dest = value_size > 0 ? out->data.data() + old_data_size : nullptr;
  set_validity_range(&out->validity, row_offset, count);
  for (int64_t i = 0; i < count; ++i) {
    if (value_size > 0) {
      std::memcpy(dest + value_size * static_cast<std::size_t>(i), value.data(),
                  value_size);
    }
    out->offsets[static_cast<std::size_t>(row_offset + i) + 1] =
        static_cast<int32_t>(old_data_size +
                             value_size * static_cast<std::size_t>(i + 1));
  }
}

void append_null_run(Utf8ColumnData *out, int64_t row_offset, int64_t count) {
  const int32_t offset = out->offsets[static_cast<std::size_t>(row_offset)];
  for (int64_t i = 0; i < count; ++i) {
    out->offsets[static_cast<std::size_t>(row_offset + i) + 1] = offset;
  }
}

sanitize::Status build_first_row_value(Utf8ColumnData *out,
                                       std::string_view value, int64_t length,
                                       bool first_row_pending) {
  if (length < 0) {
    return sanitize::Status::Invalid("metadata column length is negative");
  }
  out->validity.assign(static_cast<std::size_t>((length + 7) / 8), 0);
  if (first_row_pending && length > 0) {
    if (value.size() >
        static_cast<std::size_t>(std::numeric_limits<int32_t>::max())) {
      return sanitize::Status::Invalid(
          "metadata value exceeds Arrow UTF-8 limit");
    }
    set_validity_bit(&out->validity, 0);
    out->data.assign(value.begin(), value.end());
    const int32_t end = static_cast<int32_t>(value.size());
    out->offsets.assign(static_cast<std::size_t>(length) + 1, end);
    out->offsets[0] = 0;
    out->null_count = length - 1;
    return sanitize::Status::OK();
  }
  out->offsets.assign(static_cast<std::size_t>(length) + 1, 0);
  out->null_count = length;
  return sanitize::Status::OK();
}

sanitize::Status build_all_row_value(Utf8ColumnData *out,
                                     std::string_view value, int64_t length) {
  if (length < 0) {
    return sanitize::Status::Invalid("metadata column length is negative");
  }
  SAN_RETURN_NOT_OK(ensure_utf8_capacity(value, length));
  out->validity.assign(static_cast<std::size_t>((length + 7) / 8), 0);
  out->offsets.assign(static_cast<std::size_t>(length) + 1, 0);
  out->data.clear();
  const std::size_t row_value_size = value.size();
  out->data.reserve(row_value_size * static_cast<std::size_t>(length));
  append_utf8_run(out, value, 0, length);
  out->null_count = 0;
  return sanitize::Status::OK();
}

sanitize::Status build_row_span_value(Utf8ColumnData *out,
                                      MetadataColumn *column, int64_t length) {
  if (length < 0) {
    return sanitize::Status::Invalid("metadata column length is negative");
  }
  out->validity.assign(static_cast<std::size_t>((length + 7) / 8), 0);
  out->offsets.assign(static_cast<std::size_t>(length) + 1, 0);
  out->data.clear();

  std::size_t reserve_span_index = column->span_index;
  int64_t reserve_span_offset = column->span_offset;
  int64_t reserve_row = 0;
  std::uint64_t data_bytes = 0;
  while (reserve_row < length && reserve_span_index < column->spans.size()) {
    const MetadataSpan &span = column->spans[reserve_span_index];
    const int64_t span_remaining = span.row_count - reserve_span_offset;
    if (span_remaining <= 0) {
      ++reserve_span_index;
      reserve_span_offset = 0;
      continue;
    }
    const int64_t take = std::min(length - reserve_row, span_remaining);
    if (!span.is_null) {
      SAN_RETURN_NOT_OK(add_utf8_data_bytes(&data_bytes, span.value, take));
    }
    reserve_row += take;
    reserve_span_offset += take;
    if (reserve_span_offset >= span.row_count) {
      ++reserve_span_index;
      reserve_span_offset = 0;
    }
  }
  out->data.reserve(static_cast<std::size_t>(data_bytes));

  std::size_t span_index = column->span_index;
  int64_t span_offset = column->span_offset;
  int64_t row = 0;
  int64_t null_count = 0;
  while (row < length && span_index < column->spans.size()) {
    const MetadataSpan &span = column->spans[span_index];
    const int64_t span_remaining = span.row_count - span_offset;
    if (span_remaining <= 0) {
      ++span_index;
      span_offset = 0;
      continue;
    }
    const int64_t take = std::min(length - row, span_remaining);
    if (span.is_null) {
      append_null_run(out, row, take);
      null_count += take;
    } else {
      append_utf8_run(out, span.value, row, take);
    }
    row += take;
    span_offset += take;
    if (span_offset >= span.row_count) {
      ++span_index;
      span_offset = 0;
    }
  }
  if (row < length) {
    append_null_run(out, row, length - row);
    null_count += length - row;
  }
  out->null_count = null_count;
  column->span_index = span_index;
  column->span_offset = span_offset;
  return sanitize::Status::OK();
}

sanitize::Status build_utf8_metadata_array(Utf8ColumnData *out,
                                           MetadataColumn *column,
                                           int64_t length,
                                           bool first_row_pending) {
  if (column->is_null) {
    SAN_RETURN_NOT_OK(build_first_row_value(out, "", length, false));
  } else if (column->placement == MetadataColumnPlacement::AllRowsUtf8) {
    SAN_RETURN_NOT_OK(build_all_row_value(out, column->value, length));
  } else if (column->placement == MetadataColumnPlacement::RowSpanUtf8) {
    SAN_RETURN_NOT_OK(build_row_span_value(out, column, length));
  } else {
    SAN_RETURN_NOT_OK(
        build_first_row_value(out, column->value, length, first_row_pending));
  }

  out->buffers[0] = out->validity.empty()
                        ? nullptr
                        : static_cast<const void *>(out->validity.data());
  out->buffers[1] = out->offsets.empty()
                        ? nullptr
                        : static_cast<const void *>(out->offsets.data());
  out->buffers[2] =
      out->data.empty() ? nullptr : static_cast<const void *>(out->data.data());

  clear_array(&out->array);
  out->array.length = length;
  out->array.null_count = out->null_count;
  out->array.offset = 0;
  out->array.n_buffers = 3;
  out->array.n_children = 0;
  out->array.buffers = out->buffers;
  out->array.children = nullptr;
  out->array.dictionary = nullptr;
  out->array.release = &metadata_array_child_release;
  out->array.private_data = nullptr;
  return sanitize::Status::OK();
}

sanitize::Status build_timestamp_micros_array(TimestampMicrosColumnData *out,
                                              int64_t length) {
  if (length < 0) {
    return sanitize::Status::Invalid("metadata column length is negative");
  }
  out->values.resize(static_cast<std::size_t>(length));
  for (int64_t i = 0; i < length; ++i) {
    out->values[static_cast<std::size_t>(i)] =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
  }
  out->buffers[0] = nullptr;
  out->buffers[1] = out->values.empty()
                        ? nullptr
                        : static_cast<const void *>(out->values.data());

  clear_array(&out->array);
  out->array.length = length;
  out->array.null_count = 0;
  out->array.offset = 0;
  out->array.n_buffers = 2;
  out->array.n_children = 0;
  out->array.buffers = out->buffers;
  out->array.children = nullptr;
  out->array.dictionary = nullptr;
  out->array.release = &metadata_array_child_release;
  out->array.private_data = nullptr;
  return sanitize::Status::OK();
}

} // namespace

sanitize::Status build_metadata_column_array(MetadataColumnData *out,
                                             MetadataColumn *column,
                                             int64_t length,
                                             bool first_row_pending) {
  if (column->placement == MetadataColumnPlacement::AllRowsTimestampMicros) {
    SAN_RETURN_NOT_OK(build_timestamp_micros_array(&out->timestamp, length));
    out->array = &out->timestamp.array;
    return sanitize::Status::OK();
  }
  SAN_RETURN_NOT_OK(
      build_utf8_metadata_array(&out->utf8, column, length, first_row_pending));
  out->array = &out->utf8.array;
  return sanitize::Status::OK();
}

} // namespace core_abi3_internal
