// Arrow schema and array builders for generated metadata columns.
#include "api/python_abi3/metadata/stream/stream.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "sanitize/abi/cdata_types.hh"
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
namespace core_abi3_internal {
namespace {
struct MetadataSchemaChild {
  ArrowSchema schema{};
  std::string name;
  const char *format = nullptr;
};
struct MetadataSchemaState {
  sanitize::CSchemaGuard base;
  std::vector<MetadataSchemaChild> metadata;
  std::vector<ArrowSchema *> children;
};
struct Utf8ColumnData {
  std::vector<std::uint8_t> validity;
  std::vector<std::int32_t> offsets;
  std::vector<char> data;
  std::int64_t null_count = 0;
  const void *buffers[3]{nullptr, nullptr, nullptr};
  ArrowArray array{};
};
struct TimestampMicrosColumnData {
  std::vector<std::int64_t> values;
  const void *buffers[2]{nullptr, nullptr};
  ArrowArray array{};
};
struct MetadataArrayState {
  sanitize::CArrayGuard base;
  std::vector<Utf8ColumnData> utf8_columns;
  std::vector<TimestampMicrosColumnData> timestamp_columns;
  std::vector<ArrowArray *> children;
  const void *struct_buffers[1]{nullptr};
};
void clear_schema(ArrowSchema *schema) noexcept {
  sanitize::internal::cdata_stream::clear_schema(schema);
}
void clear_array(ArrowArray *array) noexcept {
  sanitize::internal::cdata_stream::clear_array(array);
}
void metadata_schema_child_release(ArrowSchema *schema) noexcept {
  if (schema && schema->release) {
    clear_schema(schema);
  }
}
void metadata_array_child_release(ArrowArray *array) noexcept {
  if (array && array->release) {
    clear_array(array);
  }
}
void metadata_schema_release(ArrowSchema *schema) noexcept {
  if (!schema || !schema->release) {
    return;
  }
  delete static_cast<MetadataSchemaState *>(schema->private_data);
  clear_schema(schema);
}
void metadata_array_release(ArrowArray *array) noexcept {
  if (!array || !array->release) {
    return;
  }
  delete static_cast<MetadataArrayState *>(array->private_data);
  clear_array(array);
}
void set_validity_bit(std::vector<std::uint8_t> *validity, std::int64_t index) {
  (*validity)[static_cast<std::size_t>(index >> 3)] |=
      static_cast<std::uint8_t>(1u << (index & 7));
}
void set_validity_range(std::vector<std::uint8_t> *validity, std::int64_t start,
                        std::int64_t count) {
  if (count <= 0) {
    return;
  }
  const std::int64_t end = start + count;
  const std::size_t first_byte = static_cast<std::size_t>(start >> 3);
  const std::size_t last_byte = static_cast<std::size_t>((end - 1) >> 3);
  const auto first_mask =
      static_cast<std::uint8_t>(0xFFu << static_cast<unsigned>(start & 7));
  const auto last_mask = static_cast<std::uint8_t>(
      (1u << (static_cast<unsigned>((end - 1) & 7) + 1u)) - 1u);
  if (first_byte == last_byte) {
    (*validity)[first_byte] |=
        static_cast<std::uint8_t>(first_mask & last_mask);
    return;
  }
  (*validity)[first_byte] |= first_mask;
  if (last_byte > first_byte + 1) {
    std::fill(validity->begin() + static_cast<std::ptrdiff_t>(first_byte + 1),
              validity->begin() + static_cast<std::ptrdiff_t>(last_byte),
              static_cast<std::uint8_t>(0xFFu));
  }
  (*validity)[last_byte] |= last_mask;
}

sanitize::Status ensure_utf8_capacity(std::string_view value,
                                      std::int64_t count) {
  if (count > 0 && !value.empty() &&
      static_cast<std::uint64_t>(count) >
          static_cast<std::uint64_t>(std::numeric_limits<std::int32_t>::max()) /
              value.size()) {
    return sanitize::Status::Invalid(
        "metadata value exceeds Arrow UTF-8 limit");
  }
  return sanitize::Status::OK();
}
sanitize::Status add_utf8_data_bytes(std::uint64_t *total,
                                     std::string_view value,
                                     std::int64_t count) {
  SAN_RETURN_NOT_OK(ensure_utf8_capacity(value, count));
  const auto bytes = static_cast<std::uint64_t>(value.size()) *
                     static_cast<std::uint64_t>(count);
  if (*total >
      static_cast<std::uint64_t>(std::numeric_limits<std::int32_t>::max()) -
          bytes) {
    return sanitize::Status::Invalid(
        "metadata value exceeds Arrow UTF-8 limit");
  }
  *total += bytes;
  return sanitize::Status::OK();
}

void append_utf8_run(Utf8ColumnData *out, std::string_view value,
                     std::int64_t row_offset, std::int64_t count) {
  if (count <= 0) {
    return;
  }
  const std::size_t value_size = value.size();
  const std::size_t old_data_size = out->data.size();
  out->data.resize(old_data_size +
                   value_size * static_cast<std::size_t>(count));
  char *dest = value_size > 0 ? out->data.data() + old_data_size : nullptr;
  if (!out->validity.empty()) {
    set_validity_range(&out->validity, row_offset, count);
  }
  for (std::int64_t i = 0; i < count; ++i) {
    if (value_size > 0) {
      std::memcpy(dest + value_size * static_cast<std::size_t>(i), value.data(),
                  value_size);
    }
    out->offsets[static_cast<std::size_t>(row_offset + i) + 1] =
        static_cast<std::int32_t>(old_data_size +
                                  value_size * static_cast<std::size_t>(i + 1));
  }
}

void append_null_run(Utf8ColumnData *out, std::int64_t row_offset,
                     std::int64_t count) {
  const std::int32_t offset =
      out->offsets[static_cast<std::size_t>(row_offset)];
  for (std::int64_t i = 0; i < count; ++i) {
    out->offsets[static_cast<std::size_t>(row_offset + i) + 1] = offset;
  }
}

sanitize::Status build_first_row_value(Utf8ColumnData *out,
                                       std::string_view value,
                                       std::int64_t length,
                                       bool first_row_pending) {
  if (length < 0) {
    return sanitize::Status::Invalid("metadata column length is negative");
  }
  if (first_row_pending && length > 0) {
    if (value.size() >
        static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
      return sanitize::Status::Invalid(
          "metadata value exceeds Arrow UTF-8 limit");
    }
    out->null_count = length - 1;
    if (out->null_count > 0) {
      out->validity.assign(static_cast<std::size_t>((length + 7) / 8), 0);
      set_validity_bit(&out->validity, 0);
    } else {
      out->validity.clear();
    }
    out->data.assign(value.begin(), value.end());
    const auto end = static_cast<std::int32_t>(value.size());
    out->offsets.assign(static_cast<std::size_t>(length) + 1, end);
    out->offsets[0] = 0;
    return sanitize::Status::OK();
  }
  out->validity.assign(static_cast<std::size_t>((length + 7) / 8), 0);
  out->offsets.assign(static_cast<std::size_t>(length) + 1, 0);
  out->null_count = length;
  return sanitize::Status::OK();
}

sanitize::Status build_all_row_value(Utf8ColumnData *out,
                                     std::string_view value,
                                     std::int64_t length) {
  if (length < 0) {
    return sanitize::Status::Invalid("metadata column length is negative");
  }
  SAN_RETURN_NOT_OK(ensure_utf8_capacity(value, length));
  out->validity.clear();
  out->offsets.assign(static_cast<std::size_t>(length) + 1, 0);
  out->data.clear();
  out->data.reserve(value.size() * static_cast<std::size_t>(length));
  append_utf8_run(out, value, 0, length);
  out->null_count = 0;
  return sanitize::Status::OK();
}

sanitize::Status build_row_span_value(Utf8ColumnData *out,
                                      MetadataColumn *column,
                                      std::int64_t length) {
  if (length < 0) {
    return sanitize::Status::Invalid("metadata column length is negative");
  }
  out->offsets.assign(static_cast<std::size_t>(length) + 1, 0);
  out->data.clear();

  std::size_t reserve_span_index = column->span_index;
  std::int64_t reserve_span_offset = column->span_offset;
  std::int64_t reserve_row = 0;
  std::int64_t reserve_null_count = 0;
  std::uint64_t data_bytes = 0;
  while (reserve_row < length && reserve_span_index < column->spans.size()) {
    const MetadataSpan &span = column->spans[reserve_span_index];
    const std::int64_t span_remaining = span.row_count - reserve_span_offset;
    if (span_remaining <= 0) {
      ++reserve_span_index;
      reserve_span_offset = 0;
      continue;
    }
    const std::int64_t take = std::min(length - reserve_row, span_remaining);
    if (span.is_null) {
      reserve_null_count += take;
    } else {
      SAN_RETURN_NOT_OK(add_utf8_data_bytes(&data_bytes, span.value, take));
    }
    reserve_row += take;
    reserve_span_offset += take;
    if (reserve_span_offset >= span.row_count) {
      ++reserve_span_index;
      reserve_span_offset = 0;
    }
  }
  reserve_null_count += length - reserve_row;
  if (reserve_null_count > 0) {
    out->validity.assign(static_cast<std::size_t>((length + 7) / 8), 0);
  } else {
    out->validity.clear();
  }
  out->data.reserve(static_cast<std::size_t>(data_bytes));

  std::size_t span_index = column->span_index;
  std::int64_t span_offset = column->span_offset;
  std::int64_t row = 0;
  std::int64_t null_count = 0;
  while (row < length && span_index < column->spans.size()) {
    const MetadataSpan &span = column->spans[span_index];
    const std::int64_t span_remaining = span.row_count - span_offset;
    if (span_remaining <= 0) {
      ++span_index;
      span_offset = 0;
      continue;
    }
    const std::int64_t take = std::min(length - row, span_remaining);
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
                                           std::int64_t length,
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
  out->buffers[0] = out->validity.empty() ? nullptr : out->validity.data();
  out->buffers[1] = out->offsets.empty() ? nullptr : out->offsets.data();
  out->buffers[2] = out->data.empty() ? nullptr : out->data.data();
  clear_array(&out->array);
  out->array.length = length;
  out->array.null_count = out->null_count;
  out->array.n_buffers = 3;
  out->array.buffers = out->buffers;
  out->array.release = &metadata_array_child_release;
  return sanitize::Status::OK();
}

sanitize::Status build_timestamp_micros_array(TimestampMicrosColumnData *out,
                                              std::int64_t length,
                                              std::int64_t timestamp) {
  if (length < 0) {
    return sanitize::Status::Invalid("metadata column length is negative");
  }
  out->values.assign(static_cast<std::size_t>(length), timestamp);
  out->buffers[0] = nullptr;
  out->buffers[1] = out->values.empty() ? nullptr : out->values.data();
  clear_array(&out->array);
  out->array.length = length;
  out->array.n_buffers = 2;
  out->array.buffers = out->buffers;
  out->array.release = &metadata_array_child_release;
  return sanitize::Status::OK();
}

void build_metadata_schema_children(MetadataSchemaState *state,
                                    MetadataStreamState *stream_state) {
  ArrowSchema &base = state->base.value();
  state->children.resize(static_cast<std::size_t>(base.n_children) +
                         state->metadata.size());
  for (std::int64_t i = 0; i < base.n_children; ++i) {
    state->children
        [stream_state->base_child_output_indices[static_cast<std::size_t>(i)]] =
        base.children[i];
  }
  for (std::size_t i = 0; i < state->metadata.size(); ++i) {
    auto &child = state->metadata[i];
    clear_schema(&child.schema);
    child.schema.format = child.format;
    child.schema.name = child.name.c_str();
    child.schema.flags = ARROW_FLAG_NULLABLE;
    child.schema.release = &metadata_schema_child_release;
    state->children[stream_state->metadata_child_output_indices[i]] =
        &child.schema;
  }
}

} // namespace
sanitize::Status build_metadata_schema(MetadataStreamState *stream_state,
                                       ArrowSchema *out) {
  if (!stream_state || !stream_state->inner) {
    return sanitize::Status::Invalid("metadata stream is closed");
  }
  auto state = std::unique_ptr<MetadataSchemaState>(new (std::nothrow)
                                                        MetadataSchemaState());
  if (!state) {
    return sanitize::Status::OutOfMemory("metadata stream schema OOM");
  }
  state->metadata.reserve(stream_state->columns.size());
  for (const auto &column : stream_state->columns) {
    state->metadata.push_back(MetadataSchemaChild{
        .name = column.name,
        .format =
            column.placement == MetadataColumnPlacement::AllRowsTimestampMicros
                ? "tsu:"
                : "u",
    });
  }
  const int schema_rc =
      stream_state->inner->get_schema(stream_state->inner, state->base.get());
  if (schema_rc != 0) {
    return sanitize::internal::cdata_stream::status_from_stream_error(
        schema_rc, stream_state->inner,
        "metadata stream inner get_schema failed");
  }
  ArrowSchema &base = state->base.value();
  SAN_RETURN_NOT_OK(prepare_metadata_child_layout(stream_state, base));
  build_metadata_schema_children(state.get(), stream_state);
  clear_schema(out);
  out->format = base.format;
  out->name = base.name;
  out->metadata = base.metadata;
  out->flags = base.flags;
  out->n_children = static_cast<std::int64_t>(state->children.size());
  out->children = state->children.empty() ? nullptr : state->children.data();
  out->dictionary = base.dictionary;
  out->private_data = state.release();
  out->release = &metadata_schema_release;
  return sanitize::Status::OK();
}

sanitize::Status build_metadata_array(MetadataStreamState *stream_state,
                                      ArrowArray *out) {
  if (!stream_state || !stream_state->inner) {
    return sanitize::Status::Invalid("metadata stream is closed");
  }
  if (!stream_state->child_layout_ready) {
    sanitize::CSchemaGuard base_schema;
    const int schema_rc =
        stream_state->inner->get_schema(stream_state->inner, base_schema.get());
    if (schema_rc != 0) {
      return sanitize::internal::cdata_stream::status_from_stream_error(
          schema_rc, stream_state->inner,
          "metadata stream inner get_schema failed");
    }
    SAN_RETURN_NOT_OK(
        prepare_metadata_child_layout(stream_state, base_schema.value()));
  }
  auto state = std::unique_ptr<MetadataArrayState>(new (std::nothrow)
                                                       MetadataArrayState());
  if (!state) {
    return sanitize::Status::OutOfMemory("metadata stream array OOM");
  }
  const int next_rc =
      stream_state->inner->get_next(stream_state->inner, state->base.get());
  if (next_rc != 0) {
    return sanitize::internal::cdata_stream::status_from_stream_error(
        next_rc, stream_state->inner, "metadata stream inner get_next failed");
  }
  ArrowArray &base = state->base.value();
  if (!base.release) {
    clear_array(out);
    return sanitize::Status::OK();
  }
  SAN_RETURN_NOT_OK(validate_metadata_base_array(*stream_state, base));

  const auto timestamp_count = static_cast<std::size_t>(std::ranges::count_if(
      stream_state->columns, [](const MetadataColumn &column) {
        return column.placement ==
               MetadataColumnPlacement::AllRowsTimestampMicros;
      }));
  SAN_RETURN_NOT_OK(
      validate_generated_metadata_budget(*stream_state, base, timestamp_count));
  state->timestamp_columns.reserve(timestamp_count);
  state->utf8_columns.reserve(stream_state->columns.size() - timestamp_count);
  state->children.resize(static_cast<std::size_t>(base.n_children) +
                         stream_state->columns.size());
  for (std::int64_t i = 0; i < base.n_children; ++i) {
    state->children
        [stream_state->base_child_output_indices[static_cast<std::size_t>(i)]] =
        base.children[i];
  }

  for (std::size_t i = 0; i < stream_state->columns.size(); ++i) {
    auto &column = stream_state->columns[i];
    ArrowArray *metadata_child = nullptr;
    if (column.placement == MetadataColumnPlacement::AllRowsTimestampMicros) {
      auto &data = state->timestamp_columns.emplace_back();
      const auto timestamp =
          column.has_fixed_timestamp
              ? column.timestamp_micros
              : std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();
      SAN_RETURN_NOT_OK(
          build_timestamp_micros_array(&data, base.length, timestamp));
      metadata_child = &data.array;
    } else {
      auto &data = state->utf8_columns.emplace_back();
      SAN_RETURN_NOT_OK(build_utf8_metadata_array(
          &data, &column, base.length, stream_state->first_row_pending));
      metadata_child = &data.array;
    }
    state->children[stream_state->metadata_child_output_indices[i]] =
        metadata_child;
  }

  clear_array(out);
  out->length = base.length;
  out->null_count = base.null_count;
  out->offset = base.offset;
  out->n_buffers = base.n_buffers;
  out->buffers = base.buffers ? base.buffers : state->struct_buffers;
  out->n_children = static_cast<std::int64_t>(state->children.size());
  out->children = state->children.empty() ? nullptr : state->children.data();
  out->dictionary = base.dictionary;
  out->private_data = state.release();
  out->release = &metadata_array_release;
  if (base.length > 0) {
    stream_state->first_row_pending = false;
  }
  return sanitize::Status::OK();
}

} // namespace core_abi3_internal
