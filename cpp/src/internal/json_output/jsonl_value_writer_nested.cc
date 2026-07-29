// Implements nested Arrow value JSON serialization helpers.

#include "internal/json_output/jsonl_value_writer_parts.hh"

#include "internal/json_encoding/token_writer.hh"
#include "internal/json_output/jsonl_value_writer.hh"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>

namespace sanitize::internal::jsonl_stream_writer {
namespace {

template <typename T> const T *data_buffer(const ArrowArray &array) {
  if (!array.buffers || !array.buffers[1]) {
    return nullptr;
  }
  return static_cast<const T *>(array.buffers[1]);
}

std::optional<int64_t> dictionary_index_at(const ArrowArray &array,
                                           JsonlKind index_kind, int64_t row) {
  switch (index_kind) {
  case JsonlKind::kInt8: {
    const auto *values = data_buffer<int8_t>(array);
    return values ? std::optional<int64_t>(values[array.offset + row])
                  : std::nullopt;
  }
  case JsonlKind::kUInt8: {
    const auto *values = data_buffer<uint8_t>(array);
    return values ? std::optional<int64_t>(values[array.offset + row])
                  : std::nullopt;
  }
  case JsonlKind::kInt16: {
    const auto *values = data_buffer<int16_t>(array);
    return values ? std::optional<int64_t>(values[array.offset + row])
                  : std::nullopt;
  }
  case JsonlKind::kUInt16: {
    const auto *values = data_buffer<uint16_t>(array);
    return values ? std::optional<int64_t>(values[array.offset + row])
                  : std::nullopt;
  }
  case JsonlKind::kInt32: {
    const auto *values = data_buffer<int32_t>(array);
    return values ? std::optional<int64_t>(values[array.offset + row])
                  : std::nullopt;
  }
  case JsonlKind::kUInt32: {
    const auto *values = data_buffer<uint32_t>(array);
    return values ? std::optional<int64_t>(values[array.offset + row])
                  : std::nullopt;
  }
  case JsonlKind::kInt64: {
    const auto *values = data_buffer<int64_t>(array);
    return values ? std::optional<int64_t>(values[array.offset + row])
                  : std::nullopt;
  }
  case JsonlKind::kUInt64: {
    const auto *values = data_buffer<uint64_t>(array);
    if (!values ||
        values[array.offset + row] >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      return std::nullopt;
    }
    return static_cast<int64_t>(values[array.offset + row]);
  }
  default:
    return std::nullopt;
  }
}

template <typename OffsetT>
sanitize::Status append_list_value(TextBuffer &out, const JsonlField &field,
                                   const ArrowArray &array, int64_t row) {
  if (field.children.size() != 1 || array.n_children != 1 || !array.children ||
      !array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid("JSONL writer: list/schema mismatch");
  }
  const auto *offsets = static_cast<const OffsetT *>(array.buffers[1]);
  const int64_t slot = array.offset + row;
  const auto begin = offsets[slot];
  const auto end = offsets[slot + 1];
  if (begin < 0 || end < begin) {
    return sanitize::Status::Invalid("JSONL writer: invalid list offsets");
  }
  out.push_back('[');
  for (int64_t i = static_cast<int64_t>(begin); i < static_cast<int64_t>(end);
       ++i) {
    if (i != static_cast<int64_t>(begin)) {
      out.push_back(',');
    }
    SAN_RETURN_NOT_OK(
        append_value(out, field.children[0], *array.children[0], i));
  }
  out.push_back(']');
  return sanitize::Status::OK();
}

} // namespace

sanitize::Status append_struct_value(TextBuffer &out, const JsonlField &field,
                                     const ArrowArray &array, int64_t row) {
  if (array.n_children != static_cast<int64_t>(field.children.size()) ||
      (!field.children.empty() && !array.children) ||
      field.member_prefixes.size() != field.children.size()) {
    return sanitize::Status::Invalid("JSONL writer: struct/schema mismatch");
  }
  out.push_back('{');
  for (std::size_t i = 0; i < field.children.size(); ++i) {
    out.append(field.member_prefixes[i]);
    if (array.offset > std::numeric_limits<int64_t>::max() - row) {
      return sanitize::Status::Invalid(
          "JSONL writer: struct child row offset overflow");
    }
    SAN_RETURN_NOT_OK(append_value(out, field.children[i], *array.children[i],
                                   array.offset + row));
  }
  out.push_back('}');
  return sanitize::Status::OK();
}

sanitize::Status append_list32_value(TextBuffer &out, const JsonlField &field,
                                     const ArrowArray &array, int64_t row) {
  return append_list_value<int32_t>(out, field, array, row);
}

sanitize::Status append_list64_value(TextBuffer &out, const JsonlField &field,
                                     const ArrowArray &array, int64_t row) {
  return append_list_value<int64_t>(out, field, array, row);
}

sanitize::Status append_fixed_size_list_value(TextBuffer &out,
                                              const JsonlField &field,
                                              const ArrowArray &array,
                                              int64_t row) {
  if (field.children.size() != 1 || array.n_children != 1 || !array.children ||
      field.fixed_size_list_size < 0) {
    return sanitize::Status::Invalid(
        "JSONL writer: fixed-size list/schema mismatch");
  }
  const int64_t begin = (array.offset + row) * field.fixed_size_list_size;
  const int64_t end = begin + field.fixed_size_list_size;
  out.push_back('[');
  for (int64_t i = begin; i < end; ++i) {
    if (i != begin) {
      out.push_back(',');
    }
    SAN_RETURN_NOT_OK(
        append_value(out, field.children[0], *array.children[0], i));
  }
  out.push_back(']');
  return sanitize::Status::OK();
}

sanitize::Status append_dictionary_value(TextBuffer &out,
                                         const JsonlField &field,
                                         const ArrowArray &array, int64_t row) {
  if (field.children.size() != 1 || !array.dictionary) {
    return sanitize::Status::Invalid(
        "JSONL writer: dictionary/schema mismatch");
  }
  auto index = dictionary_index_at(array, field.dictionary_index_kind, row);
  if (!index || *index < 0 || *index >= array.dictionary->length) {
    out += "null";
    return sanitize::Status::OK();
  }
  return append_value(out, field.children[0], *array.dictionary, *index);
}

} // namespace sanitize::internal::jsonl_stream_writer
