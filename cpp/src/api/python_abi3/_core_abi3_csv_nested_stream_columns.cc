/*
 * CSV nested-column stream array rewriting.
 *
 * Materializes each top-level nested Arrow array as compact JSON UTF-8 cells.
 */
#include "api/python_abi3/_core_abi3_csv_nested_stream_parts.hh"

#include "internal/json/jsonl_value_writer.hh"

#include <climits>
#include <cstddef>
#include <cstdint>
#include <string>

namespace core_abi3_internal::csv_nested_stream {

namespace {

bool validity_bit_is_set(const std::uint8_t *bitmap, std::int64_t index) {
  return (bitmap[index >> 3] & static_cast<std::uint8_t>(1u << (index & 7))) !=
         0;
}

bool array_is_null(const ArrowArray &array, std::int64_t row) {
  if (array.null_count == 0 || !array.buffers || !array.buffers[0]) {
    return false;
  }
  const auto *bitmap = static_cast<const std::uint8_t *>(array.buffers[0]);
  return !validity_bit_is_set(bitmap, array.offset + row);
}

void set_validity_bit(std::vector<std::uint8_t> *validity, std::int64_t index) {
  (*validity)[static_cast<std::size_t>(index >> 3)] |=
      static_cast<std::uint8_t>(1u << (index & 7));
}

} // namespace

sanitize::Status build_nested_utf8_array(CsvNestedUtf8Array *out,
                                         const jsonl::JsonlField &field,
                                         const ArrowArray &array,
                                         std::int64_t length) {
  if (length < 0) {
    return sanitize::Status::Invalid(
        "CSV nested stream: negative array length");
  }
  out->validity.assign(static_cast<std::size_t>((length + 7) / 8), 0);
  out->offsets.reserve(static_cast<std::size_t>(length) + 1);
  out->offsets.push_back(0);
  std::int64_t null_count = 0;
  for (std::int64_t row = 0; row < length; ++row) {
    if (array_is_null(array, row)) {
      ++null_count;
      out->offsets.push_back(out->offsets.back());
      continue;
    }
    set_validity_bit(&out->validity, row);
    const std::size_t before = out->data.size();
    SAN_RETURN_NOT_OK(jsonl::append_value(out->data, field, array, row));
    const auto added = out->data.size() - before;
    const auto next_offset = static_cast<std::int64_t>(out->offsets.back()) +
                             static_cast<std::int64_t>(added);
    if (next_offset > INT32_MAX) {
      return sanitize::Status::Invalid(
          "CSV nested stream: UTF-8 data too large");
    }
    out->offsets.push_back(static_cast<std::int32_t>(next_offset));
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
  out->array.null_count = null_count;
  out->array.offset = 0;
  out->array.n_buffers = 3;
  out->array.n_children = 0;
  out->array.buffers = out->buffers;
  out->array.children = nullptr;
  out->array.dictionary = nullptr;
  out->array.release = &csv_nested_array_child_release;
  out->array.private_data = nullptr;
  return sanitize::Status::OK();
}

} // namespace core_abi3_internal::csv_nested_stream
