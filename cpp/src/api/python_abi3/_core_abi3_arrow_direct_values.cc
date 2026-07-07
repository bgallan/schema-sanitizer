// Implements Arrow C Data value extraction for the direct frontend.

#include "api/python_abi3/_core_abi3_arrow_direct_values.hh"

#include "api/python_abi3/_core_abi3_arrow_direct_bits.hh"
#include "api/python_abi3/_core_abi3_arrow_direct_formatters.hh"
#include "api/python_abi3/_core_abi3_arrow_direct_values_dictionary.hh"
#include "api/python_abi3/_core_abi3_arrow_direct_values_nested.hh"
#include "api/python_abi3/_core_abi3_arrow_direct_values_temporal.hh"

#include <cstdint>
#include <string>
#include <string_view>

namespace core_abi3_internal {
namespace {

// Stores a temporary string for the lifetime of the current batch.
std::string_view append_stored_string(ArrowBatchStorage *storage,
                                      std::string value) {
  storage->strings.push_back(std::move(value));
  return storage->strings.back();
}

template <typename OffsetT>
sanitize::ValueView string_like_value(const ArrowArray *array, int64_t row,
                                      ArrowBatchStorage *storage, bool base64) {
  if (!array || !array->buffers || !array->buffers[1] || !array->buffers[2]) {
    return sanitize::ValueView::Null();
  }
  const auto *offsets = static_cast<const OffsetT *>(array->buffers[1]);
  const auto *data = static_cast<const char *>(array->buffers[2]);
  const int64_t slot = array->offset + row;
  const auto start = offsets[slot];
  const auto end = offsets[slot + 1];
  if (start < 0 || end < start) {
    return sanitize::ValueView::Null();
  }
  std::string_view value(data + start, static_cast<std::size_t>(end - start));
  if (!base64) {
    return sanitize::ValueView::String(value);
  }
  return sanitize::ValueView::String(
      append_stored_string(storage, base64_encode(value)));
}

} // namespace

ArrowBatchStorage::~ArrowBatchStorage() {
  if (array.release) {
    array.release(&array);
  }
}

const ArrowValueRef *store_value_ref(ArrowBatchStorage *storage,
                                     const ArrowInputNode *node,
                                     const ArrowArray *array, int64_t row) {
  storage->values.push_back(ArrowValueRef{
      .node = node, .array = array, .row = row, .storage = storage});
  return &storage->values.back();
}

sanitize::ValueView value_from_ref(const ArrowValueRef *ref) {
  if (!ref || !ref->node || !ref->array || is_null_at(ref->array, ref->row)) {
    return sanitize::ValueView::Null();
  }
  const std::string_view format(ref->node->format);
  const int64_t value_index = ref->array->offset + ref->row;
  switch (ref->node->kind) {
  case ArrowNodeKind::kNull:
    return sanitize::ValueView::Null();
  case ArrowNodeKind::kBool: {
    const auto *bitmap = static_cast<const uint8_t *>(ref->array->buffers[1]);
    return sanitize::ValueView::Bool(bit_at(bitmap, value_index));
  }
  case ArrowNodeKind::kInt:
    if (format == "c") {
      return sanitize::ValueView::Int(
          primitive_at<int8_t>(ref->array, ref->row));
    }
    if (format == "C") {
      return sanitize::ValueView::Int(
          primitive_at<uint8_t>(ref->array, ref->row));
    }
    if (format == "s") {
      return sanitize::ValueView::Int(
          primitive_at<int16_t>(ref->array, ref->row));
    }
    if (format == "S") {
      return sanitize::ValueView::Int(
          primitive_at<uint16_t>(ref->array, ref->row));
    }
    if (format == "i") {
      return sanitize::ValueView::Int(
          primitive_at<int32_t>(ref->array, ref->row));
    }
    if (format == "I") {
      return sanitize::ValueView::Int(
          primitive_at<uint32_t>(ref->array, ref->row));
    }
    return sanitize::ValueView::Int(
        primitive_at<int64_t>(ref->array, ref->row));
  case ArrowNodeKind::kUInt64Text:
    return sanitize::ValueView::String(append_stored_string(
        ref->storage,
        uint64_to_string(primitive_at<uint64_t>(ref->array, ref->row))));
  case ArrowNodeKind::kFloat:
    if (format == "f") {
      return sanitize::ValueView::Float(
          primitive_at<float>(ref->array, ref->row));
    }
    return sanitize::ValueView::Float(
        primitive_at<double>(ref->array, ref->row));
  case ArrowNodeKind::kUtf8:
    if (format == "U") {
      return string_like_value<int64_t>(ref->array, ref->row, ref->storage,
                                        false);
    }
    return string_like_value<int32_t>(ref->array, ref->row, ref->storage,
                                      false);
  case ArrowNodeKind::kBinaryBase64:
    if (format == "Z") {
      return string_like_value<int64_t>(ref->array, ref->row, ref->storage,
                                        true);
    }
    return string_like_value<int32_t>(ref->array, ref->row, ref->storage, true);
  case ArrowNodeKind::kDecimalText: {
    if (!ref->array->buffers || !ref->array->buffers[1]) {
      return sanitize::ValueView::Null();
    }
    const auto *data = static_cast<const uint8_t *>(ref->array->buffers[1]);
    const auto offset = static_cast<std::size_t>(
        (ref->array->offset + ref->row) * ref->node->decimal_byte_width);
    return sanitize::ValueView::String(append_stored_string(
        ref->storage,
        decimal_to_string(data + offset, ref->node->decimal_byte_width,
                          ref->node->decimal_scale)));
  }
  case ArrowNodeKind::kTimestamp:
    return sanitize::ValueView::Int(
        scale_timestamp_value(primitive_at<int64_t>(ref->array, ref->row),
                              ref->node->timestamp_source_units_per_second,
                              ref->node->timestamp_target_units_per_second));
  case ArrowNodeKind::kDate32:
    return sanitize::ValueView::Int(
        primitive_at<int32_t>(ref->array, ref->row));
  case ArrowNodeKind::kDate64:
    return sanitize::ValueView::Int(
        primitive_at<int64_t>(ref->array, ref->row) / 86400000LL);
  case ArrowNodeKind::kTime32s:
    return sanitize::ValueView::Int(
        primitive_at<int32_t>(ref->array, ref->row));
  case ArrowNodeKind::kTimeText:
    if (format == "ttm") {
      return sanitize::ValueView::String(append_stored_string(
          ref->storage,
          format_time_fraction(primitive_at<int32_t>(ref->array, ref->row),
                               1000LL)));
    }
    if (format == "ttu") {
      return sanitize::ValueView::String(append_stored_string(
          ref->storage,
          format_time_fraction(primitive_at<int64_t>(ref->array, ref->row),
                               1000000LL)));
    }
    return sanitize::ValueView::String(append_stored_string(
        ref->storage,
        format_time_fraction(primitive_at<int64_t>(ref->array, ref->row),
                             1000000000LL)));
  case ArrowNodeKind::kDurationText:
    return sanitize::ValueView::String(append_stored_string(
        ref->storage,
        duration_to_string(primitive_at<int64_t>(ref->array, ref->row),
                           format)));
  case ArrowNodeKind::kIntervalText:
    return sanitize::ValueView::String(append_stored_string(
        ref->storage, arrow_interval_to_string(ref->array, ref->row, format)));
  case ArrowNodeKind::kStruct:
    return sanitize::ValueView::ObjectView(ref, &arrow_direct_object_vtable());
  case ArrowNodeKind::kList:
  case ArrowNodeKind::kLargeList:
  case ArrowNodeKind::kFixedSizeList:
  case ArrowNodeKind::kMap:
    return sanitize::ValueView::ArrayView(ref, &arrow_direct_array_vtable());
  case ArrowNodeKind::kDictionary: {
    if (ref->node->children.size() != 1 || !ref->array->dictionary) {
      return sanitize::ValueView::Null();
    }
    auto index = dictionary_index_at(ref->array, format, ref->row);
    if (!index || *index < 0 || *index >= ref->array->dictionary->length) {
      return sanitize::ValueView::Null();
    }
    const ArrowValueRef *dict_ref = store_value_ref(
        ref->storage, &ref->node->children[0], ref->array->dictionary, *index);
    return value_from_ref(dict_ref);
  }
  }
  return sanitize::ValueView::Null();
}

} // namespace core_abi3_internal
