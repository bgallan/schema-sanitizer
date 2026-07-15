// Implements Arrow C Data value extraction for the direct frontend.

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_values.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_bits.hh"
#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_formatters.hh"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "sanitize/core/status.hh"

namespace core_abi3_internal {
namespace {

struct DayTimeInterval {
  int32_t days = 0;
  int32_t milliseconds = 0;
};

struct MonthDayNanoInterval {
  int32_t months = 0;
  int32_t days = 0;
  int64_t nanoseconds = 0;
};

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

sanitize::ValueView integer_value(const ArrowArray *array, int64_t row,
                                  ArrowStorageKind storage_kind) {
  switch (storage_kind) {
  case ArrowStorageKind::kInt8:
    return sanitize::ValueView::Int(primitive_at<int8_t>(array, row));
  case ArrowStorageKind::kUInt8:
    return sanitize::ValueView::Int(primitive_at<uint8_t>(array, row));
  case ArrowStorageKind::kInt16:
    return sanitize::ValueView::Int(primitive_at<int16_t>(array, row));
  case ArrowStorageKind::kUInt16:
    return sanitize::ValueView::Int(primitive_at<uint16_t>(array, row));
  case ArrowStorageKind::kInt32:
    return sanitize::ValueView::Int(primitive_at<int32_t>(array, row));
  case ArrowStorageKind::kUInt32:
    return sanitize::ValueView::Int(primitive_at<uint32_t>(array, row));
  case ArrowStorageKind::kInt64:
    return sanitize::ValueView::Int(primitive_at<int64_t>(array, row));
  default:
    return sanitize::ValueView::Null();
  }
}

std::optional<int64_t> dictionary_index_at(const ArrowArray *array,
                                           ArrowStorageKind storage_kind,
                                           int64_t row) {
  switch (storage_kind) {
  case ArrowStorageKind::kInt8:
    return primitive_at<int8_t>(array, row);
  case ArrowStorageKind::kUInt8:
    return primitive_at<uint8_t>(array, row);
  case ArrowStorageKind::kInt16:
    return primitive_at<int16_t>(array, row);
  case ArrowStorageKind::kUInt16:
    return primitive_at<uint16_t>(array, row);
  case ArrowStorageKind::kInt32:
    return primitive_at<int32_t>(array, row);
  case ArrowStorageKind::kUInt32:
    return primitive_at<uint32_t>(array, row);
  case ArrowStorageKind::kInt64:
    return primitive_at<int64_t>(array, row);
  default:
    return std::nullopt;
  }
}

std::string time_value_to_string(const ArrowArray *array, int64_t row,
                                 ArrowStorageKind storage_kind) {
  if (storage_kind == ArrowStorageKind::kTimeMilliseconds) {
    return format_time_fraction(primitive_at<int32_t>(array, row), 1000LL);
  }
  const int64_t value = primitive_at<int64_t>(array, row);
  if (storage_kind == ArrowStorageKind::kTimeMicroseconds) {
    return format_time_fraction(value, 1000000LL);
  }
  return format_time_fraction(value, 1000000000LL);
}

std::string duration_value_to_string(int64_t value,
                                     ArrowStorageKind storage_kind) {
  std::string out = std::to_string(value);
  switch (storage_kind) {
  case ArrowStorageKind::kDurationSeconds:
    out += "s";
    break;
  case ArrowStorageKind::kDurationMilliseconds:
    out += "ms";
    break;
  case ArrowStorageKind::kDurationMicroseconds:
    out += "us";
    break;
  case ArrowStorageKind::kDurationNanoseconds:
    out += "ns";
    break;
  default:
    break;
  }
  return out;
}

std::string interval_value_to_string(const ArrowArray *array, int64_t row,
                                     ArrowStorageKind storage_kind) {
  if (storage_kind == ArrowStorageKind::kIntervalMonths) {
    return month_interval_to_string(primitive_at<int32_t>(array, row));
  }
  if (!array || !array->buffers || !array->buffers[1]) {
    return {};
  }
  if (storage_kind == ArrowStorageKind::kIntervalDayTime) {
    const auto *values =
        static_cast<const DayTimeInterval *>(array->buffers[1]);
    const auto value = values[array->offset + row];
    return day_time_interval_to_string(value.days, value.milliseconds);
  }
  const auto *values =
      static_cast<const MonthDayNanoInterval *>(array->buffers[1]);
  const auto value = values[array->offset + row];
  return month_day_nano_interval_to_string(value.months, value.days,
                                           value.nanoseconds);
}

sanitize::Status object_for_each(const void *self, void *ctx,
                                 sanitize::ValueView::ObjectEachFn fn) {
  const auto *ref = static_cast<const ArrowValueRef *>(self);
  if (!ref || !ref->node || !ref->array || !ref->storage) {
    return sanitize::Status::Invalid("Arrow direct object view is invalid");
  }
  const auto &children = ref->node->children;
  if (ref->array->n_children != static_cast<int64_t>(children.size()) ||
      (!children.empty() && !ref->array->children)) {
    return sanitize::Status::Invalid("Arrow direct struct/schema mismatch");
  }
  for (std::size_t i = 0; i < children.size(); ++i) {
    const ArrowArray *child_array = ref->array->children[i];
    SAN_RETURN_NOT_OK(fn(ctx, children[i].name, 0,
                         value_at(ref->storage, &children[i], child_array,
                                  ref->row)));
  }
  return sanitize::Status::OK();
}

template <typename OffsetT>
sanitize::Status array_for_each_offset(const ArrowValueRef *ref, void *ctx,
                                       sanitize::ValueView::ArrayEachFn fn) {
  if (!ref || !ref->node || !ref->array || !ref->storage ||
      ref->node->children.size() != 1 || ref->array->n_children != 1 ||
      !ref->array->children || !ref->array->buffers ||
      !ref->array->buffers[1]) {
    return sanitize::Status::Invalid("Arrow direct list view is invalid");
  }
  const auto *offsets = static_cast<const OffsetT *>(ref->array->buffers[1]);
  const int64_t slot = ref->array->offset + ref->row;
  const auto begin = offsets[slot];
  const auto end = offsets[slot + 1];
  if (begin < 0 || end < begin) {
    return sanitize::Status::Invalid("Arrow direct list offsets are invalid");
  }
  const ArrowInputNode &child_node = ref->node->children[0];
  const ArrowArray *child_array = ref->array->children[0];
  for (int64_t i = static_cast<int64_t>(begin); i < static_cast<int64_t>(end);
       ++i) {
    SAN_RETURN_NOT_OK(
        fn(ctx, value_at(ref->storage, &child_node, child_array, i)));
  }
  return sanitize::Status::OK();
}

sanitize::Status
array_for_each_fixed_size(const ArrowValueRef *ref, void *ctx,
                          sanitize::ValueView::ArrayEachFn fn) {
  if (!ref || !ref->node || !ref->array || !ref->storage ||
      ref->node->children.size() != 1 || ref->array->n_children != 1 ||
      !ref->array->children || ref->node->fixed_size_list_size < 0) {
    return sanitize::Status::Invalid(
        "Arrow direct fixed-size list view is invalid");
  }
  const int64_t begin =
      (ref->array->offset + ref->row) * ref->node->fixed_size_list_size;
  const int64_t end = begin + ref->node->fixed_size_list_size;
  const ArrowInputNode &child_node = ref->node->children[0];
  const ArrowArray *child_array = ref->array->children[0];
  for (int64_t i = begin; i < end; ++i) {
    SAN_RETURN_NOT_OK(
        fn(ctx, value_at(ref->storage, &child_node, child_array, i)));
  }
  return sanitize::Status::OK();
}

sanitize::Status array_for_each(const void *self, void *ctx,
                                sanitize::ValueView::ArrayEachFn fn) {
  const auto *ref = static_cast<const ArrowValueRef *>(self);
  if (!ref || !ref->node) {
    return sanitize::Status::Invalid("Arrow direct array view is invalid");
  }
  if (ref->node->kind == ArrowNodeKind::kLargeList) {
    return array_for_each_offset<int64_t>(ref, ctx, fn);
  }
  if (ref->node->kind == ArrowNodeKind::kFixedSizeList) {
    return array_for_each_fixed_size(ref, ctx, fn);
  }
  return array_for_each_offset<int32_t>(ref, ctx, fn);
}

const sanitize::ValueView::ObjectVTable kObjectVTable{
    .for_each = &object_for_each,
};

const sanitize::ValueView::ArrayVTable kArrayVTable{
    .for_each = &array_for_each,
};

} // namespace

ArrowArrayStorage::~ArrowArrayStorage() {
  sanitize::internal::cdata_stream::release_array_nothrow(&array);
  sanitize::internal::cdata_stream::clear_array(&array);
}

const ArrowValueRef *store_value_ref(ArrowBatchStorage *storage,
                                     const ArrowInputNode *node,
                                     const ArrowArray *array, int64_t row) {
  storage->values.push_back(ArrowValueRef{
      .node = node, .array = array, .row = row, .storage = storage});
  return &storage->values.back();
}

namespace {

bool value_requires_stable_ref(const ArrowInputNode *node) {
  if (!node) {
    return false;
  }
  switch (node->kind) {
  case ArrowNodeKind::kStruct:
  case ArrowNodeKind::kList:
  case ArrowNodeKind::kLargeList:
  case ArrowNodeKind::kFixedSizeList:
  case ArrowNodeKind::kMap:
    return true;
  case ArrowNodeKind::kDictionary:
    return node->children.size() == 1 &&
           value_requires_stable_ref(&node->children[0]);
  default:
    return false;
  }
}

} // namespace

sanitize::ValueView value_at(ArrowBatchStorage *storage,
                             const ArrowInputNode *node,
                             const ArrowArray *array, int64_t row) {
  if (value_requires_stable_ref(node)) {
    return value_from_ref(store_value_ref(storage, node, array, row));
  }
  const ArrowValueRef ref{
      .node = node, .array = array, .row = row, .storage = storage};
  return value_from_ref(&ref);
}

sanitize::ValueView value_from_ref(const ArrowValueRef *ref) {
  if (!ref || !ref->node || !ref->array || is_null_at(ref->array, ref->row)) {
    return sanitize::ValueView::Null();
  }
  const int64_t value_index = ref->array->offset + ref->row;
  switch (ref->node->kind) {
  case ArrowNodeKind::kNull:
    return sanitize::ValueView::Null();
  case ArrowNodeKind::kBool: {
    const auto *bitmap = static_cast<const uint8_t *>(ref->array->buffers[1]);
    return sanitize::ValueView::Bool(bit_at(bitmap, value_index));
  }
  case ArrowNodeKind::kInt:
    return integer_value(ref->array, ref->row, ref->node->storage_kind);
  case ArrowNodeKind::kUInt64Text:
    return sanitize::ValueView::String(append_stored_string(
        ref->storage,
        uint64_to_string(primitive_at<uint64_t>(ref->array, ref->row))));
  case ArrowNodeKind::kFloat:
    if (ref->node->storage_kind == ArrowStorageKind::kFloat32) {
      return sanitize::ValueView::Float(
          primitive_at<float>(ref->array, ref->row));
    }
    return sanitize::ValueView::Float(
        primitive_at<double>(ref->array, ref->row));
  case ArrowNodeKind::kUtf8:
    if (ref->node->storage_kind == ArrowStorageKind::kOffset64) {
      return string_like_value<int64_t>(ref->array, ref->row, ref->storage,
                                        false);
    }
    return string_like_value<int32_t>(ref->array, ref->row, ref->storage,
                                      false);
  case ArrowNodeKind::kBinaryBase64:
    if (ref->node->storage_kind == ArrowStorageKind::kOffset64) {
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
    return sanitize::ValueView::String(append_stored_string(
        ref->storage,
        time_value_to_string(ref->array, ref->row, ref->node->storage_kind)));
  case ArrowNodeKind::kDurationText:
    return sanitize::ValueView::String(append_stored_string(
        ref->storage,
        duration_value_to_string(primitive_at<int64_t>(ref->array, ref->row),
                                 ref->node->storage_kind)));
  case ArrowNodeKind::kIntervalText:
    return sanitize::ValueView::String(append_stored_string(
        ref->storage, interval_value_to_string(ref->array, ref->row,
                                               ref->node->storage_kind)));
  case ArrowNodeKind::kStruct:
    return sanitize::ValueView::ObjectView(ref, &kObjectVTable);
  case ArrowNodeKind::kList:
  case ArrowNodeKind::kLargeList:
  case ArrowNodeKind::kFixedSizeList:
  case ArrowNodeKind::kMap:
    return sanitize::ValueView::ArrayView(ref, &kArrayVTable);
  case ArrowNodeKind::kDictionary: {
    if (ref->node->children.size() != 1 || !ref->array->dictionary) {
      return sanitize::ValueView::Null();
    }
    auto index =
        dictionary_index_at(ref->array, ref->node->storage_kind, ref->row);
    if (!index || *index < 0 || *index >= ref->array->dictionary->length) {
      return sanitize::ValueView::Null();
    }
    return value_at(ref->storage, &ref->node->children[0],
                    ref->array->dictionary, *index);
  }
  }
  return sanitize::ValueView::Null();
}

} // namespace core_abi3_internal
