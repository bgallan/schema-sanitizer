// Implements nested Arrow direct ValueView iteration.

#include "api/python_abi3/_core_abi3_arrow_direct_values_nested.hh"

#include "api/python_abi3/_core_abi3_arrow_direct_values.hh"

#include <cstddef>
#include <cstdint>

#include "sanitize/core/status.hh"

namespace core_abi3_internal {
namespace {

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
    const ArrowValueRef *child =
        store_value_ref(ref->storage, &children[i], child_array, ref->row);
    SAN_RETURN_NOT_OK(fn(ctx, children[i].name, 0, value_from_ref(child)));
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
    const ArrowValueRef *child =
        store_value_ref(ref->storage, &child_node, child_array, i);
    SAN_RETURN_NOT_OK(fn(ctx, value_from_ref(child)));
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
    const ArrowValueRef *child =
        store_value_ref(ref->storage, &child_node, child_array, i);
    SAN_RETURN_NOT_OK(fn(ctx, value_from_ref(child)));
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

const sanitize::ValueView::ObjectVTable &arrow_direct_object_vtable() {
  return kObjectVTable;
}

const sanitize::ValueView::ArrayVTable &arrow_direct_array_vtable() {
  return kArrayVTable;
}

} // namespace core_abi3_internal
