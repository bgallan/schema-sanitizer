/* Arrow C stream coalescing array finalization. */
#include "api/python_abi3/streaming/coalesce_stream_internal.hh"

#include "internal/arrow_c/cdata_stream_callbacks.hh"

#include <memory>

namespace core_abi3_internal::coalesce_detail {
namespace {

void coalesced_child_release(ArrowArray *array) {
  if (!array || !array->release) {
    return;
  }
  sanitize::internal::cdata_stream::clear_array(array);
}

void coalesced_array_release(ArrowArray *array) {
  if (!array || !array->release) {
    return;
  }
  auto *state = static_cast<CoalescedArrayState *>(array->private_data);
  delete state;
  sanitize::internal::cdata_stream::clear_array(array);
}

} // namespace

sanitize::Status finish_node(CoalescedNode *node, const CoalesceNodeSpec &spec,
                             bool root) {
  node->array.offset = 0;
  node->array.dictionary = nullptr;
  node->array.buffers = node->buffers;
  node->array.private_data = nullptr;
  node->array.release =
      root ? &coalesced_array_release : &coalesced_child_release;
  node->buffers[0] =
      node->array.null_count == 0 ? nullptr : node->validity.data();
  switch (spec.kind) {
  case CoalesceKind::kStruct:
    node->array.n_buffers = 1;
    node->buffers[1] = nullptr;
    node->buffers[2] = nullptr;
    break;
  case CoalesceKind::kList32:
    if (node->offsets32.empty()) {
      node->offsets32.push_back(0);
    }
    node->array.n_buffers = 2;
    node->buffers[1] = node->offsets32.data();
    node->buffers[2] = nullptr;
    break;
  case CoalesceKind::kList64:
    if (node->offsets64.empty()) {
      node->offsets64.push_back(0);
    }
    node->array.n_buffers = 2;
    node->buffers[1] = node->offsets64.data();
    node->buffers[2] = nullptr;
    break;
  case CoalesceKind::kUtf8:
  case CoalesceKind::kBinary:
    if (node->offsets32.empty()) {
      node->offsets32.push_back(0);
    }
    node->array.n_buffers = 3;
    node->buffers[1] = node->offsets32.data();
    node->buffers[2] = node->data.empty() ? nullptr : node->data.data();
    break;
  case CoalesceKind::kLargeUtf8:
  case CoalesceKind::kLargeBinary:
    if (node->offsets64.empty()) {
      node->offsets64.push_back(0);
    }
    node->array.n_buffers = 3;
    node->buffers[1] = node->offsets64.data();
    node->buffers[2] = node->data.empty() ? nullptr : node->data.data();
    break;
  case CoalesceKind::kBool:
  case CoalesceKind::kFixedWidth:
    node->array.n_buffers = 2;
    node->buffers[1] = node->data.empty() ? nullptr : node->data.data();
    node->buffers[2] = nullptr;
    break;
  case CoalesceKind::kDictionary:
    node->array.n_buffers = 2;
    node->buffers[1] = node->data.empty() ? nullptr : node->data.data();
    node->buffers[2] = nullptr;
    if (!node->dictionary_ready || !node->dictionary ||
        spec.children.size() != 1) {
      return sanitize::Status::Invalid(
          "coalescing stream dictionary column has no output dictionary");
    }
    SAN_RETURN_NOT_OK(
        finish_node(node->dictionary.get(), spec.children[0], false));
    node->dictionary_ptr = &node->dictionary->array;
    node->array.dictionary = node->dictionary_ptr;
    node->array.n_children = 0;
    node->array.children = nullptr;
    return sanitize::Status::OK();
  }

  node->child_ptrs.clear();
  node->child_ptrs.reserve(node->children.size());
  for (std::size_t i = 0; i < node->children.size(); ++i) {
    SAN_RETURN_NOT_OK(finish_node(&node->children[i], spec.children[i], false));
    node->child_ptrs.push_back(&node->children[i].array);
  }
  node->array.n_children = static_cast<std::int64_t>(node->child_ptrs.size());
  node->array.children =
      node->child_ptrs.empty() ? nullptr : node->child_ptrs.data();
  return sanitize::Status::OK();
}

sanitize::Status
export_coalesced_array(std::unique_ptr<CoalescedArrayState> state,
                       ArrowArray *out) {
  sanitize::internal::cdata_stream::clear_array(out);
  *out = state->root.array;
  out->private_data = state.release();
  out->release = &coalesced_array_release;
  return sanitize::Status::OK();
}

} // namespace core_abi3_internal::coalesce_detail
