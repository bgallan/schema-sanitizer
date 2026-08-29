/*
 * Implements Arrow C stream coalescing array finalization.
 *
 * The phases validate schemas, append slices, and export one owned Arrow array
 * under budget.
 */

#include "api/python_abi3/streaming/coalesce_stream_internal.hh"

#include "internal/arrow_c/cdata_stream_callbacks.hh"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>

namespace core_abi3_internal::coalesce_detail {
namespace {

/// Adds byte or item counts while clamping overflow to the representable
/// maximum.
std::size_t saturating_add(std::size_t left, std::size_t right) noexcept {
  return right > std::numeric_limits<std::size_t>::max() - left
             ? std::numeric_limits<std::size_t>::max()
             : left + right;
}

/// Multiplies byte or item counts while clamping overflow to the representable
/// maximum.
std::size_t saturating_multiply(std::size_t left, std::size_t right) noexcept {
  return left != 0 && right > std::numeric_limits<std::size_t>::max() / left
             ? std::numeric_limits<std::size_t>::max()
             : left * right;
}

/// Calculates recursively retained coalesced buffers for memory-budget
/// accounting.
std::size_t retained_bytes_impl(const CoalescedNode &node) noexcept {
  std::size_t total = node.validity.capacity();
  total = saturating_add(total, node.data.capacity());
  total = saturating_add(total, saturating_multiply(node.offsets32.capacity(),
                                                    sizeof(std::int32_t)));
  total = saturating_add(total, saturating_multiply(node.offsets64.capacity(),
                                                    sizeof(std::int64_t)));
  total = saturating_add(total, saturating_multiply(node.children.capacity(),
                                                    sizeof(CoalescedNode)));
  total = saturating_add(total, saturating_multiply(node.child_ptrs.capacity(),
                                                    sizeof(ArrowArray *)));
  for (const auto &child : node.children) {
    total = saturating_add(total, retained_bytes_impl(child));
  }
  if (node.dictionary) {
    total = saturating_add(total, sizeof(CoalescedNode));
    total = saturating_add(total, retained_bytes_impl(*node.dictionary));
  }
  return total;
}

/// Calculates offset-buffer and payload bytes retained by a variable-width
/// slice.
template <class Offset>
std::size_t variable_width_slice_bytes(const ArrowArray &array,
                                       const ArraySlice &slice) noexcept {
  if (!array.buffers || !array.buffers[1]) {
    return std::numeric_limits<std::size_t>::max();
  }
  const auto *offsets = static_cast<const Offset *>(array.buffers[1]);
  const auto begin_index = array.offset + slice.offset;
  const auto end_index = begin_index + slice.length;
  if (begin_index < 0 || end_index < begin_index) {
    return std::numeric_limits<std::size_t>::max();
  }
  const auto begin = offsets[static_cast<std::size_t>(begin_index)];
  const auto end = offsets[static_cast<std::size_t>(end_index)];
  if (begin < 0 || end < begin) {
    return std::numeric_limits<std::size_t>::max();
  }
  const auto length = static_cast<std::size_t>(slice.length);
  const auto offsets_bytes = saturating_multiply(
      saturating_add(length, std::size_t{1}), sizeof(Offset));
  return saturating_add(offsets_bytes, static_cast<std::size_t>(end - begin));
}

/// Estimates the retained bytes contributed by one Arrow array slice.
std::size_t estimated_slice_bytes(const CoalesceNodeSpec &spec,
                                  const ArraySlice &slice,
                                  bool force_validity = false) noexcept {
  if (!slice.array || slice.length < 0) {
    return std::numeric_limits<std::size_t>::max();
  }
  const auto length = static_cast<std::size_t>(slice.length);
  const bool needs_validity =
      force_validity || (slice.array->null_count != 0 && slice.array->buffers &&
                         slice.array->buffers[0]);
  std::size_t total = needs_validity ? (length + 7U) / 8U : 0;
  switch (spec.kind) {
  case CoalesceKind::kFixedWidth:
  case CoalesceKind::kDictionary:
    total =
        saturating_add(total, saturating_multiply(length, spec.fixed_width));
    break;
  case CoalesceKind::kBool:
    total = saturating_add(total, (length + 7U) / 8U);
    break;
  case CoalesceKind::kUtf8:
  case CoalesceKind::kBinary:
    total = saturating_add(
        total, variable_width_slice_bytes<std::int32_t>(*slice.array, slice));
    break;
  case CoalesceKind::kLargeUtf8:
  case CoalesceKind::kLargeBinary:
    total = saturating_add(
        total, variable_width_slice_bytes<std::int64_t>(*slice.array, slice));
    break;
  case CoalesceKind::kStruct:
    if (!slice.array->children ||
        slice.array->n_children !=
            static_cast<std::int64_t>(spec.children.size())) {
      return std::numeric_limits<std::size_t>::max();
    }
    for (std::size_t index = 0; index < spec.children.size(); ++index) {
      const auto *child = slice.array->children[index];
      total = saturating_add(
          total, estimated_slice_bytes(
                     spec.children[index],
                     ArraySlice{child, slice.array->offset + slice.offset,
                                slice.length},
                     needs_validity));
    }
    break;
  case CoalesceKind::kList32:
  case CoalesceKind::kList64: {
    if (spec.children.size() != 1 || !slice.array->children ||
        !slice.array->children[0] || !slice.array->buffers ||
        !slice.array->buffers[1]) {
      return std::numeric_limits<std::size_t>::max();
    }
    if (spec.kind == CoalesceKind::kList32) {
      const auto *offsets =
          static_cast<const std::int32_t *>(slice.array->buffers[1]);
      const auto begin_index = slice.array->offset + slice.offset;
      const auto end_index = begin_index + slice.length;
      if (begin_index < 0 || end_index < begin_index) {
        return std::numeric_limits<std::size_t>::max();
      }
      const auto begin = offsets[static_cast<std::size_t>(begin_index)];
      const auto end = offsets[static_cast<std::size_t>(end_index)];
      if (begin < 0 || end < begin) {
        return std::numeric_limits<std::size_t>::max();
      }
      total = saturating_add(
          total, saturating_multiply(saturating_add(length, std::size_t{1}),
                                     sizeof(std::int32_t)));
      total = saturating_add(
          total, estimated_slice_bytes(
                     spec.children[0],
                     ArraySlice{slice.array->children[0], begin, end - begin}));
    } else {
      const auto *offsets =
          static_cast<const std::int64_t *>(slice.array->buffers[1]);
      const auto begin_index = slice.array->offset + slice.offset;
      const auto end_index = begin_index + slice.length;
      if (begin_index < 0 || end_index < begin_index) {
        return std::numeric_limits<std::size_t>::max();
      }
      const auto begin = offsets[static_cast<std::size_t>(begin_index)];
      const auto end = offsets[static_cast<std::size_t>(end_index)];
      if (begin < 0 || end < begin) {
        return std::numeric_limits<std::size_t>::max();
      }
      total = saturating_add(
          total, saturating_multiply(saturating_add(length, std::size_t{1}),
                                     sizeof(std::int64_t)));
      total = saturating_add(
          total, estimated_slice_bytes(
                     spec.children[0],
                     ArraySlice{slice.array->children[0], begin, end - begin}));
    }
    break;
  }
  }
  if (spec.kind == CoalesceKind::kDictionary && slice.array->dictionary &&
      !spec.children.empty()) {
    total = saturating_add(
        total,
        estimated_slice_bytes(spec.children[0],
                              ArraySlice{slice.array->dictionary, 0,
                                         slice.array->dictionary->length}));
  }
  return total;
}

/// Finds the largest row prefix whose retained bytes fit the coalescing budget.
std::int64_t fitting_slice_rows_impl(const CoalesceNodeSpec &spec,
                                     const ArrowArray &array,
                                     std::int64_t offset, std::int64_t max_rows,
                                     std::size_t max_bytes) noexcept {
  const auto available = array.length - offset;
  if (available <= 0 || max_rows <= 0) {
    return 0;
  }
  const auto upper = std::min(available, max_rows);
  if (max_bytes == 0) {
    return 0;
  }
  const auto full_bytes =
      estimated_slice_bytes(spec, ArraySlice{&array, offset, upper});
  if (full_bytes <= max_bytes) {
    return upper;
  }
  std::int64_t low = 0;
  std::int64_t high = upper;
  while (low < high) {
    const auto middle = low + (high - low + 1) / 2;
    const auto bytes =
        estimated_slice_bytes(spec, ArraySlice{&array, offset, middle});
    if (bytes <= max_bytes) {
      low = middle;
    } else {
      high = middle - 1;
    }
  }
  return low;
}

/// Clears a coalesced child array through its installed Arrow release callback.
void coalesced_child_release(ArrowArray *array) {
  if (!array || !array->release) {
    return;
  }
  sanitize::internal::cdata_stream::clear_array(array);
}

/// Releases the Arrow batch coalescer callback state and clears all transferred
/// Arrow ownership.
void coalesced_array_release(ArrowArray *array) {
  if (!array || !array->release) {
    return;
  }
  auto *state = static_cast<CoalescedArrayState *>(array->private_data);
  delete state;
  sanitize::internal::cdata_stream::clear_array(array);
}

} // namespace

/// Calculates retained bytes for bounded Arrow batch coalescer memory
/// accounting.
std::size_t retained_bytes(const CoalescedNode &node) {
  return retained_bytes_impl(node);
}

/// Finds the largest coalesced slice that fits both row and byte limits.
std::int64_t fitting_slice_rows(const CoalesceNodeSpec &spec,
                                const ArrowArray &array, std::int64_t offset,
                                std::int64_t max_rows,
                                std::size_t max_bytes) noexcept {
  return fitting_slice_rows_impl(spec, array, offset, max_rows, max_bytes);
}

/// Finalizes one node and transfers its buffers into Arrow callback ownership.
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

/// Finalizes coalesced buffers into an owned Arrow array with release
/// callbacks.
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
