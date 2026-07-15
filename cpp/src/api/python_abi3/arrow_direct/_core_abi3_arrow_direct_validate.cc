// Validates logical Arrow C Data bounds before direct batch materialization.
#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_validate.hh"

#include "internal/memory/memory_budget.hh"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string_view>

namespace core_abi3_internal {
namespace {

constexpr std::int64_t kMaxArrowDepth = 64;

struct ValidationLimits {
  std::int64_t logical_slots = 0;
  std::int64_t logical_buffer_bytes = 0;
};

sanitize::Status validate_slice(const ArrowArray &array, std::int64_t first,
                                std::int64_t length, std::string_view label,
                                const ValidationLimits &limits) {
  if (array.length < 0 || array.offset < 0 || first < 0 || length < 0 ||
      first > array.length || length > array.length - first ||
      array.null_count < -1 || array.null_count > array.length ||
      array.n_buffers < 0 || array.n_children < 0) {
    return sanitize::Status::Invalid("Arrow direct ", label,
                                     " has invalid logical metadata");
  }
  if (array.null_count > 0 && (!array.buffers || !array.buffers[0])) {
    return sanitize::Status::Invalid("Arrow direct ", label,
                                     " is missing its validity bitmap");
  }
  if (array.length > limits.logical_slots) {
    return sanitize::Status::OutOfMemory("Arrow direct ", label,
                                         " exceeds logical slot limit");
  }
  if (array.offset > std::numeric_limits<std::int64_t>::max() - first ||
      array.offset + first >
          std::numeric_limits<std::int64_t>::max() - length) {
    return sanitize::Status::Invalid("Arrow direct ", label,
                                     " logical range overflows int64");
  }
  const auto logical_end = array.offset + first + length;
  if (logical_end > limits.logical_slots) {
    return sanitize::Status::OutOfMemory(
        "Arrow direct ", label, " absolute logical range exceeds slot limit");
  }
  return {};
}

sanitize::Status require_buffers(const ArrowArray &array, std::int64_t expected,
                                 std::string_view label) {
  if (array.n_buffers != expected || (expected > 0 && !array.buffers)) {
    return sanitize::Status::Invalid("Arrow direct ", label,
                                     " has invalid buffer layout");
  }
  return {};
}

sanitize::Status require_children(const ArrowArray &array,
                                  std::int64_t expected,
                                  std::string_view label) {
  if (array.n_children != expected || (expected > 0 && !array.children)) {
    return sanitize::Status::Invalid("Arrow direct ", label,
                                     " has invalid child layout");
  }
  return {};
}

template <typename OffsetT>
sanitize::Result<std::pair<std::int64_t, std::int64_t>>
validate_offsets(const ArrowArray &array, std::int64_t first_row,
                 std::int64_t length, std::string_view label,
                 std::int64_t expected_buffers, bool require_values,
                 bool bound_bytes, const ValidationLimits &limits) {
  SAN_RETURN_NOT_OK(require_buffers(array, expected_buffers, label));
  if (length == 0) {
    return std::pair<std::int64_t, std::int64_t>{0, 0};
  }
  if (!array.buffers[1] ||
      (require_values && expected_buffers > 2 && !array.buffers[2])) {
    return sanitize::Status::Invalid("Arrow direct ", label,
                                     " is missing offsets or values");
  }
  const auto *offsets = static_cast<const OffsetT *>(array.buffers[1]);
  const auto first_slot = array.offset + first_row;
  const auto last_slot = first_slot + length;
  const auto first = static_cast<std::int64_t>(offsets[first_slot]);
  if (first < 0) {
    return sanitize::Status::Invalid("Arrow direct ", label,
                                     " has a negative first offset");
  }
  auto previous = first;
  for (auto slot = first_slot + 1; slot <= last_slot; ++slot) {
    const auto current = static_cast<std::int64_t>(offsets[slot]);
    if (current < previous) {
      return sanitize::Status::Invalid("Arrow direct ", label,
                                       " offsets are not monotonic");
    }
    previous = current;
  }
  if (bound_bytes && previous > limits.logical_buffer_bytes) {
    return sanitize::Status::OutOfMemory(
        "Arrow direct ", label,
        " absolute buffer endpoint exceeds logical byte limit");
  }
  return std::pair<std::int64_t, std::int64_t>{first, previous};
}

bool slot_is_valid(const ArrowArray &array, std::int64_t row) {
  if (array.null_count == 0 || !array.buffers || !array.buffers[0]) {
    return true;
  }
  const auto *bitmap = static_cast<const std::uint8_t *>(array.buffers[0]);
  const auto index = array.offset + row;
  return (bitmap[index >> 3] & static_cast<std::uint8_t>(1U << (index & 7))) !=
         0;
}

template <typename IndexT>
sanitize::Status validate_dictionary_index_values(const ArrowArray &array,
                                                  std::int64_t first_row,
                                                  std::int64_t length,
                                                  std::int64_t dictionary_size,
                                                  std::string_view label) {
  const auto *indices = static_cast<const IndexT *>(array.buffers[1]);
  for (std::int64_t row = first_row; row < first_row + length; ++row) {
    if (!slot_is_valid(array, row)) {
      continue;
    }
    const auto index = static_cast<std::int64_t>(indices[array.offset + row]);
    if (index < 0 || index >= dictionary_size) {
      return sanitize::Status::Invalid("Arrow direct ", label,
                                       " has an out-of-range dictionary index");
    }
  }
  return {};
}

sanitize::Status validate_dictionary_indices(const ArrowInputNode &node,
                                             const ArrowArray &array,
                                             std::int64_t first_row,
                                             std::int64_t length) {
  const auto dictionary_size = array.dictionary->length;
  switch (node.storage_kind) {
  case ArrowStorageKind::kInt8:
    return validate_dictionary_index_values<std::int8_t>(
        array, first_row, length, dictionary_size, node.name);
  case ArrowStorageKind::kUInt8:
    return validate_dictionary_index_values<std::uint8_t>(
        array, first_row, length, dictionary_size, node.name);
  case ArrowStorageKind::kInt16:
    return validate_dictionary_index_values<std::int16_t>(
        array, first_row, length, dictionary_size, node.name);
  case ArrowStorageKind::kUInt16:
    return validate_dictionary_index_values<std::uint16_t>(
        array, first_row, length, dictionary_size, node.name);
  case ArrowStorageKind::kInt32:
    return validate_dictionary_index_values<std::int32_t>(
        array, first_row, length, dictionary_size, node.name);
  case ArrowStorageKind::kUInt32:
    return validate_dictionary_index_values<std::uint32_t>(
        array, first_row, length, dictionary_size, node.name);
  case ArrowStorageKind::kInt64:
    return validate_dictionary_index_values<std::int64_t>(
        array, first_row, length, dictionary_size, node.name);
  default:
    return sanitize::Status::Invalid("Arrow direct ", node.name,
                                     " uses an unsupported dictionary index");
  }
}

sanitize::Status validate_node(const ArrowInputNode &node,
                               const ArrowArray &array, std::int64_t first_row,
                               std::int64_t length, std::int64_t depth,
                               const ValidationLimits &limits) {
  if (depth > kMaxArrowDepth) {
    return sanitize::Status::OutOfMemory(
        "Arrow direct array nesting exceeds safety limit");
  }
  SAN_RETURN_NOT_OK(
      validate_slice(array, first_row, length, node.name, limits));

  switch (node.kind) {
  case ArrowNodeKind::kNull:
    SAN_RETURN_NOT_OK(require_buffers(array, 1, node.name));
    return require_children(array, 0, node.name);
  case ArrowNodeKind::kBool:
  case ArrowNodeKind::kInt:
  case ArrowNodeKind::kUInt64Text:
  case ArrowNodeKind::kFloat:
  case ArrowNodeKind::kDecimalText:
  case ArrowNodeKind::kTimestamp:
  case ArrowNodeKind::kDate32:
  case ArrowNodeKind::kDate64:
  case ArrowNodeKind::kTime32s:
  case ArrowNodeKind::kTimeText:
  case ArrowNodeKind::kDurationText:
  case ArrowNodeKind::kIntervalText:
    SAN_RETURN_NOT_OK(require_buffers(array, 2, node.name));
    SAN_RETURN_NOT_OK(require_children(array, 0, node.name));
    if (length > 0 && !array.buffers[1]) {
      return sanitize::Status::Invalid("Arrow direct ", node.name,
                                       " is missing its values buffer");
    }
    return {};
  case ArrowNodeKind::kUtf8:
  case ArrowNodeKind::kBinaryBase64:
    SAN_RETURN_NOT_OK(require_children(array, 0, node.name));
    if (node.storage_kind == ArrowStorageKind::kOffset64) {
      SAN_ASSIGN_OR_RAISE(auto ignored, validate_offsets<std::int64_t>(
                                            array, first_row, length, node.name,
                                            3, true, true, limits));
      (void)ignored;
    } else {
      SAN_ASSIGN_OR_RAISE(auto ignored, validate_offsets<std::int32_t>(
                                            array, first_row, length, node.name,
                                            3, true, true, limits));
      (void)ignored;
    }
    return {};
  case ArrowNodeKind::kStruct:
    SAN_RETURN_NOT_OK(require_buffers(array, 1, node.name));
    SAN_RETURN_NOT_OK(require_children(
        array, static_cast<std::int64_t>(node.children.size()), node.name));
    for (std::size_t index = 0; index < node.children.size(); ++index) {
      if (!array.children[index]) {
        return sanitize::Status::Invalid("Arrow direct ", node.name,
                                         " has a null child pointer");
      }
      SAN_RETURN_NOT_OK(validate_node(node.children[index],
                                      *array.children[index], first_row, length,
                                      depth + 1, limits));
    }
    return {};
  case ArrowNodeKind::kList:
  case ArrowNodeKind::kLargeList:
  case ArrowNodeKind::kMap: {
    if (node.children.size() != 1) {
      return sanitize::Status::Invalid("Arrow direct ", node.name,
                                       " schema has invalid list children");
    }
    SAN_RETURN_NOT_OK(require_children(array, 1, node.name));
    if (!array.children[0]) {
      return sanitize::Status::Invalid("Arrow direct ", node.name,
                                       " has a null values child");
    }
    std::pair<std::int64_t, std::int64_t> range;
    if (node.kind == ArrowNodeKind::kLargeList) {
      SAN_ASSIGN_OR_RAISE(range, validate_offsets<std::int64_t>(
                                     array, first_row, length, node.name, 2,
                                     false, false, limits));
    } else {
      SAN_ASSIGN_OR_RAISE(range, validate_offsets<std::int32_t>(
                                     array, first_row, length, node.name, 2,
                                     false, false, limits));
    }
    if (range.first < 0 || range.second < range.first ||
        range.second > array.children[0]->length) {
      return sanitize::Status::Invalid("Arrow direct ", node.name,
                                       " child range exceeds child length");
    }
    return validate_node(node.children[0], *array.children[0], range.first,
                         range.second - range.first, depth + 1, limits);
  }
  case ArrowNodeKind::kFixedSizeList: {
    if (node.children.size() != 1 || node.fixed_size_list_size < 0) {
      return sanitize::Status::Invalid("Arrow direct ", node.name,
                                       " has invalid fixed-list schema");
    }
    SAN_RETURN_NOT_OK(require_buffers(array, 1, node.name));
    SAN_RETURN_NOT_OK(require_children(array, 1, node.name));
    if (!array.children[0]) {
      return sanitize::Status::Invalid("Arrow direct ", node.name,
                                       " fixed-list child is missing");
    }
    const auto parent_first = array.offset + first_row;
    if (node.fixed_size_list_size > 0 &&
        (parent_first > std::numeric_limits<std::int64_t>::max() /
                            node.fixed_size_list_size ||
         length > std::numeric_limits<std::int64_t>::max() /
                      node.fixed_size_list_size)) {
      return sanitize::Status::Invalid("Arrow direct ", node.name,
                                       " fixed-list range overflows");
    }
    const auto child_first = parent_first * node.fixed_size_list_size;
    const auto child_length = length * node.fixed_size_list_size;
    if (child_first > array.children[0]->length ||
        child_length > array.children[0]->length - child_first) {
      return sanitize::Status::Invalid("Arrow direct ", node.name,
                                       " fixed-list child is too short");
    }
    return validate_node(node.children[0], *array.children[0], child_first,
                         child_length, depth + 1, limits);
  }
  case ArrowNodeKind::kDictionary:
    SAN_RETURN_NOT_OK(require_buffers(array, 2, node.name));
    SAN_RETURN_NOT_OK(require_children(array, 0, node.name));
    if (node.children.size() != 1 || !array.dictionary ||
        array.dictionary->length < 0 || (length > 0 && !array.buffers[1])) {
      return sanitize::Status::Invalid("Arrow direct ", node.name,
                                       " has invalid dictionary storage");
    }
    SAN_RETURN_NOT_OK(
        validate_dictionary_indices(node, array, first_row, length));
    return validate_node(node.children[0], *array.dictionary, 0,
                         array.dictionary->length, depth + 1, limits);
  }
  return sanitize::Status::Invalid("Arrow direct node kind is invalid");
}

} // namespace

sanitize::Status
validate_arrow_direct_batch(const ArrowArray &root,
                            const std::vector<ArrowInputNode> &fields,
                            std::int64_t memory_limit_bytes) {
  const auto budget =
      sanitize::internal::memory_budget_from_limit(memory_limit_bytes);
  const ValidationLimits limits{budget.arrow_logical_slots,
                                budget.arrow_logical_buffer_bytes};
  SAN_RETURN_NOT_OK(
      validate_slice(root, 0, root.length, "record batch", limits));
  SAN_RETURN_NOT_OK(require_buffers(root, 1, "record batch"));
  SAN_RETURN_NOT_OK(require_children(
      root, static_cast<std::int64_t>(fields.size()), "record batch"));
  for (std::size_t index = 0; index < fields.size(); ++index) {
    if (!root.children[index]) {
      return sanitize::Status::Invalid(
          "Arrow direct record batch has a null child pointer");
    }
    SAN_RETURN_NOT_OK(validate_node(fields[index], *root.children[index], 0,
                                    root.length, 1, limits));
  }
  return {};
}

} // namespace core_abi3_internal
