// Estimates conservative encoded row sizes for bounded text-output packets.
// The helpers bound parallel text encoding memory while committing prepared
// fragments in source order.

#pragma once

#include "internal/json_output/schema/model.hh"
#include "nanoarrow/nanoarrow.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace sanitize::internal::text_output_estimator {
namespace jsonl = sanitize::internal::jsonl_stream_writer;

inline constexpr std::int64_t kMaximumEstimateDepth = 64;

/// Adds two size estimates and clamps the result to the caller's cap.
[[nodiscard]] inline std::int64_t
add_capped(std::int64_t left, std::int64_t right, std::int64_t cap) noexcept {
  if (left >= cap || right >= cap - left) {
    return cap;
  }
  return left + right;
}

/// Multiplies two output-size estimates and clamps the result to the caller's
/// cap.
[[nodiscard]] inline std::int64_t multiply_capped(std::int64_t value,
                                                  std::int64_t factor,
                                                  std::int64_t cap) noexcept {
  if (value <= 0 || factor <= 0) {
    return 0;
  }
  if (value >= cap / factor) {
    return cap;
  }
  return value * factor;
}

/// Reads one bit from a non-null Arrow validity bitmap.
[[nodiscard]] inline bool validity_bit_is_set(const std::uint8_t *bitmap,
                                              std::int64_t index) noexcept {
  return (bitmap[index >> 3] & static_cast<std::uint8_t>(1u << (index & 7))) !=
         0;
}

/// Tests Arrow validity at the logical index, treating an absent bitmap as all
/// valid.
[[nodiscard]] inline bool array_is_null(const ArrowArray &array,
                                        std::int64_t row) noexcept {
  if (array.null_count == 0 || !array.buffers || !array.buffers[0]) {
    return false;
  }
  const auto *bitmap = static_cast<const std::uint8_t *>(array.buffers[0]);
  return !validity_bit_is_set(bitmap, array.offset + row);
}

template <typename OffsetT>
/// Returns the checked payload span described by Arrow variable-width offsets.
[[nodiscard]] inline std::int64_t
variable_width_bytes(const ArrowArray &array, std::int64_t row,
                     std::int64_t cap) noexcept {
  if (!array.buffers || !array.buffers[1]) {
    return cap;
  }
  const auto *offsets = static_cast<const OffsetT *>(array.buffers[1]);
  const auto slot = array.offset + row;
  const auto begin = offsets[slot];
  const auto end = offsets[slot + 1];
  if (begin < 0 || end < begin) {
    return cap;
  }
  const auto width = static_cast<std::uint64_t>(end - begin);
  if (width >= static_cast<std::uint64_t>(cap)) {
    return cap;
  }
  return static_cast<std::int64_t>(width);
}

/// Reads one signed or unsigned dictionary index using the field's declared
/// physical width.
[[nodiscard]] inline std::int64_t
dictionary_index_at(const ArrowArray &array, jsonl::JsonlKind kind,
                    std::int64_t row) noexcept {
  if (!array.buffers || !array.buffers[1]) {
    return -1;
  }
  const auto slot = array.offset + row;
  switch (kind) {
  case jsonl::JsonlKind::kInt8:
    return static_cast<const std::int8_t *>(array.buffers[1])[slot];
  case jsonl::JsonlKind::kUInt8:
    return static_cast<const std::uint8_t *>(array.buffers[1])[slot];
  case jsonl::JsonlKind::kInt16:
    return static_cast<const std::int16_t *>(array.buffers[1])[slot];
  case jsonl::JsonlKind::kUInt16:
    return static_cast<const std::uint16_t *>(array.buffers[1])[slot];
  case jsonl::JsonlKind::kInt32:
    return static_cast<const std::int32_t *>(array.buffers[1])[slot];
  case jsonl::JsonlKind::kUInt32:
    return static_cast<const std::uint32_t *>(array.buffers[1])[slot];
  case jsonl::JsonlKind::kInt64:
    return static_cast<const std::int64_t *>(array.buffers[1])[slot];
  case jsonl::JsonlKind::kUInt64: {
    const auto value =
        static_cast<const std::uint64_t *>(array.buffers[1])[slot];
    return value <= static_cast<std::uint64_t>(
                        std::numeric_limits<std::int64_t>::max())
               ? static_cast<std::int64_t>(value)
               : -1;
  }
  default:
    return -1;
  }
}

/// Estimates the encoded bytes for one Arrow value without exceeding the
/// caller's cap.
[[nodiscard]] inline std::int64_t
estimate_value_bytes(const jsonl::JsonlField &field, const ArrowArray &array,
                     std::int64_t row, std::int64_t cap,
                     std::int64_t depth = 0) noexcept {
  if (cap <= 1 || depth > kMaximumEstimateDepth) {
    return std::max<std::int64_t>(1, cap);
  }
  if (array_is_null(array, row)) {
    return std::min<std::int64_t>(4, cap);
  }

  using Kind = jsonl::JsonlKind;
  switch (field.kind) {
  case Kind::kNull:
    return std::min<std::int64_t>(4, cap);
  case Kind::kBool:
    return std::min<std::int64_t>(5, cap);
  case Kind::kInt8:
  case Kind::kUInt8:
  case Kind::kInt16:
  case Kind::kUInt16:
  case Kind::kInt32:
  case Kind::kUInt32:
  case Kind::kInt64:
  case Kind::kUInt64:
    return std::min<std::int64_t>(24, cap);
  case Kind::kFloat16:
  case Kind::kFloat32:
  case Kind::kFloat64:
    return std::min<std::int64_t>(32, cap);
  case Kind::kTimestampMillis:
  case Kind::kTimestampMicros:
  case Kind::kTimestampNanos:
  case Kind::kDate32:
  case Kind::kDate64:
  case Kind::kTime32s:
  case Kind::kTime32ms:
  case Kind::kTime64us:
  case Kind::kTime64ns:
  case Kind::kDuration:
  case Kind::kInterval:
    return std::min<std::int64_t>(64, cap);
  case Kind::kDecimal:
    return std::min<std::int64_t>(96, cap);
  case Kind::kString:
  case Kind::kLargeString: {
    const auto raw = field.kind == Kind::kString
                         ? variable_width_bytes<std::int32_t>(array, row, cap)
                         : variable_width_bytes<std::int64_t>(array, row, cap);
    return add_capped(multiply_capped(raw, 6, cap), 2, cap);
  }
  case Kind::kBinary:
  case Kind::kLargeBinary: {
    const auto raw = field.kind == Kind::kBinary
                         ? variable_width_bytes<std::int32_t>(array, row, cap)
                         : variable_width_bytes<std::int64_t>(array, row, cap);
    const auto groups = add_capped(raw, 2, cap) / 3;
    return add_capped(multiply_capped(groups, 4, cap), 2, cap);
  }
  case Kind::kFixedSizeBinary: {
    const auto raw = std::max<std::int64_t>(0, field.fixed_size_binary_size);
    const auto groups = add_capped(raw, 2, cap) / 3;
    return add_capped(multiply_capped(groups, 4, cap), 2, cap);
  }
  case Kind::kStruct: {
    std::int64_t total = 2;
    if (array.n_children != static_cast<std::int64_t>(field.children.size()) ||
        (!field.children.empty() && !array.children)) {
      return cap;
    }
    const auto exact_prefixes =
        field.member_prefixes.size() == field.children.size();
    for (std::size_t index = 0; index < field.children.size(); ++index) {
      const auto prefix_bytes =
          exact_prefixes
              ? static_cast<std::int64_t>(field.member_prefixes[index].size())
              : add_capped(
                    multiply_capped(static_cast<std::int64_t>(
                                        field.children[index].name.size()),
                                    6, cap),
                    index == 0 ? 3 : 4, cap);
      total = add_capped(total, prefix_bytes, cap);
      total = add_capped(
          total,
          estimate_value_bytes(field.children[index], *array.children[index],
                               array.offset + row, cap - total, depth + 1),
          cap);
      if (total >= cap) {
        break;
      }
    }
    return total;
  }
  case Kind::kList:
  case Kind::kMap:
  case Kind::kLargeList: {
    if (field.children.size() != 1 || array.n_children != 1 ||
        !array.children || !array.buffers || !array.buffers[1]) {
      return cap;
    }
    std::int64_t begin = 0;
    std::int64_t end = 0;
    const auto slot = array.offset + row;
    if (field.kind == Kind::kLargeList) {
      const auto *offsets = static_cast<const std::int64_t *>(array.buffers[1]);
      begin = offsets[slot];
      end = offsets[slot + 1];
    } else {
      const auto *offsets = static_cast<const std::int32_t *>(array.buffers[1]);
      begin = offsets[slot];
      end = offsets[slot + 1];
    }
    if (begin < 0 || end < begin) {
      return cap;
    }
    std::int64_t total = 2;
    for (auto item = begin; item < end && total < cap; ++item) {
      total = add_capped(total, item == begin ? 0 : 1, cap);
      total =
          add_capped(total,
                     estimate_value_bytes(field.children[0], *array.children[0],
                                          item, cap - total, depth + 1),
                     cap);
    }
    return total;
  }
  case Kind::kFixedSizeList: {
    if (field.children.size() != 1 || array.n_children != 1 ||
        !array.children || field.fixed_size_list_size < 0) {
      return cap;
    }
    const auto width = static_cast<std::int64_t>(field.fixed_size_list_size);
    const auto begin = (array.offset + row) * width;
    const auto end = begin + width;
    std::int64_t total = 2;
    for (auto item = begin; item < end && total < cap; ++item) {
      total = add_capped(total, item == begin ? 0 : 1, cap);
      total =
          add_capped(total,
                     estimate_value_bytes(field.children[0], *array.children[0],
                                          item, cap - total, depth + 1),
                     cap);
    }
    return total;
  }
  case Kind::kDictionary: {
    if (field.children.size() != 1 || !array.dictionary) {
      return cap;
    }
    const auto index =
        dictionary_index_at(array, field.dictionary_index_kind, row);
    if (index < 0 || index >= array.dictionary->length) {
      return std::min<std::int64_t>(4, cap);
    }
    return estimate_value_bytes(field.children[0], *array.dictionary, index,
                                cap, depth + 1);
  }
  }
  return cap;
}

/// Estimates the encoded bytes for one JSON Lines row without exceeding the
/// caller's cap.
[[nodiscard]] inline std::int64_t
estimate_jsonl_row_bytes(const jsonl::JsonlField &root, const ArrowArray &array,
                         std::int64_t row, std::int64_t cap) noexcept {
  return add_capped(estimate_value_bytes(root, array, row, cap), 1, cap);
}

/// Reports whether an Arrow scalar has a schema-only JSON output size bound.
[[nodiscard]] inline bool
fixed_cost_jsonl_scalar_kind(jsonl::JsonlKind kind) noexcept {
  using Kind = jsonl::JsonlKind;
  switch (kind) {
  case Kind::kNull:
  case Kind::kBool:
  case Kind::kInt8:
  case Kind::kUInt8:
  case Kind::kInt16:
  case Kind::kUInt16:
  case Kind::kInt32:
  case Kind::kUInt32:
  case Kind::kInt64:
  case Kind::kUInt64:
  case Kind::kFloat16:
  case Kind::kFloat32:
  case Kind::kFloat64:
  case Kind::kFixedSizeBinary:
  case Kind::kTimestampMillis:
  case Kind::kTimestampMicros:
  case Kind::kTimestampNanos:
  case Kind::kDate32:
  case Kind::kDate64:
  case Kind::kTime32s:
  case Kind::kTime32ms:
  case Kind::kTime64us:
  case Kind::kTime64ns:
  case Kind::kDuration:
  case Kind::kInterval:
  case Kind::kDecimal:
    return true;
  default:
    return false;
  }
}

/// Returns the schema-derived maximum JSON bytes for one fixed-cost scalar.
[[nodiscard]] inline std::int64_t
fixed_jsonl_scalar_output_upper_bound(const jsonl::JsonlField &field,
                                      std::int64_t cap) noexcept {
  using Kind = jsonl::JsonlKind;
  switch (field.kind) {
  case Kind::kNull:
    return std::min<std::int64_t>(4, cap);
  case Kind::kBool:
    return std::min<std::int64_t>(5, cap);
  case Kind::kInt8:
  case Kind::kUInt8:
  case Kind::kInt16:
  case Kind::kUInt16:
  case Kind::kInt32:
  case Kind::kUInt32:
  case Kind::kInt64:
  case Kind::kUInt64:
    return std::min<std::int64_t>(24, cap);
  case Kind::kFloat16:
  case Kind::kFloat32:
  case Kind::kFloat64:
    return std::min<std::int64_t>(32, cap);
  case Kind::kTimestampMillis:
  case Kind::kTimestampMicros:
  case Kind::kTimestampNanos:
  case Kind::kDate32:
  case Kind::kDate64:
  case Kind::kTime32s:
  case Kind::kTime32ms:
  case Kind::kTime64us:
  case Kind::kTime64ns:
  case Kind::kDuration:
  case Kind::kInterval:
    return std::min<std::int64_t>(64, cap);
  case Kind::kDecimal:
    return std::min<std::int64_t>(96, cap);
  case Kind::kFixedSizeBinary: {
    const auto raw = std::max<std::int64_t>(0, field.fixed_size_binary_size);
    const auto groups = add_capped(raw, 2, cap) / 3;
    return add_capped(multiply_capped(groups, 4, cap), 2, cap);
  }
  default:
    return cap;
  }
}

/// Returns a conservative schema-only JSON Lines row bound for flat fixed-cost
/// fields.
[[nodiscard]] inline std::int64_t estimate_wide_fixed_jsonl_row_upper_bound(
    const jsonl::JsonlField &root) noexcept {
  const auto cap = std::numeric_limits<std::int64_t>::max();
  if (root.kind != jsonl::JsonlKind::kStruct ||
      root.member_prefixes.size() != root.children.size() ||
      !std::all_of(root.children.begin(), root.children.end(),
                   [](const jsonl::JsonlField &field) {
                     return fixed_cost_jsonl_scalar_kind(field.kind);
                   })) {
    return 0;
  }
  std::int64_t total = 3; // Opening/closing braces plus trailing newline.
  for (std::size_t index = 0; index < root.children.size(); ++index) {
    total = add_capped(
        total, static_cast<std::int64_t>(root.member_prefixes[index].size()),
        cap);
    total = add_capped(
        total, fixed_jsonl_scalar_output_upper_bound(root.children[index], cap),
        cap);
  }
  return total;
}

/// Reports whether a wide fixed-cost batch has no root or top-level child
/// nulls.
[[nodiscard]] inline bool
wide_fixed_jsonl_batch_has_no_nulls(const jsonl::JsonlField &root,
                                    const ArrowArray &array) noexcept {
  if (array.null_count != 0 ||
      array.n_children != static_cast<std::int64_t>(root.children.size()) ||
      (!root.children.empty() && !array.children)) {
    return false;
  }
  for (std::size_t index = 0; index < root.children.size(); ++index) {
    if (!array.children[index] || array.children[index]->null_count != 0) {
      return false;
    }
  }
  return true;
}

/// Reports whether an Arrow field can use the allocation-free direct CSV scalar
/// formatter.
[[nodiscard]] inline bool
direct_csv_scalar_kind(jsonl::JsonlKind kind) noexcept {
  using Kind = jsonl::JsonlKind;
  switch (kind) {
  case Kind::kBool:
  case Kind::kInt8:
  case Kind::kUInt8:
  case Kind::kInt16:
  case Kind::kUInt16:
  case Kind::kInt32:
  case Kind::kUInt32:
  case Kind::kInt64:
  case Kind::kUInt64:
  case Kind::kFloat16:
  case Kind::kFloat32:
  case Kind::kFloat64:
    return true;
  default:
    return false;
  }
}

/// Estimates the escaped bytes for one CSV cell without exceeding the caller's
/// cap.
[[nodiscard]] inline std::int64_t
estimate_csv_cell_bytes(const jsonl::JsonlField &field, const ArrowArray &array,
                        std::int64_t row, std::int64_t cap) noexcept {
  using Kind = jsonl::JsonlKind;
  if (cap <= 0 || array_is_null(array, row) || field.kind == Kind::kNull) {
    return 0;
  }
  if (direct_csv_scalar_kind(field.kind)) {
    // The CSV renderer writes these tokens directly; there are no object keys
    // and no second quoting pass.
    return estimate_value_bytes(field, array, row, cap);
  }
  if (field.kind == Kind::kString || field.kind == Kind::kLargeString) {
    const auto raw = field.kind == Kind::kString
                         ? variable_width_bytes<std::int32_t>(array, row, cap)
                         : variable_width_bytes<std::int64_t>(array, row, cap);
    // In the worst case every raw byte is a quote. CSV doubles each quote and
    // adds one opening and one closing quote; controls are not JSON-escaped.
    return add_capped(multiply_capped(raw, 2, cap), 2, cap);
  }
  // Nested, temporal, decimal, binary and dictionary cells retain the generic
  // JSON-token renderer. CSV may quote that token and double every quote.
  return multiply_capped(estimate_value_bytes(field, array, row, cap), 2, cap);
}

/// Estimates the encoded bytes for one CSV row without exceeding the caller's
/// cap.
[[nodiscard]] inline std::int64_t
estimate_csv_row_bytes(const jsonl::JsonlField &root, const ArrowArray &array,
                       std::int64_t row, std::int64_t cap) noexcept {
  if (cap <= 1 || root.kind != jsonl::JsonlKind::kStruct ||
      array.n_children != static_cast<std::int64_t>(root.children.size()) ||
      (!root.children.empty() && !array.children)) {
    return multiply_capped(estimate_jsonl_row_bytes(root, array, row, cap), 2,
                           cap);
  }

  std::int64_t total = 1; // trailing newline
  for (std::size_t index = 0; index < root.children.size(); ++index) {
    total = add_capped(total, index == 0 ? 0 : 1, cap); // delimiter
    if (total >= cap) {
      break;
    }
    total = add_capped(total,
                       estimate_csv_cell_bytes(root.children[index],
                                               *array.children[index],
                                               array.offset + row, cap - total),
                       cap);
  }
  // Keep one full estimate of slack so output packets remain short enough to
  // interleave with materialization on overlapping low-core arena lanes. The
  // underlying estimate is already a strict upper bound; this margin only
  // trades granularity for lower cross-stage occupancy.
  return multiply_capped(total, 2, cap);
}

} // namespace sanitize::internal::text_output_estimator
