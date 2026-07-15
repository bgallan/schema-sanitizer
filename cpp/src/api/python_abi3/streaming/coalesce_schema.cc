/* Arrow C stream coalescing schema support. */
#include "api/python_abi3/streaming/coalesce_stream_internal.hh"

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <string_view>
#include <type_traits>
#include <utility>

namespace core_abi3_internal::coalesce_detail {
namespace {

constexpr std::int64_t kMaxArrowBatchRows = std::int64_t{1} << 24;
constexpr std::int64_t kMaxArrowChildren = 65'536;
constexpr std::size_t kMaxArrowValidationDepth = 128;

sanitize::Status validate_logical_slice(const ArrowArray &array,
                                        std::int64_t offset,
                                        std::int64_t length,
                                        std::string_view context,
                                        std::int64_t max_logical_slots) {
  if (!array.release || array.length < 0 || array.offset < 0 || offset < 0 ||
      length < 0 || offset > array.length || length > array.length - offset) {
    return sanitize::Status::Invalid("coalescing stream ", context,
                                     " has an invalid Arrow array slice");
  }
  if (array.length > kMaxArrowBatchRows) {
    return sanitize::Status::Invalid("coalescing stream ", context,
                                     " exceeds the Arrow batch row limit");
  }
  if (array.null_count < -1 || array.null_count > array.length ||
      array.n_buffers < 0 || array.n_children < 0) {
    return sanitize::Status::Invalid("coalescing stream ", context,
                                     " has invalid Arrow metadata");
  }
  if (array.offset > std::numeric_limits<std::int64_t>::max() - offset ||
      array.offset + offset >
          std::numeric_limits<std::int64_t>::max() - length) {
    return sanitize::Status::Invalid("coalescing stream ", context,
                                     " has overflowing Arrow offsets");
  }
  if (array.offset + offset + length > max_logical_slots) {
    return sanitize::Status::OutOfMemory(
        "coalescing stream ", context,
        " absolute logical range exceeds slot limit");
  }
  if (array.null_count > 0 && (!array.buffers || !array.buffers[0])) {
    return sanitize::Status::Invalid("coalescing stream ", context,
                                     " has nulls without a validity buffer");
  }
  return sanitize::Status::OK();
}

template <class Offset>
sanitize::Result<std::pair<std::int64_t, std::int64_t>>
validate_offsets(const ArrowArray &array, std::int64_t offset,
                 std::int64_t length, std::string_view context,
                 std::int64_t max_logical_buffer_bytes) {
  if (!array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid("coalescing stream ", context,
                                     " has no offsets buffer");
  }
  const auto *offsets = static_cast<const Offset *>(array.buffers[1]);
  const auto logical_begin = array.offset + offset;
  const auto logical_end = logical_begin + length;
  Offset previous = offsets[static_cast<std::size_t>(logical_begin)];
  if (previous < 0) {
    return sanitize::Status::Invalid("coalescing stream ", context,
                                     " has a negative first offset");
  }
  for (auto index = logical_begin + 1; index <= logical_end; ++index) {
    const Offset current = offsets[static_cast<std::size_t>(index)];
    if (current < previous) {
      return sanitize::Status::Invalid("coalescing stream ", context,
                                       " offsets are not monotonic");
    }
    previous = current;
  }
  const auto first = offsets[static_cast<std::size_t>(logical_begin)];
  if constexpr (std::is_same_v<Offset, std::int32_t> ||
                std::is_same_v<Offset, std::int64_t>) {
    if (static_cast<std::int64_t>(previous) > max_logical_buffer_bytes) {
      return sanitize::Status::OutOfMemory(
          "coalescing stream ", context,
          " absolute buffer endpoint exceeds logical byte limit");
    }
  }
  return std::pair<std::int64_t, std::int64_t>{
      static_cast<std::int64_t>(first), static_cast<std::int64_t>(previous)};
}

template <class Index>
sanitize::Status
validate_dictionary_indices_typed(const ArrowArray &array, std::int64_t offset,
                                  std::int64_t length,
                                  std::int64_t dictionary_length) {
  if (!array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid(
        "coalescing stream dictionary has no indices buffer");
  }
  const auto *indices = static_cast<const Index *>(array.buffers[1]);
  const auto begin = array.offset + offset;
  for (std::int64_t row = 0; row < length; ++row) {
    if (array.null_count != 0 && array.buffers[0]) {
      const auto *validity =
          static_cast<const std::uint8_t *>(array.buffers[0]);
      const auto bit = begin + row;
      if ((validity[static_cast<std::size_t>(bit >> 3)] &
           (static_cast<std::uint8_t>(1U) << (bit & 7))) == 0) {
        continue;
      }
    }
    const auto value = indices[static_cast<std::size_t>(begin + row)];
    if constexpr (std::is_signed_v<Index>) {
      if (value < 0) {
        return sanitize::Status::Invalid(
            "coalescing stream dictionary index is negative");
      }
    }
    const auto unsigned_value = static_cast<std::uint64_t>(value);
    if (unsigned_value >= static_cast<std::uint64_t>(dictionary_length)) {
      return sanitize::Status::Invalid(
          "coalescing stream dictionary index is out of range");
    }
  }
  return sanitize::Status::OK();
}

sanitize::Status
validate_arrow_node_impl(const CoalesceNodeSpec &spec, const ArrowArray &array,
                         std::int64_t offset, std::int64_t length,
                         std::size_t depth, std::int64_t max_logical_slots,
                         std::int64_t max_logical_buffer_bytes) {
  if (depth > kMaxArrowValidationDepth) {
    return sanitize::Status::Invalid(
        "coalescing stream Arrow nesting depth exceeds security limit");
  }
  SAN_RETURN_NOT_OK(
      validate_logical_slice(array, offset, length, "node", max_logical_slots));
  std::int64_t expected_buffers = 0;
  switch (spec.kind) {
  case CoalesceKind::kStruct:
    expected_buffers = 1;
    break;
  case CoalesceKind::kList32:
  case CoalesceKind::kList64:
    expected_buffers = 2;
    break;
  case CoalesceKind::kUtf8:
  case CoalesceKind::kLargeUtf8:
  case CoalesceKind::kBinary:
  case CoalesceKind::kLargeBinary:
    expected_buffers = 3;
    break;
  case CoalesceKind::kFixedWidth:
  case CoalesceKind::kBool:
  case CoalesceKind::kDictionary:
    expected_buffers = 2;
    break;
  }
  if (array.n_buffers != expected_buffers ||
      (expected_buffers > 0 && !array.buffers)) {
    return sanitize::Status::Invalid(
        "coalescing stream Arrow buffer shape changed unexpectedly");
  }
  const auto expected_children =
      spec.kind == CoalesceKind::kDictionary
          ? std::int64_t{0}
          : static_cast<std::int64_t>(spec.children.size());
  if (array.n_children != expected_children ||
      (expected_children > 0 && !array.children)) {
    return sanitize::Status::Invalid(
        "coalescing stream Arrow child shape changed unexpectedly");
  }
  if (length > 0 && spec.kind != CoalesceKind::kStruct &&
      spec.kind != CoalesceKind::kList32 &&
      spec.kind != CoalesceKind::kList64 &&
      (!array.buffers || !array.buffers[1])) {
    return sanitize::Status::Invalid(
        "coalescing stream Arrow values buffer is missing");
  }
  switch (spec.kind) {
  case CoalesceKind::kUtf8:
  case CoalesceKind::kBinary: {
    SAN_ASSIGN_OR_RAISE(auto bounds, validate_offsets<std::int32_t>(
                                         array, offset, length, "binary node",
                                         max_logical_buffer_bytes));
    if (bounds.second > bounds.first && !array.buffers[2]) {
      return sanitize::Status::Invalid(
          "coalescing stream binary data buffer is missing");
    }
    break;
  }
  case CoalesceKind::kLargeUtf8:
  case CoalesceKind::kLargeBinary: {
    SAN_ASSIGN_OR_RAISE(
        auto bounds, validate_offsets<std::int64_t>(array, offset, length,
                                                    "large binary node",
                                                    max_logical_buffer_bytes));
    if (bounds.second > bounds.first && !array.buffers[2]) {
      return sanitize::Status::Invalid(
          "coalescing stream large binary data buffer is missing");
    }
    break;
  }
  case CoalesceKind::kList32:
  case CoalesceKind::kList64: {
    if (spec.children.size() != 1 || !array.children || !array.children[0]) {
      return sanitize::Status::Invalid(
          "coalescing stream list child is missing");
    }
    sanitize::Result<std::pair<std::int64_t, std::int64_t>> bounds =
        spec.kind == CoalesceKind::kList32
            ? validate_offsets<std::int32_t>(array, offset, length, "list node",
                                             max_logical_buffer_bytes)
            : validate_offsets<std::int64_t>(array, offset, length,
                                             "large list node",
                                             max_logical_buffer_bytes);
    if (!bounds.ok()) {
      return bounds.status();
    }
    const auto [begin, end] = std::move(bounds).ValueOrDie();
    if (begin < 0 || end < begin || begin > array.children[0]->length ||
        end > array.children[0]->length) {
      return sanitize::Status::Invalid(
          "coalescing stream list offsets exceed child length");
    }
    SAN_RETURN_NOT_OK(validate_arrow_node_impl(
        spec.children[0], *array.children[0], begin, end - begin, depth + 1,
        max_logical_slots, max_logical_buffer_bytes));
    break;
  }
  case CoalesceKind::kStruct:
    for (std::size_t index = 0; index < spec.children.size(); ++index) {
      if (!array.children[index]) {
        return sanitize::Status::Invalid(
            "coalescing stream struct child is missing");
      }
      SAN_RETURN_NOT_OK(validate_arrow_node_impl(
          spec.children[index], *array.children[index], array.offset + offset,
          length, depth + 1, max_logical_slots, max_logical_buffer_bytes));
    }
    break;
  case CoalesceKind::kDictionary: {
    if (spec.children.size() != 1 || !array.dictionary ||
        !array.dictionary->release || array.dictionary->length < 0) {
      return sanitize::Status::Invalid(
          "coalescing stream dictionary values are missing");
    }
    SAN_RETURN_NOT_OK(validate_arrow_node_impl(
        spec.children[0], *array.dictionary, 0, array.dictionary->length,
        depth + 1, max_logical_slots, max_logical_buffer_bytes));
    if (spec.format == "c") {
      return validate_dictionary_indices_typed<std::int8_t>(
          array, offset, length, array.dictionary->length);
    }
    if (spec.format == "C") {
      return validate_dictionary_indices_typed<std::uint8_t>(
          array, offset, length, array.dictionary->length);
    }
    if (spec.format == "s") {
      return validate_dictionary_indices_typed<std::int16_t>(
          array, offset, length, array.dictionary->length);
    }
    if (spec.format == "S") {
      return validate_dictionary_indices_typed<std::uint16_t>(
          array, offset, length, array.dictionary->length);
    }
    if (spec.format == "i") {
      return validate_dictionary_indices_typed<std::int32_t>(
          array, offset, length, array.dictionary->length);
    }
    if (spec.format == "I") {
      return validate_dictionary_indices_typed<std::uint32_t>(
          array, offset, length, array.dictionary->length);
    }
    if (spec.format == "l") {
      return validate_dictionary_indices_typed<std::int64_t>(
          array, offset, length, array.dictionary->length);
    }
    if (spec.format == "L") {
      return validate_dictionary_indices_typed<std::uint64_t>(
          array, offset, length, array.dictionary->length);
    }
    return sanitize::Status::Invalid(
        "coalescing stream dictionary index format is unsupported");
  }
  case CoalesceKind::kFixedWidth:
  case CoalesceKind::kBool:
    break;
  }
  return sanitize::Status::OK();
}

constexpr std::array<std::string_view, 2> kInteger8Formats{"c", "C"};
constexpr std::array<std::string_view, 2> kInteger16Formats{"s", "S"};
constexpr std::array<std::string_view, 2> kInteger32Formats{"i", "I"};
constexpr std::array<std::string_view, 2> kInteger64Formats{"l", "L"};

std::size_t integer_width_for_format(std::string_view format) noexcept {
  if (std::find(kInteger8Formats.cbegin(), kInteger8Formats.cend(), format) !=
      kInteger8Formats.cend()) {
    return 1;
  }
  if (std::find(kInteger16Formats.cbegin(), kInteger16Formats.cend(), format) !=
      kInteger16Formats.cend()) {
    return 2;
  }
  if (std::find(kInteger32Formats.cbegin(), kInteger32Formats.cend(), format) !=
      kInteger32Formats.cend()) {
    return 4;
  }
  if (std::find(kInteger64Formats.cbegin(), kInteger64Formats.cend(), format) !=
      kInteger64Formats.cend()) {
    return 8;
  }
  return 0;
}

std::size_t fixed_width_for_format(std::string_view format) noexcept {
  if (const auto integer_width = integer_width_for_format(format);
      integer_width != 0) {
    return integer_width;
  }
  if (format == "e") {
    return 2;
  }
  if (format == "f" || format == "tdD" || format == "tti" || format == "tiM") {
    return 4;
  }
  if (format == "g" || format == "tdm" || format == "tts" || format == "ttm" ||
      format == "ttu" || format == "ttn" || format == "tDs" ||
      format == "tDm" || format == "tDu" || format == "tDn" ||
      format == "tiD" || format.starts_with("ts")) {
    return 8;
  }
  return format == "tin" ? 16 : 0;
}

std::size_t
dictionary_index_width_for_format(std::string_view format) noexcept {
  return integer_width_for_format(format);
}

bool parse_supported_schema_node(const ArrowSchema &schema,
                                 CoalesceNodeSpec *out, std::size_t depth = 0) {
  if (!out || depth > kMaxArrowValidationDepth) {
    return false;
  }
  if (!schema.format) {
    return false;
  }
  const std::string_view format(schema.format);
  out->format = schema.format;
  out->fixed_width = 0;
  out->children.clear();
  if (schema.dictionary != nullptr) {
    if (schema.n_children != 0) {
      return false;
    }
    const std::size_t width = dictionary_index_width_for_format(format);
    if (width == 0) {
      return false;
    }
    out->kind = CoalesceKind::kDictionary;
    out->fixed_width = width;
    out->children.resize(1);
    if (!parse_supported_schema_node(*schema.dictionary, &out->children[0],
                                     depth + 1)) {
      out->children.clear();
      return false;
    }
    return true;
  }
  if (format == "+s") {
    if (schema.n_children < 0 || schema.n_children > kMaxArrowChildren) {
      return false;
    }
    out->kind = CoalesceKind::kStruct;
    out->children.resize(static_cast<std::size_t>(schema.n_children));
    for (std::int64_t i = 0; i < schema.n_children; ++i) {
      const ArrowSchema *child = schema.children ? schema.children[i] : nullptr;
      if (!child ||
          !parse_supported_schema_node(*child, &out->children[i], depth + 1)) {
        out->children.clear();
        return false;
      }
    }
    return true;
  }
  if (format == "+l" || format == "+L") {
    if (schema.n_children != 1 || !schema.children || !schema.children[0]) {
      return false;
    }
    out->kind = format == "+l" ? CoalesceKind::kList32 : CoalesceKind::kList64;
    out->children.resize(1);
    if (!parse_supported_schema_node(*schema.children[0], &out->children[0],
                                     depth + 1)) {
      out->children.clear();
      return false;
    }
    return true;
  }
  if (schema.n_children != 0) {
    return false;
  }
  if (format == "u") {
    out->kind = CoalesceKind::kUtf8;
    return true;
  }
  if (format == "U") {
    out->kind = CoalesceKind::kLargeUtf8;
    return true;
  }
  if (format == "z") {
    out->kind = CoalesceKind::kBinary;
    return true;
  }
  if (format == "Z") {
    out->kind = CoalesceKind::kLargeBinary;
    return true;
  }
  if (format == "b") {
    out->kind = CoalesceKind::kBool;
    return true;
  }
  const std::size_t width = fixed_width_for_format(format);
  if (width == 0) {
    return false;
  }
  out->kind = CoalesceKind::kFixedWidth;
  out->fixed_width = width;
  return true;
}

} // namespace

sanitize::Status validate_arrow_node(const CoalesceNodeSpec &spec,
                                     const ArrowArray &array,
                                     std::int64_t offset, std::int64_t length,
                                     std::size_t depth,
                                     std::int64_t max_logical_slots,
                                     std::int64_t max_logical_buffer_bytes) {
  return validate_arrow_node_impl(spec, array, offset, length, depth,
                                  max_logical_slots, max_logical_buffer_bytes);
}

bool schema_supported(const ArrowSchema &schema, CoalesceNodeSpec *root) {
  if (!parse_supported_schema_node(schema, root)) {
    return false;
  }
  return root->kind == CoalesceKind::kStruct && !root->children.empty();
}

} // namespace core_abi3_internal::coalesce_detail
