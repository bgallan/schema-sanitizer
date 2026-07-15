// Validates Arrow C arrays before the JSONL writer dereferences their buffers.

#include "internal/json_output/schema/model.hh"
#include "internal/memory/memory_budget.hh"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <type_traits>
#include <utility>

namespace sanitize::internal::jsonl_stream_writer {
namespace {

constexpr std::int64_t kMaxArrowBatchRows = std::int64_t{1} << 24;
constexpr std::size_t kMaxArrowValidationDepth = 64;

sanitize::Status checked_logical_end(const ArrowArray &array,
                                     std::int64_t offset, std::int64_t length,
                                     std::int64_t *absolute_begin,
                                     std::int64_t *absolute_end,
                                     const ArrayValidationLimits &limits) {
  if (!absolute_begin || !absolute_end || array.length < 0 ||
      array.offset < 0 || offset < 0 || length < 0 || offset > array.length ||
      length > array.length - offset) {
    return sanitize::Status::Invalid(
        "JSONL writer: invalid Arrow array slice metadata");
  }
  if (array.length > kMaxArrowBatchRows) {
    return sanitize::Status::OutOfMemory(
        "JSONL writer: Arrow batch row count exceeds safety limit");
  }
  if (array.offset > std::numeric_limits<std::int64_t>::max() - offset) {
    return sanitize::Status::Invalid(
        "JSONL writer: Arrow array offset overflow");
  }
  *absolute_begin = array.offset + offset;
  if (*absolute_begin > std::numeric_limits<std::int64_t>::max() - length) {
    return sanitize::Status::Invalid(
        "JSONL writer: Arrow array logical range overflow");
  }
  *absolute_end = *absolute_begin + length;
  if (*absolute_end > limits.logical_slots) {
    return sanitize::Status::OutOfMemory(
        "JSONL writer: absolute logical range exceeds slot limit");
  }
  return sanitize::Status::OK();
}

sanitize::Status validate_byte_endpoint(std::int64_t slots, std::size_t width,
                                        std::string_view context,
                                        const ArrayValidationLimits &limits) {
  if (slots < 0 || width == 0) {
    return sanitize::Status::Invalid("JSONL writer: invalid ", context,
                                     " byte width");
  }
  const auto limit = limits.logical_buffer_bytes;
  if (static_cast<std::uint64_t>(slots) >
      static_cast<std::uint64_t>(limit) / width) {
    return sanitize::Status::OutOfMemory(
        "JSONL writer: ", context,
        " absolute buffer endpoint exceeds logical byte limit");
  }
  return sanitize::Status::OK();
}

std::size_t fixed_width_bytes(const JsonlField &field) noexcept {
  switch (field.kind) {
  case JsonlKind::kInt8:
  case JsonlKind::kUInt8:
    return 1;
  case JsonlKind::kInt16:
  case JsonlKind::kUInt16:
  case JsonlKind::kFloat16:
    return 2;
  case JsonlKind::kInt32:
  case JsonlKind::kUInt32:
  case JsonlKind::kFloat32:
  case JsonlKind::kDate32:
  case JsonlKind::kTime32s:
  case JsonlKind::kTime32ms:
    return 4;
  case JsonlKind::kInt64:
  case JsonlKind::kUInt64:
  case JsonlKind::kFloat64:
  case JsonlKind::kTimestampMillis:
  case JsonlKind::kTimestampMicros:
  case JsonlKind::kTimestampNanos:
  case JsonlKind::kDate64:
  case JsonlKind::kTime64us:
  case JsonlKind::kTime64ns:
  case JsonlKind::kDuration:
    return 8;
  case JsonlKind::kDecimal:
    return field.decimal_byte_width > 0
               ? static_cast<std::size_t>(field.decimal_byte_width)
               : 0;
  case JsonlKind::kFixedSizeBinary:
    return field.fixed_size_binary_size > 0
               ? static_cast<std::size_t>(field.fixed_size_binary_size)
               : 0;
  case JsonlKind::kInterval:
    if (field.format == "tiM") {
      return 4;
    }
    if (field.format == "tiD") {
      return 8;
    }
    return field.format == "tin" ? 16 : 0;
  default:
    return 0;
  }
}

std::size_t dictionary_index_width(JsonlKind kind) noexcept {
  JsonlField index_field;
  index_field.kind = kind;
  return fixed_width_bytes(index_field);
}

std::int64_t expected_buffers(const JsonlField &field) noexcept {
  switch (field.kind) {
  case JsonlKind::kStruct:
  case JsonlKind::kFixedSizeList:
  case JsonlKind::kNull:
    return 1;
  case JsonlKind::kList:
  case JsonlKind::kLargeList:
  case JsonlKind::kMap:
    return 2;
  case JsonlKind::kString:
  case JsonlKind::kLargeString:
  case JsonlKind::kBinary:
  case JsonlKind::kLargeBinary:
    return 3;
  default:
    return 2;
  }
}

sanitize::Status validate_common(const JsonlField &field,
                                 const ArrowArray &array, std::int64_t offset,
                                 std::int64_t length,
                                 std::int64_t *absolute_begin,
                                 std::int64_t *absolute_end,
                                 const ArrayValidationLimits &limits) {
  SAN_RETURN_NOT_OK(checked_logical_end(array, offset, length, absolute_begin,
                                        absolute_end, limits));
  if (array.null_count < -1 || array.null_count > array.length ||
      array.n_buffers < 0 || array.n_children < 0) {
    return sanitize::Status::Invalid(
        "JSONL writer: invalid Arrow array metadata");
  }
  const auto buffers = expected_buffers(field);
  if (array.n_buffers != buffers || (buffers > 0 && !array.buffers)) {
    return sanitize::Status::Invalid(
        "JSONL writer: Arrow buffer shape does not match schema for field '",
        field.name, "' (expected ", buffers, ", got ", array.n_buffers, ")");
  }
  if (array.null_count > 0 && !array.buffers[0]) {
    return sanitize::Status::Invalid(
        "JSONL writer: null values require a validity bitmap");
  }
  if (array.buffers[0] && *absolute_end > 0) {
    const auto bitmap_bytes =
        static_cast<std::uint64_t>(*absolute_end) / 8U + 1U;
    const auto limit = limits.logical_buffer_bytes;
    if (bitmap_bytes > static_cast<std::uint64_t>(limit)) {
      return sanitize::Status::OutOfMemory(
          "JSONL writer: validity bitmap exceeds logical byte limit");
    }
  }
  return sanitize::Status::OK();
}

template <class Offset>
sanitize::Result<std::pair<std::int64_t, std::int64_t>>
validate_offsets(const ArrowArray &array, std::int64_t absolute_begin,
                 std::int64_t absolute_end, std::string_view context,
                 const ArrayValidationLimits &limits) {
  if (absolute_begin == absolute_end) {
    return std::pair<std::int64_t, std::int64_t>{0, 0};
  }
  if (!array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid("JSONL writer: missing ", context,
                                     " offsets buffer");
  }
  if (absolute_end == std::numeric_limits<std::int64_t>::max()) {
    return sanitize::Status::Invalid("JSONL writer: ", context,
                                     " offset endpoint overflow");
  }
  SAN_RETURN_NOT_OK(validate_byte_endpoint(absolute_end + 1, sizeof(Offset),
                                           "offsets", limits));
  const auto *offsets = static_cast<const Offset *>(array.buffers[1]);
  Offset previous = offsets[static_cast<std::size_t>(absolute_begin)];
  if constexpr (std::is_signed_v<Offset>) {
    if (previous < 0) {
      return sanitize::Status::Invalid("JSONL writer: negative ", context,
                                       " offset");
    }
  }
  for (auto index = absolute_begin + 1; index <= absolute_end; ++index) {
    const Offset current = offsets[static_cast<std::size_t>(index)];
    if (current < previous) {
      return sanitize::Status::Invalid("JSONL writer: non-monotonic ", context,
                                       " offsets");
    }
    previous = current;
  }
  const auto first = offsets[static_cast<std::size_t>(absolute_begin)];
  if constexpr (std::is_unsigned_v<Offset>) {
    if (static_cast<std::uint64_t>(previous) >
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
      return sanitize::Status::Invalid("JSONL writer: ", context,
                                       " offset exceeds int64 range");
    }
  }
  return std::pair<std::int64_t, std::int64_t>{
      static_cast<std::int64_t>(first), static_cast<std::int64_t>(previous)};
}

template <class Index>
sanitize::Status validate_dictionary_indices(const ArrowArray &array,
                                             std::int64_t absolute_begin,
                                             std::int64_t length,
                                             std::int64_t dictionary_length) {
  if (length == 0) {
    return sanitize::Status::OK();
  }
  if (!array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid(
        "JSONL writer: dictionary indices buffer is missing");
  }
  const auto *indices = static_cast<const Index *>(array.buffers[1]);
  for (std::int64_t row = 0; row < length; ++row) {
    const auto bit = absolute_begin + row;
    if (array.null_count != 0 && array.buffers[0]) {
      const auto *validity =
          static_cast<const std::uint8_t *>(array.buffers[0]);
      if ((validity[static_cast<std::size_t>(bit >> 3)] &
           (static_cast<std::uint8_t>(1U) << (bit & 7))) == 0) {
        continue;
      }
    }
    const auto value = indices[static_cast<std::size_t>(bit)];
    if constexpr (std::is_signed_v<Index>) {
      if (value < 0) {
        return sanitize::Status::Invalid(
            "JSONL writer: dictionary index is negative");
      }
    }
    if (static_cast<std::uint64_t>(value) >=
        static_cast<std::uint64_t>(dictionary_length)) {
      return sanitize::Status::Invalid(
          "JSONL writer: dictionary index is out of range");
    }
  }
  return sanitize::Status::OK();
}

sanitize::Status
validate_array_slice_impl(const JsonlField &field, const ArrowArray &array,
                          std::int64_t offset, std::int64_t length,
                          std::size_t depth,
                          const ArrayValidationLimits &limits) {
  if (depth > kMaxArrowValidationDepth) {
    return sanitize::Status::OutOfMemory(
        "JSONL writer: Arrow nesting depth exceeds safety limit");
  }
  std::int64_t absolute_begin = 0;
  std::int64_t absolute_end = 0;
  SAN_RETURN_NOT_OK(validate_common(field, array, offset, length,
                                    &absolute_begin, &absolute_end, limits));

  const auto expected_children =
      field.kind == JsonlKind::kStruct
          ? static_cast<std::int64_t>(field.children.size())
          : ((field.kind == JsonlKind::kList ||
              field.kind == JsonlKind::kLargeList ||
              field.kind == JsonlKind::kFixedSizeList ||
              field.kind == JsonlKind::kMap)
                 ? std::int64_t{1}
                 : std::int64_t{0});
  if (array.n_children != expected_children ||
      (expected_children > 0 && !array.children)) {
    return sanitize::Status::Invalid(
        "JSONL writer: Arrow child shape does not match schema");
  }

  if (field.kind == JsonlKind::kDictionary) {
    if (field.children.size() != 1 || !array.dictionary ||
        array.dictionary->length < 0) {
      return sanitize::Status::Invalid(
          "JSONL writer: dictionary values are missing");
    }
    const auto width = dictionary_index_width(field.dictionary_index_kind);
    if (width == 0) {
      return sanitize::Status::Invalid(
          "JSONL writer: unsupported dictionary index width");
    }
    SAN_RETURN_NOT_OK(validate_byte_endpoint(absolute_end, width,
                                             "dictionary indices", limits));
    SAN_RETURN_NOT_OK(
        validate_array_slice_impl(field.children[0], *array.dictionary, 0,
                                  array.dictionary->length, depth + 1, limits));
    switch (field.dictionary_index_kind) {
    case JsonlKind::kInt8:
      return validate_dictionary_indices<std::int8_t>(
          array, absolute_begin, length, array.dictionary->length);
    case JsonlKind::kUInt8:
      return validate_dictionary_indices<std::uint8_t>(
          array, absolute_begin, length, array.dictionary->length);
    case JsonlKind::kInt16:
      return validate_dictionary_indices<std::int16_t>(
          array, absolute_begin, length, array.dictionary->length);
    case JsonlKind::kUInt16:
      return validate_dictionary_indices<std::uint16_t>(
          array, absolute_begin, length, array.dictionary->length);
    case JsonlKind::kInt32:
      return validate_dictionary_indices<std::int32_t>(
          array, absolute_begin, length, array.dictionary->length);
    case JsonlKind::kUInt32:
      return validate_dictionary_indices<std::uint32_t>(
          array, absolute_begin, length, array.dictionary->length);
    case JsonlKind::kInt64:
      return validate_dictionary_indices<std::int64_t>(
          array, absolute_begin, length, array.dictionary->length);
    case JsonlKind::kUInt64:
      return validate_dictionary_indices<std::uint64_t>(
          array, absolute_begin, length, array.dictionary->length);
    default:
      return sanitize::Status::Invalid(
          "JSONL writer: unsupported dictionary index kind");
    }
  }

  if (field.kind == JsonlKind::kStruct) {
    for (std::size_t index = 0; index < field.children.size(); ++index) {
      if (!array.children[index]) {
        return sanitize::Status::Invalid(
            "JSONL writer: struct child is missing");
      }
      SAN_RETURN_NOT_OK(validate_array_slice_impl(
          field.children[index], *array.children[index], absolute_begin, length,
          depth + 1, limits));
    }
    return sanitize::Status::OK();
  }

  if (field.kind == JsonlKind::kList || field.kind == JsonlKind::kMap ||
      field.kind == JsonlKind::kLargeList) {
    if (field.children.size() != 1 || !array.children[0]) {
      return sanitize::Status::Invalid("JSONL writer: list child is missing");
    }
    sanitize::Result<std::pair<std::int64_t, std::int64_t>> bounds =
        field.kind == JsonlKind::kLargeList
            ? validate_offsets<std::int64_t>(array, absolute_begin,
                                             absolute_end, "list", limits)
            : validate_offsets<std::int32_t>(array, absolute_begin,
                                             absolute_end, "list", limits);
    if (!bounds.ok()) {
      return bounds.status();
    }
    const auto [begin, end] = std::move(bounds).ValueOrDie();
    if (begin < 0 || end < begin || begin > array.children[0]->length ||
        end > array.children[0]->length) {
      return sanitize::Status::Invalid(
          "JSONL writer: list offsets exceed child length");
    }
    return validate_array_slice_impl(field.children[0], *array.children[0],
                                     begin, end - begin, depth + 1, limits);
  }

  if (field.kind == JsonlKind::kFixedSizeList) {
    if (field.children.size() != 1 || !array.children[0] ||
        field.fixed_size_list_size < 0) {
      return sanitize::Status::Invalid(
          "JSONL writer: fixed-size list/schema mismatch");
    }
    const auto width = static_cast<std::int64_t>(field.fixed_size_list_size);
    if (width > 0 &&
        absolute_begin > std::numeric_limits<std::int64_t>::max() / width) {
      return sanitize::Status::Invalid(
          "JSONL writer: fixed-size list child offset overflow");
    }
    const auto child_begin = absolute_begin * width;
    if (width > 0 &&
        length > std::numeric_limits<std::int64_t>::max() / width) {
      return sanitize::Status::Invalid(
          "JSONL writer: fixed-size list child length overflow");
    }
    const auto child_length = length * width;
    if (child_begin > array.children[0]->length ||
        child_length > array.children[0]->length - child_begin) {
      return sanitize::Status::Invalid(
          "JSONL writer: fixed-size list exceeds child length");
    }
    return validate_array_slice_impl(field.children[0], *array.children[0],
                                     child_begin, child_length, depth + 1,
                                     limits);
  }

  if (field.kind == JsonlKind::kString || field.kind == JsonlKind::kBinary ||
      field.kind == JsonlKind::kLargeString ||
      field.kind == JsonlKind::kLargeBinary) {
    sanitize::Result<std::pair<std::int64_t, std::int64_t>> bounds =
        (field.kind == JsonlKind::kLargeString ||
         field.kind == JsonlKind::kLargeBinary)
            ? validate_offsets<std::int64_t>(array, absolute_begin,
                                             absolute_end, "binary", limits)
            : validate_offsets<std::int32_t>(array, absolute_begin,
                                             absolute_end, "binary", limits);
    if (!bounds.ok()) {
      return bounds.status();
    }
    const auto [begin, end] = std::move(bounds).ValueOrDie();
    if (begin < 0 || end < begin) {
      return sanitize::Status::Invalid("JSONL writer: invalid binary offsets");
    }
    SAN_RETURN_NOT_OK(validate_byte_endpoint(end, 1, "binary data", limits));
    if (end > begin && !array.buffers[2]) {
      return sanitize::Status::Invalid(
          "JSONL writer: binary data buffer is missing");
    }
    return sanitize::Status::OK();
  }

  if (field.kind == JsonlKind::kBool) {
    if (length > 0 && !array.buffers[1]) {
      return sanitize::Status::Invalid(
          "JSONL writer: bool values buffer is missing");
    }
    const auto bitmap_bytes =
        static_cast<std::uint64_t>(absolute_end) / 8U + 1U;
    const auto limit = limits.logical_buffer_bytes;
    return bitmap_bytes <= static_cast<std::uint64_t>(limit)
               ? sanitize::Status::OK()
               : sanitize::Status::OutOfMemory(
                     "JSONL writer: bool bitmap exceeds logical byte limit");
  }

  if (field.kind == JsonlKind::kNull) {
    return sanitize::Status::OK();
  }

  const auto width = fixed_width_bytes(field);
  if (width == 0) {
    return sanitize::Status::Invalid(
        "JSONL writer: unsupported fixed-width field metadata");
  }
  if (length > 0 && !array.buffers[1]) {
    return sanitize::Status::Invalid("JSONL writer: values buffer is missing");
  }
  return validate_byte_endpoint(absolute_end, width, "fixed-width values",
                                limits);
}

} // namespace

ArrayValidationLimits array_validation_limits(std::int64_t memory_limit_bytes) {
  const auto budget =
      sanitize::internal::memory_budget_from_limit(memory_limit_bytes);
  return {.logical_slots = budget.arrow_logical_slots,
          .logical_buffer_bytes = budget.arrow_logical_buffer_bytes};
}

sanitize::Status validate_array_slice(const JsonlField &field,
                                      const ArrowArray &array,
                                      std::int64_t offset, std::int64_t length,
                                      const ArrayValidationLimits &limits) {
  return validate_array_slice_impl(field, array, offset, length, 0, limits);
}

} // namespace sanitize::internal::jsonl_stream_writer
