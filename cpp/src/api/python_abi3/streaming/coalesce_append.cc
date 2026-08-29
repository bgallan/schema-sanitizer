/*
 * Implements the Arrow C stream coalescing append phase.
 *
 * The phases validate schemas, append slices, and export one owned Arrow array
 * under budget.
 */

#include "api/python_abi3/streaming/coalesce_stream_internal.hh"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string_view>
#include <vector>

namespace core_abi3_internal::coalesce_detail {
namespace {

/// Reads one bit from an Arrow bitmap using least-significant-bit ordering.
bool bit_is_set(const std::uint8_t *bitmap, std::int64_t index) noexcept {
  return (bitmap[static_cast<std::size_t>(index >> 3)] &
          (static_cast<std::uint8_t>(1U) << (index & 7))) != 0;
}

/// Marks one Arrow bitmap slot as present without disturbing adjacent values.
void set_bit(std::vector<std::uint8_t> *bitmap, std::int64_t index) {
  (*bitmap)[static_cast<std::size_t>(index >> 3)] |=
      static_cast<std::uint8_t>(1U) << (index & 7);
}

/// Marks one Arrow bitmap slot as null without disturbing adjacent values.
void clear_bit(std::vector<std::uint8_t> *bitmap, std::int64_t index) {
  (*bitmap)[static_cast<std::size_t>(index >> 3)] &= static_cast<std::uint8_t>(
      ~(static_cast<std::uint8_t>(1U) << (index & 7)));
}

/// Validates one live array slice before appending it to coalesced output.
sanitize::Status validate_slice(const ArraySlice &slice,
                                std::string_view context) {
  if (!slice.array || !slice.array->release) {
    return sanitize::Status::Invalid("coalescing stream ", context,
                                     " has no live array");
  }
  if (slice.offset < 0 || slice.length < 0 || slice.array->length < 0 ||
      slice.offset > slice.array->length ||
      slice.length > slice.array->length - slice.offset) {
    return sanitize::Status::Invalid("coalescing stream ", context,
                                     " has invalid array slice");
  }
  return sanitize::Status::OK();
}

/// Reads a row's validity after applying the Arrow array and slice offsets.
bool row_is_valid(const ArrowArray &array, std::int64_t logical_row) noexcept {
  if (array.null_count == 0 || !array.buffers || !array.buffers[0]) {
    return true;
  }
  auto *validity = static_cast<const std::uint8_t *>(array.buffers[0]);
  return bit_is_set(validity, array.offset + logical_row);
}

/// Appends validity bits while maintaining the accumulated null count.
sanitize::Status append_validity(CoalescedNode *out, const ArraySlice &slice) {
  SAN_RETURN_NOT_OK(validate_slice(slice, "validity"));
  const std::int64_t start = out->array.length;
  const std::int64_t total = start + slice.length;
  if (total < start) {
    return sanitize::Status::Invalid("coalescing stream length overflow");
  }
  bool has_null = false;
  if (slice.array->null_count != 0 && slice.array->buffers &&
      slice.array->buffers[0]) {
    for (std::int64_t row = 0; row < slice.length; ++row) {
      if (!row_is_valid(*slice.array, slice.offset + row)) {
        has_null = true;
        break;
      }
    }
  }
  if (!out->validity.empty() || has_null) {
    const auto bitmap_bytes = static_cast<std::size_t>((total + 7) / 8);
    if (out->validity.empty()) {
      out->validity.resize(bitmap_bytes, 0xFFU);
    } else {
      out->validity.resize(bitmap_bytes, 0);
    }
    for (std::int64_t row = 0; row < slice.length; ++row) {
      if (row_is_valid(*slice.array, slice.offset + row)) {
        set_bit(&out->validity, start + row);
      } else {
        clear_bit(&out->validity, start + row);
        ++out->array.null_count;
      }
    }
  }
  out->array.length = total;
  return sanitize::Status::OK();
}

/// Requires the batch child count to match the coalesced schema node.
sanitize::Status ensure_child_count(const CoalesceNodeSpec &spec,
                                    const ArrowArray &array) {
  const std::int64_t expected = static_cast<std::int64_t>(spec.children.size());
  if (array.n_children != expected) {
    return sanitize::Status::Invalid(
        "coalescing stream batch shape changed unexpectedly");
  }
  if (expected > 0 && !array.children) {
    return sanitize::Status::Invalid(
        "coalescing stream array has missing children");
  }
  return sanitize::Status::OK();
}

/// Initializes coalesced child nodes and verifies their shape against the
/// schema.
sanitize::Status ensure_output_children(CoalescedNode *out,
                                        const CoalesceNodeSpec &spec) {
  if (out->children.empty() && !spec.children.empty()) {
    out->children.resize(spec.children.size());
  }
  if (out->children.size() != spec.children.size()) {
    return sanitize::Status::Invalid(
        "coalescing stream output child shape changed");
  }
  return sanitize::Status::OK();
}

/// Appends fixed-width values after extending the shared validity bitmap.
sanitize::Status append_fixed_width(CoalescedNode *out, const ArraySlice &slice,
                                    std::size_t width) {
  SAN_RETURN_NOT_OK(append_validity(out, slice));
  if (slice.length == 0) {
    return sanitize::Status::OK();
  }
  if (!slice.array->buffers || !slice.array->buffers[1]) {
    return sanitize::Status::Invalid(
        "coalescing stream fixed-width column has no values buffer");
  }
  const std::uint64_t bytes = static_cast<std::uint64_t>(slice.length) *
                              static_cast<std::uint64_t>(width);
  if (bytes >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid(
        "coalescing stream values buffer too large");
  }
  auto *values = static_cast<const std::uint8_t *>(slice.array->buffers[1]);
  values +=
      static_cast<std::size_t>(slice.array->offset + slice.offset) * width;
  const std::size_t old_size = out->data.size();
  if (static_cast<std::size_t>(bytes) >
      std::numeric_limits<std::size_t>::max() - old_size) {
    return sanitize::Status::Invalid(
        "coalescing stream fixed-width output size overflows");
  }
  out->data.resize(old_size + static_cast<std::size_t>(bytes));
  std::memcpy(out->data.data() + old_size, values,
              static_cast<std::size_t>(bytes));
  return sanitize::Status::OK();
}

/// Appends Boolean values into the coalesced bit-packed buffer.
sanitize::Status append_bool(CoalescedNode *out, const ArraySlice &slice) {
  SAN_RETURN_NOT_OK(append_validity(out, slice));
  const std::int64_t total = out->array.length;
  out->data.resize(static_cast<std::size_t>((total + 7) / 8), 0);
  if (slice.length == 0) {
    return sanitize::Status::OK();
  }
  if (!slice.array->buffers || !slice.array->buffers[1]) {
    return sanitize::Status::Invalid(
        "coalescing stream boolean column has no values buffer");
  }
  auto *values = static_cast<const std::uint8_t *>(slice.array->buffers[1]);
  const std::int64_t start = total - slice.length;
  for (std::int64_t row = 0; row < slice.length; ++row) {
    const std::int64_t source_row = slice.array->offset + slice.offset + row;
    if (bit_is_set(values, source_row)) {
      set_bit(&out->data, start + row);
    }
  }
  return sanitize::Status::OK();
}

/// Appends a variable-width slice while rebasing offsets into the output
/// buffer.
template <class Offset>
sanitize::Status append_binary_like(CoalescedNode *out, const ArraySlice &slice,
                                    std::vector<Offset> *offsets_out) {
  SAN_RETURN_NOT_OK(append_validity(out, slice));
  if (!slice.array->buffers || !slice.array->buffers[1]) {
    return sanitize::Status::Invalid(
        "coalescing stream variable-width column has missing buffers");
  }
  auto *offsets = static_cast<const Offset *>(slice.array->buffers[1]);
  auto *data = static_cast<const char *>(slice.array->buffers[2]);
  if (offsets_out->empty()) {
    offsets_out->push_back(0);
  }
  for (std::int64_t row = 0; row < slice.length; ++row) {
    const std::int64_t logical = slice.array->offset + slice.offset + row;
    const Offset begin = offsets[static_cast<std::size_t>(logical)];
    const Offset end = offsets[static_cast<std::size_t>(logical + 1)];
    if (end < begin) {
      return sanitize::Status::Invalid(
          "coalescing stream variable-width offsets are not monotonic");
    }
    const std::uint64_t value_size = static_cast<std::uint64_t>(end - begin);
    if (value_size >
        static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max() -
                                   out->data.size())) {
      return sanitize::Status::Invalid(
          "coalescing stream variable-width output is too large");
    }
    if (out->data.size() + static_cast<std::size_t>(value_size) >
        static_cast<std::size_t>(std::numeric_limits<Offset>::max())) {
      return sanitize::Status::Invalid(
          "coalescing stream variable-width output exceeds Arrow offset limit");
    }
    const std::size_t old_size = out->data.size();
    out->data.resize(old_size + static_cast<std::size_t>(value_size));
    if (value_size > 0) {
      if (!data) {
        return sanitize::Status::Invalid(
            "coalescing stream variable-width column has missing buffers");
      }
      std::memcpy(out->data.data() + old_size, data + begin,
                  static_cast<std::size_t>(value_size));
    }
    offsets_out->push_back(static_cast<Offset>(out->data.size()));
  }
  return sanitize::Status::OK();
}

// ---- append_dictionary ----
/// Extends a coalesced node by the requested number of type-correct null
/// values.
sanitize::Status append_nulls_node(const CoalesceNodeSpec &spec,
                                   CoalescedNode *out, std::int64_t length);

/// Compares all buffers and descendants of two coalesced nodes for exact
/// equality.
bool coalesced_node_equals(const CoalesceNodeSpec &spec, const CoalescedNode &a,
                           const CoalescedNode &b) {
  if (a.array.length != b.array.length ||
      a.array.null_count != b.array.null_count || a.validity != b.validity ||
      a.data != b.data || a.offsets32 != b.offsets32 ||
      a.offsets64 != b.offsets64 || a.children.size() != b.children.size()) {
    return false;
  }
  for (std::size_t i = 0; i < a.children.size(); ++i) {
    if (!coalesced_node_equals(spec.children[i], a.children[i],
                               b.children[i])) {
      return false;
    }
  }
  if (spec.kind == CoalesceKind::kDictionary) {
    if (a.dictionary_ready != b.dictionary_ready) {
      return false;
    }
    if (!a.dictionary_ready) {
      return true;
    }
    return a.dictionary && b.dictionary &&
           coalesced_node_equals(spec.children[0], *a.dictionary,
                                 *b.dictionary);
  }
  return true;
}

/// Appends dictionary indices after enforcing one stable dictionary across
/// batches.
sanitize::Status append_dictionary(const CoalesceNodeSpec &spec,
                                   CoalescedNode *out,
                                   const ArraySlice &slice) {
  if (spec.children.size() != 1) {
    return sanitize::Status::Invalid(
        "coalescing stream dictionary schema mismatch");
  }
  if (!slice.array->dictionary || !slice.array->dictionary->release) {
    return sanitize::Status::Invalid(
        "coalescing stream dictionary column has no live dictionary");
  }
  if (!out->dictionary_ready) {
    out->dictionary = std::make_unique<CoalescedNode>();
    SAN_RETURN_NOT_OK(append_node(spec.children[0], out->dictionary.get(),
                                  ArraySlice{slice.array->dictionary, 0,
                                             slice.array->dictionary->length}));
    out->dictionary_ready = true;
  } else {
    CoalescedNode current_dictionary;
    SAN_RETURN_NOT_OK(append_node(spec.children[0], &current_dictionary,
                                  ArraySlice{slice.array->dictionary, 0,
                                             slice.array->dictionary->length}));
    if (!coalesced_node_equals(spec.children[0], *out->dictionary,
                               current_dictionary)) {
      return sanitize::Status::Invalid(
          "coalescing stream dictionary values changed across batches");
    }
  }
  return append_fixed_width(out, slice, spec.fixed_width);
}

// ---- append_nested ----
/// Appends list-like rows while rebasing offsets and copying their child value
/// range.
template <class Offset>
sanitize::Status append_list_like(const CoalesceNodeSpec &spec,
                                  CoalescedNode *out, const ArraySlice &slice,
                                  std::vector<Offset> *offsets_out) {
  SAN_RETURN_NOT_OK(ensure_child_count(spec, *slice.array));
  SAN_RETURN_NOT_OK(ensure_output_children(out, spec));
  SAN_RETURN_NOT_OK(append_validity(out, slice));
  if (!slice.array->buffers || !slice.array->buffers[1]) {
    return sanitize::Status::Invalid(
        "coalescing stream list column has no offsets buffer");
  }
  if (offsets_out->empty()) {
    offsets_out->push_back(0);
  }
  if (slice.length == 0) {
    return sanitize::Status::OK();
  }
  const ArrowArray *child = slice.array->children[0];
  if (!child || !child->release) {
    return sanitize::Status::Invalid(
        "coalescing stream list column has missing child array");
  }
  auto *offsets = static_cast<const Offset *>(slice.array->buffers[1]);
  Offset output_end = offsets_out->back();
  for (std::int64_t row = 0; row < slice.length; ++row) {
    const std::int64_t logical = slice.array->offset + slice.offset + row;
    const Offset row_begin = offsets[static_cast<std::size_t>(logical)];
    const Offset row_end = offsets[static_cast<std::size_t>(logical + 1)];
    if (row_begin < 0 || row_end < row_begin) {
      return sanitize::Status::Invalid(
          "coalescing stream list row offsets are not monotonic");
    }
    if (row_is_valid(*slice.array, slice.offset + row)) {
      const Offset value_count = row_end - row_begin;
      if (output_end > std::numeric_limits<Offset>::max() - value_count) {
        return sanitize::Status::Invalid(
            "coalescing stream list output exceeds Arrow offset limit");
      }
      if (value_count > 0) {
        SAN_RETURN_NOT_OK(
            append_node(spec.children[0], &out->children[0],
                        ArraySlice{child, static_cast<std::int64_t>(row_begin),
                                   static_cast<std::int64_t>(value_count)}));
      }
      output_end = static_cast<Offset>(output_end + value_count);
    }
    offsets_out->push_back(output_end);
  }
  return sanitize::Status::OK();
}

/// Appends every struct child recursively while propagating parent nulls.
sanitize::Status append_struct(const CoalesceNodeSpec &spec, CoalescedNode *out,
                               const ArraySlice &slice) {
  SAN_RETURN_NOT_OK(ensure_child_count(spec, *slice.array));
  SAN_RETURN_NOT_OK(ensure_output_children(out, spec));
  SAN_RETURN_NOT_OK(append_validity(out, slice));
  for (std::size_t child_index = 0; child_index < spec.children.size();
       ++child_index) {
    const ArrowArray *child = slice.array->children[child_index];
    if (!child || !child->release) {
      return sanitize::Status::Invalid(
          "coalescing stream struct column has missing child array");
    }
    for (std::int64_t row = 0; row < slice.length; ++row) {
      const std::int64_t child_offset =
          slice.array->offset + slice.offset + row;
      if (row_is_valid(*slice.array, slice.offset + row)) {
        SAN_RETURN_NOT_OK(append_node(spec.children[child_index],
                                      &out->children[child_index],
                                      ArraySlice{child, child_offset, 1}));
      } else {
        SAN_RETURN_NOT_OK(append_nulls_node(spec.children[child_index],
                                            &out->children[child_index], 1));
      }
    }
  }
  return sanitize::Status::OK();
}

// ---- append_nulls ----
sanitize::Status append_nulls_node(const CoalesceNodeSpec &spec,
                                   CoalescedNode *out, std::int64_t length) {
  if (length < 0) {
    return sanitize::Status::Invalid(
        "coalescing stream cannot append negative null count");
  }
  if (length == 0) {
    return sanitize::Status::OK();
  }
  const std::int64_t start = out->array.length;
  const std::int64_t total = start + length;
  if (total < start) {
    return sanitize::Status::Invalid("coalescing stream length overflow");
  }
  const auto bitmap_bytes = static_cast<std::size_t>((total + 7) / 8);
  if (out->validity.empty()) {
    out->validity.resize(bitmap_bytes, 0xFFU);
  } else {
    out->validity.resize(bitmap_bytes, 0);
  }
  for (std::int64_t row = start; row < total; ++row) {
    clear_bit(&out->validity, row);
  }
  out->array.length = total;
  out->array.null_count += length;
  switch (spec.kind) {
  case CoalesceKind::kStruct:
    SAN_RETURN_NOT_OK(ensure_output_children(out, spec));
    for (std::size_t i = 0; i < spec.children.size(); ++i) {
      SAN_RETURN_NOT_OK(
          append_nulls_node(spec.children[i], &out->children[i], length));
    }
    break;
  case CoalesceKind::kList32:
    SAN_RETURN_NOT_OK(ensure_output_children(out, spec));
    if (out->offsets32.empty()) {
      out->offsets32.push_back(0);
    }
    out->offsets32.resize(out->offsets32.size() +
                              static_cast<std::size_t>(length),
                          out->offsets32.back());
    break;
  case CoalesceKind::kList64:
    SAN_RETURN_NOT_OK(ensure_output_children(out, spec));
    if (out->offsets64.empty()) {
      out->offsets64.push_back(0);
    }
    out->offsets64.resize(out->offsets64.size() +
                              static_cast<std::size_t>(length),
                          out->offsets64.back());
    break;
  case CoalesceKind::kUtf8:
  case CoalesceKind::kBinary:
    if (out->offsets32.empty()) {
      out->offsets32.push_back(0);
    }
    out->offsets32.resize(out->offsets32.size() +
                              static_cast<std::size_t>(length),
                          out->offsets32.back());
    break;
  case CoalesceKind::kLargeUtf8:
  case CoalesceKind::kLargeBinary:
    if (out->offsets64.empty()) {
      out->offsets64.push_back(0);
    }
    out->offsets64.resize(out->offsets64.size() +
                              static_cast<std::size_t>(length),
                          out->offsets64.back());
    break;
  case CoalesceKind::kFixedWidth: {
    const std::uint64_t bytes = static_cast<std::uint64_t>(length) *
                                static_cast<std::uint64_t>(spec.fixed_width);
    if (bytes >
        static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max() -
                                   out->data.size())) {
      return sanitize::Status::Invalid(
          "coalescing stream null fixed-width output is too large");
    }
    out->data.resize(out->data.size() + static_cast<std::size_t>(bytes), 0);
    break;
  }
  case CoalesceKind::kBool:
    out->data.resize(static_cast<std::size_t>((total + 7) / 8), 0);
    break;
  case CoalesceKind::kDictionary: {
    const std::uint64_t bytes = static_cast<std::uint64_t>(length) *
                                static_cast<std::uint64_t>(spec.fixed_width);
    if (bytes >
        static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max() -
                                   out->data.size())) {
      return sanitize::Status::Invalid(
          "coalescing stream null dictionary index output is too large");
    }
    out->data.resize(out->data.size() + static_cast<std::size_t>(bytes), 0);
    break;
  }
  }
  return sanitize::Status::OK();
}

// ---- append_dispatch ----

} // namespace

/// Appends one slice according to its recursive coalescing specification.
sanitize::Status append_node(const CoalesceNodeSpec &spec, CoalescedNode *out,
                             const ArraySlice &slice) {
  SAN_RETURN_NOT_OK(validate_slice(slice, spec.format));
  switch (spec.kind) {
  case CoalesceKind::kStruct:
    return append_struct(spec, out, slice);
  case CoalesceKind::kList32:
    return append_list_like(spec, out, slice, &out->offsets32);
  case CoalesceKind::kList64:
    return append_list_like(spec, out, slice, &out->offsets64);
  case CoalesceKind::kUtf8:
  case CoalesceKind::kBinary:
    return append_binary_like(out, slice, &out->offsets32);
  case CoalesceKind::kLargeUtf8:
  case CoalesceKind::kLargeBinary:
    return append_binary_like(out, slice, &out->offsets64);
  case CoalesceKind::kFixedWidth:
    return append_fixed_width(out, slice, spec.fixed_width);
  case CoalesceKind::kBool:
    return append_bool(out, slice);
  case CoalesceKind::kDictionary:
    return append_dictionary(spec, out, slice);
  }
  return sanitize::Status::Invalid("coalescing stream unsupported node kind");
}

} // namespace core_abi3_internal::coalesce_detail
