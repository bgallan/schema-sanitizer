/*
 * Arrow C stream batch coalescing wrapper.
 *
 * This optional native wrapper combines many tiny Arrow record batches into
 * fewer larger batches before Parquet writing. Unsupported schemas return None
 * so the Python wrapper can fail before materializing batches.
 */
#include "internal/abi/core_abi3_internal.hh"

#include "api/python_abi3/_core_abi3_stream_lifecycle.hh"
#include "internal/pipeline/cdata_stream_utils.hh"
#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/status.hh"

#include "nanoarrow/nanoarrow.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace core_abi3_internal {
namespace {

enum class CoalesceKind {
  kStruct,
  kList32,
  kList64,
  kUtf8,
  kLargeUtf8,
  kBinary,
  kLargeBinary,
  kFixedWidth,
  kBool,
  kDictionary,
};

struct CoalesceNodeSpec {
  std::string format;
  CoalesceKind kind = CoalesceKind::kFixedWidth;
  std::size_t fixed_width = 0;
  std::vector<CoalesceNodeSpec> children;
};

struct CoalescedNode {
  ArrowArray array{};
  std::vector<std::uint8_t> validity;
  std::vector<std::uint8_t> data;
  std::vector<std::int32_t> offsets32;
  std::vector<std::int64_t> offsets64;
  std::vector<CoalescedNode> children;
  std::vector<ArrowArray *> child_ptrs;
  std::unique_ptr<CoalescedNode> dictionary;
  ArrowArray *dictionary_ptr = nullptr;
  bool dictionary_ready = false;
  const void *buffers[3]{nullptr, nullptr, nullptr};
};

struct CoalescedArrayState {
  CoalescedNode root;
};

struct ArraySlice {
  const ArrowArray *array = nullptr;
  std::int64_t offset = 0;
  std::int64_t length = 0;
};

struct CoalesceStreamState {
  ArrowArrayStream *inner = nullptr;
  PyObject *stream_obj = nullptr;
  PyObject *stream_capsule = nullptr;
  CoalesceNodeSpec root;
  std::int64_t target_rows = 65536;
  std::string last_error;
  bool closed = false;
};

void release_coalesce_stream(CoalesceStreamState *state) noexcept {
  if (!state || state->closed) {
    return;
  }
  close_arrow_stream_keepalive(&state->inner, &state->stream_obj,
                               &state->stream_capsule, &state->closed);
}

bool bit_is_set(const std::uint8_t *bitmap, std::int64_t index) noexcept {
  return (bitmap[static_cast<std::size_t>(index >> 3)] &
          (static_cast<std::uint8_t>(1U) << (index & 7))) != 0;
}

void set_bit(std::vector<std::uint8_t> *bitmap, std::int64_t index) {
  (*bitmap)[static_cast<std::size_t>(index >> 3)] |=
      static_cast<std::uint8_t>(1U) << (index & 7);
}

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

bool row_is_valid(const ArrowArray &array, std::int64_t logical_row) noexcept {
  if (array.null_count == 0 || !array.buffers || !array.buffers[0]) {
    return true;
  }
  auto *validity = static_cast<const std::uint8_t *>(array.buffers[0]);
  return bit_is_set(validity, array.offset + logical_row);
}

sanitize::Status append_validity(CoalescedNode *out, const ArraySlice &slice) {
  SAN_RETURN_NOT_OK(validate_slice(slice, "validity"));
  const std::int64_t start = out->array.length;
  const std::int64_t total = start + slice.length;
  if (total < start) {
    return sanitize::Status::Invalid("coalescing stream length overflow");
  }
  out->validity.resize(static_cast<std::size_t>((total + 7) / 8), 0);
  for (std::int64_t row = 0; row < slice.length; ++row) {
    if (row_is_valid(*slice.array, slice.offset + row)) {
      set_bit(&out->validity, start + row);
    } else {
      ++out->array.null_count;
    }
  }
  out->array.length = total;
  return sanitize::Status::OK();
}

std::size_t fixed_width_for_format(std::string_view format) noexcept {
  if (format == "c" || format == "C") {
    return 1;
  }
  if (format == "s" || format == "S" || format == "e") {
    return 2;
  }
  if (format == "i" || format == "I" || format == "f" || format == "tdD" ||
      format == "tti" || format == "tiM") {
    return 4;
  }
  if (format == "l" || format == "L" || format == "g" || format == "tdm" ||
      format == "tts" || format == "ttm" || format == "ttu" ||
      format == "ttn" || format == "tDs" || format == "tDm" ||
      format == "tDu" || format == "tDn" || format == "tiD" ||
      format.starts_with("ts")) {
    return 8;
  }
  if (format == "tin") {
    return 16;
  }
  return 0;
}

std::size_t
dictionary_index_width_for_format(std::string_view format) noexcept {
  if (format == "c" || format == "C") {
    return 1;
  }
  if (format == "s" || format == "S") {
    return 2;
  }
  if (format == "i" || format == "I") {
    return 4;
  }
  if (format == "l" || format == "L") {
    return 8;
  }
  return 0;
}

bool parse_supported_schema_node(const ArrowSchema &schema,
                                 CoalesceNodeSpec *out) {
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
    if (!parse_supported_schema_node(*schema.dictionary, &out->children[0])) {
      out->children.clear();
      return false;
    }
    return true;
  }
  if (format == "+s") {
    if (schema.n_children < 0) {
      return false;
    }
    out->kind = CoalesceKind::kStruct;
    out->children.resize(static_cast<std::size_t>(schema.n_children));
    for (std::int64_t i = 0; i < schema.n_children; ++i) {
      const ArrowSchema *child = schema.children ? schema.children[i] : nullptr;
      if (!child || !parse_supported_schema_node(*child, &out->children[i])) {
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
    if (!parse_supported_schema_node(*schema.children[0], &out->children[0])) {
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

bool schema_supported(const ArrowSchema &schema, CoalesceNodeSpec *root) {
  if (!parse_supported_schema_node(schema, root)) {
    return false;
  }
  return root->kind == CoalesceKind::kStruct && !root->children.empty();
}

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
  out->data.resize(old_size + static_cast<std::size_t>(bytes));
  std::memcpy(out->data.data() + old_size, values,
              static_cast<std::size_t>(bytes));
  return sanitize::Status::OK();
}

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

sanitize::Status append_node(const CoalesceNodeSpec &spec, CoalescedNode *out,
                             const ArraySlice &slice);

sanitize::Status append_nulls_node(const CoalesceNodeSpec &spec,
                                   CoalescedNode *out, std::int64_t length);

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

sanitize::Status append_nulls_node(const CoalesceNodeSpec &spec,
                                   CoalescedNode *out, std::int64_t length) {
  if (length < 0) {
    return sanitize::Status::Invalid(
        "coalescing stream cannot append negative null count");
  }
  const std::int64_t start = out->array.length;
  const std::int64_t total = start + length;
  if (total < start) {
    return sanitize::Status::Invalid("coalescing stream length overflow");
  }
  out->validity.resize(static_cast<std::size_t>((total + 7) / 8), 0);
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

sanitize::Result<std::unique_ptr<CoalescedArrayState>>
build_coalesced_array_state(
    const std::vector<std::unique_ptr<sanitize::CArrayGuard>> &batches,
    const CoalesceNodeSpec &root_spec) {
  auto state = std::make_unique<CoalescedArrayState>();
  for (const auto &batch_guard : batches) {
    const ArrowArray &batch = batch_guard->value();
    if (!batch.release) {
      continue;
    }
    SAN_RETURN_NOT_OK(append_node(root_spec, &state->root,
                                  ArraySlice{&batch, 0, batch.length}));
  }
  SAN_RETURN_NOT_OK(finish_node(&state->root, root_spec, true));
  return state;
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

const char *coalesce_last_error(ArrowArrayStream *stream) {
  if (!stream) {
    return "invalid coalescing stream";
  }
  auto *state = static_cast<CoalesceStreamState *>(stream->private_data);
  return state ? sanitize::internal::cdata_stream::last_error_ptr(
                     state->last_error)
               : nullptr;
}

void coalesce_release(ArrowArrayStream *stream) {
  if (!stream || !stream->release) {
    return;
  }
  auto *state = static_cast<CoalesceStreamState *>(stream->private_data);
  release_coalesce_stream(state);
  delete state;
  sanitize::internal::cdata_stream::clear_stream(stream);
}

int coalesce_get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *state = static_cast<CoalesceStreamState *>(stream->private_data);
  if (!state || !state->inner) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_schema_callback(
      out, state->last_error, "coalescing_stream.get_schema",
      [&](ArrowSchema *schema) -> sanitize::Status {
        const int rc = state->inner->get_schema(state->inner, schema);
        if (rc != 0) {
          return sanitize::Status::IOError(
              "coalescing stream inner get_schema failed");
        }
        return sanitize::Status::OK();
      });
}

int coalesce_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *state = static_cast<CoalesceStreamState *>(stream->private_data);
  if (!state || !state->inner) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_array_callback(
      out, state->last_error, "coalescing_stream.get_next",
      [&](ArrowArray *array) -> sanitize::Status {
        std::vector<std::unique_ptr<sanitize::CArrayGuard>> batches;
        std::int64_t rows = 0;
        while (rows < state->target_rows) {
          auto batch = std::make_unique<sanitize::CArrayGuard>();
          const int rc = state->inner->get_next(state->inner, batch->get());
          if (rc != 0) {
            return sanitize::Status::IOError(
                "coalescing stream inner get_next failed");
          }
          if (!batch->value().release) {
            break;
          }
          rows += batch->value().length;
          batches.push_back(std::move(batch));
        }
        if (batches.empty()) {
          sanitize::internal::cdata_stream::clear_array(array);
          return sanitize::Status::OK();
        }
        auto built = build_coalesced_array_state(batches, state->root);
        if (!built.ok()) {
          return built.status();
        }
        return export_coalesced_array(std::move(built).ValueOrDie(), array);
      });
}

} // namespace

PyObject *py_coalescing_stream_wrap(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  long long target_rows_arg = 65536;
  if (!PyArg_ParseTuple(args, "O|L:coalescing_stream_wrap", &stream_obj,
                        &target_rows_arg)) {
    return nullptr;
  }
  if (target_rows_arg <= 0) {
    PyErr_SetString(PyExc_ValueError, "target_rows must be positive");
    return nullptr;
  }

  PyObject *capsule = nullptr;
  ArrowArrayStream *inner = nullptr;
  if (!acquire_arrow_stream(stream_obj, &capsule, &inner)) {
    return nullptr;
  }

  sanitize::CSchemaGuard schema;
  const int rc = inner->get_schema(inner, schema.get());
  if (rc != 0) {
    Py_DECREF(capsule);
    PyErr_SetString(PyExc_RuntimeError,
                    "coalescing stream inner get_schema failed");
    return nullptr;
  }

  CoalesceNodeSpec root;
  if (!schema_supported(schema.value(), &root)) {
    Py_DECREF(capsule);
    Py_INCREF(Py_None);
    return Py_None;
  }

  auto state = std::make_unique<CoalesceStreamState>();
  state->inner = inner;
  state->stream_capsule = capsule;
  Py_INCREF(stream_obj);
  state->stream_obj = stream_obj;
  state->root = std::move(root);
  state->target_rows = static_cast<std::int64_t>(target_rows_arg);

  auto *wrapped = new (std::nothrow) ArrowArrayStream();
  if (!wrapped) {
    release_coalesce_stream(state.get());
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(wrapped, 0, sizeof(*wrapped));
  wrapped->get_schema = &coalesce_get_schema;
  wrapped->get_next = &coalesce_get_next;
  wrapped->get_last_error = &coalesce_last_error;
  wrapped->release = &coalesce_release;
  wrapped->private_data = state.release();
  return wrap_stream_capsule_with_keepalive(stream_obj, wrapped);
}

} // namespace core_abi3_internal
