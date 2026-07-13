/* Arrow C stream coalescing schema support. */
#include "api/python_abi3/streaming/coalesce_stream_internal.hh"

#include <algorithm>
#include <array>
#include <string_view>

namespace core_abi3_internal::coalesce_detail {
namespace {

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

} // namespace

bool schema_supported(const ArrowSchema &schema, CoalesceNodeSpec *root) {
  if (!parse_supported_schema_node(schema, root)) {
    return false;
  }
  return root->kind == CoalesceKind::kStruct && !root->children.empty();
}

} // namespace core_abi3_internal::coalesce_detail
