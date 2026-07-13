// Parses Arrow C schemas into the compact native writer model.

#include "internal/json_output/schema/model.hh"

#include "internal/arrow_text/formatters.hh"

#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace sanitize::internal::jsonl_stream_writer {
namespace {

bool parse_decimal_format(std::string_view format, JsonlField *field) {
  if (!field) {
    return false;
  }
  sanitize::internal::arrow_format::DecimalFormat parsed;
  if (!sanitize::internal::arrow_format::parse_decimal_format(format,
                                                              &parsed)) {
    return false;
  }
  field->decimal_precision = parsed.precision;
  field->decimal_scale = parsed.scale;
  field->decimal_byte_width = parsed.byte_width;
  return true;
}

bool parse_fixed_size_list_format(std::string_view format, JsonlField *field) {
  if (!format.starts_with("+w:") || !field) {
    return false;
  }
  int32_t size = 0;
  const char *begin = format.data() + 3;
  const char *end = format.data() + format.size();
  auto [ptr, ec] = std::from_chars(begin, end, size);
  if (ec != std::errc() || ptr != end || size < 0) {
    return false;
  }
  field->fixed_size_list_size = size;
  return true;
}

bool parse_fixed_size_binary_format(std::string_view format,
                                    JsonlField *field) {
  if (!format.starts_with("w:") || !field) {
    return false;
  }
  int32_t size = 0;
  const char *begin = format.data() + 2;
  const char *end = format.data() + format.size();
  auto [ptr, ec] = std::from_chars(begin, end, size);
  if (ec != std::errc() || ptr != end || size <= 0) {
    return false;
  }
  field->fixed_size_binary_size = size;
  return true;
}

} // namespace

sanitize::Result<JsonlField> parse_schema_field(const ArrowSchema &schema) {
  if (!schema.format) {
    return sanitize::Status::Invalid("JSONL writer: schema format is null");
  }
  JsonlField field;
  field.name = schema.name ? schema.name : "";
  field.format = schema.format;
  field.nullable = (schema.flags & ARROW_FLAG_NULLABLE) != 0;
  SAN_ASSIGN_OR_RAISE(field.kind, kind_from_format(schema.format));
  if (field.kind == JsonlKind::kDecimal &&
      !parse_decimal_format(schema.format, &field)) {
    return sanitize::Status::Invalid(
        "JSONL writer: invalid decimal Arrow format '",
        std::string(schema.format), "'");
  }
  if (field.kind == JsonlKind::kFixedSizeList &&
      !parse_fixed_size_list_format(schema.format, &field)) {
    return sanitize::Status::Invalid(
        "JSONL writer: invalid fixed-size list Arrow format '",
        std::string(schema.format), "'");
  }
  if (field.kind == JsonlKind::kFixedSizeBinary &&
      !parse_fixed_size_binary_format(schema.format, &field)) {
    return sanitize::Status::Invalid(
        "JSONL writer: invalid fixed-size binary Arrow format '",
        std::string(schema.format), "'");
  }
  if (schema.n_children < 0) {
    return sanitize::Status::Invalid("JSONL writer: negative schema children");
  }
  field.children.reserve(static_cast<std::size_t>(schema.n_children));
  for (int64_t i = 0; i < schema.n_children; ++i) {
    if (!schema.children || !schema.children[i]) {
      return sanitize::Status::Invalid("JSONL writer: missing schema child");
    }
    SAN_ASSIGN_OR_RAISE(auto child, parse_schema_field(*schema.children[i]));
    field.children.push_back(std::move(child));
  }
  if (schema.dictionary) {
    JsonlField dictionary;
    dictionary.name = "dictionary";
    SAN_ASSIGN_OR_RAISE(dictionary, parse_schema_field(*schema.dictionary));
    field.dictionary_index_kind = field.kind;
    field.kind = JsonlKind::kDictionary;
    field.children.clear();
    field.children.push_back(std::move(dictionary));
  }
  return field;
}

sanitize::Status validate_batch(const JsonlField &root,
                                const ArrowArray &array) {
  if (root.kind != JsonlKind::kStruct) {
    return sanitize::Status::Invalid("JSONL writer: root schema is not struct");
  }
  if (array.n_children != static_cast<int64_t>(root.children.size()) ||
      (!root.children.empty() && !array.children)) {
    return sanitize::Status::Invalid(
        "JSONL writer: root array/schema mismatch");
  }
  if (array.length < 0) {
    return sanitize::Status::Invalid("JSONL writer: negative batch length");
  }
  return sanitize::Status::OK();
}

bool schema_is_supported(const ArrowSchema &schema) {
  return parse_schema_field(schema).ok();
}

} // namespace sanitize::internal::jsonl_stream_writer
