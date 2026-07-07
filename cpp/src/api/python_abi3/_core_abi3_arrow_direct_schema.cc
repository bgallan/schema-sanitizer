// Implements Arrow C schema parsing helpers for direct ingestion.

#include "api/python_abi3/_core_abi3_arrow_direct_schema.hh"

#include "api/python_abi3/_core_abi3_arrow_direct_formatters.hh"

#include <charconv>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "sanitize/core/logical_schema.hh"

namespace core_abi3_internal {
namespace {

sanitize::Result<sanitize::LogicalType>
parse_arrow_type(const ArrowSchema *schema, ArrowInputNode *node,
                 const ArrowDirectOptions &options);

sanitize::Result<sanitize::LogicalType>
parse_struct_type(const ArrowSchema *schema, ArrowInputNode *node,
                  const ArrowDirectOptions &options) {
  node->kind = ArrowNodeKind::kStruct;
  node->children.clear();
  node->children.reserve(static_cast<std::size_t>(schema->n_children));
  sanitize::LogicalType out(sanitize::LogicalKind::kStruct);
  out.fields.reserve(static_cast<std::size_t>(schema->n_children));
  for (int64_t i = 0; i < schema->n_children; ++i) {
    const ArrowSchema *child = schema->children[i];
    if (!child || !child->format) {
      return sanitize::Status::Invalid(
          "Arrow direct input has invalid child schema");
    }
    ArrowInputNode child_node;
    child_node.name = child->name ? child->name : "";
    if (child_node.name.empty()) {
      return sanitize::Status::Invalid(
          "Arrow direct input field name is empty");
    }
    SAN_ASSIGN_OR_RAISE(auto child_type,
                        parse_arrow_type(child, &child_node, options));

    sanitize::LogicalField logical_field;
    logical_field.name = child_node.name;
    logical_field.nullable = (child->flags & ARROW_FLAG_NULLABLE) != 0;
    logical_field.type = std::make_unique<sanitize::LogicalType>(child_type);
    out.fields.push_back(logical_field);

    child_node.logical_type = child_type;
    node->children.push_back(std::move(child_node));
  }
  return out;
}

sanitize::Result<sanitize::LogicalType>
parse_list_type(const ArrowSchema *schema, ArrowInputNode *node,
                ArrowNodeKind kind, const ArrowDirectOptions &options) {
  if (schema->n_children != 1 || !schema->children || !schema->children[0]) {
    return sanitize::Status::Invalid(
        "Arrow direct list input requires one child schema");
  }
  node->kind = kind;
  node->children.clear();
  node->children.reserve(1);
  ArrowInputNode child_node;
  child_node.name = "item";
  SAN_ASSIGN_OR_RAISE(auto child_type, parse_arrow_type(schema->children[0],
                                                        &child_node, options));
  child_node.logical_type = child_type;
  node->children.push_back(std::move(child_node));
  return sanitize::LogicalType::List(child_type);
}

sanitize::Result<sanitize::LogicalType>
parse_fixed_size_list_type(const ArrowSchema *schema, ArrowInputNode *node,
                           const ArrowDirectOptions &options) {
  const std::string_view format(schema->format ? schema->format : "");
  int32_t list_size = 0;
  const char *begin = format.data() + 3;
  const char *end = format.data() + format.size();
  auto [ptr, ec] = std::from_chars(begin, end, list_size);
  if (ec != std::errc() || ptr != end || list_size < 0) {
    return sanitize::Status::Invalid(
        "Arrow direct fixed-size list has invalid size");
  }
  SAN_ASSIGN_OR_RAISE(
      auto out,
      parse_list_type(schema, node, ArrowNodeKind::kFixedSizeList, options));
  node->fixed_size_list_size = list_size;
  return out;
}

sanitize::Result<sanitize::LogicalType>
parse_map_type(const ArrowSchema *schema, ArrowInputNode *node,
               const ArrowDirectOptions &options) {
  if (schema->n_children != 1 || !schema->children || !schema->children[0]) {
    return sanitize::Status::Invalid(
        "Arrow direct map input requires one entries child schema");
  }
  return parse_list_type(schema, node, ArrowNodeKind::kMap, options);
}

sanitize::Result<sanitize::LogicalType>
parse_arrow_type(const ArrowSchema *schema, ArrowInputNode *node,
                 const ArrowDirectOptions &options) {
  if (!schema || !schema->format || !node) {
    return sanitize::Status::Invalid("Arrow direct input has invalid schema");
  }
  node->format = schema->format;
  const std::string_view format(node->format);
  node->timestamp_target_units_per_second =
      timestamp_target_units_per_second(options.timestamp_precision);

  if (schema->dictionary) {
    node->kind = ArrowNodeKind::kDictionary;
    node->children.clear();
    node->children.reserve(1);
    ArrowInputNode value_node;
    value_node.name = "dictionary";
    SAN_ASSIGN_OR_RAISE(
        auto value_type,
        parse_arrow_type(schema->dictionary, &value_node, options));
    value_node.logical_type = value_type;
    node->children.push_back(std::move(value_node));
    return value_type;
  }

  if (format == "n") {
    node->kind = ArrowNodeKind::kNull;
    return sanitize::LogicalType(sanitize::LogicalKind::kNull);
  }
  if (format == "b") {
    node->kind = ArrowNodeKind::kBool;
    return sanitize::LogicalType::Bool();
  }
  if (format == "c" || format == "C" || format == "s" || format == "S" ||
      format == "i" || format == "I" || format == "l") {
    node->kind = ArrowNodeKind::kInt;
    return sanitize::LogicalType::Int64();
  }
  if (format == "L") {
    node->kind = ArrowNodeKind::kUInt64Text;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "f" || format == "g") {
    node->kind = ArrowNodeKind::kFloat;
    return sanitize::LogicalType::Float64();
  }
  if (format == "u" || format == "U") {
    node->kind = ArrowNodeKind::kUtf8;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "z" || format == "Z") {
    node->kind = ArrowNodeKind::kBinaryBase64;
    return sanitize::LogicalType::Utf8();
  }
  if (format.starts_with("d:") && parse_decimal_format(format, node)) {
    node->kind = ArrowNodeKind::kDecimalText;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "tdD") {
    node->kind = ArrowNodeKind::kDate32;
    return sanitize::LogicalType::Date32();
  }
  if (format == "tdm") {
    node->kind = ArrowNodeKind::kDate64;
    return sanitize::LogicalType::Date32();
  }
  if (format == "tts") {
    node->kind = ArrowNodeKind::kTime32s;
    return sanitize::LogicalType::Time32s();
  }
  if (format == "ttm" || format == "ttu" || format == "ttn") {
    node->kind = ArrowNodeKind::kTimeText;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "tDs" || format == "tDm" || format == "tDu" ||
      format == "tDn") {
    node->kind = ArrowNodeKind::kDurationText;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "tiM" || format == "tiD" || format == "tin") {
    node->kind = ArrowNodeKind::kIntervalText;
    return sanitize::LogicalType::Utf8();
  }
  if (format.starts_with("ts")) {
    node->kind = ArrowNodeKind::kTimestamp;
    node->timestamp_source_units_per_second =
        timestamp_source_units_per_second(format);
    return sanitize::LogicalType::TimestampNs();
  }
  if (format == "+s") {
    return parse_struct_type(schema, node, options);
  }
  if (format == "+l") {
    return parse_list_type(schema, node, ArrowNodeKind::kList, options);
  }
  if (format == "+L") {
    return parse_list_type(schema, node, ArrowNodeKind::kLargeList, options);
  }
  if (format.starts_with("+w:")) {
    return parse_fixed_size_list_type(schema, node, options);
  }
  if (format == "+m") {
    return parse_map_type(schema, node, options);
  }
  return sanitize::Status::Invalid("unsupported Arrow direct field format: ",
                                   format);
}

} // namespace

sanitize::Result<sanitize::LogicalSchema>
logical_schema_from_arrow_schema(const ArrowSchema *schema,
                                 std::vector<ArrowInputNode> *fields,
                                 const ArrowDirectOptions &options) {
  if (!schema || !schema->format || std::string_view(schema->format) != "+s") {
    return sanitize::Status::Invalid(
        "Arrow direct input requires a struct stream schema");
  }
  ArrowInputNode root;
  root.name = "";
  SAN_ASSIGN_OR_RAISE(auto root_type,
                      parse_struct_type(schema, &root, options));
  root.logical_type = std::move(root_type);
  sanitize::LogicalSchema out;
  out.fields = root.logical_type.fields;
  fields->clear();
  fields->reserve(root.children.size());
  for (auto &child : root.children) {
    fields->push_back(std::move(child));
  }
  return out;
}

} // namespace core_abi3_internal
