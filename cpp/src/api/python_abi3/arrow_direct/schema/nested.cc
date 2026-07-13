// Parses nested Arrow C schema nodes for direct ingestion.

#include "api/python_abi3/arrow_direct/schema/parser_internal.hh"

#include <charconv>
#include <cstdint>
#include <string_view>
#include <utility>

namespace core_abi3_internal::arrow_schema_internal {

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

} // namespace core_abi3_internal::arrow_schema_internal
