// Builds a logical schema and direct input plan from Arrow C schema nodes.

#include "api/python_abi3/arrow_direct/schema/logical.hh"

#include "api/python_abi3/arrow_direct/schema/parser_internal.hh"

#include <cstdint>
#include <string_view>
#include <utility>

namespace core_abi3_internal {
namespace {

constexpr std::int64_t kMaxArrowSchemaDepth = 64;
constexpr std::int64_t kMaxArrowSchemaChildren = 65'536;
constexpr std::int64_t kMaxArrowSchemaNodes = 1'000'000;

sanitize::Status validate_arrow_schema_shape(const ArrowSchema *schema,
                                               std::int64_t depth,
                                               std::int64_t *nodes) {
  if (!schema || !schema->format || !nodes) {
    return sanitize::Status::Invalid(
        "Arrow direct input has an invalid schema node");
  }
  if (depth > kMaxArrowSchemaDepth) {
    return sanitize::Status::OutOfMemory(
        "Arrow direct schema nesting exceeds safety limit");
  }
  if (schema->n_children < 0) {
    return sanitize::Status::Invalid(
        "Arrow direct schema has a negative child count");
  }
  if (schema->n_children > kMaxArrowSchemaChildren) {
    return sanitize::Status::OutOfMemory(
        "Arrow direct schema child count exceeds safety limit");
  }
  if (schema->n_children > 0 && !schema->children) {
    return sanitize::Status::Invalid(
        "Arrow direct schema is missing its children array");
  }
  if (*nodes >= kMaxArrowSchemaNodes) {
    return sanitize::Status::OutOfMemory(
        "Arrow direct schema node count exceeds safety limit");
  }
  ++*nodes;
  for (std::int64_t index = 0; index < schema->n_children; ++index) {
    if (!schema->children[index]) {
      return sanitize::Status::Invalid(
          "Arrow direct schema has a null child pointer");
    }
    SAN_RETURN_NOT_OK(validate_arrow_schema_shape(
        schema->children[index], depth + 1, nodes));
  }
  if (schema->dictionary) {
    SAN_RETURN_NOT_OK(
        validate_arrow_schema_shape(schema->dictionary, depth + 1, nodes));
  }
  return {};
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
  std::int64_t schema_nodes = 0;
  SAN_RETURN_NOT_OK(validate_arrow_schema_shape(schema, 0, &schema_nodes));
  ArrowInputNode root;
  root.name = "";
  SAN_ASSIGN_OR_RAISE(auto root_type, arrow_schema_internal::parse_struct_type(
                                          schema, &root, options));
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
