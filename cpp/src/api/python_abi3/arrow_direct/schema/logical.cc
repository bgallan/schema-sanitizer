// Builds a logical schema and direct input plan from Arrow C schema nodes.

#include "api/python_abi3/arrow_direct/schema/logical.hh"

#include "api/python_abi3/arrow_direct/schema/parser_internal.hh"

#include <string_view>
#include <utility>

namespace core_abi3_internal {

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
