// Implements JSON parsing and serialization for schema-registry documents.

#include "schema_registry/schema_registry_internal.hh"

#include "internal/parsing/json/ondemand/document.hh"
#include "sanitize/core/value_view.hh"

#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace sanitize::schema_registry_internal {
namespace {

Result<std::optional<ValueView>> object_member(ValueView object,
                                               std::string_view member) {
  if (!object.is_object()) {
    return Status::Invalid(
        "schema registry canonical_schema node is not an object");
  }
  std::optional<ValueView> out;
  SAN_RETURN_NOT_OK(object.for_each_object_field(
      [&](std::string_view key, uint64_t, ValueView value) -> Status {
        if (key == member)
          out = value;
        return Status::OK();
      }));
  return out;
}

Result<ValueView> required_object_member(ValueView object,
                                         std::string_view member,
                                         std::string_view context) {
  SAN_ASSIGN_OR_RAISE(auto value, object_member(object, member));
  if (!value) {
    return Status::Invalid("schema registry canonical_schema ", context,
                           " is missing ", member);
  }
  return *value;
}

Result<LogicalType> logical_type_from_registry_node(ValueView node);

Result<LogicalField> logical_field_from_registry_node(ValueView node) {
  if (!node.is_object()) {
    return Status::Invalid(
        "schema registry canonical_schema field is not an object");
  }

  SAN_ASSIGN_OR_RAISE(auto name_value,
                      required_object_member(node, "name", "field"));
  if (!name_value.is_string() || name_value.as_string_view().empty()) {
    return Status::Invalid(
        "schema registry canonical_schema field is missing name");
  }

  SAN_ASSIGN_OR_RAISE(auto type_value,
                      required_object_member(node, "type", "field"));
  if (!type_value.is_object()) {
    return Status::Invalid("schema registry canonical_schema field ",
                           name_value.as_string_view(), " is missing type");
  }

  bool nullable = true;
  SAN_ASSIGN_OR_RAISE(auto nullable_value, object_member(node, "nullable"));
  if (nullable_value) {
    if (!nullable_value->is_bool()) {
      return Status::Invalid("schema registry canonical_schema field ",
                             name_value.as_string_view(),
                             " nullable must be bool");
    }
    nullable = nullable_value->as_bool();
  }

  SAN_ASSIGN_OR_RAISE(auto type, logical_type_from_registry_node(type_value));
  LogicalField field;
  field.name = std::string(name_value.as_string_view());
  field.type = std::make_unique<LogicalType>(std::move(type));
  field.nullable = nullable;
  return field;
}

Result<std::vector<LogicalField>>
logical_fields_from_registry_array(ValueView fields_value,
                                   std::string_view context) {
  if (!fields_value.is_array()) {
    return Status::Invalid("schema registry canonical_schema ", context,
                           " is missing fields");
  }

  std::vector<LogicalField> fields;
  SAN_RETURN_NOT_OK(
      fields_value.for_each_array_element([&](ValueView field_value) -> Status {
        SAN_ASSIGN_OR_RAISE(auto field,
                            logical_field_from_registry_node(field_value));
        fields.push_back(std::move(field));
        return Status::OK();
      }));
  return fields;
}

Result<LogicalType> logical_type_from_registry_node(ValueView node) {
  if (!node.is_object()) {
    return Status::Invalid(
        "schema registry canonical_schema type is not an object");
  }

  SAN_ASSIGN_OR_RAISE(auto kind_value,
                      required_object_member(node, "kind", "type"));
  if (!kind_value.is_string()) {
    return Status::Invalid(
        "schema registry canonical_schema type kind must be string");
  }
  const std::string_view kind = kind_value.as_string_view();

  if (kind == "null")
    return LogicalType(LogicalKind::kNull);
  if (kind == "bool")
    return LogicalType::Bool();
  if (kind == "int64")
    return LogicalType::Int64();
  if (kind == "float64")
    return LogicalType::Float64();
  if (kind == "string")
    return LogicalType::Utf8();
  if (kind == "timestamp_ns")
    return LogicalType::TimestampNs();
  if (kind == "date32")
    return LogicalType::Date32();
  if (kind == "time32s")
    return LogicalType::Time32s();

  if (kind == "list") {
    SAN_ASSIGN_OR_RAISE(auto value,
                        required_object_member(node, "value", "list type"));
    if (!value.is_object()) {
      return Status::Invalid(
          "schema registry canonical_schema list type is missing value");
    }
    SAN_ASSIGN_OR_RAISE(auto element_type,
                        logical_type_from_registry_node(value));
    return LogicalType::List(std::move(element_type));
  }

  if (kind == "struct") {
    SAN_ASSIGN_OR_RAISE(auto fields_value,
                        required_object_member(node, "fields", "struct type"));
    SAN_ASSIGN_OR_RAISE(auto fields, logical_fields_from_registry_array(
                                         fields_value, "struct type"));
    return LogicalType::Struct(std::move(fields));
  }

  return Status::Invalid("Unsupported schema_registry canonical_schema kind: ",
                         kind);
}

} // namespace

Result<std::optional<LogicalSchema>>
canonical_schema_from_registry_json(std::string_view registry_json) {
  if (registry_json.empty())
    return std::nullopt;

  internal::JsonOnDemandDoc doc(std::pmr::new_delete_resource());
  SAN_ASSIGN_OR_RAISE(auto root, doc.ParseValue(registry_json));
  if (!root.is_object()) {
    return Status::Invalid("schema_registry must be a JSON object");
  }

  SAN_ASSIGN_OR_RAISE(auto canonical_schema,
                      object_member(root, "canonical_schema"));
  if (!canonical_schema)
    return std::nullopt;
  if (!canonical_schema->is_object()) {
    return Status::Invalid(
        "schema_registry canonical_schema must be a JSON object");
  }

  SAN_ASSIGN_OR_RAISE(
      auto fields_value,
      required_object_member(*canonical_schema, "fields", "root schema"));
  SAN_ASSIGN_OR_RAISE(auto fields, logical_fields_from_registry_array(
                                       fields_value, "root schema"));

  LogicalSchema schema;
  schema.fields = std::move(fields);
  return std::optional<LogicalSchema>(std::move(schema));
}

} // namespace sanitize::schema_registry_internal
