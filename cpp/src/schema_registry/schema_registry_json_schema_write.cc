// Implements canonical logical-schema JSON for registry documents.
// Recursive field and type writers preserve stable key order and escaping so
// equivalent schemas produce deterministic durable payloads.

#include "schema_registry/schema_registry_json_schema_write.hh"

#include "internal/json_encoding/token_writer.hh"

#include <string>
#include <string_view>
#include <vector>

namespace sanitize::schema_registry_internal {
namespace {

/// Returns the stable registry JSON kind name for one logical type.
std::string_view logical_type_kind_name(const LogicalType &type) noexcept {
  switch (type.kind) {
  case LogicalKind::kNull:
    return "null";
  case LogicalKind::kBool:
    return "bool";
  case LogicalKind::kInt64:
    return "int64";
  case LogicalKind::kFloat64:
    return "float64";
  case LogicalKind::kUtf8:
    return "string";
  case LogicalKind::kTimestampNs:
    return "timestamp_ns";
  case LogicalKind::kDate32:
    return "date32";
  case LogicalKind::kTime32s:
    return "time32s";
  case LogicalKind::kStruct:
    return "struct";
  case LogicalKind::kList:
    return "list";
  }
  return "string";
}

void append_logical_type_json(std::string &out, const LogicalType &type);

/// Appends one canonical schema field object.
void append_logical_field_json(std::string &out, const LogicalField &field) {
  out.push_back('{');
  bool first = true;
  internal::json_encoding::append_string_field(out, first, "name", field.name);
  internal::json_encoding::append_key(out, first, "nullable");
  out += field.nullable ? "true" : "false";
  internal::json_encoding::append_key(out, first, "type");
  if (field.type) {
    append_logical_type_json(out, *field.type);
  } else {
    out += "{\"kind\":\"null\"}";
  }
  out.push_back('}');
}

/// Appends a canonical schema sibling field array.
void append_logical_fields_json(std::string &out,
                                const std::vector<LogicalField> &fields) {
  out.push_back('[');
  for (std::size_t i = 0; i < fields.size(); ++i) {
    if (i != 0) {
      out.push_back(',');
    }
    append_logical_field_json(out, fields[i]);
  }
  out.push_back(']');
}

/// Appends one canonical logical type object.
void append_logical_type_json(std::string &out, const LogicalType &type) {
  out.push_back('{');
  bool first = true;
  internal::json_encoding::append_string_field(out, first, "kind",
                                               logical_type_kind_name(type));
  if (type.kind == LogicalKind::kStruct) {
    internal::json_encoding::append_key(out, first, "fields");
    append_logical_fields_json(out, type.fields);
  } else if (type.kind == LogicalKind::kList) {
    internal::json_encoding::append_key(out, first, "value");
    if (type.value) {
      append_logical_type_json(out, *type.value);
    } else {
      out += "{\"kind\":\"null\"}";
    }
  }
  out.push_back('}');
}

} // namespace

void append_canonical_schema_json(std::string &out,
                                  const LogicalSchema &schema) {
  out.push_back('{');
  bool first = true;
  internal::json_encoding::append_key(out, first, "fields");
  append_logical_fields_json(out, schema.fields);
  out.push_back('}');
}

} // namespace sanitize::schema_registry_internal
