// Implements shared logical-type helpers for the schema-registry engine.

#include "schema_registry/schema_registry_internal.hh"

#include "internal/planning/variant_field_names.hh"

#include <algorithm>
#include <string>
#include <string_view>

namespace sanitize::schema_registry_internal {

TopLevelKind top_level_kind(const LogicalType &type) noexcept {
  if (type.kind == LogicalKind::kStruct)
    return TopLevelKind::kStruct;
  if (type.kind == LogicalKind::kList)
    return TopLevelKind::kList;
  return TopLevelKind::kScalar;
}

bool same_top_level_kind(const LogicalType &left,
                         const LogicalType &right) noexcept {
  return top_level_kind(left) == top_level_kind(right);
}

std::string variant_semantic_type(const LogicalType &type) {
  switch (type.kind) {
  case LogicalKind::kNull:
    return "null";
  case LogicalKind::kBool:
    return "boolean";
  case LogicalKind::kInt64:
    return "integer";
  case LogicalKind::kFloat64:
    return "float";
  case LogicalKind::kUtf8:
    return "string";
  case LogicalKind::kTimestampNs:
    return "timestamp";
  case LogicalKind::kDate32:
    return "date";
  case LogicalKind::kTime32s:
    return "time";
  case LogicalKind::kStruct:
    return "struct";
  case LogicalKind::kList:
    return (type.value ? variant_semantic_type(*type.value)
                       : std::string("null")) +
           "_array";
  }
  return "string";
}

std::string_view
source_segment_for_output(std::string_view output_segment) noexcept {
  return sanitize::internal::variant_family_base(output_segment);
}

std::string join_path(std::string_view parent, std::string_view child) {
  if (parent.empty())
    return std::string(child);
  std::string out;
  out.reserve(parent.size() + 1U + child.size());
  out.append(parent);
  out.push_back('.');
  out.append(child);
  return out;
}

bool logical_type_equal(const LogicalType &left, const LogicalType &right) {
  if (left.kind != right.kind)
    return false;
  if (left.kind == LogicalKind::kList) {
    if (!left.value || !right.value)
      return left.value == nullptr && right.value == nullptr;
    return logical_type_equal(*left.value, *right.value);
  }
  if (left.kind == LogicalKind::kStruct) {
    if (left.fields.size() != right.fields.size())
      return false;
    for (std::size_t i = 0; i < left.fields.size(); ++i) {
      const auto &lf = left.fields[i];
      const auto &rf = right.fields[i];
      if (lf.name != rf.name || lf.nullable != rf.nullable)
        return false;
      if (static_cast<bool>(lf.type) != static_cast<bool>(rf.type))
        return false;
      if (lf.type && !logical_type_equal(*lf.type, *rf.type))
        return false;
    }
  }
  return true;
}

std::string logical_type_string(const LogicalType &type);

std::string field_type_string(const LogicalField &field) {
  if (!field.type)
    return "null";
  return logical_type_string(*field.type);
}

std::string logical_type_string(const LogicalType &type) {
  switch (type.kind) {
  case LogicalKind::kNull:
    return "null";
  case LogicalKind::kBool:
    return "bool";
  case LogicalKind::kInt64:
    return "int64";
  case LogicalKind::kFloat64:
    return "double";
  case LogicalKind::kUtf8:
    return "string";
  case LogicalKind::kTimestampNs:
    return "timestamp[ns]";
  case LogicalKind::kDate32:
    return "date32[day]";
  case LogicalKind::kTime32s:
    return "time32[s]";
  case LogicalKind::kList:
    return "list<item: " +
           (type.value ? logical_type_string(*type.value)
                       : std::string("null")) +
           ">";
  case LogicalKind::kStruct: {
    std::string out = "struct<";
    for (std::size_t i = 0; i < type.fields.size(); ++i) {
      if (i != 0)
        out += ", ";
      out += type.fields[i].name;
      out += ": ";
      out += field_type_string(type.fields[i]);
    }
    out += ">";
    return out;
  }
  }
  return "string";
}

} // namespace sanitize::schema_registry_internal
