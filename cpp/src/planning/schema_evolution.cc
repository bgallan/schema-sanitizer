// Reconciles inferred schemas with optional schema-contract contracts.

#include "internal/planning/schema_evolution.hh"

#include <algorithm>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {

namespace {

using FieldMap =
    std::unordered_map<std::string_view, const sanitize::LogicalField *>;

// Finds a field by name in a logical field list.
static const sanitize::LogicalField *
find_field(const std::vector<sanitize::LogicalField> &fields,
           std::string_view name) {
  for (const auto &f : fields) {
    if (f.name == name)
      return &f;
  }
  return nullptr;
}

// Checks that an inferred schema satisfies a strict schema-contract contract.
static sanitize::Status
check_strict_compatible(const sanitize::LogicalSchema &base,
                        const sanitize::LogicalSchema &inferred) {
  if (base.fields.empty()) {
    return sanitize::Status::Invalid(
        "Strict schema evolution requires a non-empty schema_contract");
  }

  // Inferred cannot contain extra root fields.
  for (const auto &f : inferred.fields) {
    if (!find_field(base.fields, f.name)) {
      return sanitize::Status::Invalid(
          "Strict schema evolution: observed extra field '" + f.name + "'");
    }
  }

  return sanitize::Status::OK();
}

// Appends inferred root fields that are absent from the schema contract.
static sanitize::LogicalSchema
merge_schema_additive(const sanitize::LogicalSchema &base,
                      const sanitize::LogicalSchema &inferred) {
  if (base.fields.empty())
    return inferred;
  sanitize::LogicalSchema out;
  out.fields.reserve(base.fields.size() + inferred.fields.size());
  out.fields.insert(out.fields.end(), base.fields.begin(), base.fields.end());
  std::unordered_set<std::string_view> base_names;
  base_names.reserve(base.fields.size());
  for (const auto &f : base.fields)
    base_names.insert(f.name);
  for (const auto &f : inferred.fields) {
    if (base_names.contains(f.name))
      continue;
    out.fields.push_back(f);
  }
  return out;
}

// Reorders nested logical types according to a schema-contract ordering policy.
static sanitize::LogicalType reorder_type(const sanitize::LogicalType &cur,
                                          const sanitize::LogicalType *base,
                                          FieldOrderPolicy order);

// Reorders a logical field while preserving its metadata.
static sanitize::LogicalField reorder_field(const sanitize::LogicalField &field,
                                            const sanitize::LogicalType *base,
                                            FieldOrderPolicy order) {
  sanitize::LogicalField out = field;
  if (field.type) {
    out.type = std::make_unique<sanitize::LogicalType>(
        reorder_type(*field.type, base, order));
  }
  return out;
}

// Builds a name-to-field lookup for current struct fields.
static FieldMap
build_field_map(const std::vector<sanitize::LogicalField> &cur_fields) {
  FieldMap cur_map;
  cur_map.reserve(cur_fields.size());
  for (const auto &f : cur_fields)
    cur_map.emplace(f.name, &f);
  return cur_map;
}

// Returns sorted current field names.
static std::vector<std::string_view>
sorted_field_names(const std::vector<sanitize::LogicalField> &fields) {
  std::vector<std::string_view> names;
  names.reserve(fields.size());
  for (const auto &field : fields)
    names.push_back(field.name);
  std::ranges::sort(names);
  return names;
}

// Returns the matching base field type for a struct field name.
static const sanitize::LogicalType *
base_field_type(const sanitize::LogicalType *base_struct,
                std::string_view name) {
  if (!base_struct || base_struct->kind != sanitize::LogicalKind::kStruct) {
    return nullptr;
  }
  const auto *base_field = find_field(base_struct->fields, name);
  if (!base_field || !base_field->type) {
    return nullptr;
  }
  return base_field->type.get();
}

// Appends fields present in the schema contract, preserving base order.
static void append_base_ordered_fields(
    std::vector<sanitize::LogicalField> *out, const FieldMap &cur_map,
    const sanitize::LogicalType &base_struct,
    std::unordered_set<std::string_view> *used, FieldOrderPolicy order) {
  for (const auto &base_field : base_struct.fields) {
    auto it = cur_map.find(base_field.name);
    if (it == cur_map.end())
      continue;
    const auto *field = it->second;
    used->insert(field->name);
    out->push_back(reorder_field(
        *field, base_field.type ? base_field.type.get() : nullptr, order));
  }
}

// Appends fields not already used, sorted by name.
static void append_unused_sorted_fields(
    std::vector<sanitize::LogicalField> *out,
    const std::vector<sanitize::LogicalField> &cur_fields,
    const FieldMap &cur_map, const std::unordered_set<std::string_view> &used,
    FieldOrderPolicy order) {
  std::vector<std::string_view> extra;
  extra.reserve(cur_fields.size());
  for (const auto &field : cur_fields) {
    if (!used.contains(field.name))
      extra.push_back(field.name);
  }
  std::ranges::sort(extra);
  for (const auto &name : extra)
    out->push_back(reorder_field(*cur_map.at(name), nullptr, order));
}

// Appends all current fields sorted by name.
static void
append_sorted_fields(std::vector<sanitize::LogicalField> *out,
                     const std::vector<sanitize::LogicalField> &cur_fields,
                     const FieldMap &cur_map,
                     const sanitize::LogicalType *base_struct,
                     FieldOrderPolicy order) {
  const std::vector<std::string_view> names = sorted_field_names(cur_fields);
  for (const auto &name : names) {
    out->push_back(reorder_field(*cur_map.at(name),
                                 base_field_type(base_struct, name), order));
  }
}

// Reorders struct fields.
static std::vector<sanitize::LogicalField>
reorder_struct_fields(const std::vector<sanitize::LogicalField> &cur_fields,
                      const sanitize::LogicalType *base_struct,
                      FieldOrderPolicy order) {
  const FieldMap cur_map = build_field_map(cur_fields);
  std::vector<sanitize::LogicalField> out;
  out.reserve(cur_fields.size());

  if (order == FieldOrderPolicy::kSchemaContractFirst && base_struct &&
      base_struct->kind == sanitize::LogicalKind::kStruct) {
    std::unordered_set<std::string_view> used;
    used.reserve(cur_fields.size());
    append_base_ordered_fields(&out, cur_map, *base_struct, &used, order);
    append_unused_sorted_fields(&out, cur_fields, cur_map, used, order);
    return out;
  }

  if (order == FieldOrderPolicy::kSchemaContractFirst) {
    for (const auto &field : cur_fields)
      out.push_back(reorder_field(field, nullptr, order));
    return out;
  }

  append_sorted_fields(&out, cur_fields, cur_map, base_struct, order);
  return out;
}

static sanitize::LogicalType reorder_type(const sanitize::LogicalType &cur,
                                          const sanitize::LogicalType *base,
                                          FieldOrderPolicy order) {
  if (cur.kind == sanitize::LogicalKind::kStruct) {
    sanitize::LogicalType out(sanitize::LogicalKind::kStruct);
    out.fields = reorder_struct_fields(cur.fields, base, order);
    return out;
  }
  if (cur.kind == sanitize::LogicalKind::kList) {
    sanitize::LogicalType out(sanitize::LogicalKind::kList);
    const sanitize::LogicalType *base_v = nullptr;
    if (base && base->kind == sanitize::LogicalKind::kList && base->value)
      base_v = base->value.get();
    if (cur.value)
      out.value = std::make_unique<sanitize::LogicalType>(
          reorder_type(*cur.value, base_v, order));
    return out;
  }
  return cur;
}

} // namespace

sanitize::LogicalSchema
reorder_schema_fields(const sanitize::LogicalSchema &schema,
                      const sanitize::LogicalSchema *base,
                      FieldOrderPolicy field_order) {
  if (schema.fields.empty())
    return schema;

  sanitize::LogicalType base_root(sanitize::LogicalKind::kStruct);
  const sanitize::LogicalType *base_root_ptr = nullptr;
  if (base && !base->fields.empty()) {
    base_root.fields = base->fields;
    base_root_ptr = &base_root;
  }

  sanitize::LogicalSchema out = schema;
  out.fields = reorder_struct_fields(out.fields, base_root_ptr, field_order);
  return out;
}

sanitize::Result<sanitize::LogicalSchema>
evolve_schema(const sanitize::LogicalSchema &base,
              const sanitize::LogicalSchema &inferred, SchemaEvolutionMode mode,
              FieldOrderPolicy field_order) {
  sanitize::LogicalSchema merged;

  switch (mode) {
  case SchemaEvolutionMode::kStrict: {
    SAN_RETURN_NOT_OK(check_strict_compatible(base, inferred));
    merged = base;
    break;
  }
  case SchemaEvolutionMode::kAdditive:
  default:
    merged = merge_schema_additive(base, inferred);
    break;
  }

  return reorder_schema_fields(merged, base.fields.empty() ? nullptr : &base,
                               field_order);
}

} // namespace sanitize::internal
