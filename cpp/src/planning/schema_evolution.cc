// Reconciles inferred schemas with contracts and applies recursive field order.

#include "internal/planning/schema_evolution.hh"

#include <algorithm>
#include <memory>
#include <string_view>
#include <vector>

#include "internal/string_lookup.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {
namespace {
using FieldMap = BorrowedStringLookupMap<const sanitize::LogicalField *>;

FieldMap build_field_map(const std::vector<sanitize::LogicalField> &fields) {
  FieldMap out;
  out.reserve(fields.size());
  for (const auto &field : fields) {
    out.emplace(field.name, &field);
  }
  return out;
}

sanitize::Status
check_strict_compatible(const sanitize::LogicalSchema &base,
                        const sanitize::LogicalSchema &inferred) {
  if (base.fields.empty()) {
    return sanitize::Status::Invalid(
        "Strict schema evolution requires a non-empty schema_contract");
  }
  const FieldMap base_fields = build_field_map(base.fields);
  for (const auto &field : inferred.fields) {
    if (!base_fields.contains(field.name)) {
      return sanitize::Status::Invalid(
          "Strict schema evolution: observed extra field '" + field.name + "'");
    }
  }
  return sanitize::Status::OK();
}

sanitize::LogicalType
merge_type_additive(const sanitize::LogicalType &base,
                    const sanitize::LogicalType &inferred);

sanitize::LogicalField
merge_field_additive(const sanitize::LogicalField &base,
                     const sanitize::LogicalField &inferred) {
  sanitize::LogicalField out = base;
  if (base.type && inferred.type) {
    out.type = std::make_unique<sanitize::LogicalType>(
        merge_type_additive(*base.type, *inferred.type));
  }
  return out;
}

std::vector<sanitize::LogicalField> merge_fields_additive(
    const std::vector<sanitize::LogicalField> &base_fields,
    const std::vector<sanitize::LogicalField> &inferred_fields) {
  const FieldMap base_by_name = build_field_map(base_fields);
  const FieldMap inferred_by_name = build_field_map(inferred_fields);
  std::vector<sanitize::LogicalField> out;
  out.reserve(base_fields.size() + inferred_fields.size());
  for (const auto &base_field : base_fields) {
    const auto inferred = inferred_by_name.find(base_field.name);
    if (inferred == inferred_by_name.end()) {
      out.push_back(base_field);
    } else {
      out.push_back(merge_field_additive(base_field, *inferred->second));
    }
  }
  for (const auto &field : inferred_fields) {
    if (!base_by_name.contains(field.name)) {
      out.push_back(field);
    }
  }
  return out;
}

sanitize::LogicalType
merge_type_additive(const sanitize::LogicalType &base,
                    const sanitize::LogicalType &inferred) {
  if (base.kind == sanitize::LogicalKind::kStruct &&
      inferred.kind == sanitize::LogicalKind::kStruct) {
    sanitize::LogicalType out(sanitize::LogicalKind::kStruct);
    out.fields = merge_fields_additive(base.fields, inferred.fields);
    return out;
  }
  if (base.kind == sanitize::LogicalKind::kList &&
      inferred.kind == sanitize::LogicalKind::kList && base.value &&
      inferred.value) {
    sanitize::LogicalType out(sanitize::LogicalKind::kList);
    out.value = std::make_unique<sanitize::LogicalType>(
        merge_type_additive(*base.value, *inferred.value));
    return out;
  }
  return base;
}

sanitize::LogicalSchema
merge_schema_additive(const sanitize::LogicalSchema &base,
                      const sanitize::LogicalSchema &inferred) {
  if (base.fields.empty()) {
    return inferred;
  }
  sanitize::LogicalSchema out;
  out.fields = merge_fields_additive(base.fields, inferred.fields);
  return out;
}

sanitize::LogicalType reorder_type(const sanitize::LogicalType &current,
                                   const sanitize::LogicalType *base,
                                   FieldOrderPolicy order);

sanitize::LogicalField reorder_field(const sanitize::LogicalField &field,
                                     const sanitize::LogicalType *base,
                                     FieldOrderPolicy order) {
  sanitize::LogicalField out = field;
  if (field.type) {
    out.type = std::make_unique<sanitize::LogicalType>(
        reorder_type(*field.type, base, order));
  }
  return out;
}

std::vector<std::string_view>
sorted_field_names(const std::vector<sanitize::LogicalField> &fields) {
  std::vector<std::string_view> names;
  names.reserve(fields.size());
  for (const auto &field : fields) {
    names.push_back(field.name);
  }
  std::ranges::sort(names);
  return names;
}

const sanitize::LogicalType *
field_type_or_none(const FieldMap &fields, std::string_view name) noexcept {
  const auto found = fields.find(name);
  if (found == fields.end() || !found->second->type) {
    return nullptr;
  }
  return found->second->type.get();
}

void append_base_ordered_fields(std::vector<sanitize::LogicalField> *out,
                                const FieldMap &current_by_name,
                                const sanitize::LogicalType &base_struct,
                                BorrowedStringLookupSet *used,
                                FieldOrderPolicy order) {
  for (const auto &base_field : base_struct.fields) {
    const auto current = current_by_name.find(base_field.name);
    if (current == current_by_name.end()) {
      continue;
    }
    const auto *field = current->second;
    used->insert(field->name);
    out->push_back(reorder_field(
        *field, base_field.type ? base_field.type.get() : nullptr, order));
  }
}

void append_unused_sorted_fields(
    std::vector<sanitize::LogicalField> *out,
    const std::vector<sanitize::LogicalField> &current_fields,
    const FieldMap &current_by_name, const BorrowedStringLookupSet &used,
    FieldOrderPolicy order) {
  std::vector<std::string_view> extra;
  extra.reserve(current_fields.size() - used.size());
  for (const auto &field : current_fields) {
    if (!used.contains(field.name)) {
      extra.push_back(field.name);
    }
  }
  std::ranges::sort(extra);
  for (const auto name : extra) {
    out->push_back(reorder_field(*current_by_name.at(name), nullptr, order));
  }
}

void append_sorted_fields(
    std::vector<sanitize::LogicalField> *out,
    const std::vector<sanitize::LogicalField> &current_fields,
    const FieldMap &current_by_name, const FieldMap &base_by_name,
    FieldOrderPolicy order) {
  for (const auto name : sorted_field_names(current_fields)) {
    out->push_back(reorder_field(*current_by_name.at(name),
                                 field_type_or_none(base_by_name, name),
                                 order));
  }
}

std::vector<sanitize::LogicalField>
reorder_struct_fields(const std::vector<sanitize::LogicalField> &current_fields,
                      const sanitize::LogicalType *base_struct,
                      FieldOrderPolicy order) {
  std::vector<sanitize::LogicalField> out;
  out.reserve(current_fields.size());
  const bool has_base_struct =
      base_struct && base_struct->kind == sanitize::LogicalKind::kStruct;

  if (order == FieldOrderPolicy::kSchemaContractFirst) {
    if (!has_base_struct) {
      for (const auto &field : current_fields) {
        out.push_back(reorder_field(field, nullptr, order));
      }
      return out;
    }
    const FieldMap current_by_name = build_field_map(current_fields);
    BorrowedStringLookupSet used;
    used.reserve(current_fields.size());
    append_base_ordered_fields(&out, current_by_name, *base_struct, &used,
                               order);
    append_unused_sorted_fields(&out, current_fields, current_by_name, used,
                                order);
    return out;
  }

  const FieldMap current_by_name = build_field_map(current_fields);
  const FieldMap base_by_name =
      has_base_struct ? build_field_map(base_struct->fields) : FieldMap{};
  append_sorted_fields(&out, current_fields, current_by_name, base_by_name,
                       order);
  return out;
}

sanitize::LogicalType reorder_type(const sanitize::LogicalType &current,
                                   const sanitize::LogicalType *base,
                                   FieldOrderPolicy order) {
  if (current.kind == sanitize::LogicalKind::kStruct) {
    sanitize::LogicalType out(sanitize::LogicalKind::kStruct);
    out.fields = reorder_struct_fields(current.fields, base, order);
    return out;
  }
  if (current.kind == sanitize::LogicalKind::kList) {
    sanitize::LogicalType out(sanitize::LogicalKind::kList);
    const sanitize::LogicalType *base_value = nullptr;
    if (base && base->kind == sanitize::LogicalKind::kList && base->value) {
      base_value = base->value.get();
    }
    if (current.value) {
      out.value = std::make_unique<sanitize::LogicalType>(
          reorder_type(*current.value, base_value, order));
    }
    return out;
  }
  return current;
}
} // namespace

sanitize::LogicalSchema
reorder_schema_fields(const sanitize::LogicalSchema &schema,
                      const sanitize::LogicalSchema *base,
                      FieldOrderPolicy field_order) {
  if (schema.fields.empty()) {
    return schema;
  }
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
  case SchemaEvolutionMode::kStrict:
    SAN_RETURN_NOT_OK(check_strict_compatible(base, inferred));
    if (field_order == FieldOrderPolicy::kSchemaContractFirst) {
      return base;
    }
    merged = base;
    break;
  case SchemaEvolutionMode::kAdditive:
  default:
    merged = merge_schema_additive(base, inferred);
    break;
  }
  return reorder_schema_fields(merged, base.fields.empty() ? nullptr : &base,
                               field_order);
}
} // namespace sanitize::internal
