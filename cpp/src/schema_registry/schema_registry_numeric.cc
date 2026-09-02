// Normalizes durable integer and float families in schema-registry trees.
// Recursive promotion prevents repeated numeric drift while retaining container
// shape and nonnumeric field semantics.

#include "schema_registry/schema_registry_internal.hh"

#include "internal/planning/variant_field_names.hh"
#include "internal/string_lookup.hh"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace sanitize {
namespace {

/// Returns whether a field stores a scalar signed integer.
bool is_scalar_integer_type(const LogicalField &field) noexcept {
  return field.type && field.type->kind == LogicalKind::kInt64;
}

/// Returns whether a field stores a scalar double-precision value.
bool is_scalar_float_type(const LogicalField &field) noexcept {
  return field.type && field.type->kind == LogicalKind::kFloat64;
}

/// Returns whether a field belongs to the promotable integer/float family.
bool is_scalar_numeric_type(const LogicalField &field) noexcept {
  return is_scalar_integer_type(field) || is_scalar_float_type(field);
}

/// Returns the borrowed base name used to group one version family.
std::string_view family_base_name(const LogicalField &field) noexcept {
  return internal::variant_family_base(field.name);
}

/// Rewrites a generated version suffix only when its semantic type changed.
void canonicalize_versioned_name_for_type(LogicalField &field) {
  const auto parsed = internal::parse_versioned_field_name(field.name);
  if (!parsed || !field.type)
    return;
  const std::string semantic_type =
      schema_registry_internal::variant_semantic_type(*field.type);
  if (parsed->semantic_type == semantic_type)
    return;
  field.name = internal::make_versioned_field_name(
      parsed->base, parsed->version, semantic_type);
}

struct NumericFamilyPlan {
  bool has_float = false;
  std::optional<std::size_t> first_float_position;
  std::optional<std::size_t> unversioned_numeric_position;

  /// Returns the one numeric field retained when a family contains floats.
  [[nodiscard]] std::optional<std::size_t> keep_position() const noexcept {
    if (!has_float)
      return std::nullopt;
    return unversioned_numeric_position ? unversioned_numeric_position
                                        : first_float_position;
  }
};

/// Normalizes integer/float families nested below one logical type.
void normalize_integer_float_type(LogicalType &type);

/// Collapses promotable numeric siblings while preserving field order.
void normalize_integer_float_fields(std::vector<LogicalField> &fields) {
  for (auto &field : fields) {
    if (field.type)
      normalize_integer_float_type(*field.type);
  }

  std::vector<NumericFamilyPlan> plans;
  plans.reserve(fields.size());
  std::vector<std::size_t> plan_by_field(fields.size());
  {
    internal::BorrowedStringLookupMap<std::size_t> plan_index_by_family;
    plan_index_by_family.reserve(fields.size());
    for (std::size_t i = 0; i < fields.size(); ++i) {
      const LogicalField &field = fields[i];
      const std::string_view family_base = family_base_name(field);
      auto [plan_it, inserted] =
          plan_index_by_family.try_emplace(family_base, plans.size());
      if (inserted)
        plans.emplace_back();
      plan_by_field[i] = plan_it->second;

      NumericFamilyPlan &plan = plans[plan_it->second];
      if (is_scalar_float_type(field)) {
        plan.has_float = true;
        if (!plan.first_float_position)
          plan.first_float_position = i;
      }
      if (!plan.unversioned_numeric_position && family_base == field.name &&
          is_scalar_numeric_type(field)) {
        plan.unversioned_numeric_position = i;
      }
    }
  }

  std::vector<LogicalField> out;
  out.reserve(fields.size());
  internal::BorrowedStringLookupMap<std::size_t> emitted_by_name;
  emitted_by_name.reserve(fields.size());
  for (std::size_t i = 0; i < fields.size(); ++i) {
    LogicalField field = std::move(fields[i]);
    const auto keep_position = plans[plan_by_field[i]].keep_position();
    if (keep_position && is_scalar_numeric_type(field)) {
      if (i != *keep_position)
        continue;
      if (is_scalar_integer_type(field)) {
        field.type = std::make_unique<LogicalType>(LogicalType::Float64());
      }
    }

    canonicalize_versioned_name_for_type(field);
    if (auto duplicate = emitted_by_name.find(field.name);
        duplicate != emitted_by_name.end()) {
      LogicalField &previous = out[duplicate->second];
      if (previous.type && field.type &&
          schema_registry_internal::logical_type_equal(*previous.type,
                                                       *field.type)) {
        continue;
      }
    }
    out.push_back(std::move(field));
    emitted_by_name.insert_or_assign(std::string_view(out.back().name),
                                     out.size() - 1U);
  }
  fields = std::move(out);
}

/// Recursively normalizes numeric families in struct and list children.
void normalize_integer_float_type(LogicalType &type) {
  if (type.kind == LogicalKind::kStruct) {
    normalize_integer_float_fields(type.fields);
  } else if (type.kind == LogicalKind::kList && type.value) {
    normalize_integer_float_type(*type.value);
  }
}

} // namespace

namespace schema_registry_internal {

// Normalizes integer/float version families across a registry schema.
void normalize_integer_float_schema(LogicalSchema &schema) {
  normalize_integer_float_fields(schema.fields);
}

} // namespace schema_registry_internal
} // namespace sanitize
