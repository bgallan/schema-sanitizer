// Implements snapshotted object-field lookup for wide STRUCT conversion.

#include "internal/materialization/conversion/object_fields.hh"

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "internal/planning/field_name_sanitizer.hh"
#include "internal/planning/planned_name_matcher.hh"
#include "internal/planning/variant_field_names.hh"
#include "sanitize/core/status.hh"
#include "sanitize/core/value_view.hh"

namespace sanitize::internal {
namespace {

/// Returns whether one dirty source key can feed a planned struct child.
bool field_matches_child(std::string_view key, const ColumnPlan &child,
                         const sanitize::PreparedOptions &opts) {
  bool matches = field_name_matches_output(key, child.name, opts);
  if (!matches && child.has_variant_sibling) {
    const std::string_view family_base = variant_family_base(child.name);
    matches = family_base != child.name &&
              field_name_matches_output(key, family_base, opts);
  }
  if (!matches) {
    const std::string_view original = unflattened_output_name(child.name);
    matches =
        !original.empty() && field_name_matches_output(key, original, opts);
  }
  return matches;
}

/// Assigns a source field index to one child when that child is still unmapped.
void assign_child_field(std::vector<int32_t> *indices, std::size_t child_i,
                        std::size_t field_i) {
  if (!indices || child_i >= indices->size() || (*indices)[child_i] >= 0) {
    return;
  }
  (*indices)[child_i] = static_cast<int32_t>(field_i);
}

} // namespace

Status ObjectFieldSnapshot::build(ValueView object, const ColumnPlan &plan,
                                  const sanitize::PreparedOptions &opts) {
  fields.clear();
  child_field_indices.assign(plan.children.size(), -1);
  SAN_RETURN_NOT_OK(object.for_each_object_field(
      [&](std::string_view key, uint64_t key_hash, ValueView value) -> Status {
        fields.push_back(sanitize::FieldRef{
            .key = key,
            .key_hash = key_hash,
            .value = value,
        });
        return Status::OK();
      }));

  if (plan.layout) {
    for (std::size_t field_i = 0; field_i < fields.size(); ++field_i) {
      const auto &field = fields[field_i];
      bool empty_container = false;
      SAN_RETURN_NOT_OK(field.value.container_is_empty(&empty_container));
      if (empty_container) {
        continue;
      }
      const auto *planned =
          find_planned_field(*plan.layout, field.key, field.key_hash, opts);
      if (!planned || planned->index < 0) {
        continue;
      }
      const auto child_i = static_cast<std::size_t>(planned->index);
      assign_child_field(&child_field_indices, child_i, field_i);
      if (child_i >= plan.children.size()) {
        continue;
      }
      for (int32_t sibling_index :
           plan.children[child_i].variant_sibling_indices) {
        if (sibling_index < 0) {
          continue;
        }
        const auto sibling_i = static_cast<std::size_t>(sibling_index);
        if (sibling_i < plan.children.size() &&
            field_matches_child(field.key, plan.children[sibling_i], opts)) {
          assign_child_field(&child_field_indices, sibling_i, field_i);
        }
      }
    }
    return Status::OK();
  }

  for (std::size_t field_i = 0; field_i < fields.size(); ++field_i) {
    const auto &field = fields[field_i];
    bool empty_container = false;
    SAN_RETURN_NOT_OK(field.value.container_is_empty(&empty_container));
    if (empty_container) {
      continue;
    }
    for (std::size_t child_i = 0; child_i < plan.children.size(); ++child_i) {
      if (child_field_indices[child_i] >= 0) {
        continue;
      }
      const auto &child = plan.children[child_i];
      if (field_matches_child(field.key, child, opts)) {
        child_field_indices[child_i] = static_cast<int32_t>(field_i);
      }
    }
  }
  return Status::OK();
}

bool ObjectFieldSnapshot::find(std::size_t child_index, std::string_view key,
                               const sanitize::PreparedOptions &opts,
                               ValueView *out) const {
  if (child_index < child_field_indices.size()) {
    const int32_t field_index = child_field_indices[child_index];
    if (field_index >= 0 &&
        static_cast<std::size_t>(field_index) < fields.size()) {
      if (out) {
        *out = fields[static_cast<std::size_t>(field_index)].value;
      }
      return true;
    }
    return false;
  }
  for (const auto &field : fields) {
    if (field_name_matches_output(field.key, key, opts)) {
      if (out) {
        *out = field.value;
      }
      return true;
    }
  }
  return false;
}

} // namespace sanitize::internal
