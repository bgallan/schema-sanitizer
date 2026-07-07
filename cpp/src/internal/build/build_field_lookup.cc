// Implements private row field lookup helpers for build conversion.

#include "internal/build/build_internal.hh"

#include "internal/core/value_view_util.hh"
#include "internal/planning/field_name_sanitizer.hh"
#include "internal/planning/planned_name_matcher.hh"
#include "internal/planning/variant_field_names.hh"

#include <string>
#include <string_view>

namespace sanitize::internal {
namespace {

/// Returns whether one dirty source key can feed a planned root column.
bool field_matches_column(std::string_view key,
                          const sanitize::ColumnPlan &column,
                          const sanitize::PreparedOptions &opts) {
  bool matches = field_name_matches_output(key, column.name, opts);
  if (!matches && column.has_variant_sibling) {
    const std::string_view family_base = variant_family_base(column.name);
    matches = family_base != column.name &&
              field_name_matches_output(key, family_base, opts);
  }
  if (!matches) {
    const std::string_view original = unflattened_output_name(column.name);
    matches =
        !original.empty() && field_name_matches_output(key, original, opts);
  }
  return matches;
}

/// Assigns a source field index to one column when that column is unmapped.
void assign_column_field(std::vector<int32_t> *indices, std::size_t column_i,
                         std::size_t field_i) {
  if (!indices || column_i >= indices->size() || (*indices)[column_i] >= 0) {
    return;
  }
  (*indices)[column_i] = static_cast<int32_t>(field_i);
}

} // namespace

std::string_view unflattened_name(std::string_view value) noexcept {
  return unflattened_output_name(value);
}

sanitize::Result<sanitize::ValueView>
FieldLookup::find(std::string_view key, const sanitize::PreparedOptions &opts,
                  bool *found) const {
  if (found)
    *found = false;
  if (!row || !row->fields)
    return sanitize::ValueView::Null();
  for (std::size_t i = 0; i < row->size; ++i) {
    const auto &field = row->fields[i];
    if (field_name_matches_output(field.key, key, opts)) {
      bool empty_container = false;
      SAN_RETURN_NOT_OK(
          value_view_container_is_empty(field.value, &empty_container));
      if (empty_container)
        continue;
      if (found)
        *found = true;
      return field.value;
    }
  }
  return sanitize::ValueView::Null();
}

sanitize::Result<bool>
FieldLookup::has_unplanned_field(const sanitize::StructLayout &layout,
                                 const sanitize::PreparedOptions &opts,
                                 std::string *name) const {
  if (!row || !row->fields)
    return false;
  for (std::size_t i = 0; i < row->size; ++i) {
    const auto &field = row->fields[i];
    if (matches_planned_field(layout, field.key, field.key_hash, opts))
      continue;
    bool empty_container = false;
    SAN_RETURN_NOT_OK(
        value_view_container_is_empty(field.value, &empty_container));
    if (empty_container)
      continue;
    if (name)
      name->assign(field.key.data(), field.key.size());
    return true;
  }
  return false;
}

sanitize::Status
RowFieldSnapshot::build(const sanitize::RowRef &input_row,
                        const sanitize::CompiledPlan &plan,
                        const sanitize::PreparedOptions &opts) {
  fields.clear();
  column_field_indices.assign(plan.columns.size(), -1);
  if (!input_row.fields) {
    return sanitize::Status::OK();
  }

  fields.reserve(input_row.size);
  for (std::size_t i = 0; i < input_row.size; ++i) {
    const auto &field = input_row.fields[i];
    bool empty_container = false;
    SAN_RETURN_NOT_OK(
        value_view_container_is_empty(field.value, &empty_container));
    if (empty_container) {
      continue;
    }
    const std::size_t field_i = fields.size();
    fields.push_back(field);

    const auto *planned =
        find_planned_field(plan.root_layout, field.key, field.key_hash, opts);
    if (!planned || planned->index < 0) {
      continue;
    }
    const auto column_i = static_cast<std::size_t>(planned->index);
    assign_column_field(&column_field_indices, column_i, field_i);
    if (column_i >= plan.columns.size()) {
      continue;
    }
    for (int32_t sibling_index :
         plan.columns[column_i].variant_sibling_indices) {
      if (sibling_index < 0) {
        continue;
      }
      const auto sibling_i = static_cast<std::size_t>(sibling_index);
      if (sibling_i < plan.columns.size() &&
          field_matches_column(field.key, plan.columns[sibling_i], opts)) {
        assign_column_field(&column_field_indices, sibling_i, field_i);
      }
    }
  }
  return sanitize::Status::OK();
}

bool RowFieldSnapshot::find(std::size_t column_index,
                            sanitize::ValueView *out) const {
  if (column_index >= column_field_indices.size()) {
    return false;
  }
  const int32_t field_index = column_field_indices[column_index];
  if (field_index < 0 ||
      static_cast<std::size_t>(field_index) >= fields.size()) {
    return false;
  }
  if (out) {
    *out = fields[static_cast<std::size_t>(field_index)].value;
  }
  return true;
}

} // namespace sanitize::internal
