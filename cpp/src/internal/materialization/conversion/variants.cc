// Implements version-family value routing for row materialization.

#include "internal/materialization/conversion/variants.hh"

#include "internal/parsing/string_scalar.hh"

namespace sanitize::internal {
namespace {

// Scores how directly one scalar value fits a planned scalar type.
int scalar_compatibility_score(const sanitize::ColumnPlan &plan,
                               sanitize::ValueView value,
                               const sanitize::PreparedOptions &opts) noexcept {
  using sanitize::LogicalKind;
  switch (plan.logical_type.kind) {
  case LogicalKind::kBool:
    if (value.is_bool())
      return 400;
    if (value.is_string()) {
      bool parsed = false;
      return coerce_bool_from_string(value.as_string_view(), opts, &parsed)
                 ? 350
                 : 50;
    }
    return 50;
  case LogicalKind::kInt64:
    if (value.is_int())
      return 400;
    if (value.is_string()) {
      int64_t parsed = 0;
      return coerce_int64_from_string(value.as_string_view(), opts, &parsed)
                 ? 350
                 : 50;
    }
    return 50;
  case LogicalKind::kFloat64:
    if (value.is_float())
      return 400;
    if (value.is_int())
      return 375;
    if (value.is_string()) {
      double parsed = 0.0;
      if (!coerce_float64_from_string(value.as_string_view(), opts, &parsed))
        return 50;
      int64_t integer = 0;
      if (coerce_int64_from_string(value.as_string_view(), opts, &integer))
        return 340;
      return 350;
    }
    return 50;
  case LogicalKind::kTimestampNs:
    if (value.is_int())
      return 375;
    if (value.is_string()) {
      int64_t parsed = 0;
      return coerce_timestamp_ns_from_string(value.as_string_view(), opts,
                                             &parsed)
                 ? 350
                 : 50;
    }
    return 50;
  case LogicalKind::kDate32:
    if (value.is_int())
      return 375;
    if (value.is_string()) {
      int32_t parsed = 0;
      return coerce_date_days_from_string(value.as_string_view(), opts, &parsed)
                 ? 350
                 : 50;
    }
    return 50;
  case LogicalKind::kTime32s:
    if (value.is_int())
      return 375;
    if (value.is_string()) {
      int32_t parsed = 0;
      return coerce_time_seconds_from_string(value.as_string_view(), opts,
                                             &parsed)
                 ? 350
                 : 50;
    }
    return 50;
  case LogicalKind::kUtf8:
    return value.is_string() ? 300 : 150;
  case LogicalKind::kNull:
    return value.is_null() ? 100 : 0;
  case LogicalKind::kStruct:
  case LogicalKind::kList:
    return 0;
  }
  return 0;
}

// Chooses one deterministic destination from a field version family.
template <typename GetSibling>
const sanitize::ColumnPlan *
preferred_variant(const sanitize::ColumnPlan &column, sanitize::ValueView value,
                  const sanitize::PreparedOptions &opts,
                  GetSibling get_sibling) noexcept {
  const sanitize::ColumnPlan *best = nullptr;
  int best_score = -1;
  for (const int32_t sibling_index : column.variant_sibling_indices) {
    const sanitize::ColumnPlan *sibling = get_sibling(sibling_index);
    if (!sibling)
      continue;
    const int score = variant_compatibility_score(*sibling, value, opts);
    // Later versions win exact ties, matching registry current-version
    // selection and giving every sibling the same deterministic destination.
    if (score >= best_score) {
      best = sibling;
      best_score = score;
    }
  }
  return best ? best : &column;
}

} // namespace

int variant_compatibility_score(
    const sanitize::ColumnPlan &plan, sanitize::ValueView value,
    const sanitize::PreparedOptions &opts) noexcept {
  if (value.is_null()) {
    return 0;
  }
  switch (plan.logical_type.kind) {
  case sanitize::LogicalKind::kList:
    // Lists are the widest variant: arrays fit directly, and single values can
    // be wrapped into one list element by convert_list.
    return value.is_array() ? 400 : 350;
  case sanitize::LogicalKind::kStruct:
    return value.is_object() ? 300 : 50;
  default:
    if (value.is_array() || value.is_object())
      return 25;
    return scalar_compatibility_score(plan, value, opts);
  }
}

const sanitize::ColumnPlan *preferred_root_variant_sibling(
    const sanitize::CompiledPlan &plan, const sanitize::ColumnPlan &column,
    sanitize::ValueView value, const sanitize::PreparedOptions &opts) noexcept {
  return preferred_variant(column, value, opts, [&](int32_t sibling_index) {
    if (sibling_index < 0 ||
        static_cast<std::size_t>(sibling_index) >= plan.columns.size()) {
      return static_cast<const sanitize::ColumnPlan *>(nullptr);
    }
    return &plan.columns[static_cast<std::size_t>(sibling_index)];
  });
}

const sanitize::ColumnPlan *preferred_child_variant_sibling(
    const sanitize::ColumnPlan &parent, const sanitize::ColumnPlan &child,
    sanitize::ValueView value, const sanitize::PreparedOptions &opts) noexcept {
  return preferred_variant(child, value, opts, [&](int32_t sibling_index) {
    if (sibling_index < 0 ||
        static_cast<std::size_t>(sibling_index) >= parent.children.size()) {
      return static_cast<const sanitize::ColumnPlan *>(nullptr);
    }
    return &parent.children[static_cast<std::size_t>(sibling_index)];
  });
}

} // namespace sanitize::internal
