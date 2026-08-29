// Provides schema-dominant packet estimates for wide fixed-cost CSV output.
// The helpers bound parallel text encoding memory while committing prepared
// fragments in source order.

#pragma once

#include "internal/output/text_output_estimator.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace sanitize::internal::text_output_estimator {

/// Reports whether schema metadata completely bounds a scalar's CSV encoding
/// cost.
[[nodiscard]] inline bool
fixed_cost_csv_scalar_kind(jsonl::JsonlKind kind) noexcept {
  return fixed_cost_jsonl_scalar_kind(kind);
}

/// Returns the schema-derived maximum CSV bytes for one fixed-cost scalar.
[[nodiscard]] inline std::int64_t
fixed_csv_scalar_output_upper_bound(const jsonl::JsonlField &field,
                                    std::int64_t cap) noexcept {
  // Direct numeric/bool CSV cells reuse the JSON token renderer without a
  // quoting pass. Other fixed-cost logical scalars may be CSV-quoted after
  // JSON rendering, so reserve the same strict factor used by the canonical
  // cell estimator.
  using Kind = jsonl::JsonlKind;
  switch (field.kind) {
  case Kind::kBool:
  case Kind::kInt8:
  case Kind::kUInt8:
  case Kind::kInt16:
  case Kind::kUInt16:
  case Kind::kInt32:
  case Kind::kUInt32:
  case Kind::kInt64:
  case Kind::kUInt64:
  case Kind::kFloat16:
  case Kind::kFloat32:
  case Kind::kFloat64:
    return fixed_jsonl_scalar_output_upper_bound(field, cap);
  default:
    return multiply_capped(fixed_jsonl_scalar_output_upper_bound(field, cap), 2,
                           cap);
  }
}

struct CsvFixedEstimatePlan final {
  std::int64_t fixed_base_bytes = 0;
  std::vector<std::size_t> dynamic_fields;
  std::size_t fixed_fields = 0;
  bool eligible = false;
};

/// Builds a hybrid estimate plan for wide schemas with a bounded dynamic-field
/// tail.
[[nodiscard]] inline CsvFixedEstimatePlan
make_csv_fixed_estimate_plan(const jsonl::JsonlField &root,
                             std::size_t minimum_fixed_fields = 24) {
  CsvFixedEstimatePlan plan;
  if (root.kind != jsonl::JsonlKind::kStruct) {
    return plan;
  }
  const auto cap = std::numeric_limits<std::int64_t>::max();
  plan.fixed_base_bytes = 1; // trailing newline
  plan.dynamic_fields.reserve(std::min<std::size_t>(root.children.size(), 8));
  for (std::size_t index = 0; index < root.children.size(); ++index) {
    plan.fixed_base_bytes =
        add_capped(plan.fixed_base_bytes, index == 0 ? 0 : 1, cap); // delimiter
    const auto &field = root.children[index];
    if (fixed_cost_csv_scalar_kind(field.kind)) {
      plan.fixed_base_bytes =
          add_capped(plan.fixed_base_bytes,
                     fixed_csv_scalar_output_upper_bound(field, cap), cap);
      ++plan.fixed_fields;
    } else {
      plan.dynamic_fields.push_back(index);
    }
  }
  const auto maximum_dynamic = std::max<std::size_t>(
      4, std::max<std::size_t>(1, root.children.size() / 8));
  plan.eligible = plan.fixed_fields >= minimum_fixed_fields &&
                  plan.dynamic_fields.size() <= maximum_dynamic;
  return plan;
}

/// Estimates a row by reusing the schema-only contribution of fixed columns and
/// inspecting only the bounded variable tail.
/// Nulls can only reduce fixed output bytes, so the plan remains conservative
/// for every batch. A fully fixed schema is O(1) per row.
/// The public outputs with four metadata fields are O(4), not O(N).
[[nodiscard]] inline std::int64_t estimate_csv_row_bytes_from_plan(
    const CsvFixedEstimatePlan &plan, const jsonl::JsonlField &root,
    const ArrowArray &array, std::int64_t row, std::int64_t cap) noexcept {
  if (!plan.eligible || cap <= 1 ||
      array.n_children != static_cast<std::int64_t>(root.children.size()) ||
      (!root.children.empty() && !array.children)) {
    return estimate_csv_row_bytes(root, array, row, cap);
  }
  std::int64_t total = std::min(plan.fixed_base_bytes, cap);
  for (const auto index : plan.dynamic_fields) {
    if (total >= cap || index >= root.children.size() ||
        index >= static_cast<std::size_t>(array.n_children) ||
        !array.children[index]) {
      return cap;
    }
    total = add_capped(total,
                       estimate_csv_cell_bytes(root.children[index],
                                               *array.children[index],
                                               array.offset + row, cap - total),
                       cap);
  }
  return multiply_capped(total, 2, cap);
}

} // namespace sanitize::internal::text_output_estimator
