// Declares snapshotted object-field lookup used by STRUCT conversion.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/core/value_view.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

struct ObjectFieldSnapshot {
  std::vector<sanitize::FieldRef> fields;
  std::vector<int32_t> child_field_indices;

  // Materializes one object value and pre-resolves source keys to child fields.
  [[nodiscard]] Status build(ValueView object, const ColumnPlan &plan,
                             const sanitize::PreparedOptions &opts);

  // Finds the value for one planned child after build() has completed.
  [[nodiscard]] bool find(std::size_t child_index, std::string_view key,
                          const sanitize::PreparedOptions &opts,
                          ValueView *out) const;
};

} // namespace sanitize::internal
