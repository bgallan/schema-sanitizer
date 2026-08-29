// Declares helpers for plan-ordered JSON row materialization. The pipeline
// preserves source offsets and ownership while enforcing plan order and memory
// bounds.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string_view>

#include "internal/parsing/flat_row_batch.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"

namespace sanitize {
struct CompiledPlan;
}

namespace sanitize::internal {
class JsonOnDemandDoc;

struct PlanOrderedRowScratch {

  /// Creates pool-backed seen-field scratch space for plan-ordered row
  /// emission.
  explicit PlanOrderedRowScratch(std::pmr::memory_resource *resource)
      : planned_seen(resource) {}

  std::pmr::vector<std::uint8_t> planned_seen;
};

/// Discards speculative fields and marks the current row for raw
/// materialization.
inline void rewrite_current_row_as_raw(FlatRowBatch *batch) noexcept {
  if (!batch) {
    return;
  }
  batch->truncate_current_row_fields();
  batch->set_current_row_flags(std::to_underlying(RowFlags::kRawOnly));
}

sanitize::Status
append_plan_ordered_json_row(JsonOnDemandDoc *document, FlatRowBatch *batch,
                             PlanOrderedRowScratch *scratch,
                             const CompiledPlan &plan,
                             std::string_view field_name_policy,
                             std::string_view raw, std::size_t base_offset);

} // namespace sanitize::internal
