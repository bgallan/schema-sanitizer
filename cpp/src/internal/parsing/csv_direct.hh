// Declares direct CSV materialization helpers.

#pragma once

#include <cstdint>
#include <vector>

namespace sanitize::internal {

// Metadata needed to materialize CSV rows directly without building FieldRefs.
//
// For each plan column index, maps to the CSV cell index (or -1 if not
// present).
struct CsvDirectContext {
  char delimiter = ',';
  std::vector<int32_t> col_to_csv; // size == plan.columns.size()
};

} // namespace sanitize::internal
