// Declares direct CSV materialization helpers.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace sanitize::internal {

// Metadata needed to materialize CSV rows directly without building FieldRefs.
//
// For each plan column index, maps to the CSV cell index (or -1 if not
// present).
struct CsvDirectContext {
  char delimiter = ',';
  char escape_char = '\0';
  std::size_t max_field_bytes = 64U * 1024U * 1024U;
  std::size_t max_decoded_record_bytes = 256U * 1024U * 1024U;
  std::vector<int32_t> col_to_csv; // size == plan.columns.size()
};

} // namespace sanitize::internal
