// Declares shared batch sizing helpers for ingestion pipeline passes. The code
// converts validated rows into memory-accounted Arrow C Data batches for
// ordered ingestion.

#pragma once

#include "internal/memory/memory_budget.hh"

#include <algorithm>
#include <cstdint>
#include <limits>

namespace sanitize::internal {

constexpr int64_t kDefaultBatchRows = 65536;
constexpr int64_t kInitialEstimatedRowBytes = 1024;

/// Returns the operation-budget partition available to one Arrow batch.
inline int64_t
batch_target_bytes_from_memory_limit(int64_t memory_limit_bytes) {
  if (memory_limit_bytes <= 0) {
    return memory_limit_bytes;
  }
  return memory_budget_from_limit(memory_limit_bytes).batch_target_bytes;
}

/// Derives a conservative row-batch size from the memory limit and observed row
/// cost.
inline int64_t rows_per_batch_from_memory_limit(int64_t memory_limit_bytes,
                                                int64_t estimated_row_bytes) {
  if (memory_limit_bytes <= 0) {
    return kDefaultBatchRows;
  }
  const auto safe_row_bytes = std::max<int64_t>(1, estimated_row_bytes);
  const auto target_bytes =
      batch_target_bytes_from_memory_limit(memory_limit_bytes);
  const int64_t want = std::max<int64_t>(1, target_bytes / safe_row_bytes);
  return std::min<int64_t>(kDefaultBatchRows, want);
}

/// Derives a bounded batch row count from per-row estimates and operation
/// memory.
inline int64_t rows_per_batch_from_memory_limit(int64_t memory_limit_bytes) {
  return rows_per_batch_from_memory_limit(memory_limit_bytes,
                                          kInitialEstimatedRowBytes);
}

} // namespace sanitize::internal
