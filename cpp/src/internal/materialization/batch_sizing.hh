// Shared batch sizing helpers for ingestion pipeline passes.

#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>

namespace sanitize::internal {

constexpr int64_t kDefaultBatchRows = 65536;
constexpr int64_t kInitialEstimatedRowBytes = 4096;

// Keep headroom for parser state, row scratch, and ArrowArray ownership while
// avoiding overflow for very large user-provided limits.
inline int64_t batch_target_bytes_from_memory_limit(int64_t memory_limit_bytes) {
  if (memory_limit_bytes <= 0) {
    return memory_limit_bytes;
  }
  const auto quotient = memory_limit_bytes / 100;
  const auto remainder = memory_limit_bytes % 100;
  return std::max<int64_t>(1, quotient * 85 + (remainder * 85) / 100);
}

// Derives a conservative row batch size from an optional memory limit and an
// observed materialized byte cost. The estimate is deliberately a soft target.
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

inline int64_t rows_per_batch_from_memory_limit(int64_t memory_limit_bytes) {
  return rows_per_batch_from_memory_limit(memory_limit_bytes,
                                          kInitialEstimatedRowBytes);
}

} // namespace sanitize::internal
