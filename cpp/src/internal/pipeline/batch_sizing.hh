// Shared batch sizing helpers for ingestion pipeline passes.

#pragma once

#include <algorithm>
#include <cstdint>

namespace sanitize::internal {

// Derives a conservative row batch size from an optional memory limit.
inline int64_t rows_per_batch_from_memory_limit(int64_t memory_limit_bytes) {
  constexpr int64_t kDefaultRows = 65536;
  constexpr int64_t kEstimatedRowBytes = 4096;
  if (memory_limit_bytes <= 0)
    return kDefaultRows;
  const int64_t want = memory_limit_bytes / kEstimatedRowBytes;
  return std::max<int64_t>(1, std::min<int64_t>(kDefaultRows, want));
}

} // namespace sanitize::internal
