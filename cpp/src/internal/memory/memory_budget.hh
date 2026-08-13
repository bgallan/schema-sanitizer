// Derives every native resource limit from one per-operation memory budget.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace sanitize::internal {

inline constexpr std::int64_t kDefaultMemoryLimitBytes =
    512LL * 1024LL * 1024LL;
inline constexpr std::int64_t kHardMaxMemoryLimitBytes =
    64LL * 1024LL * 1024LL * 1024LL;
inline constexpr std::int64_t kAutomaticMemoryReserveBytes =
    256LL * 1024LL * 1024LL;

// Returns a safe operation budget derived from currently available host and
// container memory. Platform discovery lives out of line so every native
// consumer shares one implementation.
[[nodiscard]] std::int64_t automatic_memory_limit_bytes() noexcept;

[[nodiscard]] constexpr std::int64_t
automatic_memory_limit_from_available(std::int64_t available_bytes) noexcept {
  if (available_bytes <= 0) {
    return kDefaultMemoryLimitBytes;
  }
  // Preserve at least one eighth of a roomy host and one quarter of a
  // constrained host. The absolute reserve stops small transient allocations
  // outside the tracked operation budget from turning normal pressure into OOM.
  const auto proportional_reserve = available_bytes / 8;
  const auto constrained_reserve =
      std::min(kAutomaticMemoryReserveBytes, available_bytes / 4);
  const auto reserve = std::max(proportional_reserve, constrained_reserve);
  return std::max<std::int64_t>(
      1, std::min(kHardMaxMemoryLimitBytes, available_bytes - reserve));
}

static_assert(automatic_memory_limit_from_available(128LL * 1024LL * 1024LL) ==
              96LL * 1024LL * 1024LL);
static_assert(automatic_memory_limit_from_available(1LL * 1024LL * 1024LL *
                                                    1024LL) ==
              768LL * 1024LL * 1024LL);
static_assert(automatic_memory_limit_from_available(16LL * 1024LL * 1024LL *
                                                    1024LL) ==
              14LL * 1024LL * 1024LL * 1024LL);
static_assert(automatic_memory_limit_from_available(100LL * 1024LL * 1024LL *
                                                    1024LL) ==
              kHardMaxMemoryLimitBytes);

struct MemoryBudget {
  std::int64_t total_bytes = kDefaultMemoryLimitBytes;
  std::int64_t io_chunk_bytes = 1024 * 1024;
  std::int64_t batch_target_bytes = 64LL * 1024LL * 1024LL;
  std::int64_t coalesce_max_bytes = 512LL * 1024LL * 1024LL;
  std::int64_t metadata_bytes = 64LL * 1024LL * 1024LL;
  std::int64_t materialized_input_bytes = 512LL * 1024LL * 1024LL;
  std::int64_t replay_spool_bytes = 2LL * 1024LL * 1024LL * 1024LL;
  std::int64_t parquet_reader_buffer_bytes = 256LL * 1024LL * 1024LL;
  std::int64_t parquet_reader_rows = 262144;
  std::int64_t parquet_row_group_bytes = 256LL * 1024LL * 1024LL;
  std::int64_t parquet_row_group_rows = 262144;
  std::int64_t parquet_page_bytes = 16LL * 1024LL * 1024LL;
  std::int64_t parquet_footer_bytes = 32LL * 1024LL * 1024LL;
  std::int64_t arrow_logical_slots = 100'000'000;
  std::int64_t arrow_logical_buffer_bytes = 512LL * 1024LL * 1024LL;
  std::int64_t async_concurrency = 32;
  std::int64_t async_prefetch_files = 64;
  std::int64_t async_retries = 4;
  double async_timeout_seconds = 120.0;
  std::int64_t remote_chunk_prefetch = 16;
  std::int64_t source_discovery_concurrency = 64;
};

[[nodiscard]] inline std::uint64_t
backpressure_timeout_millis_from(const MemoryBudget &budget) noexcept {
  if (!(budget.async_timeout_seconds > 0.0)) {
    return 1'000U;
  }
  const auto millis = budget.async_timeout_seconds * 1000.0;
  return static_cast<std::uint64_t>(
      std::min(30'000.0, std::max(1'000.0, millis)));
}

[[nodiscard]] inline std::uint64_t
backpressure_deadline_millis_from(const MemoryBudget &budget) noexcept {
  // The per-saturation timeout is deliberately capped at 30s, but the logical
  // operation deadline must preserve the caller's wider async timeout. Keeping
  // these two concepts separate prevents a 120s operation from becoming
  // terminally expired 30s after arena construction.
  if (!(budget.async_timeout_seconds > 0.0)) {
    return 1'000U;
  }
  const auto millis = budget.async_timeout_seconds * 1000.0;
  return static_cast<std::uint64_t>(
      std::min(86'400'000.0, std::max(1'000.0, millis)));
}

[[nodiscard]] constexpr std::int64_t
normalize_memory_limit_bytes(std::int64_t requested) noexcept {
  if (requested <= 0) {
    // Automatic sizing is inherently a runtime operation. Calling the
    // non-constexpr platform probe here deliberately prevents a literal
    // sentinel from being trial-constant-evaluated to the fallback default.
    return automatic_memory_limit_bytes();
  }
  return std::min(requested, kHardMaxMemoryLimitBytes);
}

[[nodiscard]] constexpr std::int64_t
bounded_fraction(std::int64_t total, std::int64_t divisor, std::int64_t maximum,
                 std::int64_t minimum = 1) noexcept {
  const auto divided = total / divisor;
  return std::min(total, std::min(maximum, std::max(minimum, divided)));
}

[[nodiscard]] constexpr MemoryBudget
memory_budget_from_limit(std::int64_t requested) noexcept {
  MemoryBudget out;
  out.total_bytes = normalize_memory_limit_bytes(requested);
  out.io_chunk_bytes =
      bounded_fraction(out.total_bytes, 64, 1LL * 1024LL * 1024LL);
  out.batch_target_bytes =
      bounded_fraction(out.total_bytes, 4, 64LL * 1024LL * 1024LL);
  out.coalesce_max_bytes =
      std::min<std::int64_t>(out.total_bytes, 512LL * 1024LL * 1024LL);
  out.metadata_bytes =
      bounded_fraction(out.total_bytes, 8, 256LL * 1024LL * 1024LL);
  out.materialized_input_bytes =
      std::min<std::int64_t>(out.total_bytes, 1LL * 1024LL * 1024LL * 1024LL);
  out.replay_spool_bytes = std::min<std::int64_t>(
      out.total_bytes * 4, 8LL * 1024LL * 1024LL * 1024LL);
  out.parquet_reader_buffer_bytes =
      bounded_fraction(out.total_bytes, 2, 1LL * 1024LL * 1024LL * 1024LL);
  out.parquet_reader_rows = std::min<std::int64_t>(
      1'048'576,
      std::max<std::int64_t>(1, out.parquet_reader_buffer_bytes / 1024));
  out.parquet_row_group_bytes =
      bounded_fraction(out.total_bytes, 2, 512LL * 1024LL * 1024LL);
  out.parquet_row_group_rows = std::min<std::int64_t>(
      1'048'576, std::max<std::int64_t>(1, out.parquet_row_group_bytes / 1024));
  out.parquet_page_bytes = std::min<std::int64_t>(
      out.parquet_row_group_bytes,
      std::min<std::int64_t>(
          64LL * 1024LL * 1024LL,
          std::max<std::int64_t>(64LL * 1024LL,
                                 out.parquet_row_group_bytes / 16)));
  out.parquet_footer_bytes = bounded_fraction(
      out.total_bytes, 16, 256LL * 1024LL * 1024LL, 64LL * 1024LL);
  out.arrow_logical_slots = std::min<std::int64_t>(
      100'000'000, std::max<std::int64_t>(1'000'000, out.total_bytes / 8));
  out.arrow_logical_buffer_bytes =
      std::min<std::int64_t>(1LL * 1024LL * 1024LL * 1024LL, out.total_bytes);
  out.async_concurrency =
      std::max<std::int64_t>(1, out.total_bytes / (16LL * 1024LL * 1024LL));
  out.async_prefetch_files =
      std::max(out.async_concurrency, out.async_concurrency * 2);
  out.async_retries = 4;
  out.async_timeout_seconds = 120.0;
  out.remote_chunk_prefetch =
      std::max<std::int64_t>(1, out.total_bytes / (32LL * 1024LL * 1024LL));
  out.source_discovery_concurrency =
      std::max<std::int64_t>(1, out.total_bytes / (8LL * 1024LL * 1024LL));
  return out;
}

} // namespace sanitize::internal
