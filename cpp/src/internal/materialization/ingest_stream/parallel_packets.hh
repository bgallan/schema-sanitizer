// Defines memory-accounted row packets for parallel materialization.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#pragma once

#include "internal/runtime/execution_policy.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/planning/plan.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace sanitize::internal {

// Bounds one packet before it enters the ordered executor.
struct MaterializationPacketLimits {
  std::size_t max_rows = 1;
  std::size_t target_bytes = 1;
};

// Owns one canonical frontend batch without copying its RowRef entries.
// Packets retain this owner while exposing disjoint contiguous row spans.
struct OwnedRowBatch {
  std::vector<RowRef> rows;
  std::shared_ptr<const void> source_owner;
  std::shared_ptr<sanitize::RowBatchReleaser> releaser;
};

// Owns row views from exactly one frontend batch. The owner keeps both the
// canonical RowRef storage and all referenced frontend bytes alive.
struct OwnedRowPacket {
  /// Creates an empty owned row packet with no outstanding ownership or memory
  /// reservation.
  OwnedRowPacket() = default;
  /// Disables copying so ownership and cleanup responsibility cannot be
  /// duplicated.
  OwnedRowPacket(const OwnedRowPacket &) = delete;
  /// Disables copying so ownership and cleanup responsibility cannot be
  /// duplicated.
  OwnedRowPacket &operator=(const OwnedRowPacket &) = delete;
  OwnedRowPacket(OwnedRowPacket &&other) noexcept;
  OwnedRowPacket &operator=(OwnedRowPacket &&other) noexcept;
  ~OwnedRowPacket();

  std::span<RowRef> rows;
  std::shared_ptr<const void> owner;
  std::shared_ptr<sanitize::RowBatchReleaser> releaser;
  std::size_t release_begin = 0;
  std::size_t release_count = 0;
  std::size_t estimated_source_bytes = 0;
  std::size_t json_tokenized_rows = 0;
  std::size_t json_tokenized_fields = 0;
  std::size_t json_plan_ordered_rows = 0;
  std::size_t json_token_fallback_rows = 0;
  std::size_t json_skipped_rows = 0;
};

/// Derives adaptive packet row and byte caps from policy and observed row cost.
[[nodiscard]] MaterializationPacketLimits
materialization_packet_limits(const ExecutionPolicy &policy,
                              std::int64_t observed_bytes_per_row) noexcept;

/// Narrows host execution policy according to materialization cost and input
/// size.
[[nodiscard]] ExecutionPolicy materialization_execution_policy(
    const CompiledPlan &plan, const ExecutionPolicy &policy,
    std::int64_t expected_rows, std::int64_t input_size_hint_bytes) noexcept;

/// Retargets JSON Lines row parallelism while preserving host and memory
/// ceilings.
[[nodiscard]] ExecutionPolicy jsonl_row_parallel_execution_policy(
    const ExecutionPolicy &policy, std::int64_t expected_rows,
    std::int64_t input_size_hint_bytes) noexcept;

/// Moves one frontend batch into a single shared owner without copying rows.
sanitize::Result<std::shared_ptr<OwnedRowBatch>> make_owned_row_batch(
    std::vector<RowRef> rows, std::shared_ptr<const void> source_owner,
    std::shared_ptr<sanitize::RowBatchReleaser> releaser = nullptr);

/// Builds the next non-empty contiguous packet, isolating an individually
/// oversized row.
sanitize::Result<OwnedRowPacket>
build_owned_row_packet(const std::shared_ptr<OwnedRowBatch> &batch_owner,
                       std::size_t start, MaterializationPacketLimits limits);

} // namespace sanitize::internal
