// Declares bounded column-partitioned packet materialization.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#pragma once

#include "internal/materialization/ingest_stream/parallel_packets.hh"
#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <span>
#include <vector>

struct ArrowArray;

namespace sanitize::internal {

class PoolResource;

struct ColumnPartitionRange {
  std::size_t first_column = 0;
  std::size_t column_count = 0;
  // Static relative conversion cost used only to balance contiguous groups.
  // It is not a byte estimate and does not alter the operation memory budget.
  std::size_t estimated_cost = 0;
};

struct ColumnPartitionFailure {
  sanitize::Status status = sanitize::Status::OK();
  std::size_t source_row_index = 0;
  std::size_t column_order = 0;
  bool present = false;
};

struct ColumnPartitionInput {
  explicit ColumnPartitionInput(std::shared_ptr<PoolResource> input_resource);

  [[nodiscard]] std::span<const std::int32_t>
  row_field_indices(std::size_t row_index) const noexcept;

  OwnedRowPacket owned;
  std::shared_ptr<PoolResource> resource;
  std::pmr::vector<std::int32_t> field_indices;
  ColumnPartitionFailure row_validation_failure;
  std::size_t column_count = 0;
  bool plan_ordered = false;
};

[[nodiscard]] bool
is_column_partition_candidate(const sanitize::CompiledPlan &plan) noexcept;

[[nodiscard]] bool should_use_column_partition(
    const sanitize::CompiledPlan &plan, const sanitize::PreparedOptions &opts,
    std::int64_t effective_workers, std::int64_t expected_rows,
    std::int64_t input_size_hint_bytes) noexcept;

[[nodiscard]] bool
column_partition_enabled(const sanitize::CompiledPlan &plan,
                         const sanitize::PreparedOptions &opts) noexcept;

sanitize::Result<std::vector<ColumnPartitionRange>>
make_column_partition_ranges(const sanitize::CompiledPlan &plan,
                             std::size_t worker_count);

[[nodiscard]] std::size_t
column_partition_packet_window(std::size_t worker_count,
                               std::size_t group_count) noexcept;

sanitize::Result<std::shared_ptr<const sanitize::CompiledPlan>>
make_column_partition_plan(const sanitize::CompiledPlan &plan,
                           const ColumnPartitionRange &range);

sanitize::Result<std::shared_ptr<const ColumnPartitionInput>>
make_column_partition_input(OwnedRowPacket &&owned,
                            const sanitize::CompiledPlan &plan,
                            const sanitize::PreparedOptions &opts,
                            std::shared_ptr<PoolResource> resource);

sanitize::Status merge_column_partition_arrays(
    std::span<std::shared_ptr<sanitize::CArrayGuard>> groups,
    const std::shared_ptr<PoolResource> &pool, ArrowArray *out);

[[nodiscard]] bool column_partition_failure_precedes(
    const ColumnPartitionFailure &candidate,
    const ColumnPartitionFailure &current) noexcept;

} // namespace sanitize::internal
