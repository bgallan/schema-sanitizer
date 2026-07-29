// Declares worker-local packet preparation for ordered materialization.

#pragma once

#include "internal/materialization/batch_appender.hh"
#include "internal/materialization/ingest_stream/column_partition.hh"
#include "internal/materialization/ingest_stream/parallel_packets.hh"
#include "internal/runtime/execution_policy.hh"
#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

#include "internal/runtime/thread_compat.hh"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace sanitize::internal {

class MemoryPool;

struct PreparedRowPacket {
  PreparedRow row;
  IngestDiagnostics diagnostics;
  sanitize::Status status;
  bool has_row = false;
};

struct MaterializationTask {
  OwnedRowPacket owned;
  std::shared_ptr<const ColumnPartitionInput> partitioned;
  std::size_t column_group_index = 0;
  std::size_t column_state_index = 0;
};

struct PreparedRowsPacket {
  std::vector<PreparedRowPacket> rows;
  std::shared_ptr<sanitize::CArrayGuard> array;
  IngestDiagnostics diagnostics;
  sanitize::Status terminal_status;
  ColumnPartitionFailure column_failure;
  std::size_t estimated_source_bytes = 0;
  std::size_t source_row_count = 0;
  std::size_t completed_source_rows = 0;
  std::size_t column_group_index = 0;
  std::size_t column_group_count = 0;
  std::size_t first_column = 0;
  std::size_t column_count = 0;
  std::int64_t materialized_bytes = 0;
  bool columnar = false;
  bool column_partitioned = false;
  bool column_state_initialized = false;
};

class ParallelRowPreparer final {
public:
  static sanitize::Result<std::shared_ptr<ParallelRowPreparer>>
  Make(std::string_view frontend_name, std::shared_ptr<const CompiledPlan> plan,
       PreparedOptionsPtr opts, std::shared_ptr<void> operation_memory_pool,
       const ExecutionPolicy &policy);

  ~ParallelRowPreparer();

  // Prepares one ordinary packet or one disjoint column group.
  sanitize::Result<PreparedRowsPacket>
  Prepare(MaterializationTask &&task, std::size_t worker_index,
          sanitize::internal::StopToken stop);

  [[nodiscard]] std::size_t column_group_count() const noexcept {
    return column_ranges_.size();
  }

  // Returns group indices in deterministic critical-path-first submission
  // order. Arrow merge and error reduction still use frozen column order.
  [[nodiscard]] std::span<const std::size_t>
  column_group_submission_order() const noexcept {
    return column_submission_order_;
  }

private:
  struct WorkerState;
  struct ColumnMaterializerState;

  ParallelRowPreparer(std::string frontend_name,
                      std::shared_ptr<const CompiledPlan> plan,
                      PreparedOptionsPtr opts);

  sanitize::Result<PreparedRowsPacket>
  prepare_rows(OwnedRowPacket &&owned, std::size_t worker_index,
               sanitize::internal::StopToken stop);

  sanitize::Result<PreparedRowPacket>
  prepare_one(const RowRef &row, std::size_t worker_index,
              sanitize::internal::StopToken stop);

  sanitize::Result<PreparedRow> prepare_raw(const RowRef &row,
                                            std::size_t worker_index,
                                            IngestDiagnostics *diagnostics);

  sanitize::Result<PreparedRowsPacket>
  prepare_columnar(OwnedRowPacket &&owned, std::size_t worker_index,
                   sanitize::internal::StopToken stop);

  sanitize::Result<PreparedRowsPacket> prepare_column_partition(
      const std::shared_ptr<const ColumnPartitionInput> &input,
      std::size_t group_index, std::size_t column_state_index,
      sanitize::internal::StopToken stop);

  sanitize::Status
  initialize_column_states(const std::shared_ptr<MemoryPool> &parent,
                           const ExecutionPolicy &policy);

  std::string frontend_name_;
  std::shared_ptr<const CompiledPlan> plan_;
  PreparedOptionsPtr opts_;
  bool prefer_raw_materialization_ = false;
  bool columnar_packets_ = false;
  std::vector<ColumnPartitionRange> column_ranges_;
  std::vector<std::size_t> column_submission_order_;
  std::vector<std::shared_ptr<const CompiledPlan>> column_plans_;
  std::vector<std::unique_ptr<WorkerState>> workers_;
  std::vector<std::unique_ptr<ColumnMaterializerState>> column_states_;
};

} // namespace sanitize::internal
