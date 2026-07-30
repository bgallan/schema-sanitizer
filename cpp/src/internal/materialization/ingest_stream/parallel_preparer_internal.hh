// Defines private worker state shared by packet-preparer translation units.
#pragma once

#include "internal/materialization/direct_rows.hh"
#include "internal/materialization/ingest_stream/parallel_preparer.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/memory/pool_resource.hh"

#include <memory_resource>
#include <optional>

namespace sanitize::internal {

struct ParallelRowPreparer::WorkerState {
  std::shared_ptr<MemoryPool> memory_pool;
  std::shared_ptr<PoolResource> resource;
  std::unique_ptr<DirectMaterializer> direct;
  BatchAppenderPtr appender;
};

struct ParallelRowPreparer::ColumnMaterializerState {
  std::shared_ptr<MemoryPool> memory_pool;
  std::shared_ptr<PoolResource> resource;
  BatchAppenderPtr appender;
  std::optional<std::pmr::vector<FieldRef>> projected_fields;
  std::size_t group_index = 0;
};

} // namespace sanitize::internal
