// Declares direct raw-row materialization into Arrow arrays.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#pragma once

#include <memory>
#include <string_view>

#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/options/options.hh"

#include "internal/materialization/batch_appender.hh"
#include "internal/memory/pool_resource.hh"

namespace sanitize::internal {

// Unified raw-row (text) materialization fast-path.
class DirectMaterializer {
public:
  /// Releases frontend-specific scratch through the polymorphic materializer
  /// interface.
  virtual ~DirectMaterializer() = default;

  /// Converts one raw frontend row without mutating a shared Arrow builder.
  virtual sanitize::Result<PreparedRow>
  PrepareRaw(const sanitize::CompiledPlan &plan, const RowRef &row,
             const PreparedOptions &opts, IngestDiagnostics *diagnostics) = 0;

  /// Appends one raw frontend row directly into the batch appender.
  virtual sanitize::Result<AppendRowResult>
  AppendRaw(BatchAppender *app, const RowRef &row, const PreparedOptions &opts,
            IngestDiagnostics *diagnostics);
};

/// Selects a frontend-specific direct materializer backed by the operation
/// pool.
sanitize::Result<std::unique_ptr<DirectMaterializer>>
make_direct_materializer(std::string_view frontend_name,
                         PoolResource *pmr_pool);

} // namespace sanitize::internal
