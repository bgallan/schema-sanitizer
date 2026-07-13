// Declares row scans that collect inference shape and scalar statistics.

#pragma once

#include "internal/inference/statistics/state.hh"

namespace sanitize::internal {

// Scans one row for structural shape information.
sanitize::Status scan_shapes_row(InferenceContext *ctx, const RowRef &row,
                                 const PreparedOptions &opts,
                                 IngestDiagnostics *diag);

// Updates scalar statistics for one row.
sanitize::Status update_stats_row(InferenceContext *ctx, const RowRef &row,
                                  const PreparedOptions &opts,
                                  IngestDiagnostics *diag);

} // namespace sanitize::internal
