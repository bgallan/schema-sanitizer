// Declares shared recursive statistics scanning helpers.

#pragma once

#include "internal/inference/depth.hh"
#include "internal/inference/statistics/state.hh"

#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/status.hh"
#include "sanitize/core/value_view.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal::statistics_scan_detail {

// Recursively updates scalar statistics and reports whether the value supplied
// evidence.
sanitize::Status update_stats_value(InferenceContext *ctx, StatsNode *stats,
                                    const ValueView &value,
                                    const PreparedOptions &opts,
                                    IngestDiagnostics *diag, PathId path,
                                    DepthState depth, StrId default_key_id,
                                    bool *out_has_evidence);

} // namespace sanitize::internal::statistics_scan_detail
