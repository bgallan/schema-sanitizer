// Declares logical schema construction from inference statistics.
// The code keeps bounded shape discovery and scalar evidence consistent across
// serial and parallel scans.

#pragma once

#include "internal/inference/statistics/state.hh"

namespace sanitize::internal {

/// Infers a logical schema from collected statistics.
sanitize::LogicalSchema infer_logical_schema(const InferenceContext &ctx,
                                             const PreparedOptions &opts);

} // namespace sanitize::internal
