// Declares value observation helpers shared by shape and statistics scans.
// The code keeps bounded shape discovery and scalar evidence consistent across
// serial and parallel scans.

#pragma once

#include <cstdint>
#include <string_view>

#include "internal/inference/statistics/state.hh"
#include "sanitize/core/status.hh"
#include "sanitize/core/value_view.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {

/// Infers the scalar-kind bitmask contributed by one observed value.
uint32_t infer_scalar_mask(const ValueView &value, const PreparedOptions &opts);

/// Interns the synthetic field name used for over-depth nested values.
StrId flattened_key_id(InferenceContext *ctx, StrId key_id,
                       std::string_view key);

/// Records the existence of an over-depth field in the shape pass.
void mark_flattened_shape(InferenceContext *ctx, IngestDiagnostics *diag,
                          PathId parent_path, StrId key_id,
                          std::string_view key);

/// Records an over-depth field as a string in the statistics pass.
void mark_flattened_stats(InferenceContext *ctx, StatsNode *parent,
                          IngestDiagnostics *diag, StrId key_id,
                          std::string_view key);

} // namespace sanitize::internal
