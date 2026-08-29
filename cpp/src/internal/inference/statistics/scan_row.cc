// Updates top-level inference statistics for one materialized row.
// The code keeps bounded shape discovery and scalar evidence consistent across
// serial and parallel scans.

#include "internal/inference/scan.hh"

#include "internal/inference/depth.hh"
#include "internal/inference/statistics/scan_internal.hh"
#include "internal/inference/value_observation.hh"

#include <cstddef>
#include <string_view>

#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/core/value_view.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {

sanitize::Status update_stats_row(InferenceContext *ctx, const RowRef &row,
                                  const PreparedOptions &opts,
                                  IngestDiagnostics *diag) {
  constexpr DepthState root_depth{};
  const StrId default_key_id = ctx->default_key_id;
  for (std::size_t i = 0; i < row.size; ++i) {
    const std::string_view key = row.fields[i].key;
    const ValueView &value = row.fields[i].value;
    bool empty_container = false;
    SAN_RETURN_NOT_OK(value.container_is_empty(&empty_container));
    if (empty_container)
      continue;

    const StrId key_id = ctx->strings.intern(key);
    if (should_flatten_nested(value, opts, root_depth)) {
      mark_flattened_stats(ctx, &ctx->root, diag, key_id, key);
      continue;
    }

    StatsNode *stats = ctx->root.child(key_id, &ctx->arena);
    const PathId path = ctx->paths.child(PathInterner::root(), key_id);
    const DepthState depth = enter_value_depth(root_depth, value);
    bool has_evidence = false;
    SAN_RETURN_NOT_OK(statistics_scan_detail::update_stats_value(
        ctx, stats, value, opts, diag, path, depth, default_key_id,
        &has_evidence));
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal
