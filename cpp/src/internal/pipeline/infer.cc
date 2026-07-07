// Scans rows to infer shapes and collect scalar statistics.

#include "internal/pipeline/infer.hh"

#include "internal/core/value_view_util.hh"
#include "internal/pipeline/infer_depth.hh"
#include "internal/pipeline/infer_scan_common.hh"

#include <cstddef>
#include <string_view>

#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/core/value_view.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {

namespace {

// Scans one value and reports whether it provides usable inference evidence.
sanitize::Status scan_shapes_value(InferenceContext *ctx, const ValueView &v,
                                   const PreparedOptions &opts,
                                   IngestDiagnostics *diag, PathId path,
                                   DepthState depth, bool *out_has_evidence);

// Scans one object field during the shape-discovery pass.
sanitize::Status
scan_shapes_object_field(InferenceContext *ctx, StrId key_id,
                         std::string_view key_sv, const ValueView &child,
                         const PreparedOptions &opts, IngestDiagnostics *diag,
                         PathId parent_path, DepthState parent_depth,
                         bool *out_has_evidence) {
  bool empty_container = false;
  SAN_RETURN_NOT_OK(value_view_container_is_empty(child, &empty_container));
  if (empty_container) {
    *out_has_evidence = false;
    return sanitize::Status::OK();
  }

  if (should_flatten_nested(child, opts, parent_depth)) {
    mark_flattened_shape(ctx, diag, parent_path, key_id, key_sv);
    *out_has_evidence = true;
    return sanitize::Status::OK();
  }

  const PathId path = ctx->paths.child(parent_path, key_id);
  const DepthState child_depth = enter_value_depth(parent_depth, child);
  return scan_shapes_value(ctx, child, opts, diag, path, child_depth,
                           out_has_evidence);
}

sanitize::Status scan_shapes_value(InferenceContext *ctx, const ValueView &v,
                                   const PreparedOptions &opts,
                                   IngestDiagnostics *diag, PathId path,
                                   DepthState depth, bool *out_has_evidence) {
  if (!out_has_evidence)
    return sanitize::Status::Invalid(
        "scan_shapes_value: out_has_evidence is null");
  *out_has_evidence = false;

  if (v.is_null()) {
    *out_has_evidence = true;
    return sanitize::Status::OK();
  }

  if (v.is_array()) {
    const PathId elem_path = ctx->paths.child(path, ctx->list_marker);
    bool has_element_evidence = false;
    SAN_RETURN_NOT_OK(
        v.for_each_array_element([&](ValueView element) -> sanitize::Status {
          bool element_has_evidence = false;
          SAN_RETURN_NOT_OK(scan_shapes_value(
              ctx, element, opts, diag, elem_path,
              enter_value_depth(depth, element), &element_has_evidence));
          has_element_evidence |= element_has_evidence;
          return sanitize::Status::OK();
        }));
    if (has_element_evidence)
      ctx->shape(path).seen_list = true;
    *out_has_evidence = has_element_evidence;
    return sanitize::Status::OK();
  }

  if (v.is_object()) {
    bool has_field_evidence = false;
    SAN_RETURN_NOT_OK(
        v.for_each_object_field([&](std::string_view key, uint64_t,
                                    ValueView child) -> sanitize::Status {
          const StrId key_id = ctx->strings.intern(key);
          bool field_has_evidence = false;
          SAN_RETURN_NOT_OK(scan_shapes_object_field(ctx, key_id, key, child,
                                                     opts, diag, path, depth,
                                                     &field_has_evidence));
          has_field_evidence |= field_has_evidence;
          return sanitize::Status::OK();
        }));
    if (has_field_evidence)
      ctx->shape(path).seen_struct = true;
    *out_has_evidence = has_field_evidence;
    return sanitize::Status::OK();
  }

  *out_has_evidence = true;
  return sanitize::Status::OK();
}

// Updates scalar and nested statistics for one object field.
sanitize::Status update_stats_object_field(
    InferenceContext *ctx, StatsNode *parent, StrId key_id,
    std::string_view key_sv, const ValueView &child,
    const PreparedOptions &opts, IngestDiagnostics *diag, PathId parent_path,
    DepthState parent_depth, StrId default_key_id, bool *out_has_evidence);

// Updates one statistics node and reports evidence from the current value.
sanitize::Status update_stats_value(InferenceContext *ctx, StatsNode *stats,
                                    const ValueView &v,
                                    const PreparedOptions &opts,
                                    IngestDiagnostics *diag, PathId path,
                                    DepthState depth, StrId default_key_id,
                                    bool *out_has_evidence) {
  if (!out_has_evidence)
    return sanitize::Status::Invalid(
        "update_stats_value: out_has_evidence is null");
  *out_has_evidence = false;

  if (v.is_null()) {
    stats->has_evidence = true;
    *out_has_evidence = true;
    return sanitize::Status::OK();
  }

  const Shape &shape = ctx->shape(path);

  // List wrapping has precedence over struct wrapping.
  if (shape.seen_list) {
    stats->is_list = true;
    StatsNode *element_stats = stats->list_elem(&ctx->arena);
    const PathId elem_path = ctx->paths.child(path, ctx->list_marker);
    if (v.is_array()) {
      bool has_element_evidence = false;
      SAN_RETURN_NOT_OK(
          v.for_each_array_element([&](ValueView element) -> sanitize::Status {
            bool element_has_evidence = false;
            SAN_RETURN_NOT_OK(
                update_stats_value(ctx, element_stats, element, opts, diag,
                                   elem_path, enter_value_depth(depth, element),
                                   default_key_id, &element_has_evidence));
            has_element_evidence |= element_has_evidence;
            return sanitize::Status::OK();
          }));
      if (has_element_evidence)
        stats->has_evidence = true;
      *out_has_evidence = has_element_evidence;
      return sanitize::Status::OK();
    }

    bool element_has_evidence = false;
    const auto status =
        update_stats_value(ctx, element_stats, v, opts, diag, elem_path, depth,
                           default_key_id, &element_has_evidence);
    if (status.ok() && diag)
      diag->scalar_wrappings++;
    if (status.ok() && element_has_evidence)
      stats->has_evidence = true;
    *out_has_evidence = element_has_evidence;
    return status;
  }

  if (shape.seen_struct) {
    stats->is_struct = true;
    if (v.is_object()) {
      bool has_field_evidence = false;
      SAN_RETURN_NOT_OK(
          v.for_each_object_field([&](std::string_view key, uint64_t,
                                      ValueView child) -> sanitize::Status {
            const StrId key_id = ctx->strings.intern(key);
            bool field_has_evidence = false;
            SAN_RETURN_NOT_OK(update_stats_object_field(
                ctx, stats, key_id, key, child, opts, diag, path, depth,
                default_key_id, &field_has_evidence));
            has_field_evidence |= field_has_evidence;
            return sanitize::Status::OK();
          }));
      if (has_field_evidence)
        stats->has_evidence = true;
      *out_has_evidence = has_field_evidence;
      return sanitize::Status::OK();
    }

    StatsNode *child_stats = stats->child(default_key_id, &ctx->arena);
    const PathId child_path = ctx->paths.child(path, default_key_id);
    bool child_has_evidence = false;
    const auto status = update_stats_value(
        ctx, child_stats, v, opts, diag, child_path,
        DepthState{.arrow = depth.arrow + 1, .parquet = depth.parquet + 1},
        default_key_id, &child_has_evidence);
    if (status.ok() && diag)
      diag->scalar_wrappings++;
    if (status.ok() && child_has_evidence)
      stats->has_evidence = true;
    *out_has_evidence = child_has_evidence;
    return status;
  }

  if (v.is_array()) {
    StatsNode *element_stats = stats->list_elem(&ctx->arena);
    const PathId elem_path = ctx->paths.child(path, ctx->list_marker);
    bool has_element_evidence = false;
    SAN_RETURN_NOT_OK(
        v.for_each_array_element([&](ValueView element) -> sanitize::Status {
          bool element_has_evidence = false;
          SAN_RETURN_NOT_OK(
              update_stats_value(ctx, element_stats, element, opts, diag,
                                 elem_path, enter_value_depth(depth, element),
                                 default_key_id, &element_has_evidence));
          has_element_evidence |= element_has_evidence;
          return sanitize::Status::OK();
        }));
    if (has_element_evidence) {
      stats->is_list = true;
      stats->has_evidence = true;
    }
    *out_has_evidence = has_element_evidence;
    return sanitize::Status::OK();
  }

  if (v.is_object()) {
    bool has_field_evidence = false;
    SAN_RETURN_NOT_OK(
        v.for_each_object_field([&](std::string_view key, uint64_t,
                                    ValueView child) -> sanitize::Status {
          const StrId key_id = ctx->strings.intern(key);
          bool field_has_evidence = false;
          SAN_RETURN_NOT_OK(update_stats_object_field(
              ctx, stats, key_id, key, child, opts, diag, path, depth,
              default_key_id, &field_has_evidence));
          has_field_evidence |= field_has_evidence;
          return sanitize::Status::OK();
        }));
    if (has_field_evidence) {
      stats->is_struct = true;
      stats->has_evidence = true;
    }
    *out_has_evidence = has_field_evidence;
    return sanitize::Status::OK();
  }

  stats->scalar_kind_mask |= infer_scalar_mask(v, opts);
  stats->has_evidence = true;
  *out_has_evidence = true;
  return sanitize::Status::OK();
}

sanitize::Status update_stats_object_field(
    InferenceContext *ctx, StatsNode *parent, StrId key_id,
    std::string_view key_sv, const ValueView &child,
    const PreparedOptions &opts, IngestDiagnostics *diag, PathId parent_path,
    DepthState parent_depth, StrId default_key_id, bool *out_has_evidence) {
  bool empty_container = false;
  SAN_RETURN_NOT_OK(value_view_container_is_empty(child, &empty_container));
  if (empty_container) {
    *out_has_evidence = false;
    return sanitize::Status::OK();
  }

  if (should_flatten_nested(child, opts, parent_depth)) {
    mark_flattened_stats(ctx, parent, diag, key_id, key_sv);
    *out_has_evidence = true;
    return sanitize::Status::OK();
  }

  StatsNode *child_stats = parent->child(key_id, &ctx->arena);
  const PathId path = ctx->paths.child(parent_path, key_id);
  const DepthState child_depth = enter_value_depth(parent_depth, child);
  return update_stats_value(ctx, child_stats, child, opts, diag, path,
                            child_depth, default_key_id, out_has_evidence);
}

} // namespace

sanitize::Status scan_shapes_row(InferenceContext *ctx, const RowRef &row,
                                 const PreparedOptions &opts,
                                 IngestDiagnostics *diag) {
  constexpr DepthState root_depth{};
  for (std::size_t i = 0; i < row.size; ++i) {
    const std::string_view key = row.fields[i].key;
    const ValueView &value = row.fields[i].value;
    bool empty_container = false;
    SAN_RETURN_NOT_OK(value_view_container_is_empty(value, &empty_container));
    if (empty_container)
      continue;

    const StrId key_id = ctx->strings.intern(key);
    if (should_flatten_nested(value, opts, root_depth)) {
      mark_flattened_shape(ctx, diag, PathInterner::root(), key_id, key);
      continue;
    }

    const PathId path = ctx->paths.child(PathInterner::root(), key_id);
    const DepthState depth = enter_value_depth(root_depth, value);
    bool has_evidence = false;
    SAN_RETURN_NOT_OK(
        scan_shapes_value(ctx, value, opts, diag, path, depth, &has_evidence));
  }
  return sanitize::Status::OK();
}

sanitize::Status update_stats_row(InferenceContext *ctx, const RowRef &row,
                                  const PreparedOptions &opts,
                                  IngestDiagnostics *diag) {
  constexpr DepthState root_depth{};
  const StrId default_key_id = ctx->default_key_id;
  for (std::size_t i = 0; i < row.size; ++i) {
    const std::string_view key = row.fields[i].key;
    const ValueView &value = row.fields[i].value;
    bool empty_container = false;
    SAN_RETURN_NOT_OK(value_view_container_is_empty(value, &empty_container));
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
    SAN_RETURN_NOT_OK(update_stats_value(ctx, stats, value, opts, diag, path,
                                         depth, default_key_id, &has_evidence));
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal
