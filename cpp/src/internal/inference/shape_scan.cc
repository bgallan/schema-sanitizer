// Discovers nested value shapes before scalar statistics are accumulated.

#include "internal/inference/scan.hh"

#include "internal/inference/depth.hh"
#include "internal/inference/value_observation.hh"

#include <cstddef>
#include <string_view>

#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/core/value_view.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {

namespace {

sanitize::Status scan_shapes_value(InferenceContext *ctx, const ValueView &v,
                                   const PreparedOptions &opts,
                                   IngestDiagnostics *diag, PathId path,
                                   DepthState depth, bool *out_has_evidence);

sanitize::Status
scan_shapes_object_field(InferenceContext *ctx, StrId key_id,
                         std::string_view key_sv, const ValueView &child,
                         const PreparedOptions &opts, IngestDiagnostics *diag,
                         PathId parent_path, DepthState parent_depth,
                         bool *out_has_evidence) {
  bool empty_container = false;
  SAN_RETURN_NOT_OK(child.container_is_empty(&empty_container));
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

} // namespace

sanitize::Status scan_shapes_row(InferenceContext *ctx, const RowRef &row,
                                 const PreparedOptions &opts,
                                 IngestDiagnostics *diag) {
  constexpr DepthState root_depth{};
  for (std::size_t i = 0; i < row.size; ++i) {
    const std::string_view key = row.fields[i].key;
    const ValueView &value = row.fields[i].value;
    bool empty_container = false;
    SAN_RETURN_NOT_OK(value.container_is_empty(&empty_container));
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

} // namespace sanitize::internal
