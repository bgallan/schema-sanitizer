// Reduces packet-local inference evidence in canonical row order.
// The code keeps bounded shape discovery and scalar evidence consistent across
// serial and parallel scans.

#include "internal/inference/parallel_evidence.hh"

#include "internal/inference/value_observation.hh"

#include <cstddef>
#include <string_view>

#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal {
namespace {

using EvidenceKind = InferenceEvidenceNode::Kind;

/// Checks packet evidence indexes, subtree bounds, and node-kind invariants
/// before reduction.
sanitize::Status validate_node(const InferenceEvidencePacket &packet,
                               std::size_t index,
                               std::size_t row_end) noexcept {
  if (index >= row_end || row_end > packet.nodes.size()) {
    return sanitize::Status::Invalid(
        "parallel inference evidence contains an invalid node index");
  }
  const auto &node = packet.nodes[index];
  if (node.subtree_end <= index || node.subtree_end > row_end) {
    return sanitize::Status::Invalid(
        "parallel inference evidence contains an invalid subtree span");
  }
  if ((node.kind == EvidenceKind::kNull || node.kind == EvidenceKind::kScalar ||
       node.kind == EvidenceKind::kFlattened) &&
      node.subtree_end != index + 1) {
    return sanitize::Status::Invalid(
        "parallel inference evidence contains a non-leaf scalar node");
  }
  return sanitize::Status::OK();
}

/// Reports whether shape evidence records an observed empty object or array.
[[nodiscard]] bool is_empty_container(const InferenceEvidenceNode &node,
                                      std::size_t index) noexcept {
  return node.empty_container(index);
}

sanitize::Status scan_shape_value(InferenceContext *ctx,
                                  const InferenceEvidencePacket &packet,
                                  std::size_t index, std::size_t row_end,
                                  IngestDiagnostics *diagnostics, PathId path,
                                  bool *out_has_evidence);

/// Scans named shape with explicit depth and memory limits while accumulating
/// source-ordered evidence.
sanitize::Status scan_named_shape(InferenceContext *ctx,
                                  const InferenceEvidencePacket &packet,
                                  std::size_t index, std::size_t row_end,
                                  IngestDiagnostics *diagnostics,
                                  PathId parent_path, StrId key_id,
                                  bool *out_has_evidence) {
  SAN_RETURN_NOT_OK(validate_node(packet, index, row_end));
  const auto &node = packet.nodes[index];
  *out_has_evidence = false;
  if (is_empty_container(node, index)) {
    return sanitize::Status::OK();
  }
  if (node.kind == EvidenceKind::kFlattened) {
    mark_flattened_shape(ctx, diagnostics, parent_path, key_id,
                         packet.keys.View(node.key_index));
    *out_has_evidence = true;
    return sanitize::Status::OK();
  }
  const PathId path = ctx->paths.child(parent_path, key_id);
  return scan_shape_value(ctx, packet, index, row_end, diagnostics, path,
                          out_has_evidence);
}

/// Scans shape object with explicit depth and memory limits while accumulating
/// source-ordered evidence.
sanitize::Status scan_shape_object(InferenceContext *ctx,
                                   const InferenceEvidencePacket &packet,
                                   std::size_t index, std::size_t row_end,
                                   IngestDiagnostics *diagnostics, PathId path,
                                   bool *out_has_evidence) {
  bool has_field_evidence = false;
  std::size_t child = index + 1;
  while (child < packet.nodes[index].subtree_end) {
    SAN_RETURN_NOT_OK(validate_node(packet, child, row_end));
    const auto &child_node = packet.nodes[child];
    if (packet.keys.View(child_node.key_index).empty()) {
      return sanitize::Status::Invalid(
          "parallel inference object evidence is missing a field key");
    }
    const StrId key_id =
        packet.keys.Resolve(child_node.key_index, &ctx->strings);
    bool field_has_evidence = false;
    SAN_RETURN_NOT_OK(scan_named_shape(ctx, packet, child, row_end, diagnostics,
                                       path, key_id, &field_has_evidence));
    has_field_evidence |= field_has_evidence;
    child = child_node.subtree_end;
  }
  if (child != packet.nodes[index].subtree_end) {
    return sanitize::Status::Invalid(
        "parallel inference object evidence has overlapping children");
  }
  if (has_field_evidence) {
    ctx->shape(path).seen_struct = true;
  }
  *out_has_evidence = has_field_evidence;
  return sanitize::Status::OK();
}

/// Scans shape array with explicit depth and memory limits while accumulating
/// source-ordered evidence.
sanitize::Status scan_shape_array(InferenceContext *ctx,
                                  const InferenceEvidencePacket &packet,
                                  std::size_t index, std::size_t row_end,
                                  IngestDiagnostics *diagnostics, PathId path,
                                  bool *out_has_evidence) {
  const PathId element_path = ctx->paths.child(path, ctx->list_marker);
  bool has_element_evidence = false;
  std::size_t child = index + 1;
  while (child < packet.nodes[index].subtree_end) {
    SAN_RETURN_NOT_OK(validate_node(packet, child, row_end));
    bool element_has_evidence = false;
    SAN_RETURN_NOT_OK(scan_shape_value(ctx, packet, child, row_end, diagnostics,
                                       element_path, &element_has_evidence));
    has_element_evidence |= element_has_evidence;
    child = packet.nodes[child].subtree_end;
  }
  if (child != packet.nodes[index].subtree_end) {
    return sanitize::Status::Invalid(
        "parallel inference array evidence has overlapping children");
  }
  if (has_element_evidence) {
    ctx->shape(path).seen_list = true;
  }
  *out_has_evidence = has_element_evidence;
  return sanitize::Status::OK();
}

/// Scans shape value with explicit depth and memory limits while accumulating
/// source-ordered evidence.
sanitize::Status scan_shape_value(InferenceContext *ctx,
                                  const InferenceEvidencePacket &packet,
                                  std::size_t index, std::size_t row_end,
                                  IngestDiagnostics *diagnostics, PathId path,
                                  bool *out_has_evidence) {
  if (!out_has_evidence) {
    return sanitize::Status::Invalid(
        "scan_shape_value: out_has_evidence is null");
  }
  SAN_RETURN_NOT_OK(validate_node(packet, index, row_end));
  const auto &node = packet.nodes[index];
  *out_has_evidence = false;
  switch (node.kind) {
  case EvidenceKind::kNull:
    return sanitize::Status::OK();
  case EvidenceKind::kScalar:
    *out_has_evidence = true;
    return sanitize::Status::OK();
  case EvidenceKind::kArray:
    return scan_shape_array(ctx, packet, index, row_end, diagnostics, path,
                            out_has_evidence);
  case EvidenceKind::kObject:
    return scan_shape_object(ctx, packet, index, row_end, diagnostics, path,
                             out_has_evidence);
  case EvidenceKind::kFlattened:
    return sanitize::Status::Invalid(
        "parallel inference flattened evidence is not attached to a field");
  }
  return sanitize::Status::Invalid(
      "parallel inference evidence contains an unknown node kind");
}

template <bool Validate>
sanitize::Status
update_stats_value(InferenceContext *ctx, StatsNode *stats,
                   const InferenceEvidencePacket &packet, std::size_t index,
                   std::size_t row_end, IngestDiagnostics *diagnostics,
                   PathId path, StrId default_key_id, bool *out_has_evidence);

template <bool Validate>
/// Updates named stats from one observation while retaining existing inferred
/// shape constraints.
sanitize::Status
update_named_stats(InferenceContext *ctx, StatsNode *parent,
                   const InferenceEvidencePacket &packet, std::size_t index,
                   std::size_t row_end, IngestDiagnostics *diagnostics,
                   PathId parent_path, StrId key_id, StrId default_key_id,
                   bool *out_has_evidence) {
  if constexpr (Validate) {
    SAN_RETURN_NOT_OK(validate_node(packet, index, row_end));
  }
  const auto &node = packet.nodes[index];
  *out_has_evidence = false;
  if (is_empty_container(node, index)) {
    return sanitize::Status::OK();
  }
  if (node.kind == EvidenceKind::kFlattened) {
    mark_flattened_stats(ctx, parent, diagnostics, key_id,
                         packet.keys.View(node.key_index));
    *out_has_evidence = true;
    return sanitize::Status::OK();
  }
  StatsNode *child_stats = parent->child(key_id, &ctx->arena);
  const PathId path = ctx->paths.child(parent_path, key_id);
  return update_stats_value<Validate>(ctx, child_stats, packet, index, row_end,
                                      diagnostics, path, default_key_id,
                                      out_has_evidence);
}

template <bool Validate>
/// Updates object stats from one observation while retaining existing inferred
/// shape constraints.
sanitize::Status
update_object_stats(InferenceContext *ctx, StatsNode *stats,
                    const InferenceEvidencePacket &packet, std::size_t index,
                    std::size_t row_end, IngestDiagnostics *diagnostics,
                    PathId path, StrId default_key_id, bool *out_has_evidence) {
  bool has_field_evidence = false;
  std::size_t child = index + 1;
  while (child < packet.nodes[index].subtree_end) {
    if constexpr (Validate) {
      SAN_RETURN_NOT_OK(validate_node(packet, child, row_end));
    }
    const auto &child_node = packet.nodes[child];
    if constexpr (Validate) {
      if (packet.keys.View(child_node.key_index).empty()) {
        return sanitize::Status::Invalid(
            "parallel inference object evidence is missing a field key");
      }
    }
    const StrId key_id =
        packet.keys.Resolve(child_node.key_index, &ctx->strings);
    bool field_has_evidence = false;
    SAN_RETURN_NOT_OK(update_named_stats<Validate>(
        ctx, stats, packet, child, row_end, diagnostics, path, key_id,
        default_key_id, &field_has_evidence));
    has_field_evidence |= field_has_evidence;
    child = child_node.subtree_end;
  }
  if (child != packet.nodes[index].subtree_end) {
    return sanitize::Status::Invalid(
        "parallel inference object evidence has overlapping children");
  }
  *out_has_evidence = has_field_evidence;
  return sanitize::Status::OK();
}

template <bool Validate>
/// Updates array stats from one observation while retaining existing inferred
/// shape constraints.
sanitize::Status
update_array_stats(InferenceContext *ctx, StatsNode *element_stats,
                   const InferenceEvidencePacket &packet, std::size_t index,
                   std::size_t row_end, IngestDiagnostics *diagnostics,
                   PathId element_path, StrId default_key_id,
                   bool *out_has_evidence) {
  bool has_element_evidence = false;
  std::size_t child = index + 1;
  while (child < packet.nodes[index].subtree_end) {
    if constexpr (Validate) {
      SAN_RETURN_NOT_OK(validate_node(packet, child, row_end));
    }
    bool element_has_evidence = false;
    SAN_RETURN_NOT_OK(update_stats_value<Validate>(
        ctx, element_stats, packet, child, row_end, diagnostics, element_path,
        default_key_id, &element_has_evidence));
    has_element_evidence |= element_has_evidence;
    child = packet.nodes[child].subtree_end;
  }
  if (child != packet.nodes[index].subtree_end) {
    return sanitize::Status::Invalid(
        "parallel inference array evidence has overlapping children");
  }
  *out_has_evidence = has_element_evidence;
  return sanitize::Status::OK();
}

template <bool Validate>
/// Updates stats value from one observation while retaining existing inferred
/// shape constraints.
sanitize::Status
update_stats_value(InferenceContext *ctx, StatsNode *stats,
                   const InferenceEvidencePacket &packet, std::size_t index,
                   std::size_t row_end, IngestDiagnostics *diagnostics,
                   PathId path, StrId default_key_id, bool *out_has_evidence) {
  if (!out_has_evidence) {
    return sanitize::Status::Invalid(
        "update_stats_value: out_has_evidence is null");
  }
  if constexpr (Validate) {
    SAN_RETURN_NOT_OK(validate_node(packet, index, row_end));
  }
  const auto &node = packet.nodes[index];
  *out_has_evidence = false;
  if (node.kind == EvidenceKind::kNull) {
    return sanitize::Status::OK();
  }

  const Shape &shape = ctx->shape(path);
  if (shape.seen_list) {
    stats->is_list = true;
    StatsNode *element_stats = stats->list_elem(&ctx->arena);
    const PathId element_path = ctx->paths.child(path, ctx->list_marker);
    bool element_has_evidence = false;
    sanitize::Status status = sanitize::Status::OK();
    if (node.kind == EvidenceKind::kArray) {
      status = update_array_stats<Validate>(
          ctx, element_stats, packet, index, row_end, diagnostics, element_path,
          default_key_id, &element_has_evidence);
    } else {
      status = update_stats_value<Validate>(
          ctx, element_stats, packet, index, row_end, diagnostics, element_path,
          default_key_id, &element_has_evidence);
      if (status.ok() && diagnostics) {
        diagnostics->scalar_wrappings++;
      }
    }
    if (status.ok() && element_has_evidence) {
      stats->has_evidence = true;
    }
    *out_has_evidence = element_has_evidence;
    return status;
  }

  if (shape.seen_struct) {
    stats->is_struct = true;
    bool child_has_evidence = false;
    sanitize::Status status = sanitize::Status::OK();
    if (node.kind == EvidenceKind::kObject) {
      status = update_object_stats<Validate>(ctx, stats, packet, index, row_end,
                                             diagnostics, path, default_key_id,
                                             &child_has_evidence);
    } else {
      StatsNode *child_stats = stats->child(default_key_id, &ctx->arena);
      const PathId child_path = ctx->paths.child(path, default_key_id);
      status = update_stats_value<Validate>(
          ctx, child_stats, packet, index, row_end, diagnostics, child_path,
          default_key_id, &child_has_evidence);
      if (status.ok() && diagnostics) {
        diagnostics->scalar_wrappings++;
      }
    }
    if (status.ok() && child_has_evidence) {
      stats->has_evidence = true;
    }
    *out_has_evidence = child_has_evidence;
    return status;
  }

  if (node.kind == EvidenceKind::kArray) {
    StatsNode *element_stats = stats->list_elem(&ctx->arena);
    const PathId element_path = ctx->paths.child(path, ctx->list_marker);
    bool has_element_evidence = false;
    SAN_RETURN_NOT_OK(update_array_stats<Validate>(
        ctx, element_stats, packet, index, row_end, diagnostics, element_path,
        default_key_id, &has_element_evidence));
    if (has_element_evidence) {
      stats->is_list = true;
      stats->has_evidence = true;
    }
    *out_has_evidence = has_element_evidence;
    return sanitize::Status::OK();
  }

  if (node.kind == EvidenceKind::kObject) {
    bool has_field_evidence = false;
    SAN_RETURN_NOT_OK(update_object_stats<Validate>(
        ctx, stats, packet, index, row_end, diagnostics, path, default_key_id,
        &has_field_evidence));
    if (has_field_evidence) {
      stats->is_struct = true;
      stats->has_evidence = true;
    }
    *out_has_evidence = has_field_evidence;
    return sanitize::Status::OK();
  }

  if (node.kind == EvidenceKind::kFlattened) {
    return sanitize::Status::Invalid(
        "parallel inference flattened evidence is not attached to a field");
  }
  stats->scalar_kind_mask |= node.scalar_kind_mask;
  stats->has_evidence = true;
  *out_has_evidence = true;
  return sanitize::Status::OK();
}

/// Reduces one packet's shape evidence into global inference state in source
/// order.
sanitize::Status reduce_shape_row(InferenceContext *ctx,
                                  const InferenceEvidencePacket &packet,
                                  const InferenceEvidenceRow &row,
                                  IngestDiagnostics *diagnostics) {
  std::size_t index = row.begin;
  while (index < row.end) {
    SAN_RETURN_NOT_OK(validate_node(packet, index, row.end));
    const auto &node = packet.nodes[index];
    if (packet.keys.View(node.key_index).empty()) {
      return sanitize::Status::Invalid(
          "parallel inference root evidence is missing a field key");
    }
    if (!is_empty_container(node, index)) {
      const StrId key_id = packet.keys.Resolve(node.key_index, &ctx->strings);
      bool has_evidence = false;
      SAN_RETURN_NOT_OK(scan_named_shape(ctx, packet, index, row.end,
                                         diagnostics, PathInterner::root(),
                                         key_id, &has_evidence));
    }
    index = node.subtree_end;
  }
  if (index != row.end) {
    return sanitize::Status::Invalid(
        "parallel inference row evidence has overlapping root fields");
  }
  return sanitize::Status::OK();
}

/// Reduces one evidence row into scalar statistics, optionally repeating
/// structural validation for lower-concurrency traversal.
template <bool Validate>
sanitize::Status reduce_stats_row(InferenceContext *ctx,
                                  const InferenceEvidencePacket &packet,
                                  const InferenceEvidenceRow &row,
                                  IngestDiagnostics *diagnostics) {
  std::size_t index = row.begin;
  while (index < row.end) {
    if constexpr (Validate) {
      SAN_RETURN_NOT_OK(validate_node(packet, index, row.end));
    }
    const auto &node = packet.nodes[index];
    if constexpr (Validate) {
      if (packet.keys.View(node.key_index).empty()) {
        return sanitize::Status::Invalid(
            "parallel inference root evidence is missing a field key");
      }
    }
    if (!is_empty_container(node, index)) {
      const StrId key_id = packet.keys.Resolve(node.key_index, &ctx->strings);
      bool has_evidence = false;
      SAN_RETURN_NOT_OK(update_named_stats<Validate>(
          ctx, &ctx->root, packet, index, row.end, diagnostics,
          PathInterner::root(), key_id, ctx->default_key_id, &has_evidence));
    }
    index = node.subtree_end;
  }
  if (index != row.end) {
    return sanitize::Status::Invalid(
        "parallel inference row evidence has overlapping root fields");
  }
  return sanitize::Status::OK();
}

} // namespace

sanitize::Status reduce_inference_evidence_row(
    InferenceContext *ctx, const InferenceEvidencePacket &packet,
    const InferenceEvidenceRow &row, const PreparedOptions &,
    IngestDiagnostics *diagnostics) {
  if (!ctx || row.begin > row.end || row.end > packet.nodes.size()) {
    return sanitize::Status::Invalid(
        "reduce_inference_evidence_row: invalid arguments");
  }
  SAN_RETURN_NOT_OK(reduce_shape_row(ctx, packet, row, diagnostics));
  if (packet.trusted_stats_reduction) {
    return reduce_stats_row<false>(ctx, packet, row, diagnostics);
  }
  return reduce_stats_row<true>(ctx, packet, row, diagnostics);
}

} // namespace sanitize::internal
