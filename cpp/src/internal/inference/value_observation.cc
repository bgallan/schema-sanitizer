// Implements value observation helpers shared by shape and statistics scans.
// The code keeps bounded shape discovery and scalar evidence consistent across
// serial and parallel scans.

#include "internal/inference/value_observation.hh"

#include <string>

#include "internal/parsing/string_scalar.hh"

namespace sanitize::internal {

uint32_t infer_scalar_mask(const ValueView &value,
                           const PreparedOptions &opts) {
  switch (value.tag()) {
  case ValueView::Tag::kNull:
    return 0;
  case ValueView::Tag::kBool:
    return K_BOOL;
  case ValueView::Tag::kInt:
    return K_INT;
  case ValueView::Tag::kFloat:
    return K_FLOAT;
  case ValueView::Tag::kString:
    return infer_scalar_mask_from_string(value.as_string_view(), opts);
  case ValueView::Tag::kObject:
  case ValueView::Tag::kArray:
    // Objects/arrays are stringified.
    return K_STR;
  }
  return K_STR;
}

StrId flattened_key_id(InferenceContext *ctx, StrId key_id,
                       std::string_view key) {
  if (auto it = ctx->flattened_key_cache.find(key_id);
      it != ctx->flattened_key_cache.end()) {
    return it->second;
  }
  std::string flat_key(key);
  flat_key.append("_flattened");
  StrId out = ctx->strings.intern(flat_key);
  ctx->flattened_key_cache.emplace(key_id, out);
  return out;
}

void mark_flattened_shape(InferenceContext *ctx, IngestDiagnostics *diag,
                          PathId parent_path, StrId key_id,
                          std::string_view key) {
  if (diag)
    diag->flattened_fields++;
  const PathId p =
      ctx->paths.child(parent_path, flattened_key_id(ctx, key_id, key));
  ctx->ensure_shape(p);
}

void mark_flattened_stats(InferenceContext *ctx, StatsNode *parent,
                          IngestDiagnostics *diag, StrId key_id,
                          std::string_view key) {
  if (diag)
    diag->flattened_fields++;
  StatsNode *ch =
      parent->child(flattened_key_id(ctx, key_id, key), &ctx->arena);
  ch->scalar_kind_mask |= K_STR;
  ch->has_evidence = true;
}

} // namespace sanitize::internal
