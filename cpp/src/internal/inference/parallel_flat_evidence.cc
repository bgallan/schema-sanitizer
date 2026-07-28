// Implements bounded flat packet-local inference helpers.

#include "internal/inference/parallel_flat_evidence.hh"

#include "internal/parsing/string_scalar.hh"

namespace sanitize::internal {
namespace {

sanitize::Status append_mask(InferenceEvidencePacket *packet,
                             std::string_view key,
                             std::uint32_t scalar_kind_mask,
                             std::size_t *ordered_index,
                             const std::shared_ptr<MemoryPool> &parent_pool,
                             std::int64_t packet_memory_limit) {
  if (!packet || !packet->flat_storage || !ordered_index) {
    return sanitize::Status::Invalid(
        "flat inference packet storage is unavailable");
  }
  auto &storage = *packet->flat_storage;
  const auto expected = *ordered_index;
  if (expected < storage.field_count) {
    auto &field = storage.field(expected);
    if (field.matches(key)) {
      field.scalar_kind_mask |= scalar_kind_mask;
      ++*ordered_index;
      return sanitize::Status::OK();
    }
  }
  for (std::size_t index = 0; index < storage.field_count; ++index) {
    auto &field = storage.field(index);
    if (field.matches(key)) {
      field.scalar_kind_mask |= scalar_kind_mask;
      ++*ordered_index;
      return sanitize::Status::OK();
    }
  }
  SAN_RETURN_NOT_OK(storage.ensure_field(storage.field_count, parent_pool,
                                         packet_memory_limit));
  auto &field = storage.field(storage.field_count);
  if (!field.assign_key(key)) {
    return sanitize::Status::NotImplemented(
        "flat inference key exceeds inline capacity");
  }
  field.scalar_kind_mask = scalar_kind_mask;
  ++storage.field_count;
  ++*ordered_index;
  return sanitize::Status::OK();
}

struct FlatJsonRootContext {
  InferenceEvidencePacket *packet = nullptr;
  const PreparedOptions *opts = nullptr;
  const std::shared_ptr<MemoryPool> *parent_pool = nullptr;
  std::int64_t packet_memory_limit = 1;
  std::size_t ordered_index = 0;
  std::stop_token stop;
};

sanitize::Status append_json_field(void *raw, std::string_view key,
                                   JsonOnDemandDoc::FlatValue value) {
  auto *context = static_cast<FlatJsonRootContext *>(raw);
  if (!context->parent_pool || !context->packet || !context->opts) {
    return sanitize::Status::Invalid(
        "flat inference callback context is unavailable");
  }
  if (context->stop.stop_requested()) {
    return sanitize::Status::Cancelled(
        "parallel inference evidence collection stopped");
  }

  std::uint32_t mask = 0;
  switch (value.kind) {
  case JsonOnDemandDoc::FlatValue::Kind::kNull:
    break;
  case JsonOnDemandDoc::FlatValue::Kind::kEmptyObject:
  case JsonOnDemandDoc::FlatValue::Kind::kEmptyArray:
    // Match the reference path: empty containers contribute no evidence and
    // do not establish first-seen root field ordering.
    return sanitize::Status::OK();
  case JsonOnDemandDoc::FlatValue::Kind::kBool:
    mask = K_BOOL;
    break;
  case JsonOnDemandDoc::FlatValue::Kind::kInt:
    mask = K_INT;
    break;
  case JsonOnDemandDoc::FlatValue::Kind::kFloat:
    mask = K_FLOAT;
    break;
  case JsonOnDemandDoc::FlatValue::Kind::kString:
    mask = infer_scalar_mask_from_string(value.string_value, *context->opts);
    break;
  case JsonOnDemandDoc::FlatValue::Kind::kNestedObject:
  case JsonOnDemandDoc::FlatValue::Kind::kNestedArray:
    return sanitize::Status::NotImplemented(
        "flat inference packet contains nested evidence");
  }
  return append_mask(context->packet, key, mask, &context->ordered_index,
                     *context->parent_pool, context->packet_memory_limit);
}

} // namespace

sanitize::Status append_flat_inference_value(
    InferenceEvidencePacket *packet, std::string_view key,
    const ValueView &value, const PreparedOptions &opts, std::stop_token stop,
    std::size_t *ordered_index, const std::shared_ptr<MemoryPool> &parent_pool,
    std::int64_t packet_memory_limit) {
  if (stop.stop_requested()) {
    return sanitize::Status::Cancelled(
        "parallel inference evidence collection stopped");
  }
  std::uint32_t mask = 0;
  switch (value.tag()) {
  case ValueView::Tag::kNull:
    break;
  case ValueView::Tag::kBool:
    mask = K_BOOL;
    break;
  case ValueView::Tag::kInt:
    mask = K_INT;
    break;
  case ValueView::Tag::kFloat:
    mask = K_FLOAT;
    break;
  case ValueView::Tag::kString:
    mask = infer_scalar_mask_from_string(value.as_string_view(), opts);
    break;
  case ValueView::Tag::kObject:
  case ValueView::Tag::kArray: {
    bool empty = false;
    SAN_RETURN_NOT_OK(value.container_is_empty(&empty));
    if (empty) {
      return sanitize::Status::OK();
    }
    return sanitize::Status::NotImplemented(
        "flat inference packet contains nested evidence");
  }
  }
  return append_mask(packet, key, mask, ordered_index, parent_pool,
                     packet_memory_limit);
}

sanitize::Status append_flat_json_inference_row(
    JsonOnDemandDoc *document, std::string_view raw, std::size_t base_offset,
    InferenceEvidencePacket *packet, const PreparedOptions &opts,
    const std::shared_ptr<MemoryPool> &parent_pool,
    std::int64_t packet_memory_limit, std::stop_token stop) {
  if (!document || !packet) {
    return sanitize::Status::Invalid(
        "flat inference JSON row context is unavailable");
  }
  FlatJsonRootContext context{.packet = packet,
                              .opts = &opts,
                              .parent_pool = &parent_pool,
                              .packet_memory_limit = packet_memory_limit,
                              .stop = stop};
  return document->ForEachFlatObjectFieldC(raw, &context, &append_json_field,
                                           base_offset);
}

} // namespace sanitize::internal
