// Implements compact worker-local inference evidence collection.
// The code keeps bounded shape discovery and scalar evidence consistent across
// serial and parallel scans.

#include "internal/inference/parallel_evidence.hh"

#include "internal/inference/depth.hh"
#include "internal/inference/parallel_flat_evidence.hh"
#include "internal/inference/value_observation.hh"
#include "internal/parsing/json/ondemand/document.hh"
#include "internal/parsing/string_scalar.hh"

#include <algorithm>
#include <cctype>
#include <limits>
#include <new>
#include <string_view>
#include <utility>

namespace sanitize::internal {

namespace {

using EvidenceKind = InferenceEvidenceNode::Kind;

/// Allocates the next packet-local evidence node and returns its stable index.
sanitize::Result<std::size_t>
append_evidence_node(InferenceEvidencePacket *packet) {
  if (packet->nodes.size() >=
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()))
      [[unlikely]] {
    return sanitize::Status::OutOfMemory(
        "parallel inference evidence exceeds 32-bit node bounds");
  }
  const auto index = packet->nodes.size();
  packet->nodes.emplace_back(packet->arena.get());
  return index;
}

/// Returns the stable packet-local index of an evidence node within its owning
/// vector.
[[nodiscard]] std::uint32_t evidence_index(std::size_t value) noexcept {
  return static_cast<std::uint32_t>(value);
}

sanitize::Status append_value(InferenceEvidencePacket *packet,
                              std::size_t node_index, const ValueView &value,
                              const PreparedOptions &opts, DepthState depth,
                              sanitize::internal::StopToken stop);

/// Adds a named value and its recursively collected shape evidence to the
/// packet.
sanitize::Status append_named_value(InferenceEvidencePacket *packet,
                                    std::string_view key,
                                    const ValueView &value,
                                    const PreparedOptions &opts,
                                    DepthState parent_depth,
                                    sanitize::internal::StopToken stop) {
  SAN_ASSIGN_OR_RAISE(const auto index, append_evidence_node(packet));
  SAN_ASSIGN_OR_RAISE(packet->nodes[index].key_index, packet->keys.Intern(key));

  bool empty = false;
  SAN_RETURN_NOT_OK(value.container_is_empty(&empty));
  if (empty) {
    packet->nodes[index].kind =
        value.is_array() ? EvidenceKind::kArray : EvidenceKind::kObject;
    packet->nodes[index].subtree_end = evidence_index(index + 1U);
    return sanitize::Status::OK();
  }
  if (should_flatten_nested(value, opts, parent_depth)) {
    packet->nodes[index].kind = EvidenceKind::kFlattened;
    packet->nodes[index].subtree_end = evidence_index(index + 1U);
    return sanitize::Status::OK();
  }
  const auto depth = value.is_array() || value.is_object()
                         ? enter_value_depth(parent_depth, value)
                         : parent_depth;
  return append_value(packet, index, value, opts, depth, stop);
}

struct ObjectCollectContext {
  InferenceEvidencePacket *packet = nullptr;
  const PreparedOptions *opts = nullptr;
  DepthState depth;
  sanitize::internal::StopToken stop;
};

/// Collects one object field through the allocation-free value-view callback.
sanitize::Status append_object_field(void *raw, std::string_view key,
                                     std::uint64_t, ValueView value) {
  auto *context = static_cast<ObjectCollectContext *>(raw);
  if (context->stop.stop_requested()) {
    return sanitize::Status::Cancelled(
        "parallel inference evidence collection stopped");
  }
  return append_named_value(context->packet, key, value, *context->opts,
                            context->depth, context->stop);
}

struct ArrayCollectContext {
  InferenceEvidencePacket *packet = nullptr;
  const PreparedOptions *opts = nullptr;
  DepthState depth;
  sanitize::internal::StopToken stop;
};

/// Adds one array element and its descendants to the packet's preorder node
/// sequence.
sanitize::Status append_array_element(void *raw, ValueView value) {
  auto *context = static_cast<ArrayCollectContext *>(raw);
  if (context->stop.stop_requested()) {
    return sanitize::Status::Cancelled(
        "parallel inference evidence collection stopped");
  }
  SAN_ASSIGN_OR_RAISE(const auto index, append_evidence_node(context->packet));
  return append_value(context->packet, index, value, *context->opts,
                      enter_value_depth(context->depth, value), context->stop);
}

/// Classifies one value and recursively records its object or array
/// descendants.
sanitize::Status append_value(InferenceEvidencePacket *packet,
                              std::size_t node_index, const ValueView &value,
                              const PreparedOptions &opts, DepthState depth,
                              sanitize::internal::StopToken stop) {
  if (stop.stop_requested()) {
    return sanitize::Status::Cancelled(
        "parallel inference evidence collection stopped");
  }
  if (value.is_null()) {
    packet->nodes[node_index].kind = EvidenceKind::kNull;
    packet->nodes[node_index].subtree_end = evidence_index(node_index + 1U);
    return sanitize::Status::OK();
  }
  if (!value.is_array() && !value.is_object()) {
    packet->nodes[node_index].kind = EvidenceKind::kScalar;
    packet->nodes[node_index].scalar_kind_mask = infer_scalar_mask(value, opts);
    packet->nodes[node_index].subtree_end = evidence_index(node_index + 1U);
    return sanitize::Status::OK();
  }
  if (value.is_array()) {
    packet->nodes[node_index].kind = EvidenceKind::kArray;
    ArrayCollectContext context{
        .packet = packet, .opts = &opts, .depth = depth, .stop = stop};
    SAN_RETURN_NOT_OK(value.for_each_array_element([&](ValueView child) {
      return append_array_element(&context, child);
    }));
  } else {
    packet->nodes[node_index].kind = EvidenceKind::kObject;
    ObjectCollectContext context{
        .packet = packet, .opts = &opts, .depth = depth, .stop = stop};
    SAN_RETURN_NOT_OK(value.for_each_object_field(
        [&](std::string_view key, std::uint64_t hash, ValueView child) {
          return append_object_field(&context, key, hash, child);
        }));
  }
  packet->nodes[node_index].subtree_end = evidence_index(packet->nodes.size());
  return sanitize::Status::OK();
}

/// Collects shape and scalar evidence from every field in one materialized row.
sanitize::Status append_materialized_row(const RowRef &row,
                                         const PreparedOptions &opts,
                                         InferenceEvidencePacket *packet,
                                         sanitize::internal::StopToken stop) {
  constexpr DepthState root_depth{};
  for (std::size_t index = 0; index < row.size; ++index) {
    if (stop.stop_requested()) {
      return sanitize::Status::Cancelled(
          "parallel inference evidence collection stopped");
    }
    SAN_RETURN_NOT_OK(append_named_value(packet, row.fields[index].key,
                                         row.fields[index].value, opts,
                                         root_depth, stop));
  }
  return sanitize::Status::OK();
}

/// Removes JSON whitespace from both ends of a source slice without allocating.
[[nodiscard]] std::string_view trim_json_ws(std::string_view value) noexcept {
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.front())) != 0) {
    value.remove_prefix(1);
  }
  return value;
}

struct JsonRootContext {
  InferenceEvidencePacket *packet = nullptr;
  const PreparedOptions *opts = nullptr;
  sanitize::internal::StopToken stop;
};

/// Collects one root-object field through the on-demand JSON callback.
sanitize::Status append_json_root_field(void *raw, std::string_view key,
                                        std::uint64_t hash, ValueView value) {
  auto *context = static_cast<JsonRootContext *>(raw);
  ObjectCollectContext object_context{.packet = context->packet,
                                      .opts = context->opts,
                                      .depth = DepthState{},
                                      .stop = context->stop};
  return append_object_field(&object_context, key, hash, value);
}

/// Parses raw JSON when available and collects one row's recursive inference
/// evidence.
sanitize::Status append_json_row(const RowRef &row, const PreparedOptions &opts,
                                 JsonOnDemandDoc *document,
                                 InferenceEvidencePacket *packet,
                                 sanitize::internal::StopToken stop) {
  if (row.raw.empty()) {
    return append_materialized_row(row, opts, packet, stop);
  }
  document->Reset();
  const auto probe = trim_json_ws(row.raw);
  if (!probe.empty() && probe.front() == '{') {
    JsonRootContext context{.packet = packet, .opts = &opts, .stop = stop};
    return document->ForEachObjectFieldC(
        row.raw, &context, &append_json_root_field, row.base_offset);
  }
  SAN_ASSIGN_OR_RAISE(auto value,
                      document->ParseValue(row.raw, row.base_offset));
  return append_named_value(packet, opts.spec.default_key_name, value, opts,
                            DepthState{}, stop);
}

/// Derives the inference evidence packet byte limit from the operation
/// execution policy.
[[nodiscard]] std::int64_t
packet_limit_from_policy(const ExecutionPolicy &policy) noexcept {
  // Each completed evidence packet can remain in the reorder window while its
  // worker starts another task. Give one packet at most one effective worker
  // arena and rely on the operation parent pool as the final aggregate guard.
  return std::max<std::int64_t>(1, policy.worker_arena_bytes);
}

} // namespace

struct ParallelInferenceEvidenceBuilder::WorkerState {
  std::shared_ptr<MemoryPool> parser_pool;
  std::shared_ptr<PoolResource> parser_resource;
  std::unique_ptr<JsonOnDemandDoc> json_document;
};

ParallelInferenceEvidenceBuilder::ParallelInferenceEvidenceBuilder(
    std::string frontend_name, const PreparedOptions *opts,
    std::shared_ptr<MemoryPool> parent_pool, std::int64_t packet_memory_limit)
    : frontend_name_(std::move(frontend_name)), opts_(opts),
      parent_pool_(std::move(parent_pool)),
      packet_memory_limit_(packet_memory_limit),
      parse_json_raw_(frontend_name_ == "json" || frontend_name_ == "jsonl" ||
                      frontend_name_ == "json_array") {}

/// Releases worker-local inference arenas and any packet reservation still
/// held.
ParallelInferenceEvidenceBuilder::~ParallelInferenceEvidenceBuilder() = default;

/// Validates dependencies and constructs the packet-local parallel inference
/// evidence builder.
sanitize::Result<std::shared_ptr<ParallelInferenceEvidenceBuilder>>
ParallelInferenceEvidenceBuilder::Make(
    std::string_view frontend_name, const PreparedOptions *opts,
    std::shared_ptr<void> operation_memory_pool,
    const ExecutionPolicy &policy) {
  if (!opts || !operation_memory_pool || policy.effective_workers <= 1) {
    return sanitize::Status::Invalid(
        "ParallelInferenceEvidenceBuilder::Make: invalid arguments");
  }
  auto parent = std::static_pointer_cast<MemoryPool>(operation_memory_pool);
  auto builder = std::shared_ptr<ParallelInferenceEvidenceBuilder>(
      new (std::nothrow) ParallelInferenceEvidenceBuilder(
          std::string(frontend_name), opts, parent,
          packet_limit_from_policy(policy)));
  if (!builder) {
    return sanitize::Status::OutOfMemory(
        "ParallelInferenceEvidenceBuilder::Make: allocation failed");
  }
  try {
    builder->workers_.reserve(
        static_cast<std::size_t>(policy.effective_workers));
    for (std::int64_t index = 0; index < policy.effective_workers; ++index) {
      auto worker = std::make_unique<WorkerState>();
      worker->parser_pool =
          make_tracking_memory_pool(parent, policy.worker_arena_bytes,
                                    "schema_sanitizer::InferenceParserWorker[" +
                                        std::to_string(index) + "]");
      worker->parser_resource =
          std::make_shared<PoolResource>(worker->parser_pool);
      if (builder->parse_json_raw_) {
        worker->json_document =
            std::make_unique<JsonOnDemandDoc>(worker->parser_resource.get());
      }
      builder->workers_.push_back(std::move(worker));
    }
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "ParallelInferenceEvidenceBuilder::Make: worker allocation failed");
  }
  return builder;
}

sanitize::Status ParallelInferenceEvidenceBuilder::append_row(
    const RowRef &row, WorkerState *worker, InferenceEvidencePacket *packet,
    sanitize::internal::StopToken stop) const {
  const auto begin = packet->nodes.size();
  if (parse_json_raw_ && worker->json_document) {
    SAN_RETURN_NOT_OK(append_json_row(row, *opts_, worker->json_document.get(),
                                      packet, stop));
  } else {
    SAN_RETURN_NOT_OK(append_materialized_row(row, *opts_, packet, stop));
  }
  if (row.raw.size() >
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()))
      [[unlikely]] {
    return sanitize::Status::OutOfMemory(
        "parallel inference row exceeds 32-bit source-byte bounds");
  }
  packet->rows.push_back(InferenceEvidenceRow{
      .begin = evidence_index(begin),
      .end = evidence_index(packet->nodes.size()),
      .source_bytes = static_cast<std::uint32_t>(row.raw.size())});
  return sanitize::Status::OK();
}

sanitize::Status ParallelInferenceEvidenceBuilder::append_flat_row(
    const RowRef &row, WorkerState *worker, InferenceEvidencePacket *packet,
    sanitize::internal::StopToken stop) const {
  if (stop.stop_requested()) {
    return sanitize::Status::Cancelled(
        "parallel inference evidence collection stopped");
  }

  if (parse_json_raw_ && worker->json_document && !row.raw.empty()) {
    worker->json_document->Reset();
    const auto probe = trim_json_ws(row.raw);
    if (!probe.empty() && probe.front() == '{') {
      SAN_RETURN_NOT_OK(append_flat_json_inference_row(
          worker->json_document.get(), row.raw, row.base_offset, packet, *opts_,
          parent_pool_, packet_memory_limit_, stop));
    } else {
      SAN_ASSIGN_OR_RAISE(auto value, worker->json_document->ParseValue(
                                          row.raw, row.base_offset));
      std::size_t ordered_index = 0;
      SAN_RETURN_NOT_OK(append_flat_inference_value(
          packet, opts_->spec.default_key_name, value, *opts_, stop,
          &ordered_index, parent_pool_, packet_memory_limit_));
    }
  } else {
    std::size_t ordered_index = 0;
    for (std::size_t index = 0; index < row.size; ++index) {
      SAN_RETURN_NOT_OK(append_flat_inference_value(
          packet, row.fields[index].key, row.fields[index].value, *opts_, stop,
          &ordered_index, parent_pool_, packet_memory_limit_));
    }
  }

  if (packet->flat_row_count < std::numeric_limits<std::size_t>::max()) {
    ++packet->flat_row_count;
  }
  const auto remaining =
      std::numeric_limits<std::size_t>::max() - packet->flat_source_bytes;
  packet->flat_source_bytes = row.raw.size() > remaining
                                  ? std::numeric_limits<std::size_t>::max()
                                  : packet->flat_source_bytes + row.raw.size();
  return sanitize::Status::OK();
}

/// Consumes the accumulated packet-local evidence and returns an immutable
/// inference packet.
sanitize::Result<InferenceEvidencePacket>
ParallelInferenceEvidenceBuilder::Build(OwnedRowPacket &&owned,
                                        std::size_t worker_index,
                                        sanitize::internal::StopToken stop) {
  if (worker_index >= workers_.size() || owned.rows.empty()) {
    return sanitize::Status::Invalid(
        "ParallelInferenceEvidenceBuilder::Build: invalid packet");
  }

  auto *worker = workers_[worker_index].get();
  const bool flat_jsonl_candidate = frontend_name_ == "jsonl";
  if (flat_jsonl_candidate) {
    InferenceEvidencePacket flat_packet;
    try {
      flat_packet.flat_storage = std::make_unique<FlatInferenceStorage>();
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "ParallelInferenceEvidenceBuilder::Build: flat storage failed");
    }
    bool fallback_to_generic = false;
    for (const auto &row : owned.rows) {
      const auto status = append_flat_row(row, worker, &flat_packet, stop);
      if (status.code() == sanitize::StatusCode::kNotImplemented) {
        fallback_to_generic = true;
        break;
      }
      SAN_RETURN_NOT_OK(status);
    }
    if (!fallback_to_generic) {
      flat_packet.flat_scalar_aggregate = true;
      return flat_packet;
    }
  }

  auto packet_pool =
      make_tracking_memory_pool(parent_pool_, packet_memory_limit_,
                                "schema_sanitizer::InferenceEvidencePacket",
                                /*thread_safe_registry=*/false);
  auto packet_resource = std::make_shared<PoolResource>(packet_pool);
  InferenceEvidencePacket packet(packet_pool, packet_resource);
  packet.trusted_stats_reduction = workers_.size() >= 4U;
  try {
    packet.rows.reserve(owned.rows.size());
    const auto estimated_nodes = std::max<std::size_t>(
        owned.rows.size(), owned.estimated_source_bytes / 24);
    const auto node_budget = std::max<std::size_t>(
        owned.rows.size(), static_cast<std::size_t>(packet_memory_limit_) /
                               (sizeof(InferenceEvidenceNode) * 2));
    packet.nodes.reserve(
        std::min({estimated_nodes, node_budget, std::size_t{1U << 20}}));
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "ParallelInferenceEvidenceBuilder::Build: generic reserve failed");
  }

  for (const auto &row : owned.rows) {
    SAN_RETURN_NOT_OK(append_row(row, worker, &packet, stop));
  }
  return packet;
}

} // namespace sanitize::internal
