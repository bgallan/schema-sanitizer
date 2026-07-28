// Implements schema inference for ingestion preparation.

#include "ingest/prepare/prepare_internal.hh"

#include "sanitize/runtime/execution_context.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string_view>
#include <utility>

#include "internal/inference/parallel_evidence.hh"
#include "internal/inference/scan.hh"
#include "internal/inference/schema.hh"
#include "internal/inference/statistics/state.hh"
#include "internal/materialization/batch_sizing.hh"
#include "internal/materialization/ingest_stream/parallel_packets.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/runtime/execution_policy.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/ordered_executor.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"

namespace sanitize::ingest_internal {
namespace {

struct InferenceProgress {
  int64_t rows = 0;
  int64_t bytes = 0;
};

void record_inferred_bytes(InferenceProgress *progress,
                           std::size_t row_bytes) noexcept {
  if (progress->rows < std::numeric_limits<int64_t>::max()) {
    progress->rows += 1;
  }
  const auto bytes = static_cast<uint64_t>(row_bytes);
  const auto remaining = static_cast<uint64_t>(
      std::numeric_limits<int64_t>::max() - progress->bytes);
  progress->bytes = bytes > remaining
                        ? std::numeric_limits<int64_t>::max()
                        : progress->bytes + static_cast<int64_t>(bytes);
}

void record_inferred_row(InferenceProgress *progress,
                         const RowRef &row) noexcept {
  record_inferred_bytes(progress, row.raw.size());
}

sanitize::Status scan_inference_batch(internal::InferenceContext *ctx,
                                      const RowBatch &batch,
                                      const PreparedOptions &opts,
                                      IngestDiagnostics *diagnostics,
                                      InferenceProgress *progress,
                                      ExecutionContext *execution_context) {
  std::size_t interrupt_countdown = 0;
  for (const auto &row : batch.rows) {
    if (execution_context && (interrupt_countdown++ & std::size_t{1023}) == 0) {
      SAN_RETURN_NOT_OK(execution_context->CheckInterrupt());
    }
    SAN_RETURN_NOT_OK(internal::scan_shapes_row(ctx, row, opts, diagnostics));
    SAN_RETURN_NOT_OK(internal::update_stats_row(ctx, row, opts, diagnostics));
    record_inferred_row(progress, row);
  }
  return sanitize::Status::OK();
}

using InferenceExecutor =
    internal::OrderedExecutor<internal::OwnedRowPacket,
                              internal::InferenceEvidencePacket>;

[[nodiscard]] internal::ExecutionPolicy
inference_execution_policy(const internal::ExecutionPolicy &policy) noexcept {
  if (policy.effective_workers <= 1) {
    return policy;
  }
  const auto inference_workers =
      std::min(policy.effective_workers,
               std::max<std::int64_t>(2, policy.effective_workers / 4));
  internal::ExecutionPolicy out =
      internal::execution_policy_with_worker_ceiling(policy, inference_workers,
                                                     1);
  out.effective_workers = std::max<std::int64_t>(2, out.effective_workers);
  out.task_queue_capacity = out.effective_workers;
  out.reorder_capacity = out.effective_workers;
  const auto max = std::numeric_limits<std::int64_t>::max();
  const auto total_worker_bytes =
      policy.worker_arena_bytes > max / policy.effective_workers
          ? max
          : policy.worker_arena_bytes * policy.effective_workers;
  out.worker_arena_bytes =
      std::max<std::int64_t>(1, total_worker_bytes / out.effective_workers);
  return out;
}

sanitize::Status scan_owned_inference_packet(
    internal::InferenceContext *ctx, const internal::OwnedRowPacket &packet,
    const PreparedOptions &opts, IngestDiagnostics *diagnostics,
    InferenceProgress *progress, ExecutionContext *execution_context) {
  std::size_t interrupt_countdown = 0;
  for (const auto &row : packet.rows) {
    if (execution_context && (interrupt_countdown++ & std::size_t{1023}) == 0) {
      SAN_RETURN_NOT_OK(execution_context->CheckInterrupt());
    }
    SAN_RETURN_NOT_OK(internal::scan_shapes_row(ctx, row, opts, diagnostics));
    SAN_RETURN_NOT_OK(internal::update_stats_row(ctx, row, opts, diagnostics));
    record_inferred_row(progress, row);
  }
  return sanitize::Status::OK();
}

sanitize::Status
reduce_flat_evidence_packet(internal::InferenceContext *ctx,
                            const internal::InferenceEvidencePacket &packet,
                            InferenceProgress *progress,
                            ExecutionContext *execution_context) {
  if (!ctx || !packet.flat_scalar_aggregate) {
    return sanitize::Status::Invalid(
        "reduce_flat_evidence_packet: invalid arguments");
  }
  if (execution_context) {
    SAN_RETURN_NOT_OK(execution_context->CheckInterrupt());
  }
  if (!packet.flat_storage) {
    return sanitize::Status::Invalid(
        "reduce_flat_evidence_packet: flat storage is unavailable");
  }
  for (std::size_t index = 0; index < packet.flat_storage->field_count;
       ++index) {
    const auto &field = packet.flat_storage->field(index);
    const auto key_id = ctx->strings.intern(field.key());
    (void)ctx->paths.child(internal::PathInterner::root(), key_id);
    auto *stats = ctx->root.child(key_id, &ctx->arena);
    stats->scalar_kind_mask |= field.scalar_kind_mask;
    stats->has_evidence |= field.scalar_kind_mask != 0;
  }

  const auto row_room = static_cast<std::uint64_t>(
      std::numeric_limits<int64_t>::max() - progress->rows);
  const auto packet_rows = static_cast<std::uint64_t>(packet.flat_row_count);
  progress->rows = packet_rows > row_room
                       ? std::numeric_limits<int64_t>::max()
                       : progress->rows + static_cast<int64_t>(packet_rows);

  const auto byte_room = static_cast<std::uint64_t>(
      std::numeric_limits<int64_t>::max() - progress->bytes);
  const auto packet_bytes =
      static_cast<std::uint64_t>(packet.flat_source_bytes);
  progress->bytes = packet_bytes > byte_room
                        ? std::numeric_limits<int64_t>::max()
                        : progress->bytes + static_cast<int64_t>(packet_bytes);
  return sanitize::Status::OK();
}

sanitize::Status reduce_evidence_packet(
    internal::InferenceContext *ctx,
    const internal::InferenceEvidencePacket &packet,
    const PreparedOptions &opts, IngestDiagnostics *diagnostics,
    InferenceProgress *progress, ExecutionContext *execution_context) {
  if (packet.flat_scalar_aggregate) {
    return reduce_flat_evidence_packet(ctx, packet, progress,
                                       execution_context);
  }
  std::size_t interrupt_countdown = 0;
  for (const auto &row : packet.rows) {
    if (execution_context && (interrupt_countdown++ & std::size_t{1023}) == 0) {
      SAN_RETURN_NOT_OK(execution_context->CheckInterrupt());
    }
    SAN_RETURN_NOT_OK(internal::reduce_inference_evidence_row(
        ctx, packet, row, opts, diagnostics));
    record_inferred_bytes(progress, row.source_bytes);
  }
  return sanitize::Status::OK();
}

[[nodiscard]] std::size_t saturating_add(std::size_t left,
                                         std::size_t right) noexcept {
  const auto max = std::numeric_limits<std::size_t>::max();
  return right > max - left ? max : left + right;
}

struct InferenceBatchProfile {
  std::size_t rows = 0;
  std::size_t source_bytes = 0;
  std::size_t sampled_rows = 0;
  std::size_t nested_values = 0;
};

[[nodiscard]] InferenceBatchProfile
profile_inference_batch(std::string_view, const RowBatch &batch) noexcept {
  constexpr std::size_t kMaxSampleRows = 256;
  InferenceBatchProfile profile;
  profile.rows = batch.rows.size();
  const auto sample_count = std::min(batch.rows.size(), kMaxSampleRows);
  for (std::size_t row_index = 0; row_index < sample_count; ++row_index) {
    const auto &row = batch.rows[row_index];
    ++profile.sampled_rows;
    const auto estimated_row_bytes =
        !row.raw.empty()
            ? row.raw.size()
            : saturating_add(sizeof(RowRef), row.size * sizeof(FieldRef));
    profile.source_bytes =
        saturating_add(profile.source_bytes, estimated_row_bytes);
    for (std::size_t field = 0; field < row.size; ++field) {
      const auto &value = row.fields[field].value;
      if (value.is_array() || value.is_object()) {
        ++profile.nested_values;
      }
    }
  }
  if (profile.sampled_rows != 0 && profile.sampled_rows < profile.rows) {
    const auto max = std::numeric_limits<std::size_t>::max();
    profile.source_bytes =
        profile.source_bytes > max / profile.rows
            ? max
            : (profile.source_bytes * profile.rows) / profile.sampled_rows;
  }
  return profile;
}

[[nodiscard]] bool
should_parallelize_inference(std::string_view frontend_name,
                             const RowBatch &batch,
                             const internal::ExecutionPolicy &policy) noexcept {
  constexpr std::size_t kMinimumRows = 2048;
  constexpr std::size_t kMinimumBytes = 512U * 1024U;
  constexpr std::size_t kLargeBatchBytes = 2U * 1024U * 1024U;
  if (policy.effective_workers <= 1 || batch.rows.empty()) {
    return false;
  }
  const bool raw_only_jsonl =
      frontend_name == "jsonl" &&
      (batch.rows.front().flags & std::to_underlying(RowFlags::kRawOnly)) != 0;
  // The JSONL frontend can intentionally defer parsing to inference workers.
  // Such batches have no materialized fields, so they must use the raw-aware
  // evidence builder even when the input is too small for the normal
  // parallelism threshold.
  if (raw_only_jsonl) {
    return true;
  }
  constexpr std::int64_t kMinimumInferenceWorkerPoolBytes =
      96LL * 1024LL * 1024LL;
  const auto max = std::numeric_limits<std::int64_t>::max();
  const auto worker_pool_bytes =
      policy.worker_arena_bytes > max / policy.effective_workers
          ? max
          : policy.worker_arena_bytes * policy.effective_workers;
  if (worker_pool_bytes < kMinimumInferenceWorkerPoolBytes) {
    return false;
  }
  const auto profile = profile_inference_batch(frontend_name, batch);
  if (profile.sampled_rows == 0) {
    return false;
  }
  const bool enough_work =
      (profile.rows >= kMinimumRows && profile.source_bytes >= kMinimumBytes) ||
      profile.source_bytes >= kLargeBatchBytes;
  if (!enough_work) {
    return false;
  }
  if (frontend_name == "jsonl") {
    return true;
  }
  if (profile.nested_values == 0) {
    return false;
  }
  // Require nested evidence in at least one eighth of sampled rows. This keeps
  // flat JSON/CSV on the serial path, where reparsing and packet ownership cost
  // more than scalar classification, while admitting nested workloads that
  // amortize worker coordination.
  return profile.nested_values >=
         std::max<std::size_t>(1, profile.sampled_rows / 8);
}

sanitize::Status scan_remaining_inference_serial(
    FrontendHandle &frontend, internal::InferenceContext *ctx,
    const PreparedOptions &opts, IngestDiagnostics *diagnostics,
    InferenceProgress *progress, ExecutionContext *execution_context,
    int64_t wanted_rows, RowBatch first_batch) {
  if (!first_batch.rows.empty()) {
    SAN_RETURN_NOT_OK(scan_inference_batch(ctx, first_batch, opts, diagnostics,
                                           progress, execution_context));
  }
  while (true) {
    if (execution_context) {
      SAN_RETURN_NOT_OK(execution_context->CheckInterrupt());
    }
    SAN_ASSIGN_OR_RAISE(RowBatch batch, frontend.next_batch(wanted_rows));
    if (batch.rows.empty()) {
      return sanitize::Status::OK();
    }
    SAN_RETURN_NOT_OK(scan_inference_batch(ctx, batch, opts, diagnostics,
                                           progress, execution_context));
  }
}

sanitize::Status scan_inference_parallel(
    std::string_view frontend_name, FrontendHandle &frontend,
    internal::InferenceContext *ctx, const PreparedOptions &opts,
    IngestDiagnostics *diagnostics, InferenceProgress *progress,
    ExecutionContext *execution_context, std::shared_ptr<void> memory_pool,
    std::shared_ptr<internal::OperationTaskArena> task_arena,
    const internal::ExecutionPolicy &policy) {
  const int64_t wanted_rows =
      internal::rows_per_batch_from_memory_limit(opts.spec.memory_limit_bytes);
  if (execution_context) {
    SAN_RETURN_NOT_OK(execution_context->CheckInterrupt());
  }
  SAN_ASSIGN_OR_RAISE(RowBatch first_batch, frontend.next_batch(wanted_rows));
  if (first_batch.rows.empty()) {
    return sanitize::Status::OK();
  }
  if (!should_parallelize_inference(frontend_name, first_batch, policy)) {
    return scan_remaining_inference_serial(frontend, ctx, opts, diagnostics,
                                           progress, execution_context,
                                           wanted_rows, std::move(first_batch));
  }

  const auto inference_policy = inference_execution_policy(policy);
  SAN_ASSIGN_OR_RAISE(auto builder,
                      internal::ParallelInferenceEvidenceBuilder::Make(
                          frontend_name, &opts, memory_pool, inference_policy));
  SAN_ASSIGN_OR_RAISE(
      auto executor,
      InferenceExecutor::Make(
          static_cast<std::size_t>(inference_policy.effective_workers),
          static_cast<std::size_t>(inference_policy.task_queue_capacity),
          static_cast<std::size_t>(inference_policy.reorder_capacity),
          [builder](internal::OwnedRowPacket &&packet, std::size_t worker_index,
                    std::stop_token stop) {
            return builder->Build(std::move(packet), worker_index, stop);
          },
          std::move(task_arena), internal::TaskArenaLane::kUpstream,
          internal::TaskTelemetryKind::kInference));

  std::uint64_t next_ordinal = 0;
  RowBatch batch = std::move(first_batch);
  while (true) {
    SAN_ASSIGN_OR_RAISE(auto row_owner,
                        internal::make_owned_row_batch(std::move(batch.rows),
                                                       std::move(batch.owner)));
    std::size_t dispatch_index = 0;
    std::size_t pending_packets = 0;
    auto limits = internal::materialization_packet_limits(inference_policy, 1);
    // A compact evidence node is still substantially larger than the shortest
    // JSON token that can produce it. Reserve for roughly 32x source expansion
    // under small memory limits; one source row above this packet target is
    // handled by the ordered serial fallback below.
    limits.target_bytes = std::min(
        limits.target_bytes, static_cast<std::size_t>(std::max<std::int64_t>(
                                 1, inference_policy.worker_arena_bytes / 32)));
    const auto maximum_parallel_source_bytes = static_cast<std::size_t>(
        std::max<std::int64_t>(1, inference_policy.worker_arena_bytes / 8));
    while (dispatch_index < row_owner->rows.size() || pending_packets > 0) {
      while (dispatch_index < row_owner->rows.size() &&
             executor->in_flight() < executor->dispatch_window()) {
        SAN_ASSIGN_OR_RAISE(auto packet,
                            internal::build_owned_row_packet(
                                row_owner, dispatch_index, limits));
        const auto row_count = packet.rows.size();
        if (packet.estimated_source_bytes > maximum_parallel_source_bytes ||
            (packet.rows.size() == 1 &&
             packet.estimated_source_bytes > limits.target_bytes)) {
          // One very large row can expand well beyond its source bytes while
          // evidence nodes and decoded keys are retained. Drain all earlier
          // ordinals, then use the reference scanner for this isolated packet
          // instead of allowing one worker result to consume the reorder pool.
          if (pending_packets > 0) {
            break;
          }
          SAN_RETURN_NOT_OK(scan_owned_inference_packet(
              ctx, packet, opts, diagnostics, progress, execution_context));
          dispatch_index += row_count;
          continue;
        }
        const auto submit_status = executor->Submit(InferenceExecutor::Packet{
            .ordinal = next_ordinal++, .payload = std::move(packet)});
        if (!submit_status.ok()) {
          executor->Cancel();
          return submit_status;
        }
        dispatch_index += row_count;
        ++pending_packets;
      }

      if (pending_packets == 0) {
        continue;
      }
      auto outcome_result = executor->TakeNext();
      if (!outcome_result.ok()) {
        executor->Cancel();
        return outcome_result.status();
      }
      auto outcome = std::move(outcome_result).ValueOrDie();
      --pending_packets;
      if (!outcome.result.ok()) {
        const auto status = outcome.result.status();
        executor->Cancel();
        return status;
      }
      SAN_RETURN_NOT_OK(reduce_evidence_packet(
          ctx, std::move(outcome.result).ValueOrDie(), opts, diagnostics,
          progress, execution_context));
    }

    if (execution_context) {
      SAN_RETURN_NOT_OK(execution_context->CheckInterrupt());
    }
    SAN_ASSIGN_OR_RAISE(batch, frontend.next_batch(wanted_rows));
    if (batch.rows.empty()) {
      break;
    }
  }
  return executor->FinishSubmission();
}

void apply_inference_diagnostics(const InferenceProgress &progress,
                                 IngestDiagnostics *diagnostics) noexcept {
  if (!diagnostics) {
    return;
  }
  diagnostics->inferred_rows = progress.rows;
  diagnostics->inferred_bytes = progress.bytes;
}

} // namespace

sanitize::Result<LogicalSchema> infer_schema_from_frontend(
    std::string_view frontend_name, FrontendHandle &frontend,
    const PreparedOptions &opts, IngestDiagnostics *diagnostics,
    bool *out_consumed, ExecutionContext *execution_context,
    std::shared_ptr<void> operation_memory_pool,
    std::shared_ptr<internal::OperationTaskArena> task_arena) {
  if (!operation_memory_pool) {
    return sanitize::Status::Invalid(
        "infer_schema_from_frontend: operation memory pool is null");
  }
  internal::PoolResource pool_resource(operation_memory_pool.get());
  internal::InferenceContext ctx(&pool_resource);
  ctx.set_default_key(opts.spec.default_key_name);
  InferenceProgress progress;

  const auto policy = internal::execution_policy_from(
      opts.spec.threading_mode, opts.spec.memory_limit_bytes);
  if (opts.spec.threading_mode == ThreadingMode::kMulti &&
      policy.effective_workers > 1) {
    SAN_RETURN_NOT_OK(scan_inference_parallel(
        frontend_name, frontend, &ctx, opts, diagnostics, &progress,
        execution_context, operation_memory_pool, std::move(task_arena),
        policy));
  } else {
    const int64_t want = internal::rows_per_batch_from_memory_limit(
        opts.spec.memory_limit_bytes);
    while (true) {
      if (execution_context) {
        SAN_RETURN_NOT_OK(execution_context->CheckInterrupt());
      }
      SAN_ASSIGN_OR_RAISE(RowBatch batch, frontend.next_batch(want));
      if (batch.rows.empty()) {
        break;
      }
      SAN_RETURN_NOT_OK(scan_inference_batch(&ctx, batch, opts, diagnostics,
                                             &progress, execution_context));
    }
  }

  apply_inference_diagnostics(progress, diagnostics);
  if (out_consumed) {
    *out_consumed = progress.rows > 0;
  }

  auto schema = internal::infer_logical_schema(ctx, opts);
  if (diagnostics) {
    diagnostics->arrow_schema_depth = arrow_schema_depth(schema);
    diagnostics->parquet_schema_depth = parquet_schema_depth(schema);
  }
  return schema;
}

} // namespace sanitize::ingest_internal
