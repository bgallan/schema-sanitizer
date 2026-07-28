// Implements bounded ordered packet preparation for multi-threaded ingestion.

#include "internal/materialization/ingest_stream/parallel_source.hh"
#include "internal/materialization/ingest_stream/parallel_source_impl.hh"

#include "frontends/json/text_row_pipeline.hh"
#include "internal/arrow_c/cdata_schema_builder.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/materialization/batch_appender.hh"
#include "internal/memory/pool_resource.hh"
#include "sanitize/core/status.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace sanitize::internal {

ParallelIngestStreamSource::ParallelIngestStreamSource(Init init)
    : fields_(std::move(init.fields)), frontend_(std::move(init.frontend)),
      plan_keepalive_(std::move(init.plan)), opts_(std::move(init.opts)),
      diagnostics_(std::move(init.diagnostics)),
      owned_ctx_keepalive_(std::move(init.owned_ctx)),
      operation_memory_pool_keepalive_(std::move(init.operation_memory_pool)),
      task_arena_keepalive_(std::move(init.task_arena)),
      telemetry_keepalive_(std::move(init.telemetry)),
      app_(std::move(init.app)), pool_keepalive_(std::move(init.pool)),
      preparer_keepalive_(std::move(init.preparer)),
      json_validator_keepalive_(std::move(init.json_validator)),
      json_validation_executor_(std::move(init.json_validation_executor)),
      executor_(std::move(init.executor)), policy_(init.policy),
      column_partition_mode_(init.column_partition_mode),
      jsonl_row_parallel_mode_(init.jsonl_row_parallel_mode) {
  if (jsonl_row_parallel_mode_ && opts_) {
    json_token_index_max_fields_ =
        json_token_index_max_fields(opts_->spec.memory_limit_bytes);
  }
}

ParallelIngestStreamSource::~ParallelIngestStreamSource() {
  if (telemetry_keepalive_) {
    telemetry_keepalive_->Finish();
  }
}

sanitize::Status
ParallelIngestStreamSource::GetSchema(struct ArrowSchema *out) {
  return export_fields_as_struct_schema(fields_, out,
                                        opts_->spec.timestamp_precision);
}

std::shared_ptr<OperationTaskArena>
ParallelIngestStreamSource::TaskArena() const noexcept {
  return task_arena_keepalive_;
}

sanitize::Status ParallelIngestStreamSource::GetNext(struct ArrowArray *out) {
  PerformancePhaseScope stream_scope(telemetry_keepalive_,
                                     PerformancePhase::kStreamGetNext);
  if (!out) {
    return sanitize::Status::Invalid(
        "ParallelIngestStreamSource::GetNext: out is null");
  }
  SAN_RETURN_NOT_OK(check_interrupt());
  if (eof_) {
    sanitize::internal::cdata_stream::clear_array(out);
    return sanitize::Status::OK();
  }

  const BatchLimits limits = batch_limits();
  for (;;) {
    SAN_RETURN_NOT_OK(batch_appender_reset(app_.get()));
    ready_columnar_array_.reset();
    SAN_RETURN_NOT_OK(fill_appender(limits));
    if (ready_columnar_array_) {
      const auto ready_rows = ready_columnar_array_->value().length;
      if (ready_rows > 0) {
        const auto sample =
            std::max<std::int64_t>(1, ready_columnar_bytes_ / ready_rows);
        observed_bytes_per_row_ =
            has_observed_batch_size_
                ? std::max<std::int64_t>(
                      1, (observed_bytes_per_row_ * 3 + sample) / 4)
                : sample;
        has_observed_batch_size_ = true;
      }
      *out = ready_columnar_array_->value();
      sanitize::internal::cdata_stream::clear_array(
          ready_columnar_array_->get());
      ready_columnar_array_.reset();
      diagnostics_.record_direct(out, limits.max_rows, limits.max_bytes,
                                 ready_columnar_bytes_);
      if (telemetry_keepalive_) {
        telemetry_keepalive_->AddCounter(PerformanceCounter::kOutputBatches);
      }
      ready_columnar_bytes_ = 0;
      return sanitize::Status::OK();
    }
    if (batch_appender_length(app_.get()) == 0) {
      if (eof_) {
        diagnostics_.flush_direct();
        sanitize::internal::cdata_stream::clear_array(out);
        return sanitize::Status::OK();
      }
      continue;
    }
    update_observed_batch_size();
    {
      PerformancePhaseScope finalize_scope(telemetry_keepalive_,
                                           PerformancePhase::kArrowFinalize);
      SAN_RETURN_NOT_OK(batch_appender_finish(app_.get(), out));
    }
    diagnostics_.record_finished(out);
    if (telemetry_keepalive_) {
      telemetry_keepalive_->AddCounter(PerformanceCounter::kOutputBatches);
    }
    return sanitize::Status::OK();
  }
}

sanitize::Status ParallelIngestStreamSource::Close() {
  closed_ = true;
  active_prepared_packet_.reset();
  ready_columnar_array_.reset();
  clear_column_partition_assemblies();
  validated_jsonl_packets_.clear();
  current_rows_keepalive_.reset();
  ready_columnar_bytes_ = 0;
  if (json_validation_executor_) {
    json_validation_executor_->Cancel();
  }
  if (executor_) {
    executor_->Cancel();
  }
  if (telemetry_keepalive_) {
    telemetry_keepalive_->Finish();
  }
  return sanitize::Status::OK();
}

ParallelIngestStreamSource::BatchLimits
ParallelIngestStreamSource::batch_limits() const {
  const int64_t memory_limit = opts_ ? opts_->spec.memory_limit_bytes : -1;
  const int64_t max_rows =
      rows_per_batch_from_memory_limit(memory_limit, observed_bytes_per_row_);
  const int64_t target_bytes =
      batch_target_bytes_from_memory_limit(memory_limit);
  return BatchLimits{
      .max_rows = max_rows, .max_bytes = target_bytes, .capacity = max_rows};
}

bool ParallelIngestStreamSource::appender_is_full(
    const BatchLimits &limits) const {
  if (ready_columnar_array_) {
    return true;
  }
  const int64_t current_length = batch_appender_length(app_.get());
  if (limits.max_rows > 0 && current_length >= limits.max_rows) {
    return true;
  }
  return limits.max_bytes > 0 && current_length > 0 &&
         batch_appender_bytes(app_.get()) >= limits.max_bytes;
}

bool ParallelIngestStreamSource::byte_limit_reached(
    const BatchLimits &limits) const {
  return limits.max_bytes > 0 && batch_appender_length(app_.get()) > 0 &&
         batch_appender_bytes(app_.get()) >= limits.max_bytes;
}

sanitize::Status
ParallelIngestStreamSource::fill_appender(const BatchLimits &limits) {
  PerformancePhaseScope coordinator_scope(telemetry_keepalive_,
                                          PerformancePhase::kCoordinatorWork);
  std::size_t interrupt_countdown = 0;
  while (!appender_is_full(limits)) {
    if ((interrupt_countdown++ & std::size_t{1023}) == 0) {
      SAN_RETURN_NOT_OK(check_interrupt());
    }
    SAN_ASSIGN_OR_RAISE(bool has_work, dispatch_available(limits));
    if (!has_work) {
      eof_ = true;
      break;
    }
    SAN_RETURN_NOT_OK(consume_next_prepared_row());
    if (byte_limit_reached(limits)) {
      break;
    }
  }
  return sanitize::Status::OK();
}

void ParallelIngestStreamSource::update_observed_batch_size() {
  const auto rows = batch_appender_length(app_.get());
  const auto bytes = batch_appender_bytes(app_.get());
  if (rows <= 0) {
    return;
  }
  const auto sample = std::max<int64_t>(1, bytes / rows);
  observed_bytes_per_row_ =
      has_observed_batch_size_
          ? std::max<int64_t>(1, (observed_bytes_per_row_ * 3 + sample) / 4)
          : sample;
  has_observed_batch_size_ = true;
}

sanitize::Result<std::shared_ptr<sanitize::ExportBatchSource>>
make_parallel_ingest_stream_source(
    std::string_view frontend_name, std::vector<RuntimeFieldLayout> fields,
    FrontendHandle frontend, std::shared_ptr<const CompiledPlan> plan,
    PreparedOptionsPtr opts, std::shared_ptr<IngestDiagnostics> diagnostics,
    std::shared_ptr<sanitize::ExecutionContext> owned_ctx,
    std::shared_ptr<void> operation_memory_pool,
    std::shared_ptr<OperationTaskArena> task_arena,
    std::shared_ptr<PerformanceTelemetry> telemetry,
    const ExecutionPolicy &policy, std::int64_t input_size_hint_bytes) {
  if (!plan) {
    return sanitize::Status::Invalid(
        "make_parallel_ingest_stream_source: plan is null");
  }
  if (!opts) {
    return sanitize::Status::Invalid(
        "make_parallel_ingest_stream_source: opts is null");
  }
  if (policy.effective_workers <= 1) {
    return sanitize::Status::Invalid(
        "make_parallel_ingest_stream_source: effective worker count must be "
        "greater than one");
  }
  if (policy.materialization_packet_target_bytes <= 0 ||
      policy.materialization_packet_max_rows <= 1) {
    return sanitize::Status::Invalid(
        "make_parallel_ingest_stream_source: packet policy is invalid");
  }

  const auto expected_rows = diagnostics ? diagnostics->inferred_rows : 0;
  const bool partition_eligible =
      frontend_name == "jsonl" && column_partition_enabled(*plan, *opts);
  const bool partition_candidate =
      partition_eligible &&
      should_use_column_partition(*plan, *opts, policy.effective_workers,
                                  expected_rows, input_size_hint_bytes);
  const bool jsonl_row_parallel = partition_eligible && !partition_candidate;
  const bool deferred_json_rows =
      (frontend_name == "json" || frontend_name == "json_array") &&
      !partition_candidate;
  auto stage_policy = materialization_execution_policy(
      *plan, policy, expected_rows, input_size_hint_bytes);
  if (partition_candidate) {
    stage_policy = execution_policy_with_worker_ceiling(
        policy,
        std::min(policy.effective_workers,
                 std::max<std::int64_t>(16, policy.effective_workers / 2)));
    stage_policy.materialization_packet_target_bytes = std::max<std::int64_t>(
        128 * 1024, policy.materialization_packet_target_bytes / 4);
    stage_policy.materialization_packet_max_rows =
        std::min<std::int64_t>(policy.materialization_packet_max_rows, 2048);
    frontend.set_materialization_mode(
        FrontendMaterializationMode::kPlanOrdered);
  } else if (jsonl_row_parallel) {
    stage_policy = jsonl_row_parallel_execution_policy(policy, expected_rows,
                                                       input_size_hint_bytes);
    frontend.set_materialization_mode(
        FrontendMaterializationMode::kDeferredValidationRaw);
  } else if (deferred_json_rows) {
    // JSON arrays and document-array routes already frame complete values.
    // Defer the authoritative parse to worker-local direct materializers so
    // the coordinator never parses the same row a second time.
    frontend.set_materialization_mode(
        FrontendMaterializationMode::kWorkerAuthoritativeRaw);
  }

  auto pool = std::make_shared<PoolResource>(operation_memory_pool);
  SAN_ASSIGN_OR_RAISE(auto app, make_batch_appender(*plan, pool));
  SAN_ASSIGN_OR_RAISE(auto preparer, ParallelRowPreparer::Make(
                                         frontend_name, plan, opts,
                                         operation_memory_pool, stage_policy));
  const bool column_partition_mode =
      partition_candidate && preparer->column_group_count() > 1;
  std::shared_ptr<ParallelJsonRowValidator> json_validator;
  std::unique_ptr<ParallelJsonValidationExecutor> json_validation_executor;
  if (jsonl_row_parallel) {
    SAN_ASSIGN_OR_RAISE(json_validator,
                        ParallelJsonRowValidator::Make(operation_memory_pool,
                                                       plan, stage_policy));
    SAN_ASSIGN_OR_RAISE(
        json_validation_executor,
        ParallelJsonValidationExecutor::Make(
            static_cast<std::size_t>(stage_policy.effective_workers),
            static_cast<std::size_t>(stage_policy.task_queue_capacity),
            static_cast<std::size_t>(stage_policy.reorder_capacity),
            [json_validator](JsonValidationTask &&task,
                             std::size_t worker_index, std::stop_token stop) {
              return json_validator->Validate(std::move(task), worker_index,
                                              stop);
            },
            task_arena, TaskArenaLane::kUpstream,
            TaskTelemetryKind::kJsonValidation));
  }

  SAN_ASSIGN_OR_RAISE(
      auto executor,
      ParallelPacketExecutor::Make(
          static_cast<std::size_t>(stage_policy.effective_workers),
          static_cast<std::size_t>(stage_policy.task_queue_capacity),
          static_cast<std::size_t>(stage_policy.reorder_capacity),
          [preparer](MaterializationTask &&task, std::size_t worker_index,
                     std::stop_token stop) {
            return preparer->Prepare(std::move(task), worker_index, stop);
          },
          task_arena, TaskArenaLane::kUpstream,
          TaskTelemetryKind::kMaterialization));

  std::shared_ptr<sanitize::ExportBatchSource> source =
      std::make_shared<ParallelIngestStreamSource>(
          ParallelIngestStreamSource::Init{
              .fields = std::move(fields),
              .frontend = std::move(frontend),
              .plan = std::move(plan),
              .opts = std::move(opts),
              .diagnostics = std::move(diagnostics),
              .owned_ctx = std::move(owned_ctx),
              .operation_memory_pool = std::move(operation_memory_pool),
              .task_arena = std::move(task_arena),
              .telemetry = std::move(telemetry),
              .app = std::move(app),
              .pool = std::move(pool),
              .preparer = std::move(preparer),
              .json_validator = std::move(json_validator),
              .json_validation_executor = std::move(json_validation_executor),
              .executor = std::move(executor),
              .policy = stage_policy,
              .column_partition_mode = column_partition_mode,
              .jsonl_row_parallel_mode = jsonl_row_parallel,
          });
  return source;
}

} // namespace sanitize::internal
