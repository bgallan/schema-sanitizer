/* Native path-source registry stream runtime. */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include "api/c/schema_sanitizer_c_sink_internal.hh"
#include "api/python_abi3/metadata/columns/api.hh"
#include "api/python_abi3/metadata/stream/stream.hh"
#include "api/python_abi3/path_sources/path_sources.hh"
#include "api/python_abi3/registry/native_multi_source_stream.hh"
#include "api/python_abi3/registry/path_source_sinks_internal.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "api/python_abi3/registry/registry_stream_metadata.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/arrow_c/cdata_stream_runtime.hh"
#include "internal/runtime/execution_policy.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/performance_telemetry.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/ingest/ingest.hh"
#include "sanitize/registry/registry.hh"
#include "sanitize/runtime/execution_context.hh"
#include "sanitize/schema_registry/schema_registry.hh"

namespace core_abi3_internal::path_registry_detail {

NativePathSourcesStreamState::~NativePathSourcesStreamState() {
  if (telemetry) {
    telemetry->Finish();
  }
}

sanitize::Status
ensure_operation_task_arena(NativePathSourcesStreamState *state) {
  if (!state || !state->prepared) {
    return sanitize::Status::Invalid(
        "native path sources stream has no prepared options");
  }
  if (state->task_arena) {
    return sanitize::Status::OK();
  }
  const auto policy = sanitize::internal::execution_policy_from(
      state->prepared->spec.threading_mode,
      state->prepared->spec.memory_limit_bytes);
  if (!state->ctx || !state->ctx->ctx) {
    return sanitize::Status::Invalid(
        "native path sources stream has no execution context");
  }
  state->operation_memory_pool =
      state->ctx->ctx->make_operation_memory_pool_handle(
          state->prepared->spec.memory_limit_bytes);
  if (!state->operation_memory_pool) {
    return sanitize::Status::OutOfMemory(
        "operation memory pool allocation failed");
  }
  state->telemetry = state->ctx->ctx->begin_performance_telemetry(
      state->operation_memory_pool, state->prepared->spec.memory_limit_bytes,
      policy.effective_workers,
      state->prepared->spec.threading_mode == sanitize::ThreadingMode::kMulti);
  SAN_ASSIGN_OR_RAISE(state->task_arena,
                      sanitize::internal::OperationTaskArena::Make(
                          static_cast<std::size_t>(std::max<std::int64_t>(
                              1, policy.effective_workers)),
                          state->telemetry));
  return sanitize::Status::OK();
}

void merge_materialization_diagnostics(
    sanitize::IngestDiagnostics *target,
    const sanitize::IngestDiagnostics &child) noexcept {
  if (!target) {
    return;
  }
  target->inferred_rows += child.inferred_rows;
  target->inferred_bytes += child.inferred_bytes;
  target->arrow_schema_depth =
      std::max(target->arrow_schema_depth, child.arrow_schema_depth);
  target->parquet_schema_depth =
      std::max(target->parquet_schema_depth, child.parquet_schema_depth);
  target->materialized_rows += child.materialized_rows;
  target->batches += child.batches;
  target->flattened_fields += child.flattened_fields;
  target->scalar_wrappings += child.scalar_wrappings;
  target->direct_arrow_input += child.direct_arrow_input;
  target->skipped_rows += child.skipped_rows;
}

bool bind_path_source_diagnostics(
    NativePathSourcesStreamState *state,
    schema_sanitizer_diagnostics *diagnostics) noexcept {
  if (!state || !diagnostics) {
    return false;
  }
  try {
    if (!state->aggregate_diagnostics) {
      state->aggregate_diagnostics =
          std::make_shared<sanitize::IngestDiagnostics>();
    }
  } catch (const std::bad_alloc &) {
    return false;
  }
  diagnostics->diagnostics = state->aggregate_diagnostics;
  return true;
}

void close_current_source(NativePathSourcesStreamState *state) noexcept {
  if (!state) {
    return;
  }
  state->metadata.reset();
  if (state->diagnostics && state->diagnostics->diagnostics &&
      state->diagnostics->diagnostics != state->aggregate_diagnostics) {
    merge_materialization_diagnostics(state->aggregate_diagnostics.get(),
                                      *state->diagnostics->diagnostics);
  }
  release_sink_outputs(state->inner, state->diagnostics);
  state->inner = nullptr;
  state->diagnostics = nullptr;
}

sanitize::Result<sanitize::IngestStream> ingest_path_source_with_registry_plan(
    NativePathSourcesStreamState *state, const PathSourceSpec &source,
    PathSourceInput input,
    std::shared_ptr<const NativeRegistryPlan> registry_plan) {
  if (!state || !registry_plan) {
    return sanitize::Status::Invalid("native registry plan is null");
  }
  const std::string frontend_name(
      path_source_materializer_frontend(input.frontend));
  const std::int64_t input_size_hint_bytes = input.input_size_hint_bytes;
  SAN_RETURN_NOT_OK(ensure_operation_task_arena(state));
  SAN_ASSIGN_OR_RAISE(auto frontend, path_source_frontend(std::move(input),
                                                          state->prepared->spec,
                                                          state->task_arena));

  frontend.set_plan(registry_plan->plan.get());
  auto diagnostics = std::make_shared<sanitize::IngestDiagnostics>();
  diagnostics->arrow_schema_depth =
      sanitize::arrow_schema_depth(registry_plan->schema);
  diagnostics->parquet_schema_depth =
      sanitize::parquet_schema_depth(registry_plan->schema);

  sanitize::PreparedIngest prepared;
  prepared.frontend_name = frontend_name;
  prepared.frontend = std::move(frontend);
  prepared.owned_ctx = state->ctx ? state->ctx->ctx : nullptr;
  prepared.ctx = prepared.owned_ctx.get();
  if (!prepared.ctx) {
    return sanitize::Status::Invalid(
        "prepared ingest has no execution context");
  }
  prepared.operation_memory_pool = state->operation_memory_pool;
  if (!prepared.operation_memory_pool) {
    return sanitize::Status::OutOfMemory(
        "operation memory pool allocation failed");
  }
  prepared.task_arena = state->task_arena;
  prepared.plan = registry_plan->plan;
  prepared.opts = state->prepared;
  prepared.diagnostics = std::move(diagnostics);
  prepared.logical_schema = registry_plan->schema;
  prepared.input_size_hint_bytes = input_size_hint_bytes;
  prepared.inference_consumed = false;
  (void)source;
  return sanitize::ingest_to_stream(std::move(prepared));
}

sanitize::Status open_next_source(NativePathSourcesStreamState *state) {
  if (!state) {
    return sanitize::Status::Invalid("native path sources stream is closed");
  }
  close_current_source(state);
  if (state->index >= state->sources.size()) {
    if (state->chunk_provider && !state->chunk_provider_exhausted) {
      SAN_RETURN_NOT_OK(load_next_provider_chunk(state));
    }
  }
  if (state->index >= state->sources.size()) {
    return sanitize::Status::OK();
  }
  const std::size_t source_index = state->index;
  const PathSourceSpec &source = state->sources[source_index];
  bool source_file_in_inner = false;
  std::shared_ptr<const NativeRegistryPlan> active_registry_plan =
      state->registry_plan;
  PathSourceInput input;

  // Compatible local path sources can share one native frontend and one
  // operation arena. This removes per-file stream teardown and lets packet
  // preparation remain continuously populated across source boundaries. Keep
  // per-source metadata wrappers ungrouped because their first-row semantics
  // must be applied independently.
  const bool can_group_materialization =
      state->registry_enabled && state->registry_plan;
  if (can_group_materialization) {
    SAN_ASSIGN_OR_RAISE(
        auto group,
        next_path_source_group_plan(state->sources, source_index,
                                    PathSourceGroupPurpose::kMaterialization,
                                    state->prepared->spec.input_text_encoding));
    if (group.grouped) {
      SAN_ASSIGN_OR_RAISE(input, path_source_group_input(
                                     state->sources, group,
                                     state->prepared->spec.input_text_encoding,
                                     state->prepared->spec.memory_limit_bytes));
      if (state->source_file_column) {
        if (!state->source_file_registry_plan) {
          SAN_ASSIGN_OR_RAISE(
              auto augmented_plan,
              make_native_registry_plan_with_generated_source_file(
                  *state->registry_plan));
          state->source_file_registry_plan = std::move(augmented_plan);
        }
        active_registry_plan = state->source_file_registry_plan;
        source_file_in_inner = group.source_file_in_inner;
      }
      state->index = group.end;
    }
  }

  if (path_source_input_empty(input)) {
    SAN_ASSIGN_OR_RAISE(input, path_source_input(state->prepared, source));
    state->index = source_index + 1;
  }

  if (state->registry_enabled && state->registry_plan) {
    auto out_r = ingest_path_source_with_registry_plan(
        state, source, std::move(input), active_registry_plan);
    if (!out_r.ok()) {
      return sanitize::Status::Invalid(
          path_source_error_message(source, out_r.status().ToString()));
    }
    auto out = std::move(out_r).ValueOrDie();
    state->inner = out.stream.release();
    state->diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
    if (!state->diagnostics) {
      schema_sanitizer_stream_free(state->inner);
      state->inner = nullptr;
      return sanitize::Status::OutOfMemory(
          "context_to_registry_sink_from_path_sources: diagnostics allocation "
          "failed");
    }
    state->diagnostics->diagnostics = std::move(out.diagnostics);
  } else if (state->registry_enabled) {
    PyRegistrySinkOutputs outputs;
    int st = context_to_registry_sink_from_source_internal(
        state->ctx, state->sink_name.c_str(), input.frontend.c_str(),
        std::move(input.chunk_source), state->prepared,
        state->registry_json.c_str(), state->field_name_policy.c_str(),
        state->schema_mode.c_str(),
        ::RegistrySinkOutputs{
            .sink = SinkOutputs{.stream = &outputs.main_stream,
                                .diagnostics = &outputs.diagnostics},
            .registry_json = &outputs.registry_json,
            .drifts_json = &outputs.drifts_json,
            .conversion_timestamp = &outputs.conversion_timestamp},
        &outputs.err, "context_to_registry_sink_from_path_sources");
    if (st != SCHEMA_SANITIZER_STATUS_OK) {
      std::string message = path_source_error_message(
          source, outputs.err ? outputs.err : "native source failed");
      release_registry_outputs(&outputs);
      schema_sanitizer_free_string(outputs.err);
      return sanitize::Status::Invalid(message);
    }
    schema_sanitizer_free_string(outputs.registry_json);
    schema_sanitizer_free_string(outputs.drifts_json);
    schema_sanitizer_free_string(outputs.conversion_timestamp);
    state->inner = outputs.main_stream;
    state->diagnostics = outputs.diagnostics;
  } else {
    ArrowArrayStream *main_stream = nullptr;
    schema_sanitizer_diagnostics *diagnostics = nullptr;
    char *err = nullptr;
    int st = context_to_sink_from_source_internal(
        state->ctx, state->sink_name.c_str(), input.frontend.c_str(),
        std::move(input.chunk_source), state->prepared,
        SinkOutputs{.stream = &main_stream, .diagnostics = &diagnostics}, &err,
        "context_to_sink_from_path_sources");
    if (st != SCHEMA_SANITIZER_STATUS_OK) {
      std::string message =
          path_source_error_message(source, err ? err : "native source failed");
      release_sink_outputs(main_stream, diagnostics);
      schema_sanitizer_free_string(err);
      return sanitize::Status::Invalid(message);
    }
    state->inner = main_stream;
    state->diagnostics = diagnostics;
  }
  state->metadata = std::make_unique<MetadataStreamState>();
  configure_metadata_stream_budget(state->metadata.get(),
                                   state->prepared->spec.memory_limit_bytes);
  state->metadata->inner = state->inner;
  if (!state->task_arena) {
    state->task_arena = sanitize::internal::task_arena_for_stream(state->inner);
  }
  if (state->task_arena) {
    sanitize::internal::attach_task_arena(state->inner, state->task_arena);
  }
  state->metadata->columns =
      metadata_columns_for_child(state, source, source_file_in_inner);
  state->metadata->first_row_pending = state->first_row_pending;
  return sanitize::Status::OK();
}

sanitize::Status path_sources_open_next(void *state) {
  return open_next_source(static_cast<NativePathSourcesStreamState *>(state));
}

void path_sources_close_current(void *state) noexcept {
  close_current_source(static_cast<NativePathSourcesStreamState *>(state));
}

MetadataStreamState *path_sources_metadata(void *state) noexcept {
  auto *typed = static_cast<NativePathSourcesStreamState *>(state);
  return typed && typed->metadata ? typed->metadata.get() : nullptr;
}

std::string &path_sources_error(void *state) noexcept {
  return static_cast<NativePathSourcesStreamState *>(state)->last_error;
}

bool *path_sources_first_row_pending(void *state) noexcept {
  return &static_cast<NativePathSourcesStreamState *>(state)->first_row_pending;
}

void path_sources_destroy_state(void *state) noexcept {
  auto *typed = static_cast<NativePathSourcesStreamState *>(state);
  close_chunk_provider(typed);
  delete typed;
}

const NativeMultiSourceStreamOps kPathSourcesOps{
    .schema_context = "path_sources.get_schema",
    .next_context = "path_sources.get_next",
    .empty_message = "native path sources stream has no sources",
    .invalid_stream_message = "invalid native path sources stream",
    .open_next = &path_sources_open_next,
    .close_current = &path_sources_close_current,
    .metadata = &path_sources_metadata,
    .last_error = &path_sources_error,
    .first_row_pending = &path_sources_first_row_pending,
    .destroy_state = &path_sources_destroy_state,
};

const char *path_sources_last_error(ArrowArrayStream *stream) {
  return native_multi_source_last_error(stream, kPathSourcesOps);
}

void path_sources_release(ArrowArrayStream *stream) {
  native_multi_source_release(stream, kPathSourcesOps);
}

int path_sources_get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
  return native_multi_source_get_schema(stream, out, kPathSourcesOps);
}

int path_sources_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  return native_multi_source_get_next(stream, out, kPathSourcesOps);
}

PyObject *pack_chunk_provider_registry_stream(
    PyObject *ctx_obj, schema_sanitizer_context *ctx, const char *sink_name,
    PyObject *stream_provider_obj,
    const sanitize::PreparedOptionsPtr &prepared_options,
    std::shared_ptr<NativeRegistryPlan> registry_plan,
    const char *field_name_policy, const char *schema_mode,
    PyObject *first_row_columns, PyObject *timestamp_columns) {
  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_json = registry_plan->registry_json;
  state->drifts_json = registry_plan->drifts_json;
  state->conversion_timestamp = registry_plan->conversion_timestamp;
  state->field_name_policy = field_name_policy ? field_name_policy : "";
  state->schema_mode = schema_mode ? schema_mode : "additive";
  state->prepared = prepared_options;
  state->registry_plan = registry_plan;
  if (!append_registry_metadata_columns(first_row_columns, timestamp_columns,
                                        &state->first_row_columns,
                                        &state->timestamp_columns)) {
    return nullptr;
  }
  append_registry_first_row_columns(&state->first_row_columns,
                                    state->registry_json, state->drifts_json);

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &path_sources_get_schema;
  stream->get_next = &path_sources_get_next;
  stream->get_last_error = &path_sources_last_error;
  stream->release = &path_sources_release;

  PyRegistrySinkOutputs outputs;
  outputs.main_stream = stream;
  outputs.diagnostics = new (std::nothrow) schema_sanitizer_diagnostics();
  if (!outputs.diagnostics) {
    schema_sanitizer_stream_free(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  if (!bind_path_source_diagnostics(state.get(), outputs.diagnostics)) {
    release_registry_outputs(&outputs);
    PyErr_NoMemory();
    return nullptr;
  }
  outputs.registry_json = dup_cstr(state->registry_json);
  outputs.drifts_json = dup_cstr(state->drifts_json);
  outputs.conversion_timestamp = dup_cstr(state->conversion_timestamp);
  if (!outputs.registry_json || !outputs.drifts_json ||
      !outputs.conversion_timestamp) {
    release_registry_outputs(&outputs);
    PyErr_NoMemory();
    return nullptr;
  }

  Py_INCREF(stream_provider_obj);
  state->chunk_provider = stream_provider_obj;
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      ctx_obj, outputs.main_stream, outputs.diagnostics, outputs.registry_json,
      outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
}

} // namespace core_abi3_internal::path_registry_detail
