/*
 * Implements the Arrow-source registry multi-stream runtime.
 *
 * The routines preserve source order and Arrow ownership while applying
 * compiled registry plans.
 */

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.hh"
#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"
#include "api/python_abi3/metadata/columns/api.hh"
#include "api/python_abi3/metadata/stream/stream.hh"
#include "api/python_abi3/registry/native_multi_source_stream.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "api/python_abi3/registry/registry_stream_metadata.hh"
#include "internal/abi/python_abi3/native_sink.hh"
#include "internal/arrow_c/cdata_schema_builder.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/arrow_c/cdata_stream_runtime.hh"
#include "internal/memory/memory_budget.hh"
#include "internal/planning/options_schema_serialization.hh"
#include "internal/runtime/execution_policy.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/performance_telemetry.hh"
#include "sanitize/ingest/ingest.hh"
#include "sanitize/registry/registry.hh"
#include "sanitize/runtime/execution_context.hh"

#include "api/python_abi3/registry/arrow_source_sinks_internal.hh"

namespace core_abi3_internal::arrow_registry_detail {

/// Releases resources retained by `NativeArrowSourcesStreamState` without
/// propagating cleanup failures.
NativeArrowSourcesStreamState::~NativeArrowSourcesStreamState() {
  close_arrow_chunk_provider(this);
  decref_arrow_sources(&sources);
  if (telemetry) {
    telemetry->Finish();
  }
}

/// Lazily creates the task arena and memory accounting for an Arrow-source
/// stream.
sanitize::Status
ensure_operation_task_arena(NativeArrowSourcesStreamState *state) {
  if (!state || !state->prepared) {
    return sanitize::Status::Invalid(
        "native Arrow sources stream has no prepared options");
  }
  if (state->task_arena) {
    return sanitize::Status::OK();
  }
  const auto policy = sanitize::internal::execution_policy_from(
      state->prepared->spec.threading_mode,
      state->prepared->spec.memory_limit_bytes);
  if (!state->ctx || !state->ctx->ctx) {
    return sanitize::Status::Invalid(
        "native Arrow sources stream has no execution context");
  }
  state->operation_memory_pool =
      state->ctx->ctx->make_operation_memory_pool_handle(
          state->prepared->spec.memory_limit_bytes,
          state->prepared->operation_memory_ledger);
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
  const auto arena_budget = sanitize::internal::memory_budget_from_limit(
      state->prepared->spec.memory_limit_bytes);
  state->task_arena->SetBackpressureTimeoutMillis(
      sanitize::internal::backpressure_timeout_millis_from(arena_budget));
  state->task_arena->SetBackpressureDeadlineMillis(
      sanitize::internal::backpressure_deadline_millis_from(arena_budget));
  return sanitize::Status::OK();
}

/// Derives generated metadata columns for one child of the schema-registry
/// stream.
std::vector<MetadataColumn>
metadata_columns_for_child(const NativeArrowSourcesStreamState *state,
                           const ArrowSourceSpec &source) {
  return registry_child_metadata_columns(
      state->first_row_columns, state->timestamp_columns,
      state->first_row_pending, source.source_file,
      /*include_source_file=*/true);
}

/// Closes current source idempotently and preserves any prior schema-registry
/// stream failure.
void close_current_source(NativeArrowSourcesStreamState *state) noexcept {
  if (!state) {
    return;
  }
  state->metadata.reset();
  release_sink_outputs(state->inner, state->diagnostics);
  state->inner = nullptr;
  state->diagnostics = nullptr;
}

/// Initializes generated metadata state around the newly opened Arrow source.
sanitize::Status
finish_opened_source_metadata(NativeArrowSourcesStreamState *state,
                              const ArrowSourceSpec &source) {
  if (!state || !state->inner) {
    return sanitize::Status::Invalid("native Arrow source stream is null");
  }
  state->metadata = std::make_unique<MetadataStreamState>();
  configure_metadata_stream_budget(state->metadata.get(),
                                   state->prepared->spec.memory_limit_bytes);
  state->metadata->inner = state->inner;
  SAN_RETURN_NOT_OK(ensure_operation_task_arena(state));
  sanitize::internal::attach_task_arena(state->inner, state->task_arena);
  state->metadata->columns = metadata_columns_for_child(state, source);
  state->metadata->first_row_pending = state->first_row_pending;
  return sanitize::Status::OK();
}

/// Attempts to forward a schema-compatible Arrow source without
/// materialization.
sanitize::Result<bool>
try_open_passthrough_arrow_source(NativeArrowSourcesStreamState *state,
                                  const ArrowSourceSpec &source) {
  if (!state || !state->registry_plan) {
    return false;
  }

  PyObject *capsule = nullptr;
  ArrowArrayStream *inner = nullptr;
  {
    GilGuard gil;
    if (!acquire_arrow_stream(source.stream_obj, &capsule, &inner)) {
      PyErr_Clear();
      return false;
    }
  }
  std::unique_ptr<PyObject, decltype(&decref_with_gil)> capsule_owner(
      capsule, decref_with_gil);

  auto compatible = arrow_stream_schema_matches_registry_plan(
      inner, *state->registry_plan, state->prepared->spec.timestamp_precision);
  if (!compatible.ok()) {
    return compatible.status();
  }
  if (!compatible.ValueOrDie()) {
    return false;
  }

  auto diagnostics = std::unique_ptr<NativeDiagnostics>(
      new (std::nothrow) NativeDiagnostics());
  if (!diagnostics) {
    return sanitize::Status::OutOfMemory(
        "context_to_registry_sink_arrow_sources: diagnostics allocation "
        "failed");
  }
  auto diag_shared = std::make_shared<sanitize::IngestDiagnostics>();
  diag_shared->bind_operation_memory_pool(state->operation_memory_pool);
  diag_shared->arrow_schema_depth =
      sanitize::arrow_schema_depth(state->registry_plan->schema);
  diag_shared->parquet_schema_depth =
      sanitize::parquet_schema_depth(state->registry_plan->schema);
  diag_shared->direct_arrow_input = 1;

  auto proxy = make_passthrough_arrow_stream(source.stream_obj, inner, capsule,
                                             diag_shared);
  if (!proxy.ok()) {
    return proxy.status();
  }
  (void)capsule_owner.release();
  diagnostics->diagnostics = std::move(diag_shared);

  state->inner = *proxy;
  state->diagnostics = diagnostics.release();
  return true;
}

/// Materializes one Arrow source under the compiled registry plan and retains
/// its diagnostics.
sanitize::Result<sanitize::IngestStream>
ingest_arrow_source_with_registry_plan(NativeArrowSourcesStreamState *state,
                                       sanitize::FrontendHandle frontend) {
  if (!state || !state->registry_plan) {
    return sanitize::Status::Invalid("native Arrow registry plan is null");
  }
  frontend.set_plan(state->registry_plan->plan.get());

  auto diagnostics = std::make_shared<sanitize::IngestDiagnostics>();
  diagnostics->bind_operation_memory_pool(state->operation_memory_pool);
  diagnostics->arrow_schema_depth =
      sanitize::arrow_schema_depth(state->registry_plan->schema);
  diagnostics->parquet_schema_depth =
      sanitize::parquet_schema_depth(state->registry_plan->schema);
  diagnostics->direct_arrow_input = 1;

  sanitize::PreparedIngest prepared;
  prepared.frontend_name = "arrow";
  prepared.frontend = std::move(frontend);
  prepared.owned_ctx = state->ctx ? state->ctx->ctx : nullptr;
  prepared.ctx = prepared.owned_ctx.get();
  if (!prepared.ctx) {
    return sanitize::Status::Invalid(
        "prepared ingest has no execution context");
  }
  SAN_RETURN_NOT_OK(ensure_operation_task_arena(state));
  prepared.operation_memory_pool = state->operation_memory_pool;
  if (!prepared.operation_memory_pool) {
    return sanitize::Status::OutOfMemory(
        "operation memory pool allocation failed");
  }
  prepared.task_arena = state->task_arena;
  prepared.plan = state->registry_plan->plan;
  prepared.opts = state->prepared;
  prepared.diagnostics = std::move(diagnostics);
  prepared.logical_schema = state->registry_plan->schema;
  prepared.inference_consumed = false;
  return sanitize::ingest_to_stream(std::move(prepared));
}

/// Opens the next Arrow source and initializes its registry-stream state.
sanitize::Status open_next_source(NativeArrowSourcesStreamState *state) {
  if (!state) {
    return sanitize::Status::Invalid("native Arrow sources stream is closed");
  }
  close_current_source(state);
  if (state->index >= state->sources.size()) {
    if (state->chunk_provider && !state->chunk_provider_exhausted) {
      SAN_RETURN_NOT_OK(load_next_arrow_provider_chunk(state));
    }
  }
  if (state->index >= state->sources.size()) {
    return sanitize::Status::OK();
  }
  const ArrowSourceSpec &source = state->sources[state->index++];

  SAN_ASSIGN_OR_RAISE(bool passthrough,
                      try_open_passthrough_arrow_source(state, source));
  if (passthrough) {
    return finish_opened_source_metadata(state, source);
  }

  sanitize::LogicalSchema input_schema;
  sanitize::Result<sanitize::FrontendHandle> frontend_r =
      sanitize::Status::Invalid("native Arrow source was not opened");
  {
    GilGuard gil;
    frontend_r = make_arrow_frontend(
        source.stream_obj, &input_schema,
        ArrowDirectOptions{
            .timestamp_precision = state->prepared->spec.timestamp_precision,
            .memory_limit_bytes = state->prepared->spec.memory_limit_bytes});
  }
  SAN_ASSIGN_OR_RAISE(auto frontend, std::move(frontend_r));
  sanitize::Result<sanitize::IngestStream> out_r =
      sanitize::Status::Invalid("native Arrow source was not ingested");
  if (state->registry_plan) {
    out_r = ingest_arrow_source_with_registry_plan(state, std::move(frontend));
  } else {
    auto merged_r = sanitize::merge_schema_registry(make_registry_merge_input(
        std::move(input_schema), state->registry_json.c_str(),
        state->field_name_policy.c_str(),
        state->prepared->spec.default_key_name,
        state->prepared->spec.field_order,
        state->prepared->operation_detected_at));
    if (!merged_r.ok()) {
      return merged_r.status();
    }
    auto merged = std::move(merged_r).ValueOrDie();
    out_r = ingest_direct_arrow_stream(std::move(frontend),
                                       std::move(merged.schema),
                                       state->prepared, state->ctx->ctx);
  }
  if (!out_r.ok()) {
    return out_r.status();
  }

  auto sink = native_sink_from_ingest_stream(std::move(out_r).ValueOrDie());
  if (!sink.ok()) {
    return sink.status();
  }
  auto output = std::move(sink).ValueOrDie();
  state->inner = output.stream.release();
  state->diagnostics = output.diagnostics.release();
  return finish_opened_source_metadata(state, source);
}

/// Opens the next input and binds it to the active schema-registry stream
/// state.
sanitize::Status arrow_sources_open_next(void *state) {
  return open_next_source(static_cast<NativeArrowSourcesStreamState *>(state));
}

/// Closes the current input and finalizes its schema-registry stream
/// diagnostics.
void arrow_sources_close_current(void *state) noexcept {
  close_current_source(static_cast<NativeArrowSourcesStreamState *>(state));
}

/// Returns the metadata columns associated with the current schema-registry
/// stream source.
MetadataStreamState *arrow_sources_metadata(void *state) noexcept {
  auto *typed = static_cast<NativeArrowSourcesStreamState *>(state);
  return typed && typed->metadata ? typed->metadata.get() : nullptr;
}

/// Returns the error text retained by the current registry source state.
std::string &arrow_sources_error(void *state) noexcept {
  return static_cast<NativeArrowSourcesStreamState *>(state)->last_error;
}

/// Reports whether the current source still owes its generated first metadata
/// row.
bool *arrow_sources_first_row_pending(void *state) noexcept {
  return &static_cast<NativeArrowSourcesStreamState *>(state)
              ->first_row_pending;
}

/// Destroys the heap-owned schema-registry stream state after its final
/// callback completes.
void arrow_sources_destroy_state(void *state) noexcept {
  delete static_cast<NativeArrowSourcesStreamState *>(state);
}

const NativeMultiSourceStreamOps kArrowSourcesOps{
    .schema_context = "arrow_sources.get_schema",
    .next_context = "arrow_sources.get_next",
    .empty_message = "native Arrow sources stream has no sources",
    .invalid_stream_message = "invalid native Arrow sources stream",
    .open_next = &arrow_sources_open_next,
    .close_current = &arrow_sources_close_current,
    .metadata = &arrow_sources_metadata,
    .last_error = &arrow_sources_error,
    .first_row_pending = &arrow_sources_first_row_pending,
    .destroy_state = &arrow_sources_destroy_state,
};

/// Exposes the most recent schema-registry stream failure through the Arrow C
/// Stream callback.
const char *arrow_sources_last_error(ArrowArrayStream *stream) {
  return native_multi_source_last_error(stream, kArrowSourcesOps);
}

/// Releases the schema-registry stream callback state and clears all
/// transferred Arrow ownership.
void arrow_sources_release(ArrowArrayStream *stream) {
  native_multi_source_release(stream, kArrowSourcesOps);
}

/// Exports the current schema through the schema-registry stream Arrow C Stream
/// callback.
int arrow_sources_get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
  return native_multi_source_get_schema(stream, out, kArrowSourcesOps);
}

/// Produces the next array through the registry multi-source Arrow C Stream
/// callback.
int arrow_sources_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  return native_multi_source_get_next(stream, out, kArrowSourcesOps);
}

/// Packages an Arrow-source registry stream and diagnostics for Python.
PyObject *pack_arrow_source_registry_stream(
    PyObject *keepalive, std::unique_ptr<NativeArrowSourcesStreamState> state,
    PyObject *chunk_provider) {
  if (!state || !state->registry_plan) {
    PyErr_SetString(PyExc_RuntimeError,
                    "Arrow-source stream has no compiled registry plan");
    return nullptr;
  }

  if (chunk_provider) {
    Py_INCREF(chunk_provider);
    state->chunk_provider = chunk_provider;
  }

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &arrow_sources_get_schema;
  stream->get_next = &arrow_sources_get_next;
  stream->get_last_error = &arrow_sources_last_error;
  stream->release = &arrow_sources_release;

  PyRegistrySinkOutputs outputs;
  outputs.main_stream = stream;
  outputs.diagnostics = new (std::nothrow) NativeDiagnostics();
  if (!outputs.diagnostics) {
    release_arrow_stream(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  outputs.registry_json = state->registry_plan->registry_json;
  outputs.drifts_json = state->registry_plan->drifts_json;
  outputs.conversion_timestamp = state->registry_plan->conversion_timestamp;

  auto registry_plan = state->registry_plan;
  stream->private_data = state.release();
  return pack_registry_stream_result_with_state(
      keepalive, outputs.main_stream, outputs.diagnostics,
      outputs.registry_json, outputs.drifts_json, outputs.conversion_timestamp,
      std::move(registry_plan));
}

/// Packages an Arrow-provider registry stream and diagnostics for Python.
PyObject *pack_arrow_source_provider_registry_stream(
    PyObject *ctx_obj, NativeContext *ctx, PyObject *stream_provider_obj,
    const sanitize::PreparedOptionsPtr &prepared_options,
    std::shared_ptr<NativeRegistryPlan> registry_plan,
    const char *field_name_policy, const char *schema_mode,
    PyObject *first_row_columns, PyObject *timestamp_columns) {
  auto state = std::make_unique<NativeArrowSourcesStreamState>();
  state->ctx = ctx;
  state->registry_json = registry_plan->registry_json;
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
                                    registry_plan->registry_json,
                                    registry_plan->drifts_json);
  return pack_arrow_source_registry_stream(ctx_obj, std::move(state),
                                           stream_provider_obj);
}

} // namespace core_abi3_internal::arrow_registry_detail
