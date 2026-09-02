// Runs Python ABI3 sink requests directly through the native C++ APIs. The
// bridge selects input adapters, applies registry policy, and returns owned
// Arrow streams.

#include "internal/abi/python_abi3/native_sink.hh"

#include <algorithm>
#include <exception>
#include <memory>
#include <new>
#include <string>
#include <utility>

#include "internal/planning/plan_compile.hh"
#include "sanitize/ingest/ingest.hh"
#include "sanitize/registry/registry.hh"

namespace core_abi3_internal {
namespace {

/// Converts the active standard exception into a native invalid-status result.
sanitize::Status exception_status(std::string_view where,
                                  const std::exception &error) {
  return sanitize::Status::IOError(where, ": ", error.what());
}

/// Represents a non-standard native exception without losing sink context.
sanitize::Status unknown_exception_status(std::string_view where) {
  return sanitize::Status::IOError(where, ": unknown error");
}

} // namespace

/// Packages an ingest stream and its inference diagnostics as native sink
/// output.
sanitize::Result<NativeSinkOutput>
native_sink_from_ingest_stream(sanitize::IngestStream output) {
  try {
    NativeSinkOutput result;
    result.stream.reset(output.stream.release());
    result.diagnostics = std::make_unique<NativeDiagnostics>();
    result.diagnostics->diagnostics = std::move(output.diagnostics);
    if (!result.diagnostics->diagnostics) {
      result.diagnostics->diagnostics =
          std::make_shared<sanitize::IngestDiagnostics>();
    }
    result.diagnostics->inference_snapshot = *result.diagnostics->diagnostics;
    result.diagnostics->has_inference_snapshot = true;
    // Materialization owns the live reader counters. Keep inference counters
    // in the snapshot so diagnostics do not depend on preparatory pass count.
    result.diagnostics->diagnostics->reader =
        sanitize::ReaderResourceDiagnostics{};
    return result;
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "native sink result allocation failed");
  } catch (const std::exception &error) {
    return exception_status("native_sink_from_ingest_stream", error);
  } catch (...) {
    return unknown_exception_status("native_sink_from_ingest_stream");
  }
}

/// Prepares a source and executes it into the requested native table or stream
/// sink.
sanitize::Result<NativeSinkOutput> native_sink_from_source(
    NativeContext *ctx, std::string_view sink_name,
    std::string_view frontend_name, sanitize::ChunkSourcePtr source,
    const sanitize::PreparedOptionsPtr &prepared, std::string_view where) {
  if (!ctx || !ctx->ctx) {
    return sanitize::Status::Invalid(where, ": ctx is null");
  }
  if (sink_name.empty()) {
    return sanitize::Status::Invalid(where, ": sink_name is null/empty");
  }
  if (frontend_name.empty()) {
    return sanitize::Status::Invalid(where, ": frontend_name is null/empty");
  }
  if (!source) {
    return sanitize::Status::Invalid(where, ": source is null");
  }
  if (!prepared) {
    return sanitize::Status::Invalid(where, ": prepared options are null");
  }

  try {
    auto frontend = sanitize::make_builtin_frontend(
        frontend_name, std::move(source), prepared->spec);
    if (!frontend) {
      return sanitize::Status::IOError(
          where, ": frontend not registered: ", frontend_name);
    }
    SAN_ASSIGN_OR_RAISE(auto ingest, sanitize::prepare_ingest(
                                         frontend_name, std::move(frontend),
                                         prepared, ctx->ctx.get()));
    ingest.owned_ctx = ctx->ctx;
    ingest.ctx = ctx->ctx.get();
    if (sink_name != "table" && sink_name != "stream") {
      return sanitize::Status::Invalid("sink not registered: ", sink_name);
    }
    SAN_ASSIGN_OR_RAISE(auto output,
                        sanitize::ingest_to_stream(std::move(ingest)));
    return native_sink_from_ingest_stream(std::move(output));
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(where, ": out of memory");
  } catch (const std::exception &error) {
    return exception_status(where, error);
  } catch (...) {
    return unknown_exception_status(where);
  }
}

/// Accepts additive mode or strict mode backed by a canonical registry schema.
sanitize::Status validate_registry_sink_mode(std::string_view schema_mode,
                                             std::string_view registry_json,
                                             std::string_view where) {
  const std::string_view mode =
      schema_mode.empty() ? std::string_view("additive") : schema_mode;
  if (mode == "strict") {
    auto has_schema =
        sanitize::schema_registry_has_canonical_schema(registry_json);
    if (!has_schema.ok()) {
      return has_schema.status();
    }
    if (!has_schema.ValueOrDie()) {
      return sanitize::Status::Invalid(
          where, ": schema_mode='strict' requires schema_registry to contain "
                 "canonical_schema. Use schema_mode='additive' for the first "
                 "registry-backed run.");
    }
    return sanitize::Status::OK();
  }
  if (mode == "additive") {
    return sanitize::Status::OK();
  }
  return sanitize::Status::Invalid(
      where, ": schema_mode must be 'strict' or 'additive'");
}

/// Builds the registry merge request from prepared options and an incoming
/// logical schema.
sanitize::SchemaRegistryMergeInput make_registry_merge_input(
    sanitize::LogicalSchema inferred_schema, std::string_view registry_json,
    std::string_view field_name_policy, std::string_view default_key_name,
    sanitize::FieldOrderPolicy field_order, std::string_view detected_at,
    sanitize::SchemaEvolutionMode schema_evolution) {
  sanitize::SchemaRegistryMergeInput input;
  input.inferred_schema = std::move(inferred_schema);
  input.registry_json = registry_json;
  input.field_name_policy = field_name_policy.empty()
                                ? "lower_snake"
                                : std::string(field_name_policy);
  input.default_key_name = default_key_name.empty()
                               ? std::string("default_key")
                               : std::string(default_key_name);
  input.field_order = field_order;
  input.detected_at = detected_at;
  input.schema_evolution = schema_evolution;
  return input;
}

/// Translates prepared options into the registry merge evolution policy.
sanitize::SchemaEvolutionMode
registry_schema_evolution_mode(std::string_view schema_mode) noexcept {
  return schema_mode == "strict" ? sanitize::SchemaEvolutionMode::kStrict
                                 : sanitize::SchemaEvolutionMode::kAdditive;
}

/// Applies registry evolution to a source before producing native sink output.
sanitize::Result<NativeRegistrySinkOutput> native_registry_sink_from_source(
    NativeContext *ctx, std::string_view sink_name,
    std::string_view frontend_name, sanitize::ChunkSourcePtr source,
    const sanitize::PreparedOptionsPtr &prepared,
    std::string_view registry_json, std::string_view field_name_policy,
    std::string_view schema_mode, std::string_view where) {
  if (!ctx || !ctx->ctx) {
    return sanitize::Status::Invalid(where, ": ctx is null");
  }
  if (sink_name.empty()) {
    return sanitize::Status::Invalid(where, ": sink_name is null/empty");
  }
  if (frontend_name.empty()) {
    return sanitize::Status::Invalid(where, ": frontend_name is null/empty");
  }
  if (!source) {
    return sanitize::Status::Invalid(where, ": source is null");
  }
  if (!prepared) {
    return sanitize::Status::Invalid(where, ": prepared options are null");
  }
  SAN_RETURN_NOT_OK(
      validate_registry_sink_mode(schema_mode, registry_json, where));

  try {
    auto frontend = sanitize::make_builtin_frontend(
        frontend_name, std::move(source), prepared->spec);
    if (!frontend) {
      return sanitize::Status::IOError(
          where, ": frontend not registered: ", frontend_name);
    }
    SAN_ASSIGN_OR_RAISE(auto ingest, sanitize::prepare_ingest(
                                         frontend_name, std::move(frontend),
                                         prepared, ctx->ctx.get()));
    ingest.owned_ctx = ctx->ctx;
    ingest.ctx = ctx->ctx.get();

    SAN_ASSIGN_OR_RAISE(
        auto merged,
        sanitize::merge_schema_registry(make_registry_merge_input(
            std::move(ingest.logical_schema), registry_json, field_name_policy,
            prepared->spec.default_key_name, prepared->spec.field_order,
            prepared->operation_detected_at)));
    SAN_ASSIGN_OR_RAISE(auto compiled, sanitize::compile_plan(merged.schema));
    auto plan = std::make_shared<sanitize::CompiledPlan>(std::move(compiled));
    ingest.frontend.set_plan(plan.get());
    ingest.plan = std::move(plan);
    ingest.logical_schema = merged.schema;
    if (ingest.diagnostics) {
      ingest.diagnostics->arrow_schema_depth =
          sanitize::arrow_schema_depth(merged.schema);
      ingest.diagnostics->parquet_schema_depth =
          sanitize::parquet_schema_depth(merged.schema);
    }

    SAN_ASSIGN_OR_RAISE(auto stream,
                        sanitize::ingest_to_stream(std::move(ingest)));
    SAN_ASSIGN_OR_RAISE(auto sink,
                        native_sink_from_ingest_stream(std::move(stream)));
    NativeRegistrySinkOutput result;
    result.sink = std::move(sink);
    result.registry_json = std::move(merged.registry_json);
    result.drifts_json = std::move(merged.drifts_json);
    result.conversion_timestamp = std::move(merged.detected_at);
    return result;
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(where, ": out of memory");
  } catch (const std::exception &error) {
    return exception_status(where, error);
  } catch (...) {
    return unknown_exception_status(where);
  }
}

} // namespace core_abi3_internal
