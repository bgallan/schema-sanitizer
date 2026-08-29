// Owns direct C++ sink results used by the private Python ABI3 module.
// These definitions keep interpreter ownership and method-table details behind
// the private extension boundary.

#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/native_state.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/ingest/ingest_types.hh"
#include "sanitize/options/options.hh"
#include "sanitize/schema_registry/schema_registry.hh"

namespace core_abi3_internal {

struct NativeSinkOutput {
  OwnedArrowStream stream;
  std::unique_ptr<NativeDiagnostics> diagnostics;
};

struct NativeRegistrySinkOutput {
  NativeSinkOutput sink;
  std::string registry_json = "{}";
  std::string drifts_json = "[]";
  std::string conversion_timestamp;
};

sanitize::Result<NativeSinkOutput>
native_sink_from_ingest_stream(sanitize::IngestStream output);

sanitize::Result<NativeSinkOutput> native_sink_from_source(
    NativeContext *ctx, std::string_view sink_name,
    std::string_view frontend_name, sanitize::ChunkSourcePtr source,
    const sanitize::PreparedOptionsPtr &prepared, std::string_view where);

sanitize::Result<NativeRegistrySinkOutput> native_registry_sink_from_source(
    NativeContext *ctx, std::string_view sink_name,
    std::string_view frontend_name, sanitize::ChunkSourcePtr source,
    const sanitize::PreparedOptionsPtr &prepared,
    std::string_view registry_json, std::string_view field_name_policy,
    std::string_view schema_mode, std::string_view where);

sanitize::Status validate_registry_sink_mode(std::string_view schema_mode,
                                             std::string_view registry_json,
                                             std::string_view where);

sanitize::SchemaRegistryMergeInput
make_registry_merge_input(sanitize::LogicalSchema inferred_schema,
                          std::string_view registry_json,
                          std::string_view field_name_policy,
                          std::string_view default_key_name = "default_key",
                          sanitize::FieldOrderPolicy field_order =
                              sanitize::FieldOrderPolicy::kAlphabetically,
                          std::string_view detected_at = {},
                          sanitize::SchemaEvolutionMode schema_evolution =
                              sanitize::SchemaEvolutionMode::kAdditive);

sanitize::SchemaEvolutionMode
registry_schema_evolution_mode(std::string_view schema_mode) noexcept;

} // namespace core_abi3_internal
