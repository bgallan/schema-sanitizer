// Declares shared helpers for C bridge sink translation units.

#pragma once

#include "internal/abi/schema_sanitizer_c_internal.hh"

#include "nanoarrow/nanoarrow.h"

#include "sanitize/ingest/ingest.hh"
#include "sanitize/schema_registry/schema_registry.hh"

#include <string_view>

// Bundles optional C bridge outputs passed through internal sink helpers.
struct SinkOutputs {
  ArrowArrayStream **stream = nullptr;
  schema_sanitizer_diagnostics **diagnostics = nullptr;
};

// Bundles optional registry-backed sink outputs.
struct RegistrySinkOutputs {
  SinkOutputs sink;
  char **registry_json = nullptr;
  char **drifts_json = nullptr;
  char **conversion_timestamp = nullptr;
};

// Clears optional sink output pointers before an ingest attempt.
void clear_sink_outputs(SinkOutputs outputs);
// Clears optional registry sink output pointers before an ingest attempt.
void clear_registry_sink_outputs(RegistrySinkOutputs outputs);
// Converts a prepared ingest stream into Arrow C Stream outputs.
int ingest_stream_to_streams(sanitize::IngestStream o, SinkOutputs outputs,
                             char **out_error, const char *where);
// Validates the common context-to-sink arguments before ingestion starts.
int validate_context_sink_frontend(schema_sanitizer_context *ctx,
                                   const char *sink_name,
                                   const char *frontend_name,
                                   ArrowArrayStream **out_stream,
                                   const char *where, char **out_error);
// Reuses supplied prepared options or builds defaults for the sink call.
int resolve_prepared_options(
    schema_sanitizer_prepared_options *prepared_options,
    sanitize::PreparedOptionsPtr *out_prep, char **out_error);
// Converts a context/source pair into a named sink output.
int context_to_sink_from_source_internal(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, sanitize::ChunkSourcePtr src,
    const sanitize::PreparedOptionsPtr &prep, SinkOutputs outputs,
    char **out_error, const char *where);
// Validates whether registry-backed conversion may use the requested mode.
int validate_registry_sink_mode(const char *schema_mode,
                                const char *registry_json, char **out_error,
                                const char *where);
// Creates a native schema-registry merge input from sink call arguments.
sanitize::SchemaRegistryMergeInput
make_registry_merge_input(sanitize::LogicalSchema inferred_schema,
                          const char *registry_json,
                          const char *field_name_policy,
                          std::string_view default_key_name = "default_key",
                          sanitize::FieldOrderPolicy field_order =
                              sanitize::FieldOrderPolicy::kAlphabetically,
                          std::string_view detected_at = {});
// Copies registry/drift JSON strings into optional sink outputs.
int copy_registry_json_outputs(
    const sanitize::SchemaRegistryMergeResult &merged,
    RegistrySinkOutputs outputs, char **out_error, const char *where);
// Converts a source through registry merge, plan compilation, and sink output.
int context_to_registry_sink_from_source_internal(
    schema_sanitizer_context *ctx, const char *sink_name,
    const char *frontend_name, sanitize::ChunkSourcePtr src,
    const sanitize::PreparedOptionsPtr &prep, const char *registry_json,
    const char *field_name_policy, const char *schema_mode,
    RegistrySinkOutputs outputs, char **out_error, const char *where);
