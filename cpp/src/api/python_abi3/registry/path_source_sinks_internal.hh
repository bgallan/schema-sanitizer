// Declares shared state and operations for path-source registry sink methods.
// The routines preserve source order and Arrow ownership while applying
// compiled registry plans.

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#ifndef Py_LIMITED_API
#define Py_LIMITED_API 0x030A0000
#endif
#include <Python.h>

#include "api/python_abi3/metadata/columns/api.hh"
#include "api/python_abi3/metadata/stream/stream.hh"
#include "api/python_abi3/path_sources/path_sources.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "internal/abi/python_abi3/native_sink.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"
#include "sanitize/registry/registry.hh"

struct ArrowArrayStream;
namespace sanitize::internal {
class PerformanceTelemetry;
}

namespace sanitize::internal {
class OperationTaskArena;
}

namespace core_abi3_internal::path_registry_detail {

struct PyRegistrySinkOutputs {
  ArrowArrayStream *main_stream = nullptr;
  NativeDiagnostics *diagnostics = nullptr;
  std::string registry_json = "{}";
  std::string drifts_json = "[]";
  std::string conversion_timestamp;
};

struct NativePathSourcesStreamState {
  ~NativePathSourcesStreamState();
  NativeContext *ctx = nullptr;
  sanitize::PreparedOptionsPtr prepared;
  std::shared_ptr<void> operation_memory_pool;
  std::shared_ptr<sanitize::internal::PerformanceTelemetry> telemetry;
  std::shared_ptr<sanitize::internal::OperationTaskArena> task_arena;
  std::string sink_name;
  bool registry_enabled = true;
  bool source_file_column = true;
  std::string registry_json;
  std::string drifts_json;
  std::string conversion_timestamp;
  std::string field_name_policy;
  std::string schema_mode;
  std::vector<PathSourceSpec> sources;
  std::vector<MetadataColumn> first_row_columns;
  std::vector<MetadataColumn> timestamp_columns;
  std::shared_ptr<const NativeRegistryPlan> registry_plan;
  std::shared_ptr<const NativeRegistryPlan> source_file_registry_plan;
  std::size_t index = 0;
  bool first_row_pending = true;
  PyObject *chunk_provider = nullptr;
  bool chunk_provider_exhausted = false;
  ArrowArrayStream *inner = nullptr;
  NativeDiagnostics *diagnostics = nullptr;
  std::shared_ptr<sanitize::IngestDiagnostics> aggregate_diagnostics;
  std::unique_ptr<MetadataStreamState> metadata;
  std::string last_error;
};

void release_registry_outputs(PyRegistrySinkOutputs *outputs);
bool bind_path_source_diagnostics(NativePathSourcesStreamState *state,
                                  NativeDiagnostics *diagnostics) noexcept;
void close_chunk_provider(NativePathSourcesStreamState *state) noexcept;
[[nodiscard]] bool
path_source_input_empty(const PathSourceInput &input) noexcept;
sanitize::Status load_next_provider_chunk(NativePathSourcesStreamState *state);
std::vector<MetadataColumn>
metadata_columns_for_child(const NativePathSourcesStreamState *state,
                           const PathSourceSpec &source,
                           bool source_file_in_inner = false);
std::string path_source_error_message(const PathSourceSpec &source,
                                      const std::string &message);
PyObject *pack_registry_or_raise_with_metadata(
    sanitize::Result<NativeRegistrySinkOutput> result, PyObject *keepalive,
    PyObject *first_row_columns, PyObject *all_row_columns,
    PyObject *row_span_columns, PyObject *timestamp_columns,
    std::int64_t memory_limit_bytes);

bool provider_has_next_sources(PyObject *provider_obj);
sanitize::Result<sanitize::SchemaRegistryMergeResult>
merge_path_source_provider_schemas(
    NativeContext *ctx, PyObject *provider_obj,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy, bool skip_invalid_json_sources,
    const sanitize::LogicalSchema *previous_schema = nullptr,
    sanitize::SchemaEvolutionMode schema_evolution =
        sanitize::SchemaEvolutionMode::kAdditive);

const char *path_sources_last_error(ArrowArrayStream *stream);
void path_sources_release(ArrowArrayStream *stream);
int path_sources_get_schema(ArrowArrayStream *stream, ArrowSchema *out);
int path_sources_get_next(ArrowArrayStream *stream, ArrowArray *out);

PyObject *pack_path_source_registry_stream(
    PyObject *keepalive, std::unique_ptr<NativePathSourcesStreamState> state,
    PyObject *chunk_provider = nullptr);
PyObject *pack_chunk_provider_registry_stream(
    PyObject *ctx_obj, NativeContext *ctx, const char *sink_name,
    PyObject *stream_provider_obj,
    const sanitize::PreparedOptionsPtr &prepared_options,
    std::shared_ptr<NativeRegistryPlan> registry_plan,
    const char *field_name_policy, const char *schema_mode,
    PyObject *first_row_columns, PyObject *timestamp_columns);

} // namespace core_abi3_internal::path_registry_detail
