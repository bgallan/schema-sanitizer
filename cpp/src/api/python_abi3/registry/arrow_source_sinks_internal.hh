// Declares shared state and operations for Arrow-source registry sink methods.
// The routines preserve source order and Arrow ownership while applying
// compiled registry plans.

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#ifndef Py_LIMITED_API
#define Py_LIMITED_API 0x030A0000
#endif
#include <Python.h>

#include "api/python_abi3/metadata/columns/api.hh"
#include "api/python_abi3/metadata/stream/stream.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "internal/abi/python_abi3/native_sink.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"
#include "sanitize/registry/registry.hh"

struct ArrowArray;
struct ArrowArrayStream;
namespace sanitize::internal {
class PerformanceTelemetry;
}

namespace sanitize::internal {
class OperationTaskArena;
}
struct ArrowSchema;

namespace core_abi3_internal::arrow_registry_detail {

class GilGuard {
public:
  /// Acquires the Python GIL for the duration of a native registry callback.
  GilGuard() : state_(PyGILState_Ensure()) {}

  /// Disables copying so the acquired GIL state is released exactly once.
  GilGuard(const GilGuard &) = delete;

  /// Disables copy assignment so GIL ownership cannot be duplicated.
  GilGuard &operator=(const GilGuard &) = delete;

  /// Restores the Python thread state captured when the guard was created.
  ~GilGuard() { PyGILState_Release(state_); }

private:
  PyGILState_STATE state_;
};

struct PyRegistrySinkOutputs {
  ArrowArrayStream *main_stream = nullptr;
  NativeDiagnostics *diagnostics = nullptr;
  std::string registry_json = "{}";
  std::string drifts_json = "[]";
  std::string conversion_timestamp;
};

struct ArrowSourceSpec {
  PyObject *stream_obj = nullptr;
  std::string source_file;
};

struct NativeArrowSourcesStreamState {
  ~NativeArrowSourcesStreamState();
  NativeContext *ctx = nullptr;
  sanitize::PreparedOptionsPtr prepared;
  std::shared_ptr<void> operation_memory_pool;
  std::shared_ptr<sanitize::internal::PerformanceTelemetry> telemetry;
  std::shared_ptr<sanitize::internal::OperationTaskArena> task_arena;
  std::string registry_json;
  std::string field_name_policy;
  std::string schema_mode;
  PyObject *chunk_provider = nullptr;
  bool chunk_provider_exhausted = false;
  std::vector<ArrowSourceSpec> sources;
  std::vector<MetadataColumn> first_row_columns;
  std::vector<MetadataColumn> timestamp_columns;
  std::shared_ptr<const NativeRegistryPlan> registry_plan;
  std::size_t index = 0;
  bool first_row_pending = true;
  ArrowArrayStream *inner = nullptr;
  NativeDiagnostics *diagnostics = nullptr;
  std::unique_ptr<MetadataStreamState> metadata;
  std::string last_error;
};

void release_registry_outputs(PyRegistrySinkOutputs *outputs);
PyObject *pack_registry_probe(const sanitize::SchemaRegistryMergeResult &merged,
                              const sanitize::IngestDiagnostics &diagnostics);
bool parse_arrow_sources(PyObject *sources_obj,
                         std::vector<ArrowSourceSpec> *sources);
void decref_arrow_sources(std::vector<ArrowSourceSpec> *sources) noexcept;
bool arrow_provider_has_next_sources(PyObject *provider_obj);
void close_arrow_chunk_provider(NativeArrowSourcesStreamState *state) noexcept;
sanitize::Status
load_next_arrow_provider_chunk(NativeArrowSourcesStreamState *state);
sanitize::Status python_arrow_provider_error_status(const char *where);

sanitize::Result<bool>
arrow_stream_schema_matches_registry_plan(ArrowArrayStream *stream,
                                          const NativeRegistryPlan &plan,
                                          std::string_view timestamp_precision);
sanitize::Result<ArrowArrayStream *> make_passthrough_arrow_stream(
    PyObject *stream_obj, ArrowArrayStream *inner, PyObject *capsule,
    std::shared_ptr<sanitize::IngestDiagnostics> diagnostics);

sanitize::Result<sanitize::SchemaRegistryMergeResult>
merge_arrow_source_schemas(
    NativeContext *ctx, const std::vector<ArrowSourceSpec> &sources,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy,
    const sanitize::LogicalSchema *previous_schema = nullptr);

sanitize::Result<sanitize::SchemaRegistryMergeResult>
merge_arrow_source_provider_schemas(
    NativeContext *ctx, PyObject *provider_obj,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy,
    const sanitize::LogicalSchema *previous_schema = nullptr);

const char *arrow_sources_last_error(ArrowArrayStream *stream);
void arrow_sources_release(ArrowArrayStream *stream);
int arrow_sources_get_schema(ArrowArrayStream *stream, ArrowSchema *out);
int arrow_sources_get_next(ArrowArrayStream *stream, ArrowArray *out);

PyObject *pack_arrow_source_registry_stream(
    PyObject *keepalive, std::unique_ptr<NativeArrowSourcesStreamState> state,
    PyObject *chunk_provider = nullptr);
PyObject *pack_arrow_source_provider_registry_stream(
    PyObject *ctx_obj, NativeContext *ctx, PyObject *stream_provider_obj,
    const sanitize::PreparedOptionsPtr &prepared_options,
    std::shared_ptr<NativeRegistryPlan> registry_plan,
    const char *field_name_policy, const char *schema_mode,
    PyObject *first_row_columns, PyObject *timestamp_columns);

} // namespace core_abi3_internal::arrow_registry_detail
