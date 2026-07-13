// Shared state and operations for Arrow-source registry sink methods.
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

#include "api/c/schema_sanitizer_c_sink_internal.hh"
#include "api/python_abi3/metadata/columns/api.hh"
#include "api/python_abi3/metadata/stream/stream.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"
#include "sanitize/registry/registry.hh"

struct ArrowArray;
struct ArrowArrayStream;
struct ArrowSchema;

namespace core_abi3_internal::arrow_registry_detail {

class GilGuard {
public:
  GilGuard() : state_(PyGILState_Ensure()) {}
  GilGuard(const GilGuard &) = delete;
  GilGuard &operator=(const GilGuard &) = delete;
  ~GilGuard() { PyGILState_Release(state_); }

private:
  PyGILState_STATE state_;
};

struct PyRegistrySinkOutputs {
  ArrowArrayStream *main_stream = nullptr;
  schema_sanitizer_diagnostics *diagnostics = nullptr;
  char *registry_json = nullptr;
  char *drifts_json = nullptr;
  char *conversion_timestamp = nullptr;
  char *err = nullptr;
};

struct ArrowSourceSpec {
  PyObject *stream_obj = nullptr;
  std::string source_file;
};

struct NativeArrowSourcesStreamState {
  schema_sanitizer_context *ctx = nullptr;
  sanitize::PreparedOptionsPtr prepared;
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
  schema_sanitizer_diagnostics *diagnostics = nullptr;
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
    schema_sanitizer_context *ctx, const std::vector<ArrowSourceSpec> &sources,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy,
    const sanitize::LogicalSchema *previous_schema = nullptr);

sanitize::Result<sanitize::SchemaRegistryMergeResult>
merge_arrow_source_provider_schemas(
    schema_sanitizer_context *ctx, PyObject *provider_obj,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy,
    const sanitize::LogicalSchema *previous_schema = nullptr);

const char *arrow_sources_last_error(ArrowArrayStream *stream);
void arrow_sources_release(ArrowArrayStream *stream);
int arrow_sources_get_schema(ArrowArrayStream *stream, ArrowSchema *out);
int arrow_sources_get_next(ArrowArrayStream *stream, ArrowArray *out);

PyObject *pack_arrow_source_provider_registry_stream(
    PyObject *ctx_obj, schema_sanitizer_context *ctx,
    PyObject *stream_provider_obj,
    const sanitize::PreparedOptionsPtr &prepared_options,
    std::shared_ptr<NativeRegistryPlan> registry_plan,
    const char *field_name_policy, const char *schema_mode,
    PyObject *first_row_columns, PyObject *timestamp_columns);

} // namespace core_abi3_internal::arrow_registry_detail
