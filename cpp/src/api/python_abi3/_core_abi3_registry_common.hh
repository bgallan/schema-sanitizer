// Shared helpers for native registry-backed multi-source Python ABI streams.

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#ifndef Py_LIMITED_API
#define Py_LIMITED_API 0x030A0000
#endif
#include <Python.h>

#include "api/python_abi3/_core_abi3_metadata_columns.hh"
#include "api/python_abi3/_core_abi3_metadata_stream_builders.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"
#include "sanitize/schema_registry/schema_registry.hh"

struct ArrowArray;
struct ArrowArrayStream;
struct ArrowSchema;

namespace core_abi3_internal {

struct NativeRegistryPlan {
  sanitize::LogicalSchema schema;
  std::shared_ptr<const sanitize::CompiledPlan> plan;
  std::string registry_json;
  std::string drifts_json;
  std::string conversion_timestamp;
};

sanitize::Result<std::shared_ptr<NativeRegistryPlan>>
make_native_registry_plan(sanitize::SchemaRegistryMergeResult merged);

sanitize::Result<std::shared_ptr<NativeRegistryPlan>>
make_native_registry_plan_with_generated_source_file(
    const NativeRegistryPlan &base);

sanitize::Result<std::shared_ptr<NativeRegistryPlan>>
make_native_registry_plan_from_json(
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy, const char *drifts_json,
    const char *conversion_timestamp);

PyObject *
wrap_native_registry_state(std::shared_ptr<const NativeRegistryPlan> plan);

std::shared_ptr<const NativeRegistryPlan>
native_registry_state_from_py(PyObject *obj);

PyObject *py_registry_state_from_json(PyObject *, PyObject *);

PyObject *pack_registry_stream_result_with_state(
    PyObject *keepalive, ArrowArrayStream *main_stream,
    schema_sanitizer_diagnostics *diagnostics, char *registry_json,
    char *drifts_json, char *conversion_timestamp,
    std::shared_ptr<const NativeRegistryPlan> registry_plan);

void append_registry_first_row_columns(std::vector<MetadataColumn> *columns,
                                       const std::string &registry_json,
                                       const std::string &drifts_json);

std::vector<MetadataColumn> registry_child_metadata_columns(
    const std::vector<MetadataColumn> &first_row_columns,
    const std::vector<MetadataColumn> &timestamp_columns,
    bool first_row_pending, std::string_view source_file,
    bool include_source_file);

struct NativeMultiSourceStreamOps {
  const char *schema_context = nullptr;
  const char *next_context = nullptr;
  const char *empty_message = nullptr;
  const char *invalid_stream_message = nullptr;
  sanitize::Status (*open_next)(void *state) = nullptr;
  void (*close_current)(void *state) noexcept = nullptr;
  MetadataStreamState *(*metadata)(void *state) noexcept = nullptr;
  std::string &(*last_error)(void *state) noexcept = nullptr;
  bool *(*first_row_pending)(void *state) noexcept = nullptr;
  void (*destroy_state)(void *state) noexcept = nullptr;
};

const char *
native_multi_source_last_error(ArrowArrayStream *stream,
                               const NativeMultiSourceStreamOps &ops);

void native_multi_source_release(ArrowArrayStream *stream,
                                 const NativeMultiSourceStreamOps &ops);

int native_multi_source_get_schema(ArrowArrayStream *stream, ArrowSchema *out,
                                   const NativeMultiSourceStreamOps &ops);

int native_multi_source_get_next(ArrowArrayStream *stream, ArrowArray *out,
                                 const NativeMultiSourceStreamOps &ops);

} // namespace core_abi3_internal
