// Shared implementation contract for Python ABI3 schema-probe methods.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "api/python_abi3/path_sources/path_sources.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/options/options.hh"

namespace core_abi3_internal::schema_probe_detail {

sanitize::Result<std::vector<std::string>> paths_from_py(PyObject *paths_obj);

sanitize::Result<sanitize::ChunkSourcePtr>
chunk_source_from_source_py(const char *source_name, PyObject *payload_obj,
                            const sanitize::PreparedOptionsPtr &prepared);

PyObject *raise_status(const sanitize::Status &status, const char *where);

PyObject *schema_probe_or_raise(schema_sanitizer_context *ctx,
                                const char *frontend_name,
                                sanitize::ChunkSourcePtr src,
                                sanitize::PreparedOptionsPtr prepared_options,
                                const char *where);

PyObject *registry_probe_or_raise(schema_sanitizer_context *ctx,
                                  const char *frontend_name,
                                  sanitize::ChunkSourcePtr src,
                                  sanitize::PreparedOptionsPtr prepared_options,
                                  const char *registry_json,
                                  const char *field_name_policy,
                                  const char *schema_mode, const char *where);

PyObject *registry_probe_path_sources_or_raise(
    schema_sanitizer_context *ctx, const std::vector<PathSourceSpec> &sources,
    sanitize::PreparedOptionsPtr prepared_options, const char *registry_json,
    const char *field_name_policy, const char *schema_mode, const char *where,
    bool skip_invalid_json_sources = false);

PyObject *registry_probe_path_sources_state_or_raise(
    schema_sanitizer_context *ctx, const std::vector<PathSourceSpec> &sources,
    sanitize::PreparedOptionsPtr prepared_options,
    std::shared_ptr<const NativeRegistryPlan> base_registry_plan,
    const char *field_name_policy, const char *schema_mode, const char *where,
    bool skip_invalid_json_sources = false);

PyObject *registry_probe_path_source_provider_or_raise(
    schema_sanitizer_context *ctx, PyObject *provider,
    sanitize::PreparedOptionsPtr prepared_options, const char *registry_json,
    const char *field_name_policy, const char *schema_mode, const char *where,
    bool skip_invalid_json_sources,
    std::shared_ptr<const NativeRegistryPlan> base_registry_plan = nullptr);

} // namespace core_abi3_internal::schema_probe_detail
