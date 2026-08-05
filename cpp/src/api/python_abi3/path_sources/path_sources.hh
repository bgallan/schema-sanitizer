// Shared native path-source helpers for Python ABI3 wrappers.

#pragma once

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "frontends/csv/source_projection.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/ingest/ingest.hh"
#include "sanitize/schema_registry/schema_registry.hh"

namespace sanitize::internal {
class OperationTaskArena;
}

namespace core_abi3_internal {

struct PathSourceSpec {
  std::string frontend;
  std::string path;
  std::string source_file;
};

struct PathSourceInput {
  std::string frontend;
  sanitize::ChunkSourcePtr chunk_source;
  std::vector<std::string> paths;
  std::vector<std::string> source_names;
  sanitize::internal::CsvSourceProjectionSetPtr csv_source_projections;
  std::int64_t input_size_hint_bytes = 0;
};

enum class PathSourceGroupPurpose {
  // Infer schema from groups using the same source-boundary rules as normal
  // materialization, with caller-controlled JSON fallback on group failures.
  kProbe,
  // Materialize grouped inputs only when the frontend can preserve source_file
  // row ownership inside the native row stream.
  kMaterialization,
};

struct PathSourceGroupPlan {
  std::size_t start = 0;
  std::size_t end = 0;
  std::string frontend;
  bool grouped = false;
  bool source_file_in_inner = false;
};

struct PathSourceRegistryProbeResult {
  sanitize::SchemaRegistryMergeResult merged;
  sanitize::IngestDiagnostics diagnostics;
};

struct ParsedPathSources {
  std::vector<PathSourceSpec> owned;
  const std::vector<PathSourceSpec> *borrowed = nullptr;

  [[nodiscard]] const std::vector<PathSourceSpec> &get() const noexcept {
    return borrowed ? *borrowed : owned;
  }
};

bool parse_path_sources(PyObject *sources_obj,
                        std::vector<PathSourceSpec> *out);
bool parse_path_sources_view(PyObject *sources_obj, ParsedPathSources *out);

PyObject *py_path_source_plan_create(PyObject *, PyObject *);

sanitize::Result<sanitize::internal::CsvSourceProjectionSetPtr>
csv_source_projections_from_path_sources(
    std::span<const PathSourceSpec> sources,
    const sanitize::PreparedOptionsPtr &prepared);

sanitize::Result<PathSourceInput>
path_source_input(const sanitize::PreparedOptionsPtr &prepared,
                  const PathSourceSpec &source);

sanitize::Result<PathSourceGroupPlan>
next_path_source_group_plan(const std::vector<PathSourceSpec> &sources,
                            std::size_t start, PathSourceGroupPurpose purpose,
                            std::string_view input_text_encoding);

sanitize::Result<PathSourceInput>
path_source_group_input(const std::vector<PathSourceSpec> &sources,
                        const PathSourceGroupPlan &group,
                        const sanitize::PreparedOptionsPtr &prepared);

sanitize::Result<sanitize::FrontendHandle> path_source_frontend(
    PathSourceInput input, const sanitize::Options &options,
    std::shared_ptr<sanitize::internal::OperationTaskArena> task_arena =
        nullptr);

std::string_view path_source_materializer_frontend(std::string_view frontend);

sanitize::Status
validate_csv_path_source_headers(const std::vector<PathSourceSpec> &sources,
                                 const sanitize::PreparedOptionsPtr &prepared);

bool path_source_failure_is_skippable_json(const PathSourceSpec &source,
                                           const sanitize::Status &status);

void merge_path_source_diagnostics(sanitize::IngestDiagnostics &out,
                                   const sanitize::IngestDiagnostics &child);

sanitize::Result<sanitize::PreparedIngest>
prepare_path_source_ingest(schema_sanitizer_context *ctx,
                           const sanitize::PreparedOptionsPtr &prepared,
                           const PathSourceSpec &source);

sanitize::Result<PathSourceRegistryProbeResult> merge_path_source_schemas(
    schema_sanitizer_context *ctx, const std::vector<PathSourceSpec> &sources,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy, bool skip_invalid_json_sources = false,
    const sanitize::LogicalSchema *previous_schema = nullptr,
    sanitize::SchemaEvolutionMode schema_evolution =
        sanitize::SchemaEvolutionMode::kAdditive);

} // namespace core_abi3_internal
