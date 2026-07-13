// Path-source schema probing and registry merge orchestration.

#include "api/python_abi3/path_sources/path_sources.hh"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "api/c/schema_sanitizer_c_sink_internal.hh"
#include "frontends/builtin_frontends.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/parsing/streaming/csv/scanner.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/registry/registry.hh"

namespace core_abi3_internal {

sanitize::Result<sanitize::PreparedIngest>
prepare_path_source_ingest(schema_sanitizer_context *ctx,
                           const sanitize::PreparedOptionsPtr &prepared,
                           const PathSourceSpec &source) {
  if (!ctx) {
    return sanitize::Status::Invalid("context is null");
  }
  SAN_ASSIGN_OR_RAISE(auto input, path_source_input(prepared, source));
  const std::string frontend_name(
      path_source_materializer_frontend(input.frontend));
  SAN_ASSIGN_OR_RAISE(auto frontend,
                      path_source_frontend(std::move(input), prepared->spec));
  return sanitize::prepare_ingest(frontend_name.c_str(), std::move(frontend),
                                  prepared, ctx->ctx.get());
}

namespace {
bool json_group_failure_should_retry_per_source(
    const sanitize::Status &status) {
  return status.message().contains("trailing characters after top-level array");
}

sanitize::Result<sanitize::PreparedIngest>
prepare_path_source_group_ingest(schema_sanitizer_context *ctx,
                                 const sanitize::PreparedOptionsPtr &prepared,
                                 const std::vector<PathSourceSpec> &sources,
                                 std::size_t start, std::size_t end) {
  if (!ctx) {
    return sanitize::Status::Invalid("context is null");
  }
  if (start >= end || end > sources.size()) {
    return sanitize::Status::Invalid("path-source group is empty");
  }
  if (end == start + 1) {
    return prepare_path_source_ingest(ctx, prepared, sources[start]);
  }
  const PathSourceGroupPlan group{
      .start = start,
      .end = end,
      .frontend = sources[start].frontend,
      .grouped = true,
      .source_file_in_inner = true,
  };
  SAN_ASSIGN_OR_RAISE(
      auto input, path_source_group_input(sources, group,
                                          prepared->spec.input_text_encoding));
  const std::string frontend_name(
      path_source_materializer_frontend(input.frontend));
  SAN_ASSIGN_OR_RAISE(auto frontend,
                      path_source_frontend(std::move(input), prepared->spec));
  return sanitize::prepare_ingest(frontend_name.c_str(), std::move(frontend),
                                  prepared, ctx->ctx.get());
}

} // namespace

sanitize::Result<PathSourceRegistryProbeResult> merge_path_source_schemas(
    schema_sanitizer_context *ctx, const std::vector<PathSourceSpec> &sources,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy, bool skip_invalid_json_sources,
    const sanitize::LogicalSchema *previous_schema) {
  if (!ctx) {
    return sanitize::Status::Invalid("context is null");
  }
  SAN_RETURN_NOT_OK(validate_csv_path_source_headers(sources, prepared));

  std::string combined_registry = "{}";
  sanitize::LogicalSchema combined_schema;
  sanitize::IngestDiagnostics diagnostics;
  bool has_schema = false;
  std::vector<std::string> skipped_errors;
  auto merge_ingest = [&](sanitize::PreparedIngest ingest) -> sanitize::Status {
    if (ingest.diagnostics) {
      merge_path_source_diagnostics(diagnostics, *ingest.diagnostics);
    }
    auto merged = sanitize::merge_schema_registry(make_registry_merge_input(
        std::move(ingest.logical_schema), combined_registry.c_str(),
        field_name_policy, ingest.opts->spec.default_key_name,
        ingest.opts->spec.field_order));
    if (!merged.ok()) {
      return merged.status();
    }
    auto merged_value = std::move(merged).ValueOrDie();
    combined_schema = std::move(merged_value.schema);
    combined_registry = std::move(merged_value.registry_json);
    has_schema = true;
    return sanitize::Status::OK();
  };

  auto merge_single_source =
      [&](const PathSourceSpec &source) -> sanitize::Status {
    auto prepared_ingest = prepare_path_source_ingest(ctx, prepared, source);
    if (!prepared_ingest.ok()) {
      if (skip_invalid_json_sources && path_source_failure_is_skippable_json(
                                           source, prepared_ingest.status())) {
        skipped_errors.push_back(source.path + ": " +
                                 prepared_ingest.status().ToString());
        return sanitize::Status::OK();
      }
      return prepared_ingest.status();
    }
    return merge_ingest(std::move(prepared_ingest).ValueOrDie());
  };

  auto merge_group_sources =
      [&](const PathSourceGroupPlan &group,
          sanitize::Status group_status) -> sanitize::Status {
    const std::string &frontend = sources[group.start].frontend;
    if (frontend == "json") {
      if (!skip_invalid_json_sources &&
          !json_group_failure_should_retry_per_source(group_status)) {
        return group_status;
      }
    } else if (frontend == "json_array") {
      if (!skip_invalid_json_sources) {
        return group_status;
      }
    } else {
      return group_status;
    }
    for (std::size_t single = group.start; single < group.end; ++single) {
      SAN_RETURN_NOT_OK(merge_single_source(sources[single]));
    }
    return sanitize::Status::OK();
  };

  for (std::size_t i = 0; i < sources.size();) {
    if (!check_python_signals()) {
      return sanitize::Status::Cancelled("Python signal received");
    }
    SAN_ASSIGN_OR_RAISE(
        auto group,
        next_path_source_group_plan(sources, i, PathSourceGroupPurpose::kProbe,
                                    prepared->spec.input_text_encoding));
    if (group.grouped) {
      auto prepared_ingest = prepare_path_source_group_ingest(
          ctx, prepared, sources, group.start, group.end);
      if (prepared_ingest.ok()) {
        SAN_RETURN_NOT_OK(
            merge_ingest(std::move(prepared_ingest).ValueOrDie()));
      } else {
        SAN_RETURN_NOT_OK(merge_group_sources(group, prepared_ingest.status()));
      }
      i = group.end;
      continue;
    }
    SAN_RETURN_NOT_OK(merge_single_source(sources[i]));
    i = group.end;
  }
  if (!has_schema) {
    if (skipped_errors.empty()) {
      return sanitize::Status::Invalid("sources must not be empty");
    }
    std::string detail;
    const std::size_t first =
        skipped_errors.size() > 3 ? skipped_errors.size() - 3 : 0;
    for (std::size_t i = first; i < skipped_errors.size(); ++i) {
      if (!detail.empty()) {
        detail.append("; ");
      }
      detail.append(skipped_errors[i]);
    }
    return sanitize::Status::Invalid(
        "Schema warm-up found no valid JSON sources: ", detail);
  }

  auto merge_input = make_registry_merge_input(
      std::move(combined_schema), registry_json, field_name_policy,
      prepared->spec.default_key_name, prepared->spec.field_order);
  sanitize::Result<sanitize::SchemaRegistryMergeResult> final_merged_r =
      previous_schema ? sanitize::merge_schema_registry_with_previous_schema(
                            merge_input, *previous_schema)
                      : sanitize::merge_schema_registry(merge_input);
  if (!final_merged_r.ok()) {
    return final_merged_r.status();
  }
  auto final_merged = std::move(final_merged_r).ValueOrDie();
  return PathSourceRegistryProbeResult{std::move(final_merged), diagnostics};
}
} // namespace core_abi3_internal
